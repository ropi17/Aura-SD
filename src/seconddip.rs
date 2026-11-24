//! Fitur "Second Dip": copytrade wallet target lewat Solana WSS.
//!
//! Alur (sesuai desain yang disepakati):
//!   1. WSS `logsSubscribe` (mentions: target wallet, commitment "processed")
//!      -> setiap transaksi target masuk, fetch detailnya via RPC HTTP.
//!   2. Dari perubahan saldo token milik target, tentukan BUY/SELL + harga entry
//!      (port logika `parse_trade` dari `monitor.py` Python).
//!      Dari transaksi yang sama, kenali juga vault pool (akun token pool yang
//!      menjadi lawan transaksi) — dipakai untuk harga real-time.
//!   3. Kalau BUY terdeteksi dan mode = SECOND_DIP dan belum ada target aktif:
//!      simpan target aktif (mint + entry + vault pool) dan mulai price watcher.
//!   4. Price watcher pantau harga via RPC Helius tiap 1 detik (baca saldo
//!      kedua vault pool, hitung harga dari rasionya — TANPA Dexscreener):
//!      - Pemicu 1: harga >= Pemicu 1 -> BUAT limit order Buy Dip di Aura
//!        (harga beli = Pemicu 2), walau harga di bawah entry.
//!      - Satu-satunya pembatalan: timeout (tidak kena Pemicu 1 dalam
//!        trigger1_timeout_mins) — TIDAK ADA batal karena harga turun
//!        di bawah entry (perintah user 2026-11-23).
//!   5. Order dibuat SETELAH harga sudah di atas harga beli (trigger1 > trigger2),
//!      jadi tidak langsung fill.
//!
//! Modul ini TIDAK memegang private key / API key. Aura API key dibaca dari
//! env `AURA_API_KEY` oleh main.rs (seperti biasa) dan dipakai lewat gRPC client
//! yang sudah terhubung. WSS/RPC endpoint dibaca dari env `WSS_URL` / `RPC_URL`
//! (default endpoint publik yang sudah teruji dari VM ini).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine as _;
use bytes::Bytes;
use log::{debug, error, info};
use teloxide::Bot;
use teloxide::prelude::Requester;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use futures_util::SinkExt;
use tokio_tungstenite::{client_async, connect_async, MaybeTlsStream, tungstenite::protocol::Message};

use crate::db;
use crate::{BotState, InterceptorFn, NoCtx};

pub const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
pub const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
pub const DEFAULT_WSS_URL: &str = "wss://api.mainnet-beta.solana.com";
pub const DEFAULT_RPC_URL: &str = "https://solana-rpc.publicnode.com";
/// Program Meteora DAMM v2 (cp-amm) — pool-nya menyimpan sqrtPrice.
pub const DAMM_V2_PROGRAM: &str = "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG";
/// Offset sqrtPrice (u128 LE) di data akun pool DAMM v2 (1112 byte).
/// Diverifikasi dengan SDK @meteora-ag/cp-amm-sdk: fetchPoolState mengembalikan
/// sqrtPrice yang sama dengan u128 di offset ini; harganya cocok dengan
/// Dexscreener (±3%) — jauh lebih akurat dari vault-ratio (±90% meleset).
pub const SQRT_PRICE_OFFSET: usize = 456;
/// Offset vault di data akun pool DAMM v2 (untuk verifikasi pool).
pub const POOL_VAULT_A_OFFSET: usize = 232;
pub const POOL_VAULT_B_OFFSET: usize = 264;

/// Keputusan dari satu harga terhadap target: Trigger (di atas/sama dengan
/// Pemicu 1 — SELALU buat order, walau harga di bawah entry), atau Wait
/// (belum kena Pemicu 1 — lanjut pantau). TIDAK ADA Cancel karena harga:
/// satu-satunya pembatalan adalah timeout 10 menit tanpa kena Pemicu 1
/// (perintah user 2026-11-23).
#[derive(Debug, PartialEq, Eq)]
pub enum PriceDecision {
    Trigger,
    Wait,
}

pub fn decide_price(price: f64, trigger1: f64) -> PriceDecision {
    if price >= trigger1 {
        PriceDecision::Trigger
    } else {
        PriceDecision::Wait
    }
}

/// Harga Pemicu 1 dari setting: mode "USD" -> harga absolut USD (kalau > 0),
/// selain itu mode "PCT" -> entry * (1 + pct%).
pub fn calc_trigger1_price(entry_price: f64, mode: &str, pct: f64, usd: f64) -> f64 {
    if mode == "USD" && usd > 0.0 {
        usd
    } else {
        entry_price * (1.0 + pct / 100.0)
    }
}
/// User-Agent browser — RPC publik menolak User-Agent default (HTTP 403).
const BROWSER_UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
const PRICE_POLL_SECS: u64 = 1;
const MAX_ORDER_RETRIES: u32 = 3;

/// Target Second Dip yang sedang dipantau harganya (runtime saja, tidak di-DB).
/// Field paid/paid_asset/slot/sig disimpan untuk diagnostik & pelacakan.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct SecondDipTarget {
    pub mint: String,
    /// Wallet target yang melakukan BUY (owner trade).
    pub wallet: String,
    pub entry_price: f64, // USD per token
    pub paid: f64,        // yang dibayar wallet target (USDC / wSOL)
    pub paid_asset: String,
    pub slot: u64,
    pub sig: String,
    /// Vault pool (akun token) untuk harga real-time via RPC — TANPA Dexscreener.
    pub base_vault: Option<String>,
    pub quote_vault: Option<String>,
    /// Akun pool DAMM v2 (untuk harga sqrtPrice via LaserStream WSS).
    /// None kalau pool bukan DAMM v2 / tidak ketemu — fallback ke vault-ratio.
    pub pool: Option<String>,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    /// Mint token quote pool (USDC / wSOL) — dari ParsedTrade.
    pub quote_mint: String,
    pub created_at: Instant,
}

/// Hasil deteksi trade dari sebuah transaksi (port `parse_trade` Python).
#[derive(Clone, Debug)]
pub struct ParsedTrade {
    pub side: String, // "BUY" atau "SELL"
    pub mint: String,
    pub tokens: f64,
    pub paid: f64,
    pub paid_asset: String, // "USDC" atau "wSOL"
    pub entry: f64,         // harga per token dalam paid_asset
    /// Vault pool lawan transaksi (untuk harga real-time). None kalau tidak ketemu.
    pub base_vault: Option<String>,
    pub quote_vault: Option<String>,
    /// Akun pool DAMM v2 (diisi setelah parse, via find_pool_account).
    pub pool: Option<String>,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    /// Mint token quote pool (USDC / wSOL) — ditentukan dari vault pool yang
    /// ketemu, BUKAN dari paid_asset. Dipakai price watcher supaya harga
    /// tidak salah konversi (pool USDC jangan dikali harga SOL).
    pub quote_mint: String,
}

/// Konversi angka ke subscript unicode (untuk notasi jumlah nol).
fn to_subscript(n: usize) -> String {
    let digits = ['₀', '₁', '₂', '₃', '₄', '₅', '₆', '₇', '₈', '₉'];
    n.to_string()
        .chars()
        .map(|c| {
            let d = c.to_digit(10).unwrap_or(0) as usize;
            digits[d]
        })
        .collect()
}

/// Format harga USD ringkas. Harga kecil (< 0.001) pakai notasi subskrip
/// ala Dexscreener supaya nol-nya tidak kepanjangan dan tetap kebaca:
/// 0.0000007 -> "0.0₆7", 0.0000025 -> "0.0₅25", 0.00001379 -> "0.0₄1379".
/// Harga >= 0.001 tampil biasa (maks 12 desimal, tanpa nol trailing).
pub fn fmt_price(p: f64) -> String {
    if !p.is_finite() {
        return "0".to_string();
    }
    if p < 0.0 {
        return format!("-{}", fmt_price(-p));
    }
    if p == 0.0 {
        return "0".to_string();
    }
    if p < 0.001 {
        let s = format!("{:.12}", p);
        let s = s.trim_end_matches('0');
        let frac = s.strip_prefix("0.").unwrap_or(s);
        let zeros = frac.chars().take_while(|&c| c == '0').count();
        if zeros >= 2 {
            let sig = frac[zeros..].trim_end_matches('0');
            if !sig.is_empty() {
                return format!("0.0{}{}", to_subscript(zeros), sig);
            }
        }
        let s = s.trim_end_matches('.');
        return if s.is_empty() {
            "0".to_string()
        } else {
            s.to_string()
        };
    }
    let s = format!("{:.12}", p);
    let s = s.trim_end_matches('0').trim_end_matches('.');
    if s.is_empty() {
        "0".to_string()
    } else {
        s.to_string()
    }
}

/// Ambil NAMA token (ticker) dari Dexscreener untuk satu mint.
/// Dipakai di notifikasi Second Dip supaya yang muncul nama token, bukan address.
/// Kalau Dexscreener tidak kenal / gagal, fallback ke address singkat.
pub(crate) async fn fetch_token_name(mint: &str) -> String {
    let info = crate::fetch_dexscreener_info(&[mint.to_string()]).await;
    crate::ticker_display(mint, &info)
}

/// Singkat alamat: 6 awal ... 4 akhir.
pub fn short_addr(addr: &str) -> String {
    if addr.len() >= 10 {
        format!("{}...{}", &addr[..6], &addr[addr.len() - 4..])
    } else {
        addr.to_string()
    }
}

/// Label pendek wallet untuk notifikasi: "9S7hhP" untuk wallet 9S7hhP,
/// `xxxx...yyyy` untuk wallet lain.
pub fn wallet_label(addr: &str) -> String {
    if addr.ends_with("9S7hhP") {
        "9S7hhP".to_string()
    } else {
        short_addr(addr)
    }
}

// ─── HTTP proxy (VM ini memblokir direct TCP) ────────────────────────────────
// Semua koneksi keluar bot (Telegram, WSS, RPC HTTP, Helius RPC, Binance)
// harus lewat HTTP proxy. URL proxy dibaca dari env saat runtime — JANGAN
// hardcode URL/username/password proxy di kode (credential bisa berubah).
// Urutan: HTTPS_PROXY -> https_proxy -> ALL_PROXY -> all_proxy.
// Kalau tidak ada proxy di env, fallback ke koneksi langsung (VPS tanpa proxy).
pub fn proxy_url_from_env() -> Option<String> {
    for key in ["HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"] {
        if let Ok(v) = std::env::var(key) {
            let v = v.trim().to_string();
            if !v.is_empty() {
                return Some(v);
            }
        }
    }
    None
}

/// Builder reqwest dengan proxy dari env (kalau ada).
/// Timeout 15 detik: request HTTP (mis. Dexscreener) tidak boleh menggantung
/// tanpa batas — handler pesan memegang lock state selama fetch, jadi request
/// yang menggantung akan membekukan seluruh bot (laporan user 2026-11-22:
/// tempel address hSOL tidak memunculkan apa-apa).
fn builder_with_proxy() -> reqwest::ClientBuilder {
    let builder = reqwest::Client::builder().timeout(Duration::from_secs(15));
    match proxy_url_from_env() {
        Some(url) => match reqwest::Proxy::all(&url) {
            Ok(proxy) => builder.proxy(proxy),
            Err(e) => {
                error!("[Proxy] URL proxy tidak valid, pakai direct: {}", e);
                builder
            }
        },
        None => builder,
    }
}

/// reqwest::Client untuk Telegram (dipakai main.rs): proxy dari env kalau ada.
pub fn build_proxied_http_client() -> reqwest::Client {
    builder_with_proxy()
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

/// reqwest 0.11 Client KHUSUS untuk Telegram: teloxide-core 0.9.1 memakai
/// reqwest 0.11, jadi `Bot::with_client` butuh Client 0.11 (bukan 0.12).
/// Proxy dari env kalau ada, fallback ke direct.
pub fn build_proxied_telegram_client() -> reqwest011::Client {
    let builder = reqwest011::Client::builder();
    let builder = match proxy_url_from_env() {
        Some(url) => match reqwest011::Proxy::all(&url) {
            Ok(proxy) => builder.proxy(proxy),
            Err(e) => {
                error!("[Proxy] URL proxy tidak valid, pakai direct: {}", e);
                builder
            }
        },
        None => builder,
    };
    builder.build().unwrap_or_else(|_| reqwest011::Client::new())
}

fn http_client() -> reqwest::Client {
    builder_with_proxy()
        .user_agent(BROWSER_UA)
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

// ─── WSS lewat HTTP CONNECT proxy ─────────────────────────────────────────────

/// Alias stream WSS — sama seperti return type `connect_async`.
type WsStream = tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Timeout per langkah koneksi WSS (TCP / CONNECT / TLS / handshake).
/// Tanpa ini, satu langkah yang macet membuat watcher diam selamanya:
/// `run_wss_watcher` tidak pernah mendapat error, jadi tidak pernah retry.
const WSS_STEP_TIMEOUT: Duration = Duration::from_secs(15);

/// Jalankan future dengan timeout 15 detik; error menyebut nama langkah
/// supaya log langsung menunjukkan di mana koneksi macet.
/// Bound sengaja `Into<Box<dyn Error>>` (bukan `E: Error`): error dari
/// `proxy_connect_handshake` sudah berupa `Box<dyn Error>` dan `Box<dyn Error>`
/// tidak memenuhi bound `Error` (butuh `Sized`), jadi bound `Error` gagal compile.
async fn step_timeout<T, E>(
    step: &str,
    fut: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    match tokio::time::timeout(WSS_STEP_TIMEOUT, fut).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(e.into()),
        Err(_) => Err(format!("timeout {} ({} detik)", step, WSS_STEP_TIMEOUT.as_secs()).into()),
    }
}

/// Kirim CONNECT ke proxy + baca respons header sampai `\r\n\r\n`.
/// Return status HTTP CONNECT (200 = tunnel terbentuk).
/// Dipanggil dengan `step_timeout` dari `connect_wss` — jangan panggil langsung.
async fn proxy_connect_handshake(
    stream: &mut TcpStream,
    host: &str,
    port: u16,
    proxy: &url::Url,
) -> Result<u32, Box<dyn std::error::Error + Send + Sync>> {
    // CONNECT target:443 + auth Basic dari userinfo proxy (kalau ada).
    let mut req = format!(
        "CONNECT {}:{} HTTP/1.1\r\nHost: {}:{}\r\n",
        host, port, host, port
    );
    if !proxy.username().is_empty() {
        let userinfo = match proxy.password() {
            Some(pw) => format!("{}:{}", proxy.username(), pw),
            None => proxy.username().to_string(),
        };
        let b64 = base64::engine::general_purpose::STANDARD.encode(userinfo.as_bytes());
        req.push_str(&format!("Proxy-Authorization: Basic {}\r\n", b64));
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes()).await?;
    stream.flush().await?;

    // Baca respons sampai \r\n\r\n; lanjut hanya kalau status 200.
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err("Proxy menutup koneksi saat CONNECT".into());
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.len() > 16384 {
            break;
        }
    }
    let head = String::from_utf8_lossy(&buf);
    let status: u32 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    Ok(status)
}

/// Konek WSS: lewat HTTP CONNECT proxy kalau ada proxy di env, atau langsung.
/// Dipakai di `run_wss_watcher` (satu-satunya tempat WSS dikonek).
/// Setiap langkah dibatasi timeout 15 detik + dicatat di log, jadi kalau
/// macet, error-nya menyebut langkahnya dan watcher bisa retry (bukan diam).
async fn connect_wss(wss_url: &str) -> Result<WsStream, Box<dyn std::error::Error + Send + Sync>> {
    let Some(proxy_url) = proxy_url_from_env() else {
        // Tanpa proxy: perilaku lama (direct), tapi tetap pakai timeout.
        info!("[SecondDip] Konek WSS direct ke {}", wss_url);
        let (ws, _) = step_timeout("konek WSS direct", connect_async(wss_url)).await?;
        info!("[SecondDip] WSS direct: WS handshake selesai");
        return Ok(ws);
    };

    // --- Lewat proxy: CONNECT tunnel -> TLS -> WebSocket handshake. ---
    let target = url::Url::parse(wss_url)?;
    let host = target
        .host_str()
        .ok_or("WSS URL tanpa host")?
        .to_string();
    let port = target.port_or_known_default().unwrap_or(443);

    let proxy = url::Url::parse(&proxy_url)?;
    let proxy_host = proxy
        .host_str()
        .ok_or("Proxy URL tanpa host")?
        .to_string();
    let proxy_port = proxy.port_or_known_default().unwrap_or(80);

    // (a) TCP ke proxy.
    info!(
        "[SecondDip] (a) TCP connect ke proxy {}:{} (target {}:{})",
        proxy_host, proxy_port, host, port
    );
    let mut stream = step_timeout(
        "TCP connect ke proxy",
        TcpStream::connect(format!("{}:{}", proxy_host, proxy_port)),
    )
    .await?;
    info!("[SecondDip] (a) TCP ke proxy OK");

    // (b) CONNECT + baca respons (kirim request + tunggu header 200).
    info!("[SecondDip] (b) Kirim CONNECT {}:{} ke proxy...", host, port);
    let status = step_timeout(
        "baca respons CONNECT",
        proxy_connect_handshake(&mut stream, &host, port, &proxy),
    )
    .await?;
    if status != 200 {
        return Err(format!("Proxy CONNECT ditolak (HTTP {})", status).into());
    }
    info!(
        "[SecondDip] (b) CONNECT 200 OK — tunnel ke {}:{} terbentuk",
        host, port
    );

    // (c) TLS (rustls + root CA).
    // PENTING — dua hal yang terbukti lewat tes:
    // (1) ALPN WAJIB diisi ("http/1.1"). ClientHello TANPA ALPN di-drop
    //     diam-diam oleh jalur egress proxy ini (handshake menggantung),
    //     sedangkan dengan ALPN handshake selesai ~0,13 detik.
    // (2) Root CA harus dari OS (rustls-native-certs) + webpki-roots.
    //     Egress proxy ini MITM TLS: sertifikat server diterbitkan ulang
    //     dengan "Hatch Sandbox Egress CA" (CA internal runtime, ada di
    //     CA store OS tapi TIDAK ada di webpki-roots). Tanpa CA OS,
    //     handshake gagal "invalid peer certificate: UnknownIssuer".
    //     webpki-roots tetap dipakai supaya di VPS (tanpa MITM) sertifikat
    //     publik biasa tetap terverifikasi.
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    // rustls-native-certs 0.8 mengembalikan struct CertificateResult
    // (bukan Result): certs yang berhasil dimuat + errors per sertifikat.
    let native = rustls_native_certs::load_native_certs();
    let mut added = 0usize;
    for cert in native.certs {
        if roots.add(cert).is_ok() {
            added += 1;
        }
    }
    if !native.errors.is_empty() {
        error!(
            "[SecondDip] (c) {} error saat muat CA OS (lanjut dengan sertifikat yang berhasil dimuat)",
            native.errors.len()
        );
    }
    info!("[SecondDip] (c) CA OS dimuat: {} sertifikat", added);
    let mut tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls_config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(tls_config));
    let server_name = rustls::pki_types::ServerName::try_from(host.as_str())?.to_owned();
    info!("[SecondDip] (c) TLS handshake ke {} mulai...", host);
    let tls_stream = step_timeout("TLS handshake", connector.connect(server_name, stream)).await?;
    info!("[SecondDip] (c) TLS handshake selesai");

    // (d) WebSocket handshake di atas stream TLS.
    info!("[SecondDip] (d) WS handshake ke {} mulai...", host);
    let (ws, _) = step_timeout(
        "WS handshake",
        client_async(wss_url, MaybeTlsStream::Rustls(tls_stream)),
    )
    .await?;
    info!("[SecondDip] (d) WS handshake selesai");
    Ok(ws)
}

// ─── Deteksi trade dari transaksi (port parse_trade) ──────────────────────────

fn balances(
    meta: &serde_json::Value,
    key: &str,
    target_wallet: &str,
) -> HashMap<String, f64> {
    let mut out: HashMap<String, f64> = HashMap::new();
    if let Some(arr) = meta.get(key).and_then(|v| v.as_array()) {
        for b in arr {
            if b.get("owner").and_then(|o| o.as_str()) == Some(target_wallet) {
                if let Some(mint) = b.get("mint").and_then(|m| m.as_str()) {
                    let amt = b
                        .get("uiTokenAmount")
                        .and_then(|u| u.get("uiAmount"))
                        .and_then(|a| a.as_f64())
                        .unwrap_or(0.0);
                    // Dijumlah, bukan ditimpa: satu wallet bisa punya >1 akun
                    // untuk mint yang sama.
                    *out.entry(mint.to_string()).or_insert(0.0) += amt;
                }
            }
        }
    }
    out
}

/// Dari transaksi, cari vault pool (akun token pool) yang menjadi lawan transaksi.
/// Vault base = akun token (mint yang dibeli) milik BUKAN target, saldonya BERKURANG.
/// Vault quote = akun USDC/wSOL milik BUKAN target, saldonya BERTAMBAH.
/// Pool = owner yang memiliki KEDUA vault itu.
/// Return (base_vault, quote_vault, base_decimals, quote_decimals).
fn find_pool_vaults(
    tx: &serde_json::Value,
    target_wallet: &str,
    bought_mint: &str,
    quote_mint: &str,
) -> Option<(String, String, u8, u8)> {
    let meta = tx.get("meta")?;
    let keys = tx
        .get("transaction")?
        .get("message")?
        .get("accountKeys")?
        .as_array()?;
    let addr_of = |idx: usize| -> Option<String> {
        let k = keys.get(idx)?;
        if let Some(s) = k.as_str() {
            return Some(s.to_string());
        }
        k.get("pubkey")
            .and_then(|p| p.as_str())
            .map(|s| s.to_string())
    };
    // Kumpulkan saldo pre & post per accountIndex: (mint, owner, pre, post, decimals).
    // Hanya akun yang muncul di KEDUA sisi yang dipertimbangkan (vault pasti sudah ada
    // sebelum dan sesudah transaksi; akun baru/ditutup bukan vault).
    let mut map: std::collections::HashMap<usize, (String, String, f64, f64, u8)> =
        std::collections::HashMap::new();
    let mut in_pre: std::collections::HashSet<usize> = std::collections::HashSet::new();
    let mut in_post: std::collections::HashSet<usize> = std::collections::HashSet::new();
    let collect = |key: &str, is_pre: bool, map: &mut std::collections::HashMap<usize, (String, String, f64, f64, u8)>, seen: &mut std::collections::HashSet<usize>| {
        if let Some(arr) = meta.get(key).and_then(|v| v.as_array()) {
            for b in arr {
                let mint = b
                    .get("mint")
                    .and_then(|m| m.as_str())
                    .unwrap_or("")
                    .to_string();
                let owner = b
                    .get("owner")
                    .and_then(|o| o.as_str())
                    .unwrap_or("")
                    .to_string();
                let amt = b
                    .get("uiTokenAmount")
                    .and_then(|u| u.get("uiAmount"))
                    .and_then(|a| a.as_f64())
                    .unwrap_or(0.0);
                let dec = b
                    .get("uiTokenAmount")
                    .and_then(|u| u.get("decimals"))
                    .and_then(|d| d.as_u64())
                    .unwrap_or(0) as u8;
                let idx = b
                    .get("accountIndex")
                    .and_then(|i| i.as_u64())
                    .unwrap_or(0) as usize;
                seen.insert(idx);
                match map.get_mut(&idx) {
                    Some(e) => {
                        if is_pre {
                            e.2 = amt;
                        } else {
                            e.3 = amt;
                        }
                    }
                    None => {
                        if is_pre {
                            map.insert(idx, (mint, owner, amt, 0.0, dec));
                        } else {
                            map.insert(idx, (mint, owner, 0.0, amt, dec));
                        }
                    }
                }
            }
        }
    };
    collect("preTokenBalances", true, &mut map, &mut in_pre);
    collect("postTokenBalances", false, &mut map, &mut in_post);
    let mut base_cands: Vec<(String, String, u8)> = Vec::new();
    let mut quote_cands: Vec<(String, String, u8)> = Vec::new();
    for (idx, (mint, owner, pre_amt, post_amt, dec)) in &map {
        if owner == target_wallet {
            continue;
        }
        // Vault harus ada di pre DAN post.
        if !(in_pre.contains(idx) && in_post.contains(idx)) {
            continue;
        }
        if mint == bought_mint && *post_amt < *pre_amt {
            if let Some(addr) = addr_of(*idx) {
                base_cands.push((owner.clone(), addr, *dec));
            }
        }
        if mint == quote_mint && *post_amt > *pre_amt {
            if let Some(addr) = addr_of(*idx) {
                quote_cands.push((owner.clone(), addr, *dec));
            }
        }
    }
    for (b_owner, b_addr, b_dec) in &base_cands {
        for (q_owner, q_addr, q_dec) in &quote_cands {
            if b_owner == q_owner {
                return Some((b_addr.clone(), q_addr.clone(), *b_dec, *q_dec));
            }
        }
    }
    None
}

/// Sewa minimum (rent-exempt) untuk satu akun token SPL: 2.039.280 lamport.
/// Dipakai untuk mengoreksi harga beli/jual native-SOL — kalau ATA token
/// baru dibuat (buy) atau ditutup (sell) di tx yang sama, sewa itu bukan
/// bagian dari harga.
const TOKEN_ACCOUNT_RENT_LAMPORTS: f64 = 2_039_280.0;

/// Saldo lamport (SOL native) wallet target di transaksi ini:
/// (pre, post, fee). Dipakai untuk mendeteksi buy/sell yang dibayar
/// atau diterima dalam SOL native (Jupiter/dll) — tidak ada perubahan
/// saldo token USDC/wSOL milik wallet, jadi `balances()` tidak melihatnya.
fn lamport_delta(
    tx: &serde_json::Value,
    target_wallet: &str,
) -> Option<(f64, f64, f64)> {
    let meta = tx.get("meta")?;
    let fee = meta.get("fee").and_then(|f| f.as_u64()).unwrap_or(0) as f64;
    let pre = meta.get("preBalances").and_then(|v| v.as_array())?;
    let post = meta.get("postBalances").and_then(|v| v.as_array())?;
    let keys = tx
        .get("transaction")?
        .get("message")?
        .get("accountKeys")?
        .as_array()?;
    // Wallet (fee payer) selalu ada di accountKeys statis.
    let idx = keys.iter().position(|k| {
        if let Some(s) = k.as_str() {
            s == target_wallet
        } else {
            k.get("pubkey").and_then(|p| p.as_str()) == Some(target_wallet)
        }
    })?;
    let pre_lam = pre.get(idx).and_then(|v| v.as_u64()).unwrap_or(0) as f64;
    let post_lam = post.get(idx).and_then(|v| v.as_u64()).unwrap_or(0) as f64;
    Some((pre_lam, post_lam, fee))
}

/// Dari transaksi (jsonParsed getTransaction), tentukan BUY/SELL milik target.
/// Return None kalau transaksi gagal atau bukan buy/sell.
pub fn parse_trade(tx: &serde_json::Value, target_wallet: &str) -> Option<ParsedTrade> {
    let meta = tx.get("meta")?;
    // Transaksi gagal — abaikan.
    if let Some(err) = meta.get("err") {
        if !err.is_null() {
            return None;
        }
    }
    let pre = balances(meta, "preTokenBalances", target_wallet);
    let post = balances(meta, "postTokenBalances", target_wallet);

    let mut bought: Vec<(String, f64)> = Vec::new();
    let mut sold: Vec<(String, f64)> = Vec::new();
    let mut mints: HashSet<&String> = HashSet::new();
    for m in pre.keys().chain(post.keys()) {
        mints.insert(m);
    }
    for m in mints {
        let d = post.get(m).copied().unwrap_or(0.0) - pre.get(m).copied().unwrap_or(0.0);
        if d > 0.0 && m != USDC_MINT && m != WSOL_MINT {
            bought.push((m.clone(), d));
        } else if d < 0.0 && m != USDC_MINT && m != WSOL_MINT {
            sold.push((m.clone(), -d));
        }
    }

    let usdc_spent = pre.get(USDC_MINT).copied().unwrap_or(0.0)
        - post.get(USDC_MINT).copied().unwrap_or(0.0);
    let usdc_got = post.get(USDC_MINT).copied().unwrap_or(0.0)
        - pre.get(USDC_MINT).copied().unwrap_or(0.0);
    let wsol_spent = pre.get(WSOL_MINT).copied().unwrap_or(0.0)
        - post.get(WSOL_MINT).copied().unwrap_or(0.0);
    let wsol_got = post.get(WSOL_MINT).copied().unwrap_or(0.0)
        - pre.get(WSOL_MINT).copied().unwrap_or(0.0);

    // Kalau ada >1 token yang berubah, pilih yang jumlahnya terbesar (deterministik).
    let pick_largest = |mut v: Vec<(String, f64)>| -> (String, f64) {
        v.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        v.into_iter().next().unwrap()
    };

    if !bought.is_empty() {
        let (mint, amt) = pick_largest(bought);
        if usdc_spent > 0.0 {
            let v = find_pool_vaults(tx, target_wallet, &mint, USDC_MINT);
            let (bv, qv, bd, qd) = match v {
                Some((a, b, c, d)) => (Some(a), Some(b), c, d),
                None => (None, None, 0, 0),
            };
            return Some(ParsedTrade {
                side: "BUY".to_string(),
                mint,
                tokens: amt,
                paid: usdc_spent,
                paid_asset: "USDC".to_string(),
                entry: if amt > 0.0 { usdc_spent / amt } else { 0.0 },
                base_vault: bv,
                quote_vault: qv,
                pool: None,
                base_decimals: bd,
                quote_decimals: qd,
                quote_mint: USDC_MINT.to_string(),
            });
        }
        if wsol_spent > 0.0 {
            let v = find_pool_vaults(tx, target_wallet, &mint, WSOL_MINT);
            let (bv, qv, bd, qd) = match v {
                Some((a, b, c, d)) => (Some(a), Some(b), c, d),
                None => (None, None, 0, 0),
            };
            return Some(ParsedTrade {
                side: "BUY".to_string(),
                mint,
                tokens: amt,
                paid: wsol_spent,
                paid_asset: "wSOL".to_string(),
                entry: if amt > 0.0 { wsol_spent / amt } else { 0.0 },
                base_vault: bv,
                quote_vault: qv,
                pool: None,
                base_decimals: bd,
                quote_decimals: qd,
                quote_mint: WSOL_MINT.to_string(),
            });
        }
        // Bayar pakai SOL native (Jupiter/dll): wallet tidak punya perubahan
        // saldo token USDC/wSOL — SOL dibelanjakan langsung sebagai lamport.
        // Tanpa ini, buy via Jupiter tidak terdeteksi sama sekali.
        if let Some((pre_lam, post_lam, fee_lam)) = lamport_delta(tx, target_wallet) {
            let mut sol_spent_lam = pre_lam - post_lam - fee_lam;
            // Kalau ATA token yang dibeli baru dibuat di tx ini, sewa ATA
            // (2.039.280 lamport) bukan bagian dari harga beli.
            if !pre.contains_key(&mint) {
                sol_spent_lam -= TOKEN_ACCOUNT_RENT_LAMPORTS;
            }
            let sol_spent = sol_spent_lam / 1e9;
            if sol_spent > 0.0 {
                // Pool bisa USDC-quoted (mis. FOTF) atau wSOL-quoted — coba keduanya.
                // Catat quote mint yang ketemu: inilah quote pool yang sebenarnya,
                // jangan ditebak dari paid_asset (bayar SOL tidak berarti pool wSOL).
                let (v, qm) = match find_pool_vaults(tx, target_wallet, &mint, USDC_MINT) {
                    Some(x) => (Some(x), USDC_MINT),
                    None => match find_pool_vaults(tx, target_wallet, &mint, WSOL_MINT) {
                        Some(x) => (Some(x), WSOL_MINT),
                        None => (None, WSOL_MINT),
                    },
                };
                let (bv, qv, bd, qd) = match v {
                    Some((a, b, c, d)) => (Some(a), Some(b), c, d),
                    None => (None, None, 0, 0),
                };
                return Some(ParsedTrade {
                    side: "BUY".to_string(),
                    mint,
                    tokens: amt,
                    paid: sol_spent,
                    // SOL native = wSOL 1:1 — entry_usd di handle_buy
                    // dikonversi pakai harga SOL, sama seperti wSOL.
                    paid_asset: "wSOL".to_string(),
                    entry: if amt > 0.0 { sol_spent / amt } else { 0.0 },
                    base_vault: bv,
                    quote_vault: qv,
                    pool: None,
                    base_decimals: bd,
                    quote_decimals: qd,
                    quote_mint: qm.to_string(),
                });
            }
        }
    }
    if !sold.is_empty() {
        let (mint, amt) = pick_largest(sold);
        if usdc_got > 0.0 {
            return Some(ParsedTrade {
                side: "SELL".to_string(),
                mint,
                tokens: amt,
                paid: usdc_got,
                paid_asset: "USDC".to_string(),
                entry: if amt > 0.0 { usdc_got / amt } else { 0.0 },
                quote_mint: USDC_MINT.to_string(),
                base_vault: None,
                quote_vault: None,
                pool: None,
                base_decimals: 0,
                quote_decimals: 0,
            });
        }
        if wsol_got > 0.0 {
            return Some(ParsedTrade {
                side: "SELL".to_string(),
                mint,
                tokens: amt,
                paid: wsol_got,
                paid_asset: "wSOL".to_string(),
                entry: if amt > 0.0 { wsol_got / amt } else { 0.0 },
                quote_mint: WSOL_MINT.to_string(),
                base_vault: None,
                quote_vault: None,
                pool: None,
                base_decimals: 0,
                quote_decimals: 0,
            });
        }
        // Terima SOL native (Jupiter/dll): tidak ada token USDC/wSOL masuk.
        if let Some((pre_lam, post_lam, fee_lam)) = lamport_delta(tx, target_wallet) {
            let mut sol_got_lam = (post_lam - pre_lam) + fee_lam;
            // Kalau ATA token yang dijual ditutup di tx ini, sewa ATA kembali
            // ke wallet — bukan bagian dari harga jual.
            if pre.contains_key(&mint) && !post.contains_key(&mint) {
                sol_got_lam -= TOKEN_ACCOUNT_RENT_LAMPORTS;
            }
            let sol_got = sol_got_lam / 1e9;
            if sol_got > 0.0 {
                return Some(ParsedTrade {
                    side: "SELL".to_string(),
                    mint,
                    tokens: amt,
                    paid: sol_got,
                    paid_asset: "wSOL".to_string(),
                    entry: if amt > 0.0 { sol_got / amt } else { 0.0 },
                    quote_mint: WSOL_MINT.to_string(),
                    base_vault: None,
                    quote_vault: None,
                    pool: None,
                    base_decimals: 0,
                    quote_decimals: 0,
                });
            }
        }
    }
    None
}

// ─── RPC (HTTP) — Helius ────────────────────────────────────────────────────

/// Ambil detail transaksi via RPC HTTP POST getTransaction (SATU PER SATU —
/// publicnode menolak batch getTransaction).
async fn fetch_transaction(rpc_url: &str, sig: &str) -> Option<serde_json::Value> {
    let client = http_client();
    let payload = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "getTransaction",
        "params": [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1}],
    });
    for attempt in 1..=3u32 {
        match client.post(rpc_url).json(&payload).send().await {
            Ok(resp) => match resp.json::<serde_json::Value>().await {
                Ok(v) => {
                    // result null = tx belum terindeks RPC; result hilang = error/rate-limit.
                    // Keduanya harus di-retry, bukan langsung return (sebelumnya langsung
                    // return dan notifikasi dibuang diam-diam).
                    match v.get("result") {
                        Some(r) if !r.is_null() => return Some(r.clone()),
                        _ => {
                            error!(
                                "[SecondDip] getTransaction result kosong (coba {}): {}",
                                attempt,
                                v.get("error")
                                    .map(|e| e.to_string())
                                    .unwrap_or_else(|| "null".to_string())
                            );
                        }
                    }
                }
                Err(e) => error!(
                    "[SecondDip] getTransaction parse gagal (coba {}): {}",
                    attempt, e
                ),
            },
            Err(e) => error!(
                "[SecondDip] getTransaction gagal (coba {}): {}",
                attempt, e
            ),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    None
}

/// URL RPC untuk harga real-time. Helius didahulukan (HELIUS_RPC_URL),
/// kalau tidak ada pakai RPC_URL biasa, kalau tidak ada pakai default publik.
/// Format Helius: https://mainnet.helius-rpc.com/?api-key=KEY
fn price_rpc_url() -> String {
    if let Ok(u) = std::env::var("HELIUS_RPC_URL") {
        if !u.trim().is_empty() {
            return u;
        }
    }
    // Helius RPC dari API key (dipakai WSS juga) — jauh lebih andal daripada
    // publicnode untuk tx yang baru mendarat.
    if let Some(u) = helius_rpc_url() {
        return u;
    }
    std::env::var("RPC_URL").unwrap_or_else(|_| DEFAULT_RPC_URL.to_string())
}

/// Saldo (raw units) sebuah akun token via RPC `getTokenAccountBalance`.
async fn fetch_token_balance(rpc_url: &str, vault: &str) -> Option<f64> {
    let client = http_client();
    let payload = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "getTokenAccountBalance",
        "params": [vault],
    });
    let resp = client.post(rpc_url).json(&payload).send().await.ok()?;
    let v = resp.json::<serde_json::Value>().await.ok()?;
    let amount_s = v
        .get("result")?
        .get("value")?
        .get("amount")?
        .as_str()?;
    amount_s.parse::<f64>().ok()
}

/// Harga USD token dari saldo vault pool via RPC (Helius) — TANPA Dexscreener.
/// price = (quote_vault / 10^quote_dec) / (base_vault / 10^base_dec).
/// Kalau quote = wSOL, konversi ke USD pakai harga SOL saat ini.
async fn fetch_vault_price(
    rpc_url: &str,
    base_vault: &str,
    quote_vault: &str,
    base_dec: u8,
    quote_dec: u8,
    quote_mint: &str,
) -> Option<f64> {
    let (b, q) = tokio::join!(
        fetch_token_balance(rpc_url, base_vault),
        fetch_token_balance(rpc_url, quote_vault)
    );
    let (b, q) = (b?, q?);
    if b <= 0.0 || q <= 0.0 {
        return None;
    }
    let base_ui = b / 10f64.powi(base_dec as i32);
    let quote_ui = q / 10f64.powi(quote_dec as i32);
    if base_ui <= 0.0 {
        return None;
    }
    let price_in_quote = quote_ui / base_ui;
    if quote_mint == USDC_MINT {
        Some(price_in_quote)
    } else {
        // quote = wSOL -> konversi ke USD
        let sol = crate::fetch_sol_price_usd().await?;
        if sol > 0.0 {
            Some(price_in_quote * sol)
        } else {
            None
        }
    }
}

// ─── Harga akurat via sqrtPrice DAMM v2 + LaserStream WSS (metode 5) ────────

/// URL WSS Helius LaserStream (wss://mainnet.helius-rpc.com/?api-key=KEY).
/// None kalau HELIUS_API_KEY tidak diset — fallback ke WSS publik.
fn helius_wss_url() -> Option<String> {
    let key = std::env::var("HELIUS_API_KEY").ok()?;
    let key = key.trim();
    if key.is_empty() {
        return None;
    }
    Some(format!("wss://mainnet.helius-rpc.com/?api-key={}", key))
}

/// URL RPC Helius dari API key yang sama (dipakai fetch transaksi & harga).
fn helius_rpc_url() -> Option<String> {
    let key = std::env::var("HELIUS_API_KEY").ok()?;
    let key = key.trim();
    if key.is_empty() {
        return None;
    }
    Some(format!("https://mainnet.helius-rpc.com/?api-key={}", key))
}

/// Harga (USD) dari data akun pool DAMM v2 (raw bytes, 1112 byte).
/// Baca sqrtPrice (u128 LE) di offset 456, lalu:
///   price_quote_per_base = (sqrtPrice / 2^64)^2 * (10^base_dec / 10^quote_dec)
/// Kalau quote = wSOL, konversi ke USD pakai `sol_usd` (None = gagal).
/// Return None kalau data terlalu pendek / sqrtPrice 0 / harga tidak valid.
/// Rumus diverifikasi dengan SDK @meteora-ag/cp-amm-sdk (`getPriceFromSqrtPrice`).
fn price_from_sqrt_price_data(
    data: &[u8],
    base_dec: u8,
    quote_dec: u8,
    quote_mint: &str,
    sol_usd: Option<f64>,
) -> Option<f64> {
    if data.len() < SQRT_PRICE_OFFSET + 16 {
        return None;
    }
    let mut buf = [0u8; 16];
    buf.copy_from_slice(&data[SQRT_PRICE_OFFSET..SQRT_PRICE_OFFSET + 16]);
    let sqrt_price = u128::from_le_bytes(buf);
    if sqrt_price == 0 {
        return None;
    }
    // (sqrtPrice / 2^64)^2 — pakai f64 (presisi cukup untuk harga).
    let ratio = sqrt_price as f64 / 18446744073709551616.0; // 2^64
    let price_raw = ratio * ratio;
    if !(price_raw > 0.0) || !price_raw.is_finite() {
        return None;
    }
    let price_in_quote =
        price_raw * 10f64.powi(base_dec as i32) / 10f64.powi(quote_dec as i32);
    if !(price_in_quote > 0.0) || !price_in_quote.is_finite() {
        return None;
    }
    if quote_mint == USDC_MINT {
        Some(price_in_quote)
    } else {
        let sol = sol_usd?;
        if sol > 0.0 {
            Some(price_in_quote * sol)
        } else {
            None
        }
    }
}

/// Harga USD token dari sqrtPrice pool DAMM v2 via RPC `getAccountInfo`.
/// Dipakai sebagai fallback polling (1 detik) kalau WSS accountSubscribe gagal.
/// Return None kalau pool bukan DAMM v2 / data tidak valid / RPC gagal.
async fn fetch_pool_price(
    rpc_url: &str,
    pool: &str,
    base_dec: u8,
    quote_dec: u8,
    quote_mint: &str,
) -> Option<f64> {
    let client = http_client();
    let payload = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "getAccountInfo",
        "params": [pool, {"encoding": "base64"}],
    });
    let resp = client.post(rpc_url).json(&payload).send().await.ok()?;
    let v = resp.json::<serde_json::Value>().await.ok()?;
    let data_b64 = v
        .get("result")?
        .get("value")?
        .get("data")?
        .as_array()?
        .get(0)?
        .as_str()?;
    let raw = base64::engine::general_purpose::STANDARD
        .decode(data_b64)
        .ok()?;
    let sol_usd = if quote_mint == USDC_MINT {
        None
    } else {
        crate::fetch_sol_price_usd().await
    };
    price_from_sqrt_price_data(&raw, base_dec, quote_dec, quote_mint, sol_usd)
}

/// Cari akun pool DAMM v2 dari transaksi buy.
/// Ambil semua accountKeys transaksi, baca via `getMultipleAccounts`,
/// cari akun yang owner-nya program DAMM v2, datanya 1112 byte,
/// dan vault-nya (offset 232/264) cocok dengan base/quote vault.
/// Return alamat pool (base58) atau None kalau tidak ketemu / bukan DAMM v2.
async fn find_pool_account(
    rpc_url: &str,
    tx: &serde_json::Value,
    base_vault: &str,
    quote_vault: &str,
) -> Option<String> {
    use std::str::FromStr;
    let keys = tx
        .get("transaction")?
        .get("message")?
        .get("accountKeys")?
        .as_array()?;
    let mut addrs: Vec<String> = Vec::new();
    for k in keys {
        if let Some(s) = k.as_str() {
            addrs.push(s.to_string());
        } else if let Some(p) = k.get("pubkey").and_then(|p| p.as_str()) {
            addrs.push(p.to_string());
        }
    }
    if addrs.is_empty() {
        return None;
    }
    let base_addr = solana_address::Address::from_str(base_vault).ok()?;
    let quote_addr = solana_address::Address::from_str(quote_vault).ok()?;
    let client = http_client();
    let payload = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "getMultipleAccounts",
        "params": [addrs, {"encoding": "base64"}],
    });
    let resp = client.post(rpc_url).json(&payload).send().await.ok()?;
    let v = resp.json::<serde_json::Value>().await.ok()?;
    let accounts = v.get("result")?.get("value")?.as_array()?;
    for (i, acc) in accounts.iter().enumerate() {
        let Some(acc) = acc.as_object() else {
            continue;
        };
        let owner = acc.get("owner").and_then(|o| o.as_str()).unwrap_or("");
        if owner != DAMM_V2_PROGRAM {
            continue;
        }
        let Some(data_b64) = acc
            .get("data")
            .and_then(|d| d.as_array())
            .and_then(|a| a.get(0))
            .and_then(|s| s.as_str())
        else {
            continue;
        };
        let Ok(raw) = base64::engine::general_purpose::STANDARD.decode(data_b64) else {
            continue;
        };
        if raw.len() != 1112 {
            continue;
        }
        // Vault A/B di offset 232/264 — harus cocok dengan vault transaksi.
        let va = &raw[POOL_VAULT_A_OFFSET..POOL_VAULT_A_OFFSET + 32];
        let vb = &raw[POOL_VAULT_B_OFFSET..POOL_VAULT_B_OFFSET + 32];
        let base_bytes: &[u8] = base_addr.as_ref();
        let quote_bytes: &[u8] = quote_addr.as_ref();
        let matches = (va == base_bytes && vb == quote_bytes)
            || (va == quote_bytes && vb == base_bytes);
        if matches {
            return Some(addrs[i].clone());
        }
    }
    None
}

/// Harga USD target: sqrtPrice (akurat) kalau pool DAMM v2 ketemu,
/// fallback ke vault-ratio kalau pool tidak ada / bukan DAMM v2.
async fn fetch_price_for_target(
    rpc_url: &str,
    target: &SecondDipTarget,
    base_vault: &str,
    quote_vault: &str,
    quote_mint: &str,
) -> Option<f64> {
    if let Some(pool) = &target.pool {
        if let Some(p) = fetch_pool_price(
            rpc_url,
            pool,
            target.base_decimals,
            target.quote_decimals,
            quote_mint,
        )
        .await
        {
            return Some(p);
        }
        // sqrtPrice gagal (pool berubah / RPC error) — fallback vault-ratio.
    }
    fetch_vault_price(
        rpc_url,
        base_vault,
        quote_vault,
        target.base_decimals,
        target.quote_decimals,
        quote_mint,
    )
    .await
}

// ─── Notifikasi ──────────────────────────────────────────────────────────────

async fn notify_all(
    bot: &Bot,
    chats: &HashSet<teloxide::types::ChatId>,
    text: &str,
) {
    for chat in chats {
        let _ = bot.send_message(*chat, text).await;
    }
}

async fn active_chats(state: &Arc<Mutex<BotState>>) -> HashSet<teloxide::types::ChatId> {
    state.lock().await.active_chats.clone()
}

/// Tulis satu baris ke file log alur Second Dip (`logs/seconddip_flow.log`,
/// relatif terhadap working directory bot — di server: /home/ubuntu/aura/logs/).
/// Timestamp WITA (UTC+8). Best-effort: tidak pernah panic; kalau gagal tulis, diam saja.
fn log_flow(line: &str) {
    use std::io::Write;
    let ts = (chrono::Utc::now() + chrono::TimeDelta::seconds(8 * 3600))
        .format("%Y-%m-%d %H:%M:%S");
    let log_line = format!("[{} WITA] {}\n", ts, line);
    let _ = std::fs::create_dir_all("logs");
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("logs/seconddip_flow.log")
    {
        let _ = f.write_all(log_line.as_bytes());
    }
}

async fn clear_target(state: &Arc<Mutex<BotState>>, mint: &str) {
    let mut st = state.lock().await;
    if matches!(&st.second_dip_target, Some(t) if t.mint == mint) {
        st.second_dip_target = None;
    }
}

// ─── WSS watcher ─────────────────────────────────────────────────────────────

/// Task utama Second Dip. Jalan terus sejak bot start, tapi hanya benar-benar
/// memantau kalau `second_dip_active && dip_mode == "SECOND_DIP"`.
/// Reconnect otomatis kalau WSS putus (tidur 5 detik, lalu coba lagi).
pub async fn run_wss_watcher(bot: Bot, state: Arc<Mutex<BotState>>) {
    // WSS Helius LaserStream didahulukan (lebih stabil dari WSS publik yang
    // putus ~1x/menit); fallback ke WSS_URL env / default publik.
    let wss_url = helius_wss_url().unwrap_or_else(|| {
        std::env::var("WSS_URL").unwrap_or_else(|_| DEFAULT_WSS_URL.to_string())
    });
    let rpc_url = price_rpc_url();
    info!("[SecondDip] Watcher mulai. WSS: {}", wss_url);

    loop {
        // Status WSS: belum terhubung di awal tiap percobaan (re)connect.
        {
            let mut st = state.lock().await;
            st.second_dip_wss_connected = false;
        }
        // Gate: hanya konek WSS kalau fitur aktif & mode Second Dip.
        // Daftar target dibaca ulang setiap reconnect (tambah/hapus dari
        // Telegram langsung dipakai tanpa restart bot).
        let (active, mode, targets) = {
            let st = state.lock().await;
            (
                st.second_dip_active,
                st.dip_mode.clone(),
                st.second_dip_targets.clone(),
            )
        };
        if !active || mode != "SECOND_DIP" {
            tokio::time::sleep(Duration::from_secs(10)).await;
            continue;
        }
        if targets.is_empty() {
            info!("[SecondDip] Daftar target kosong — menunggu wallet ditambahkan dari Telegram");
            tokio::time::sleep(Duration::from_secs(10)).await;
            continue;
        }

        // connect_wss mengembalikan WsStream saja (bukan tuple seperti connect_async).
        // Log ini membedakan "gate lolos, connect_wss dipanggil" dari "tertahan di gate".
        info!("[SecondDip] Mencoba konek WSS (gate lolos, {} target)...", targets.len());
        let mut ws = match connect_wss(&wss_url).await {
            Ok(pair) => pair,
            Err(e) => {
                error!("[SecondDip] WSS connect gagal: {} — retry 5 detik", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
        info!("[SecondDip] WSS terhubung, subscribe logs {} wallet", targets.len());
        // Satu subscription per wallet (bukan satu subscription multi-mention).
        // Helius LaserStream terbukti tidak mengirim notifikasi untuk
        // subscription multi-mention: 3 buy wallet test (21:23, 21:38, 21:45
        // WITA) tidak satu pun masuk padahal subscribe JSON benar.
        let mut sub_ok = true;
        for (i, w) in targets.iter().enumerate() {
            let sub = serde_json::json!({
                "jsonrpc": "2.0", "id": 100 + i, "method": "logsSubscribe",
                "params": [{"mentions": [w]}, {"commitment": "processed"}],
            });
            info!("[SecondDip] Subscribe wallet {}", wallet_label(w));
            if let Err(e) = ws.send(Message::text(sub.to_string())).await {
                error!("[SecondDip] subscribe gagal ({}): {} — reconnect", w, e);
                sub_ok = false;
                break;
            }
        }
        if !sub_ok {
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }
        // Subscribe berhasil — WSS terhubung.
        {
            let mut st = state.lock().await;
            st.second_dip_wss_connected = true;
        }

        // Inner loop: baca notifikasi sampai WSS putus / fitur dimatikan.
        // Ping WSS tiap 30 detik idle untuk mengukur latensi (RTT Ping/Pong) —
        // disimpan ke state agar tampil di menu (tombol status WSS).
        let mut ping_sent: Option<Instant> = None;
        loop {
            let (active, mode) = {
                let st = state.lock().await;
                (st.second_dip_active, st.dip_mode.clone())
            };
            if !active || mode != "SECOND_DIP" {
                info!("[SecondDip] dimatikan dari Telegram — putuskan WSS");
                break;
            }

            // Timeout baca 10 detik: kalau idle, kirim Ping untuk ukur latensi
            // (sekaligus cek flag aktif berkala; koneksi idle lama itu normal).
            let msg = match tokio::time::timeout(Duration::from_secs(10), ws.next()).await {
                Ok(m) => m,
                Err(_) => {
                    // Idle 10 detik — kirim Ping, catat waktu kirim untuk RTT.
                    match ws.send(Message::Ping(Bytes::from_static(b"ping"))).await {
                        Ok(()) => {
                            ping_sent = Some(Instant::now());
                        }
                        Err(e) => {
                            error!("[SecondDip] WSS ping gagal: {} — reconnect 5 detik", e);
                            break;
                        }
                    }
                    continue;
                }
            };
            let Some(msg) = msg else {
                error!("[SecondDip] WSS stream habis — reconnect 5 detik");
                break;
            };
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    error!("[SecondDip] WSS error: {} — reconnect 5 detik", e);
                    break;
                }
            };
            match msg {
                Message::Ping(data) => {
                    let _ = ws.send(Message::Pong(data)).await;
                    continue;
                }
                Message::Pong(_) => {
                    if let Some(sent) = ping_sent.take() {
                        let rtt_ms = sent.elapsed().as_millis() as u64;
                        {
                            let mut st = state.lock().await;
                            st.second_dip_wss_ping_ms = Some(rtt_ms);
                        }
                        debug!("[SecondDip] WSS ping: {}ms", rtt_ms);
                    }
                    continue;
                }
                Message::Binary(_) | Message::Frame(_) => continue,
                Message::Close(_) => {
                    error!("[SecondDip] WSS ditutup server — reconnect 5 detik");
                    break;
                }
                Message::Text(text) => {
                    let Ok(v) = serde_json::from_str::<serde_json::Value>(text.as_str()) else {
                        continue;
                    };
                    if v.get("method").and_then(|m| m.as_str()) != Some("logsNotification") {
                        continue;
                    }
                    let Some(value) = v
                        .get("params")
                        .and_then(|p| p.get("result"))
                        .and_then(|r| r.get("value"))
                    else {
                        continue;
                    };
                    let Some(sig) = value.get("signature").and_then(|s| s.as_str()) else {
                        continue;
                    };
                    let slot = value.get("slot").and_then(|s| s.as_u64()).unwrap_or(0);
                    // DIAGNOSTIK SEMENTARA: setiap notifikasi yang masuk dicatat.
                    info!(
                        "[SecondDip] Notifikasi logs sig {} slot {}",
                        &sig[..16.min(sig.len())],
                        slot
                    );

                    // Fetch + parse di luar lock state.
                    // Notifikasi bisa dari wallet target mana saja — coba tiap
                    // target sampai ada yang cocok (parse_trade miliknya).
                    let Some(tx) = fetch_transaction(&rpc_url, sig).await else {
                        error!(
                            "[SecondDip] getTransaction gagal total untuk sig {} — lewati notifikasi",
                            &sig[..16.min(sig.len())]
                        );
                        continue;
                    };
                    let mut found: Option<(String, ParsedTrade)> = None;
                    for t in &targets {
                        if let Some(trade) = parse_trade(&tx, t) {
                            found = Some((t.clone(), trade));
                            break;
                        }
                    }
                    let Some((trader, trade)) = found else {
                        // DIAGNOSTIK SEMENTARA: notifikasi masuk tapi bukan trade target.
                        info!(
                            "[SecondDip] sig {} bukan trade target — abaikan",
                            &sig[..16.min(sig.len())]
                        );
                        continue;
                    };
                    let trader_label = wallet_label(&trader);
                    // Log diagnostik: trade terdeteksi (BUY/SELL) — tetap tercatat
                    // di log server, tapi SELL tidak dinotifikasi ke Telegram.
                    info!(
                        "[SecondDip] Trade terdeteksi: {} {} {} token {:.0} @ ${} ({} {:.2} {})",
                        trader_label,
                        trade.side,
                        short_addr(&trade.mint),
                        trade.tokens,
                        fmt_price(trade.entry),
                        trade.paid_asset,
                        trade.paid,
                        trade.paid_asset
                    );
                    // SELL disembunyikan dari UI (permintaan user 2026-11-19):
                    // hanya BUY yang muncul di bot. Target aktif TIDAK dibatalkan.
                    if trade.side != "BUY" {
                        continue;
                    }
                    // ── Notifikasi pemantauan: hanya BUY target muncul di bot. ──
                    {
                        let chats = active_chats(&state).await;
                        let text = format!(
                            "🔔 PEMBELIAN TERDETEKSI — {} Beli token!\n\n🏦 Token: `{}`\n📦 Jumlah: {:.0} token\n💰 Bayar: {:.2} {}\n💵 Harga: ${} / token\n🧱 Slot: {}\n🔗 Sig: `{}...`",
                            trader_label,
                            short_addr(&trade.mint),
                            trade.tokens,
                            trade.paid,
                            trade.paid_asset,
                            fmt_price(trade.entry),
                            slot,
                            &sig[..16.min(sig.len())]
                        );
                        notify_all(&bot, &chats, &text).await;
                    }
                    // Cari akun pool DAMM v2 dari transaksi (untuk harga sqrtPrice
                    // via LaserStream WSS). Kalau tidak ketemu / bukan DAMM v2,
                    // pool tetap None dan price watcher fallback ke vault-ratio.
                    let mut trade = trade;
                    if let (Some(bv), Some(qv)) =
                        (trade.base_vault.clone(), trade.quote_vault.clone())
                    {
                        trade.pool =
                            find_pool_account(&rpc_url, &tx, &bv, &qv).await;
                        if let Some(p) = &trade.pool {
                            info!(
                                "[SecondDip] Pool DAMM v2 ketemu: {} (token {})",
                                p, short_addr(&trade.mint)
                            );
                        }
                    }
                    handle_buy(bot.clone(), state.clone(), trade, slot, sig.to_string(), &trader).await;
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// BUY target terdeteksi: simpan target aktif + notifikasi + spawn price watcher.
async fn handle_buy(
    bot: Bot,
    state: Arc<Mutex<BotState>>,
    trade: ParsedTrade,
    slot: u64,
    sig: String,
    wallet: &str,
) {
    let (t1, t1_mode, t1_usd, t2, t2_mode, t2_usd, t2_mcap, tip, prio, amount, has_target) = {
        let st = state.lock().await;
        (
            st.second_dip_trigger1_pct,
            st.second_dip_trigger1_mode.clone(),
            st.second_dip_trigger1_usd,
            st.second_dip_trigger2_pct,
            st.second_dip_trigger2_mode.clone(),
            st.second_dip_trigger2_usd,
            st.second_dip_trigger2_mcap,
            st.second_dip_tip,
            st.second_dip_prio,
            st.second_dip_amount_usd,
            st.second_dip_target.is_some(),
        )
    };
    if has_target {
        info!("[SecondDip] BUY baru terdeteksi tapi masih ada target aktif — abaikan (satu target = satu order)");
        return;
    }

    // Entry harus dalam USD (harga vault dihitung dalam USD).
    // Kalau target bayar pakai wSOL, konversi dengan harga SOL saat ini.
    let entry_usd = if trade.paid_asset == "USDC" {
        trade.entry
    } else {
        match crate::fetch_sol_price_usd().await {
            Some(p) if p > 0.0 => trade.entry * p,
            _ => {
                error!("[SecondDip] Gagal ambil harga SOL untuk konversi entry wSOL — BUY diabaikan");
                return;
            }
        }
    };
    if entry_usd <= 0.0 || !entry_usd.is_finite() {
        return;
    }

    let trigger1_price = calc_trigger1_price(entry_usd, &t1_mode, t1, t1_usd);
    // Pemicu 2 hormati mode (USD / PCT / MCAP) — sama seperti watcher.
    // Sebelumnya selalu entry*(1+t2%) walau mode USD, jadi log + notifikasi
    // menampilkan (+20%) padahal order nantinya pakai harga USD.
    let trigger2_price = if t2_mode == "USD" {
        t2_usd
    } else {
        entry_usd * (1.0 + t2 / 100.0)
    };

    // Vault pool untuk harga real-time via RPC (Helius). Kalau tidak ketemu
    // dari transaksi, target TIDAK dibuat — tanpa vault, harga tidak bisa dipantau.
    let (Some(base_vault), Some(quote_vault)) = (trade.base_vault.clone(), trade.quote_vault.clone()) else {
        let chats = active_chats(&state).await;
        notify_all(
            &bot,
            &chats,
            &format!(
                "⚠️ Second Dip: vault pool token `{}` tidak ketemu di transaksi — target dibatalkan (harga tidak bisa dipantau via RPC).",
                short_addr(&trade.mint)
            ),
        )
        .await;
        return;
    };
    let target = SecondDipTarget {
        mint: trade.mint.clone(),
        wallet: wallet.to_string(),
        entry_price: entry_usd,
        paid: trade.paid,
        paid_asset: trade.paid_asset.clone(),
        slot,
        sig: sig.clone(),
        base_vault: Some(base_vault),
        quote_vault: Some(quote_vault),
        pool: trade.pool.clone(),
        base_decimals: trade.base_decimals,
        quote_decimals: trade.quote_decimals,
        quote_mint: trade.quote_mint.clone(),
        created_at: Instant::now(),
    };
    {
        let mut st = state.lock().await;
        st.second_dip_target = Some(target.clone());
    }

    let pemicu1_log = if t1_mode == "USD" {
        format!("${} (USD)", fmt_price(trigger1_price))
    } else {
        format!("${} (+{}%)", fmt_price(trigger1_price), t1)
    };
    let pemicu2_log = if t2_mode == "USD" {
        format!("${} (USD)", fmt_price(trigger2_price))
    } else if t2_mode == "MCAP" {
        format!("${} (MCAP {})", fmt_price(trigger2_price), fmt_price(t2_mcap))
    } else {
        format!("${} (+{}%)", fmt_price(trigger2_price), t2)
    };
    let chats = active_chats(&state).await;
    // Nama token (ticker) dari Dexscreener; kalau tidak ada, pakai address singkat.
    let token_name = fetch_token_name(&trade.mint).await;
    let text = format!(
        "🎯 *Second Dip: BUY Target Terdeteksi!* ({})\n\n🏦 Token: *{}* (`{}`)\n💰 Entry: `${}` (bayar {:.2} {} → {:.0} token)\n📈 Pemicu 1 (buat order): `{}`\n🎯 Pemicu 2 (harga beli): `{}`\n💵 Jumlah: ${:.2} | ⚡ Tip: {} SOL | ⛽ Prio: {} SOL\n\n_Memantau harga via RPC Helius (real-time, 1 detik)..._",
        wallet_label(wallet),
        token_name,
        short_addr(&trade.mint),
        fmt_price(entry_usd),
        trade.paid,
        trade.paid_asset,
        trade.tokens,
        pemicu1_log,
        pemicu2_log,
        amount,
        tip,
        prio
    );
    notify_all(&bot, &chats, &text).await;
    info!(
        "[SecondDip] Target aktif: wallet={} mint={} entry={} vault={:?} slot={} sig={}",
        wallet_label(wallet), trade.mint, entry_usd, target.base_vault, slot, sig
    );
    log_flow(&format!(
        "TARGET BARU | wallet={} | token={} | entry=${} | bayar {:.2} {} -> {:.0} token | slot={} | sig={} | pemicu1={} | pemicu2={} | jumlah=${:.2} | tip={} SOL | prio={} SOL",
        wallet_label(wallet),
        trade.mint,
        fmt_price(entry_usd),
        trade.paid,
        trade.paid_asset,
        trade.tokens,
        slot,
        sig,
        pemicu1_log,
        pemicu2_log,
        amount,
        tip,
        prio
    ));

    tokio::spawn(run_price_watcher(bot, state, target));
}

/// Pantau harga via LaserStream WSS `accountSubscribe` ke akun pool DAMM v2.
/// Setiap ada swap, Helius push data pool terbaru → harga sqrtPrice dihitung
/// langsung (akurat, sub-detik). Return true kalau selesai (order dibuat /
/// target batal), false kalau WSS gagal (caller fallback ke polling RPC).
async fn run_price_watcher_ws(
    bot: Bot,
    state: Arc<Mutex<BotState>>,
    target: &SecondDipTarget,
    pool: &str,
    wss_url: &str,
    quote_mint: &str,
) -> bool {
    info!(
        "[SecondDip] Harga via LaserStream WSS accountSubscribe pool {}",
        short_addr(pool)
    );
    let mut ws = match connect_wss(wss_url).await {
        Ok(w) => w,
        Err(e) => {
            error!("[SecondDip] Konek WSS harga gagal: {} — fallback polling", e);
            return false;
        }
    };
    let sub = serde_json::json!({
        "jsonrpc": "2.0", "id": 2, "method": "accountSubscribe",
        "params": [pool, {"encoding": "base64", "commitment": "processed"}],
    });
    if let Err(e) = ws.send(Message::text(sub.to_string())).await {
        error!("[SecondDip] accountSubscribe gagal: {} — fallback polling", e);
        return false;
    }
    info!("[SecondDip] accountSubscribe pool {} OK", short_addr(pool));

    // ── Cek harga AWAL sekali (accountSubscribe hanya push kalau ada swap,
    // jadi tanpa cek ini bot tidak tahu harga saat ini). Kalau harga sudah
    // di atas Pemicu 1 (misal mode USD dan entry sudah di atasnya), langsung
    // buat order tanpa menunggu swap berikutnya. Kalau harga sudah di bawah
    // entry, batalkan target. Kalau fetch gagal, lanjut ke loop notifikasi.
    {
        let (t1, t1_mode, t1_usd, t2, tip, prio, amount) = {
            let st = state.lock().await;
            (
                st.second_dip_trigger1_pct,
                st.second_dip_trigger1_mode.clone(),
                st.second_dip_trigger1_usd,
                st.second_dip_trigger2_pct,
                st.second_dip_tip,
                st.second_dip_prio,
                st.second_dip_amount_usd,
            )
        };
        let trigger1 = calc_trigger1_price(target.entry_price, &t1_mode, t1, t1_usd);
        let (t2_mode, trigger2, t2_mcap) = {
            let st = state.lock().await;
            let mode = st.second_dip_trigger2_mode.clone();
            let t2_usd = match mode.as_str() {
                "USD" => st.second_dip_trigger2_usd,
                _ => target.entry_price * (1.0 + t2 / 100.0),
            };
            (mode, t2_usd, st.second_dip_trigger2_mcap)
        };
        let rpc_url = price_rpc_url();
        if let Some(price) = fetch_pool_price(
            &rpc_url,
            pool,
            target.base_decimals,
            target.quote_decimals,
            quote_mint,
        )
        .await
        {
            info!(
                "[SecondDip] Harga awal pool {}: ${} (entry ${}, pemicu1 ${})",
                short_addr(pool),
                fmt_price(price),
                fmt_price(target.entry_price),
                fmt_price(trigger1)
            );
            match decide_price(price, trigger1) {
                PriceDecision::Trigger => {
                    info!(
                        "[SecondDip] Pemicu 1 sudah kena di harga awal! harga={} trigger1={} — buat limit order",
                        price, trigger1
                    );
                    log_flow(&format!(
                        "PEMICU 1 KENA | token={} | harga=${} | entry=${} | pemicu1=${} | naik {:.1}% dari entry | (cek harga awal)",
                        target.mint,
                        fmt_price(price),
                        fmt_price(target.entry_price),
                        fmt_price(trigger1),
                        (price / target.entry_price - 1.0) * 100.0
                    ));
                    place_order_and_finish(
                        bot.clone(),
                        state.clone(),
                        target,
                        trigger2,
                        t2_mode,
                        t2_mcap,
                        tip,
                        prio,
                        amount,
                    )
                    .await;
                    return true;
                }
                PriceDecision::Wait => {
                    // Harga awal belum kena Pemicu 1 — lanjut tunggu notifikasi.
                }
            }
        } else {
            info!("[SecondDip] Gagal ambil harga awal pool — lanjut tunggu notifikasi WSS");
        }
    }

    let mut ping_sent: Option<Instant> = None;
    loop {
        // Cek target masih aktif + timeout Pemicu 1 (tiap iterasi).
        let (active, mode, t1, t1_mode, t1_usd, t2, tip, prio, amount, still_active, timeout_mins) = {
            let st = state.lock().await;
            let same = matches!(&st.second_dip_target, Some(t) if t.mint == target.mint);
            (
                st.second_dip_active,
                st.dip_mode.clone(),
                st.second_dip_trigger1_pct,
                st.second_dip_trigger1_mode.clone(),
                st.second_dip_trigger1_usd,
                st.second_dip_trigger2_pct,
                st.second_dip_tip,
                st.second_dip_prio,
                st.second_dip_amount_usd,
                same,
                st.second_dip_trigger1_timeout_mins,
            )
        };
        if !still_active {
            info!("[SecondDip] Target {} sudah tidak aktif — WSS harga berhenti", target.mint);
            return true;
        }
        if !active || mode != "SECOND_DIP" {
            info!("[SecondDip] Second Dip dimatikan — WSS harga berhenti");
            return true;
        }
        if timeout_mins > 0
            && target.created_at.elapsed() > Duration::from_secs(timeout_mins * 60)
        {
            clear_target(&state, &target.mint).await;
            log_flow(&format!(
                "TARGET BATAL | token={} | alasan=timeout {} tanpa mencapai Pemicu 1 | entry=${}",
                target.mint,
                crate::fmt_timeout_short(timeout_mins),
                fmt_price(target.entry_price)
            ));
            let chats = active_chats(&state).await;
            notify_all(
                &bot,
                &chats,
                &format!(
                    "⏰ Second Dip: timeout {} tanpa mencapai Pemicu 1 — target `{}` dibatalkan.",
                    crate::fmt_timeout_short(timeout_mins),
                    short_addr(&target.mint)
                ),
            )
            .await;
            return true;
        }
        let trigger1 = calc_trigger1_price(target.entry_price, &t1_mode, t1, t1_usd);
        let (t2_mode, trigger2, t2_mcap) = {
            let st = state.lock().await;
            let mode = st.second_dip_trigger2_mode.clone();
            let t2_usd = match mode.as_str() {
                "USD" => st.second_dip_trigger2_usd,
                _ => target.entry_price * (1.0 + t2 / 100.0),
            };
            (mode, t2_usd, st.second_dip_trigger2_mcap)
        };

        // Tunggu notifikasi akun (timeout 1 detik = tick untuk cek berkala).
        let msg = match tokio::time::timeout(Duration::from_secs(1), ws.next()).await {
            Ok(m) => m,
            Err(_) => {
                // Idle 1 detik — kirim Ping (jaga koneksi) lalu lanjut.
                let _ = ws.send(Message::Ping(Bytes::from_static(b"ping"))).await;
                ping_sent = Some(Instant::now());
                continue;
            }
        };
        let Some(msg) = msg else {
            error!("[SecondDip] WSS harga stream habis — fallback polling");
            return false;
        };
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                error!("[SecondDip] WSS harga error: {} — fallback polling", e);
                return false;
            }
        };
        match msg {
            Message::Ping(data) => {
                let _ = ws.send(Message::Pong(data)).await;
                continue;
            }
            Message::Pong(_) => {
                if let Some(sent) = ping_sent.take() {
                    debug!("[SecondDip] WSS harga ping: {}ms", sent.elapsed().as_millis());
                }
                continue;
            }
            Message::Binary(_) | Message::Frame(_) => continue,
            Message::Close(_) => {
                error!("[SecondDip] WSS harga ditutup server — fallback polling");
                return false;
            }
            Message::Text(text) => {
                let Ok(v) = serde_json::from_str::<serde_json::Value>(text.as_str()) else {
                    continue;
                };
                if v.get("method").and_then(|m| m.as_str()) != Some("accountNotification") {
                    continue;
                }
                let Some(data_b64) = v
                    .get("params")
                    .and_then(|p| p.get("result"))
                    .and_then(|r| r.get("value"))
                    .and_then(|val| val.get("data"))
                    .and_then(|d| d.as_array())
                    .and_then(|a| a.get(0))
                    .and_then(|s| s.as_str())
                else {
                    continue;
                };
                let Ok(raw) = base64::engine::general_purpose::STANDARD.decode(data_b64) else {
                    continue;
                };
                // Harga sqrtPrice — butuh harga SOL kalau quote = wSOL.
                let sol_usd = if quote_mint == USDC_MINT {
                    None
                } else {
                    crate::fetch_sol_price_usd().await
                };
                let Some(price) = price_from_sqrt_price_data(
                    &raw,
                    target.base_decimals,
                    target.quote_decimals,
                    quote_mint,
                    sol_usd,
                ) else {
                    continue;
                };
                // Pemicu 1: kalau sudah kena, langsung buat order walau harga
                // di bawah entry (perintah user 2026-11-23). Tidak ada batal
                // karena harga — satu-satunya pembatalan adalah timeout.
                if price >= trigger1 {
                    info!(
                        "[SecondDip] Pemicu 1 kena (WSS)! harga={} trigger1={} — buat limit order",
                        price, trigger1
                    );
                    log_flow(&format!(
                        "PEMICU 1 KENA | token={} | harga=${} | entry=${} | pemicu1=${} | naik {:.1}% dari entry",
                        target.mint,
                        fmt_price(price),
                        fmt_price(target.entry_price),
                        fmt_price(trigger1),
                        (price / target.entry_price - 1.0) * 100.0
                    ));
                    place_order_and_finish(
                        bot.clone(),
                        state.clone(),
                        target,
                        trigger2,
                        t2_mode,
                        t2_mcap,
                        tip,
                        prio,
                        amount,
                    )
                    .await;
                    return true;
                }
                // Belum kena — tunggu notifikasi berikutnya.
            }
        }
    }
}

// ─── Price watcher ───────────────────────────────────────────────────────────

/// Pantau harga target tiap 1 detik (via RPC Helius, baca saldo vault pool)
/// sampai Pemicu 1 kena (buat order — walau harga di bawah entry),
/// atau timeout tanpa kena Pemicu 1 (batal). TIDAK ADA batal karena harga
/// turun di bawah entry (perintah user 2026-11-23).
async fn run_price_watcher(bot: Bot, state: Arc<Mutex<BotState>>, target: SecondDipTarget) {
    let (Some(base_vault), Some(quote_vault)) =
        (target.base_vault.clone(), target.quote_vault.clone())
    else {
        clear_target(&state, &target.mint).await;
        log_flow(&format!(
            "TARGET BATAL | token={} | alasan=vault pool tidak ada",
            target.mint
        ));
        let chats = active_chats(&state).await;
        notify_all(
            &bot,
            &chats,
            &format!(
                "⚠️ Second Dip: vault pool token `{}` tidak ada — target dibatalkan.",
                short_addr(&target.mint)
            ),
        )
        .await;
        return;
    };
    let rpc_url = price_rpc_url();
    // Quote mint pool yang sebenarnya (dari ParsedTrade) — JANGAN ditebak
    // dari paid_asset: bayar pakai SOL tidak berarti pool-nya wSOL-quoted
    // (FOTF: bayar SOL, pool USDC — ditebak wSOL bikin harga dikali harga
    // SOL, 74x terlalu tinggi).
    let quote_mint = target.quote_mint.as_str();

    // ── Mode 5 (LaserStream WSS accountSubscribe): harga push real-time ──
    // Kalau pool DAMM v2 ketemu + HELIUS_API_KEY ada, pantau harga lewat
    // accountSubscribe (update tiap ada swap, sub-detik). Kalau WSS gagal,
    // jatuh ke polling RPC 1 detik di bawah (tetap akurat via sqrtPrice).
    if let (Some(pool), Some(wss_url)) = (target.pool.clone(), helius_wss_url()) {
        if run_price_watcher_ws(
            bot.clone(),
            state.clone(),
            &target,
            &pool,
            &wss_url,
            quote_mint,
        )
        .await
        {
            return; // selesai via WSS (order dibuat / target batal)
        }
        info!(
            "[SecondDip] WSS harga pool {} gagal — fallback ke polling RPC 1 detik",
            short_addr(&pool)
        );
    }

    loop {
        // Cek target masih aktif & masih target yang sama.
        let (active, mode, t1, t1_mode, t1_usd, t2, tip, prio, amount, still_active) = {
            let st = state.lock().await;
            let same = matches!(&st.second_dip_target, Some(t) if t.mint == target.mint);
            (
                st.second_dip_active,
                st.dip_mode.clone(),
                st.second_dip_trigger1_pct,
                st.second_dip_trigger1_mode.clone(),
                st.second_dip_trigger1_usd,
                st.second_dip_trigger2_pct,
                st.second_dip_tip,
                st.second_dip_prio,
                st.second_dip_amount_usd,
                same,
            )
        };
        if !still_active {
            info!(
                "[SecondDip] Target {} sudah tidak aktif — price watcher berhenti",
                target.mint
            );
            return;
        }
        if !active || mode != "SECOND_DIP" {
            info!("[SecondDip] Second Dip dimatikan — price watcher berhenti");
            return;
        }
        // Batas waktu Pemicu 1 dari setting (0 = tidak ada batas).
        let timeout_mins = {
            let st = state.lock().await;
            st.second_dip_trigger1_timeout_mins
        };
        if timeout_mins > 0
            && target.created_at.elapsed() > Duration::from_secs(timeout_mins * 60)
        {
            clear_target(&state, &target.mint).await;
            log_flow(&format!(
                "TARGET BATAL | token={} | alasan=timeout {} tanpa mencapai Pemicu 1 | entry=${}",
                target.mint,
                crate::fmt_timeout_short(timeout_mins),
                fmt_price(target.entry_price)
            ));
            let chats = active_chats(&state).await;
            notify_all(
                &bot,
                &chats,
                &format!(
                    "⏰ Second Dip: timeout {} tanpa mencapai Pemicu 1 — target `{}` dibatalkan.",
                    crate::fmt_timeout_short(timeout_mins),
                    short_addr(&target.mint)
                ),
            )
            .await;
            return;
        }

        let trigger1 = calc_trigger1_price(target.entry_price, &t1_mode, t1, t1_usd);
        // Harga beli Pemicu 2 sesuai mode (PCT / USD / MCAP).
        // PCT: entry + t2%. USD: harga absolut. MCAP: order pakai Target::Mcap.
        let (t2_mode, trigger2, t2_mcap) = {
            let st = state.lock().await;
            let mode = st.second_dip_trigger2_mode.clone();
            let t2_usd = match mode.as_str() {
                "USD" => st.second_dip_trigger2_usd,
                _ => target.entry_price * (1.0 + t2 / 100.0),
            };
            (mode, t2_usd, st.second_dip_trigger2_mcap)
        };

        match fetch_price_for_target(
            &rpc_url,
            &target,
            &base_vault,
            &quote_vault,
            quote_mint,
        )
        .await
        {
            Some(price) => {
                // Pemicu 1: kalau sudah kena, langsung buat order walau harga
                // di bawah entry (perintah user 2026-11-23). Tidak ada batal
                // karena harga — satu-satunya pembatalan adalah timeout.
                if price >= trigger1 {
                    info!(
                        "[SecondDip] Pemicu 1 kena! harga={} trigger1={} — buat limit order",
                        price, trigger1
                    );
                    log_flow(&format!(
                        "PEMICU 1 KENA | token={} | harga=${} | entry=${} | pemicu1=${} | naik {:.1}% dari entry",
                        target.mint,
                        fmt_price(price),
                        fmt_price(target.entry_price),
                        fmt_price(trigger1),
                        (price / target.entry_price - 1.0) * 100.0
                    ));
                    place_order_and_finish(
                        bot.clone(),
                        state.clone(),
                        &target,
                        trigger2,
                        t2_mode,
                        t2_mcap,
                        tip,
                        prio,
                        amount,
                    )
                    .await;
                    return;
                }
                // Belum kena — lanjut poll.
            }
            None => {
                // Gagal fetch harga sesaat (jaringan / RPC) — jangan
                // batalkan, coba lagi 1 detik.
                error!(
                    "[SecondDip] Gagal fetch harga vault via RPC untuk {} — coba lagi",
                    short_addr(&target.mint)
                );
            }
        }
        tokio::time::sleep(Duration::from_secs(PRICE_POLL_SECS)).await;
    }
}

// ─── Penempatan limit order Buy Dip ke Aura ──────────────────────────────────

/// Buat order + bersihkan target aktif + notifikasi.
/// Retry maks 3x; kalau tetap gagal, catat ke error_logs + notifikasi.
async fn place_order_and_finish(
    bot: Bot,
    state: Arc<Mutex<BotState>>,
    target: &SecondDipTarget,
    trigger2_usd: f64,
    trigger2_mode: String,
    trigger2_mcap: f64,
    tip: f64,
    prio: f64,
    amount_usd: f64,
) {
    let (client_opt, db_conn, chats, dry_run) = {
        let st = state.lock().await;
        (
            st.aura_client.clone(),
            st.db_conn.clone(),
            st.active_chats.clone(),
            st.second_dip_dry_run,
        )
    };

    // ── Mode Simulasi (dry run): JANGAN sentuh gRPC sama sekali. ──
    // Order hanya dicatat di DB lokal (ditandai "SIMULASI") + notifikasi.
    // Jalur ini tetap jalan penuh walau aura_client = None (tanpa API key).
    if dry_run {
        let target_str = format!("SIMULASI {}$", fmt_price(trigger2_usd));
        if let Ok(conn) = db_conn.try_lock() {
            let _ = db::insert_limit_order(
                &conn,
                "BUY",
                &target.mint,
                &target_str,
                tip,
                prio,
                amount_usd,
            );
        }
        clear_target(&state, &target.mint).await;
        log_flow(&format!(
            "ORDER SIMULASI | token={} | harga_beli=${} | jumlah=${:.2} | tip={} SOL | prio={} SOL | (tidak dikirim ke Aura)",
            target.mint,
            fmt_price(trigger2_usd),
            amount_usd,
            tip,
            prio
        ));
        // Nama token (ticker) dari Dexscreener; kalau tidak ada, pakai address singkat.
        let token_name = fetch_token_name(&target.mint).await;
        let text = format!(
            "🧪 *SIMULASI Second Dip — Order TIDAK Dikirim ke Aura!*\n\n🏦 Token: *{}* (`{}`)\n🎯 Harga beli: `${}`\n💰 Jumlah: ${:.2}\n⚡ Tip: {} SOL | ⛽ Prio: {} SOL\n\n_Ini simulasi. Kalau Terhubung ke Aura dihidupkan (ON), order yang sama akan dikirim ke Aura._",
            token_name,
            short_addr(&target.mint),
            fmt_price(trigger2_usd),
            amount_usd,
            tip,
            prio
        );
        notify_all(&bot, &chats, &text).await;
        return;
    }

    let Some(client) = client_opt else {
        clear_target(&state, &target.mint).await;
        log_flow(&format!(
            "ORDER GAGAL | token={} | alasan=koneksi Aura gRPC tidak tersedia",
            target.mint
        ));
        notify_all(
            &bot,
            &chats,
            "❌ Second Dip: koneksi Aura gRPC tidak tersedia — order gagal dibuat.",
        )
        .await;
        return;
    };

    // Simpan dulu ke DB lokal (seperti alur limit order manual).
    let target_str = format!("{}$", fmt_price(trigger2_usd));
    let mut db_id = 0i64;
    if let Ok(conn) = db_conn.try_lock() {
        if let Ok(id) = db::insert_limit_order(
            &conn,
            "BUY",
            &target.mint,
            &target_str,
            tip,
            prio,
            amount_usd,
        ) {
            db_id = id;
        }
    }

    let mut last_err = String::new();
    let mut aura_id: Option<i64> = None;
    let expire_mins = {
        let st = state.lock().await;
        st.second_dip_trigger2_timeout_mins
    };
    for attempt in 1..=MAX_ORDER_RETRIES {
        match place_buy_dip_order(
            &client,
            &target.mint,
            trigger2_usd,
            trigger2_mode.as_str(),
            trigger2_mcap,
            amount_usd,
            tip,
            prio,
            expire_mins,
        )
        .await
        {
            Ok(id) => {
                aura_id = Some(id);
                break;
            }
            Err(e) => {
                last_err = e;
                error!(
                    "[SecondDip] place order gagal (coba {}/{}): {}",
                    attempt, MAX_ORDER_RETRIES, last_err
                );
                if attempt < MAX_ORDER_RETRIES {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }

    clear_target(&state, &target.mint).await;

    match aura_id {
        Some(id) => {
            if let Ok(conn) = db_conn.try_lock() {
                let _ = db::update_aura_order_id(&conn, db_id, id);
            }
            // Auto Sell: catat order beli ini untuk dilacak (kalau toggle ON).
            maybe_track_auto_sell(
                &state,
                id,
                &target.mint,
                trigger2_usd,
                amount_usd,
                tip,
                prio,
                "second_dip",
            )
            .await;
            log_flow(&format!(
                "ORDER REAL TERKIRIM | token={} | harga_beli=${} | jumlah=${:.2} | tip={} SOL | prio={} SOL | aura_order_id={}",
                target.mint,
                fmt_price(trigger2_usd),
                amount_usd,
                tip,
                prio,
                id
            ));
            let text = format!(
                "✅ *Second Dip: Limit Order Dibuat!*\n\n🏦 Token: `{}`\n🎯 Harga beli: `${}`\n💰 Jumlah: ${:.2}\n⚡ Tip: {} SOL | ⛽ Prio: {} SOL\n📋 Aura Order ID: {}\n\n_Order akan fill kalau harga dump ke harga beli._",
                short_addr(&target.mint),
                fmt_price(trigger2_usd),
                amount_usd,
                tip,
                prio,
                id
            );
            notify_all(&bot, &chats, &text).await;
        }
        None => {
            if let Ok(conn) = db_conn.try_lock() {
                let _ = db::insert_error_log(
                    &conn,
                    db_id,
                    &target.mint,
                    &format!("Second Dip place order gagal 3x: {}", last_err),
                );
            }
            log_flow(&format!(
                "ORDER GAGAL | token={} | alasan=gagal 3x percobaan | error={}",
                target.mint,
                last_err
            ));
            notify_all(
                &bot,
                &chats,
                &format!(
                    "❌ Second Dip: gagal membuat limit order setelah 3x percobaan.\n🏦 Token: `{}`\n🔴 Error: {}",
                    short_addr(&target.mint),
                    last_err
                ),
            )
            .await;
        }
    }
}

/// Wallet utama user (W2, Bz9Fifd) — dipakai untuk order Second Dip + TP-nya.
/// API key Aura memberi akses ke 2 wallet; wallets[0] adalah W1 (AURA3K)
/// yang TIDAK dipakai user (saldo 0 SOL / 0 USDC). Order Second Dip NATF
/// 327223 gagal terisi karena terkirim ke W1 sebagai beli SOL 0.677 —
/// wallet tidak punya dana. Kalau W2 tidak ada di list (misal ganti key),
/// fallback ke wallets[0] supaya order tetap terkirim.
fn pick_main_wallet(wallets: &[solana_address::Address]) -> solana_address::Address {
    use std::str::FromStr;
    const MAIN_WALLET: &str = "Bz9FifdG8qeszPxoUSqi44chKejd6fUxFTkKLVM3TrRm";
    if let Ok(addr) = solana_address::Address::from_str(MAIN_WALLET) {
        if wallets.contains(&addr) {
            return addr;
        }
    }
    wallets.first().cloned().unwrap_or_default()
}

/// Jumlah beli USD -> USDC (6 desimal), sama seperti buy_swap_amount di main.rs.
/// $50 -> 50_000_000.
fn amount_usd_to_usdc(amount_usd: f64) -> u64 {
    (amount_usd * 1e6) as u64
}

/// Satu percobaan place limit order BUY ke Aura gRPC (mengikuti pola
/// `place_limit_buy` manual): Target::Price (USD -> SOL), Direction::Below,
/// slippage 100%, tip/prio dari setting Second Dip, amount USD -> USDC.
async fn place_buy_dip_order(
    client: &aura_api_client::client::AuraClients<InterceptorFn, NoCtx>,
    mint: &str,
    trigger2_usd: f64,
    // Mode harga Pemicu 2: "PCT"/"USD" -> Target::Price, "MCAP" -> Target::Mcap.
    trigger2_mode: &str,
    trigger2_mcap: f64,
    amount_usd: f64,
    tip: f64,
    prio: f64,
    // Batas waktu order (menit). 0 = pakai default Aura.
    expire_mins: u64,
) -> Result<i64, String> {
    use aura_api_client::types::{
        ApiLimitOrder, ApiOrders, Direction, OrderEventTrigger, OrderState, RawOrder,
        SwapAmount, Target, TxnProcessors, UpdateTokenLimitOrders, UserNonceStrategy,
    };
    use std::str::FromStr;

    let mint_addr = solana_address::Address::from_str(mint)
        .map_err(|_| format!("mint tidak valid: {}", mint))?;

    let sol_price = crate::fetch_sol_price_usd()
        .await
        .ok_or_else(|| "gagal ambil harga SOL".to_string())?;
    if sol_price <= 0.0 {
        return Err("harga SOL tidak valid".to_string());
    }

    // Target order sesuai mode Pemicu 2:
    // - PCT/USD: Target::Price, harga USD dibagi harga SOL dulu
    //   (perintah user 2026-11-21, supaya tampilan di Aura sama).
    // - MCAP: Target::Mcap, mcap USD skala 1e6 (seperti parse_target di main.rs).
    // Fixed (tidak berubah): Slippage 100%, OrderEventTrigger::Immediate (Instant),
    // SwapAmount::Buy USDC exact (fee mode Exact), Nonce Hybrid.
    let target = if trigger2_mode == "MCAP" {
        let scaled = (trigger2_mcap * 1_000_000.0) as u64;
        let mcap_ud128 = fastnum::UD128::from(scaled) / fastnum::UD128::from(1_000_000u64);
        Target::Mcap {
            mcap: mcap_ud128,
            price: None,
            direction: Direction::Below,
        }
    } else {
        let scale: u64 = 1_000_000_000_000;
        let price_sol = trigger2_usd / sol_price;
        let scaled = (price_sol * scale as f64) as u64;
        let price_ud128 = fastnum::UD128::from(scaled) / fastnum::UD128::from(scale);
        Target::Price {
            price: price_ud128,
            direction: Direction::Below,
        }
    };

    // Jumlah beli dalam USDC (6 desimal) — sama seperti limit order biasa
    // (buy_swap_amount di main.rs). Sebelumnya dikonversi ke SOL dan order
    // tidak pernah bisa terisi karena wallet tidak punya SOL cukup
    // (NATF 327223: beli 0.677 SOL di W1 yang saldonya 0).
    let usdc_amount = amount_usd_to_usdc(amount_usd);

    let tip_lam = (tip * 1e9) as u64;
    let prio_lam = (prio * 1e9) as u64;

    // Slippage 100% (standar Buy Dip user).
    let slippage_val = fastnum::UD128::from(1_000_000u64) / fastnum::UD128::from(1_000_000u64);

    let wallet = {
        // Cache alamat wallet utama (W2) — tidak berubah selama bot jalan.
        // Tanpa cache, tiap pasang order fetch wallet dulu (1 panggilan gRPC)
        // dan rawan kena rate limit per-detik Aura. (Optimasi 2026-11-22.)
        static CACHED_WALLET: std::sync::OnceLock<solana_address::Address> =
            std::sync::OnceLock::new();
        if let Some(w) = CACHED_WALLET.get() {
            *w
        } else {
            let mut tc = client.aura();
            match tc
                .fetch_full_wallet_info(
                    (),
                    tonic::Request::new(aura_api_client::types::FetchFullWalletsInfoReq {}),
                )
                .await
            {
                Ok(res) => {
                    let w = pick_main_wallet(&res.into_inner().wallets);
                    let _ = CACHED_WALLET.set(w);
                    w
                }
                Err(e) => return Err(format!("gagal fetch wallet: {}", e.message())),
            }
        }
    };

    let api_order = ApiLimitOrder {
        state: OrderState::Api {
            id: None,
            expire_dur: if expire_mins > 0 {
                Some(chrono::TimeDelta::minutes(expire_mins as i64))
            } else {
                None
            },
            activate_dur: None,
        },
        order: RawOrder {
            slippage: slippage_val,
            tip: decisol::Lamports::from(tip_lam),
            fee: decisol::Lamports::from(prio_lam),
            target,
            amount: SwapAmount::Buy(decisol::QuoteLamports::Usdc(
                decisol::Usdc::new(usdc_amount),
            )),
            procs: TxnProcessors {
                jito_validators: false,
                jito_bundled: false,
                aura: true,
                bloxroute: false,
                nozomi: false,
                next_block: false,
                slot0: false,
                astra: false,
                block_razor: false,
                node1: false,
                tpu_penetrator: false,
                helius: true,
                stellium: true,
                soyas: true,
                falcon: true,
                raiden: true,
                circular: true,
                flash_block: true,
                moon: true,
                blocksprint: true,
                aura_revert: false,
                landx: true,
                manka: true,
                blockrush: true,
            },
            nonce: UserNonceStrategy::Hybrid,
            slot_latency: 0,
        },
        trigger: OrderEventTrigger::Immediate,
        wallet,
    };

    let req = tonic::Request::new(UpdateTokenLimitOrders {
        mint: mint_addr,
        orders: ApiOrders {
            orders: vec![api_order],
        },
    });
    // Rate limiter Aura: jeda min 350ms sejak panggilan Aura terakhir,
    // supaya tidak tembus 4 RPS (order 322888 gagal 2026-11-22).
    crate::aura_slot_wait().await;
    let resp = client
        .limit_orders()
        .place_limit_orders((), req)
        .await
        .map_err(|e| e.message().to_string())?;
    let id = resp
        .into_inner()
        .ids
        .get(0)
        .copied()
        .map(|x| x.0)
        .ok_or_else(|| "Aura tidak mengembalikan order id".to_string())?;
    Ok(id)
}

// ─── Auto Sell (Take Profit otomatis setelah limit order beli terisi) ────────

/// Hitung harga Take Profit dalam USD dari harga beli.
/// - USD: harga absolut tp_usd.
/// - PCT / MCAP: buy_price * (1 + pct/100). Untuk MCAP, harga ini hanya
///   dipakai untuk log — order-nya memakai Target::Mcap.
pub fn calc_tp_price(buy_price: f64, mode: &str, pct: f64, usd: f64) -> f64 {
    if mode == "USD" && usd > 0.0 {
        usd
    } else {
        buy_price * (1.0 + pct / 100.0)
    }
}

/// Pasang limit order SELL (Take Profit) ke Aura gRPC — cerminan
/// `place_buy_dip_order`, tapi arah jual: Target::Price Above TP (atau
/// Target::Mcap untuk mode MCAP), SwapAmount::SellPerc 100% (jual seluruh
/// posisi), slippage 100%, Nonce Hybrid. Return Aura order id.
/// Konversi % jual (1–100) ke fraksi UD128 untuk SwapAmount::SellPerc.
/// Repo aura_api_client menampilkan SellPerc sebagai amount × 100 %,
/// jadi 100% = 1, 50% = 0.5, 25% = 0.25.
pub fn sell_pct_to_frac(sell_pct: f64) -> fastnum::UD128 {
    let pct = sell_pct.clamp(0.0, 100.0);
    fastnum::UD128::from((pct * 1_000_000.0) as u64) / fastnum::UD128::from(100_000_000u64)
}

async fn place_tp_sell_order(
    client: &aura_api_client::client::AuraClients<InterceptorFn, NoCtx>,
    mint: &str,
    tp_price_usd: f64,
    tp_mode: &str,
    tp_mcap: f64,
    tip: f64,
    prio: f64,
    sell_pct: f64,
) -> Result<i64, String> {
    use aura_api_client::types::{
        ApiLimitOrder, ApiOrders, Direction, OrderEventTrigger, OrderState, RawOrder,
        SwapAmount, Target, TxnProcessors, UpdateTokenLimitOrders, UserNonceStrategy,
    };
    use std::str::FromStr;

    let mint_addr = solana_address::Address::from_str(mint)
        .map_err(|_| format!("mint tidak valid: {}", mint))?;

    let sol_price = crate::fetch_sol_price_usd()
        .await
        .ok_or_else(|| "gagal ambil harga SOL".to_string())?;
    if sol_price <= 0.0 {
        return Err("harga SOL tidak valid".to_string());
    }

    // Target jual: harga NAIK ke TP baru jual (Direction::Above).
    // - PCT/USD: Target::Price, harga USD dibagi harga SOL dulu
    //   (perintah user 2026-11-21, supaya tampilan di Aura sama).
    // - MCAP: Target::Mcap, mcap USD skala 1e6.
    let target = if tp_mode == "MCAP" {
        let scaled = (tp_mcap * 1_000_000.0) as u64;
        let mcap_ud128 = fastnum::UD128::from(scaled) / fastnum::UD128::from(1_000_000u64);
        Target::Mcap {
            mcap: mcap_ud128,
            price: None,
            direction: Direction::Above,
        }
    } else {
        let scale: u64 = 1_000_000_000_000;
        let price_sol = tp_price_usd / sol_price;
        let scaled = (price_sol * scale as f64) as u64;
        let price_ud128 = fastnum::UD128::from(scaled) / fastnum::UD128::from(scale);
        Target::Price {
            price: price_ud128,
            direction: Direction::Above,
        }
    };

    let tip_lam = (tip * 1e9) as u64;
    let prio_lam = (prio * 1e9) as u64;

    // Slippage 100% (sama seperti Buy Dip).
    let slippage_val = fastnum::UD128::from(1_000_000u64) / fastnum::UD128::from(1_000_000u64);

    let wallet = {
        // Cache alamat wallet utama (W2) — sama seperti order belinya,
        // supaya TP jual dari wallet yang sama dengan posisi.
        // Tanpa cache, tiap pasang TP fetch wallet dulu (1 panggilan gRPC)
        // dan rawan kena rate limit per-detik Aura. (Optimasi 2026-11-22.)
        static CACHED_WALLET_TP: std::sync::OnceLock<solana_address::Address> =
            std::sync::OnceLock::new();
        if let Some(w) = CACHED_WALLET_TP.get() {
            *w
        } else {
            let mut tc = client.aura();
            match tc
                .fetch_full_wallet_info(
                    (),
                    tonic::Request::new(aura_api_client::types::FetchFullWalletsInfoReq {}),
                )
                .await
            {
                Ok(res) => {
                    let w = pick_main_wallet(&res.into_inner().wallets);
                    let _ = CACHED_WALLET_TP.set(w);
                    w
                }
                Err(e) => return Err(format!("gagal fetch wallet: {}", e.message())),
            }
        }
    };

    let api_order = ApiLimitOrder {
        state: OrderState::Api {
            id: None,
            expire_dur: None,
            activate_dur: None,
        },
        order: RawOrder {
            slippage: slippage_val,
            tip: decisol::Lamports::from(tip_lam),
            fee: decisol::Lamports::from(prio_lam),
            target,
            amount: SwapAmount::SellPerc {
                amount: sell_pct_to_frac(sell_pct),
            },
            procs: TxnProcessors {
                jito_validators: false,
                jito_bundled: false,
                aura: true,
                bloxroute: false,
                nozomi: false,
                next_block: false,
                slot0: false,
                astra: false,
                block_razor: false,
                node1: false,
                tpu_penetrator: false,
                helius: true,
                stellium: true,
                soyas: true,
                falcon: true,
                raiden: true,
                circular: true,
                flash_block: true,
                moon: true,
                blocksprint: true,
                aura_revert: false,
                landx: true,
                manka: true,
                blockrush: true,
            },
            nonce: UserNonceStrategy::Hybrid,
            slot_latency: 0,
        },
        trigger: OrderEventTrigger::Immediate,
        wallet,
    };

    let req = tonic::Request::new(UpdateTokenLimitOrders {
        mint: mint_addr,
        orders: ApiOrders {
            orders: vec![api_order],
        },
    });
    // Rate limiter Aura: jeda min 350ms sejak panggilan Aura terakhir,
    // supaya tidak tembus 4 RPS (order 322888 gagal 2026-11-22).
    crate::aura_slot_wait().await;
    let resp = client
        .limit_orders()
        .place_limit_orders((), req)
        .await
        .map_err(|e| e.message().to_string())?;
    let id = resp
        .into_inner()
        .ids
        .get(0)
        .copied()
        .map(|x| x.0)
        .ok_or_else(|| "Aura tidak mengembalikan order id".to_string())?;
    Ok(id)
}

/// Catat order beli untuk dilacak Auto Sell — hanya kalau toggle Auto Sell
/// untuk sumber order itu ON (manual pakai config Limit Biasa, Second Dip
/// pakai config Second Dip). Dipanggil setelah order beli REAL berhasil
/// dibuat (Second Dip + manual). Kalau toggle OFF, tidak dicatat (tidak ada
/// TP yang dipasang nanti).
pub async fn maybe_track_auto_sell(
    state: &Arc<Mutex<BotState>>,
    aura_order_id: i64,
    mint: &str,
    buy_price: f64,
    amount_usd: f64,
    tip: f64,
    prio: f64,
    source: &str,
) {
    let (active, db_conn) = {
        let st = state.lock().await;
        // Manual (limit biasa): ikut tombol ON/OFF di panel Limit Buy.
        // Second Dip: ikut tombol ON/OFF di menu Second Dip
        // (perintah user 2026-11-22). OFF = order tidak dilacak, tidak
        // ada TP yang dipasang untuk order itu.
        let on = if source == "second_dip" {
            st.auto_sell_sd_active
        } else {
            st.auto_sell_active
        };
        (on, st.db_conn.clone())
    };
    if !active {
        return;
    }
    if let Ok(conn) = db_conn.try_lock() {
        // Idempotent: kalau order sudah dilacak (mis. dipasang dari Sonic
        // saat placement), jangan timpa — source (manual/second_dip) dan
        // harga entry-nya harus tetap dari saat order dipasang. Panggilan
        // dari stream User Activity hanya melacak order yang BELUM dilacak
        // (mis. dipasang dari bot resmi Aura). (Perintah user 2026-11-22.)
        if db::is_auto_sell_tracked(&conn, aura_order_id).unwrap_or(false) {
            return;
        }
        let _ = db::insert_auto_sell_tracked(
            &conn, aura_order_id, mint, buy_price, amount_usd, tip, prio, source,
        );
        info!(
            "[AutoSell] Lacak order beli {} ({} {}) untuk TP",
            aura_order_id,
            source,
            short_addr(mint)
        );
    };
}

/// Pasang TP sell SEGERA untuk order beli yang baru terisi (fast path).
/// Dipanggil dari stream User Activity saat BUY Confirmed — supaya TP
/// terkirim ke Aura dalam ~1 detik, tanpa nunggu tick watcher.
/// (Watcher DIHAPUS total, perintah user 2026-11-22 — stream yang pasang TP.)
/// - Source (manual/second_dip) diambil dari baris lacak di DB, jadi config
///   yang dipakai selalu benar walau order dipasang dari Sonic (SD) atau
///   bot resmi Aura (manual).
/// - Kalau toggle config itu OFF, atau TP sudah terpasang, diam saja.
/// - Sukses: tandai sell_placed=1 + notif ke user. Gagal: log + notif gagal
///   (TIDAK ADA RETRY — tidak ada watcher lagi — user harus tahu).
pub async fn place_tp_now(
    state: &Arc<Mutex<BotState>>,
    bot: &Bot,
    aura_order_id: i64,
    mint: &str,
    buy_price: f64,
) {
    // Ambil client + config (manual & SD) + baris lacak dari DB.
    let (client_opt, db_conn, chats, cfg_manual, cfg_sd, tracked) = {
        let st = state.lock().await;
        let cfg_manual = AutoSellCfg {
            active: st.auto_sell_active,
            tp_mode: st.auto_sell_tp_mode.clone(),
            tp_pct: st.auto_sell_tp_pct,
            tp_usd: st.auto_sell_tp_usd,
            tp_mcap: st.auto_sell_tp_mcap,
            sell_pct: st.auto_sell_sell_pct,
            tip: st.auto_sell_tip,
            prio: st.auto_sell_prio,
        };
        let cfg_sd = AutoSellCfg {
            active: st.auto_sell_sd_active,
            tp_mode: st.auto_sell_sd_tp_mode.clone(),
            tp_pct: st.auto_sell_sd_tp_pct,
            tp_usd: st.auto_sell_sd_tp_usd,
            tp_mcap: st.auto_sell_sd_tp_mcap,
            sell_pct: st.auto_sell_sd_sell_pct,
            tip: st.auto_sell_sd_tip,
            prio: st.auto_sell_sd_prio,
        };
        let tracked = match st.db_conn.try_lock() {
            Ok(conn) => db::get_auto_sell_tracked(&conn, aura_order_id).unwrap_or(None),
            Err(_) => None,
        };
        (
            st.aura_client.clone(),
            st.db_conn.clone(),
            st.active_chats.clone(),
            cfg_manual,
            cfg_sd,
            tracked,
        )
    };
    // Tidak dilacak (toggle OFF saat dilacak, atau bukan limit order) -> diam.
    let Some((source, sell_placed)) = tracked else {
        return;
    };
    // TP sudah terpasang (mis. fill ganda) -> jangan dobel.
    if sell_placed == 1 {
        return;
    }
    let cfg = if source == "second_dip" {
        &cfg_sd
    } else {
        &cfg_manual
    };
    if !cfg.active {
        return;
    }
    let Some(client) = client_opt else {
        return;
    };
    let tp_price = calc_tp_price(buy_price, &cfg.tp_mode, cfg.tp_pct, cfg.tp_usd);
    info!(
        "[AutoSell] Order beli {} ({}) terisi — pasang TP sell SEGERA @ ${} ({}), jual {}%",
        aura_order_id,
        short_addr(mint),
        fmt_price(tp_price),
        cfg.tp_mode,
        cfg.sell_pct
    );
    log_flow(&format!(
        "AUTO SELL PASANG TP (cepat) | order={} | token={} | buy=${} | tp=${} ({}) | jual={}%",
        aura_order_id,
        mint,
        fmt_price(buy_price),
        fmt_price(tp_price),
        cfg.tp_mode,
        cfg.sell_pct
    ));
    // Coba pasang TP; kalau kena rate limit, tunggu 2 detik lalu coba
    // lagi, maks 2x retry (bukan watcher — supaya TP tidak hilang cuma
    // karena tabrakan di detik yang sama, seperti order 322888).
    let sell_res = {
        let mut attempt = 0;
        loop {
            attempt += 1;
            match place_tp_sell_order(
                &client,
                mint,
                tp_price,
                &cfg.tp_mode,
                cfg.tp_mcap,
                cfg.tip,
                cfg.prio,
                cfg.sell_pct,
            )
            .await
            {
                Ok(id) => break Ok(id),
                Err(e) if attempt <= 2 && e.to_lowercase().contains("rate limit") => {
                    info!(
                        "[AutoSell] TP order {} kena rate limit, retry {}/2 setelah 2 detik...",
                        aura_order_id, attempt
                    );
                    log_flow(&format!(
                        "AUTO SELL RETRY (cepat) | order={} | token={} | percobaan={}/2 | err={}",
                        aura_order_id, mint, attempt, e
                    ));
                    tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
                }
                Err(e) => break Err(e),
            }
        }
    };
    match sell_res {
        Ok(sell_id) => {
            if let Ok(conn) = db_conn.try_lock() {
                let _ = db::mark_auto_sell_placed(&conn, aura_order_id, sell_id);
            }
            log_flow(&format!(
                "AUTO SELL TERPASANG | token={} | buy=${} | tp=${} ({}) | jual={}% | sell_id={}",
                mint,
                fmt_price(buy_price),
                fmt_price(tp_price),
                cfg.tp_mode,
                cfg.sell_pct,
                sell_id
            ));
            notify_all(
                bot,
                &chats,
                &format!(
                    "🎯 *Auto Sell: Take Profit Dipasang!*\n\n🏦 Token: `{}`\n💰 Harga beli: `${}`\n🎯 Harga jual (TP): `${}` ({})\n💰 Jual: `{}%` posisi\n📋 Sell Order ID: {}\n\n_Order jual akan fill kalau harga naik ke TP._",
                    short_addr(mint),
                    fmt_price(buy_price),
                    fmt_price(tp_price),
                    cfg.tp_mode,
                    cfg.sell_pct,
                    sell_id
                ),
            )
            .await;
        }
        Err(e) => {
            error!(
                "[AutoSell] Pasang TP CEPAT gagal untuk order {}: {} — retry 2x (rate limit) sudah dicoba / tidak berlaku. Cek manual!",
                aura_order_id, e
            );
            log_flow(&format!(
                "AUTO SELL GAGAL (cepat) | order={} | token={} | err={}",
                aura_order_id, mint, e
            ));
            notify_all(
                bot,
                &chats,
                &format!(
                    "⚠️ *Auto Sell GAGAL pasang TP!*\n\n🏦 Token: `{}`\n📋 Buy Order ID: {}\n❌ {}\n\n_Retry 1x khusus rate limit sudah dicoba (kalau itu penyebabnya) — cek / pasang TP manual._",
                    short_addr(mint),
                    aura_order_id,
                    e
                ),
            )
            .await;
        }
    }
}

/// Satu set config Auto Sell (dipakai per sumber order: manual / second_dip).
/// Tip & Prio di sini adalah nilai yang dikirim ke Aura saat pasang order
/// jual TP (bukan Tip/Prio order belinya).
#[derive(Clone)]
struct AutoSellCfg {
    active: bool,
    tp_mode: String,
    tp_pct: f64,
    tp_usd: f64,
    tp_mcap: f64,
    sell_pct: f64,
    tip: f64,
    prio: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx_buy_usdc() -> serde_json::Value {
        serde_json::json!({
            "meta": {
                "err": null,
                "preTokenBalances": [
                    {"owner": "AURARwy7F1sfbxeRkdcUCVg2eG1mpeHG1jChRU9S7hhP", "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "uiTokenAmount": {"uiAmount": 500.0}},
                    {"owner": "AURARwy7F1sfbxeRkdcUCVg2eG1mpeHG1jChRU9S7hhP", "mint": "TOKENMINTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "uiTokenAmount": {"uiAmount": 0.0}}
                ],
                "postTokenBalances": [
                    {"owner": "AURARwy7F1sfbxeRkdcUCVg2eG1mpeHG1jChRU9S7hhP", "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "uiTokenAmount": {"uiAmount": 290.0}},
                    {"owner": "AURARwy7F1sfbxeRkdcUCVg2eG1mpeHG1jChRU9S7hhP", "mint": "TOKENMINTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "uiTokenAmount": {"uiAmount": 1000000.0}}
                ]
            }
        })
    }

    #[test]
    fn test_parse_trade_buy_usdc() {
        let tx = tx_buy_usdc();
        let t = parse_trade(&tx, "AURARwy7F1sfbxeRkdcUCVg2eG1mpeHG1jChRU9S7hhP").unwrap();
        assert_eq!(t.side, "BUY");
        assert_eq!(t.mint, "TOKENMINTAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
        assert_eq!(t.tokens, 1_000_000.0);
        assert_eq!(t.paid, 210.0);
        assert_eq!(t.paid_asset, "USDC");
        assert!((t.entry - 0.00021).abs() < 1e-12);
    }

    #[test]
    fn test_parse_trade_failed_tx_ignored() {
        let mut tx = tx_buy_usdc();
        tx["meta"]["err"] = serde_json::json!({"InstructionError": [0, "Custom"]});
        assert!(parse_trade(&tx, "AURARwy7F1sfbxeRkdcUCVg2eG1mpeHG1jChRU9S7hhP").is_none());
    }

    #[test]
    fn test_parse_trade_other_owner_ignored() {
        let tx = tx_buy_usdc();
        assert!(parse_trade(&tx, "SomeOtherWallet111111111111111111111111111").is_none());
    }

    #[test]
    fn test_parse_trade_real_sell_usdc() {
        // Transaksi SELL nyata 9S7hhP (diambil dari Helius, 2026-11-19):
        // jual token 6WFq terima USDC. parse_trade HARUS mengembalikan SELL.
        let data = std::fs::read_to_string("tests/fixtures/tx_sell_2hLbmLYioE2D.json")
            .expect("fixture tx sell tidak ketemu");
        let tx: serde_json::Value = serde_json::from_str(&data).unwrap();
        let t = parse_trade(&tx, "AURARwy7F1sfbxeRkdcUCVg2eG1mpeHG1jChRU9S7hhP");
        let t = t.expect("parse_trade mengembalikan None untuk SELL nyata — BUG");
        assert_eq!(t.side, "SELL");
        assert_eq!(t.paid_asset, "USDC");
        assert!(t.tokens > 0.0, "tokens harus > 0");
        assert!(t.paid > 0.0, "paid (USDC diterima) harus > 0");
        assert!(t.entry > 0.0, "entry harus > 0");
    }

    #[test]
    fn test_parse_trade_real_sell_usdc_big() {
        // SELL besar 9S7hhP: jual ~141jt token 6WFq terima ~828 USDC.
        let data = std::fs::read_to_string("tests/fixtures/tx_sell_4iS5jJwsi8xU.json")
            .expect("fixture tx sell tidak ketemu");
        let tx: serde_json::Value = serde_json::from_str(&data).unwrap();
        let t = parse_trade(&tx, "AURARwy7F1sfbxeRkdcUCVg2eG1mpeHG1jChRU9S7hhP");
        let t = t.expect("parse_trade mengembalikan None untuk SELL besar — BUG");
        assert_eq!(t.side, "SELL");
        assert_eq!(t.paid_asset, "USDC");
        assert!(t.tokens > 100_000_000.0);
        assert!(t.paid > 800.0);
    }

    #[test]
    fn test_sell_pct_to_frac() {
        // Repo aura_api_client: SellPerc ditampilkan sebagai amount x 100 %,
        // jadi 100% = 1, 50% = 0.5, 25% = 0.25.
        assert_eq!(sell_pct_to_frac(100.0).to_string(), "1");
        assert_eq!(sell_pct_to_frac(50.0).to_string(), "0.5");
        assert_eq!(sell_pct_to_frac(25.0).to_string(), "0.25");
        assert_eq!(sell_pct_to_frac(0.0).to_string(), "0");
        // Di-clamp ke 0-100
        assert_eq!(sell_pct_to_frac(150.0).to_string(), "1");
    }

    #[test]
    fn test_fmt_price() {
        // Harga kecil pakai subskrip (ala Dexscreener)
        assert_eq!(fmt_price(0.0000012), "0.0₅12");
        assert_eq!(fmt_price(0.0000007), "0.0₆7");
        assert_eq!(fmt_price(0.0000025), "0.0₅25");
        assert_eq!(fmt_price(0.00001379), "0.0₄1379");
        assert_eq!(fmt_price(0.0001), "0.0₃1");
        // Harga >= 0.001 tampil biasa
        assert_eq!(fmt_price(2.5), "2.5");
        assert_eq!(fmt_price(0.05), "0.05");
        assert_eq!(fmt_price(0.001), "0.001");
        assert_eq!(fmt_price(0.0), "0");
    }

    #[test]
    fn test_calc_trigger1_price() {
        // Mode PCT: entry * (1 + pct%).
        assert!((calc_trigger1_price(0.000007, "PCT", 100.0, 0.0) - 0.000014).abs() < 1e-12);
        // Mode USD: harga absolut, abaikan entry & pct.
        assert!((calc_trigger1_price(0.000007, "USD", 100.0, 0.00001) - 0.00001).abs() < 1e-12);
        // Mode USD tapi usd = 0 (belum diatur) -> fallback ke PCT.
        assert!((calc_trigger1_price(0.000007, "USD", 100.0, 0.0) - 0.000014).abs() < 1e-12);
        // Mode tak dikenal -> fallback ke PCT.
        assert!((calc_trigger1_price(0.000007, "XXX", 50.0, 0.00001) - 0.0000105).abs() < 1e-12);
    }

    #[test]
    fn test_calc_tp_price() {
        // PCT default +40%: beli $0.0000025 -> TP $0.0000035.
        assert!((calc_tp_price(0.0000025, "PCT", 40.0, 0.0) - 0.0000035).abs() < 1e-12);
        // USD: harga absolut, abaikan beli & pct.
        assert!((calc_tp_price(0.0000025, "USD", 40.0, 0.000004) - 0.000004).abs() < 1e-12);
        // USD tapi usd = 0 -> fallback ke PCT.
        assert!((calc_tp_price(0.0000025, "USD", 40.0, 0.0) - 0.0000035).abs() < 1e-12);
        // MCAP: fallback ke PCT (harga hanya untuk log).
        assert!((calc_tp_price(0.0000025, "MCAP", 40.0, 0.0) - 0.0000035).abs() < 1e-12);
    }

    #[test]
    fn test_decide_price() {
        // Di atas/equal Pemicu 1 -> Trigger (termasuk harga di bawah entry —
        // Pemicu 1 selalu buat order, perintah user 2026-11-23).
        assert_eq!(decide_price(0.000014, 0.000014), PriceDecision::Trigger);
        assert_eq!(decide_price(0.000008, 0.000007), PriceDecision::Trigger);
        assert_eq!(decide_price(0.000012, 0.000010), PriceDecision::Trigger);
        assert_eq!(decide_price(0.000010, 0.000010), PriceDecision::Trigger);
        // Di bawah Pemicu 1 -> Wait (tidak ada Cancel karena harga).
        assert_eq!(decide_price(0.000006, 0.000014), PriceDecision::Wait);
        assert_eq!(decide_price(0.000010, 0.000014), PriceDecision::Wait);
        assert_eq!(decide_price(0.000007, 0.000014), PriceDecision::Wait);
    }

    #[test]
    fn test_price_from_sqrt_price_data() {
        // sqrtPrice NIGGER pool DAMM v2 (BqZrzm...) dari SDK Meteora:
        // 190011405113501370 -> ~1.06101101858e-7 SOL/NIGGER (dec 6/9).
        // Dengan SOL $71.86 -> ~$0.000007624 (Dexscreener $0.000007375, ±3%).
        let mut data = vec![0u8; 1112];
        let sp: u128 = 190011405113501370;
        data[SQRT_PRICE_OFFSET..SQRT_PRICE_OFFSET + 16]
            .copy_from_slice(&sp.to_le_bytes());
        let price = price_from_sqrt_price_data(&data, 6, 9, WSOL_MINT, Some(71.86))
            .expect("harga sqrtPrice harus terhitung");
        // Cocok dengan SDK (±1%) dan jauh lebih akurat dari vault-ratio ($0.00001418).
        assert!((price - 0.000007624).abs() < 0.0000001, "price={}", price);
        // Data terlalu pendek / sqrtPrice 0 -> None.
        assert!(price_from_sqrt_price_data(&data[..100], 6, 9, WSOL_MINT, Some(71.86)).is_none());
        let zero = vec![0u8; 1112];
        assert!(price_from_sqrt_price_data(&zero, 6, 9, WSOL_MINT, Some(71.86)).is_none());
    }

    #[test]
    fn test_wallet_label() {
        assert_eq!(
            wallet_label("AURARwy7F1sfbxeRkdcUCVg2eG1mpeHG1jChRU9S7hhP"),
            "9S7hhP"
        );
        assert_eq!(
            wallet_label("So11111111111111111111111111111111111111112"),
            "So1111...1112"
        );
    }

    #[test]
    fn test_pick_main_wallet() {
        // Pilih W2 (wallet utama user), bukan wallets[0] (W1, tidak dipakai).
        use std::str::FromStr;
        let w1 = solana_address::Address::from_str(
            "AURA3KiErbWcxjWVArL4Qn5KcVfrMVgTz9nKWJWWJUAC",
        )
        .unwrap();
        let w2 = solana_address::Address::from_str(
            "Bz9FifdG8qeszPxoUSqi44chKejd6fUxFTkKLVM3TrRm",
        )
        .unwrap();
        // W2 ada di list (urutan apa pun) -> pilih W2.
        assert_eq!(pick_main_wallet(&[w1, w2]), w2);
        assert_eq!(pick_main_wallet(&[w2, w1]), w2);
        // W2 tidak ada (misal ganti key) -> fallback ke wallets[0].
        assert_eq!(pick_main_wallet(&[w1]), w1);
        // List kosong -> default kosong.
        assert_eq!(
            pick_main_wallet(&[]),
            solana_address::Address::default()
        );
    }

    #[test]
    fn test_amount_usd_to_usdc() {
        // USDC 6 desimal: $50 -> 50_000_000, $25 -> 25_000_000, $1 -> 1_000_000.
        // (Sebelumnya: $50 -> 0.677 SOL lamports, order tidak pernah terisi.)
        assert_eq!(amount_usd_to_usdc(50.0), 50_000_000);
        assert_eq!(amount_usd_to_usdc(25.0), 25_000_000);
        assert_eq!(amount_usd_to_usdc(1.0), 1_000_000);
        assert_eq!(amount_usd_to_usdc(0.0), 0);
    }

    #[test]
    fn test_parse_trade_native_sol_buy_jupiter() {
        // Buy FOTF via Jupiter bayar SOL native (wallet test 65faAa...,
        // 2026-11-18 21:23:07 WITA, sig 2m6bViXj...). Wallet tidak punya
        // perubahan saldo token USDC/wSOL — tanpa jalur SOL native,
        // parse_trade mengembalikan None dan pemicu 1 tidak pernah kena.
        let tx: serde_json::Value =
            serde_json::from_str(include_str!("testdata_buy_jupiter.json")).unwrap();
        let t = parse_trade(
            &tx,
            "65faAafDDPapnAsbX2LXFNqKQDupbWUgfg4RM674gzJD",
        )
        .expect("buy Jupiter SOL native harus terdeteksi");
        assert_eq!(t.side, "BUY");
        assert_eq!(t.mint, "HaVfEUNV8KGMrwyGP34FktFU6ZKfR3UFdExEY2tKvhcD");
        assert!((t.tokens - 73011.999).abs() < 0.01, "tokens {}", t.tokens);
        // Bayar ~0.0115 SOL (lamport turun 0.0145 - fee 0.001 - sewa ATA 0.002).
        assert!(t.paid > 0.005 && t.paid < 0.02, "paid SOL {}", t.paid);
        assert_eq!(t.paid_asset, "wSOL");
        // Entry ~1.6e-7 SOL/token — di atas pemicu 1 USD $0.000007
        // (SOL ~$70 -> entry ~$0.000011).
        assert!(t.entry > 0.0 && t.entry < 1e-6, "entry {}", t.entry);
        // Vault pool FOTF/USDC harus ketemu (pool DAMM v2 EGnQh2fp...).
        assert!(t.base_vault.is_some(), "base vault harus ketemu");
        assert!(t.quote_vault.is_some(), "quote vault harus ketemu");
        // Pool FOTF USDC-quoted — quote_mint HARUS USDC walau bayar pakai SOL.
        // Kalau ini wSOL, price watcher mengalikan harga dengan harga SOL
        // (74x terlalu tinggi) dan pemicu jadi salah.
        assert_eq!(
            t.quote_mint, "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            "quote_mint harus USDC untuk pool FOTF"
        );
    }

    #[test]
    fn test_parse_trade_native_sol_buy_jupiter_small() {
        // Buy KEDUA via Jupiter (wallet test, 2026-11-18 21:38:14 WITA,
        // sig 2kSPwk5G...): cuma 7.344 FOTF seharga ~0.001 SOL. ATA FOTF
        // sudah ada (saldo 0) jadi TIDAK ada sewa ATA — preTokenBalances
        // berisi FOTF uiAmount null -> balances() memasukkan 0.0, jadi
        // pre.contains_key benar dan sewa tidak dikurangi. Kalau sewa
        // keliru dikurangi, sol_spent jadi negatif dan buy tidak terdeteksi.
        let tx: serde_json::Value =
            serde_json::from_str(include_str!("testdata_buy2_jupiter.json")).unwrap();
        let t = parse_trade(
            &tx,
            "65faAafDDPapnAsbX2LXFNqKQDupbWUgfg4RM674gzJD",
        )
        .expect("buy kecil Jupiter SOL native harus terdeteksi");
        assert_eq!(t.side, "BUY");
        assert_eq!(t.mint, "HaVfEUNV8KGMrwyGP34FktFU6ZKfR3UFdExEY2tKvhcD");
        assert!((t.tokens - 7344.644704).abs() < 0.01, "tokens {}", t.tokens);
        // Bayar ~0.001 SOL (lamport turun 1.011.273 - fee 11.273).
        assert!(t.paid > 0.0005 && t.paid < 0.002, "paid SOL {}", t.paid);
        assert_eq!(t.paid_asset, "wSOL");
        assert!(t.entry > 0.0 && t.entry < 1e-6, "entry {}", t.entry);
        assert!(t.base_vault.is_some(), "base vault harus ketemu");
        assert!(t.quote_vault.is_some(), "quote vault harus ketemu");
        assert_eq!(
            t.quote_mint, "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            "quote_mint harus USDC untuk pool FOTF"
        );
    }
}

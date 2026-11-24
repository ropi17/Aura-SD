use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex;
use teloxide::{prelude::*, utils::command::BotCommands};
use teloxide::types::{ChatId, InlineKeyboardButton, InlineKeyboardMarkup, MessageId};
use governor::{Quota, RateLimiter};
use nonzero_ext::nonzero;
use log::{info, error};

use tonic::transport::Endpoint;
use tonic::{Request, Status};
use tokio_stream::StreamExt;

// aura_api_client
use aura_api_client::client::AuraClients;
use aura_api_client::client_ext::UserCtxInterceptor;
use aura_api_client::types::UserActionEventSub;

/// A no-op UserCtxInterceptor: all per-call context is handled by the
/// connection-level `auth_interceptor` function, so we don't need a per-call payload.
#[derive(Clone, Copy)]
struct NoCtx;

impl UserCtxInterceptor for NoCtx {
    type Payload = ();
    fn intercept<T>(_payload: (), _req: &mut tonic::Request<T>) -> Result<(), tonic::Status> {
        Ok(())
    }
}

mod db;
mod seconddip;
use db::{init_db, load_settings, load_chats};

#[derive(Clone, PartialEq)]
enum AppMode {
    Simulation,
    Mainnet,
}

#[derive(BotCommands, Clone)]
#[command(rename_rule = "lowercase", description = "Perintah bot ini:")]
enum Command {
    #[command(description = "Mulai bot dan tampilkan Menu Utama.")]
    Start,
    #[command(description = "Ubah ke mode Simulasi.")]
    ModeSimulasi,
    #[command(description = "Ubah ke mode Mainnet (ASLI/LIVE).")]
    ModeMainnet,
    #[command(description = "Simulasi trigger limit buy kena.")]
    SimulateBuy,
    #[command(description = "Bersihkan pesan sebelumnya untuk tampilan lebih rapi.")]
    Clear,
    #[command(description = "Tampilkan kembali panel Swap Sell jika tertumpuk.")]
    Panel,
}

#[derive(Clone)]
struct LimitOrder {
    id: usize,
    token: String,
    amount_usd: f64,
    target: String,       // unified target: mcap / price-usd / persen-change
    tip_fee: f64,
    prio_fee: f64,
    aura_order_id: Option<i64>,
}

/// Preset tip/prio untuk quick-set (Kecil/Sedang/Besar/Mega)
#[derive(Clone)]
struct TxPreset {
    pub label: String,
    pub tip: f64,
    pub prio: f64,
}

impl TxPreset {
    fn new(label: &str, tip: f64, prio: f64) -> Self {
        TxPreset { label: label.to_string(), tip, prio }
    }
}

/// Index preset aktif untuk quick-set buy (None = tidak ada yang aktif)
#[derive(Clone, PartialEq)]
enum ActivePreset {
    None,
    Idx(usize),
}

#[derive(Clone, PartialEq)]
#[allow(dead_code)]
enum EditField {
    None,
    // Limit Buy (Baru)
    BuyAmount,
    BuyTarget,            // unified: mcap / price-usd / persen-change
    BuyTargetUsd,         // target tipe USD Price (dari picker target)
    BuyTargetPct,         // target tipe Persen (dari picker target)
    BuyTargetMcap,        // target tipe Mcap (dari picker target)
    BuyTargetPnl,         // target tipe PnL (dari picker target)
    BuyTip,
    BuyPrio,
    // Quick preset editing (idx preset, field: 0=tip 1=prio)
    BuyPresetTip(usize),
    BuyPresetPrio(usize),
    // Second Dip
    SdTrigger1,
    SdTrigger1Usd,
    SdTrigger2,
    SdTrigger1Timeout,
    SdTrigger2Timeout,
    SdTrigger2Usd,
    SdTrigger2Mcap,
    SdTip,
    SdPrio,
    SdAmount,
    SdWallet,
    // Auto Sell (Take Profit) — config Limit Order Biasa (manual)
    AutoSellTp,
    AutoSellTpUsd,
    AutoSellTpMcap,
    AutoSellSellPct,
    AutoSellTip,
    AutoSellPrio,
    // Auto Sell (Take Profit) — config Second Dip
    AutoSellSdTp,
    AutoSellSdTpUsd,
    AutoSellSdTpMcap,
    AutoSellSdSellPct,
    AutoSellSdTip,
    AutoSellSdPrio,
    // Swap Sell Panel
    SellTip,
    SellPrio,
    SellSlippage,
    // History edit (by DB id)
    HistTarget(i64),
    HistTipById(i64),
    HistPrioById(i64),
    // Limit Order Setup preset fields (idx: 0=kecil,1=sedang,2=besar,3=mega)
    SetupPresetTip(usize),
    SetupPresetPrio(usize),
    // Legacy (kept for compat)
    HistAmount(i64),
    HistMcap(usize),
    HistTip(usize),
    HistPrio(usize),
}

type InterceptorFn = fn(Request<()>) -> Result<Request<()>, Status>;

struct BotState {
    aura_client: Option<AuraClients<InterceptorFn, NoCtx>>,
    #[allow(dead_code)]
    aura_api_key: String,
    mode: AppMode,
    limiter: Arc<governor::DefaultDirectRateLimiter>,
    db_conn: Arc<Mutex<rusqlite::Connection>>,
    edit_field: EditField,

    // Second Dip Settings (copytrade wallet target)
    second_dip_active: bool,
    /// Daftar wallet target Second Dip (multi-wallet, maks 10).
    /// Sumber kebenaran adalah tabel `second_dip_targets` di DB.
    second_dip_targets: Vec<String>,
    second_dip_trigger1_pct: f64,
    /// Mode harga Pemicu 1: "PCT" (% di atas entry) atau "USD" (harga absolut USD).
    second_dip_trigger1_mode: String,
    /// Harga Pemicu 1 absolut USD (mode "USD").
    second_dip_trigger1_usd: f64,
    second_dip_trigger2_pct: f64,
    /// Batas waktu Pemicu 1 (menit). 0 = tidak ada batas (tunggu selamanya).
    second_dip_trigger1_timeout_mins: u64,
    /// Batas waktu Pemicu 2 / masa berlaku order (menit). 0 = default Aura.
    second_dip_trigger2_timeout_mins: u64,
    /// Mode harga Pemicu 2: "PCT" (% di atas entry), "USD" (harga absolut),
    /// atau "MCAP" (market cap USD).
    second_dip_trigger2_mode: String,
    /// Harga Pemicu 2 absolut USD (mode "USD").
    second_dip_trigger2_usd: f64,
    /// Market cap Pemicu 2 USD (mode "MCAP").
    second_dip_trigger2_mcap: f64,
    second_dip_tip: f64,
    second_dip_prio: f64,
    second_dip_amount_usd: f64,
    dip_mode: String,             // "BUY_DIP" atau "SECOND_DIP"
    second_dip_dry_run: bool,     // true = simulasi, order TIDAK dikirim ke Aura
    // ── Auto Sell (Take Profit otomatis setelah limit order beli terisi) ──
    // Config LIMIT ORDER BIASA (manual).
    auto_sell_active: bool,
    auto_sell_tp_mode: String,
    auto_sell_tp_pct: f64,
    auto_sell_tp_usd: f64,
    auto_sell_tp_mcap: f64,
    auto_sell_sell_pct: f64,
    // Config SECOND DIP (terpisah dari limit order biasa).
    auto_sell_sd_active: bool,
    auto_sell_sd_tp_mode: String,
    auto_sell_sd_tp_pct: f64,
    auto_sell_sd_tp_usd: f64,
    auto_sell_sd_tp_mcap: f64,
    auto_sell_sd_sell_pct: f64,
    // Tip & Prio order jual TP (dikirim ke Aura saat pasang TP sell).
    auto_sell_tip: f64,
    auto_sell_prio: f64,
    auto_sell_sd_tip: f64,
    auto_sell_sd_prio: f64,
    // Target aktif Second Dip (runtime saja, tidak disimpan di DB)
    second_dip_target: Option<crate::seconddip::SecondDipTarget>,
    /// Status koneksi WSS Second Dip (runtime saja, tidak disimpan di DB).
    /// true = WSS terhubung + subscribe aktif; false = diskonek / belum konek.
    second_dip_wss_connected: bool,
    /// Latensi WSS terakhir (RTT Ping/Pong, milidetik). None = belum ada pengukuran.
    second_dip_wss_ping_ms: Option<u64>,
    /// Latensi server Aura terakhir (RTT TCP connect ke trade.aura.rehab:40051, milidetik). None = belum ada pengukuran.
    second_dip_aura_ping_ms: Option<u64>,
    /// Pesan submenu Pemicu 1 yang sedang tampil (chat, msg) — agar edit nilai
    /// bisa update pesan itu di tempat (seperti Aura), bukan kirim pesan baru.
    sd_trigger1_msg: Option<(ChatId, MessageId)>,
    /// Pesan submenu Pemicu 2 yang sedang tampil (chat, msg).
    sd_trigger2_msg: Option<(ChatId, MessageId)>,
    /// Pesan halaman Auto Sell Setup yang sedang tampil (chat, msg).
    as_setup_msg: Option<(ChatId, MessageId)>,
    /// Pesan panel Limit Buy yang sedang tampil (chat, msg) — agar edit nilai
    /// / mode meng-update panel itu di tempat, bukan kirim panel baru di bawah.
    limitbuy_msg: Option<(ChatId, MessageId)>,
    /// Pesan tampilan Orders berkelompok (untuk refresh in-place).
    orders_msg: Option<(ChatId, MessageId)>,
    /// Detail order History yang sedang tampil (chat, msg, local_id) — agar
    /// edit bisa update pesan itu di tempat, bukan kirim pesan baru
    /// (Perubahan 2 & 3, 2026-11-21).
    hist_detail_msg: Option<(ChatId, MessageId, i64)>,
    /// Edit order yang sedang di-stage — belum ke DB/Aura sampai tekan
    /// tombol 💾 SIMPAN (Perubahan 2, 2026-11-21).
    pending_hist_edit: Option<PendingHistEdit>,
    /// Pesan prompt "Ketik nilai..." terakhir (chat, msg) — dihapus setelah
    /// nilai berhasil disimpan, supaya chat tidak menumpuk.
    sd_prompt_msg: Option<(ChatId, MessageId)>,

    // Swap Sell Settings
    sell_tip_fee: f64,
    sell_prio_fee: f64,
    sell_slippage: String,

    // Manual Limit Buy Settings (mode pembuatan order baru)
    active_token: Option<String>,
    active_token_name: Option<String>, // Nama/ticker token yang ditempel (dari Dexscreener).
    active_token_price: Option<f64>, // Harga USD token yang ditempel (dari Dexscreener).
    active_token_liquidity: Option<f64>, // Liquiditas USD token yang ditempel (dari Dexscreener).
    buy_amount: f64,
    buy_currency: String,        // "USD" or "SOL"
    buy_target: String,          // unified target: mcap / price-usd / persen-change
    buy_tip_fee: f64,
    buy_prio_fee: f64,

    // Quick-set presets untuk Tip & Prio (Kecil/Sedang/Besar/Mega)
    buy_presets: Vec<TxPreset>,
    buy_active_preset: ActivePreset,

    // Preset values (from setup menu)
    preset_kecil_tip: f64,
    preset_kecil_prio: f64,
    preset_sedang_tip: f64,
    preset_sedang_prio: f64,
    preset_besar_tip: f64,
    preset_besar_prio: f64,
    preset_mega_tip: f64,
    preset_mega_prio: f64,

    // History of limit orders (runtime cache, source of truth is db)
    orders: Vec<LimitOrder>,
    next_order_id: usize,

    // Active chats for notifications
    active_chats: std::collections::HashSet<ChatId>,

    // Track panel swap sell (chat_id, msg_id) agar bisa di-edit saat auto limit terpenuhi
    swap_panel_msgs: Vec<(ChatId, teloxide::types::MessageId)>,

    // Deduplikasi tx signature dari stream
    processed_signatures: std::collections::HashSet<String>,

    // Token aktif yang dibeli (untuk tombol konfirmasi swap sell)
    bought_token: Option<String>,

    // Mode pembelian: "LIMIT" atau "SWAP"
    buy_mode: String,
}

impl BotState {
    pub fn get_db_settings(&self) -> db::DbSettings {
        db::DbSettings {
            second_dip_active: self.second_dip_active,
            // Kolom settings lama (satu wallet) tidak lagi dipakai logika,
            // tapi tetap disinkronkan ke target pertama untuk kompatibilitas.
            second_dip_target_wallet: self
                .second_dip_targets
                .first()
                .cloned()
                .unwrap_or_else(|| db::DEFAULT_SECOND_DIP_WALLET.to_string()),
            second_dip_trigger1_pct: self.second_dip_trigger1_pct,
            second_dip_trigger1_mode: self.second_dip_trigger1_mode.clone(),
            second_dip_trigger1_usd: self.second_dip_trigger1_usd,
            second_dip_trigger2_pct: self.second_dip_trigger2_pct,
            second_dip_trigger1_timeout_mins: self.second_dip_trigger1_timeout_mins,
            second_dip_trigger2_timeout_mins: self.second_dip_trigger2_timeout_mins,
            second_dip_trigger2_mode: self.second_dip_trigger2_mode.clone(),
            second_dip_trigger2_usd: self.second_dip_trigger2_usd,
            second_dip_trigger2_mcap: self.second_dip_trigger2_mcap,
            second_dip_tip: self.second_dip_tip,
            second_dip_prio: self.second_dip_prio,
            second_dip_amount_usd: self.second_dip_amount_usd,
            dip_mode: self.dip_mode.clone(),
            second_dip_dry_run: self.second_dip_dry_run,
            auto_sell_active: self.auto_sell_active,
            auto_sell_tp_mode: self.auto_sell_tp_mode.clone(),
            auto_sell_tp_pct: self.auto_sell_tp_pct,
            auto_sell_tp_usd: self.auto_sell_tp_usd,
            auto_sell_tp_mcap: self.auto_sell_tp_mcap,
            auto_sell_sell_pct: self.auto_sell_sell_pct,
            auto_sell_sd_active: self.auto_sell_sd_active,
            auto_sell_sd_tp_mode: self.auto_sell_sd_tp_mode.clone(),
            auto_sell_sd_tp_pct: self.auto_sell_sd_tp_pct,
            auto_sell_sd_tp_usd: self.auto_sell_sd_tp_usd,
            auto_sell_sd_tp_mcap: self.auto_sell_sd_tp_mcap,
            auto_sell_sd_sell_pct: self.auto_sell_sd_sell_pct,
            auto_sell_tip: self.auto_sell_tip,
            auto_sell_prio: self.auto_sell_prio,
            auto_sell_sd_tip: self.auto_sell_sd_tip,
            auto_sell_sd_prio: self.auto_sell_sd_prio,
            sell_tip_fee: self.sell_tip_fee,
            sell_prio_fee: self.sell_prio_fee,
            sell_slippage: self.sell_slippage.clone(),
            preset_kecil_tip: self.preset_kecil_tip,
            preset_kecil_prio: self.preset_kecil_prio,
            preset_sedang_tip: self.preset_sedang_tip,
            preset_sedang_prio: self.preset_sedang_prio,
            preset_besar_tip: self.preset_besar_tip,
            preset_besar_prio: self.preset_besar_prio,
            preset_mega_tip: self.preset_mega_tip,
            preset_mega_prio: self.preset_mega_prio,
            bought_token: self.bought_token.clone(),
        }
    }

    pub fn save_db(&self) {
        if let Ok(conn) = self.db_conn.try_lock() {
            let set = self.get_db_settings();
            let _ = db::save_settings(&conn, &set);
        }
    }

    /// Sync buy_presets vec from the preset_* fields
    pub fn sync_presets(&mut self) {
        self.buy_presets = vec![
            TxPreset::new("Kecil", self.preset_kecil_tip, self.preset_kecil_prio),
            TxPreset::new("Sedang", self.preset_sedang_tip, self.preset_sedang_prio),
            TxPreset::new("Besar", self.preset_besar_tip, self.preset_besar_prio),
            TxPreset::new("Mega", self.preset_mega_tip, self.preset_mega_prio),
        ];
    }
}

static API_KEY: std::sync::OnceLock<String> = std::sync::OnceLock::new();

fn auth_interceptor(mut request: Request<()>) -> Result<Request<()>, Status> {
    if let Some(key) = API_KEY.get() {
        if let Ok(val) = key.parse::<tonic::metadata::MetadataValue<_>>() {
            request.metadata_mut().insert("auth", val);
        }
    }
    Ok(request)
}

#[tokio::main]
async fn main() {
    // Pasang CryptoProvider rustls 0.23 secara eksplisit SEBELUM TLS dipakai
    // apa pun. Alasan: feature `ring` DAN `aws-lc-rs` sama-sama aktif untuk
    // rustls 0.23 lewat feature unification (reqwest 0.12 bawa aws-lc-rs),
    // sehingga rustls tidak bisa memilih provider otomatis dan panic
    // ("Could not automatically determine the process-level CryptoProvider").
    // install_default() memilih `ring` dan menyelesaikan ambiguitas itu.
    // Abaikan error kalau provider sudah terpasang.
    let _ = rustls::crypto::ring::default_provider().install_default();

    dotenvy::dotenv().ok();
    pretty_env_logger::init();
    info!("Memulai Aura Custom Bot...");

    // Telegram lewat HTTP proxy (VM ini memblokir direct TCP): client reqwest
    // eksplisit dengan proxy dari env, fallback ke direct kalau tidak ada proxy.
    // Token tetap dibaca dari env TELOXIDE_TOKEN seperti sebelumnya.
    // (Client harus reqwest 0.11 karena teloxide-core memakai reqwest 0.11.)
    let token = env::var("TELOXIDE_TOKEN").expect("TELOXIDE_TOKEN tidak diset (cek .env)");
    let bot = Bot::with_client(token, crate::seconddip::build_proxied_telegram_client());
    let api_key = env::var("AURA_API_KEY").unwrap_or_else(|_| "DUMMY_KEY".to_string()).trim().to_string();
    let _ = API_KEY.set(api_key.clone());
    let initial_mode = match env::var("AURA_MODE").unwrap_or_default().to_uppercase().as_str() {
        "MAINNET" => AppMode::Mainnet,
        _ => AppMode::Simulation,
    };

    let quota = Quota::per_second(nonzero!(4u32));
    let limiter = Arc::new(RateLimiter::direct(quota));

    // Setup Aura gRPC Client
    let mut aura_clients_opt = None;
    if api_key != "DUMMY_KEY" {
        let endpoint = Endpoint::from_static("http://trade.aura.rehab:40051")
            .http2_keep_alive_interval(std::time::Duration::from_secs(30))
            .keep_alive_timeout(std::time::Duration::from_secs(10))
            .keep_alive_while_idle(true);
        match endpoint.connect().await {
            Ok(channel) => {
                let interceptor: fn(Request<()>) -> Result<Request<()>, Status> = auth_interceptor;
                let clients = AuraClients::<_, NoCtx>::new(channel, interceptor);
                aura_clients_opt = Some(clients);
                info!("Berhasil terhubung ke Aura gRPC (trade.aura.rehab:40051)");
            }
            Err(e) => {
                error!("Gagal koneksi ke gRPC Aura: {:?}", e);
            }
        }
    }

    // Initialize Database
    let conn = init_db().expect("Gagal inisialisasi SQLite database");
    let loaded_settings = load_settings(&conn).unwrap_or_default();
    
    // Pre-populate active_chats dari env var TELEGRAM_CHAT_ID dan Database
    let mut initial_chats = load_chats(&conn).unwrap_or_default();
    if let Ok(chat_id_str) = env::var("TELEGRAM_CHAT_ID") {
        for part in chat_id_str.split(',') {
            if let Ok(id) = part.trim().parse::<i64>() {
                initial_chats.insert(teloxide::types::ChatId(id));
                let _ = db::save_chat(&conn, teloxide::types::ChatId(id));
                info!("Chat ID {} ditambahkan dari env TELEGRAM_CHAT_ID", id);
            }
        }
    }

    // Daftar wallet target Second Dip (init_db sudah memigrasi wallet lama
    // dari kolom settings ke tabel second_dip_targets).
    let initial_targets = db::load_second_dip_targets(&conn)
        .unwrap_or_else(|_| vec![loaded_settings.second_dip_target_wallet.clone()]);

    let mut st = BotState {
        aura_api_key: api_key,
        mode: initial_mode,
        aura_client: aura_clients_opt.clone(),
        limiter,
        db_conn: Arc::new(Mutex::new(conn)),
        edit_field: EditField::None,
        second_dip_active: loaded_settings.second_dip_active,
        second_dip_targets: initial_targets,
        second_dip_trigger1_pct: loaded_settings.second_dip_trigger1_pct,
        second_dip_trigger1_mode: loaded_settings.second_dip_trigger1_mode,
        second_dip_trigger1_usd: loaded_settings.second_dip_trigger1_usd,
        second_dip_trigger2_pct: loaded_settings.second_dip_trigger2_pct,
        second_dip_trigger1_timeout_mins: loaded_settings.second_dip_trigger1_timeout_mins,
        second_dip_trigger2_timeout_mins: loaded_settings.second_dip_trigger2_timeout_mins,
        second_dip_trigger2_mode: loaded_settings.second_dip_trigger2_mode,
        second_dip_trigger2_usd: loaded_settings.second_dip_trigger2_usd,
        second_dip_trigger2_mcap: loaded_settings.second_dip_trigger2_mcap,
        second_dip_tip: loaded_settings.second_dip_tip,
        second_dip_prio: loaded_settings.second_dip_prio,
        second_dip_amount_usd: loaded_settings.second_dip_amount_usd,
        dip_mode: loaded_settings.dip_mode,
        second_dip_dry_run: loaded_settings.second_dip_dry_run,
        auto_sell_active: loaded_settings.auto_sell_active,
        auto_sell_tp_mode: loaded_settings.auto_sell_tp_mode,
        auto_sell_tp_pct: loaded_settings.auto_sell_tp_pct,
        auto_sell_tp_usd: loaded_settings.auto_sell_tp_usd,
        auto_sell_tp_mcap: loaded_settings.auto_sell_tp_mcap,
        auto_sell_sell_pct: loaded_settings.auto_sell_sell_pct,
        auto_sell_sd_active: loaded_settings.auto_sell_sd_active,
        auto_sell_sd_tp_mode: loaded_settings.auto_sell_sd_tp_mode,
        auto_sell_sd_tp_pct: loaded_settings.auto_sell_sd_tp_pct,
        auto_sell_sd_tp_usd: loaded_settings.auto_sell_sd_tp_usd,
        auto_sell_sd_tp_mcap: loaded_settings.auto_sell_sd_tp_mcap,
        auto_sell_sd_sell_pct: loaded_settings.auto_sell_sd_sell_pct,
        auto_sell_tip: loaded_settings.auto_sell_tip,
        auto_sell_prio: loaded_settings.auto_sell_prio,
        auto_sell_sd_tip: loaded_settings.auto_sell_sd_tip,
        auto_sell_sd_prio: loaded_settings.auto_sell_sd_prio,
        second_dip_target: None,
        second_dip_wss_connected: false,
        second_dip_wss_ping_ms: None,
        second_dip_aura_ping_ms: None,
        sd_trigger1_msg: None,
        sd_trigger2_msg: None,
        as_setup_msg: None,
        limitbuy_msg: None,
        orders_msg: None,
        hist_detail_msg: None,
        pending_hist_edit: None,
        sd_prompt_msg: None,
        sell_tip_fee: loaded_settings.sell_tip_fee,
        sell_prio_fee: loaded_settings.sell_prio_fee,
        sell_slippage: loaded_settings.sell_slippage,
        preset_kecil_tip: loaded_settings.preset_kecil_tip,
        preset_kecil_prio: loaded_settings.preset_kecil_prio,
        preset_sedang_tip: loaded_settings.preset_sedang_tip,
        preset_sedang_prio: loaded_settings.preset_sedang_prio,
        preset_besar_tip: loaded_settings.preset_besar_tip,
        preset_besar_prio: loaded_settings.preset_besar_prio,
        preset_mega_tip: loaded_settings.preset_mega_tip,
        preset_mega_prio: loaded_settings.preset_mega_prio,
        active_token: None,
        active_token_name: None,
        active_token_price: None,
        active_token_liquidity: None,
        buy_amount: 2.0,
        buy_currency: "USD".to_string(),
        buy_target: "50 Mcap".to_string(),
        buy_tip_fee: 0.001,
        buy_prio_fee: 0.001,
        buy_presets: Vec::new(),
        buy_active_preset: ActivePreset::None,
        orders: Vec::new(),
        next_order_id: 1,
        active_chats: initial_chats,
        swap_panel_msgs: Vec::new(),
        processed_signatures: std::collections::HashSet::new(),
        bought_token: loaded_settings.bought_token,
        buy_mode: "LIMIT".to_string(),
    };
    st.sync_presets();
    let state = Arc::new(Mutex::new(st));

    // Start UserActivity Stream and Ping if client is available
    if let Some(clients) = aura_clients_opt {
        let clients_ping = clients.clone();

        // Shared flag: ping dimulai HANYA setelah stream user_activity tersambung
        let stream_ready = Arc::new(AtomicBool::new(false));
        let stream_ready_ping = stream_ready.clone();

        // 1. Ping Loop — WAJIB setiap 10 detik per README Aura agar stream tetap hidup
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            use aura_api_client::types::Ping as AuraPing;
            let mut ping_count: u64 = 0;
            let mut consecutive_failures: u32 = 0;
            loop {
                interval.tick().await;

                // Tunggu sampai stream user_activity sudah tersambung
                if !stream_ready_ping.load(Ordering::Relaxed) {
                    info!("⏳ Menunggu UserActivity stream tersambung sebelum ping...");
                    continue;
                }

                ping_count += 1;
                let req = Request::new(AuraPing { count: ping_count });
                // Rate limiter: jangan tabrakan dengan panggilan Aura lain.
                aura_slot_wait().await;
                match clients_ping.aura().user_ping((), req).await {
                    Ok(_) => {
                        consecutive_failures = 0;
                        if ping_count == 1 {
                            info!("✅ Ping pertama ke server Aura berhasil! Stream aktif.");
                        }
                    }
                    Err(e) => {
                        consecutive_failures += 1;
                        error!("❌ Gagal ping Aura (#{ping_count}): {:?}", e);
                        // Jika gagal >3x berturut, reset flag stream agar listener bisa reconnect
                        if consecutive_failures >= 3 {
                            error!("⚠️ Ping gagal {} kali berturut — reset stream_ready flag", consecutive_failures);
                            stream_ready_ping.store(false, Ordering::Relaxed);
                            consecutive_failures = 0;
                        }
                    }
                }
            }
        });

        // 1b. SOL Price Refresher — ambil harga SOL tiap 1 jam (perintah user
        // 2026-11-21) agar harga yang dipakai bot untuk membagi harga token
        // selalu akurat (cache tidak lebih tua dari 1 jam, walau bot sedang idle).
        // Tick pertama langsung jalan saat startup (cache langsung hangat).
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600));
            loop {
                interval.tick().await;
                match fetch_sol_price_usd().await {
                    Some(p) => info!("[SOL Price] Refresh berkala (1 jam): {} USD", p),
                    None => error!("[SOL Price] Refresh berkala gagal (belum ada cache)"),
                }
            }
        });

        // 2. UserActivity Stream Listener

        let st_clone = state.clone();
        let bot_clone = bot.clone();
        let stream_ready_listener = stream_ready.clone();
        tokio::spawn(async move {
            loop {
                info!("Menyambungkan UserActivity Stream...");
                let req = Request::new(UserActionEventSub {});
                match clients.aura().user_activity((), req).await {
                    Ok(resp) => {
                        let mut stream = resp.into_inner();
                        info!("UserActivity Stream tersambung!");
                        // Aktifkan flag → ping loop bisa mulai
                        stream_ready_listener.store(true, Ordering::Relaxed);
                        
                        // sig -> (token, aura_order_id, tip_sol, prio_sol).
                        // tip/prio disimpan supaya saat buy limit order terisi
                        // (TradeCallback Confirmed), Auto Sell bisa melacak
                        // order itu (termasuk yang dipasang dari bot resmi
                        // Aura) — perintah user 2026-11-22.
                        let mut pending_limit_sigs: std::collections::HashMap<String, (String, i64, f64, f64)> = std::collections::HashMap::new();
                        let mut pending_swap_sigs: std::collections::HashMap<String, String> = std::collections::HashMap::new();

                        while let Some(msg) = stream.next().await {
                            match msg {
                                Ok(action) => {
                                    use aura_api_client::types::{
                                        UserAction, TradeStateUpdate, TxnConfirmState,
                                        ParsedTradeUi, ConfirmTradeKind,
                                    };

                                    // ── Pong/Ping: log saja, skip ──
                                    if matches!(action, UserAction::Pong(_) | UserAction::Ping(_)) {
                                        continue;
                                    }

                                    // ── SELALU log ke server (bukan Telegram) ──
                                    info!("[Aura Stream] {:?}", action);

                                    // ── Deteksi Trade Triggered ──
                                    if let UserAction::TradeEvent(confirm_state) = &action {
                                        match &confirm_state.confirm_trade_kind {
                                            ConfirmTradeKind::LimitOrder { token, order } => {
                                                let aura_id = if let aura_api_client::types::OrderState::Placed { id, .. } = &order.state { id.0 } else { 0 };
                                                if aura_id != 0 {
                                                    let token_str = format!("{}", token);
                                                    // Simpan tip/prio order (untuk Auto Sell tracking
                                                    // saat buy terisi — termasuk order dari bot resmi Aura).
                                                    let tip_sol = lamports_to_sol(&order.order.tip);
                                                    let prio_sol = lamports_to_sol(&order.order.fee);
                                                    for sig in &confirm_state.sigs {
                                                        pending_limit_sigs.insert(format!("{}", sig), (token_str.clone(), aura_id, tip_sol, prio_sol));
                                                    }
                                                }
                                            }
                                            ConfirmTradeKind::Swap { token, .. } => {
                                                let token_str = format!("{}", token);
                                                for sig in &confirm_state.sigs {
                                                    pending_swap_sigs.insert(format!("{}", sig), token_str.clone());
                                                }
                                            }
                                            _ => {}
                                        }
                                    }

                                    // ── Deteksi Transaksi Gagal ──
                                    // Hanya simpan ke log DB, JANGAN kirim notif ke Telegram
                                    // Karena engine Aura multi-attempt: gagal 1 attempt bukan berarti order gagal total
                                    if let UserAction::TradeCallback(TradeStateUpdate::Landed { state: TxnConfirmState::Failed { err, .. }, signature }) = &action {
                                        let sig_str = format!("{}", signature);
                                        if let Some((token, aura_id, _, _)) = pending_limit_sigs.remove(&sig_str) {
                                            info!("[Aura Stream] Limit Order Gagal on-chain (1 attempt), AuraID={}, Err={:?} — Disimpan ke log saja.", aura_id, err);
                                            // Bersihkan semua signature lain aura_id ini agar tidak ada notif ganda
                                            pending_limit_sigs.retain(|_, (_, aid, _, _)| *aid != aura_id);
                                            let err_msg = format!("{:?}", err);
                                            // Simpan ke DB — akses langsung lewat lock state
                                            let st = st_clone.lock().await;
                                            if let Ok(conn) = st.db_conn.try_lock() {
                                                let mut local_id = aura_id;
                                                if let Ok(orders) = crate::db::load_limit_orders(&conn) {
                                                    if let Some(o) = orders.iter().find(|o| o.aura_order_id == Some(aura_id)) {
                                                        local_id = o.id;
                                                    }
                                                }
                                                // Simpan ke error log DB saja (tidak kirim ke Telegram)
                                                let _ = crate::db::insert_error_log(&conn, local_id, &token, &err_msg);
                                            }
                                            drop(st);
                                        } else if let Some(token) = pending_swap_sigs.remove(&sig_str) {
                                            info!("[Aura Stream] Swap Sell Gagal on-chain (1 attempt), Token={}, Err={:?} — Disimpan ke log saja.", token, err);
                                            let err_msg = format!("{:?}", err);
                                            let db_conn = {
                                                let st = st_clone.lock().await;
                                                st.db_conn.clone()
                                            };
                                            if let Ok(conn) = db_conn.try_lock() {
                                                let _ = crate::db::insert_error_log(&conn, 0, &token, &err_msg);
                                            };
                                        }
                                    }

                                    // ── Deteksi SELL on-chain (auto limit / swap manual) ──
                                    // Ini harus dicek SEBELUM BUY agar tidak tertukar
                                    if let UserAction::TradeCallback(TradeStateUpdate::Landed {
                                        state: TxnConfirmState::Confirmed { trades, .. }, signature
                                    }) = &action {
                                        let has_sell = trades.iter().any(|t| matches!(t, ParsedTradeUi::Sell { .. }));
                                        if has_sell {
                                            let sig_str = format!("{}", signature);
                                            let is_limit = pending_limit_sigs.contains_key(&sig_str);
                                            pending_limit_sigs.remove(&sig_str);
                                            pending_swap_sigs.remove(&sig_str);

                                            let sell_info = trades.iter().find_map(|t| {
                                                if let ParsedTradeUi::Sell { mint, ticker, quote, pnl, .. } = t {
                                                    Some((format!("{}", mint), ticker.clone(), quote.clone(), pnl.clone()))
                                                } else {
                                                    None
                                                }
                                            });

                                            if let Some((mint_str, ticker, quote_val, pnl_val)) = sell_info {
                                                info!("[Aura Stream] Terdeteksi SELL berhasil on-chain! Token={}, is_limit={}", mint_str, is_limit);


                                                let (panel_msgs, chats) = {
                                                    let st = st_clone.lock().await;
                                                    (st.swap_panel_msgs.clone(), st.active_chats.clone())
                                                };

                                                let pnl_str = pnl_val
                                                    .map(|p| {
                                                        let p_f64 = format!("{}", p).parse::<f64>().unwrap_or(1.0);
                                                        format!("{:.2}", (p_f64 - 1.0) * 100.0)
                                                    })
                                                    .unwrap_or_else(|| "N/A".to_string());

                                                // quote bisa USDC (6 desimal) atau SOL (9 desimal).
                                                // Display QuoteLamports sudah dalam satuan aslinya:
                                                // Usdc -> USD, Lamports/Wsol -> SOL. Jangan asumsi SOL.
                                                let (asset_label, amount_val, usd_received) = {
                                                    let val: f64 = format!("{}", quote_val).parse().unwrap_or(0.0);
                                                    match quote_val.quote_kind() {
                                                        decisol::QuoteKind::Usd => ("USDC", val, val),
                                                        decisol::QuoteKind::Sol => {
                                                            let sol_price_now = fetch_sol_price_usd().await.unwrap_or(0.0);
                                                            ("SOL", val, val * sol_price_now)
                                                        }
                                                    }
                                                };

                                                let ticker_display = if ticker.is_empty() {
                                                    let s = &mint_str;
                                                    format!("{}...{}", &s[..6.min(s.len())], &s[s.len().saturating_sub(4)..])
                                                } else {
                                                    ticker.clone()
                                                };

                                                let sold_text = if is_limit {
                                                    format!(
                                                        "✅ *Token Terjual via Limit Order!*\n\n\
                                                        🏦 Token: `{}` ({})\n\
                                                        💰 {} Diterima: `{:.6}` {} (${:.2})\n\
                                                        📈 PNL: `{}%`\n\n\
                                                        _Limit order sell telah dieksekusi oleh Aura._",
                                                        ticker_display, mint_str, asset_label, amount_val, asset_label, usd_received, pnl_str
                                                    )
                                                } else {
                                                    format!(
                                                        "✅ *Terjual via Swap Sell Manual!*\n\n\
                                                        🏦 Token: `{}` ({})\n\
                                                        💰 {} Diterima: `{:.6}` {} (${:.2})\n\
                                                        📈 PNL: `{}%`\n\n\
                                                        _Transaksi swap sell manual berhasil dikonfirmasi di blockchain._",
                                                        ticker_display, mint_str, asset_label, amount_val, asset_label, usd_received, pnl_str
                                                    )
                                                };

                                                for (chat_id, msg_id) in &panel_msgs {
                                                    let _ = bot_clone
                                                        .edit_message_text(*chat_id, *msg_id, &sold_text)
                                                        .await;
                                                }
                                                if panel_msgs.is_empty() {
                                                    for chat in &chats {
                                                        let _ = bot_clone.send_message(*chat, &sold_text).await;
                                                    }
                                                }
                                                {
                                                    let mut st = st_clone.lock().await;
                                                    st.swap_panel_msgs.clear();
                                                    st.bought_token = None;
                                                    st.save_db();
                                                }
                                                continue;
                                            }
                                        }
                                    }

                                    // ── Deteksi event BUY yang presisi berdasarkan enum proto ──
                                    // Hanya menggunakan TradeCallback::Landed agar terpicu 1x saat tx sukses on-chain
                                    // Sekalian ambil harga isi (USD) + nilai quote (USD) dari trade BUY,
                                    // untuk Auto Sell tracking order dari bot resmi Aura.
                                    let (is_buy_confirmed, mint_from_event, txn_sig_opt, buy_price_usd, buy_amount_usd) = match &action {
                                        UserAction::TradeCallback(update) => {
                                            match update {
                                                TradeStateUpdate::Landed { signature, state } => {
                                                    match state {
                                                        TxnConfirmState::Confirmed { trades, .. } => {
                                                            let mut mint_str: Option<String> = None;
                                                            let mut price_usd = 0.0f64;
                                                            let mut amount_usd = 0.0f64;
                                                            for t in trades {
                                                                if let ParsedTradeUi::Buy { mint, quote, price_usd: p, .. } = t {
                                                                    mint_str = Some(format!("{}", mint));
                                                                    price_usd = format!("{}", p).parse::<f64>().unwrap_or(0.0);
                                                                    let qval: f64 = format!("{}", quote).parse().unwrap_or(0.0);
                                                                    amount_usd = match quote.quote_kind() {
                                                                        decisol::QuoteKind::Usd => qval,
                                                                        decisol::QuoteKind::Sol => {
                                                                            let sp = fetch_sol_price_usd().await.unwrap_or(0.0);
                                                                            qval * sp
                                                                        }
                                                                    };
                                                                    break;
                                                                }
                                                            }
                                                            let has_buy = mint_str.is_some();
                                                            (has_buy, mint_str, Some(format!("{}", signature)), price_usd, amount_usd)
                                                        }
                                                        _ => (false, None, None, 0.0, 0.0),
                                                    }
                                                }
                                                TradeStateUpdate::Lost { signatures } => {
                                                    // Cek apakah salah satu signature ini milik limit order kita
                                                    // JANGAN kirim notif ke Telegram - engine Aura memang multi-attempt
                                                    // signature yg Lost belum tentu berarti order gagal
                                                    for sig in signatures {
                                                        let sig_str = format!("{}", sig);
                                                        if let Some((token, aura_id, _, _)) = pending_limit_sigs.remove(&sig_str) {
                                                            info!("[Aura Stream] Limit Order sig Lost (multi-attempt normal) AuraID={} — disimpan ke log saja.", aura_id);
                                                            let err_msg = format!("Signature Lost (multi-attempt): AuraID={}", aura_id);
                                                            // Simpan ke DB — akses langsung lewat lock state
                                                            let st_tmp = st_clone.lock().await;
                                                            if let Ok(conn) = st_tmp.db_conn.try_lock() {
                                                                let mut local_id = aura_id;
                                                                if let Ok(orders) = crate::db::load_limit_orders(&conn) {
                                                                    if let Some(o) = orders.iter().find(|o| o.aura_order_id == Some(aura_id)) {
                                                                        local_id = o.id;
                                                                    }
                                                                }
                                                                // Simpan ke log DB saja - TIDAK kirim notif ke Telegram
                                                                let _ = crate::db::insert_error_log(&conn, local_id, &token, &err_msg);
                                                            }
                                                            drop(st_tmp);
                                                        } else if let Some(token) = pending_swap_sigs.remove(&sig_str) {
                                                            info!("[Aura Stream] Swap Sell sig Lost (multi-attempt normal). Token={} — disimpan ke log saja.", token);
                                                            let err_msg = format!("Swap Sell sig Lost (multi-attempt): Token={}", token);
                                                            let db_conn = {
                                                                let st = st_clone.lock().await;
                                                                st.db_conn.clone()
                                                            };
                                                            if let Ok(conn) = db_conn.try_lock() {
                                                                let _ = crate::db::insert_error_log(&conn, 0, &token, &err_msg);
                                                            };
                                                        }
                                                    }
                                                    (false, None, None, 0.0, 0.0)
                                                }
                                            }
                                        }
                                        _ => (false, None, None, 0.0, 0.0),
                                    };

                                    if is_buy_confirmed {
                                        let sig = txn_sig_opt.unwrap_or_else(|| "unknown".to_string());
                                        // Deduplikasi berdasarkan signature
                                        {
                                            let mut st = st_clone.lock().await;
                                            if st.processed_signatures.contains(&sig) {
                                                continue;
                                            }
                                            st.processed_signatures.insert(sig.clone());
                                        }
                                        
                                        info!("[Aura Stream] Terdeteksi transaksi BUY berhasil! Sig: {}", sig);

                                        // ── AUTO SELL: lacak buy limit order dari stream ──
                                        // Kalau buy ini berasal dari limit order (ada di
                                        // pending_limit_sigs — termasuk yang dipasang dari
                                        // bot resmi Aura, bukan cuma dari Sonic), dan toggle
                                        // Auto Sell manual ON, lacak order ini supaya watcher
                                        // otomatis pasang TP sell sesuai Auto Sell Setup.
                                        // Swap buy TIDAK dilacak (bukan limit order).
                                        // Kalau order sudah dilacak (dipasang dari Sonic),
                                        // maybe_track_auto_sell diam-diam skip (idempotent),
                                        // jadi source manual/second_dip tidak tertimpa.
                                        // (Perintah user 2026-11-22.)
                                        //
                                        // FAST PATH (perintah user 2026-11-22: hapus watcher
                                        // total, jual di detik yang sama saat beli terdeteksi):
                                        // begitu buy terisi, LANGSUNG pasang TP sell di sini
                                        // (~1 detik) via place_tp_now — tidak nunggu tick
                                        // watcher. place_tp_now ambil source dari DB (jadi
                                        // config manual/SD selalu benar) dan diam kalau
                                        // toggle OFF / TP sudah terpasang.
                                        if let Some((_, aura_id, tip_sol, prio_sol)) = pending_limit_sigs.remove(&sig) {
                                            if let Some(mint) = &mint_from_event {
                                                crate::seconddip::maybe_track_auto_sell(
                                                    &st_clone,
                                                    aura_id,
                                                    mint,
                                                    buy_price_usd,
                                                    buy_amount_usd,
                                                    tip_sol,
                                                    prio_sol,
                                                    "manual",
                                                )
                                                .await;
                                                crate::seconddip::place_tp_now(
                                                    &st_clone,
                                                    &bot_clone,
                                                    aura_id,
                                                    mint,
                                                    buy_price_usd,
                                                )
                                                .await;
                                            }
                                        }

                                        // Ambil settings dari state
                                        let (active_token, chats) = {
                                            let mut st = st_clone.lock().await;
                                            if let Some(ref mint) = mint_from_event {
                                                if mint.len() >= 32 {
                                                    st.active_token = Some(mint.clone());
                                                    st.bought_token = Some(mint.clone());
                                                    st.save_db();
                                                }
                                            }
                                            (
                                                st.active_token.clone(),
                                                st.active_chats.clone(),
                                            )
                                        };

                                        let token_display = active_token.as_deref().unwrap_or("Unknown Token");

                                        // Persiapkan Teks Swap Panel (belum dikirim)
                                        let _swap_text = format!(
                                            "🟢 *Pembelian Terdeteksi!*\n\n🏦 Token: `{}`\n\n*Klik tombol di bawah untuk Swap Sell atau lihat PNL:*",
                                            token_display
                                        );
                                        let _swap_keyboard = teloxide::types::InlineKeyboardMarkup::new(vec![
                                            vec![teloxide::types::InlineKeyboardButton::callback(
                                                "🔴 Confirm Swap Sell", "execute_swap_sell",
                                            )],
                                            vec![teloxide::types::InlineKeyboardButton::callback(
                                                "🔄 Refresh PNL", "refresh_pnl",
                                            )],
                                        ]);

                                        // ── Kirim Swap Sell Panel ke Telegram & simpan msg_id ──
                                        let swap_text = format!(
                                            "🟢 *Pembelian Terdeteksi!*\n\n🏦 Token: `{}`\n\n*Klik tombol di bawah untuk Swap Sell atau lihat PNL:*",
                                            token_display
                                        );
                                        let swap_keyboard = teloxide::types::InlineKeyboardMarkup::new(vec![
                                            vec![teloxide::types::InlineKeyboardButton::callback(
                                                "🔴 Confirm Swap Sell", "execute_swap_sell",
                                            )],
                                            vec![teloxide::types::InlineKeyboardButton::callback(
                                                "🔄 Refresh PNL", "refresh_pnl",
                                            )],
                                        ]);

                                        // Kirim panel dan simpan msg_id
                                        let mut new_panels = Vec::new();
                                        for chat in &chats {
                                            if let Ok(sent) = bot_clone
                                                .send_message(*chat, &swap_text)
                                                .reply_markup(swap_keyboard.clone())
                                                .await
                                            {
                                                new_panels.push((*chat, sent.id));
                                            }
                                        }
                                        // Simpan ke state
                                        {
                                            let mut st = st_clone.lock().await;
                                            st.swap_panel_msgs.extend(new_panels.clone());
                                        }

                                    }
                                    // ── Semua event lain: hanya log, TIDAK kirim ke Telegram ──
                                }
                                Err(e) => {
                                    error!("Stream message error: {:?}", e);
                                    break; // keluar untuk reconnect
                                }
                            }
                        }
                        // Stream terputus — nonaktifkan flag agar ping tidak jalan saat reconnect
                        stream_ready_listener.store(false, Ordering::Relaxed);
                        info!("UserActivity Stream terputus, akan reconnect dalam 5 detik...");
                    }
                    Err(e) => {
                        error!("Gagal subscribe UserActivity: {:?}", e);
                        stream_ready_listener.store(false, Ordering::Relaxed);
                    }
                }
                // Tunggu sebelum mencoba koneksi ulang
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        });
    }

    // Second Dip WSS watcher — jalan terus sejak bot start, tapi hanya
    // benar-benar memantau kalau second_dip_active && dip_mode == "SECOND_DIP"
    // (dicek di dalam task, jadi toggle dari Telegram langsung berlaku).
    {
        let bot_sd = bot.clone();
        let state_sd = state.clone();
        tokio::spawn(async move {
            crate::seconddip::run_wss_watcher(bot_sd, state_sd).await;
        });
    }

    // (Auto Sell watcher DIHAPUS TOTAL — perintah user 2026-11-22.
    // TP sell sekarang dipasang langsung dari stream User Activity
    // (place_tp_now) begitu BUY terisi, ~1 detik, tanpa nunggu tick.
    // Sync limit order di bawah tetap jalan (60 detik).)

    // Sync limit order berkala (tiap 60 detik): samakan DB lokal dengan Aura.
    // Order yang dibuat di bot resmi Aura masuk ke Sonic; order yang dihapus
    // di Aura ikut terhapus dari Sonic. Tidak kirim notifikasi — diam-diam saja.
    {
        let state_sy = state.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                let st = state_sy.lock().await;
                let (added, removed) = sync_orders_from_aura(&st).await;
                if added > 0 || removed > 0 {
                    info!(
                        "[Sync Orders] berkala: +{} order baru dari Aura, -{} order dihapus",
                        added, removed
                    );
                }
            }
        });
    }

    // Aura ping — ukur latensi TCP ke server Aura (trade.aura.rehab:40051)
    // tiap 10 detik, disimpan ke second_dip_aura_ping_ms untuk ditampilkan
    // di menu Second Dip. Hanya TCP connect (tanpa gRPC call), jadi aman.
    {
        let state_ap = state.clone();
        tokio::spawn(async move {
            loop {
                let start = std::time::Instant::now();
                let ms = match tokio::net::TcpStream::connect("trade.aura.rehab:40051").await {
                    Ok(stream) => {
                        drop(stream);
                        Some(start.elapsed().as_millis() as u64)
                    }
                    Err(_) => None,
                };
                {
                    let mut st = state_ap.lock().await;
                    st.second_dip_aura_ping_ms = ms;
                }
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            }
        });
    }

    let handler = dptree::entry()
        .branch(Update::filter_message().filter_command::<Command>().endpoint(answer_command))
        .branch(Update::filter_message().filter(|msg: Message| {
            msg.text().is_some() && !msg.text().unwrap().starts_with('/')
        }).endpoint(handle_text_message))
        .branch(Update::filter_callback_query().endpoint(handle_callback));

    Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![state])
        .enable_ctrlc_handler()
        .build()
        .dispatch()
        .await;
}

// ─── Keyboard Builders ────────────────────────────────────────────────────────

fn make_main_menu_keyboard(st: &BotState) -> InlineKeyboardMarkup {
    let mut rows = Vec::new();
    if let Some(token) = &st.bought_token {
        let short = if token.len() >= 10 {
            format!("{}...{}", &token[..6], &token[token.len()-4..])
        } else {
            token.clone()
        };
        rows.push(vec![
            InlineKeyboardButton::callback(format!("🔴 Swap Sell Konfirmasi ({})", short), "show_swap_panel"),
        ]);
    }
    rows.push(vec![
        InlineKeyboardButton::callback("🔄 Swap Sell", "menu_swapsell"),
        InlineKeyboardButton::callback("🎯 Second Dip", "menu_seconddip"),
    ]);
    rows.push(vec![
        InlineKeyboardButton::callback("📋 Limit Order History", "menu_history"),
        InlineKeyboardButton::callback("🎯 Auto Sell Setup", "menu_auto_sell_setup"),
    ]);
    rows.push(vec![
        InlineKeyboardButton::callback("📜 Limit Order Logs", "menu_lo_logs"),
        InlineKeyboardButton::callback("💧 Claim Your SOL", "menu_claim_sol"),
    ]);
    InlineKeyboardMarkup::new(rows)
}

fn make_swapsell_keyboard(st: &BotState) -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![
            InlineKeyboardButton::callback(format!("⚡ Tip | {} SOL", st.sell_tip_fee), "edit_sell_tip"),
            InlineKeyboardButton::callback(format!("⛽ P.Fee | {} SOL", st.sell_prio_fee), "edit_sell_prio"),
        ],
        vec![
            InlineKeyboardButton::callback(format!("🏄‍♂️ Slippage | {}", st.sell_slippage), "edit_sell_slippage"),
            InlineKeyboardButton::callback("🔄 Refresh PNL", "menu_swapsell"),
        ],
        vec![InlineKeyboardButton::callback("<< Back", "menu_main")],
    ])
}

/// Versi publik dari fmt_timeout (dipakai seconddip.rs untuk notifikasi).
pub fn fmt_timeout_short(mins: u64) -> String {
    fmt_timeout(mins)
}

/// Format batas waktu (menit) untuk tombol: 0 = "default", >= 60 tampilkan jam.
fn fmt_timeout(mins: u64) -> String {
    if mins == 0 {
        "default".to_string()
    } else if mins >= 60 && mins % 60 == 0 {
        format!("{} jam", mins / 60)
    } else {
        format!("{} mnt", mins)
    }
}

/// Keyboard submenu Pemicu 1: Harga + Mode, Batas Waktu.
fn make_sd_trigger1_keyboard(st: &BotState) -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            format!("💲 Harga Pemicu 1 | {}", fmt_trigger1_value(st)),
            "edit_sd_trigger1_price",
        )],
        vec![InlineKeyboardButton::callback(
            format!("📊 Mode Harga | {}", st.second_dip_trigger1_mode),
            "cycle_sd_trigger1_mode",
        )],
        vec![InlineKeyboardButton::callback(
            format!(
                "⏰ Batas Waktu | {}",
                fmt_timeout(st.second_dip_trigger1_timeout_mins)
            ),
            "edit_sd_trigger1_timeout",
        )],
        vec![InlineKeyboardButton::callback("<< Back", "menu_seconddip")],
    ])
}

/// Label nilai Pemicu 1 sesuai mode: PCT -> "+100%", USD -> "$0.00001000".
fn fmt_trigger1_value(st: &BotState) -> String {
    match st.second_dip_trigger1_mode.as_str() {
        "USD" => {
            if st.second_dip_trigger1_usd > 0.0 {
                format!("${}", crate::seconddip::fmt_price(st.second_dip_trigger1_usd))
            } else {
                "belum diatur".to_string()
            }
        }
        _ => format!("+{}%", st.second_dip_trigger1_pct),
    }
}

/// Label nilai Pemicu 2 sesuai mode: PCT -> "+20%", USD -> "$0.00000120", MCAP -> "$100K mcap".
fn fmt_trigger2_value(st: &BotState) -> String {
    match st.second_dip_trigger2_mode.as_str() {
        "USD" => {
            if st.second_dip_trigger2_usd > 0.0 {
                format!("${}", crate::seconddip::fmt_price(st.second_dip_trigger2_usd))
            } else {
                "belum diatur".to_string()
            }
        }
        "MCAP" => {
            if st.second_dip_trigger2_mcap > 0.0 {
                format!("${} mcap", fmt_mcap_short(st.second_dip_trigger2_mcap))
            } else {
                "belum diatur".to_string()
            }
        }
        _ => format!("+{}%", st.second_dip_trigger2_pct),
    }
}

/// Format mcap singkat: 100000 -> "100K", 2500000 -> "2.5M".
fn fmt_mcap_short(v: f64) -> String {
    if v >= 1_000_000.0 {
        let m = v / 1_000_000.0;
        if (m * 10.0).fract() == 0.0 {
            format!("{}M", m as u64)
        } else {
            format!("{:.1}M", m)
        }
    } else if v >= 1_000.0 {
        let k = v / 1_000.0;
        if (k * 10.0).fract() == 0.0 {
            format!("{}K", k as u64)
        } else {
            format!("{:.1}K", k)
        }
    } else {
        format!("{}", v as u64)
    }
}

/// Keyboard submenu Pemicu 2 (seperti Aura): Harga + Mode, Tip, Prio, Batas Waktu.
/// Yang fixed (tidak bisa diubah): Slippage 100%, Active/Instant
/// (OrderEventTrigger::Immediate), Fee mode Exact (SwapAmount::Buy lamports).
fn make_sd_trigger2_keyboard(st: &BotState) -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            format!("💲 Harga Pemicu 2 | {}", fmt_trigger2_value(st)),
            "edit_sd_trigger2_price",
        )],
        vec![InlineKeyboardButton::callback(
            format!("📊 Mode Harga | {}", st.second_dip_trigger2_mode),
            "cycle_sd_trigger2_mode",
        )],
        vec![
            InlineKeyboardButton::callback(
                format!("⚡ Tip | {} SOL", st.second_dip_tip),
                "edit_sd_tip",
            ),
            InlineKeyboardButton::callback(
                format!("⛽ Prio | {} SOL", st.second_dip_prio),
                "edit_sd_prio",
            ),
        ],
        vec![InlineKeyboardButton::callback(
            format!("💰 Jumlah | ${:.2}", st.second_dip_amount_usd),
            "edit_sd_amount",
        )],
        vec![InlineKeyboardButton::callback(
            format!(
                "⏰ Batas Waktu | {}",
                fmt_timeout(st.second_dip_trigger2_timeout_mins)
            ),
            "edit_sd_trigger2_timeout",
        )],
        vec![InlineKeyboardButton::callback(
            "🔒 Slippage 100% | Instant | Exact (fixed)",
            "sd_trigger2_fixed_info",
        )],
        vec![InlineKeyboardButton::callback("<< Back", "menu_seconddip")],
    ])
}

/// Teks submenu Pemicu 1.
fn sd_trigger1_menu_text(st: &BotState) -> String {
    match st.second_dip_trigger1_mode.as_str() {
        "USD" => format!(
            "📈 **Pemicu 1**\n\n💲 Harga pemicu: `{}` (absolut USD)\n📊 Mode harga: `USD` (ketuk untuk ganti: % / USD)\n⏰ Batas waktu: `{}`\n\nKalau harga tidak naik ke `{}` dalam {}, target dibatalkan.\n\nMode %: harga pemicu = entry + %. Mode USD: harga pemicu absolut.\n\nPilih yang mau diubah:",
            fmt_trigger1_value(st),
            fmt_timeout(st.second_dip_trigger1_timeout_mins),
            fmt_trigger1_value(st),
            fmt_timeout(st.second_dip_trigger1_timeout_mins)
        ),
        _ => format!(
            "📈 **Pemicu 1**\n\n💲 Harga pemicu: `+{}%` dari entry\n📊 Mode harga: `PCT` (ketuk untuk ganti: % / USD)\n⏰ Batas waktu: `{}`\n\nKalau harga tidak naik `+{}%` dalam {}, target dibatalkan.\n\nMode %: harga pemicu = entry + %. Mode USD: harga pemicu absolut.\n\nPilih yang mau diubah:",
            st.second_dip_trigger1_pct,
            fmt_timeout(st.second_dip_trigger1_timeout_mins),
            st.second_dip_trigger1_pct,
            fmt_timeout(st.second_dip_trigger1_timeout_mins)
        ),
    }
}

/// Teks submenu Pemicu 2.
fn sd_trigger2_menu_text(st: &BotState) -> String {
    format!(
        "🎯 **Pemicu 2** (seperti Aura)\n\n💲 Harga beli: `{}`\n📊 Mode harga: `{}` (ketuk untuk ganti: % / USD / Mcap)\n⚡ Tip: `{}` SOL | ⛽ Prio: `{}` SOL\n💰 Jumlah beli: `${:.2}`\n⏰ Batas waktu order: `{}`\n🔒 Fixed: Slippage 100% | Instant | Fee Exact\n\nMode %: harga beli = entry + %. Mode USD: harga beli absolut. Mode Mcap: order trigger saat mcap menyentuh nilai itu.\n\nPilih yang mau diubah:",
        fmt_trigger2_value(st),
        st.second_dip_trigger2_mode,
        st.second_dip_tip,
        st.second_dip_prio,
        st.second_dip_amount_usd,
        fmt_timeout(st.second_dip_trigger2_timeout_mins)
    )
}

/// Label nilai Take Profit Auto Sell sesuai mode: PCT -> "+40%",
/// USD -> "$0.00000400", MCAP -> "$100K mcap".
/// Format nilai Take Profit untuk SATU config Auto Sell (pure — dipakai
/// config manual maupun Second Dip). Mode %: "+40%". Mode USD: "$0.000004".
/// Mode MCAP: "$100K mcap".
fn fmt_tp_value(mode: &str, usd: f64, mcap: f64, pct: f64) -> String {
    match mode {
        "USD" => {
            if usd > 0.0 {
                format!("${}", crate::seconddip::fmt_price(usd))
            } else {
                "belum diatur".to_string()
            }
        }
        "MCAP" => {
            if mcap > 0.0 {
                format!("${} mcap", fmt_mcap_short(mcap))
            } else {
                "belum diatur".to_string()
            }
        }
        _ => format!("+{}%", pct),
    }
}

/// Nilai TP config LIMIT ORDER BIASA (manual).
fn fmt_auto_sell_tp_value(st: &BotState) -> String {
    fmt_tp_value(
        &st.auto_sell_tp_mode,
        st.auto_sell_tp_usd,
        st.auto_sell_tp_mcap,
        st.auto_sell_tp_pct,
    )
}

/// Nilai TP config SECOND DIP.
fn fmt_auto_sell_sd_tp_value(st: &BotState) -> String {
    fmt_tp_value(
        &st.auto_sell_sd_tp_mode,
        st.auto_sell_sd_tp_usd,
        st.auto_sell_sd_tp_mcap,
        st.auto_sell_sd_tp_pct,
    )
}

/// Label tombol Auto Sell di panel Limit Buy (pure, bisa dites tanpa BotState).
/// ON tampilkan nilai TP-nya; OFF tanpa nilai.
fn auto_sell_toggle_label(name: &str, active: bool, tp_value: &str) -> String {
    if active {
        format!("🎯 Auto Sell {} | 🟢 ON ({})", name, tp_value)
    } else {
        format!("🎯 Auto Sell {} | 🔴 OFF", name)
    }
}

/// Baris pemisah di keyboard Auto Sell Setup (tombol dummy — ditekan tidak
/// berbuat apa-apa). Dipakai sebagai pembatas antar grup tombol, sama seperti
/// garis "-" di teks (perintah user 2026-11-21). Label tombol "Biasa"/"SD"
/// dibiarkan apa adanya; yang jadi pembatas cuma garis ini.
fn as_divider_row() -> Vec<InlineKeyboardButton> {
    vec![InlineKeyboardButton::callback(
        "-------------------------",
        "as_divider",
    )]
}

/// Keyboard halaman Auto Sell Setup — 2 config terpisah (Limit Order Biasa
/// dan Second Dip), ala panel limit order Aura. Tipe fixed Take Profit,
/// slippage fixed 100% (hanya ditampilkan di teks, bukan tombol).
/// Tidak ada tombol ON/OFF di sini (perintah user 2026-11-21): halaman ini
/// hanya untuk setting nilai — Tip, Prio, Trigger, % jual. ON/OFF manual
/// lewat tombol di panel Limit Buy; ON/OFF Second Dip lewat tombol di menu
/// Second Dip (perintah user 2026-11-22).
fn make_auto_sell_setup_keyboard(st: &BotState) -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        // ── Config 1: Limit Order Biasa (manual) ──
        // Toggle ON/OFF Auto Sell Biasa — pindah ke halaman ini
        // (perintah user 2026-11-22): 2 tombol toggle di Setup.
        vec![InlineKeyboardButton::callback(
            auto_sell_toggle_label("Biasa", st.auto_sell_active, &fmt_auto_sell_tp_value(st)),
            "as_toggle_manual",
        )],
        vec![
            InlineKeyboardButton::callback(
                format!("⚡ Tip Biasa | {} SOL", st.auto_sell_tip),
                "as_tip_manual",
            ),
            InlineKeyboardButton::callback(
                format!("⛽ P.Fee Biasa | {} SOL", st.auto_sell_prio),
                "as_prio_manual",
            ),
        ],
        vec![
            InlineKeyboardButton::callback(
                format!("🎯 TP Biasa | {}", fmt_auto_sell_tp_value(st)),
                "as_tp_manual",
            ),
            InlineKeyboardButton::callback(
                format!("📊 Mode | {}", st.auto_sell_tp_mode),
                "as_mode_manual",
            ),
        ],
        vec![InlineKeyboardButton::callback(
            format!("💰 Jual Biasa | {}% posisi", st.auto_sell_sell_pct),
            "as_sellpct_manual",
        )],
        as_divider_row(),
        // ── Config 2: Second Dip ──
        // Toggle ON/OFF Auto Sell Second Dip (pindah ke halaman ini).
        vec![InlineKeyboardButton::callback(
            auto_sell_toggle_label("SD", st.auto_sell_sd_active, &fmt_auto_sell_sd_tp_value(st)),
            "as_toggle_sd",
        )],
        vec![
            InlineKeyboardButton::callback(
                format!("⚡ Tip SD | {} SOL", st.auto_sell_sd_tip),
                "as_tip_sd",
            ),
            InlineKeyboardButton::callback(
                format!("⛽ P.Fee SD | {} SOL", st.auto_sell_sd_prio),
                "as_prio_sd",
            ),
        ],
        vec![
            InlineKeyboardButton::callback(
                format!("🎯 TP SD | {}", fmt_auto_sell_sd_tp_value(st)),
                "as_tp_sd",
            ),
            InlineKeyboardButton::callback(
                format!("📊 Mode SD | {}", st.auto_sell_sd_tp_mode),
                "as_mode_sd",
            ),
        ],
        vec![InlineKeyboardButton::callback(
            format!("💰 Jual SD | {}% posisi", st.auto_sell_sd_sell_pct),
            "as_sellpct_sd",
        )],
        as_divider_row(),
        vec![InlineKeyboardButton::callback("<< Back", "menu_main")],
    ])
}

/// Teks halaman Auto Sell Setup — tiap config dipisah garis "-" (perintah
/// user 2026-11-21) supaya jelas mana Limit Biasa, mana Second Dip.
fn auto_sell_setup_text(st: &BotState) -> String {
    format!(
        "🎯 **Auto Sell Setup**\n\nTipe: `Take Profit` (fixed)\nSlippage: `100%` (fixed)\n\nSetelah limit order BELI terisi, bot otomatis pasang order jual di harga TP — dengan Tip & P.Fee di bawah ini. Order beli & jual tidak pernah aktif bersamaan — jual baru dipasang setelah beli terisi.\n\n📋 **Limit Order Biasa:**\n⚡ Tip: `{} SOL` | ⛽ P.Fee: `{} SOL`\n🎯 Trigger: `{}`\n📊 Mode: `{}`\n💰 Jual: `{}%` posisi\n-------------------------\n\n🎯 **Second Dip:**\n⚡ Tip: `{} SOL` | ⛽ P.Fee: `{} SOL`\n🎯 Trigger: `{}`\n📊 Mode: `{}`\n💰 Jual: `{}%` posisi\n-------------------------\n\nMode %: TP = harga beli + %. Mode USD: harga jual absolut.\n\nPilih yang mau diubah:",
        st.auto_sell_tip,
        st.auto_sell_prio,
        fmt_auto_sell_tp_value(st),
        st.auto_sell_tp_mode,
        st.auto_sell_sell_pct,
        st.auto_sell_sd_tip,
        st.auto_sell_sd_prio,
        fmt_auto_sell_sd_tp_value(st),
        st.auto_sell_sd_tp_mode,
        st.auto_sell_sd_sell_pct
    )
}

/// Update pesan halaman Auto Sell Setup di tempat setelah nilai diubah.
async fn refresh_auto_sell_setup(bot: &Bot, st: &BotState) -> bool {
    let Some((c, m)) = st.as_setup_msg else {
        return false;
    };
    // Fallback keyboard saja kalau teks tidak berubah (Telegram tolak
    // edit_message_text kalau teks sama — label tombol ikut gagal update).
    // Kalau isi + tombol juga sama persis ("message is not modified"),
    // halaman sudah benar — return true, jangan kirim pesan baru.
    match bot
        .edit_message_text(c, m, auto_sell_setup_text(st))
        .reply_markup(make_auto_sell_setup_keyboard(st))
        .await
    {
        Ok(_) => return true,
        Err(e) if is_not_modified_error(&e) => return true,
        Err(_) => {}
    }
    match bot.edit_message_reply_markup(c, m)
        .reply_markup(make_auto_sell_setup_keyboard(st))
        .await
    {
        Ok(_) => true,
        Err(e) if is_not_modified_error(&e) => true,
        Err(_) => false,
    }
}

/// Update pesan submenu Pemicu 1/2 di tempat setelah nilai diubah.
/// Kalau pesan submenu masih tersimpan, edit teks + keyboard-nya di tempat
/// (label tombol ikut ter-update, tidak ada pesan baru). Kalau gagal
/// (pesan dihapus / tidak ada / isi sama), kembalikan false agar pemanggil
/// bisa fallback kirim pesan konfirmasi baru.
async fn refresh_sd_submenu(bot: &Bot, st: &BotState, which: u8) -> bool {
    let (chat_id, msg_id, text, kb) = match which {
        1 => {
            let Some((c, m)) = st.sd_trigger1_msg else {
                return false;
            };
            (
                c,
                m,
                sd_trigger1_menu_text(st),
                make_sd_trigger1_keyboard(st),
            )
        }
        _ => {
            let Some((c, m)) = st.sd_trigger2_msg else {
                return false;
            };
            (
                c,
                m,
                sd_trigger2_menu_text(st),
                make_sd_trigger2_keyboard(st),
            )
        }
    };
    // Fallback keyboard saja kalau teks tidak berubah (Telegram tolak
    // edit_message_text kalau teks sama — label tombol ikut gagal update).
    // Kalau isi + tombol juga sama persis ("message is not modified"),
    // submenu sudah benar — return true, jangan kirim pesan baru.
    match bot
        .edit_message_text(chat_id, msg_id, text)
        .reply_markup(kb.clone())
        .await
    {
        Ok(_) => return true,
        Err(e) if is_not_modified_error(&e) => return true,
        Err(_) => {}
    }
    match bot.edit_message_reply_markup(chat_id, msg_id)
        .reply_markup(kb)
        .await
    {
        Ok(_) => true,
        Err(e) if is_not_modified_error(&e) => true,
        Err(_) => false,
    }
}

/// Hapus pesan prompt "Ketik nilai..." terakhir (kalau ada) setelah nilai
/// berhasil disimpan — supaya chat tidak menumpuk.
async fn delete_sd_prompt(bot: &Bot, st: &mut BotState) {
    if let Some((chat_id, msg_id)) = st.sd_prompt_msg.take() {
        let _ = bot.delete_message(chat_id, msg_id).await;
    }
}

fn make_seconddip_keyboard(st: &BotState) -> InlineKeyboardMarkup {
    let status_text = if st.second_dip_active { "🟢 ON" } else { "🔴 OFF" };
    let aura_text = if st.second_dip_dry_run { "🔴 OFF" } else { "🟢 ON" };
    let mode_buydip = if st.dip_mode == "BUY_DIP" { "✅ Mode: Buy Dip" } else { "Mode: Buy Dip" };
    let mode_seconddip = if st.dip_mode == "SECOND_DIP" { "✅ Mode: Second Dip" } else { "Mode: Second Dip" };
    let wss_label = match (st.second_dip_wss_connected, st.second_dip_wss_ping_ms) {
        (true, Some(ms)) => format!("WSS 🟢 {}ms", ms),
        (true, None) => "WSS 🟢".to_string(),
        (false, _) => "WSS 🔴 Diskonek".to_string(),
    };
    let aura_label = match (st.aura_client.is_some(), st.second_dip_aura_ping_ms) {
        (true, Some(ms)) => format!("Aura 🟢 {}ms", ms),
        (true, None) => "Aura 🟢".to_string(),
        (false, _) => "Aura 🔴 Diskonek".to_string(),
    };
    InlineKeyboardMarkup::new(vec![
        vec![
            InlineKeyboardButton::callback(wss_label, "menu_seconddip"),
            InlineKeyboardButton::callback(aura_label, "menu_seconddip"),
            InlineKeyboardButton::callback("🔄 Refresh", "menu_seconddip"),
        ],
        vec![InlineKeyboardButton::callback(
            format!("🎯 Second Dip | {}", status_text),
            "toggle_seconddip",
        )],
        vec![InlineKeyboardButton::callback(
            format!("🔗 Terhubung ke Aura | {}", aura_text),
            "toggle_seconddip_dryrun",
        )],
        vec![
            InlineKeyboardButton::callback(mode_buydip, "set_dip_mode_buydip"),
            InlineKeyboardButton::callback(mode_seconddip, "set_dip_mode_seconddip"),
        ],
        vec![
            InlineKeyboardButton::callback(format!("📈 Pemicu 1 | {} | ⏰ {} mnt", fmt_trigger1_value(st), fmt_timeout(st.second_dip_trigger1_timeout_mins)), "menu_sd_trigger1"),
            InlineKeyboardButton::callback(format!("🎯 Pemicu 2 | {} | ⏰ {}", fmt_trigger2_value(st), fmt_timeout(st.second_dip_trigger2_timeout_mins)), "menu_sd_trigger2"),
        ],
        // Toggle Auto Sell SD PINDAH ke halaman Auto Sell Setup
        // (perintah user 2026-11-22) — tombol di bawah ini buka Setup.
        vec![InlineKeyboardButton::callback(
            format!(
                "🎯 Auto Sell (TP) | {} | Jual {}%",
                fmt_auto_sell_sd_tp_value(st),
                st.auto_sell_sd_sell_pct
            ),
            "menu_auto_sell_setup",
        )],
        vec![InlineKeyboardButton::callback(format!("🎯 Target Wallet | {}", st.second_dip_targets.len()), "menu_sd_targets")],
        vec![InlineKeyboardButton::callback("<< Back", "menu_main")],
    ])
}

/// Teks status menu Second Dip (ditampilkan di atas keyboard).
fn seconddip_menu_text(st: &BotState) -> String {
    let status = if st.second_dip_active { "🟢 AKTIF" } else { "🔴 NONAKTIF" };
    let mode = if st.dip_mode == "SECOND_DIP" { "Second Dip (copytrade)" } else { "Buy Dip (limit order biasa)" };
    let aura_conn = if st.second_dip_dry_run {
        "🔴 OFF (order TIDAK dikirim ke Aura)"
    } else {
        "🟢 ON (order NYATA ke Aura)"
    };
    let target_count = st.second_dip_targets.len();
    let wss = match (st.second_dip_wss_connected, st.second_dip_wss_ping_ms) {
        (true, Some(ms)) => format!("WSS 🟢 {}ms", ms),
        (true, None) => "WSS 🟢".to_string(),
        (false, _) => "WSS 🔴 Diskonek".to_string(),
    };
    let aura_ping = match (st.aura_client.is_some(), st.second_dip_aura_ping_ms) {
        (true, Some(ms)) => format!("Aura 🟢 {}ms", ms),
        (true, None) => "Aura 🟢".to_string(),
        (false, _) => "Aura 🔴 Diskonek".to_string(),
    };
    let auto_sell = if st.auto_sell_sd_active {
        format!("🟢 ON (TP {}, Jual {}%)", fmt_auto_sell_sd_tp_value(st), st.auto_sell_sd_sell_pct)
    } else {
        "🔴 OFF".to_string()
    };
    let mut text = format!(
        "🎯 **Second Dip**\n\nStatus: {}\n{}\n{}\nMode: {}\n🔗 Terhubung ke Aura: {}\n🎯 Target Wallet: {}\n📈 Pemicu 1 (buat order): `{}`, batas waktu `{}`\n🎯 Pemicu 2 (harga beli): `{}`, batas waktu `{}`\n🎯 Auto Sell (TP): {}\n\n",
        status, wss, aura_ping, mode, aura_conn, target_count, fmt_trigger1_value(st),
        fmt_timeout(st.second_dip_trigger1_timeout_mins), fmt_trigger2_value(st),
        fmt_timeout(st.second_dip_trigger2_timeout_mins), auto_sell
    );
    if let Some(t) = &st.second_dip_target {
        text.push_str(&format!(
            "📌 *Target aktif:* `{}`\nEntry: `${}`\n",
            crate::seconddip::short_addr(&t.mint),
            crate::seconddip::fmt_price(t.entry_price)
        ));
    } else {
        text.push_str("_Belum ada target aktif. Bot memantau BUY dari wallet target..._");
    }
    text.push_str("\nKlik tombol lalu ketik nilainya.");
    text
}

/// Teks halaman daftar wallet target Second Dip.
fn sd_targets_text(targets: &[String]) -> String {
    let mut text = format!(
        "🎯 *Target Wallet Second Dip* ({}/{})\n\n",
        targets.len(),
        db::MAX_SECOND_DIP_TARGETS
    );
    if targets.is_empty() {
        text.push_str("_Belum ada wallet target. Tambahkan minimal 1 wallet supaya bot bisa memantau._");
    } else {
        for (i, w) in targets.iter().enumerate() {
            text.push_str(&format!("{}. `{}`\n", i + 1, crate::seconddip::short_addr(w)));
        }
        text.push_str("\n_Klik ❌ Hapus untuk mengeluarkan wallet dari daftar._");
    }
    text
}

/// Keyboard halaman daftar wallet target: hapus per wallet + tambah + kembali.
fn make_sd_targets_keyboard(targets: &[String]) -> InlineKeyboardMarkup {
    let mut rows: Vec<Vec<InlineKeyboardButton>> = Vec::new();
    for w in targets {
        rows.push(vec![InlineKeyboardButton::callback(
            format!("❌ Hapus {}", crate::seconddip::short_addr(w)),
            format!("sd_remove_target_{}", w),
        )]);
    }
    rows.push(vec![InlineKeyboardButton::callback(
        "➕ Tambah Wallet",
        "sd_add_target",
    )]);
    rows.push(vec![InlineKeyboardButton::callback(
        "<< Back",
        "menu_seconddip",
    )]);
    InlineKeyboardMarkup::new(rows)
}

fn make_limitbuy_keyboard(st: &BotState) -> InlineKeyboardMarkup {
    // ── Tip & Prio aktif (dari preset aktif atau manual) ────────────────────────
    let tip_label = format!("⚡ Tip | {} SOL", st.buy_tip_fee);
    let prio_label = format!("⛽ P.Fee | {} SOL", st.buy_prio_fee);

    // ── Target unified label ─────────────────────────────────────────────────────
    let target_label = if st.buy_target.is_empty() {
        "🎯 Target | (belum diset)".to_string()
    } else {
        format!("🎯 Target | {}", format_target_display(&st.buy_target))
    };

    let mut rows: Vec<Vec<InlineKeyboardButton>> = vec![];

    // Row manual Tip & Prio
    rows.push(vec![
        InlineKeyboardButton::callback(tip_label, "edit_buy_tip"),
        InlineKeyboardButton::callback(prio_label, "edit_buy_prio"),
    ]);

    // ── Row preset Kecil/Sedang/Besar/Mega: 4 tombol sejajar 1 baris, 2 baris tiap tombol ──
    // Telegram tidak support newline di button text, jadi format: "Kecil\nT:x P:y"
    // Kita bikin 4 tombol 1 baris saja
    let mut preset_row: Vec<InlineKeyboardButton> = vec![];
    for (i, p) in st.buy_presets.iter().enumerate() {
        let active = st.buy_active_preset == ActivePreset::Idx(i);
        let check = if active { "✅" } else { "" };
        let label = format!("{}{}\nT:{} P:{}", check, p.label, p.tip, p.prio);
        preset_row.push(InlineKeyboardButton::callback(label, format!("preset_select_{}", i)));
    }
    rows.push(preset_row);

    // ── Row edit nilai preset yang aktif (Tip & Prio-nya) ───────────────────────
    if let ActivePreset::Idx(idx) = &st.buy_active_preset {
        let p = &st.buy_presets[*idx];
        rows.push(vec![
            InlineKeyboardButton::callback(
                format!("✏️ {} Tip | {} SOL", p.label, p.tip),
                format!("preset_edit_tip_{}", idx),
            ),
            InlineKeyboardButton::callback(
                format!("✏️ {} Prio | {} SOL", p.label, p.prio),
                format!("preset_edit_prio_{}", idx),
            ),
        ]);
    }

    // Row Slippage
    rows.push(vec![InlineKeyboardButton::callback("🏄‍♂️ Slippage | 90%", "none")]);
    // Row Mode Toggle
    let mode_label = format!("🔄 Mode | {}", if st.buy_mode == "SWAP" { "Swap" } else { "Limit Order" });
    rows.push(vec![InlineKeyboardButton::callback(mode_label, "toggle_buy_mode")]);
    // Row Amount + Currency Toggle
    let currency_label = if st.buy_currency == "SOL" { "SOL" } else { "$" };
    rows.push(vec![
        InlineKeyboardButton::callback(format!("💰 {:.4} {}", st.buy_amount, currency_label), "edit_buy_amount"),
        InlineKeyboardButton::callback(if st.buy_currency == "SOL" { "🔀 Ganti ke USD" } else { "🔀 Ganti ke SOL" }, "toggle_buy_currency"),
    ]);
    // Row Target + Execute/Place based on mode
    if st.buy_mode == "SWAP" {
        rows.push(vec![InlineKeyboardButton::callback("⚡ Execute Swap ⚡", "place_swap_buy")]);
    } else {
        rows.push(vec![InlineKeyboardButton::callback(target_label, "edit_buy_target")]);
        // Toggle Auto Sell PINDAH ke halaman Auto Sell Setup
        // (perintah user 2026-11-22) — tidak ada lagi di panel ini.
        rows.push(vec![InlineKeyboardButton::callback("📥 Place Order 📥", "place_limit_buy")]);
    }
    // Row Back
    rows.push(vec![InlineKeyboardButton::callback("<< Back", "menu_main")]);

    InlineKeyboardMarkup::new(rows)
}

/// Format harga untuk tampilan singkat (subscript zeros):
/// 0.0000005$ → 0.0₆5$  (subscript digit menunjukkan jumlah nol)
fn format_price_short(price_str: &str) -> String {
    let s = price_str.trim().trim_end_matches('$').trim();
    if let Ok(val) = s.parse::<f64>() {
        if val > 0.0 && val < 1.0 {
            let after_dot = s.splitn(2, '.').nth(1).unwrap_or("");
            let leading_zeros = after_dot.chars().take_while(|&c| c == '0').count();
            if leading_zeros >= 2 {
                let sig = after_dot.trim_start_matches('0').trim_end_matches('0');
                let sub = to_subscript(leading_zeros);
                return format!("0.0{}{}$", sub, sig);
            }
        }
        return format!("{}$", s);
    }
    price_str.to_string()
}

/// Parse target string ke Aura Target enum. Membutuhkan sol_price_usd untuk konversi harga USD.
fn parse_target_str_to_aura(
    target_str: &str,
    sol_price_usd: f64,
    order_type: &str,
) -> Option<aura_api_client::types::Target> {
    use aura_api_client::types::{Direction, MarketExecuteMode, Target};
    let s = target_str.trim();
    let lower = s.to_lowercase();
    
    let default_dir = if order_type.eq_ignore_ascii_case("SELL") { Direction::Above } else { Direction::Below };

    // Market: "Market" -> Target::Market (mode Always) — order limit yang
    // tereksekusi langsung di harga pasar (tipe Market di bot resmi Aura).
    if lower == "market" {
        return Some(Target::Market { mode: MarketExecuteMode::Always });
    }

    // PNL: format "PNL 60%" / "60%pnl" / "70%pnl" → Target::Profit
    if lower.contains("pnl") {
        let num_part: String = s.chars().filter(|&c| c == '-' || c == '.' || c.is_ascii_digit()).collect();
        if let Ok(pnl_f64) = num_part.parse::<f64>() {
            let mult = (1.0 + pnl_f64 / 100.0).max(0.0);
            let pnl_scaled = (mult * 1_000_000f64) as u64;
            let profit_perc = fastnum::UD128::from(pnl_scaled) / fastnum::UD128::from(1_000_000u64);
            let direction = if pnl_f64 < 0.0 { Direction::Below } else { Direction::Above };
            return Some(Target::Profit {
                init_profit_perc: profit_perc,
                recalced_profit: None,
                direction,
            });
        }
        return None;
    }

    if s.contains('%') {
        // PricePerc: e.g. "-20%" beli kalau harga turun 20%, "80%" naik 80%
        let num_part: String = s.chars().filter(|&c| c == '-' || c == '.' || c.is_ascii_digit()).collect();
        if let Ok(perc_f64) = num_part.parse::<f64>() {
            let mult = (1.0 + perc_f64 / 100.0).max(0.0);
            let scaled = (mult * 1_000_000.0) as u64;
            let price_perc = fastnum::UD128::from(scaled) / fastnum::UD128::from(1_000_000u64);
            let direction = if perc_f64 < 0.0 { Direction::Below } else { Direction::Above };
            return Some(Target::PricePerc { price_perc, price: None, direction });
        }
    } else if lower.contains("mcap") || lower.contains("cap") || lower.contains('k') || lower.contains('m') {
        // Mcap target (USD): e.g. "100K mcap", "11M mcap", "2.35M mcap", "100000 mcap"
        let num_str: String = s.chars().filter(|&c| c == '.' || c.is_ascii_digit()).collect();
        let multiplier: f64 = if lower.contains('k') { 1_000.0 }
            else if lower.contains('m') { 1_000_000.0 }
            else { 1.0 };
        if let Ok(val) = num_str.parse::<f64>() {
            let mcap_val = val * multiplier;
            let scaled = (mcap_val * 1_000_000.0) as u64;
            let mcap_ud128 = fastnum::UD128::from(scaled) / fastnum::UD128::from(1_000_000u64);
            return Some(Target::Mcap { mcap: mcap_ud128, price: None, direction: default_dir });
        }
    } else {
        // Price USD — dibagi harga SOL dulu, jadi UD128 = harga SOL/token.
        // Perintah user 2026-11-21: kirim apa adanya tanpa kali/bagi TIDAK
        // berlaku untuk tampilan Aura — Aura menampilkan UD128 x harga SOL,
        // jadi supaya tampilannya sama dengan harga yang dimasukkan, UD128
        // harus = USD / harga SOL.
        let stripped = s.trim_start_matches('$').trim_end_matches('$').trim();
        if let Ok(price_usd) = stripped.parse::<f64>() {
            if price_usd > 0.0 && sol_price_usd > 0.0 {
                let price_sol = price_usd / sol_price_usd;
                let scale: u64 = 1_000_000_000_000;
                let scaled = (price_sol * scale as f64) as u64;
                let price_ud128 = fastnum::UD128::from(scaled) / fastnum::UD128::from(scale);
                return Some(Target::Price { price: price_ud128, direction: default_dir });
            }
        }
    }
    None
}

/// Cache harga SOL: (harga USD, waktu fetch). TTL 1 jam (perintah user 2026-11-21):
/// harga SOL yang dipakai bot untuk membagi harga token harus selalu akurat
/// (tidak lebih tua dari 1 jam). Kalau fetch baru gagal, cache lama dipakai
/// (walau kadaluarsa) daripada None/0.0.
static SOL_PRICE_CACHE: std::sync::Mutex<Option<(f64, std::time::Instant)>> =
    std::sync::Mutex::new(None);
const SOL_PRICE_TTL: std::time::Duration = std::time::Duration::from_secs(3600);

/// FAKTOR SEMENTARA (pengujian, perintah user 2026-11-22): harga SOL yang
/// dipakai Sonic dikali faktor ini supaya sama dengan harga SOL yang dipakai
/// Aura. Dihitung dari order hSOL 322696: user pasang target $1 di Sonic,
/// Aura menampilkan 1.0275.
///   UD128 Sonic = 1.0 / 70.30 = 0.0142247 SOL (Binance $70.30)
///   Tampilan Aura = UD128 x SOL_Aura = 1.0275  ->  SOL_Aura = $72.24
///   Faktor = 72.24 / 70.30 = 1.0275
/// HAPUS / ganti dengan fetch harga SOL Aura yang sebenarnya setelah pengujian
/// selesai — faktor ini snapshot, akan meleset kalau harga SOL bergerak jauh.
const SOL_PRICE_AURA_FACTOR: f64 = 1.0275;

/// Cek apakah cache harga SOL masih segar (< TTL 1 jam).
fn sol_price_cache_fresh(cached_at: std::time::Instant, now: std::time::Instant) -> bool {
    now.duration_since(cached_at) < SOL_PRICE_TTL
}

/// Parse harga SOL dari respons Binance `{"price": "67.81"}`.
fn parse_sol_price_binance(json: &serde_json::Value) -> Option<f64> {
    let price_str = json["price"].as_str()?;
    let price = price_str.parse::<f64>().ok()?;
    if price > 0.0 { Some(price) } else { None }
}

/// Parse harga SOL dari respons CoinGecko `{"solana": {"usd": 67.81}}`.
fn parse_sol_price_coingecko(json: &serde_json::Value) -> Option<f64> {
    let price = json["solana"]["usd"].as_f64()?;
    if price > 0.0 { Some(price) } else { None }
}

/// Parse harga SOL dari respons Dexscreener (harga wSOL, `pairs[].priceUsd`).
fn parse_sol_price_dexscreener(json: &serde_json::Value) -> Option<f64> {
    let pairs = json["pairs"].as_array()?;
    for pair in pairs {
        if let Some(price_str) = pair["priceUsd"].as_str() {
            if let Ok(price) = price_str.parse::<f64>() {
                if price > 0.0 {
                    return Some(price);
                }
            }
        }
    }
    None
}

/// Fetch harga SOL dalam USD dari Binance API.
async fn fetch_sol_price_binance(client: &reqwest::Client) -> Option<f64> {
    let url = "https://api.binance.com/api/v3/ticker/price?symbol=SOLUSDT";
    match client.get(url).send().await {
        Ok(resp) => {
            if let Ok(json) = resp.json::<serde_json::Value>().await {
                if let Some(price) = parse_sol_price_binance(&json) {
                    return Some(price);
                }
            }
            error!("[SOL Price] Gagal parse respons Binance API");
            None
        }
        Err(e) => {
            error!("[SOL Price] Gagal fetch Binance API: {}", e);
            None
        }
    }
}

/// Fetch harga SOL dalam USD dari CoinGecko (fallback kalau Binance gagal).
async fn fetch_sol_price_coingecko(client: &reqwest::Client) -> Option<f64> {
    let url = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd";
    match client.get(url).send().await {
        Ok(resp) => {
            if let Ok(json) = resp.json::<serde_json::Value>().await {
                if let Some(price) = parse_sol_price_coingecko(&json) {
                    return Some(price);
                }
            }
            error!("[SOL Price] Gagal parse respons CoinGecko API");
            None
        }
        Err(e) => {
            error!("[SOL Price] Gagal fetch CoinGecko API: {}", e);
            None
        }
    }
}

/// Fetch harga SOL dalam USD dari Dexscreener (fallback terakhir; harga wSOL).
async fn fetch_sol_price_dexscreener(client: &reqwest::Client) -> Option<f64> {
    let url = "https://api.dexscreener.com/latest/dex/tokens/So11111111111111111111111111111111111111112";
    match client.get(url).send().await {
        Ok(resp) => {
            if let Ok(json) = resp.json::<serde_json::Value>().await {
                if let Some(price) = parse_sol_price_dexscreener(&json) {
                    return Some(price);
                }
            }
            error!("[SOL Price] Gagal parse respons Dexscreener API");
            None
        }
        Err(e) => {
            error!("[SOL Price] Gagal fetch Dexscreener API: {}", e);
            None
        }
    }
}

/// Rate limiter Aura (2026-11-22).
/// Aura membatasi 4 request/detik per API key DAN per IP. Bot tidak punya
/// pembatas sisi klien, jadi kalau beberapa hal jalan bareng dalam 1 detik
/// (pasang TP + ping + sync) bisa tembus limit dan order gagal — terbukti
/// 2026-11-22 09:09:06 (order 322888, TP gagal "per-second rate limit").
/// Helper ini memastikan jeda min 350ms antar panggilan gRPC Aura
/// (maks ~2.8 RPS, aman di bawah 4 RPS). Dipanggil sebelum tiap panggilan
/// gRPC Aura yang jalan otomatis (TP, buy, ping, sync).
static AURA_LAST_CALL: std::sync::OnceLock<tokio::sync::Mutex<Option<std::time::Instant>>> =
    std::sync::OnceLock::new();
const AURA_MIN_INTERVAL: std::time::Duration = std::time::Duration::from_millis(350);

pub async fn aura_slot_wait() {
    let m = AURA_LAST_CALL.get_or_init(|| tokio::sync::Mutex::new(None));
    let mut last = m.lock().await;
    if let Some(prev) = *last {
        let elapsed = prev.elapsed();
        if elapsed < AURA_MIN_INTERVAL {
            tokio::time::sleep(AURA_MIN_INTERVAL - elapsed).await;
        }
    }
    *last = Some(std::time::Instant::now());
}

/// Fetch harga SOL dalam USD.
/// Urutan sumber (perintah user 2026-11-21): Binance -> CoinGecko -> Dexscreener.
/// Cache 1 jam: kalau cache masih segar, langsung dipakai tanpa fetch baru.
/// Kalau semua sumber gagal, dipakai cache lama (walau kadaluarsa) daripada None.
/// Return Some(price) jika berhasil, None jika gagal dan belum pernah ada cache.
async fn fetch_sol_price_usd() -> Option<f64> {
    // 1. Cek cache: kalau masih segar (< 1 jam), pakai cache.
    {
        let cache = SOL_PRICE_CACHE.lock().unwrap();
        if let Some((price, fetched_at)) = *cache {
            if sol_price_cache_fresh(fetched_at, std::time::Instant::now()) {
                return Some(price);
            }
        }
    }
    // 2. Cache kadaluarsa / kosong: coba sumber berurutan.
    // Pakai client ber-proxy (Binance juga kena blokir direct TCP di VM ini).
    let client = crate::seconddip::build_proxied_http_client();
    let mut source = "";
    let price = if let Some(p) = fetch_sol_price_binance(&client).await {
        source = "Binance";
        Some(p)
    } else if let Some(p) = fetch_sol_price_coingecko(&client).await {
        source = "CoinGecko";
        Some(p)
    } else if let Some(p) = fetch_sol_price_dexscreener(&client).await {
        source = "Dexscreener";
        Some(p)
    } else {
        None
    };
    match price {
        Some(p) => {
            // SEMENTARA (pengujian): samakan dengan harga SOL Aura (x1.0275).
            let p_aura = p * SOL_PRICE_AURA_FACTOR;
            info!("[SOL Price] Fetched from {}: {} USD -> Aura-adjusted: {} USD (x{})", source, p, p_aura, SOL_PRICE_AURA_FACTOR);
            let mut cache = SOL_PRICE_CACHE.lock().unwrap();
            *cache = Some((p_aura, std::time::Instant::now()));
            Some(p_aura)
        }
        None => {
            error!("[SOL Price] Semua sumber gagal (Binance, CoinGecko, Dexscreener)");
            // Fallback ke cache lama (walau kadaluarsa) daripada None.
            let cache = SOL_PRICE_CACHE.lock().unwrap();
            cache.map(|(price, _)| price)
        }
    }
}

/// Format target unified: detect apakah mcap, price, persen, atau PNL
fn format_target_display(target: &str) -> String {
    let s = target.trim();
    let lower = s.to_lowercase();

    // PNL
    if lower.contains("pnl") {
        return s.to_string();
    }

    // Persen change: mengandung '%'
    if s.contains('%') {
        return s.to_string();
    }

    // McAp: mengandung "mcap", "k", "m", dsb – atau tidak ada '$' dan tidak bisa di-parse sebagai angka kecil
    if lower.contains("mcap") || lower.contains("cap") {
        return s.to_string();
    }

    // Coba parse sebagai price (USD)
    let stripped = s.trim_start_matches('$').trim_end_matches('$').trim();
    if let Ok(val) = stripped.parse::<f64>() {
        if val < 1.0 && val > 0.0 {
            return format_price_short(stripped);
        }
        return format!("{}$", stripped);
    }

    // Fallback: tampilkan apa adanya
    s.to_string()
}

/// Konversi angka ke subscript unicode (untuk notasi jumlah nol)
fn to_subscript(n: usize) -> String {
    let digits = ['₀','₁','₂','₃','₄','₅','₆','₇','₈','₉'];
    n.to_string().chars().map(|c| {
        let d = c.to_digit(10).unwrap_or(0) as usize;
        digits[d]
    }).collect()
}

/// Parse input target universal dari user:
/// - McAp: `100K mcap` | `11M mcap` | `2.35M mcap` | `100000 mcap`
/// - Price: `0.000005$` | `$1` | `0.001$`
/// - Persen: `80%` | `-20%`
/// - Pnl : `70%pnl` | `pnl70%` | `60%pnl`
/// Mengembalikan target_normalized atau None jika tidak valid
fn parse_target_input(input: &str) -> Option<String> {
    let s = input.trim();
    let lower = s.to_lowercase();

    // PNL: format "70%pnl" | "pnl70%" | "60%pnl" | "pnl60%" | "70pnl%" | "pnl 70%" | "70% pnl"
    if lower.contains("pnl") {
        let num_part: String = s.chars().filter(|&c| c == '-' || c == '.' || c.is_ascii_digit()).collect();
        if let Ok(val) = num_part.parse::<f64>() {
            return Some(format!("PNL {}%", val));
        }
        return None;
    }

    // Persen: mengandung '%'
    if lower.contains('%') {
        let num_part: String = s.chars().filter(|&c| c == '-' || c == '.' || c.is_ascii_digit()).collect();
        if let Ok(val) = num_part.parse::<f64>() {
            return Some(format!("{}%", val));
        }
        return None;
    }

    // Price USD: mengandung '$'
    let stripped_dollar = s.trim_start_matches('$').trim_end_matches('$').trim();
    if s.contains('$') {
        if let Ok(val) = stripped_dollar.parse::<f64>() {
            if val > 0.0 {
                return Some(format!("{}$", stripped_dollar));
            }
        }
        return None;
    }

    // McAp: mengandung "mcap", "cap"
    if lower.contains("mcap") || lower.contains("cap") {
        return Some(s.to_string());
    }

    // Angka dengan suffix K/M untuk mcap: "100k", "11m", "2.35m"
    if lower.ends_with('k') || lower.ends_with('m') {
        return Some(format!("{} mcap", s));
    }

    // Coba parse sebagai angka murni: jika < 1 → price, jika besar → mcap
    if let Ok(val) = s.parse::<f64>() {
        if val <= 0.0 { return None; }
        if val < 100.0 {
            // Kemungkinan price USD
            return Some(format!("{}$", val));
        } else {
            // Kemungkinan mcap (angka besar)
            return Some(format!("{} mcap", val));
        }
    }

    None
}



/// Hapus order lama di Aura (by aura_order_id) lalu buat order baru dengan
/// field yang sudah di-update. Menyimpan aura_order_id baru ke DB lokal.
/// Dipakai saat edit Tip/Prio sebuah order. Dijalankan dari `tokio::spawn`.
#[allow(clippy::too_many_arguments)]
async fn recreate_aura_order(
    client: AuraClients<InterceptorFn, NoCtx>,
    db_conn: Arc<Mutex<rusqlite::Connection>>,
    local_id: i64,
    token: String,
    order_type: String,
    target: String,
    tip: f64,
    prio: f64,
    amount_sol: f64,
    old_aura_id: Option<i64>,
    slippage: String,
    bot: Bot,
    chat_id: ChatId,
) {
    use aura_api_client::types::{
        ApiLimitOrder, ApiOrders, OrderEventTrigger, OrderId, OrderState, RawOrder,
        SwapAmount, TxnProcessors, UpdateTokenLimitOrders, UserNonceStrategy,
    };
    use std::str::FromStr;
    let sol_price = fetch_sol_price_usd().await.unwrap_or(0.0);
    let Some(aura_target) = parse_target_str_to_aura(&target, sol_price, &order_type) else {
        let _ = bot.send_message(chat_id, "⚠️ Gagal parse target untuk sync ke Aura.").await;
        return;
    };
    let Ok(mint) = solana_address::Address::from_str(&token) else { return };
    let wallet = {
        let mut tc = client.aura();
        match tc.fetch_full_wallet_info((), tonic::Request::new(aura_api_client::types::FetchFullWalletsInfoReq {})).await {
            Ok(r) => r.into_inner().wallets.get(0).cloned().unwrap_or_default(),
            Err(_) => solana_address::Address::default(),
        }
    };
    let slippage_f64 = slippage.replace("%", "").trim().parse::<f64>().unwrap_or(95.0);
    let slippage_scaled = (slippage_f64 / 100.0 * 1_000_000.0) as u64;
    let slippage_val = fastnum::UD128::from(slippage_scaled) / fastnum::UD128::from(1_000_000u64);
    let tip_lam = (tip * 1e9) as u64;
    let prio_lam = (prio * 1e9) as u64;
    let swap_amount = if order_type.eq_ignore_ascii_case("BUY") {
        // amount_sol di DB sebenarnya USD -> bayar USDC (aturan user: jangan SOL).
        buy_swap_amount(amount_sol, "USD")
    } else {
        SwapAmount::SellPerc { amount: fastnum::udec128!(1) }
    };
    let api_order = ApiLimitOrder {
        state: OrderState::Api { id: None, expire_dur: None, activate_dur: None },
        order: RawOrder {
            slippage: slippage_val,
            tip: decisol::Lamports::from(tip_lam),
            fee: decisol::Lamports::from(prio_lam),
            target: aura_target,
            amount: swap_amount,
            procs: TxnProcessors {
                jito_validators: false, jito_bundled: false, aura: true, bloxroute: false,
                nozomi: false, next_block: false, slot0: false, astra: false,
                block_razor: false, node1: false, tpu_penetrator: false, helius: true,
                stellium: true, soyas: true, falcon: true, raiden: true, circular: true,
                flash_block: true, moon: true, blocksprint: true, aura_revert: false,
                landx: true, manka: true, blockrush: true,
            },
            nonce: UserNonceStrategy::Hybrid,
            slot_latency: 0,
        },
        trigger: OrderEventTrigger::Immediate,
        wallet,
    };
    // Hapus order lama di Aura (kalau ada).
    if let Some(a_id) = old_aura_id {
        let del_req = tonic::Request::new(aura_api_client::types::DeleteOrders {
            mint,
            all: false,
            ids: vec![OrderId(a_id)],
        });
        let _ = client.limit_orders().delete_limit_orders((), del_req).await;
    }
    let req = tonic::Request::new(UpdateTokenLimitOrders {
        mint,
        orders: ApiOrders { orders: vec![api_order] },
    });
    match client.limit_orders().place_limit_orders((), req).await {
        Ok(resp) => {
            if let Some(new_a_id) = resp.into_inner().ids.get(0).copied().map(|x| x.0) {
                if let Ok(conn) = db_conn.try_lock() {
                    let _ = crate::db::update_aura_order_id(&conn, local_id, new_a_id);
                }
            }
            log::info!("[Edit Order] Berhasil sync order #{} ke Aura gRPC", local_id);
        }
        Err(e) => {
            let _ = bot.send_message(chat_id, format!("⚠️ Update lokal berhasil, tapi gagal sync ke Aura: {}", e.message())).await;
        }
    }
}

fn make_order_inline_keyboard(id: i64, is_buy: bool, current_tip: f64, current_prio: f64, has_pending: bool, always_save: bool, _st: &BotState) -> InlineKeyboardMarkup {
    // User minta: preset Kecil/Sedang/Besar/Mega diganti jadi 2 tombol langsung:
    // TIP dan PRIO (ketik nilai sendiri, tanpa preset).
    let mut rows = vec![
        vec![
            InlineKeyboardButton::callback(
                format!("⚡ TIP: {}", current_tip),
                format!("edit_hist_tip_{}", id),
            ),
            InlineKeyboardButton::callback(
                format!("⛽ PRIO: {}", current_prio),
                format!("edit_hist_prio_{}", id),
            ),
        ],
    ];

    if is_buy {
        rows.push(vec![
            InlineKeyboardButton::callback("🎯 TARGET", format!("edit_hist_target_{}", id)),
            InlineKeyboardButton::callback("💰 AMOUNT", format!("edit_hist_amount_{}", id)),
        ]);
    } else {
        rows.push(vec![
            InlineKeyboardButton::callback("🎯 TARGET", format!("edit_hist_target_{}", id)),
        ]);
    }

    // Perubahan 2 (2026-11-21): tombol SIMPAN selalu tampil di halaman edit
    // (user bingung karena teks menyebut SIMPAN tapi tombolnya tidak ada sebelum
    // ada perubahan). BATAL hanya tampil kalau ada edit yang di-stage.
    // Perubahan dikirim ke Aura saat tekan SIMPAN.
    if has_pending || always_save {
        let mut save_row = vec![
            InlineKeyboardButton::callback("💾 SIMPAN", format!("hist_save_{}", id)),
        ];
        if has_pending {
            save_row.push(InlineKeyboardButton::callback("❌ BATAL", format!("hist_cancel_{}", id)));
        }
        rows.push(save_row);
    }

    rows.push(vec![
        InlineKeyboardButton::callback("🔧 Buat ATA", format!("create_ata_{}", id)),
        InlineKeyboardButton::callback("🗑 HAPUS", format!("delete_order_{}", id)),
    ]);

    // Perbaikan 2026-11-21: tombol Kembali di halaman edit order (user minta).
    // Tekan ini = kembali ke Histori Limit Order (hapus pesan detail, refresh
    // tampilan History di tempat — tanpa pesan/tombol baru).
    rows.push(vec![
        InlineKeyboardButton::callback("<< Orders", "orders_refresh"),
    ]);

    InlineKeyboardMarkup::new(rows)
}

fn make_setup_keyboard(st: &BotState) -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(format!("Kecil T: {} P: {}", st.preset_kecil_tip, st.preset_kecil_prio), "setup_preset_0")],
        vec![InlineKeyboardButton::callback(format!("Sedang T: {} P: {}", st.preset_sedang_tip, st.preset_sedang_prio), "setup_preset_1")],
        vec![InlineKeyboardButton::callback(format!("Besar T: {} P: {}", st.preset_besar_tip, st.preset_besar_prio), "setup_preset_2")],
        vec![InlineKeyboardButton::callback(format!("Mega T: {} P: {}", st.preset_mega_tip, st.preset_mega_prio), "setup_preset_3")],
        vec![InlineKeyboardButton::callback("<< Back", "menu_main")],
    ])
}

// ─── Messages & Formatters ────────────────────────────────────────────────────

/// Teks panel Limit Buy (dipakai send + refresh di tempat).
/// Format liquiditas USD jadi ringkas: $12.3K / $1.2M / $123.
fn fmt_liquidity(usd: f64) -> String {
    if usd >= 1_000_000.0 {
        format!("${:.1}M", usd / 1_000_000.0)
    } else if usd >= 1_000.0 {
        format!("${:.1}K", usd / 1_000.0)
    } else if usd > 0.0 {
        format!("${:.0}", usd)
    } else {
        "—".to_string()
    }
}

/// Teks panel Limit Buy — TEKS POLOS tanpa Markdown (pesan dikirim tanpa
/// parse mode, jadi `*` dan backtick akan tampil mentah kalau dipakai).
/// Isi: nama token, harga, liquiditas. Tanpa address, tanpa baris Target
/// (target diatur lewat tombol Target di keyboard).
fn limitbuy_menu_text(st: &BotState) -> String {
    if let Some(token) = &st.active_token {
        let short = format!("{}...{}", &token[..6], &token[token.len() - 4..]);
        let name = match &st.active_token_name {
            Some(n) if !n.is_empty() => n.clone(),
            _ => short,
        };
        let price_str = match st.active_token_price {
            Some(p) if p > 0.0 => format!("${}", crate::seconddip::fmt_price(p)),
            _ => "—".to_string(),
        };
        let liq_str = match st.active_token_liquidity {
            Some(l) => fmt_liquidity(l),
            _ => "—".to_string(),
        };
        format!(
            "🪙 Token: {}\n💰 Harga: {}\n💧 Liquid: {}\n\nSilakan klik tombol yang ingin diubah, lalu ketik nilainya.",
            name, price_str, liq_str
        )
    } else {
        String::new()
    }
}

/// Teks halaman picker tipe Target (ala bot resmi Aura) — 5 pilihan:
/// Market, USD Price, Persen, Mcap, PnL. Dipilih dari tombol Target di
/// panel Limit Buy. Memilih tipe HANYA mengatur target — order TIDAK
/// dikirim ke gRPC sampai user tekan Place Order (lapis pengaman).
fn target_picker_text() -> String {
    "🎯 Pilih Tipe Target:\n\n\
    📈 Market — beli langsung di harga pasar (tidak perlu isi nilai)\n\
    💲 USD Price — beli saat harga menyentuh $...\n\
    📊 Persen — beli saat harga naik/turun ...%\n\
    🏦 Mcap — beli saat market cap menyentuh $...\n\
    📈 PnL — beli saat profit ...%\n\n\
    Pilih salah satu:".to_string()
}

/// Keyboard picker tipe Target.
fn make_target_picker_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![
            InlineKeyboardButton::callback("📈 Market", "tt_market"),
            InlineKeyboardButton::callback("💲 USD Price", "tt_usd"),
        ],
        vec![
            InlineKeyboardButton::callback("📊 Persen", "tt_pct"),
            InlineKeyboardButton::callback("🏦 Mcap", "tt_mcap"),
        ],
        vec![InlineKeyboardButton::callback("📈 PnL", "tt_pnl")],
        vec![InlineKeyboardButton::callback("<< Back", "tt_back")],
    ])
}

/// Cek apakah error Telegram berarti "pesan tidak berubah" (isi + tombol
/// sama persis dengan yang sudah tampil). Ini BUKAN kegagalan — panel sudah
/// menampilkan konten yang benar, jadi refresh dianggap berhasil supaya
/// tidak ada panel/tombol baru yang muncul (perintah user 2026-11-22:
/// isi field tertentu hanya update nilai di tombol yang sama).
fn is_not_modified_error(e: &teloxide::RequestError) -> bool {
    e.to_string().contains("message is not modified")
}

/// Update panel Limit Buy di tempat (edit pesan yang tersimpan).
/// Return false kalau belum ada panel tersimpan / edit gagal — pemanggil
/// bisa fallback kirim pesan baru.
async fn refresh_limitbuy_menu(bot: &Bot, st: &BotState) -> bool {
    let Some((c, m)) = st.limitbuy_msg else {
        log::warn!("[LimitBuy] refresh GAGAL: limitbuy_msg=None (panel belum dikenali, akan kirim baru)");
        return false;
    };
    // Telegram menolak edit_message_text kalau teksnya SAMA ("message is not
    // modified") — dan itu ikut menggagalkan update keyboard. Kalau teks
    // tidak berubah (mis. cuma label tombol yang berubah), update keyboard
    // saja lewat edit_message_reply_markup.
    // Kalau isi + tombol JUGA sama persis (nilai yang diketik tidak mengubah
    // apa pun yang tampil), Telegram tolak keduanya dengan "message is not
    // modified" — itu berarti panel sudah benar, BUKAN gagal: return true
    // supaya tidak ada panel/tombol baru yang dikirim.
    match bot
        .edit_message_text(c, m, limitbuy_menu_text(st))
        .reply_markup(make_limitbuy_keyboard(st))
        .await
    {
        Ok(_) => return true,
        Err(e) if is_not_modified_error(&e) => return true,
        Err(e) => log::warn!("[LimitBuy] edit_message_text gagal (msg {:?}): {} — coba edit keyboard saja", m, e),
    }
    match bot.edit_message_reply_markup(c, m)
        .reply_markup(make_limitbuy_keyboard(st))
        .await
    {
        Ok(_) => true,
        Err(e) if is_not_modified_error(&e) => true,
        Err(e) => {
            log::warn!("[LimitBuy] edit_message_reply_markup JUGA gagal (msg {:?}): {} — akan kirim panel baru", m, e);
            false
        }
    }
}

/// Tampilkan / refresh panel Limit Buy: edit di tempat kalau panel sudah
/// ada, baru kirim pesan baru kalau belum ada / edit gagal. Simpan ID
/// pesannya supaya edit berikutnya bisa di tempat lagi.
async fn send_limitbuy_menu(bot: &Bot, chat_id: ChatId, st: &mut BotState) -> ResponseResult<()> {
    if refresh_limitbuy_menu(bot, st).await {
        return Ok(());
    }
    log::warn!("[LimitBuy] refresh gagal -> kirim panel BARU (limitbuy_msg={:?})", st.limitbuy_msg);
    if st.active_token.is_some() {
        let text = limitbuy_menu_text(st);
        let msg = bot.send_message(chat_id, text).reply_markup(make_limitbuy_keyboard(st)).await?;
        st.limitbuy_msg = Some((chat_id, msg.id));
    }
    Ok(())
}

fn order_detail_text(o: &LimitOrder) -> String {
    let short = format!("{}...{}", &o.token[..6], &o.token[o.token.len()-4..]);
    format!(
        "📋 **Detail Order #{}**\n\nToken: `{}`\nFull: `{}`\n\n💰 Jumlah Beli: ${:.2}\n🎯 Target: {}\
\n⚡ Tip: {} SOL\n⛽ P.Fee: {} SOL\n🏄‍♂️ Slippage: 90%\n\n*Klik tombol yang ingin diedit, lalu balas dengan nominal barunya.*",
        o.id, short, o.token, o.amount_usd, format_target_display(&o.target), o.tip_fee, o.prio_fee
    )
}


fn make_order_detail_keyboard(o: &LimitOrder, st: &BotState) -> InlineKeyboardMarkup {
    make_order_inline_keyboard(o.id as i64, true, o.tip_fee, o.prio_fee, false, false, st)
}

fn parse_number(text: &str) -> Option<f64> {
    text.to_lowercase()
        .replace("sol", "")
        .replace("$", "")
        .trim()
        .parse::<f64>()
        .ok()
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

async fn answer_command(
    bot: Bot,
    msg: Message,
    cmd: Command,
    state: Arc<Mutex<BotState>>,
) -> ResponseResult<()> {
    match cmd {
        Command::Start => {
            let mut st = state.lock().await;
            st.limiter.until_ready().await;
            // Reset state edit: kalau user sebelumnya lagi mengedit field
            // (mis. Target), tempelan address sesudah /start harus dibaca
            // sebagai token Limit Buy, bukan sebagai nilai field lama.
            st.edit_field = EditField::None;
            st.active_chats.insert(msg.chat.id);
            let mode = if st.mode == AppMode::Mainnet { "MAINNET" } else { "SIMULASI" };
            let kb = make_main_menu_keyboard(&st);
            bot.send_message(msg.chat.id, format!("👋 **Selamat datang!**\nMode: `{}`\n\nPilih menu atau **Paste Address Token** untuk Limit Buy.", mode))
                .reply_markup(kb).await?;
        }
        Command::ModeSimulasi => {
            state.lock().await.mode = AppMode::Simulation;
            bot.send_message(msg.chat.id, "✅ Mode **SIMULASI**.").await?;
        }
        Command::ModeMainnet => {
            state.lock().await.mode = AppMode::Mainnet;
            bot.send_message(msg.chat.id, "⚠️ Mode **MAINNET** aktif!").await?;
        }
        Command::SimulateBuy => {
            {
                let mut st = state.lock().await;
                st.bought_token = Some("AURA_SIMULATED_TOKEN".to_string());
                st.active_token = Some("AURA_SIMULATED_TOKEN".to_string());
                st.save_db();
            }
            bot.send_message(msg.chat.id, "🟢 **Limit Buy Terpicu!**\nToken: $AURA\nHarga Beli: $0.15")
                .reply_markup(InlineKeyboardMarkup::new(vec![
                    vec![InlineKeyboardButton::callback("🔴 Confirm Swap Sell", "execute_swap_sell")],
                    vec![InlineKeyboardButton::callback("🔄 Refresh PNL", "refresh_pnl")],
                ])).await?;
        }
        Command::Clear => {
            // Hapus tombol-tombol yang tidak terpakai dari state jika ada
            {
                let mut st = state.lock().await;
                st.swap_panel_msgs.clear();
                st.bought_token = None;
                st.save_db();
            }
            bot.send_message(msg.chat.id, "🧹 Layar dibersihkan.\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n✅ Selesai.").await?;
        }
        Command::Panel => {
            let mut st = state.lock().await;
            send_limitbuy_menu(&bot, msg.chat.id, &mut st).await?;
        }
    }
    Ok(())
}

async fn handle_text_message(
    bot: Bot,
    msg: Message,
    state: Arc<Mutex<BotState>>,
) -> ResponseResult<()> {
    if let Some(text) = msg.text() {
        let mut st = state.lock().await;
        st.limiter.until_ready().await;
        let text_trim = text.trim();
        let lower = text_trim.to_lowercase();
        let chat_id = msg.chat.id;
        // Hapus pesan input user setelah diproses (seperti Aura) supaya chat
        // tidak penuh: nilai yang diketik (Prio, target, dsb.) / alamat token.
        // Di-set true kalau pesan ini dikonsumsi sebagai input.
        let mut delete_user_msg = false;

        // Register active chat
        st.active_chats.insert(chat_id);

        // Cek Address Token dulu — HANYA kalau tidak sedang mengedit field.
        // Kalau sedang tambah wallet target (SdWallet) atau edit field lain,
        // tempelan alamat harus masuk ke handler field itu, bukan ditelan
        // jadi token Limit Buy.
        let is_base58 = text_trim.chars().all(|c| c.is_alphanumeric());
        if st.edit_field == EditField::None
            && text_trim.len() >= 32
            && text_trim.len() <= 50
            && !text_trim.contains(' ')
            && is_base58
        {
            st.active_token = Some(text_trim.to_string());
            // Ambil nama + harga + liquiditas token dari Dexscreener untuk panel Limit Buy.
            let info = fetch_dexscreener_info(&[text_trim.to_string()]).await;
            if let Some((t, p, l)) = info.get(text_trim) {
                if !t.is_empty() {
                    st.active_token_name = Some(t.clone());
                }
                st.active_token_price = Some(*p);
                st.active_token_liquidity = Some(*l);
            }
            st.edit_field = EditField::None;
            // Token BARU ditempel: hapus panel lama (kalau ada) lalu kirim
            // panel BARU — jangan edit di tempat. Panel lama bisa sudah jauh
            // di atas (tidak terlihat user), sehingga tempel address terasa
            // "tidak muncul apa-apa" padahal panel sudah ter-update di tempat
            // yang tidak terlihat (laporan user 2026-11-22, token hSOL).
            if let Some((c, m)) = st.limitbuy_msg.take() {
                let _ = bot.delete_message(c, m).await;
            }
            send_limitbuy_menu(&bot, chat_id, &mut st).await?;
            // Hapus pesan alamat token yang ditempel user (seperti Aura).
            let _ = bot.delete_message(chat_id, msg.id).await;
            return Ok(());
        }

        // Lagi mengedit field -> input ini adalah nilai, hapus pesannya nanti.
        if st.edit_field != EditField::None {
            delete_user_msg = true;
        }

        // Tangani input berdasarkan field yang sedang diedit
        match st.edit_field.clone() {
            EditField::BuyAmount => {
                let lower_trimmed = lower.trim();
                let is_sol = lower_trimmed.ends_with("sol");
                let is_usd = lower_trimmed.ends_with("usd") || lower_trimmed.ends_with("usdt") || lower_trimmed.ends_with("$");
                
                let cleaned = lower_trimmed
                    .replace("sol", "")
                    .replace("usdt", "")
                    .replace("usd", "")
                    .replace("$", "");
                    
                if let Some(val) = parse_number(&cleaned) {
                    st.buy_amount = val;
                    if is_sol {
                        st.buy_currency = "SOL".to_string();
                    } else if is_usd {
                        st.buy_currency = "USD".to_string();
                    }
                    st.edit_field = EditField::None;
                    send_limitbuy_menu(&bot, chat_id, &mut st).await?;
                    delete_sd_prompt(&bot, &mut st).await;
                }
            }
            EditField::BuyTarget => {
                // Parse universal target: mcap / price-usd / persen-change
                // Contoh: "100000 mcap" | "30K Mcap" | "0.000005$" | "80%" | "-20%"
                if let Some(parsed) = parse_target_input(text_trim) {
                    st.buy_target = parsed;
                    st.edit_field = EditField::None;
                    send_limitbuy_menu(&bot, chat_id, &mut st).await?;
                    delete_sd_prompt(&bot, &mut st).await;
                } else {
                    bot.send_message(chat_id,
                        "❌ Format tidak valid.\n\n\
                        Gunakan salah satu:\n\
                        • *McAp*: `100000 mcap` | `30K Mcap` | `11M mcap`\n\
                        • *Price USD*: `0.000005$` | `$1` | `0.001$`\n\
                        • *Persen*: `80%` | `-20%` | `%80`"
                    ).await?;
                }
            }

            EditField::BuyTargetUsd => {
                // Target tipe USD Price (dari picker target): angka saja
                // atau dengan '$'. Disimpan "X$" seperti biasa.
                let cleaned = text_trim.trim_start_matches('$').trim_end_matches('$').trim();
                match cleaned.parse::<f64>() {
                    Ok(val) if val > 0.0 => {
                        st.buy_target = format!("{}$", cleaned);
                        st.edit_field = EditField::None;
                        send_limitbuy_menu(&bot, chat_id, &mut st).await?;
                        delete_sd_prompt(&bot, &mut st).await;
                    }
                    _ => {
                        bot.send_message(chat_id, "❌ Format tidak valid. Ketik harga dalam USD, contoh: `0.000005$` atau `$1`").await?;
                    }
                }
            }

            EditField::BuyTargetPct => {
                // Target tipe Persen (dari picker target): "-20" / "-20%" / "80%".
                let cleaned = text_trim.trim_end_matches('%').trim();
                match cleaned.parse::<f64>() {
                    Ok(val) => {
                        st.buy_target = format!("{}%", val);
                        st.edit_field = EditField::None;
                        send_limitbuy_menu(&bot, chat_id, &mut st).await?;
                        delete_sd_prompt(&bot, &mut st).await;
                    }
                    _ => {
                        bot.send_message(chat_id, "❌ Format tidak valid. Ketik persen, contoh: `-20%` atau `80%`").await?;
                    }
                }
            }

            EditField::BuyTargetMcap => {
                // Target tipe Mcap (dari picker target): "100K mcap" / "11M" / "2500000".
                let lower = text_trim.to_lowercase();
                let cleaned = lower.replace("mcap", "").replace("cap", "").trim().to_string();
                let num_ok = !cleaned.is_empty()
                    && cleaned.trim_end_matches(['k', 'm']).parse::<f64>().is_ok();
                if num_ok {
                    st.buy_target = format!("{} mcap", cleaned);
                    st.edit_field = EditField::None;
                    send_limitbuy_menu(&bot, chat_id, &mut st).await?;
                    delete_sd_prompt(&bot, &mut st).await;
                } else {
                    bot.send_message(chat_id, "❌ Format tidak valid. Ketik mcap dalam USD, contoh: `100K mcap` | `11M mcap` | `2500000`").await?;
                }
            }

            EditField::BuyTargetPnl => {
                // Target tipe PnL (dari picker target): "70" / "70%" / "70%pnl".
                // Disimpan "PNL X%" seperti alur lama.
                let lower = text_trim.to_lowercase();
                let cleaned = lower.replace("pnl", "").replace('%', "").trim().to_string();
                match cleaned.parse::<f64>() {
                    Ok(val) => {
                        st.buy_target = format!("PNL {}%", val);
                        st.edit_field = EditField::None;
                        send_limitbuy_menu(&bot, chat_id, &mut st).await?;
                        delete_sd_prompt(&bot, &mut st).await;
                    }
                    _ => {
                        bot.send_message(chat_id, "❌ Format tidak valid. Ketik PNL, contoh: `70%pnl` atau `70`").await?;
                    }
                }
            }

            EditField::BuyTip => {
                if let Some(val) = parse_number(&lower) {
                    st.buy_tip_fee = val;
                    st.edit_field = EditField::None;
                    send_limitbuy_menu(&bot, chat_id, &mut st).await?;
                    delete_sd_prompt(&bot, &mut st).await;
                }
            }
            EditField::BuyPrio => {
                if let Some(val) = parse_number(&lower) {
                    st.buy_prio_fee = val;
                    st.edit_field = EditField::None;
                    send_limitbuy_menu(&bot, chat_id, &mut st).await?;
                    delete_sd_prompt(&bot, &mut st).await;
                }
            }
            EditField::BuyPresetTip(idx) => {
                if let Some(val) = parse_number(&lower) {
                    if idx < st.buy_presets.len() {
                        st.buy_presets[idx].tip = val;
                        // Jika preset ini aktif, apply ke buy_tip_fee juga
                        if st.buy_active_preset == ActivePreset::Idx(idx) {
                            st.buy_tip_fee = val;
                        }
                    }
                    st.edit_field = EditField::None;
                    send_limitbuy_menu(&bot, chat_id, &mut st).await?;
                    delete_sd_prompt(&bot, &mut st).await;
                }
            }
            EditField::BuyPresetPrio(idx) => {
                if let Some(val) = parse_number(&lower) {
                    if idx < st.buy_presets.len() {
                        st.buy_presets[idx].prio = val;
                        // Jika preset ini aktif, apply ke buy_prio_fee juga
                        if st.buy_active_preset == ActivePreset::Idx(idx) {
                            st.buy_prio_fee = val;
                        }
                    }
                    st.edit_field = EditField::None;
                    send_limitbuy_menu(&bot, chat_id, &mut st).await?;
                    delete_sd_prompt(&bot, &mut st).await;
                }
            }
            EditField::AutoSellSellPct => {
                if let Some(val) = parse_number(&lower) {
                    if val > 0.0 && val <= 100.0 {
                        st.auto_sell_sell_pct = val;
                        st.edit_field = EditField::None;
                        st.save_db();
                        if !refresh_auto_sell_setup(&bot, &st).await {
                            bot.send_message(
                                chat_id,
                                format!(
                                    "✅ Jumlah jual diubah ke {}% posisi — saat TP kena, bot jual {}% dari posisi.",
                                    val, val
                                ),
                            )
                            .reply_markup(make_auto_sell_setup_keyboard(&st))
                            .await?;
                        }
                        delete_sd_prompt(&bot, &mut st).await;
                    } else {
                        bot.send_message(chat_id, "❌ % jual harus 1–100. Contoh: `50` (jual setengah posisi).").await?;
                    }
                } else {
                    bot.send_message(chat_id, "❌ Format tidak valid. Ketik angka persen, contoh: `50` (jual setengah posisi).").await?;
                }
            }
            // ── Tip / Prio order jual TP (dikirim ke Aura saat pasang TP) ──
            EditField::AutoSellTip => {
                if let Some(val) = parse_number(&lower) {
                    if val >= 0.0 {
                        st.auto_sell_tip = val;
                        st.edit_field = EditField::None;
                        st.save_db();
                        if !refresh_auto_sell_setup(&bot, &st).await {
                            bot.send_message(chat_id, format!("✅ Tip TP (Limit Biasa) diubah ke {} SOL", val))
                                .reply_markup(make_auto_sell_setup_keyboard(&st)).await?;
                        }
                        delete_sd_prompt(&bot, &mut st).await;
                    } else {
                        bot.send_message(chat_id, "❌ Tip harus >= 0. Contoh: `0.0035`.").await?;
                    }
                } else {
                    bot.send_message(chat_id, "❌ Format tidak valid. Ketik Tip dalam SOL, contoh: `0.0035`.").await?;
                }
            }
            EditField::AutoSellPrio => {
                if let Some(val) = parse_number(&lower) {
                    if val >= 0.0 {
                        st.auto_sell_prio = val;
                        st.edit_field = EditField::None;
                        st.save_db();
                        if !refresh_auto_sell_setup(&bot, &st).await {
                            bot.send_message(chat_id, format!("✅ P.Fee TP (Limit Biasa) diubah ke {} SOL", val))
                                .reply_markup(make_auto_sell_setup_keyboard(&st)).await?;
                        }
                        delete_sd_prompt(&bot, &mut st).await;
                    } else {
                        bot.send_message(chat_id, "❌ P.Fee harus >= 0. Contoh: `0.0035`.").await?;
                    }
                } else {
                    bot.send_message(chat_id, "❌ Format tidak valid. Ketik P.Fee dalam SOL, contoh: `0.0035`.").await?;
                }
            }
            EditField::AutoSellSdTip => {
                if let Some(val) = parse_number(&lower) {
                    if val >= 0.0 {
                        st.auto_sell_sd_tip = val;
                        st.edit_field = EditField::None;
                        st.save_db();
                        if !refresh_auto_sell_setup(&bot, &st).await {
                            bot.send_message(chat_id, format!("✅ Tip TP (Second Dip) diubah ke {} SOL", val))
                                .reply_markup(make_auto_sell_setup_keyboard(&st)).await?;
                        }
                        delete_sd_prompt(&bot, &mut st).await;
                    } else {
                        bot.send_message(chat_id, "❌ Tip harus >= 0. Contoh: `0.012`.").await?;
                    }
                } else {
                    bot.send_message(chat_id, "❌ Format tidak valid. Ketik Tip dalam SOL, contoh: `0.012`.").await?;
                }
            }
            EditField::AutoSellSdPrio => {
                if let Some(val) = parse_number(&lower) {
                    if val >= 0.0 {
                        st.auto_sell_sd_prio = val;
                        st.edit_field = EditField::None;
                        st.save_db();
                        if !refresh_auto_sell_setup(&bot, &st).await {
                            bot.send_message(chat_id, format!("✅ P.Fee TP (Second Dip) diubah ke {} SOL", val))
                                .reply_markup(make_auto_sell_setup_keyboard(&st)).await?;
                        }
                        delete_sd_prompt(&bot, &mut st).await;
                    } else {
                        bot.send_message(chat_id, "❌ P.Fee harus >= 0. Contoh: `0.012`.").await?;
                    }
                } else {
                    bot.send_message(chat_id, "❌ Format tidak valid. Ketik P.Fee dalam SOL, contoh: `0.012`.").await?;
                }
            }
            // ── Auto Sell Second Dip (mirror handler manual, field SD) ──
            EditField::AutoSellSdTp => {
                if let Some(val) = parse_number(&lower) {
                    if val > 0.0 {
                        st.auto_sell_sd_tp_pct = val;
                        st.edit_field = EditField::None;
                        st.save_db();
                        if !refresh_auto_sell_setup(&bot, &st).await {
                            bot.send_message(
                                chat_id,
                                format!(
                                    "✅ Take Profit Second Dip diubah ke +{}% — jual {}% saat harga naik {}% di atas harga beli.",
                                    val, st.auto_sell_sd_sell_pct, val
                                ),
                            )
                            .reply_markup(make_auto_sell_setup_keyboard(&st))
                            .await?;
                        }
                        delete_sd_prompt(&bot, &mut st).await;
                    } else {
                        bot.send_message(chat_id, "❌ TP harus > 0. Contoh: `40` (artinya +40%).").await?;
                    }
                } else {
                    bot.send_message(chat_id, "❌ Format tidak valid. Ketik angka persen, contoh: `40` (artinya +40%).").await?;
                }
            }
            EditField::AutoSellSdTpUsd => {
                if let Some(val) = parse_number(&lower) {
                    if val > 0.0 {
                        st.auto_sell_sd_tp_usd = val;
                        st.edit_field = EditField::None;
                        st.save_db();
                        if !refresh_auto_sell_setup(&bot, &st).await {
                            bot.send_message(
                                chat_id,
                                format!(
                                    "✅ Harga Take Profit Second Dip diubah ke ${} (mode USD) — jual {}% saat harga menyentuh harga ini.",
                                    crate::seconddip::fmt_price(val), st.auto_sell_sd_sell_pct
                                ),
                            )
                            .reply_markup(make_auto_sell_setup_keyboard(&st))
                            .await?;
                        }
                        delete_sd_prompt(&bot, &mut st).await;
                    } else {
                        bot.send_message(chat_id, "❌ Harga TP harus > 0. Contoh: `0.000004`.").await?;
                    }
                } else {
                    bot.send_message(chat_id, "❌ Format tidak valid. Ketik harga USD, contoh: `0.000004`.").await?;
                }
            }
            EditField::AutoSellSdTpMcap => {
                // Terima "100000", "100K", "2.5M" — parse seperti target manual.
                let s = lower.trim().trim_end_matches("mcap").trim();
                let (num_str, mult): (String, f64) = if s.ends_with('k') {
                    (s.trim_end_matches('k').to_string(), 1_000.0)
                } else if s.ends_with('m') {
                    (s.trim_end_matches('m').to_string(), 1_000_000.0)
                } else {
                    (s.to_string(), 1.0)
                };
                if let Some(val) = parse_number(&num_str) {
                    let mcap = val * mult;
                    if mcap > 0.0 {
                        st.auto_sell_sd_tp_mcap = mcap;
                        st.edit_field = EditField::None;
                        st.save_db();
                        if !refresh_auto_sell_setup(&bot, &st).await {
                            bot.send_message(
                                chat_id,
                                format!(
                                    "✅ Mcap Take Profit Second Dip diubah ke ${} mcap (mode MCAP) — jual {}% saat mcap menyentuh nilai ini.",
                                    fmt_mcap_short(mcap), st.auto_sell_sd_sell_pct
                                ),
                            )
                            .reply_markup(make_auto_sell_setup_keyboard(&st))
                            .await?;
                        }
                        delete_sd_prompt(&bot, &mut st).await;
                    } else {
                        bot.send_message(chat_id, "❌ Mcap harus > 0. Contoh: `100000` atau `100K`.").await?;
                    }
                } else {
                    bot.send_message(chat_id, "❌ Format tidak valid. Ketik mcap USD, contoh: `100000` atau `100K`.").await?;
                }
            }
            EditField::AutoSellSdSellPct => {
                if let Some(val) = parse_number(&lower) {
                    if val > 0.0 && val <= 100.0 {
                        st.auto_sell_sd_sell_pct = val;
                        st.edit_field = EditField::None;
                        st.save_db();
                        if !refresh_auto_sell_setup(&bot, &st).await {
                            bot.send_message(
                                chat_id,
                                format!(
                                    "✅ Jumlah jual Second Dip diubah ke {}% posisi — saat TP kena, bot jual {}% dari posisi.",
                                    val, val
                                ),
                            )
                            .reply_markup(make_auto_sell_setup_keyboard(&st))
                            .await?;
                        }
                        delete_sd_prompt(&bot, &mut st).await;
                    } else {
                        bot.send_message(chat_id, "❌ % jual harus 1–100. Contoh: `50` (jual setengah posisi).").await?;
                    }
                } else {
                    bot.send_message(chat_id, "❌ Format tidak valid. Ketik angka persen, contoh: `50` (jual setengah posisi).").await?;
                }
            }
            EditField::SellTip => {
                if let Some(val) = parse_number(&lower) {
                    st.sell_tip_fee = val;
                    st.edit_field = EditField::None;
                    bot.send_message(chat_id, format!("Tip Swap Sell diubah ke {} SOL", val))
                        .reply_markup(make_swapsell_keyboard(&st)).await?;
                }
            }
            EditField::SellPrio => {
                if let Some(val) = parse_number(&lower) {
                    st.sell_prio_fee = val;
                    st.edit_field = EditField::None;
                    bot.send_message(chat_id, format!("P.Fee Swap Sell diubah ke {} SOL", val))
                        .reply_markup(make_swapsell_keyboard(&st)).await?;
                }
            }
            EditField::SellSlippage => {
                st.sell_slippage = text_trim.to_string();
                st.edit_field = EditField::None;
                bot.send_message(chat_id, format!("Slippage Swap Sell diubah ke {}", st.sell_slippage))
                    .reply_markup(make_swapsell_keyboard(&st)).await?;
            }
            EditField::SdTrigger1 => {
                if let Some(val) = parse_number(&lower) {
                    if val > 0.0 && val > st.second_dip_trigger2_pct {
                        st.second_dip_trigger1_pct = val;
                        st.edit_field = EditField::None;
                        st.save_db();
                        if !refresh_sd_submenu(&bot, &st, 1).await {
                            bot.send_message(chat_id, format!("✅ Pemicu 1 diubah ke {}% — limit order dibuat kalau harga naik {}% dari entry.", val, val))
                                .reply_markup(make_sd_trigger1_keyboard(&st)).await?;
                        }
                        delete_sd_prompt(&bot, &mut st).await;
                    } else {
                        bot.send_message(chat_id, format!("❌ Pemicu 1 harus > 0 dan lebih besar dari Pemicu 2 (saat ini {}%).", st.second_dip_trigger2_pct)).await?;
                    }
                } else {
                    bot.send_message(chat_id, "❌ Format tidak valid. Ketik angka persen, contoh: `100` (artinya +100%).").await?;
                }
            }
            EditField::SdTrigger1Usd => {
                if let Some(val) = parse_number(&lower) {
                    // Kalau Pemicu 2 juga mode USD, Pemicu 1 harus di atasnya.
                    let ok = val > 0.0
                        && (st.second_dip_trigger2_mode.as_str() != "USD"
                            || val > st.second_dip_trigger2_usd);
                    if ok {
                        st.second_dip_trigger1_usd = val;
                        st.edit_field = EditField::None;
                        st.save_db();
                        if !refresh_sd_submenu(&bot, &st, 1).await {
                            bot.send_message(
                                chat_id,
                                format!(
                                    "✅ Pemicu 1 diubah ke ${} (mode USD) — limit order dibuat saat harga menyentuh harga absolut ini.",
                                    crate::seconddip::fmt_price(val)
                                ),
                            )
                            .reply_markup(make_sd_trigger1_keyboard(&st))
                            .await?;
                        }
                        delete_sd_prompt(&bot, &mut st).await;
                    } else if val <= 0.0 {
                        bot.send_message(chat_id, "❌ Harga harus > 0. Contoh: `0.00001`.").await?;
                    } else {
                        bot.send_message(chat_id, format!("❌ Pemicu 1 (USD) harus lebih besar dari Pemicu 2 (saat ini ${}).", crate::seconddip::fmt_price(st.second_dip_trigger2_usd))).await?;
                    }
                } else {
                    bot.send_message(chat_id, "❌ Format tidak valid. Ketik harga USD, contoh: `0.00001`.").await?;
                }
            }
            EditField::SdTrigger2 => {
                if let Some(val) = parse_number(&lower) {
                    if val > 0.0 && val < st.second_dip_trigger1_pct {
                        st.second_dip_trigger2_pct = val;
                        st.edit_field = EditField::None;
                        st.save_db();
                        if !refresh_sd_submenu(&bot, &st, 2).await {
                            bot.send_message(chat_id, format!("✅ Pemicu 2 diubah ke {}% — harga beli = entry + {}%.", val, val))
                                .reply_markup(make_sd_trigger2_keyboard(&st)).await?;
                        }
                        delete_sd_prompt(&bot, &mut st).await;
                    } else {
                        bot.send_message(chat_id, format!("❌ Pemicu 2 harus > 0 dan lebih kecil dari Pemicu 1 (saat ini {}%).", st.second_dip_trigger1_pct)).await?;
                    }
                } else {
                    bot.send_message(chat_id, "❌ Format tidak valid. Ketik angka persen, contoh: `20` (artinya +20%).").await?;
                }
            }
            EditField::SdTrigger1Timeout => {
                if let Some(val) = parse_number(&lower) {
                    if val >= 0.0 && val <= 10080.0 {
                        st.second_dip_trigger1_timeout_mins = val as u64;
                        st.edit_field = EditField::None;
                        st.save_db();
                        if !refresh_sd_submenu(&bot, &st, 1).await {
                            bot.send_message(
                                chat_id,
                                format!(
                                    "✅ Batas Waktu Pemicu 1 diubah ke {} — kalau harga tidak naik ke Pemicu 1 dalam waktu ini, target dibatalkan.",
                                    fmt_timeout(st.second_dip_trigger1_timeout_mins)
                                ),
                            )
                            .reply_markup(make_sd_trigger1_keyboard(&st))
                            .await?;
                        }
                        delete_sd_prompt(&bot, &mut st).await;
                    } else {
                        bot.send_message(chat_id, "❌ Batas waktu harus 0–10080 menit (maks 7 hari). Ketik `0` = tidak ada batas.").await?;
                    }
                } else {
                    bot.send_message(chat_id, "❌ Format tidak valid. Ketik angka menit, contoh: `120` (= 2 jam).").await?;
                }
            }
            EditField::SdTrigger2Timeout => {
                if let Some(val) = parse_number(&lower) {
                    if val >= 0.0 && val <= 20160.0 {
                        st.second_dip_trigger2_timeout_mins = val as u64;
                        st.edit_field = EditField::None;
                        st.save_db();
                        if !refresh_sd_submenu(&bot, &st, 2).await {
                            bot.send_message(
                                chat_id,
                                format!(
                                    "✅ Batas Waktu Pemicu 2 diubah ke {} — order kedaluwarsa kalau tidak fill dalam waktu ini.",
                                    fmt_timeout(st.second_dip_trigger2_timeout_mins)
                                ),
                            )
                            .reply_markup(make_sd_trigger2_keyboard(&st))
                            .await?;
                        }
                        delete_sd_prompt(&bot, &mut st).await;
                    } else {
                        bot.send_message(chat_id, "❌ Batas waktu harus 0–20160 menit (maks 14 hari, batas Aura). Ketik `0` = pakai default Aura.").await?;
                    }
                } else {
                    bot.send_message(chat_id, "❌ Format tidak valid. Ketik angka menit, contoh: `1440` (= 24 jam).").await?;
                }
            }
            EditField::SdTrigger2Usd => {
                if let Some(val) = parse_number(&lower) {
                    if val > 0.0 {
                        st.second_dip_trigger2_usd = val;
                        st.edit_field = EditField::None;
                        st.save_db();
                        if !refresh_sd_submenu(&bot, &st, 2).await {
                            bot.send_message(
                                chat_id,
                                format!(
                                    "✅ Harga Pemicu 2 diubah ke ${} (mode USD) — limit order dibuat di harga absolut ini.",
                                    crate::seconddip::fmt_price(val)
                                ),
                            )
                            .reply_markup(make_sd_trigger2_keyboard(&st))
                            .await?;
                        }
                        delete_sd_prompt(&bot, &mut st).await;
                    } else {
                        bot.send_message(chat_id, "❌ Harga harus > 0. Contoh: `0.0000012`.").await?;
                    }
                } else {
                    bot.send_message(chat_id, "❌ Format tidak valid. Ketik harga USD, contoh: `0.0000012`.").await?;
                }
            }
            EditField::SdTrigger2Mcap => {
                // Terima "100000", "100K", "2.5M" — parse seperti target manual.
                let s = lower.trim().trim_end_matches("mcap").trim();
                let (num_str, mult): (String, f64) = if s.ends_with('k') {
                    (s.trim_end_matches('k').to_string(), 1_000.0)
                } else if s.ends_with('m') {
                    (s.trim_end_matches('m').to_string(), 1_000_000.0)
                } else {
                    (s.to_string(), 1.0)
                };
                if let Some(val) = parse_number(&num_str) {
                    let mcap = val * mult;
                    if mcap > 0.0 {
                        st.second_dip_trigger2_mcap = mcap;
                        st.edit_field = EditField::None;
                        st.save_db();
                        if !refresh_sd_submenu(&bot, &st, 2).await {
                            bot.send_message(
                                chat_id,
                                format!(
                                    "✅ Mcap Pemicu 2 diubah ke ${} mcap (mode MCAP) — order trigger saat mcap menyentuh nilai ini.",
                                    fmt_mcap_short(mcap)
                                ),
                            )
                            .reply_markup(make_sd_trigger2_keyboard(&st))
                            .await?;
                        }
                        delete_sd_prompt(&bot, &mut st).await;
                    } else {
                        bot.send_message(chat_id, "❌ Mcap harus > 0. Contoh: `100000` atau `100K`.").await?;
                    }
                } else {
                    bot.send_message(chat_id, "❌ Format tidak valid. Ketik mcap USD, contoh: `100000` atau `100K`.").await?;
                }
            }
            EditField::AutoSellTp => {
                if let Some(val) = parse_number(&lower) {
                    if val > 0.0 {
                        st.auto_sell_tp_pct = val;
                        st.edit_field = EditField::None;
                        st.save_db();
                        if !refresh_auto_sell_setup(&bot, &st).await {
                            bot.send_message(
                                chat_id,
                                format!(
                                    "✅ Take Profit diubah ke +{}% — jual {}% saat harga naik {}% di atas harga beli.",
                                    val, st.auto_sell_sell_pct, val
                                ),
                            )
                            .reply_markup(make_auto_sell_setup_keyboard(&st))
                            .await?;
                        }
                        delete_sd_prompt(&bot, &mut st).await;
                    } else {
                        bot.send_message(chat_id, "❌ TP harus > 0. Contoh: `40` (artinya +40%).").await?;
                    }
                } else {
                    bot.send_message(chat_id, "❌ Format tidak valid. Ketik angka persen, contoh: `40` (artinya +40%).").await?;
                }
            }
            EditField::AutoSellTpUsd => {
                if let Some(val) = parse_number(&lower) {
                    if val > 0.0 {
                        st.auto_sell_tp_usd = val;
                        st.edit_field = EditField::None;
                        st.save_db();
                        if !refresh_auto_sell_setup(&bot, &st).await {
                            bot.send_message(
                                chat_id,
                                format!(
                                    "✅ Harga Take Profit diubah ke ${} (mode USD) — jual {}% saat harga menyentuh harga ini.",
                                    crate::seconddip::fmt_price(val), st.auto_sell_sell_pct
                                ),
                            )
                            .reply_markup(make_auto_sell_setup_keyboard(&st))
                            .await?;
                        }
                        delete_sd_prompt(&bot, &mut st).await;
                    } else {
                        bot.send_message(chat_id, "❌ Harga TP harus > 0. Contoh: `0.000004`.").await?;
                    }
                } else {
                    bot.send_message(chat_id, "❌ Format tidak valid. Ketik harga USD, contoh: `0.000004`.").await?;
                }
            }
            EditField::AutoSellTpMcap => {
                // Terima "100000", "100K", "2.5M" — parse seperti target manual.
                let s = lower.trim().trim_end_matches("mcap").trim();
                let (num_str, mult): (String, f64) = if s.ends_with('k') {
                    (s.trim_end_matches('k').to_string(), 1_000.0)
                } else if s.ends_with('m') {
                    (s.trim_end_matches('m').to_string(), 1_000_000.0)
                } else {
                    (s.to_string(), 1.0)
                };
                if let Some(val) = parse_number(&num_str) {
                    let mcap = val * mult;
                    if mcap > 0.0 {
                        st.auto_sell_tp_mcap = mcap;
                        st.edit_field = EditField::None;
                        st.save_db();
                        if !refresh_auto_sell_setup(&bot, &st).await {
                            bot.send_message(
                                chat_id,
                                format!(
                                    "✅ Mcap Take Profit diubah ke ${} mcap (mode MCAP) — jual {}% saat mcap menyentuh nilai ini.",
                                    fmt_mcap_short(mcap), st.auto_sell_sell_pct
                                ),
                            )
                            .reply_markup(make_auto_sell_setup_keyboard(&st))
                            .await?;
                        }
                        delete_sd_prompt(&bot, &mut st).await;
                    } else {
                        bot.send_message(chat_id, "❌ Mcap harus > 0. Contoh: `100000` atau `100K`.").await?;
                    }
                } else {
                    bot.send_message(chat_id, "❌ Format tidak valid. Ketik mcap USD, contoh: `100000` atau `100K`.").await?;
                }
            }
            EditField::SdTip => {
                if let Some(val) = parse_number(&lower) {
                    if val >= 0.0 {
                        st.second_dip_tip = val;
                        st.edit_field = EditField::None;
                        st.save_db();
                        if !refresh_sd_submenu(&bot, &st, 2).await {
                            bot.send_message(chat_id, format!("✅ Tip Second Dip diubah ke {} SOL", val))
                                .reply_markup(make_sd_trigger2_keyboard(&st)).await?;
                        }
                        delete_sd_prompt(&bot, &mut st).await;
                    }
                }
            }
            EditField::SdPrio => {
                if let Some(val) = parse_number(&lower) {
                    if val >= 0.0 {
                        st.second_dip_prio = val;
                        st.edit_field = EditField::None;
                        st.save_db();
                        if !refresh_sd_submenu(&bot, &st, 2).await {
                            bot.send_message(chat_id, format!("✅ P.Fee Second Dip diubah ke {} SOL", val))
                                .reply_markup(make_sd_trigger2_keyboard(&st)).await?;
                        }
                        delete_sd_prompt(&bot, &mut st).await;
                    }
                }
            }
            EditField::SdAmount => {
                if let Some(val) = parse_number(&lower) {
                    if val > 0.0 {
                        st.second_dip_amount_usd = val;
                        st.edit_field = EditField::None;
                        st.save_db();
                        if !refresh_sd_submenu(&bot, &st, 2).await {
                            bot.send_message(chat_id, format!("✅ Jumlah beli Second Dip diubah ke ${:.2}", val))
                                .reply_markup(make_sd_trigger2_keyboard(&st)).await?;
                        }
                        delete_sd_prompt(&bot, &mut st).await;
                    }
                }
            }
            EditField::SdWallet => {
                // Tambah wallet target (bukan ganti): validasi base58, tolak
                // duplikat, batasi maks 10 wallet. Kalau input tidak valid,
                // edit_field tetap SdWallet supaya user bisa tempel lagi.
                let addr = text_trim.to_string();
                if !db::is_valid_solana_address(&addr) {
                    bot.send_message(chat_id, "❌ Alamat wallet tidak valid. Paste alamat Solana (32-44 karakter base58, tanpa 0/O/I/l).").await?;
                } else if st.second_dip_targets.contains(&addr) {
                    bot.send_message(chat_id, format!("⚠️ Wallet `{}` sudah ada di daftar target.", crate::seconddip::short_addr(&addr))).await?;
                } else if st.second_dip_targets.len() >= db::MAX_SECOND_DIP_TARGETS {
                    bot.send_message(chat_id, format!("❌ Daftar target sudah penuh (maks {} wallet). Hapus dulu salah satu wallet sebelum menambah.", db::MAX_SECOND_DIP_TARGETS)).await?;
                } else {
                    let mut ok = false;
                    if let Ok(conn) = st.db_conn.try_lock() {
                        ok = db::add_second_dip_target(&conn, &addr).unwrap_or(false);
                    }
                    if ok {
                        st.second_dip_targets.push(addr.clone());
                        st.edit_field = EditField::None;
                        st.save_db();
                        bot.send_message(chat_id, format!("✅ Wallet `{}` ditambahkan ke daftar target ({} wallet diikuti).", crate::seconddip::short_addr(&addr), st.second_dip_targets.len()))
                            .reply_markup(make_sd_targets_keyboard(&st.second_dip_targets)).await?;
                    } else {
                        bot.send_message(chat_id, "❌ Gagal menyimpan wallet ke database. Coba lagi.").await?;
                    }
                }
            }
            EditField::HistAmount(id) => {
                if let Some(val) = parse_number(&lower) {
                    if val > 0.0 {
                        // Perubahan 2: stage saja — belum ke DB/Aura sampai tekan SIMPAN.
                        if let Some(mut p) = pending_hist_edit_or_init(&mut *st, id) {
                            p.amount_sol = val;
                            st.pending_hist_edit = Some(p);
                        }
                        st.edit_field = EditField::None;
                        refresh_hist_detail(&bot, &mut *st, chat_id, id, None).await;
                    }
                }
            }
            EditField::HistMcap(id) => {
                if let Some(o) = st.orders.iter_mut().find(|o| o.id == id) { o.target = text_trim.to_string(); }
                st.edit_field = EditField::None;
                if let Some(o) = st.orders.iter().find(|o| o.id == id).cloned() {
                    bot.send_message(chat_id, order_detail_text(&o)).reply_markup(make_order_detail_keyboard(&o, &st)).await?;
                }
            }
            EditField::HistTip(id) => {
                if let Some(val) = parse_number(&lower) {
                    if let Some(o) = st.orders.iter_mut().find(|o| o.id == id) { o.tip_fee = val; }
                    st.edit_field = EditField::None;
                    if let Some(o) = st.orders.iter().find(|o| o.id == id).cloned() {
                        bot.send_message(chat_id, order_detail_text(&o)).reply_markup(make_order_detail_keyboard(&o, &st)).await?;
                    }
                }
            }
            EditField::HistPrio(id) => {
                if let Some(val) = parse_number(&lower) {
                    if let Some(o) = st.orders.iter_mut().find(|o| o.id == id) { o.prio_fee = val; }
                    st.edit_field = EditField::None;
                    if let Some(o) = st.orders.iter().find(|o| o.id == id).cloned() {
                        bot.send_message(chat_id, order_detail_text(&o)).reply_markup(make_order_detail_keyboard(&o, &st)).await?;
                    }
                }
            }
            EditField::HistTipById(id) => {
                if let Some(val) = parse_number(&lower) {
                    // Perubahan 2: stage saja — belum ke DB/Aura sampai tekan SIMPAN.
                    if let Some(mut p) = pending_hist_edit_or_init(&mut *st, id) {
                        p.tip = val;
                        st.pending_hist_edit = Some(p);
                    }
                    st.edit_field = EditField::None;
                    refresh_hist_detail(&bot, &mut *st, chat_id, id, None).await;
                }
            }
            EditField::HistPrioById(id) => {
                if let Some(val) = parse_number(&lower) {
                    // Perubahan 2: stage saja — belum ke DB/Aura sampai tekan SIMPAN.
                    if let Some(mut p) = pending_hist_edit_or_init(&mut *st, id) {
                        p.prio = val;
                        st.pending_hist_edit = Some(p);
                    }
                    st.edit_field = EditField::None;
                    refresh_hist_detail(&bot, &mut *st, chat_id, id, None).await;
                }
            }
            EditField::HistTarget(id) => {
                // Validasi dan normalisasi target input.
                let normalized = parse_target_input(text_trim).unwrap_or_else(|| text_trim.to_string());
                // Perubahan 2: stage saja — belum ke DB/Aura sampai tekan SIMPAN.
                if let Some(mut p) = pending_hist_edit_or_init(&mut *st, id) {
                    p.target = normalized;
                    st.pending_hist_edit = Some(p);
                }
                st.edit_field = EditField::None;
                refresh_hist_detail(&bot, &mut *st, chat_id, id, None).await;
            }
            EditField::SetupPresetTip(idx) => {
                if lower.contains("tip") || lower.contains("prio") {
                    let mut tip_updated = false;
                    let mut prio_updated = false;
                    let tokens: Vec<&str> = lower.split_whitespace().collect();
                    for i in 0..tokens.len() {
                        if tokens[i] == "tip" && i + 1 < tokens.len() {
                            if let Some(val) = parse_number(tokens[i+1]) {
                                match idx {
                                    0 => st.preset_kecil_tip = val,
                                    1 => st.preset_sedang_tip = val,
                                    2 => st.preset_besar_tip = val,
                                    _ => st.preset_mega_tip = val,
                                }
                                tip_updated = true;
                            }
                        } else if (tokens[i] == "prio" || tokens[i] == "priority") && i + 1 < tokens.len() {
                            if let Some(val) = parse_number(tokens[i+1]) {
                                match idx {
                                    0 => st.preset_kecil_prio = val,
                                    1 => st.preset_sedang_prio = val,
                                    2 => st.preset_besar_prio = val,
                                    _ => st.preset_mega_prio = val,
                                }
                                prio_updated = true;
                            }
                        }
                    }
                    if tip_updated || prio_updated {
                        st.sync_presets();
                        st.save_db();
                        st.edit_field = EditField::None;
                        let label = match idx { 0 => "Kecil", 1 => "Sedang", 2 => "Besar", _ => "Mega" };
                        bot.send_message(chat_id, format!("✅ Preset **{}** berhasil diperbarui!", label))
                            .reply_markup(make_setup_keyboard(&st)).await?;
                        return Ok(());
                    }
                }
                
                // Fallback (sequential)
                if let Some(val) = parse_number(&lower) {
                    match idx {
                        0 => st.preset_kecil_tip = val,
                        1 => st.preset_sedang_tip = val,
                        2 => st.preset_besar_tip = val,
                        _ => st.preset_mega_tip = val,
                    }
                    st.edit_field = EditField::SetupPresetPrio(idx);
                    let label = match idx { 0 => "Kecil", 1 => "Sedang", 2 => "Besar", _ => "Mega" };
                    bot.send_message(chat_id, format!("✅ Tip disimpan! Sekarang ketik *Priority Fee* untuk preset **{}** (contoh: `0.002`)", label)).await?;
                }
            }
            EditField::SetupPresetPrio(idx) => {
                if let Some(val) = parse_number(&lower) {
                    match idx {
                        0 => st.preset_kecil_prio = val,
                        1 => st.preset_sedang_prio = val,
                        2 => st.preset_besar_prio = val,
                        _ => st.preset_mega_prio = val,
                    }
                    st.sync_presets();
                    st.save_db();
                    st.edit_field = EditField::None;
                    let label = match idx { 0 => "Kecil", 1 => "Sedang", 2 => "Besar", _ => "Mega" };
                    bot.send_message(chat_id, format!("✅ Preset **{}** berhasil diperbarui!", label))
                        .reply_markup(make_setup_keyboard(&st)).await?;
                }
            }
            EditField::None => {
                // Ignore text if we are not editing anything, 
                // UN Kecuali fallback global kalau user ngetik "5$" tanpa nge-klik tombol edit Amount dulu (untuk UX lama)
                if lower.ends_with('$') || lower.ends_with("usd") || lower.ends_with("usdt") {
                    let cleaned = lower.replace("$", "").replace("usdt", "").replace("usd", "");
                    if let Some(val) = parse_number(&cleaned) {
                        st.buy_amount = val;
                        st.buy_currency = "USD".to_string();
                        delete_user_msg = true;
                        if st.active_token.is_some() { send_limitbuy_menu(&bot, chat_id, &mut st).await?; }
                    }
                } else if lower.ends_with("sol") {
                    let cleaned = lower.replace("sol", "");
                    if let Some(val) = parse_number(&cleaned) {
                        st.buy_amount = val;
                        st.buy_currency = "SOL".to_string();
                        delete_user_msg = true;
                        if st.active_token.is_some() { send_limitbuy_menu(&bot, chat_id, &mut st).await?; }
                    }
                } else if lower.contains("mcap") {
                    st.buy_target = text_trim.to_string();
                    delete_user_msg = true;
                    if st.active_token.is_some() { send_limitbuy_menu(&bot, chat_id, &mut st).await?; }
                } else if lower.ends_with("sol") {
                    if let Some(val) = parse_number(&lower) {
                        st.buy_tip_fee = val;
                        st.buy_prio_fee = val;
                        delete_user_msg = true;
                        if st.active_token.is_some() { send_limitbuy_menu(&bot, chat_id, &mut st).await?; }
                    }
                }
            }
            _ => {
                // handle other edit fields
                st.edit_field = EditField::None;
            }
        }
        // Hapus pesan nilai yang diketik user (seperti Aura) supaya chat
        // tidak penuh. Error diabaikan (mis. pesan sudah terhapus / bot
        // tidak punya izin) supaya alur utama tidak terganggu.
        if delete_user_msg {
            let _ = bot.delete_message(chat_id, msg.id).await;
        }
        st.save_db();
    }
    Ok(())
}

async fn handle_callback(
    bot: Bot,
    q: CallbackQuery,
    state: Arc<Mutex<BotState>>,
) -> ResponseResult<()> {
    if let Some(data) = q.data {
        let mut st = state.lock().await;
        st.limiter.until_ready().await;
        let chat_id = if let Some(msg) = &q.message { msg.chat.id } else { return Ok(()); };
        let msg_id  = if let Some(msg) = &q.message { msg.id }      else { return Ok(()); };

        // Register active chat
        st.active_chats.insert(chat_id);

        if data == "delete_this_message" {
            let _ = bot.delete_message(chat_id, msg_id).await;
            bot.answer_callback_query(q.id).await?;
            return Ok(());
        }

        if data.starts_with("create_ata_") {
            let id: i64 = data.trim_start_matches("create_ata_").parse().unwrap_or(0);
            let mut order_token = String::new();
            if let Ok(conn) = st.db_conn.try_lock() {
                if let Ok(order) = crate::db::get_limit_order(&conn, id) {
                    order_token = order.token;
                }
            }
            if order_token.is_empty() {
                bot.send_message(chat_id, "❌ Limit order tidak ditemukan.").await?;
                bot.answer_callback_query(q.id.clone()).await?;
                return Ok(());
            }

            if let Some(client) = st.aura_client.clone() {
                let bot_clone = bot.clone();
                let token_clone = order_token.clone();
                let chat_id_clone = chat_id;
                
                tokio::spawn(async move {
                    use aura_api_client::types::{OpenTaRequest, TokenAccountKindUi, FetchFullWalletsInfoReq};
                    use std::str::FromStr;

                    if let Ok(mint_addr) = solana_address::Address::from_str(&token_clone) {
                        let wallet_res = client.aura().fetch_full_wallet_info((), tonic::Request::new(FetchFullWalletsInfoReq {})).await;
                        let wallet = match wallet_res {
                            Ok(res) => res.into_inner().wallets.get(0).cloned().unwrap_or_default(),
                            Err(e) => {
                                let _ = bot_clone.send_message(chat_id_clone, format!("❌ Gagal mengambil info wallet: {}", e.message())).await;
                                return;
                            }
                        };

                        let req = tonic::Request::new(OpenTaRequest {
                            owner: wallet,
                            mint: mint_addr,
                            kind: TokenAccountKindUi::Ata,
                            is_2022: false,
                        });

                        match client.utils().open_ta((), req).await {
                            Ok(resp) => {
                                let sig = resp.into_inner();
                                let sig_str = format!("{}", sig);
                                let solscan_link = format!("https://solscan.io/tx/{}", sig_str);
                                let success_text = format!("Transaction sent: <a href=\"{}\">{}</a>", solscan_link, sig_str);
                                let keyboard = teloxide::types::InlineKeyboardMarkup::new(vec![vec![
                                    teloxide::types::InlineKeyboardButton::callback("Hapus", "delete_this_message")
                                ]]);
                                let _ = bot_clone.send_message(chat_id_clone, success_text)
                                    .parse_mode(teloxide::types::ParseMode::Html)
                                    .reply_markup(keyboard)
                                    .await;
                            }
                            Err(e) => {
                                let _ = bot_clone.send_message(chat_id_clone, format!("❌ Gagal membuat ATA: {}", e.message())).await;
                            }
                        }
                    } else {
                        let _ = bot_clone.send_message(chat_id_clone, "❌ Alamat token tidak valid!").await;
                    }
                });
            } else {
                bot.send_message(chat_id, "❌ Client Aura gRPC belum terhubung.").await?;
            }
            bot.answer_callback_query(q.id.clone()).await?;
            return Ok(());
        }

        // Prefix routing: detail order per token & hapus semua order satu token
        if data.starts_with("orders_token_") {
            let mint = data.trim_start_matches("orders_token_").to_string();
            bot.answer_callback_query(q.id.clone()).await?;
            drop(st);
            let st2 = state.lock().await;
            send_orders_token_detail(&bot, chat_id, &st2, &mint).await?;
            return Ok(());
        }
        if data.starts_with("orders_clear_token_") {
            let mint = data.trim_start_matches("orders_clear_token_").to_string();
            bot.answer_callback_query(q.id.clone()).text("Menghapus order token ini...").await?;
            drop(st);
            let st2 = state.lock().await;
            if let Some(client) = st2.aura_client.clone() {
                use std::str::FromStr;
                if let Ok(m) = solana_address::Address::from_str(&mint) {
                    let req = tonic::Request::new(aura_api_client::types::DeleteOrders {
                        mint: m,
                        all: true,
                        ids: vec![],
                    });
                    let _ = client.limit_orders().delete_limit_orders((), req).await;
                }
            }
            if let Ok(conn) = st2.db_conn.try_lock() {
                let _ = db::delete_limit_orders_by_token(&conn, &mint);
            }
            bot.send_message(chat_id, format!("✅ Semua order untuk `{}` sudah dihapus.", crate::seconddip::short_addr(&mint))).await?;
            return Ok(());
        }
        // Edit satu order (dari detail token): tampilkan detail + keyboard edit.
        if data.starts_with("edit_hist_order_") {
            let id: i64 = data.trim_start_matches("edit_hist_order_").parse().unwrap_or(0);
            bot.answer_callback_query(q.id.clone()).await?;
            drop(st);
            let mut st2 = state.lock().await;
            send_hist_order_detail(&bot, chat_id, &mut st2, id).await?;
            return Ok(());
        }
        // Perubahan 2: SIMPAN — tulis semua perubahan stage ke DB, lalu kirim
        // ke Aura (hapus order lama, buat baru) SEKALI. History tidak di-refresh.
        if data.starts_with("hist_save_") {
            let id: i64 = data.trim_start_matches("hist_save_").parse().unwrap_or(0);
            let pending = st.pending_hist_edit.clone();
            if pending.as_ref().map(|p| p.local_id == id).unwrap_or(false) {
                bot.answer_callback_query(q.id.clone()).text("⏳ Menyimpan ke Aura...").await?;
            } else {
                // Tombol SIMPAN selalu tampil (2026-11-21); kalau belum ada
                // perubahan, tidak ada yang dikirim — cukup beri tahu user.
                bot.answer_callback_query(q.id.clone()).text("Tidak ada perubahan untuk disimpan.").await?;
                refresh_hist_detail(&bot, &mut *st, chat_id, id, None).await;
                return Ok(());
            }
            if let Some(p) = pending.filter(|p| p.local_id == id) {
                // 1. Tulis ke DB lokal.
                if let Ok(conn) = st.db_conn.try_lock() {
                    let _ = db::update_limit_order(&conn, id, &p.target, p.tip, p.prio);
                    let _ = db::update_limit_order_amount(&conn, id, p.amount_sol);
                }
                // 2. Sync ke Aura (hapus lama, buat baru) di background.
                if let Some(client) = st.aura_client.clone() {
                    let db_conn_clone = st.db_conn.clone();
                    let slippage = st.sell_slippage.clone();
                    let bot_clone = bot.clone();
                    tokio::spawn(async move {
                        recreate_aura_order(
                            client, db_conn_clone, id, p.token, p.order_type, p.target,
                            p.tip, p.prio, p.amount_sol, p.aura_order_id, slippage, bot_clone, chat_id,
                        ).await;
                    });
                }
                st.pending_hist_edit = None;
                st.edit_field = EditField::None;
            }
            refresh_hist_detail(&bot, &mut *st, chat_id, id, None).await;
            return Ok(());
        }
        // BATAL — buang semua perubahan stage, kembali ke nilai DB.
        if data.starts_with("hist_cancel_") {
            let id: i64 = data.trim_start_matches("hist_cancel_").parse().unwrap_or(0);
            bot.answer_callback_query(q.id.clone()).text("Perubahan dibatalkan.").await?;
            st.pending_hist_edit = None;
            st.edit_field = EditField::None;
            refresh_hist_detail(&bot, &mut *st, chat_id, id, None).await;
            return Ok(());
        }
        // Prefix routing for history order view & delete
        if data.starts_with("delete_order_") {
            let id: i64 = data.trim_start_matches("delete_order_").parse().unwrap_or(0);
            
            let mut aura_order_id = None;
            let mut order_token = String::new();
            let mut order_type = String::new();
            
            if let Ok(conn) = st.db_conn.try_lock() {
                if let Ok(order) = crate::db::get_limit_order(&conn, id) {
                    aura_order_id = order.aura_order_id;
                    order_token = order.token;
                    order_type = order.order_type;
                }
                let _ = db::delete_limit_order(&conn, id);
                // Kalau order beli ini sedang dilacak Auto Sell, hentikan
                // pelacakannya juga — kalau tidak, watcher terus mengecek
                // posisi untuk order yang sudah dihapus (buang-buang panggilan
                // API sampai kena rate limit).
                if let Some(a_id) = aura_order_id {
                    let _ = db::remove_auto_sell_tracked(&conn, a_id);
                }
            }

            if let Some(client) = st.aura_client.clone() {
                use std::str::FromStr;
                if let Ok(mint) = solana_address::Address::from_str(&order_token) {
                    let req = if let Some(a_id) = aura_order_id {
                        tonic::Request::new(aura_api_client::types::DeleteOrders {
                            mint,
                            all: false,
                            ids: vec![aura_api_client::types::OrderId(a_id)],
                        })
                    } else {
                        tonic::Request::new(aura_api_client::types::DeleteOrders {
                            mint,
                            all: true,
                            ids: vec![],
                        })
                    };
                    tokio::spawn(async move {
                        let res = client.limit_orders().delete_limit_orders((), req).await;
                        match res {
                            Ok(_) => info!("Berhasil menghapus order dari Aura gRPC"),
                            Err(e) => info!("Gagal menghapus order dari Aura gRPC: {}", e.message()),
                        }
                    });
                }
            }

            // Order dihapus: buang pending edit + jejak pesan detailnya (kalau ada).
            if st.pending_hist_edit.as_ref().map(|p| p.local_id == id).unwrap_or(false) {
                st.pending_hist_edit = None;
            }
            st.hist_detail_msg = None;
            drop(st); // release lock before await
            let mut st2 = state.lock().await;
            // Hapus pesan detail otomatis (jangan tinggalkan pesan konfirmasi
            // di chat — perintah user 2026-11-22), lalu refresh History di
            // tempat supaya daftar order langsung ter-update.
            bot.answer_callback_query(q.id.clone())
                .text(format!("✅ {} #{} dihapus.", order_type, id))
                .await?;
            let _ = bot.delete_message(chat_id, msg_id).await;
            if !refresh_orders_grouped(&bot, &mut st2).await {
                send_orders_grouped(&bot, chat_id, &mut st2).await?;
            }
            return Ok(());
        }

        if data.starts_with("hist_preset_") {
            // hist_preset_0_123
            let parts: Vec<&str> = data.split('_').collect();
            if parts.len() == 4 {
                let preset_idx: usize = parts[2].parse().unwrap_or(0);
                let order_id: i64 = parts[3].parse().unwrap_or(0);
                let (tip, prio, preset_label) = match preset_idx {
                    0 => (st.preset_kecil_tip, st.preset_kecil_prio, "Kecil"),
                    1 => (st.preset_sedang_tip, st.preset_sedang_prio, "Sedang"),
                    2 => (st.preset_besar_tip, st.preset_besar_prio, "Besar"),
                    _ => (st.preset_mega_tip, st.preset_mega_prio, "Mega"),
                };
                if let Ok(conn) = st.db_conn.try_lock() {
                    let orders = db::load_limit_orders(&conn).unwrap_or_default();
                    if let Some(pos) = orders.iter().position(|o| o.id == order_id) {
                        let o = &orders[pos];
                        let display_num = pos + 1;
                        let _ = db::update_limit_order(&conn, order_id, &o.target, tip, prio);
                        bot.answer_callback_query(q.id.clone()).text(format!("✅ Preset {} diaplikasikan!", preset_label)).await?;
                        // update msg - format baru
                        let short_token = if o.token.len() >= 10 {
                            format!("{}...{}", &o.token[..6], &o.token[o.token.len()-4..])
                        } else {
                            o.token.clone()
                        };
                        let target_display = format_target_display(&o.target);
                        let is_buy = o.order_type.eq_ignore_ascii_case("BUY");
                        let amount_str = if is_buy && o.amount_sol > 0.0 {
                            format!("\n💰 Amount | {:.2} USD", o.amount_sol)
                        } else if !is_buy && o.amount_sol > 0.0 {
                            format!("\n💰 Amount | {:.2} SOL", o.amount_sol)
                        } else {
                            String::new()
                        };
                        let text = format!(
                            "#{} LIMIT ORDER | {}\nToken: {}{}\n🎯 Target | {}\n⚡ T:{} SOL ⛽ P:{} SOL",
                            display_num, o.order_type, short_token, amount_str, target_display, tip, prio
                        );
                        bot.edit_message_text(chat_id, msg_id, text).reply_markup(make_order_inline_keyboard(o.id, is_buy, tip, prio, false, false, &st)).await?;

                        // Sync tip/prio update ke Aura gRPC jika ada aura_order_id
                        if let Some(a_id) = o.aura_order_id {
                            if let Some(client) = st.aura_client.clone() {
                                use aura_api_client::types::{
                                    ApiLimitOrder, ApiOrders, OrderEventTrigger, OrderId,
                                    OrderState, RawOrder, SwapAmount, TxnProcessors,
                                    UpdateTokenLimitOrders, UserNonceStrategy,
                                };
                                use std::str::FromStr;
                                let target_clone = o.target.clone();
                                let token_clone = o.token.clone();
                                let order_type_clone = o.order_type.clone();
                                let amount_sol = o.amount_sol;
                                let slippage_str = st.sell_slippage.clone();
                                let bot_clone = bot.clone();
                                if let Ok(mint) = solana_address::Address::from_str(&token_clone) {
                                    let slippage_f64 = slippage_str.replace("%", "").trim().parse::<f64>().unwrap_or(95.0);
                                    let slippage_scaled = (slippage_f64 / 100.0 * 1_000_000.0) as u64;
                                    let slippage_val = fastnum::UD128::from(slippage_scaled) / fastnum::UD128::from(1_000_000u64);
                                    let tip_lam = (tip * 1e9) as u64;
                                    let prio_lam = (prio * 1e9) as u64;
                                    let db_conn_clone = st.db_conn.clone();
                                    tokio::spawn(async move {
                                        let sol_price = fetch_sol_price_usd().await.unwrap_or(0.0);
                                        if let Some(aura_target) = parse_target_str_to_aura(&target_clone, sol_price, &order_type_clone) {
                                            let wallet = {
                                                let mut tc = client.aura();
                                                match tc.fetch_full_wallet_info((), tonic::Request::new(aura_api_client::types::FetchFullWalletsInfoReq {})).await {
                                                    Ok(r) => r.into_inner().wallets.get(0).cloned().unwrap_or_default(),
                                                    Err(_) => solana_address::Address::default(),
                                                }
                                            };
                                            
                                            let swap_amount = if order_type_clone.eq_ignore_ascii_case("BUY") {
                                                // amount_sol sebenarnya USD -> bayar USDC (aturan user: jangan SOL).
                                                buy_swap_amount(amount_sol, "USD")
                                            } else {
                                                SwapAmount::SellPerc { amount: fastnum::udec128!(1) }
                                            };

                                            let api_order = ApiLimitOrder {
                                                state: OrderState::Api {
                                                    id: None, // New ID will be assigned
                                                    expire_dur: None,
                                                    activate_dur: None,
                                                },
                                                order: RawOrder {
                                                    slippage: slippage_val,
                                                    tip: decisol::Lamports::from(tip_lam),
                                                    fee: decisol::Lamports::from(prio_lam),
                                                    target: aura_target,
                                                    amount: swap_amount,
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
                                            
                                            // Delete old order
                                            let del_req = tonic::Request::new(aura_api_client::types::DeleteOrders {
                                                mint,
                                                all: false,
                                                ids: vec![OrderId(a_id)],
                                            });
                                            let _ = client.limit_orders().delete_limit_orders((), del_req).await;

                                            let req = tonic::Request::new(UpdateTokenLimitOrders {
                                                mint,
                                                orders: ApiOrders { orders: vec![api_order] },
                                            });
                                            match client.limit_orders().place_limit_orders((), req).await {
                                                Ok(resp) => {
                                                    if let Some(new_a_id) = resp.into_inner().ids.get(0).copied().map(|x| x.0) {
                                                        if let Ok(conn) = db_conn_clone.try_lock() {
                                                            let _ = crate::db::update_aura_order_id(&conn, order_id, new_a_id);
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    let _ = bot_clone.send_message(chat_id, format!("⚠️ Update lokal berhasil, tapi gagal sync ke Aura: {}", e.message())).await;
                                                }
                                            }
                                        }
                                    });
                                }
                            }
                        }

                        return Ok(());
                    }
                }
            }
        }
        
        if data.starts_with("edit_hist_target_") {
            let id: i64 = data.trim_start_matches("edit_hist_target_").parse().unwrap_or(0);
            st.edit_field = EditField::HistTarget(id);
            bot.answer_callback_query(q.id.clone()).text("Aim & Slay 🌞 Ketik target!").await?;
            // Perubahan 3: prompt di-update di pesan detail yang sama (di tempat),
            // bukan kirim pesan baru — biar riwayat chat tidak menumpuk.
            refresh_hist_detail(&bot, &mut *st, chat_id, id, Some("🎯 Ketik target baru:\n\n• McAp: `100K mcap` | `11M mcap` | `2.35M mcap`\n• Price: `0.000005$` | `$1`\n• Persen: `80%` | `-20%`\n• Pnl : `70%pnl` | `pnl70%`")).await;
            return Ok(());
        }

        // Edit Tip langsung (tanpa preset): ketik nilai SOL baru.
        if data.starts_with("edit_hist_tip_") {
            let id: i64 = data.trim_start_matches("edit_hist_tip_").parse().unwrap_or(0);
            st.edit_field = EditField::HistTipById(id);
            bot.answer_callback_query(q.id.clone()).text("Ketik Tip baru...").await?;
            refresh_hist_detail(&bot, &mut *st, chat_id, id, Some("⚡ Ketik nilai Tip baru dalam SOL (contoh: `0.0035` atau `0.25`):")).await;
            return Ok(());
        }

        // Edit Prio langsung (tanpa preset): ketik nilai SOL baru.
        if data.starts_with("edit_hist_prio_") {
            let id: i64 = data.trim_start_matches("edit_hist_prio_").parse().unwrap_or(0);
            st.edit_field = EditField::HistPrioById(id);
            bot.answer_callback_query(q.id.clone()).text("Ketik Prio baru...").await?;
            refresh_hist_detail(&bot, &mut *st, chat_id, id, Some("⛽ Ketik nilai Priority Fee baru dalam SOL (contoh: `0.0035` atau `0.055`):")).await;
            return Ok(());
        }

        if data.starts_with("edit_hist_amount_") {
            let id: i64 = data.trim_start_matches("edit_hist_amount_").parse().unwrap_or(0);
            st.edit_field = EditField::HistAmount(id);
            bot.answer_callback_query(q.id.clone()).text("Menunggu input amount...").await?;
            refresh_hist_detail(&bot, &mut *st, chat_id, id, Some("💰 Ketik jumlah pembelian baru dalam USD (contoh: `5` atau `10.5`):")).await;
            return Ok(());
        }

        // ── Prefix routing untuk Quick-set Preset ────────────────────────────────
        // preset_select_<idx>: user memilih preset, apply ke buy_tip_fee & buy_prio_fee
        if data.starts_with("preset_select_") {
            let idx: usize = data.trim_start_matches("preset_select_").parse().unwrap_or(0);
            if idx < st.buy_presets.len() {
                // Toggle: jika sudah aktif, deaktifkan; jika belum, aktifkan
                if st.buy_active_preset == ActivePreset::Idx(idx) {
                    let label = st.buy_presets[idx].label.clone();
                    st.buy_active_preset = ActivePreset::None;
                    bot.answer_callback_query(q.id.clone()).text(format!("Preset {} dinonaktifkan", label)).await?;
                } else {
                    // Clone nilai dulu sebelum mutable borrow
                    let (tip, prio, label) = {
                        let p = &st.buy_presets[idx];
                        (p.tip, p.prio, p.label.clone())
                    };
                    st.buy_tip_fee = tip;
                    st.buy_prio_fee = prio;
                    st.buy_active_preset = ActivePreset::Idx(idx);
                    bot.answer_callback_query(q.id.clone()).text(format!("✅ Preset {} aktif! Tip={} Prio={}", label, tip, prio)).await?;
                }
                // Refresh keyboard
                let keyboard = make_limitbuy_keyboard(&st);
                let _ = bot.edit_message_reply_markup(chat_id, msg_id).reply_markup(keyboard).await;
                st.limitbuy_msg = Some((chat_id, msg_id));
            }
            return Ok(());
        }
        // preset_edit_tip_<idx>: edit nilai tip preset tertentu
        if data.starts_with("preset_edit_tip_") {
            let idx: usize = data.trim_start_matches("preset_edit_tip_").parse().unwrap_or(0);
            if idx < st.buy_presets.len() {
                let label = st.buy_presets[idx].label.clone();
                st.limitbuy_msg = Some((chat_id, msg_id));
                st.edit_field = EditField::BuyPresetTip(idx);
                bot.answer_callback_query(q.id.clone()).text(format!("Ketik Tip baru untuk preset {}", label)).await?;
                let prompt_msg = bot.send_message(chat_id, format!("✏️ Ketik nilai Tip untuk preset **{}** (contoh: 0.005)", label)).await?;
                st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
            }
            return Ok(());
        }
        // preset_edit_prio_<idx>: edit nilai prio preset tertentu
        if data.starts_with("preset_edit_prio_") {
            let idx: usize = data.trim_start_matches("preset_edit_prio_").parse().unwrap_or(0);
            if idx < st.buy_presets.len() {
                let label = st.buy_presets[idx].label.clone();
                st.limitbuy_msg = Some((chat_id, msg_id));
                st.edit_field = EditField::BuyPresetPrio(idx);
                bot.answer_callback_query(q.id.clone()).text(format!("Ketik Prio baru untuk preset {}", label)).await?;
                let prompt_msg = bot.send_message(chat_id, format!("✏️ Ketik nilai P.Fee untuk preset **{}** (contoh: 0.005)", label)).await?;
                st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
            }
            return Ok(());
        }

        // sd_remove_target_<wallet>: hapus satu wallet dari daftar target
        // Second Dip, lalu refresh halaman daftar.
        if let Some(wallet) = data.strip_prefix("sd_remove_target_") {
            let wallet = wallet.to_string();
            let mut removed = false;
            if let Ok(conn) = st.db_conn.try_lock() {
                removed = db::remove_second_dip_target(&conn, &wallet).unwrap_or(false);
            }
            if removed {
                st.second_dip_targets.retain(|w| w != &wallet);
                st.save_db();
                bot.answer_callback_query(q.id.clone()).text("Wallet dihapus dari daftar target").await?;
                let text = sd_targets_text(&st.second_dip_targets);
                let kb = make_sd_targets_keyboard(&st.second_dip_targets);
                bot.edit_message_text(chat_id, msg_id, text).reply_markup(kb).await?;
            } else {
                bot.answer_callback_query(q.id.clone()).text("Wallet tidak ada di daftar").await?;
            }
            return Ok(());
        }

        // Exact match routing
        match data.as_str() {
            "menu_main" => {
                let kb = make_main_menu_keyboard(&st);
                bot.edit_message_text(chat_id, msg_id, "👋 **Menu Utama**").reply_markup(kb).await?;
                bot.answer_callback_query(q.id.clone()).await?;
            }
            "menu_claim_sol" => {
                let text = "💧 *Claim Your SOL*\n\n\
                *Info Penting:*\n\
                Secara native di jaringan Solana, Anda **tidak memerlukan layanan eksternal (seperti web pihak ketiga)** untuk mengklaim SOL dari token kosong. Itu adalah fitur bawaan Solana (SPL Token) di mana Anda cukup mengirimkan instruksi `CloseAccount` untuk menghapus ATA kosong dan uang sewa (rent) sebesar ~0.002 SOL akan otomatis kembali ke dompet Anda.\n\n\
                **Status Bot Saat Ini:**\n\
                Koneksi Aura gRPC saat ini belum menyediakan fitur/endpoint untuk melakukan `CloseAccount`. Karena bot ini tidak memegang Private Key Anda secara langsung (semua transaksi diamankan melalui Aura API), bot belum bisa menghapus token kosong Anda dari sini.\n\n\
                *Saran:*\n\
                Untuk mengklaim SOL Anda saat ini, Anda bisa menggunakan dompet seperti Phantom/Solflare yang memiliki fitur bawaan untuk menghapus akun token spam/kosong, atau menggunakan dApps tepercaya seperti *Sol Incinerator* atau *Step Finance*. Hati-hati dengan situs web pihak ketiga yang tidak resmi!";
                
                let kb = teloxide::types::InlineKeyboardMarkup::new(vec![
                    vec![teloxide::types::InlineKeyboardButton::callback("<< Back", "menu_main")]
                ]);
                let _ = bot.edit_message_text(chat_id, msg_id, text)
                    .parse_mode(teloxide::types::ParseMode::Markdown)
                    .reply_markup(kb)
                    .await;
                bot.answer_callback_query(q.id.clone()).await?;
            }

            "show_swap_panel" => {
                let kb = InlineKeyboardMarkup::new(vec![
                    vec![InlineKeyboardButton::callback("🔴 Confirm Swap Sell", "execute_swap_sell")],
                    vec![InlineKeyboardButton::callback("🔄 Refresh PNL", "refresh_pnl")],
                ]);
                let token_display = if let Some(t) = &st.bought_token { t.clone() } else { "Token aktif".to_string() };
                bot.send_message(chat_id, format!("🟢 **Panel Swap Sell (Konfirmasi)**\nToken: {}", token_display))
                    .reply_markup(kb).await?;
                bot.answer_callback_query(q.id.clone()).await?;
            }
            "menu_swapsell" => {
                let token = st.active_token.clone().unwrap_or_else(|| "Tidak ada token".to_string());
                let bot_clone = bot.clone();
                let client_opt = st.aura_client.clone();
                let _q_id = q.id.clone();
                let keyboard = make_swapsell_keyboard(&st);
                tokio::spawn(async move {
                    let mut pnl_text = format!("Token: `{}`\nPNL: 0.00%\nAmount SOL: 0", token);
                    if token != "Tidak ada token" {
                        if let Some(client) = client_opt {
                            let start_time = std::time::Instant::now();
                            let mint_addr = token.parse::<solana_address::Address>().ok();
                            let req = tonic::Request::new(aura_api_client::types::TokenPositionsUiReq { mint: mint_addr });
                            
                            if let Ok(resp) = client.aura().get_token_positions_ui((), req).await {
                                let elapsed = start_time.elapsed();
                                let ui = resp.into_inner();
                                
                                let mut found = false;
                                for pos in ui.positions {
                                    let mint_str = format!("{:?}", pos.mint);
                                    if mint_str.contains(&token) {
                                        let pnl_str = if let Some(p) = pos.pnl {
                                            let p_f64 = format!("{}", p).parse::<f64>().unwrap_or(1.0);
                                            let pnl_pct = (p_f64 - 1.0) * 100.0;
                                            format!("{:.2}", pnl_pct)
                                        } else {
                                            "0.00".to_string()
                                        };
                                        let raw_lamports: u64 = pos.quote_value.into();
                                        let sol_val = raw_lamports as f64 / 1e9;
                                        let sol_price_usd = fetch_sol_price_usd().await.unwrap_or(0.0);
                                        let usd_val = sol_val * sol_price_usd;
                                        pnl_text = format!("Token: `{}`\nPNL: {}%\nAmount SOL: {:.6} SOL (${:.2})\n⏱️ Latency: {:?}", token, pnl_str, sol_val, usd_val, elapsed);
                                        found = true;
                                        break;
                                    }
                                }
                                
                                if !found {
                                    pnl_text = format!("Token: `{}`\nPNL: 0.00%\nAmount SOL: 0\n⏱️ Latency: {:?}", token, elapsed);
                                }
                            }
                        }
                    }
                    let _ = bot_clone.edit_message_text(chat_id, msg_id, pnl_text)
                        .reply_markup(keyboard)
                        .await;
                });
                bot.answer_callback_query(q.id.clone()).text("Memuat data token...").await?;
            }
            "refresh_pnl" => {
                let token = st.active_token.clone().unwrap_or_else(|| "Tidak ada token".to_string());
                let bot_clone = bot.clone();
                let client_opt = st.aura_client.clone();
                let _q_id = q.id.clone();
                // Gunakan keyboard panel saja, jangan setting
                let panel_keyboard = teloxide::types::InlineKeyboardMarkup::new(vec![
                    vec![teloxide::types::InlineKeyboardButton::callback(
                        "🔴 Confirm Swap Sell", "execute_swap_sell",
                    )],
                    vec![teloxide::types::InlineKeyboardButton::callback(
                        "🔄 Refresh PNL", "refresh_pnl",
                    )],
                ]);
                tokio::spawn(async move {
                    let mut pnl_text = format!("Token: `{}`\nPNL: 0.00%\nAmount SOL: 0", token);
                    if token != "Tidak ada token" {
                        if let Some(client) = client_opt {
                            let start_time = std::time::Instant::now();
                            let mint_addr = token.parse::<solana_address::Address>().ok();
                            let req = tonic::Request::new(aura_api_client::types::TokenPositionsUiReq { mint: mint_addr });
                            
                            if let Ok(resp) = client.aura().get_token_positions_ui((), req).await {
                                let elapsed = start_time.elapsed();
                                let ui = resp.into_inner();
                                
                                let mut found = false;
                                for pos in ui.positions {
                                    let mint_str = format!("{:?}", pos.mint);
                                    if mint_str.contains(&token) {
                                        let pnl_str = if let Some(p) = pos.pnl {
                                            let p_f64 = format!("{}", p).parse::<f64>().unwrap_or(1.0);
                                            let pnl_pct = (p_f64 - 1.0) * 100.0;
                                            format!("{:.2}", pnl_pct)
                                        } else {
                                            "0.00".to_string()
                                        };
                                        let raw_lamports: u64 = pos.quote_value.into();
                                        let sol_val = raw_lamports as f64 / 1e9;
                                        let sol_price_usd = fetch_sol_price_usd().await.unwrap_or(0.0);
                                        let usd_val = sol_val * sol_price_usd;
                                        pnl_text = format!("Token: `{}`\nPNL: {}%\nAmount SOL: {:.6} SOL (${:.2})\n⏱️ Latency: {:?}", token, pnl_str, sol_val, usd_val, elapsed);
                                        found = true;
                                        break;
                                    }
                                }
                                
                                if !found {
                                    pnl_text = format!("Token: `{}`\nPNL: 0.00%\nAmount SOL: 0\n⏱️ Latency: {:?}", token, elapsed);
                                }
                            }
                        }
                    }
                    let _ = bot_clone.edit_message_text(chat_id, msg_id, pnl_text)
                        .reply_markup(panel_keyboard)
                        .await;
                });
                bot.answer_callback_query(q.id.clone()).text("Memuat data token...").await?;
            }
            "menu_seconddip" => {
                let text = seconddip_menu_text(&st);
                bot.edit_message_text(chat_id, msg_id, text).reply_markup(make_seconddip_keyboard(&st)).await?;
                bot.answer_callback_query(q.id.clone()).await?;
            }
            "menu_history" => {
                drop(st); // release lock before await
                let mut st2 = state.lock().await;
                bot.answer_callback_query(q.id.clone()).text("Menyinkronkan order dari Aura...").await?;
                // Sinkron dulu: order dari bot resmi Aura masuk ke DB lokal,
                // order yang sudah dihapus di Aura ikut terhapus dari Sonic.
                let (added, removed) = sync_orders_from_aura(&st2).await;
                if added > 0 || removed > 0 {
                    info!("[Sync Orders] menu_history: +{} baru, -{} dihapus", added, removed);
                }
                send_orders_grouped(&bot, chat_id, &mut st2).await?;
                return Ok(());
            }
            "orders_refresh" => {
                // Perbaikan 2026-11-21: tombol << Orders di detail token / detail
                // edit sebelumnya tidak berbuat apa-apa (hanya me-refresh pesan
                // History di atas, pesan detail tetap tampil). Sekarang: hapus
                // pesan detail itu, lalu refresh tampilan History di tempat —
                // tanpa pesan/tombol baru.
                let orders_mid = st.orders_msg.map(|(_, m)| m);
                let detail_mid = st.hist_detail_msg.map(|(_, m, _)| m);
                let is_detail = Some(msg_id) == detail_mid;
                drop(st); // release lock before await
                let mut st2 = state.lock().await;
                bot.answer_callback_query(q.id.clone()).text("Memuat ulang...").await?;
                if Some(msg_id) != orders_mid {
                    // Ditekan dari pesan detail (token / edit) — hapus pesan itu.
                    let _ = bot.delete_message(chat_id, msg_id).await;
                    if is_detail {
                        st2.hist_detail_msg = None;
                        st2.pending_hist_edit = None;
                        st2.edit_field = EditField::None;
                    }
                }
                if !refresh_orders_grouped(&bot, &mut st2).await {
                    send_orders_grouped(&bot, chat_id, &mut st2).await?;
                }
                return Ok(());
            }
            "orders_clear_all" => {
                bot.answer_callback_query(q.id.clone()).await?;
                let kb = InlineKeyboardMarkup::new(vec![
                    vec![InlineKeyboardButton::callback(
                        "✅ Ya, hapus semua order",
                        "orders_clear_yes",
                    )],
                    vec![InlineKeyboardButton::callback("❌ Batal", "orders_refresh")],
                ]);
                bot.send_message(
                    chat_id,
                    "⚠️ *Hapus SEMUA limit order aktif?*\n\nSemua order aktif di Aura akan dibatalkan dan dihapus dari database lokal.\n\nTindakan ini tidak bisa dibatalkan.",
                )
                .reply_markup(kb)
                .await?;
                return Ok(());
            }
            "orders_clear_yes" => {
                drop(st); // release lock before await
                bot.answer_callback_query(q.id.clone()).text("Menghapus semua order...").await?;
                let mut st2 = state.lock().await;
                // Hapus dari Aura per mint (all: true), lalu bersihkan DB lokal.
                if let Some(client) = st2.aura_client.clone() {
                    let sol_price = fetch_sol_price_usd().await.unwrap_or(0.0);
                    let local_targets = local_targets_map(&st2);
                    let grouped = fetch_active_orders_grouped(&client, sol_price, &local_targets).await;
                    for mint in grouped.keys() {
                        use std::str::FromStr;
                        if let Ok(m) = solana_address::Address::from_str(mint) {
                            let req = tonic::Request::new(aura_api_client::types::DeleteOrders {
                                mint: m,
                                all: true,
                                ids: vec![],
                            });
                            let _ = client.limit_orders().delete_limit_orders((), req).await;
                        }
                    }
                }
                if let Ok(conn) = st2.db_conn.try_lock() {
                    let _ = db::delete_all_limit_orders(&conn);
                }
                st2.orders_msg = None;
                bot.send_message(chat_id, "✅ Semua limit order aktif sudah dihapus.").await?;
                return Ok(());
            }
            "menu_lo_setup" => {
                bot.send_message(chat_id, "⚙️ *Limit Order Setup*\n\nTap salah satu preset untuk mengatur Tip & Priority Fee.\nNilai ini digunakan saat Anda memilih preset di history order.")
                    .reply_markup(make_setup_keyboard(&st)).await?;
                bot.answer_callback_query(q.id.clone()).await?;
            }
            "menu_lo_logs" => {
                drop(st);
                let st2 = state.lock().await;
                bot.answer_callback_query(q.id.clone()).await?;
                send_error_logs(&bot, chat_id, &st2).await?;
                return Ok(());
            }
            "clear_error_logs" => {
                if let Ok(conn) = st.db_conn.try_lock() {
                    let _ = db::clear_error_logs(&conn);
                }
                bot.edit_message_text(chat_id, msg_id, "✅ Semua log error telah dihapus.").await?;
                bot.answer_callback_query(q.id.clone()).await?;
            }
            "toggle_seconddip" => {
                st.second_dip_active = !st.second_dip_active;
                st.save_db();
                let text = seconddip_menu_text(&st);
                bot.edit_message_text(chat_id, msg_id, text).reply_markup(make_seconddip_keyboard(&st)).await?;
                bot.answer_callback_query(q.id.clone()).await?;
            }
            "toggle_seconddip_dryrun" => {
                st.second_dip_dry_run = !st.second_dip_dry_run;
                st.save_db();
                let text = seconddip_menu_text(&st);
                bot.edit_message_text(chat_id, msg_id, text).reply_markup(make_seconddip_keyboard(&st)).await?;
                bot.answer_callback_query(q.id.clone()).await?;
            }
            // (toggle_auto_sell_sd DIHAPUS — toggle pindah ke halaman Auto Sell
            // Setup, callback as_toggle_sd. Perintah user 2026-11-22.)
            "set_dip_mode_buydip" => {
                st.dip_mode = "BUY_DIP".to_string();
                st.save_db();
                let text = seconddip_menu_text(&st);
                bot.edit_message_text(chat_id, msg_id, text).reply_markup(make_seconddip_keyboard(&st)).await?;
                bot.answer_callback_query(q.id.clone()).await?;
            }
            "set_dip_mode_seconddip" => {
                st.dip_mode = "SECOND_DIP".to_string();
                st.save_db();
                let text = seconddip_menu_text(&st);
                bot.edit_message_text(chat_id, msg_id, text).reply_markup(make_seconddip_keyboard(&st)).await?;
                bot.answer_callback_query(q.id.clone()).await?;
            }
            // Limit Order Setup preset handlers
            "setup_preset_0" | "setup_preset_1" | "setup_preset_2" | "setup_preset_3" => {
                let idx: usize = data.trim_start_matches("setup_preset_").parse().unwrap_or(0);
                let label = match idx { 0 => "Kecil", 1 => "Sedang", 2 => "Besar", _ => "Mega" };
                st.edit_field = EditField::SetupPresetTip(idx);
                bot.answer_callback_query(q.id.clone()).text(format!("Edit {} Tip & Prio", label)).await?;
                bot.send_message(chat_id, format!("✏️ Ketik *Tip* untuk preset **{}** (contoh: `0.002`)", label))
                    .await?;
            }
            // Swap Sell Edits
            "edit_sell_tip" => {
                st.edit_field = EditField::SellTip;
                bot.answer_callback_query(q.id.clone()).text("Menunggu input tip...").await?;
                bot.send_message(chat_id, "✏️ Ketik nilai Tip Swap Sell (contoh: 0.005)").await?;
            }
            "edit_sell_prio" => {
                st.edit_field = EditField::SellPrio;
                bot.answer_callback_query(q.id.clone()).text("Menunggu input p.fee...").await?;
                bot.send_message(chat_id, "✏️ Ketik nilai P.Fee Swap Sell (contoh: 0.005)").await?;
            }
            "edit_sell_slippage" => {
                st.edit_field = EditField::SellSlippage;
                bot.answer_callback_query(q.id.clone()).text("Menunggu input slippage...").await?;
                bot.send_message(chat_id, "✏️ Ketik nilai Slippage Swap Sell (contoh: 95%)").await?;
            }
            // Second Dip — submenu Pemicu 1 / Pemicu 2 (klik tombol dulu, baru atur)
            "menu_sd_trigger1" | "edit_sd_trigger1" => {
                bot.answer_callback_query(q.id.clone()).text("Pemicu 1").await?;
                bot.edit_message_text(chat_id, msg_id, sd_trigger1_menu_text(&st))
                    .reply_markup(make_sd_trigger1_keyboard(&st))
                    .await?;
                st.sd_trigger1_msg = Some((chat_id, msg_id));
            }
            "menu_sd_trigger2" | "edit_sd_trigger2" => {
                bot.answer_callback_query(q.id.clone()).text("Pemicu 2").await?;
                bot.edit_message_text(chat_id, msg_id, sd_trigger2_menu_text(&st))
                    .reply_markup(make_sd_trigger2_keyboard(&st))
                    .await?;
                st.sd_trigger2_msg = Some((chat_id, msg_id));
            }
            "menu_auto_sell_setup" => {
                // Halaman Auto Sell Setup — 2 config (Limit Biasa + Second Dip).
                bot.answer_callback_query(q.id.clone()).text("Auto Sell Setup").await?;
                bot.edit_message_text(chat_id, msg_id, auto_sell_setup_text(&st))
                    .reply_markup(make_auto_sell_setup_keyboard(&st))
                    .await?;
                st.as_setup_msg = Some((chat_id, msg_id));
            }
            "as_tip_manual" => {
                st.edit_field = EditField::AutoSellTip;
                bot.answer_callback_query(q.id.clone()).text("Menunggu input Tip...").await?;
                let prompt_msg = bot.send_message(chat_id, "⚡ Ketik *Tip* (Limit Biasa) untuk order jual TP dalam SOL (contoh: `0.0035`).\n\nTip ini dikirim ke Aura saat bot pasang order jual TP.").await?;
                st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
            }
            "as_prio_manual" => {
                st.edit_field = EditField::AutoSellPrio;
                bot.answer_callback_query(q.id.clone()).text("Menunggu input P.Fee...").await?;
                let prompt_msg = bot.send_message(chat_id, "⛽ Ketik *Priority Fee* (Limit Biasa) untuk order jual TP dalam SOL (contoh: `0.0035`).\n\nP.Fee ini dikirim ke Aura saat bot pasang order jual TP.").await?;
                st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
            }
            "as_tip_sd" => {
                st.edit_field = EditField::AutoSellSdTip;
                bot.answer_callback_query(q.id.clone()).text("Menunggu input Tip...").await?;
                let prompt_msg = bot.send_message(chat_id, "⚡ Ketik *Tip* (Second Dip) untuk order jual TP dalam SOL (contoh: `0.012`).\n\nTip ini dikirim ke Aura saat bot pasang order jual TP.").await?;
                st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
            }
            "as_prio_sd" => {
                st.edit_field = EditField::AutoSellSdPrio;
                bot.answer_callback_query(q.id.clone()).text("Menunggu input P.Fee...").await?;
                let prompt_msg = bot.send_message(chat_id, "⛽ Ketik *Priority Fee* (Second Dip) untuk order jual TP dalam SOL (contoh: `0.012`).\n\nP.Fee ini dikirim ke Aura saat bot pasang order jual TP.").await?;
                st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
            }
            "as_toggle_manual" => {
                // Toggle ON/OFF Auto Sell Limit Biasa — dari halaman Setup
                // (perintah user 2026-11-22: toggle pindah ke Setup).
                // ON = order limit biasa ikut dilacak untuk TP; OFF = tidak.
                st.auto_sell_active = !st.auto_sell_active;
                st.save_db();
                bot.answer_callback_query(q.id.clone())
                    .text(format!(
                        "Auto Sell Limit Biasa {}",
                        if st.auto_sell_active {
                            "ON — order limit akan dilacak untuk TP"
                        } else {
                            "OFF — order limit tidak dilacak"
                        }
                    ))
                    .await?;
                if !refresh_auto_sell_setup(&bot, &st).await {
                    bot.edit_message_text(chat_id, msg_id, auto_sell_setup_text(&st))
                        .reply_markup(make_auto_sell_setup_keyboard(&st))
                        .await?;
                    st.as_setup_msg = Some((chat_id, msg_id));
                }
            }
            "as_toggle_sd" => {
                // Toggle ON/OFF Auto Sell Second Dip — dari halaman Setup.
                st.auto_sell_sd_active = !st.auto_sell_sd_active;
                st.save_db();
                bot.answer_callback_query(q.id.clone())
                    .text(format!(
                        "Auto Sell Second Dip {}",
                        if st.auto_sell_sd_active {
                            "ON — order Second Dip akan dilacak untuk TP"
                        } else {
                            "OFF — order Second Dip tidak dilacak"
                        }
                    ))
                    .await?;
                if !refresh_auto_sell_setup(&bot, &st).await {
                    bot.edit_message_text(chat_id, msg_id, auto_sell_setup_text(&st))
                        .reply_markup(make_auto_sell_setup_keyboard(&st))
                        .await?;
                    st.as_setup_msg = Some((chat_id, msg_id));
                }
            }
            "as_mode_manual" => {
                // Putar mode trigger: % <-> USD (sesuai permintaan user).
                st.auto_sell_tp_mode = match st.auto_sell_tp_mode.as_str() {
                    "USD" => "PCT".to_string(),
                    _ => "USD".to_string(),
                };
                st.save_db();
                bot.answer_callback_query(q.id.clone())
                    .text(format!("Mode TP Limit Biasa: {}", st.auto_sell_tp_mode))
                    .await?;
                if !refresh_auto_sell_setup(&bot, &st).await {
                    bot.edit_message_text(chat_id, msg_id, auto_sell_setup_text(&st))
                        .reply_markup(make_auto_sell_setup_keyboard(&st))
                        .await?;
                    st.as_setup_msg = Some((chat_id, msg_id));
                }
            }
            "as_mode_sd" => {
                st.auto_sell_sd_tp_mode = match st.auto_sell_sd_tp_mode.as_str() {
                    "USD" => "PCT".to_string(),
                    _ => "USD".to_string(),
                };
                st.save_db();
                bot.answer_callback_query(q.id.clone())
                    .text(format!("Mode TP Second Dip: {}", st.auto_sell_sd_tp_mode))
                    .await?;
                if !refresh_auto_sell_setup(&bot, &st).await {
                    bot.edit_message_text(chat_id, msg_id, auto_sell_setup_text(&st))
                        .reply_markup(make_auto_sell_setup_keyboard(&st))
                        .await?;
                    st.as_setup_msg = Some((chat_id, msg_id));
                }
            }
            "as_tp_manual" => {
                // Arahkan input sesuai mode TP (PCT / USD / MCAP legacy).
                match st.auto_sell_tp_mode.as_str() {
                    "USD" => {
                        st.edit_field = EditField::AutoSellTpUsd;
                        bot.answer_callback_query(q.id.clone()).text("Menunggu input harga USD...").await?;
                        let prompt_msg = bot.send_message(chat_id, "💲 Ketik *Harga Take Profit* (Limit Biasa) dalam USD (contoh: `0.000004`).\n\nOrder jual dibuat di harga absolut ini (arah Above).").await?;
                        st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
                    }
                    "MCAP" => {
                        st.edit_field = EditField::AutoSellTpMcap;
                        bot.answer_callback_query(q.id.clone()).text("Menunggu input mcap...").await?;
                        let prompt_msg = bot.send_message(chat_id, "📊 Ketik *Mcap Take Profit* (Limit Biasa) dalam USD (contoh: `100000` = $100K mcap, `2500000` = $2.5M mcap).\n\nOrder jual trigger saat market cap menyentuh nilai ini.").await?;
                        st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
                    }
                    _ => {
                        st.edit_field = EditField::AutoSellTp;
                        bot.answer_callback_query(q.id.clone()).text("Menunggu input TP...").await?;
                        let prompt_msg = bot.send_message(chat_id, "✏️ Ketik *Take Profit* (Limit Biasa) — % di atas harga beli (contoh: `40` = +40%).").await?;
                        st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
                    }
                }
            }
            "as_tp_sd" => {
                match st.auto_sell_sd_tp_mode.as_str() {
                    "USD" => {
                        st.edit_field = EditField::AutoSellSdTpUsd;
                        bot.answer_callback_query(q.id.clone()).text("Menunggu input harga USD...").await?;
                        let prompt_msg = bot.send_message(chat_id, "💲 Ketik *Harga Take Profit* (Second Dip) dalam USD (contoh: `0.000004`).\n\nOrder jual dibuat di harga absolut ini (arah Above).").await?;
                        st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
                    }
                    "MCAP" => {
                        st.edit_field = EditField::AutoSellSdTpMcap;
                        bot.answer_callback_query(q.id.clone()).text("Menunggu input mcap...").await?;
                        let prompt_msg = bot.send_message(chat_id, "📊 Ketik *Mcap Take Profit* (Second Dip) dalam USD (contoh: `100000` = $100K mcap, `2500000` = $2.5M mcap).\n\nOrder jual trigger saat market cap menyentuh nilai ini.").await?;
                        st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
                    }
                    _ => {
                        st.edit_field = EditField::AutoSellSdTp;
                        bot.answer_callback_query(q.id.clone()).text("Menunggu input TP...").await?;
                        let prompt_msg = bot.send_message(chat_id, "✏️ Ketik *Take Profit* (Second Dip) — % di atas harga beli (contoh: `40` = +40%).").await?;
                        st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
                    }
                }
            }
            "as_sellpct_manual" => {
                st.edit_field = EditField::AutoSellSellPct;
                bot.answer_callback_query(q.id.clone()).text("Menunggu input % jual...").await?;
                let prompt_msg = bot.send_message(chat_id, "💰 Ketik *% posisi yang dijual* (Limit Biasa) saat TP kena (contoh: `100` = jual semua, `50` = jual setengah, `25` = jual seperempat).\n\nHarus 1–100.").await?;
                st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
            }
            "as_sellpct_sd" => {
                st.edit_field = EditField::AutoSellSdSellPct;
                bot.answer_callback_query(q.id.clone()).text("Menunggu input % jual...").await?;
                let prompt_msg = bot.send_message(chat_id, "💰 Ketik *% posisi yang dijual* (Second Dip) saat TP kena (contoh: `100` = jual semua, `50` = jual setengah, `25` = jual seperempat).\n\nHarus 1–100.").await?;
                st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
            }
            // (toggle_auto_sell_lb DIHAPUS — toggle pindah ke halaman Auto Sell
            // Setup, callback as_toggle_manual. Perintah user 2026-11-22.)
            "edit_sd_trigger1_price" => {
                // Arahkan input sesuai mode harga Pemicu 1 (PCT / USD).
                match st.second_dip_trigger1_mode.as_str() {
                    "USD" => {
                        st.edit_field = EditField::SdTrigger1Usd;
                        bot.answer_callback_query(q.id.clone()).text("Menunggu input harga USD...").await?;
                        let prompt_msg = bot.send_message(chat_id, "💲 Ketik *Harga Pemicu 1* dalam USD (contoh: `0.00001`).\n\nLimit order dibuat saat harga menyentuh harga absolut ini.").await?;
                        st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
                    }
                    _ => {
                        st.edit_field = EditField::SdTrigger1;
                        bot.answer_callback_query(q.id.clone()).text("Menunggu input harga pemicu 1...").await?;
                        let prompt_msg = bot.send_message(chat_id, "✏️ Ketik *Harga Pemicu 1* — % kenaikan dari entry untuk membuat limit order (contoh: `100` = +100%).\n\nHarus lebih besar dari Pemicu 2.").await?;
                        st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
                    }
                }
            }
            "cycle_sd_trigger1_mode" => {
                // Putar mode: PCT -> USD -> PCT.
                st.second_dip_trigger1_mode = match st.second_dip_trigger1_mode.as_str() {
                    "USD" => "PCT".to_string(),
                    _ => "USD".to_string(),
                };
                st.save_db();
                bot.answer_callback_query(q.id.clone()).text(format!("Mode Pemicu 1: {}", st.second_dip_trigger1_mode)).await?;
                bot.edit_message_text(chat_id, msg_id, sd_trigger1_menu_text(&st))
                    .reply_markup(make_sd_trigger1_keyboard(&st))
                    .await?;
                st.sd_trigger1_msg = Some((chat_id, msg_id));
            }
            "edit_sd_trigger1_timeout" => {
                st.edit_field = EditField::SdTrigger1Timeout;
                bot.answer_callback_query(q.id.clone()).text("Menunggu input batas waktu...").await?;
                let prompt_msg = bot.send_message(chat_id, "⏰ Ketik *Batas Waktu Pemicu 1* dalam menit (contoh: `120` = 2 jam, `60` = 1 jam).\n\nKalau harga tidak naik ke Pemicu 1 dalam waktu ini, target dibatalkan.\nKetik `0` = tidak ada batas waktu.").await?;
                st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
            }
            "edit_sd_trigger2_price" => {
                // Arahkan input sesuai mode harga Pemicu 2 (PCT / USD / MCAP).
                match st.second_dip_trigger2_mode.as_str() {
                    "USD" => {
                        st.edit_field = EditField::SdTrigger2Usd;
                        bot.answer_callback_query(q.id.clone()).text("Menunggu input harga USD...").await?;
                        let prompt_msg = bot.send_message(chat_id, "💲 Ketik *Harga Pemicu 2* dalam USD (contoh: `0.0000012`).\n\nLimit order dibuat di harga absolut ini (Buy Dip, arah Below).").await?;
                        st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
                    }
                    "MCAP" => {
                        st.edit_field = EditField::SdTrigger2Mcap;
                        bot.answer_callback_query(q.id.clone()).text("Menunggu input mcap...").await?;
                        let prompt_msg = bot.send_message(chat_id, "📊 Ketik *Mcap Pemicu 2* dalam USD (contoh: `100000` = $100K mcap, `2500000` = $2.5M mcap).\n\nLimit order trigger saat market cap menyentuh nilai ini.").await?;
                        st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
                    }
                    _ => {
                        st.edit_field = EditField::SdTrigger2;
                        bot.answer_callback_query(q.id.clone()).text("Menunggu input harga pemicu 2...").await?;
                        let prompt_msg = bot.send_message(chat_id, "✏️ Ketik *Harga Pemicu 2* — % di atas entry untuk harga beli di limit order (contoh: `20` = +20%).\n\nHarus lebih kecil dari Pemicu 1.").await?;
                        st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
                    }
                }
            }
            "cycle_sd_trigger2_mode" => {
                // Putar mode: PCT -> USD -> MCAP -> PCT.
                st.second_dip_trigger2_mode = match st.second_dip_trigger2_mode.as_str() {
                    "USD" => "MCAP".to_string(),
                    "MCAP" => "PCT".to_string(),
                    _ => "USD".to_string(),
                };
                st.save_db();
                bot.answer_callback_query(q.id.clone()).text(format!("Mode Pemicu 2: {}", st.second_dip_trigger2_mode)).await?;
                bot.edit_message_text(chat_id, msg_id, sd_trigger2_menu_text(&st))
                    .reply_markup(make_sd_trigger2_keyboard(&st))
                    .await?;
                st.sd_trigger2_msg = Some((chat_id, msg_id));
            }
            "sd_trigger2_fixed_info" => {
                bot.answer_callback_query(q.id.clone()).text("Pengaturan fixed").await?;
                bot.send_message(chat_id, "🔒 *Pengaturan Fixed Pemicu 2* (tidak bisa diubah, sesuai Aura):\n\n• *Slippage:* 100%\n• *Trigger:* Instant — order aktif langsung saat harga target kena (`OrderEventTrigger::Immediate`)\n• *Fee mode:* Exact — jumlah beli $26 exact dalam lamports (`SwapAmount::Buy`), bukan persen\n• *Nonce:* Hybrid\n• *Processor:* Aura + Helius + 11 router lain").await?;
            }
            "edit_sd_trigger2_timeout" => {
                st.edit_field = EditField::SdTrigger2Timeout;
                bot.answer_callback_query(q.id.clone()).text("Menunggu input batas waktu...").await?;
                let prompt_msg = bot.send_message(chat_id, "⏰ Ketik *Batas Waktu Pemicu 2* (masa berlaku order) dalam menit (contoh: `1440` = 24 jam, `720` = 12 jam).\n\nKalau order tidak fill dalam waktu ini, order kedaluwarsa dan batal.\nKetik `0` = pakai default Aura.").await?;
                st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
            }
            "edit_sd_tip" => {
                st.edit_field = EditField::SdTip;
                bot.answer_callback_query(q.id.clone()).text("Menunggu input tip...").await?;
                let prompt_msg = bot.send_message(chat_id, "✏️ Ketik nilai Tip Second Dip (contoh: 0.0035)").await?;
                st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
            }
            "edit_sd_prio" => {
                st.edit_field = EditField::SdPrio;
                bot.answer_callback_query(q.id.clone()).text("Menunggu input p.fee...").await?;
                let prompt_msg = bot.send_message(chat_id, "✏️ Ketik nilai P.Fee Second Dip (contoh: 0.0035)").await?;
                st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
            }
            "edit_sd_amount" => {
                st.edit_field = EditField::SdAmount;
                bot.answer_callback_query(q.id.clone()).text("Menunggu input jumlah...").await?;
                let prompt_msg = bot.send_message(chat_id, "✏️ Ketik jumlah beli Second Dip dalam USD (contoh: `26`)").await?;
                st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
            }
            "menu_sd_targets" => {
                let text = sd_targets_text(&st.second_dip_targets);
                let kb = make_sd_targets_keyboard(&st.second_dip_targets);
                bot.edit_message_text(chat_id, msg_id, text).reply_markup(kb).await?;
                bot.answer_callback_query(q.id.clone()).await?;
            }
            "sd_add_target" => {
                st.edit_field = EditField::SdWallet;
                bot.answer_callback_query(q.id.clone()).text("Menunggu input wallet...").await?;
                bot.send_message(chat_id, "✏️ Tempel address wallet target Solana (akan DITAMBAHKAN ke daftar, bukan mengganti).\n\nContoh 9S7hhP: `AURARwy7F1sfbxeRkdcUCVg2eG1mpeHG1jChRU9S7hhP`").await?;
            }
            // Buy Limit Edits
            "edit_buy_amount" => {
                // Catat panel yang tombolnya ditekan — supaya nilai yang
                // diketik nanti meng-update panel INI, bukan kirim panel baru.
                st.limitbuy_msg = Some((chat_id, msg_id));
                st.edit_field = EditField::BuyAmount;
                bot.answer_callback_query(q.id.clone()).text("Menunggu input jumlah...").await?;
                let prompt_msg = bot.send_message(chat_id, "✏️ Ketik jumlah beli baru beserta formatnya (contoh: `10 USD`, `5 USDT`, atau `0.5 SOL`).\n\nJika angka saja, akan menggunakan format yang sedang aktif.").parse_mode(teloxide::types::ParseMode::Markdown).await?;
                st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
            }
            "edit_buy_target" => {
                // Buka picker tipe Target ala Aura (5 pilihan) di tempat —
                // ganti isi panel, bukan kirim pesan baru. Memilih tipe
                // HANYA mengatur target; order TIDAK dikirim ke gRPC
                // sampai user tekan Place Order (lapis pengaman).
                st.limitbuy_msg = Some((chat_id, msg_id));
                st.edit_field = EditField::None;
                bot.answer_callback_query(q.id.clone()).text("Pilih tipe target...").await?;
                if bot
                    .edit_message_text(chat_id, msg_id, target_picker_text())
                    .reply_markup(make_target_picker_keyboard())
                    .await
                    .is_err()
                {
                    let _ = bot
                        .edit_message_reply_markup(chat_id, msg_id)
                        .reply_markup(make_target_picker_keyboard())
                        .await;
                }
            }
            "tt_market" => {
                // Market: HANYA set target jadi "Market" — TIDAK kirim ke
                // gRPC. Order baru dikirim saat user tekan Place Order
                // (lapis pengaman, perintah user 2026-11-22).
                st.buy_target = "Market".to_string();
                st.edit_field = EditField::None;
                bot.answer_callback_query(q.id.clone()).text("Target: Market").await?;
                let _ = refresh_limitbuy_menu(&bot, &st).await;
            }
            "tt_usd" => {
                st.edit_field = EditField::BuyTargetUsd;
                bot.answer_callback_query(q.id.clone()).text("Ketik harga USD...").await?;
                let _ = refresh_limitbuy_menu(&bot, &st).await;
                let prompt_msg = bot.send_message(chat_id,
                    "💲 Ketik harga target dalam USD (contoh: `0.000005$` atau `$1`)",
                ).await?;
                st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
            }
            "tt_pct" => {
                st.edit_field = EditField::BuyTargetPct;
                bot.answer_callback_query(q.id.clone()).text("Ketik persen...").await?;
                let _ = refresh_limitbuy_menu(&bot, &st).await;
                let prompt_msg = bot.send_message(chat_id,
                    "📊 Ketik persen target (contoh: `-20%` beli saat turun 20%, `80%` beli saat naik 80%)",
                ).await?;
                st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
            }
            "tt_mcap" => {
                st.edit_field = EditField::BuyTargetMcap;
                bot.answer_callback_query(q.id.clone()).text("Ketik mcap...").await?;
                let _ = refresh_limitbuy_menu(&bot, &st).await;
                let prompt_msg = bot.send_message(chat_id,
                    "🏦 Ketik mcap target dalam USD (contoh: `100K mcap` | `11M mcap` | `2500000`)",
                ).await?;
                st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
            }
            "tt_pnl" => {
                st.edit_field = EditField::BuyTargetPnl;
                bot.answer_callback_query(q.id.clone()).text("Ketik PNL...").await?;
                let _ = refresh_limitbuy_menu(&bot, &st).await;
                let prompt_msg = bot.send_message(chat_id,
                    "📈 Ketik PNL target (contoh: `70%pnl` atau `70`)",
                ).await?;
                st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
            }
            "tt_back" => {
                st.edit_field = EditField::None;
                bot.answer_callback_query(q.id.clone()).await?;
                let _ = refresh_limitbuy_menu(&bot, &st).await;
            }
            "edit_buy_tip" => {
                st.limitbuy_msg = Some((chat_id, msg_id));
                st.edit_field = EditField::BuyTip;
                bot.answer_callback_query(q.id.clone()).text("Menunggu input tip...").await?;
                let prompt_msg = bot.send_message(chat_id, "✏️ Ketik nilai Tip baru (contoh: 0.005)").await?;
                st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
            }
            "edit_buy_prio" => {
                st.limitbuy_msg = Some((chat_id, msg_id));
                st.edit_field = EditField::BuyPrio;
                bot.answer_callback_query(q.id.clone()).text("Menunggu input p.fee...").await?;
                let prompt_msg = bot.send_message(chat_id, "✏️ Ketik nilai P.Fee baru (contoh: 0.005)").await?;
                st.sd_prompt_msg = Some((chat_id, prompt_msg.id));
            }
            "toggle_buy_mode" => {
                if st.buy_mode == "LIMIT" {
                    st.buy_mode = "SWAP".to_string();
                } else {
                    st.buy_mode = "LIMIT".to_string();
                }
                let new_mode = st.buy_mode.clone();
                bot.answer_callback_query(q.id.clone()).text(format!("Mode diubah ke {}", new_mode)).await?;
                if st.active_token.is_some() {
                    // Edit panel yang tombolnya ditekan, di tempat (bukan pesan baru).
                    // Kalau teks tidak berubah, Telegram tolak edit_message_text —
                    // fallback update keyboard saja supaya label tombol tetap berubah.
                    if bot
                        .edit_message_text(chat_id, msg_id, limitbuy_menu_text(&st))
                        .reply_markup(make_limitbuy_keyboard(&st))
                        .await
                        .is_err()
                    {
                        let _ = bot
                            .edit_message_reply_markup(chat_id, msg_id)
                            .reply_markup(make_limitbuy_keyboard(&st))
                            .await;
                    }
                    st.limitbuy_msg = Some((chat_id, msg_id));
                }
            }
            "toggle_buy_currency" => {
                if st.buy_currency == "USD" {
                    st.buy_currency = "SOL".to_string();
                } else {
                    st.buy_currency = "USD".to_string();
                }
                let label = st.buy_currency.clone();
                bot.answer_callback_query(q.id.clone()).text(format!("Format diubah ke {}", label)).await?;
                if st.active_token.is_some() {
                    // Edit panel yang tombolnya ditekan, di tempat (bukan pesan baru).
                    // Kalau teks tidak berubah, Telegram tolak edit_message_text —
                    // fallback update keyboard saja supaya label tombol tetap berubah.
                    if bot
                        .edit_message_text(chat_id, msg_id, limitbuy_menu_text(&st))
                        .reply_markup(make_limitbuy_keyboard(&st))
                        .await
                        .is_err()
                    {
                        let _ = bot
                            .edit_message_reply_markup(chat_id, msg_id)
                            .reply_markup(make_limitbuy_keyboard(&st))
                            .await;
                    }
                    st.limitbuy_msg = Some((chat_id, msg_id));
                }
            }
            "place_swap_buy" => {
                if let Some(client) = st.aura_client.clone() {
                    if let Some(token) = st.active_token.clone() {
                        let bot_clone = bot.clone();
                        let tip_fee = st.buy_tip_fee;
                        let prio_fee = st.buy_prio_fee;
                        let buy_val = st.buy_amount;
                        let buy_cur = st.buy_currency.clone();
                        let chat_id_clone = chat_id;
                        bot.answer_callback_query(q.id.clone()).text("Memproses Swap Buy...").await?;
                        tokio::spawn(async move {
                            use aura_api_client::types::{MarketTrade, UserNonceStrategy, ApiOrders, TradeFilters};
                            use std::str::FromStr;
                            let _ = bot_clone.send_message(chat_id_clone, "⏳ Memproses Swap Buy ke Aura...").await;
                            match solana_address::Address::from_str(&token) {
                                Ok(mint_addr) => {
                                    let tip_lamports = (tip_fee * 1e9) as u64;
                                    let prio_lamports = (prio_fee * 1e9) as u64;
                                    
                                    // Bayar sesuai currency (USD -> USDC, SOL -> SOL).
                                    let swap_buy_amount = buy_swap_amount(buy_val, &buy_cur);
                                    // Untuk tampilan saja: ekuivalen SOL.
                                    let buy_amount_sol = if buy_cur == "SOL" {
                                        buy_val
                                    } else {
                                        let sol_price_usd = fetch_sol_price_usd().await.unwrap_or(0.0);
                                        if sol_price_usd > 0.0 { buy_val / sol_price_usd } else { 0.0 }
                                    };

                                    let slippage_scaled = (90.0_f64 / 100.0 * 1_000_000.0) as u64;
                                    let slippage_val = fastnum::UD128::from(slippage_scaled) / fastnum::UD128::from(1_000_000u64);
                                    let success_text = format!(
                                                "✅ *Swap Buy Ditempatkan!*\n\n\
                                                🏦 Token: `{}`\n\
                                                💰 Amount: `{:.2} {}` (≈ `{:.6} SOL`)\n\
                                                ⚡ Tip: {} SOL | ⛽ Prio: {} SOL\n\n\
                                                _Transaksi telah dikirim ke Aura (Market Swap)._",
                                                token, buy_val, buy_cur, buy_amount_sol, tip_fee, prio_fee
                                            );
                                    let req = tonic::Request::new(MarketTrade {
                                        wallet: None,
                                        amount: swap_buy_amount,
                                        mint: mint_addr,
                                        slippage: slippage_val,
                                        tip: decisol::Lamports::from(tip_lamports),
                                        priority_fee: decisol::Lamports::from(prio_lamports),
                                        procs: None,
                                        nonce: UserNonceStrategy::Hybrid,
                                        slot_latency: None,
                                        expire_at: None,
                                        rpc_nonce: None,
                                        max_price_impact: None,
                                        limit_orders: ApiOrders { orders: vec![] },
                                        filters: TradeFilters { min_mcap: None, max_mcap: None },
                                    });
                                    match client.aura().trade((), req).await {
                                        Ok(_) => {
                                            let _ = bot_clone.send_message(chat_id_clone, success_text)
                                                .parse_mode(teloxide::types::ParseMode::Markdown).await;
                                        }
                                        Err(e) => {
                                            let _ = bot_clone.send_message(chat_id_clone, format!("❌ Swap Buy Gagal: {}", e.message())).await;
                                        }
                                    }
                                }
                                Err(_) => {
                                    let _ = bot_clone.send_message(chat_id_clone, "❌ Alamat token tidak valid!").await;
                                }
                            }
                        });
                    } else {
                        bot.answer_callback_query(q.id.clone()).text("Pilih token terlebih dahulu").await?;
                    }
                } else {
                    bot.send_message(chat_id, "❌ Client Aura gRPC belum terhubung.").await?;
                    bot.answer_callback_query(q.id.clone()).await?;
                }
            }
            "place_limit_buy" => {
                if let Some(token) = st.active_token.clone() {
                    let target_str = st.buy_target.clone();
                    let buy_val = st.buy_amount;
                    let buy_cur = st.buy_currency.clone();
                    let tip_fee = st.buy_tip_fee;
                    let prio_fee = st.buy_prio_fee;
                    let order_id = st.next_order_id;

                    let o = LimitOrder {
                        id: order_id,
                        token: token.clone(),
                        amount_usd: buy_val,
                        target: target_str.clone(),
                        tip_fee,
                        prio_fee,
                        aura_order_id: None,
                    };
                    // Save to SQLite
                    let mut db_id = 0;
                    if let Ok(conn) = st.db_conn.try_lock() {
                        if let Ok(id) = db::insert_limit_order(&conn, "BUY", &token, &target_str, tip_fee, prio_fee, buy_val) {
                            db_id = id;
                        }
                    }
                    st.orders.push(o);
                    st.next_order_id += 1;

                    // Kirim ke Aura gRPC API
                    if let Some(client) = st.aura_client.clone() {
                        use aura_api_client::types::{
                            ApiLimitOrder, ApiOrders, Direction, MarketExecuteMode,
                            OrderEventTrigger, OrderState, RawOrder, Target,
                            TxnProcessors, UpdateTokenLimitOrders, UserNonceStrategy,
                        };
                        use std::str::FromStr;

                        let token_clone = token.clone();
                        let bot_clone = bot.clone();
                        let target_display = format_target_display(&target_str);
                        let slippage_str = st.sell_slippage.clone();
                        // Harga panel saat ini (Dexscreener, USD) — dipakai sebagai
                        // harga beli untuk pelacakan Auto Sell kalau targetnya Market.
                        let market_price_opt = st.active_token_price;
                        let db_conn = st.db_conn.clone();
                        let state_clone = state.clone();

                        tokio::spawn(async move {
                            let Ok(mint_addr) = solana_address::Address::from_str(&token_clone) else {
                                let _ = bot_clone.send_message(chat_id, "❌ Alamat token tidak valid!").await;
                                return;
                            };

                            let tip_lam = (tip_fee * 1e9) as u64;
                            let prio_lam = (prio_fee * 1e9) as u64;

                            // Selalu fetch harga SOL — dibutuhkan untuk konversi amount USD dan target harga USD
                            let sol_price_usd = match fetch_sol_price_usd().await {
                                Some(price) => price,
                                None => {
                                    let _ = bot_clone.send_message(chat_id, "❌ Gagal mengambil harga SOL dari API. Batal mengirim order ke Aura.").await;
                                    return;
                                }
                            };

                            // Bayar sesuai currency yang dipilih user
                            // (USD -> USDC langsung, SOL -> SOL lamports).
                            let buy_amount = buy_swap_amount(buy_val, &buy_cur);

                            // Parse target string → Aura Target enum
                            let aura_target_opt: Option<Target> = {
                                let s = target_str.trim();
                                let lower = s.to_lowercase();
                                if lower == "market" {
                                    // Market Buy: limit order tipe Market (mode Always) —
                                    // tereksekusi langsung di harga pasar, tapi tetap
                                    // order limit (dapat aura_order_id) sehingga ikut
                                    // dilacak Auto Sell.
                                    Some(Target::Market { mode: MarketExecuteMode::Always })
                                } else if s.contains('%') {
                                    // PricePerc: misalnya "-20%" berarti beli kalau harga turun 20%
                                    let num_part: String = s.chars().filter(|&c| c == '-' || c == '.' || c.is_ascii_digit()).collect();
                                    if let Ok(perc_f64) = num_part.parse::<f64>() {
                                        let abs_perc = perc_f64.abs();
                                        let scaled = (abs_perc / 100.0 * 1_000_000.0) as u64;
                                        let price_perc = fastnum::UD128::from(scaled) / fastnum::UD128::from(1_000_000u64);
                                        Some(Target::PricePerc { price_perc, price: None, direction: Direction::Below })
                                    } else {
                                        None
                                    }
                                } else if lower.contains("mcap") || lower.contains("cap") || lower.ends_with('k') || lower.ends_with('m') {
                                    // Mcap target (dalam USD, Aura terima langsung)
                                    let num_str = s.replace(|c: char| c.is_alphabetic(), "").trim().to_string();
                                    let multiplier: f64 = if lower.ends_with('k') { 1_000.0 }
                                        else if lower.ends_with('m') { 1_000_000.0 }
                                        else { 1.0 };
                                    if let Ok(val) = num_str.parse::<f64>() {
                                        let mcap_val = val * multiplier;
                                        let scaled = (mcap_val * 1_000_000.0) as u64;
                                        let mcap_ud128 = fastnum::UD128::from(scaled) / fastnum::UD128::from(1_000_000u64);
                                        Some(Target::Mcap { mcap: mcap_ud128, price: None, direction: Direction::Below })
                                    } else {
                                        None
                                    }
                                } else {
                                    // Price USD: angka dengan atau tanpa '$'
                                    // Dibagi harga SOL dulu (perintah user 2026-11-21),
                                    // supaya tampilan di Aura sama dengan harga yang dimasukkan.
                                    let stripped = s.trim_start_matches('$').trim_end_matches('$').trim();
                                    if let Ok(price_usd) = stripped.parse::<f64>() {
                                        if price_usd > 0.0 {
                                            let price_sol = price_usd / sol_price_usd;
                                            info!(
                                                "[Limit Buy] Target: {}$ USD / SOL ${} = {} SOL (dikirim sebagai UD128)",
                                                price_usd, sol_price_usd, price_sol
                                            );
                                            // Scale dengan 1_000_000_000_000 untuk presisi harga micro
                                            let scale: u64 = 1_000_000_000_000;
                                            let scaled = (price_sol * scale as f64) as u64;
                                            let price_ud128 = fastnum::UD128::from(scaled) / fastnum::UD128::from(scale);
                                            Some(Target::Price { price: price_ud128, direction: Direction::Below })
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                }
                            };

                            let Some(aura_target) = aura_target_opt else {
                                let _ = bot_clone.send_message(chat_id,
                                    format!("❌ Gagal parse target `{}` ke format Aura. Order disimpan lokal saja.", target_display)
                                ).await;
                                return;
                            };

                            let slippage_f64 = slippage_str.replace("%", "").trim().parse::<f64>().unwrap_or(95.0);
                            let slippage_scaled = (slippage_f64 / 100.0 * 1_000_000.0) as u64;
                            let slippage_val = fastnum::UD128::from(slippage_scaled) / fastnum::UD128::from(1_000_000u64);

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
                                    target: aura_target,
                                    amount: buy_amount,
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
                                wallet: {
                                    let mut trade_client = client.aura();
                                    match trade_client.fetch_full_wallet_info((), tonic::Request::new(aura_api_client::types::FetchFullWalletsInfoReq {})).await {
                                        Ok(res) => {
                                            if let Some(w) = res.into_inner().wallets.get(0).cloned() {
                                                w
                                            } else {
                                                let _ = bot_clone.send_message(chat_id, "❌ Gagal menemukan wallet. Order menggunakan default.").await;
                                                solana_address::Address::default()
                                            }
                                        }
                                        Err(e) => {
                                            let _ = bot_clone.send_message(chat_id, format!("❌ Gagal fetch wallet: {}", e.message())).await;
                                            solana_address::Address::default()
                                        }
                                    }
                                },
                            };

                            let req = tonic::Request::new(UpdateTokenLimitOrders {
                                mint: mint_addr,
                                orders: ApiOrders { orders: vec![api_order] },
                            });

                            let short = if token_clone.len() >= 10 {
                                format!("{}...{}", &token_clone[..6], &token_clone[token_clone.len()-4..])
                            } else {
                                token_clone.clone()
                            };

                            match client.limit_orders().place_limit_orders((), req).await {
                                Ok(resp) => {
                                    if let Some(aura_id) = resp.into_inner().ids.get(0).copied().map(|x| x.0) {
                                        if let Ok(conn) = db_conn.try_lock() {
                                            let _ = crate::db::update_aura_order_id(&conn, db_id, aura_id);
                                        }
                                        // Auto Sell: catat order beli manual untuk dilacak
                                        // (kalau toggle ON). Hanya untuk target harga USD
                                        // (angka / "$..."); target % / mcap dilewati karena
                                        // harga belinya relatif terhadap harga saat itu.
                                        let buy_price_opt: Option<f64> = {
                                            let s = target_str.trim();
                                            let lower = s.to_lowercase();
                                            if lower == "market" {
                                                // Market Buy: harga beli = harga panel saat
                                                // ini (Dexscreener, display) — dipakai
                                                // watcher untuk hitung trigger TP (%).
                                                // Harga isi sebenarnya bisa sedikit beda.
                                                market_price_opt.filter(|v| *v > 0.0)
                                            } else if s.contains('%')
                                                || lower.contains("mcap")
                                                || lower.contains("cap")
                                                || lower.ends_with('k')
                                                || lower.ends_with('m')
                                            {
                                                None
                                            } else {
                                                let stripped = s
                                                    .trim_start_matches('$')
                                                    .trim_end_matches('$')
                                                    .trim();
                                                stripped.parse::<f64>().ok().filter(|v| *v > 0.0)
                                            }
                                        };
                                        if let Some(buy_price) = buy_price_opt {
                                            let amount_usd = if buy_cur == "SOL" {
                                                buy_val * sol_price_usd
                                            } else {
                                                buy_val
                                            };
                                            crate::seconddip::maybe_track_auto_sell(
                                                &state_clone,
                                                aura_id,
                                                &token_clone,
                                                buy_price,
                                                amount_usd,
                                                tip_fee,
                                                prio_fee,
                                                "manual",
                                            )
                                            .await;
                                        }
                                    }
                                    // Beralih ke History (tampilan baru: dikelompokkan per token).
                                    let mut st_ref = state_clone.lock().await;
                                    let _ = send_orders_grouped(&bot_clone, chat_id, &mut st_ref).await;
                                }
                                Err(e) => {
                                    error!("[Limit Buy] Gagal place_limit_orders: {:?}", e);
                                    let _ = bot_clone.send_message(chat_id,
                                        format!("❌ Gagal mengirim Limit Buy ke Aura!\n🏦 Token: `{}`\n🔴 Error: {}\n\n_Order sudah disimpan lokal._", short, e.message())
                                    ).await;
                                }
                            }
                        });

                        bot.answer_callback_query(q.id.clone()).text("⏳ Mengirim order ke Aura...").await?;
                    } else {
                        bot.answer_callback_query(q.id.clone()).text("Order disimpan lokal (no gRPC)!").await?;
                        bot.send_message(chat_id, "⚠️ Koneksi Aura gRPC tidak tersedia. Order disimpan lokal saja.\n\n📋 Buka *Limit Order History* untuk melihat.").await?;
                    }
                } else {
                    bot.answer_callback_query(q.id.clone()).text("Tidak ada token aktif!").await?;
                }
            }
            "execute_swap_sell" => {
                if let Some(client) = st.aura_client.clone() {
                    if let Some(token) = st.active_token.clone() {
                        let bot_clone = bot.clone();
                        let tip_fee = st.sell_tip_fee;
                        let prio_fee = st.sell_prio_fee;
                        let slippage_str = st.sell_slippage.clone();
                        let chat_id_clone = chat_id;
                        // Ambil daftar panel yg perlu diedit setelah sell berhasil
                        let panel_msgs = st.swap_panel_msgs.clone();
                        st.swap_panel_msgs.clear(); // clear dulu agar tidak diedit ulang
                        st.bought_token = None; // hapus tombol konfirmasi
                        st.save_db();
                        
                        tokio::spawn(async move {
                            use aura_api_client::types::{MarketTrade, SwapAmount, UserNonceStrategy, ApiOrders, TradeFilters};
                            use std::str::FromStr;
                            
                            let _ = bot_clone.send_message(chat_id_clone, "⏳ Memproses Swap Sell Manual 100% ke Aura...").await;
                            
                            if let Ok(mint_addr) = solana_address::Address::from_str(&token) {
                                let tip_lamports = (tip_fee * 1e9) as u64;
                                let prio_lamports = (prio_fee * 1e9) as u64;
                                
                                let slippage_f64 = slippage_str.replace("%", "").trim().parse::<f64>().unwrap_or(15.0);
                                let slippage_scaled = (slippage_f64 / 100.0 * 1_000_000.0) as u64;
                                let slippage_val = fastnum::UD128::from(slippage_scaled) / fastnum::UD128::from(1_000_000u64);
                                
                                let req = tonic::Request::new(MarketTrade {
                                    wallet: None,
                                    amount: SwapAmount::SellPerc { amount: fastnum::udec128!(1) },
                                    mint: mint_addr,
                                    slippage: slippage_val,
                                    tip: decisol::Lamports::from(tip_lamports),
                                    priority_fee: decisol::Lamports::from(prio_lamports),
                                    procs: None,
                                    nonce: UserNonceStrategy::Hybrid,
                                    slot_latency: None,
                                    expire_at: None,
                                    rpc_nonce: None,
                                    max_price_impact: None,
                                    limit_orders: ApiOrders { orders: vec![] },
                                    filters: TradeFilters { min_mcap: None, max_mcap: None },
                                });

                                match client.aura().trade((), req).await {
                                    Ok(_resp) => {
                                        let short = if token.len() >= 10 {
                                            format!("{}...{}", &token[..6], &token[token.len()-4..])
                                        } else {
                                            token.clone()
                                        };
                                        let sold_text = format!(
                                            "✅ *Terjual via Swap Sell Manual!*\n\n\
                                            🏦 Token: `{}`\n\
                                            💰 100% posisi dijual ke pasar\n\
                                            ⚡ Tip: {} SOL\n\
                                            ⛽ P.Fee: {} SOL\n\n\
                                            _Transaksi dikirim ke Aura gRPC._",
                                            short, tip_fee, prio_fee
                                        );
                                        // Edit panel asli jika ada
                                        if panel_msgs.is_empty() {
                                            let _ = bot_clone.send_message(chat_id_clone, &sold_text).await;
                                        } else {
                                            for (c, m) in &panel_msgs {
                                                let _ = bot_clone.edit_message_text(*c, *m, &sold_text).await;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        let _ = bot_clone.send_message(chat_id_clone, format!("❌ Swap Sell Manual Gagal: {}", e.message())).await;
                                    }
                                }
                            } else {
                                let _ = bot_clone.send_message(chat_id_clone, "❌ Alamat token tidak valid!").await;
                            }
                        });
                    } else {
                        bot.send_message(chat_id, "❌ Tidak ada token aktif yang terdeteksi untuk dijual.").await?;
                    }
                } else {
                    bot.send_message(chat_id, "❌ Koneksi ke Aura gRPC tidak tersedia.").await?;
                }
                bot.answer_callback_query(q.id.clone()).await?;
            }

            "none" => {
                bot.answer_callback_query(q.id.clone()).text("Terkunci (Fixed Setting).").await?;
            }
            "as_divider" => {
                // Garis pemisah di keyboard Auto Sell Setup — tidak berbuat
                // apa-apa, cuma hilangkan spinner loading.
                bot.answer_callback_query(q.id.clone()).await?;
            }
            _ => {}
        }
    }
    Ok(())
}

// ── Tampilan Orders berkelompok (ala bot resmi Aura) ──
// Sumber order: Aura gRPC get_limit_orders (hanya yang masih aktif).
// Harga + ticker token: Dexscreener (TAMPILAN SAJA, bukan eksekusi).
// Trigger price: didekode dari Target order Aura (Price/Mcap/%), diubah ke USD.

/// Satu order aktif dari Aura gRPC, sudah didekode untuk tampilan.
struct GroupedOrder {
    aura_id: i64,
    mint: String,
    /// "BUY" / "SELL"
    side: &'static str,
    /// Teks trigger, mis. "$0.000025" / "$100K mcap" / "+40%".
    trigger: String,
    /// Label jenis, mis. "Buy Dip" / "Take Profit" / "Stop Loss" / "Mcap".
    kind: &'static str,
    /// Tip dalam SOL.
    tip_sol: f64,
    /// Priority fee dalam SOL.
    prio_sol: f64,
    /// Amount dalam USD (untuk BUY). 0.0 untuk SELL (jual pakai %).
    amount_usd: f64,
    /// Teks target untuk disimpan ke DB lokal (mis. "0.000025$" / "100000 mcap").
    target_text: String,
}

/// Nilai order yang sedang diedit (stage) — belum tersimpan ke DB / Aura
/// sampai user tekan tombol 💾 SIMPAN (Perubahan 2, 2026-11-21).
#[derive(Clone)]
struct PendingHistEdit {
    local_id: i64,
    token: String,
    order_type: String,
    target: String,
    amount_sol: f64,
    tip: f64,
    prio: f64,
    aura_order_id: Option<i64>,
}

/// UD128 (decimal) → f64. Dipakai untuk membaca Target::Price (SOL/token)
/// dan Target::Mcap (USD) dari Aura gRPC.
fn ud128_to_f64(v: &fastnum::UD128) -> f64 {
    v.to_string().parse::<f64>().unwrap_or(0.0)
}

/// Label + teks trigger dari Target Aura, plus teks target untuk DB lokal.
/// `sol_price` dipakai untuk mengubah Target::Price (SOL/token) ke USD.
/// Mengembalikan (teks_tampil, kind, teks_db).
fn decode_trigger(target: &aura_api_client::types::Target, _sol_price: f64) -> (String, &'static str, String) {
    use aura_api_client::types::{Direction, Target};
    match target {
        Target::Price { price, direction } => {
            // Tampilkan UD128 apa adanya sebagai USD — order lama yang sudah
            // ada di Aura tersimpan sebagai USD langsung (misal FOTF 315987
            // UD128 $0.0000012). Jangan dikali harga SOL, nanti tampilannya
            // berubah 75x untuk order lama. (Perintah user 2026-11-21:
            // sinkronkan tampilan ke Aura, jangan jadikan hasil kali sebagai
            // tampilan.)
            let price_usd = ud128_to_f64(price);
            let kind = match direction {
                Direction::Below => "Buy Dip",
                Direction::Above => "Take Profit",
            };
            (
                format!("${}", crate::seconddip::fmt_price(price_usd)),
                kind,
                format!("{}$", price_usd),
            )
        }
        Target::Mcap { mcap, direction, .. } => {
            let mcap_usd = ud128_to_f64(mcap);
            let kind = match direction {
                Direction::Below => "Mcap Buy",
                Direction::Above => "Mcap Sell",
            };
            (
                format!("${} mcap", fmt_mcap_short(mcap_usd)),
                kind,
                format!("{} mcap", mcap_usd),
            )
        }
        Target::PricePerc { price_perc, direction, .. } => {
            let mult = ud128_to_f64(price_perc);
            let pct = (mult - 1.0) * 100.0;
            let kind = match direction {
                Direction::Below => "Buy Dip",
                Direction::Above => "Take Profit",
            };
            (format!("{:+.0}%", pct), kind, format!("{:.0}%", pct))
        }
        Target::Profit { init_profit_perc, .. } => {
            let pct = ud128_to_f64(init_profit_perc) * 100.0;
            (
                format!("+{:.0}% profit", pct),
                "Take Profit",
                format!("PNL {:.0}%", pct),
            )
        }
        Target::MovingPerc { price_perc, direction, .. } => {
            let mult = ud128_to_f64(price_perc);
            let pct = (mult - 1.0) * 100.0;
            let kind = match direction {
                Direction::Below => "Buy Dip",
                Direction::Above => "Take Profit",
            };
            (
                format!("{:+.0}% (trailing)", pct),
                kind,
                format!("{:.0}%", pct),
            )
        }
        Target::Market { .. } => ("Market".to_string(), "Market", "Market".to_string()),
    }
}

/// SwapAmount BUY sesuai currency yang dipilih user.
/// USD -> USDC langsung (hemat ~51k CU CAMM swap vs bayar pakai SOL).
/// SOL -> SOL lamports.
/// Dipakai semua jalur Limit Buy (buat baru + edit) agar tidak selalu SOL.
fn buy_swap_amount(amount: f64, buy_cur: &str) -> aura_api_client::types::SwapAmount {
    if buy_cur == "SOL" {
        // amount sudah SOL.
        let lamports = (amount * 1e9) as u64;
        aura_api_client::types::SwapAmount::Buy(decisol::QuoteLamports::Lamports(
            decisol::Lamports::from(lamports),
        ))
    } else {
        // amount adalah USD -> USDC (6 desimal).
        let usdc_lam = (amount * 1e6) as u64;
        aura_api_client::types::SwapAmount::Buy(decisol::QuoteLamports::Usdc(
            decisol::Usdc::new(usdc_lam),
        ))
    }
}

/// Amount BUY dalam USD dari SwapAmount.
/// QuoteLamports::Usdc/Usdt/Usd1 -> Display sudah USD.
/// QuoteLamports::Lamports/Wsol -> Display SOL, dikali harga SOL.
/// SELL (persen) -> 0.0.
fn buy_amount_usd(amount: &aura_api_client::types::SwapAmount, sol_price: f64) -> f64 {
    use aura_api_client::types::SwapAmount;
    match amount {
        SwapAmount::Buy(q) => {
            let val: f64 = format!("{}", q).parse().unwrap_or(0.0);
            match q.quote_kind() {
                decisol::QuoteKind::Usd => val,
                decisol::QuoteKind::Sol => val * sol_price,
            }
        }
        _ => 0.0,
    }
}

/// Lamports (decisol) -> SOL. Display Lamports sudah dalam SOL.
fn lamports_to_sol(v: &decisol::Lamports) -> f64 {
    format!("{}", v).parse().unwrap_or(0.0)
}

/// Ambil (ticker, harga USD) untuk banyak mint sekaligus dari Dexscreener.
/// Dipakai HANYA untuk tampilan (bukan eksekusi). Per mint dipilih pair
/// pertama yang dikembalikan (Dexscreener mengurutkan dari likuiditas terbesar).
/// Mengembalikan map mint -> (ticker, price_usd).
/// Info token dari Dexscreener (TAMPILAN SAJA): (ticker, harga USD, liquiditas USD).
pub(crate) async fn fetch_dexscreener_info(mints: &[String]) -> std::collections::HashMap<String, (String, f64, f64)> {
    let mut out: std::collections::HashMap<String, (String, f64, f64)> = std::collections::HashMap::new();
    if mints.is_empty() {
        return out;
    }
    let url = format!(
        "https://api.dexscreener.com/latest/dex/tokens/{}",
        mints.join(",")
    );
    let client = crate::seconddip::build_proxied_http_client();
    let Ok(resp) = client.get(&url).send().await else {
        return out;
    };
    let Ok(json) = resp.json::<serde_json::Value>().await else {
        return out;
    };
    if let Some(pairs) = json["pairs"].as_array() {
        for p in pairs {
            // Token bisa di sisi base ATAU quote — cek keduanya.
            let (mint, ticker) = if let Some(addr) = p["baseToken"]["address"].as_str() {
                if mints.iter().any(|m| m == addr) {
                    (addr.to_string(), p["baseToken"]["symbol"].as_str().unwrap_or("").to_string())
                } else if let Some(qaddr) = p["quoteToken"]["address"].as_str() {
                    if mints.iter().any(|m| m == qaddr) {
                        (qaddr.to_string(), p["quoteToken"]["symbol"].as_str().unwrap_or("").to_string())
                    } else {
                        continue;
                    }
                } else {
                    continue;
                }
            } else {
                continue;
            };
            if out.contains_key(&mint) {
                continue;
            }
            let Some(price_str) = p["priceUsd"].as_str() else {
                continue;
            };
            let Ok(price) = price_str.parse::<f64>() else {
                continue;
            };
            let liquidity = p["liquidity"]["usd"].as_f64().unwrap_or(0.0);
            out.insert(mint, (ticker, price, liquidity));
        }
    }
    out
}

/// Tampilkan nama token (ticker) kalau ada, kalau tidak ada pakai address singkat.
/// Dipakai di semua tempat yang menampilkan token, supaya user lihat nama, bukan address.
pub(crate) fn ticker_display(mint: &str, info: &std::collections::HashMap<String, (String, f64, f64)>) -> String {
    if let Some((ticker, _, _)) = info.get(mint) {
        if !ticker.is_empty() {
            return ticker.clone();
        }
    }
    if mint.len() > 10 {
        format!("{}...{}", &mint[..6], &mint[mint.len() - 4..])
    } else {
        mint.to_string()
    }
}

/// Ambil semua order AKTIF dari Aura gRPC, dikelompokkan per mint.
/// Mengembalikan map mint -> daftar order (urut by aura id).
/// `sol_price` dipakai untuk mengubah Target::Price (SOL/token) ke USD.
/// Ambil harga USD dari target lokal (mis. "0.0000001$" -> 0.0000001).
/// Return None kalau target bukan harga USD (mcap/persen/PNL).
fn local_target_usd(target: &str) -> Option<f64> {
    let s = target.trim();
    // Format target USD: angka + '$' di akhir, mis. "0.0000001$".
    let stripped = s.strip_suffix('$')?.trim();
    if stripped.is_empty()
        || !stripped
            .chars()
            .all(|c| c.is_ascii_digit() || c == '.')
    {
        return None;
    }
    let val: f64 = stripped.parse().ok()?;
    if val > 0.0 {
        Some(val)
    } else {
        None
    }
}

/// Map aura_order_id -> target lokal (USD yang diketik user).
/// Dipakai History agar trigger tampil dalam USD, bukan nilai SOL hasil pembagian.
fn local_targets_map(st: &BotState) -> std::collections::HashMap<i64, String> {
    let mut map = std::collections::HashMap::new();
    if let Ok(conn) = st.db_conn.try_lock() {
        if let Ok(orders) = db::load_limit_orders(&conn) {
            for o in orders {
                if let Some(aid) = o.aura_order_id {
                    map.insert(aid, o.target);
                }
            }
        }
    }
    map
}

async fn fetch_active_orders_grouped(
    client: &AuraClients<InterceptorFn, NoCtx>,
    sol_price: f64,
    local_targets: &std::collections::HashMap<i64, String>,
) -> std::collections::BTreeMap<String, Vec<GroupedOrder>> {
    let mut grouped: std::collections::BTreeMap<String, Vec<GroupedOrder>> = std::collections::BTreeMap::new();
    let req = tonic::Request::new(aura_api_client::client::types::GetLimitOrders);
    // Rate limiter: jangan tabrakan dengan pasang TP / ping.
    aura_slot_wait().await;
    let Ok(resp) = client.limit_orders().get_limit_orders((), req).await else {
        return grouped;
    };
    for api_order in resp.into_inner().orders {
        let (aura_id, mint) = match &api_order.state {
            aura_api_client::types::OrderState::Placed { id, mint, .. } => (id.0, mint.to_string()),
            _ => continue, // hanya yang Placed (aktif)
        };
        let side = match &api_order.order.amount {
            aura_api_client::types::SwapAmount::Buy(_)
            | aura_api_client::types::SwapAmount::BuyPerc { .. } => "BUY",
            _ => "SELL",
        };
        let (mut trigger, kind, mut target_text) = decode_trigger(&api_order.order.target, sol_price);
        // Perubahan 1 (2026-11-21): tampilkan trigger dalam USD yang diketik
        // user, bukan nilai SOL hasil pembagian. Target lokal (DB) menyimpan
        // USD aslinya; order dari bot resmi Aura tidak punya target lokal dan
        // UD128-nya memang USD, jadi decode_trigger apa adanya sudah benar.
        if let Some(local_target) = local_targets.get(&aura_id) {
            if let Some(usd) = local_target_usd(local_target) {
                trigger = format!("${}", crate::seconddip::fmt_price(usd));
                target_text = local_target.clone();
            }
        }
        grouped.entry(mint.clone()).or_default().push(GroupedOrder {
            aura_id,
            mint,
            side,
            trigger,
            kind,
            tip_sol: lamports_to_sol(&api_order.order.tip),
            prio_sol: lamports_to_sol(&api_order.order.fee),
            amount_usd: buy_amount_usd(&api_order.order.amount, sol_price),
            target_text,
        });
    }
    grouped
}


/// Sinkronisasi limit order lokal dengan Aura gRPC (dua arah, satu sumber kebenaran: Aura).
/// - Order AKTIF di Aura tapi belum ada di DB lokal -> ditambahkan (supaya
///   order yang dibuat lewat bot resmi Aura muncul di Sonic).
/// - Order di DB lokal (yang punya aura_order_id) tapi sudah tidak aktif
///   di Aura (dihapus / terisi / expired) -> dihapus dari DB lokal.
///   Jadi kalau user hapus limit order di bot resmi Aura, order itu juga
///   hilang dari Sonic.
/// Hanya jalan kalau fetch Aura berhasil; kalau gagal, tidak ada yang diubah
/// (supaya gangguan jaringan tidak menghapus order lokal).
/// Mengembalikan (jumlah_ditambah, jumlah_dihapus).
async fn sync_orders_from_aura(st: &BotState) -> (usize, usize) {
    let Some(client) = st.aura_client.clone() else {
        return (0, 0);
    };
    let sol_price = fetch_sol_price_usd().await.unwrap_or(0.0);
    let local_targets = local_targets_map(st);
    let grouped = fetch_active_orders_grouped(&client, sol_price, &local_targets).await;
    // Kumpulkan semua order aktif dari Aura (by aura_order_id).
    let mut aura_orders: std::collections::HashMap<i64, &GroupedOrder> =
        std::collections::HashMap::new();
    for orders in grouped.values() {
        for o in orders {
            aura_orders.insert(o.aura_id, o);
        }
    }
    let Ok(conn) = st.db_conn.try_lock() else {
        return (0, 0);
    };
    let Ok(local) = db::load_limit_orders(&conn) else {
        return (0, 0);
    };
    let mut added = 0usize;
    let mut removed = 0usize;
    // 1) Tambah order Aura yang belum ada di lokal.
    for (aura_id, o) in &aura_orders {
        let exists = local.iter().any(|lo| lo.aura_order_id == Some(*aura_id));
        if !exists {
            let order_type = if o.side == "BUY" { "BUY" } else { "SELL" };
            if db::insert_limit_order_with_aura_id(
                &conn,
                order_type,
                &o.mint,
                &o.target_text,
                o.tip_sol,
                o.prio_sol,
                o.amount_usd,
                *aura_id,
            )
            .is_ok()
            {
                added += 1;
            }
        }
    }
    // 2) Hapus order lokal yang sudah tidak aktif di Aura.
    //    Hanya yang punya aura_order_id (order lokal tanpa id Aura tidak disentuh).
    for lo in &local {
        if let Some(aura_id) = lo.aura_order_id {
            if !aura_orders.contains_key(&aura_id) {
                if db::delete_limit_order(&conn, lo.id).is_ok() {
                    removed += 1;
                }
            }
        }
    }
    if added > 0 || removed > 0 {
        info!(
            "[Sync Orders] Aura -> lokal: +{} order baru, -{} order tidak aktif lagi",
            added, removed
        );
    }
    (added, removed)
}

/// Bangun teks + keyboard tampilan Orders berkelompok.
/// Mengembalikan (teks, keyboard). Dipakai oleh send & refresh.
async fn build_orders_grouped_view(
    st: &BotState,
) -> Option<(String, InlineKeyboardMarkup)> {
    let client = st.aura_client.as_ref()?;
    let sol_price = fetch_sol_price_usd().await.unwrap_or(0.0);
    let local_targets = local_targets_map(st);
    let grouped = fetch_active_orders_grouped(client, sol_price, &local_targets).await;
    if grouped.is_empty() {
        return None;
    }
    // Harga + ticker dari Dexscreener (tampilan saja).
    let mints: Vec<String> = grouped.keys().cloned().collect();
    let info = fetch_dexscreener_info(&mints).await;
    let mut text = format!(
        "🌞 *Aura Orders*\n\nTokens with active limit orders: {}\nPage 1 / 1\n\n",
        grouped.len()
    );
    for (mint, orders) in &grouped {
        let ticker = ticker_display(mint, &info);
        let price = info.get(mint).map(|(_, p, _)| *p).unwrap_or(0.0);
        let price_str = if price > 0.0 {
            format!("${}", crate::seconddip::fmt_price(price))
        } else {
            "—".to_string()
        };
        text.push_str(&format!("🪙 *{}*\n", ticker));
        text.push_str(&format!("├ 💰 Harga: {}\n", price_str));
        if orders.len() == 1 {
            let o = &orders[0];
            text.push_str(&format!("├ 🎯 Trigger: {} ({})\n", o.trigger, o.kind));
        } else {
            text.push_str("├ 🎯 Trigger:\n");
            for o in orders {
                text.push_str(&format!("│  • {} ({}) — {}\n", o.trigger, o.kind, o.side));
            }
        }
        text.push_str(&format!("└ 📝 Orders: {}\n\n", orders.len()));
    }
    text.push_str("Tap a coin below to manage its orders");
    // Keyboard: 2 tombol per baris (ticker | jumlah), lalu Clear All + Back/Refresh.
    let mut rows: Vec<Vec<InlineKeyboardButton>> = Vec::new();
    let mut row: Vec<InlineKeyboardButton> = Vec::new();
    for (mint, orders) in &grouped {
        let ticker = ticker_display(mint, &info);
        row.push(InlineKeyboardButton::callback(
            format!("{} | {} 📝", ticker, orders.len()),
            format!("orders_token_{}", mint),
        ));
        if row.len() == 2 {
            rows.push(std::mem::take(&mut row));
        }
    }
    if !row.is_empty() {
        rows.push(row);
    }
    rows.push(vec![InlineKeyboardButton::callback(
        "🗑️ Clear All",
        "orders_clear_all",
    )]);
    rows.push(vec![
        InlineKeyboardButton::callback("<<•Back", "menu_main"),
        InlineKeyboardButton::callback("Refresh 🔄", "orders_refresh"),
    ]);
    let kb = InlineKeyboardMarkup::new(rows);
    Some((text, kb))
}

/// Kirim tampilan Orders berkelompok (baru). Menyimpan message id untuk refresh.
async fn send_orders_grouped(bot: &Bot, chat_id: ChatId, st: &mut BotState) -> ResponseResult<()> {
    if st.aura_client.is_none() {
        bot.send_message(chat_id, "❌ Aura belum terhubung.").await?;
        return Ok(());
    }
    match build_orders_grouped_view(st).await {
        Some((text, kb)) => {
            let msg = bot.send_message(chat_id, text).reply_markup(kb).await?;
            st.orders_msg = Some((chat_id, msg.id));
        }
        None => {
            bot.send_message(chat_id, "📭 Belum ada limit order yang aktif.")
                .await?;
        }
    }
    Ok(())
}

/// Refresh tampilan Orders di tempat (edit pesan yang tersimpan).
/// Mengembalikan true kalau berhasil edit, false kalau harus kirim baru.
async fn refresh_orders_grouped(bot: &Bot, st: &mut BotState) -> bool {
    let Some((c, m)) = st.orders_msg else {
        return false;
    };
    let Some((text, kb)) = build_orders_grouped_view(st).await else {
        // Tidak ada order aktif lagi — edit jadi pesan kosong.
        let _ = bot
            .edit_message_text(c, m, "📭 Belum ada limit order yang aktif.")
            .await;
        return true;
    };
    // Kalau isi + tombol juga sama persis ("message is not modified"),
    // tampilan sudah benar — return true, jangan kirim pesan baru.
    match bot.edit_message_text(c, m, text).await {
        Ok(_) => {
            // Keyboard ikut di-update (teks mungkin sama tapi harga berubah).
            let _ = bot.edit_message_reply_markup(c, m).reply_markup(kb).await;
            true
        }
        Err(e) if is_not_modified_error(&e) => true,
        Err(_) => match bot
            .edit_message_reply_markup(c, m)
            .reply_markup(kb)
            .await
        {
            // Teks sama (Telegram tolak edit text) — keyboard saja yang berubah.
            Ok(_) => true,
            Err(e) if is_not_modified_error(&e) => true,
            Err(_) => false,
        },
    }
}


/// Ambil pending edit untuk order ini, atau buat baru dari DB (stage awal).
/// Return None kalau order tidak ada di DB.
fn pending_hist_edit_or_init(st: &mut BotState, local_id: i64) -> Option<PendingHistEdit> {
    if let Some(p) = &st.pending_hist_edit {
        if p.local_id == local_id {
            return Some(p.clone());
        }
    }
    let order = if let Ok(conn) = st.db_conn.try_lock() {
        db::get_limit_order(&conn, local_id).ok()
    } else {
        None
    }?;
    let p = PendingHistEdit {
        local_id,
        token: order.token,
        order_type: order.order_type,
        target: order.target,
        amount_sol: order.amount_sol,
        tip: order.tip_fee,
        prio: order.prio_fee,
        aura_order_id: order.aura_order_id,
    };
    st.pending_hist_edit = Some(p.clone());
    Some(p)
}

/// Bangun (teks, keyboard) detail satu order History dari DB lokal,
/// ditimpa nilai pending edit kalau ada (Perubahan 2, 2026-11-21).
/// `prompt` = baris prompt tambahan (mis. "Ketik target baru..."), None kalau tidak ada.
async fn build_hist_detail(
    st: &BotState,
    local_id: i64,
    prompt: Option<&str>,
) -> Option<(String, InlineKeyboardMarkup)> {
    let order = (if let Ok(conn) = st.db_conn.try_lock() {
        db::get_limit_order(&conn, local_id).ok()
    } else {
        None
    })?;
    // Nilai tampil: pakai pending edit kalau ada (belum disimpan), kalau tidak pakai DB.
    let (target, amount_sol, tip, prio) = match &st.pending_hist_edit {
        Some(p) if p.local_id == local_id => {
            (p.target.clone(), p.amount_sol, p.tip, p.prio)
        }
        _ => (
            order.target.clone(),
            order.amount_sol,
            order.tip_fee,
            order.prio_fee,
        ),
    };
    let has_pending = st
        .pending_hist_edit
        .as_ref()
        .map(|p| p.local_id == local_id)
        .unwrap_or(false);
    // Tampilkan NAMA token (ticker dari Dexscreener), bukan address.
    let info = fetch_dexscreener_info(&[order.token.clone()]).await;
    let token_name = ticker_display(&order.token, &info);
    let short_token = if order.token.len() > 10 {
        format!("{}...{}", &order.token[..6], &order.token[order.token.len() - 4..])
    } else {
        order.token.clone()
    };
    let is_buy = order.order_type.eq_ignore_ascii_case("BUY");
    let amount_str = if amount_sol > 0.0 {
        if is_buy {
            format!("${:.2}", amount_sol)
        } else {
            format!("{:.2} SOL", amount_sol)
        }
    } else {
        "—".to_string()
    };
    let aura_str = match order.aura_order_id {
        Some(aid) => format!("{}", aid),
        None => "—".to_string(),
    };
    let mut text = format!(
        "📋 *Detail Order* | {}\n\n🪙 Token: *{}* (`{}`)\n💰 Amount: {}\n🎯 Target: {}\n⚡ Tip: {} SOL  ⛽ Prio: {} SOL\n📋 Aura ID: {}",
        order.order_type,
        token_name,
        short_token,
        amount_str,
        format_target_display(&target),
        tip,
        prio,
        aura_str
    );
    if has_pending {
        text.push_str("\n\n⏳ *Ada perubahan belum disimpan — tekan 💾 SIMPAN untuk kirim ke Aura.*");
    } else {
        text.push_str("\n\n*Ketuk TARGET / AMOUNT / TIP / PRIO untuk mengubah. Perubahan dikirim ke Aura saat tekan 💾 SIMPAN.*");
    }
    if let Some(p) = prompt {
        text.push_str(&format!("\n\n{}", p));
    }
    let kb = make_order_inline_keyboard(order.id, is_buy, tip, prio, has_pending, true, st);
    Some((text, kb))
}

/// Update pesan detail order History di tempat (edit), atau kirim baru kalau
/// belum ada / edit gagal. Dipakai semua jalur edit (Perubahan 2 & 3, 2026-11-21)
/// supaya riwayat chat tidak menumpuk dan tidak ada tombol/pesan baru.
async fn refresh_hist_detail(
    bot: &Bot,
    st: &mut BotState,
    chat_id: ChatId,
    local_id: i64,
    prompt: Option<&str>,
) {
    let Some((text, kb)) = build_hist_detail(st, local_id, prompt).await else {
        return;
    };
    if let Some((c, m, mid)) = st.hist_detail_msg {
        if mid == local_id {
            if bot.edit_message_text(c, m, text.clone()).await.is_ok() {
                let _ = bot.edit_message_reply_markup(c, m).reply_markup(kb).await;
                return;
            }
            // Edit gagal (pesan terhapus) — kirim baru di bawah.
        }
    }
    if let Ok(msg) = bot.send_message(chat_id, text).reply_markup(kb).await {
        st.hist_detail_msg = Some((chat_id, msg.id, local_id));
    }
}

/// Kirim detail satu order (dari DB lokal) lengkap dengan keyboard edit.
/// Dipakai oleh tombol "✏️ Edit Order #N" di detail token.
async fn send_hist_order_detail(bot: &Bot, chat_id: ChatId, st: &mut BotState, local_id: i64) -> ResponseResult<()> {
    refresh_hist_detail(bot, st, chat_id, local_id, None).await;
    Ok(())
}

/// Detail order untuk satu token (diketuk dari tampilan Orders).
/// Menampilkan tiap order: trigger, amount, tip/prio, dan tombol hapus.
async fn send_orders_token_detail(
    bot: &Bot,
    chat_id: ChatId,
    st: &BotState,
    mint: &str,
) -> ResponseResult<()> {
    let Some(client) = &st.aura_client else {
        bot.send_message(chat_id, "❌ Aura belum terhubung.").await?;
        return Ok(());
    };
    let sol_price = fetch_sol_price_usd().await.unwrap_or(0.0);
    let local_targets = local_targets_map(st);
    let grouped = fetch_active_orders_grouped(client, sol_price, &local_targets).await;
    let Some(orders) = grouped.get(mint) else {
        bot.send_message(chat_id, "📭 Tidak ada order aktif untuk token ini.")
            .await?;
        return Ok(());
    };
    let info = fetch_dexscreener_info(&[mint.to_string()]).await;
    let ticker = ticker_display(mint, &info);
    let price = info.get(mint).map(|(_, p, _)| *p).unwrap_or(0.0);
    // Data lokal (amount/tip/prio) by aura_order_id.
    let local: std::collections::HashMap<i64, crate::db::DbLimitOrder> =
        if let Ok(conn) = st.db_conn.try_lock() {
            db::load_limit_orders(&conn)
                .unwrap_or_default()
                .into_iter()
                .filter_map(|o| o.aura_order_id.map(|aid| (aid, o)))
                .collect()
        } else {
            std::collections::HashMap::new()
        };
    let price_str = if price > 0.0 {
        format!("${}", crate::seconddip::fmt_price(price))
    } else {
        "—".to_string()
    };
    let mut text = format!("🪙 *{}*\n💰 Harga: {}\n\n", ticker, price_str);
    let mut rows: Vec<Vec<InlineKeyboardButton>> = Vec::new();
    for (i, o) in orders.iter().enumerate() {
        let (amount_str, tip_prio) = match local.get(&o.aura_id) {
            Some(lo) => (
                if lo.amount_sol > 0.0 {
                    format!("${:.2}", lo.amount_sol)
                } else {
                    "—".to_string()
                },
                format!("T:{:.4} P:{:.4}", lo.tip_fee, lo.prio_fee),
            ),
            None => ("—".to_string(), String::new()),
        };
        text.push_str(&format!(
            "*Order #{}* | {} ({})\n🎯 Trigger: {}\n💰 Amount: {}  ⚡ {}\n📋 Aura ID: {}\n\n",
            i + 1,
            o.side,
            o.kind,
            o.trigger,
            amount_str,
            tip_prio,
            o.aura_id
        ));
        if let Some(lo) = local.get(&o.aura_id) {
            rows.push(vec![
                InlineKeyboardButton::callback(
                    format!("✏️ Edit Order #{}", i + 1),
                    format!("edit_hist_order_{}", lo.id),
                ),
                InlineKeyboardButton::callback(
                    format!("🗑️ Hapus #{}", i + 1),
                    format!("delete_order_{}", lo.id),
                ),
            ]);
        }
    }
    rows.push(vec![InlineKeyboardButton::callback(
        format!("🗑️ Hapus Semua {}", ticker),
        format!("orders_clear_token_{}", mint),
    )]);
    rows.push(vec![
        InlineKeyboardButton::callback("<< Orders", "orders_refresh"),
        InlineKeyboardButton::callback(
            "Refresh 🔄",
            format!("orders_token_{}", mint),
        ),
    ]);
    let kb = InlineKeyboardMarkup::new(rows);
    bot.send_message(chat_id, text).reply_markup(kb).await?;
    Ok(())
}


async fn send_error_logs(bot: &Bot, chat_id: ChatId, st: &BotState) -> ResponseResult<()> {
    if let Ok(conn) = st.db_conn.try_lock() {
        let logs = db::load_error_logs(&conn).unwrap_or_default();
        if logs.is_empty() {
            bot.send_message(chat_id, "📜 Tidak ada log error saat ini.\n\n_Semua transaksi limit order berjalan normal, atau belum ada limit order yang dieksekusi._").await?;
        } else {
            let mut text = String::from("📜 *Error Logs Limit Order*\n\n");
            for l in logs.iter().take(10) {
                text.push_str(&format!(
                    "`[{}]`\nOrder #{} | Token: `{}`\nError: _{}_\n\n",
                    l.created_at, l.order_id, l.token, l.error_msg
                ));
            }
            if logs.len() > 10 {
                text.push_str(&format!("_(+ {} log lainnya — klik hapus untuk bersihkan semua)_", logs.len() - 10));
            }
            let kb = InlineKeyboardMarkup::new(vec![
                vec![InlineKeyboardButton::callback("🗑 Hapus Semua Log", "clear_error_logs")],
                vec![InlineKeyboardButton::callback("<< Back", "menu_main")],
            ]);
            bot.send_message(chat_id, text).reply_markup(kb).await?;
        }
    } else {
        bot.send_message(chat_id, "❌ Database sedang sibuk, coba lagi.").await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aura_api_client::types::Target;











    #[test]
    fn test_local_target_usd() {
        // Perubahan 1 (2026-11-21): trigger History tampil dalam USD yang
        // diketik user. Target lokal "0.0000001$" -> 0.0000001.
        assert_eq!(local_target_usd("0.0000001$"), Some(0.0000001));
        assert_eq!(local_target_usd("0.000005$"), Some(0.000005));
        assert_eq!(local_target_usd("25$"), Some(25.0));
        // Bukan harga USD -> None (biar decode_trigger yang handle).
        assert_eq!(local_target_usd("100000 mcap"), None);
        assert_eq!(local_target_usd("+40%"), None);
        assert_eq!(local_target_usd("0$"), None);
        assert_eq!(local_target_usd(""), None);
        assert_eq!(local_target_usd("abc$"), None);
    }

    #[test]
    fn test_sol_price_parsers() {
        // Binance: {"price": "67.81"}
        let b: serde_json::Value = serde_json::from_str(r#"{"symbol":"SOLUSDT","price":"67.81"}"#).unwrap();
        assert_eq!(parse_sol_price_binance(&b), Some(67.81));
        let b_bad: serde_json::Value = serde_json::from_str(r#"{"price":"0"}"#).unwrap();
        assert_eq!(parse_sol_price_binance(&b_bad), None);
        // CoinGecko: {"solana": {"usd": 67.81}}
        let c: serde_json::Value = serde_json::from_str(r#"{"solana":{"usd":67.81}}"#).unwrap();
        assert_eq!(parse_sol_price_coingecko(&c), Some(67.81));
        let c_bad: serde_json::Value = serde_json::from_str(r#"{"solana":{}}"#).unwrap();
        assert_eq!(parse_sol_price_coingecko(&c_bad), None);
        // Dexscreener: {"pairs": [{"priceUsd": "67.81"}]}
        let d: serde_json::Value = serde_json::from_str(r#"{"pairs":[{"priceUsd":"67.81"},{"priceUsd":"67.80"}]}"#).unwrap();
        assert_eq!(parse_sol_price_dexscreener(&d), Some(67.81));
        let d_empty: serde_json::Value = serde_json::from_str(r#"{"pairs":[]}"#).unwrap();
        assert_eq!(parse_sol_price_dexscreener(&d_empty), None);
        let d_no_pairs: serde_json::Value = serde_json::from_str(r#"{}"#).unwrap();
        assert_eq!(parse_sol_price_dexscreener(&d_no_pairs), None);
    }

    #[test]
    fn test_sol_price_cache_fresh() {
        // Cache harga SOL TTL 1 jam (perintah user 2026-11-21).
        let now = std::time::Instant::now();
        // Baru saja di-fetch -> segar.
        assert!(sol_price_cache_fresh(now, now));
        // 30 menit lalu -> masih segar.
        assert!(sol_price_cache_fresh(
            now - std::time::Duration::from_secs(1800),
            now
        ));
        // 59 menit lalu -> masih segar.
        assert!(sol_price_cache_fresh(
            now - std::time::Duration::from_secs(3540),
            now
        ));
        // Tepat 1 jam lalu -> kadaluarsa (harus fetch baru).
        assert!(!sol_price_cache_fresh(
            now - std::time::Duration::from_secs(3600),
            now
        ));
        // 2 jam lalu -> kadaluarsa.
        assert!(!sol_price_cache_fresh(
            now - std::time::Duration::from_secs(7200),
            now
        ));
    }

    #[test]
    fn test_decode_trigger() {
        use aura_api_client::types::{Direction, Target};
        // Target::Price: UD128 ditampilkan apa adanya sebagai USD.
        // $0.000025 -> UD128 0.000025.
        let price = fastnum::UD128::from(25_000u64) / fastnum::UD128::from(1_000_000_000_000u64);
        let (txt, kind, _) = decode_trigger(
            &Target::Price { price, direction: Direction::Below },
            200.0,
        );
        assert_eq!(kind, "Buy Dip");
        // fmt_price pakai notasi subscript: 0.000025 -> 0.0₄25
        assert!(txt.contains("25"), "trigger: {}", txt);
        // Target::Price Above -> Take Profit.
        let (txt2, kind2, _) = decode_trigger(
            &Target::Price { price, direction: Direction::Above },
            200.0,
        );
        assert_eq!(kind2, "Take Profit");
        assert!(txt2.contains("25"), "trigger: {}", txt2);
        // Target::Mcap: $100K mcap (skala 1e6).
        let mcap = fastnum::UD128::from(100_000_000_000u64) / fastnum::UD128::from(1_000_000u64);
        let (txt3, kind3, _) = decode_trigger(
            &Target::Mcap { mcap, price: None, direction: Direction::Below },
            200.0,
        );
        assert_eq!(kind3, "Mcap Buy");
        assert!(txt3.contains("100"), "mcap trigger: {}", txt3);
        // ud128_to_f64
        assert!((ud128_to_f64(&fastnum::UD128::from(5u64)) - 5.0).abs() < 1e-9);
    }

    #[test]
    fn test_parse_target_inputs() {
        assert_eq!(parse_target_input("100K mcap"), Some("100K mcap".to_string()));
        assert_eq!(parse_target_input("11M mcap"), Some("11M mcap".to_string()));
        assert_eq!(parse_target_input("2.35M mcap"), Some("2.35M mcap".to_string()));
        assert_eq!(parse_target_input("0.000005$"), Some("0.000005$".to_string()));
        assert_eq!(parse_target_input("$1"), Some("1$".to_string()));
        assert_eq!(parse_target_input("80%"), Some("80%".to_string()));
        assert_eq!(parse_target_input("-20%"), Some("-20%".to_string()));
        assert_eq!(parse_target_input("70%pnl"), Some("PNL 70%".to_string()));
        assert_eq!(parse_target_input("pnl70%"), Some("PNL 70%".to_string()));
        assert_eq!(parse_target_input("60%pnl"), Some("PNL 60%".to_string()));
    }

    #[test]
    fn test_parse_target_str_to_aura() {
        let sol_price = 150.0;
        
        // PNL 60% -> Target::Profit with 1.6
        let t = parse_target_str_to_aura("PNL 60%", sol_price, "SELL");
        assert!(matches!(t, Some(Target::Profit { .. })));

        // McAp 100K mcap -> Target::Mcap
        let t = parse_target_str_to_aura("100K mcap", sol_price, "SELL");
        assert!(matches!(t, Some(Target::Mcap { .. })));

        // McAp 11M mcap -> Target::Mcap
        let t = parse_target_str_to_aura("11M mcap", sol_price, "SELL");
        assert!(matches!(t, Some(Target::Mcap { .. })));

        // Price 0.000005$ -> Target::Price dengan UD128 = USD / harga SOL
        // (perintah user 2026-11-21, supaya tampilan di Aura sama).
        // sol_price 150 -> UD128 = 0.000005 / 150 = 0.000000033333...
        let t = parse_target_str_to_aura("0.000005$", sol_price, "BUY");
        match t {
            Some(Target::Price { price, .. }) => {
                let v = crate::ud128_to_f64(&price);
                let expected = 0.000005 / sol_price;
                assert!((v - expected).abs() < 1e-12, "UD128 harus = USD / harga SOL, dapat {}", v);
            }
            _ => panic!("0.000005$ harus jadi Target::Price"),
        }

        // Persen 80% -> Target::PricePerc
        let t = parse_target_str_to_aura("80%", sol_price, "SELL");
        assert!(matches!(t, Some(Target::PricePerc { .. })));

        // Market -> Target::Market (mode Always) — order limit tipe Market
        // (tereksekusi langsung di harga pasar, dipakai untuk uji Auto Sell).
        let t = parse_target_str_to_aura("Market", sol_price, "BUY");
        assert!(matches!(t, Some(Target::Market { .. })));
        let t = parse_target_str_to_aura("market", sol_price, "BUY");
        assert!(matches!(t, Some(Target::Market { .. })));
    }

    #[test]
    fn test_parse_number() {
        assert_eq!(parse_number("5"), Some(5.0));
        assert_eq!(parse_number("10.5"), Some(10.5));
        assert_eq!(parse_number("5$"), Some(5.0));
        assert_eq!(parse_number("0.25 sol"), Some(0.25));
    }

    #[test]
    fn test_buy_swap_amount_usdc_vs_sol() {
        use aura_api_client::types::SwapAmount;
        // USD -> USDC langsung: $25 = 25_000_000 micro-USDC (6 desimal).
        // Display USDC: 4 angka desimal ("25.0000").
        let usd = buy_swap_amount(25.0, "USD");
        match usd {
            SwapAmount::Buy(q) => {
                assert!(matches!(q.quote_kind(), decisol::QuoteKind::Usd));
                assert_eq!(format!("{}", q), "25.0000");
            }
            _ => panic!("harusnya Buy"),
        }
        // SOL -> SOL lamports: 0.35 SOL = 350_000_000 lamports.
        // Display SOL: 4 angka desimal ("0.3500").
        let sol = buy_swap_amount(0.35, "SOL");
        match sol {
            SwapAmount::Buy(q) => {
                assert!(matches!(q.quote_kind(), decisol::QuoteKind::Sol));
                assert_eq!(format!("{}", q), "0.3500");
            }
            _ => panic!("harusnya Buy"),
        }
    }

    #[test]
    fn test_auto_sell_toggle_label() {
        // Tombol Auto Sell di halaman Auto Sell Setup (perintah user 2026-11-22).
        assert_eq!(
            auto_sell_toggle_label("Biasa", true, "+100%"),
            "🎯 Auto Sell Biasa | 🟢 ON (+100%)"
        );
        assert_eq!(
            auto_sell_toggle_label("Biasa", false, "+100%"),
            "🎯 Auto Sell Biasa | 🔴 OFF"
        );
        assert_eq!(
            auto_sell_toggle_label("SD", true, "$0.000004"),
            "🎯 Auto Sell SD | 🟢 ON ($0.000004)"
        );
    }

    #[test]
    fn test_fmt_tp_value() {
        // Nilai TP per config Auto Sell Setup 2 config (2026-11-21).
        assert_eq!(fmt_tp_value("PCT", 0.0, 0.0, 100.0), "+100%");
        assert_eq!(fmt_tp_value("PCT", 0.0, 0.0, 40.0), "+40%");
        assert_eq!(fmt_tp_value("USD", 0.000004, 0.0, 100.0), "$0.0₅4");
        assert_eq!(fmt_tp_value("USD", 0.0, 0.0, 100.0), "belum diatur");
        assert_eq!(fmt_tp_value("MCAP", 0.0, 100000.0, 100.0), "$100K mcap");
        assert_eq!(fmt_tp_value("MCAP", 0.0, 0.0, 100.0), "belum diatur");
    }
}

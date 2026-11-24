use rusqlite::{Connection, Result, params};
use std::collections::HashSet;
use teloxide::types::ChatId;

/// Alamat wallet default yang dipantau fitur Second Dip (9S7hhP).
pub const DEFAULT_SECOND_DIP_WALLET: &str = "AURARwy7F1sfbxeRkdcUCVg2eG1mpeHG1jChRU9S7hhP";

/// Batas maksimal wallet target Second Dip (supaya `logsSubscribe` WSS
/// tidak kebanyakan mentions).
pub const MAX_SECOND_DIP_TARGETS: usize = 10;

/// Alfabet base58 Solana (tanpa 0, O, I, l).
const BASE58_ALPHABET: &str = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

/// Validasi address Solana: 32-44 karakter base58.
pub fn is_valid_solana_address(addr: &str) -> bool {
    let len = addr.len();
    (32..=44).contains(&len) && addr.chars().all(|c| BASE58_ALPHABET.contains(c))
}

// ─── Settings ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DbSettings {
    pub sell_tip_fee: f64,
    pub sell_prio_fee: f64,
    pub sell_slippage: String,
    // Preset sizes (kecil/sedang/besar/mega) — tip & prio
    pub preset_kecil_tip: f64,
    pub preset_kecil_prio: f64,
    pub preset_sedang_tip: f64,
    pub preset_sedang_prio: f64,
    pub preset_besar_tip: f64,
    pub preset_besar_prio: f64,
    pub preset_mega_tip: f64,
    pub preset_mega_prio: f64,
    pub bought_token: Option<String>,
    // ── Second Dip (copytrade wallet target) ──
    pub second_dip_active: bool,
    pub second_dip_target_wallet: String,
    /// Pemicu 1: % kenaikan harga dari entry untuk MEMBUAT limit order.
    pub second_dip_trigger1_pct: f64,
    /// Mode harga Pemicu 1: "PCT" (% di atas entry) atau "USD" (harga absolut USD).
    /// Default "PCT".
    pub second_dip_trigger1_mode: String,
    /// Harga Pemicu 1 absolut dalam USD (dipakai kalau mode = "USD").
    pub second_dip_trigger1_usd: f64,
    /// Pemicu 2: % di atas entry untuk HARGA BELI di limit order Aura.
    pub second_dip_trigger2_pct: f64,
    /// Batas waktu Pemicu 1 (menit): kalau harga tidak mencapai Pemicu 1 dalam
    /// waktu ini, target dibatalkan. Default 120 (2 jam).
    pub second_dip_trigger1_timeout_mins: u64,
    /// Batas waktu Pemicu 2 / masa berlaku order (menit): kalau limit order
    /// tidak fill dalam waktu ini, order kedaluwarsa. 0 = pakai default Aura.
    pub second_dip_trigger2_timeout_mins: u64,
    /// Mode harga Pemicu 2: "PCT" (% di atas entry), "USD" (harga absolut USD),
    /// atau "MCAP" (market cap USD). Default "PCT".
    pub second_dip_trigger2_mode: String,
    /// Harga Pemicu 2 absolut dalam USD (dipakai kalau mode = "USD").
    pub second_dip_trigger2_usd: f64,
    /// Market cap Pemicu 2 dalam USD (dipakai kalau mode = "MCAP").
    pub second_dip_trigger2_mcap: f64,
    pub second_dip_tip: f64,
    pub second_dip_prio: f64,
    /// Jumlah beli dalam USD (USDC).
    pub second_dip_amount_usd: f64,
    /// Mode aktif: "BUY_DIP" (limit order biasa) atau "SECOND_DIP".
    pub dip_mode: String,
    /// Mode simulasi Second Dip: true = order TIDAK dikirim ke Aura (dry run).
    pub second_dip_dry_run: bool,
    // ── Auto Sell (Take Profit otomatis setelah limit order beli terisi) ──
    /// Toggle Auto Sell: true = setelah order beli terisi, bot otomatis
    /// pasang order jual (Take Profit). Berlaku untuk Second Dip + manual.
    pub auto_sell_active: bool,
    /// Mode harga Take Profit: "PCT" (% di atas harga beli), "USD" (harga
    /// absolut USD), atau "MCAP" (market cap USD). Default "PCT".
    pub auto_sell_tp_mode: String,
    /// % Take Profit di atas harga beli (dipakai kalau mode = "PCT").
    pub auto_sell_tp_pct: f64,
    /// Harga Take Profit absolut dalam USD (dipakai kalau mode = "USD").
    pub auto_sell_tp_usd: f64,
    /// Market cap Take Profit dalam USD (dipakai kalau mode = "MCAP").
    pub auto_sell_tp_mcap: f64,
    /// % posisi yang dijual saat TP kena (dipakai untuk SwapAmount::SellPerc).
    /// 100 = jual seluruh posisi. Default 100.0.
    pub auto_sell_sell_pct: f64,
    // ── Auto Sell Second Dip (config terpisah dari limit order biasa) ──
    /// Toggle Auto Sell khusus order beli Second Dip.
    pub auto_sell_sd_active: bool,
    /// Mode harga TP Second Dip: "PCT" / "USD" / "MCAP".
    pub auto_sell_sd_tp_mode: String,
    /// % TP Second Dip di atas harga beli (mode "PCT").
    pub auto_sell_sd_tp_pct: f64,
    /// Harga TP Second Dip absolut USD (mode "USD").
    pub auto_sell_sd_tp_usd: f64,
    /// Mcap TP Second Dip USD (mode "MCAP").
    pub auto_sell_sd_tp_mcap: f64,
    /// % posisi Second Dip yang dijual saat TP kena.
    pub auto_sell_sd_sell_pct: f64,
    // ── Tip & Prio order jual TP (dikirim ke Aura saat pasang TP sell) ──
    /// Tip (SOL) untuk order jual TP Limit Biasa.
    pub auto_sell_tip: f64,
    /// Priority fee (SOL) untuk order jual TP Limit Biasa.
    pub auto_sell_prio: f64,
    /// Tip (SOL) untuk order jual TP Second Dip.
    pub auto_sell_sd_tip: f64,
    /// Priority fee (SOL) untuk order jual TP Second Dip.
    pub auto_sell_sd_prio: f64,
}

impl Default for DbSettings {
    fn default() -> Self {
        Self {
            sell_tip_fee: 0.0015,
            sell_prio_fee: 0.0015,
            sell_slippage: "95%".to_string(),
            preset_kecil_tip: 0.0005,
            preset_kecil_prio: 0.0005,
            preset_sedang_tip: 0.001,
            preset_sedang_prio: 0.001,
            preset_besar_tip: 0.002,
            preset_besar_prio: 0.002,
            preset_mega_tip: 0.005,
            preset_mega_prio: 0.005,
            bought_token: None,
            second_dip_active: false,
            second_dip_target_wallet: DEFAULT_SECOND_DIP_WALLET.to_string(),
            second_dip_trigger1_pct: 100.0,
            second_dip_trigger1_mode: "PCT".to_string(),
            second_dip_trigger1_usd: 0.0,
            second_dip_trigger1_timeout_mins: 120,
            second_dip_trigger2_timeout_mins: 0,
            second_dip_trigger2_mode: "PCT".to_string(),
            second_dip_trigger2_usd: 0.0,
            second_dip_trigger2_mcap: 0.0,
            second_dip_trigger2_pct: 20.0,
            second_dip_tip: 0.0035,
            second_dip_prio: 0.0035,
            second_dip_amount_usd: 26.0,
            dip_mode: "BUY_DIP".to_string(),
            second_dip_dry_run: true,
            auto_sell_active: false,
            auto_sell_tp_mode: "PCT".to_string(),
            auto_sell_tp_pct: 40.0,
            auto_sell_tp_usd: 0.0,
            auto_sell_tp_mcap: 0.0,
            auto_sell_sell_pct: 100.0,
            // Config Second Dip defaultnya sama dengan config manual.
            auto_sell_sd_active: false,
            auto_sell_sd_tp_mode: "PCT".to_string(),
            auto_sell_sd_tp_pct: 40.0,
            auto_sell_sd_tp_usd: 0.0,
            auto_sell_sd_tp_mcap: 0.0,
            auto_sell_sd_sell_pct: 100.0,
            // Tip/Prio TP sell default = default Aura (0.0035 SOL).
            auto_sell_tip: 0.0035,
            auto_sell_prio: 0.0035,
            auto_sell_sd_tip: 0.0035,
            auto_sell_sd_prio: 0.0035,
        }
    }
}

// ─── Limit Order History (persistent) ─────────────────────────────────────────

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct DbLimitOrder {
    pub id: i64,
    pub order_type: String, // "BUY" or "SELL"
    pub token: String,
    pub target: String,
    pub tip_fee: f64,
    pub prio_fee: f64,
    pub created_at: String,
    pub aura_order_id: Option<i64>,
    pub amount_sol: f64, // lamport amount in SOL (for BUY: SOL to spend; for SELL: 0 = 100%)
}


// ─── Error Logs ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct DbErrorLog {
    pub id: i64,
    pub order_id: i64,
    pub token: String,
    pub error_msg: String,
    pub created_at: String,
}

// ─── Init ──────────────────────────────────────────────────────────────────────

pub fn init_db() -> Result<Connection> {
    let conn = Connection::open("bot_data.db")?;

    // Settings table
    // NOTE: kolom auto_limit_* / limit_* lama sengaja TIDAK dihapus (SQLite tidak
    // mendukung DROP COLUMN yang aman di semua versi) — kolom itu tidak lagi
    // dipakai oleh kode, hanya tersisa di skema DB lama.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS settings (
            id INTEGER PRIMARY KEY,
            auto_limit_active INTEGER DEFAULT 0,
            limit_tip_fee REAL DEFAULT 0.0015,
            limit_prio_fee REAL DEFAULT 0.0015,
            limit_act_time TEXT DEFAULT '0s',
            limit_target_pnl TEXT DEFAULT '50%',
            sell_tip_fee REAL DEFAULT 0.0015,
            sell_prio_fee REAL DEFAULT 0.0015,
            sell_slippage TEXT DEFAULT '95%',
            preset_kecil_tip REAL DEFAULT 0.0005,
            preset_kecil_prio REAL DEFAULT 0.0005,
            preset_sedang_tip REAL DEFAULT 0.001,
            preset_sedang_prio REAL DEFAULT 0.001,
            preset_besar_tip REAL DEFAULT 0.002,
            preset_besar_prio REAL DEFAULT 0.002,
            preset_mega_tip REAL DEFAULT 0.005,
            preset_mega_prio REAL DEFAULT 0.005
        )",
        [],
    )?;

    // Chats table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS chats (
            chat_id INTEGER PRIMARY KEY
        )",
        [],
    )?;

    // Limit orders history table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS limit_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_type TEXT NOT NULL,
            token TEXT NOT NULL,
            target TEXT NOT NULL,
            tip_fee REAL NOT NULL,
            prio_fee REAL NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            aura_order_id INTEGER,
            amount_sol REAL DEFAULT 0.0
        )",
        [],
    )?;

    // Error logs table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS error_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER DEFAULT 0,
            token TEXT NOT NULL,
            error_msg TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )",
        [],
    )?;

    // Migrations / Ensure columns exist
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN bought_token TEXT", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN auto_limit_mode TEXT DEFAULT 'LIMIT'", []);
    // Second Dip columns
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN second_dip_active INTEGER DEFAULT 0", []);
    let _ = conn.execute(
        "ALTER TABLE settings ADD COLUMN second_dip_target_wallet TEXT DEFAULT 'AURARwy7F1sfbxeRkdcUCVg2eG1mpeHG1jChRU9S7hhP'",
        [],
    );
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN second_dip_trigger1_pct REAL DEFAULT 100.0", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN second_dip_trigger2_pct REAL DEFAULT 20.0", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN second_dip_tip REAL DEFAULT 0.0035", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN second_dip_prio REAL DEFAULT 0.0035", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN second_dip_amount_usd REAL DEFAULT 26.0", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN dip_mode TEXT DEFAULT 'BUY_DIP'", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN second_dip_dry_run INTEGER DEFAULT 1", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN second_dip_trigger1_timeout_mins INTEGER DEFAULT 120", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN second_dip_trigger2_timeout_mins INTEGER DEFAULT 0", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN second_dip_trigger2_mode TEXT DEFAULT 'PCT'", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN second_dip_trigger2_usd REAL DEFAULT 0.0", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN second_dip_trigger2_mcap REAL DEFAULT 0.0", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN second_dip_trigger1_mode TEXT DEFAULT 'PCT'", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN second_dip_trigger1_usd REAL DEFAULT 0.0", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN auto_sell_active INTEGER DEFAULT 0", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN auto_sell_tp_mode TEXT DEFAULT 'PCT'", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN auto_sell_tp_pct REAL DEFAULT 40.0", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN auto_sell_tp_usd REAL DEFAULT 0.0", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN auto_sell_tp_mcap REAL DEFAULT 0.0", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN auto_sell_sell_pct REAL DEFAULT 100.0", []);
    // Config Auto Sell Second Dip (terpisah dari limit order biasa).
    // Tanpa DEFAULT supaya baris lama NULL dulu, lalu disalin dari config
    // manual — jadi perilaku Second Dip tidak berubah diam-diam.
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN auto_sell_sd_active INTEGER", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN auto_sell_sd_tp_mode TEXT", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN auto_sell_sd_tp_pct REAL", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN auto_sell_sd_tp_usd REAL", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN auto_sell_sd_tp_mcap REAL", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN auto_sell_sd_sell_pct REAL", []);
    let _ = conn.execute(
        "UPDATE settings SET
            auto_sell_sd_active = COALESCE(auto_sell_active, 0),
            auto_sell_sd_tp_mode = COALESCE(auto_sell_tp_mode, 'PCT'),
            auto_sell_sd_tp_pct = COALESCE(auto_sell_tp_pct, 40.0),
            auto_sell_sd_tp_usd = COALESCE(auto_sell_tp_usd, 0.0),
            auto_sell_sd_tp_mcap = COALESCE(auto_sell_tp_mcap, 0.0),
            auto_sell_sd_sell_pct = COALESCE(auto_sell_sell_pct, 100.0)
         WHERE auto_sell_sd_active IS NULL",
        [],
    );
    // Tip/Prio order jual TP. Manual default 0.0035 (default Aura).
    // Second Dip disalin dari tip/prio BELI Second Dip (0.012 saat ini)
    // supaya TP sell-nya tetap sama seperti perilaku lama.
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN auto_sell_tip REAL DEFAULT 0.0035", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN auto_sell_prio REAL DEFAULT 0.0035", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN auto_sell_sd_tip REAL", []);
    let _ = conn.execute("ALTER TABLE settings ADD COLUMN auto_sell_sd_prio REAL", []);
    let _ = conn.execute(
        "UPDATE settings SET
            auto_sell_sd_tip = COALESCE(second_dip_tip, 0.0035),
            auto_sell_sd_prio = COALESCE(second_dip_prio, 0.0035)
         WHERE auto_sell_sd_tip IS NULL",
        [],
    );

    // Insert default settings row if not present
    let count: i64 = conn.query_row("SELECT count(*) FROM settings WHERE id = 1", [], |row| row.get(0))?;
    if count == 0 {
        let def = DbSettings::default();
        conn.execute(
            "INSERT INTO settings (id, sell_tip_fee, sell_prio_fee, sell_slippage,
             preset_kecil_tip, preset_kecil_prio, preset_sedang_tip, preset_sedang_prio,
             preset_besar_tip, preset_besar_prio, preset_mega_tip, preset_mega_prio, bought_token,
             second_dip_active, second_dip_target_wallet, second_dip_trigger1_pct, second_dip_trigger2_pct,
             second_dip_tip, second_dip_prio, second_dip_amount_usd, dip_mode, second_dip_dry_run,
             second_dip_trigger1_timeout_mins, second_dip_trigger2_timeout_mins,
             second_dip_trigger2_mode, second_dip_trigger2_usd, second_dip_trigger2_mcap,
             second_dip_trigger1_mode, second_dip_trigger1_usd)
             VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28)",
            params![
                def.sell_tip_fee,
                def.sell_prio_fee,
                def.sell_slippage,
                def.preset_kecil_tip,
                def.preset_kecil_prio,
                def.preset_sedang_tip,
                def.preset_sedang_prio,
                def.preset_besar_tip,
                def.preset_besar_prio,
                def.preset_mega_tip,
                def.preset_mega_prio,
                def.bought_token,
                def.second_dip_active as i32,
                def.second_dip_target_wallet,
                def.second_dip_trigger1_pct,
                def.second_dip_trigger2_pct,
                def.second_dip_tip,
                def.second_dip_prio,
                def.second_dip_amount_usd,
                def.dip_mode,
                def.second_dip_dry_run as i32,
                def.second_dip_trigger1_timeout_mins as i64,
                def.second_dip_trigger2_timeout_mins as i64,
                def.second_dip_trigger2_mode,
                def.second_dip_trigger2_usd,
                def.second_dip_trigger2_mcap,
                def.second_dip_trigger1_mode,
                def.second_dip_trigger1_usd,
            ],
        )?;
    }

    // Tabel wallet target Second Dip (multi-wallet).
    create_second_dip_targets_table(&conn)?;
    // Tabel lacak order beli untuk Auto Sell (Take Profit).
    create_auto_sell_tracked_table(&conn)?;
    // Migrasi: kalau tabel masih kosong, isi dari kolom settings lama
    // (supaya wallet 9S7hhP yang sudah diset tidak hilang).
    let target_count: i64 = conn.query_row(
        "SELECT count(*) FROM second_dip_targets",
        [],
        |row| row.get(0),
    )?;
    if target_count == 0 {
        let legacy: String = conn
            .query_row(
                "SELECT second_dip_target_wallet FROM settings WHERE id = 1",
                [],
                |row| row.get::<_, Option<String>>(0),
            )
            .unwrap_or(None)
            .unwrap_or_else(|| DEFAULT_SECOND_DIP_WALLET.to_string());
        let seed = if is_valid_solana_address(&legacy) {
            legacy
        } else {
            DEFAULT_SECOND_DIP_WALLET.to_string()
        };
        let _ = conn.execute(
            "INSERT OR IGNORE INTO second_dip_targets (wallet) VALUES (?1)",
            [seed],
        );
    }

    Ok(conn)
}

// ─── Settings CRUD ─────────────────────────────────────────────────────────────

pub fn load_settings(conn: &Connection) -> Result<DbSettings> {
    conn.query_row(
        "SELECT sell_tip_fee, sell_prio_fee, sell_slippage,
                preset_kecil_tip, preset_kecil_prio, preset_sedang_tip, preset_sedang_prio,
                preset_besar_tip, preset_besar_prio, preset_mega_tip, preset_mega_prio, bought_token,
                second_dip_active, second_dip_target_wallet, second_dip_trigger1_pct, second_dip_trigger2_pct,
                second_dip_tip, second_dip_prio, second_dip_amount_usd, dip_mode, second_dip_dry_run,
                second_dip_trigger1_timeout_mins, second_dip_trigger2_timeout_mins,
                second_dip_trigger2_mode, second_dip_trigger2_usd, second_dip_trigger2_mcap,
                second_dip_trigger1_mode, second_dip_trigger1_usd,
                auto_sell_active, auto_sell_tp_mode, auto_sell_tp_pct,
                auto_sell_tp_usd, auto_sell_tp_mcap, auto_sell_sell_pct,
                auto_sell_sd_active, auto_sell_sd_tp_mode, auto_sell_sd_tp_pct,
                auto_sell_sd_tp_usd, auto_sell_sd_tp_mcap, auto_sell_sd_sell_pct,
                auto_sell_tip, auto_sell_prio, auto_sell_sd_tip, auto_sell_sd_prio
         FROM settings WHERE id = 1",
        [],
        |row| {
            Ok(DbSettings {
                sell_tip_fee: row.get(0)?,
                sell_prio_fee: row.get(1)?,
                sell_slippage: row.get(2)?,
                preset_kecil_tip: row.get(3)?,
                preset_kecil_prio: row.get(4)?,
                preset_sedang_tip: row.get(5)?,
                preset_sedang_prio: row.get(6)?,
                preset_besar_tip: row.get(7)?,
                preset_besar_prio: row.get(8)?,
                preset_mega_tip: row.get(9)?,
                preset_mega_prio: row.get(10)?,
                bought_token: row.get::<_, Option<String>>(11).unwrap_or(None),
                second_dip_active: row.get::<_, i32>(12).unwrap_or(0) != 0,
                second_dip_target_wallet: row
                    .get::<_, Option<String>>(13)
                    .unwrap_or(None)
                    .unwrap_or_else(|| DEFAULT_SECOND_DIP_WALLET.to_string()),
                second_dip_trigger1_pct: row.get::<_, Option<f64>>(14).unwrap_or(None).unwrap_or(100.0),
                second_dip_trigger2_pct: row.get::<_, Option<f64>>(15).unwrap_or(None).unwrap_or(20.0),
                second_dip_tip: row.get::<_, Option<f64>>(16).unwrap_or(None).unwrap_or(0.0035),
                second_dip_prio: row.get::<_, Option<f64>>(17).unwrap_or(None).unwrap_or(0.0035),
                second_dip_amount_usd: row.get::<_, Option<f64>>(18).unwrap_or(None).unwrap_or(26.0),
                dip_mode: row
                    .get::<_, Option<String>>(19)
                    .unwrap_or(None)
                    .unwrap_or_else(|| "BUY_DIP".to_string()),
                // Default true (aman): kalau kolom belum ada di DB lama, mode simulasi aktif.
                second_dip_dry_run: row.get::<_, i32>(20).unwrap_or(1) != 0,
                second_dip_trigger1_timeout_mins: row.get::<_, i64>(21).unwrap_or(120).max(0) as u64,
                second_dip_trigger2_timeout_mins: row.get::<_, i64>(22).unwrap_or(0).max(0) as u64,
                second_dip_trigger2_mode: row
                    .get::<_, Option<String>>(23)
                    .unwrap_or(None)
                    .unwrap_or_else(|| "PCT".to_string()),
                second_dip_trigger2_usd: row.get::<_, Option<f64>>(24).unwrap_or(None).unwrap_or(0.0),
                second_dip_trigger2_mcap: row.get::<_, Option<f64>>(25).unwrap_or(None).unwrap_or(0.0),
                second_dip_trigger1_mode: row
                    .get::<_, Option<String>>(26)
                    .unwrap_or(None)
                    .unwrap_or_else(|| "PCT".to_string()),
                second_dip_trigger1_usd: row.get::<_, Option<f64>>(27).unwrap_or(None).unwrap_or(0.0),
                auto_sell_active: row.get::<_, i32>(28).unwrap_or(0) != 0,
                auto_sell_tp_mode: row
                    .get::<_, Option<String>>(29)
                    .unwrap_or(None)
                    .unwrap_or_else(|| "PCT".to_string()),
                auto_sell_tp_pct: row.get::<_, Option<f64>>(30).unwrap_or(None).unwrap_or(40.0),
                auto_sell_tp_usd: row.get::<_, Option<f64>>(31).unwrap_or(None).unwrap_or(0.0),
                auto_sell_tp_mcap: row.get::<_, Option<f64>>(32).unwrap_or(None).unwrap_or(0.0),
                auto_sell_sell_pct: row.get::<_, Option<f64>>(33).unwrap_or(None).unwrap_or(100.0),
                // Config Second Dip: NULL (DB lama / baris baru) -> pakai
                // nilai config manual supaya perilaku tidak berubah.
                auto_sell_sd_active: row.get::<_, Option<i32>>(34).unwrap_or(None).map(|v| v != 0).unwrap_or_else(|| row.get::<_, i32>(28).unwrap_or(0) != 0),
                auto_sell_sd_tp_mode: row
                    .get::<_, Option<String>>(35)
                    .unwrap_or(None)
                    .unwrap_or_else(|| {
                        row.get::<_, Option<String>>(29)
                            .unwrap_or(None)
                            .unwrap_or_else(|| "PCT".to_string())
                    }),
                auto_sell_sd_tp_pct: row.get::<_, Option<f64>>(36).unwrap_or(None).unwrap_or_else(|| row.get::<_, Option<f64>>(30).unwrap_or(None).unwrap_or(40.0)),
                auto_sell_sd_tp_usd: row.get::<_, Option<f64>>(37).unwrap_or(None).unwrap_or_else(|| row.get::<_, Option<f64>>(31).unwrap_or(None).unwrap_or(0.0)),
                auto_sell_sd_tp_mcap: row.get::<_, Option<f64>>(38).unwrap_or(None).unwrap_or_else(|| row.get::<_, Option<f64>>(32).unwrap_or(None).unwrap_or(0.0)),
                auto_sell_sd_sell_pct: row.get::<_, Option<f64>>(39).unwrap_or(None).unwrap_or_else(|| row.get::<_, Option<f64>>(33).unwrap_or(None).unwrap_or(100.0)),
                auto_sell_tip: row.get::<_, Option<f64>>(40).unwrap_or(None).unwrap_or(0.0035),
                auto_sell_prio: row.get::<_, Option<f64>>(41).unwrap_or(None).unwrap_or(0.0035),
                // Tip/Prio TP Second Dip: NULL -> pakai tip/prio BELI
                // Second Dip (kolom 16/17), lalu 0.0035.
                auto_sell_sd_tip: row.get::<_, Option<f64>>(42).unwrap_or(None).unwrap_or_else(|| row.get::<_, Option<f64>>(16).unwrap_or(None).unwrap_or(0.0035)),
                auto_sell_sd_prio: row.get::<_, Option<f64>>(43).unwrap_or(None).unwrap_or_else(|| row.get::<_, Option<f64>>(17).unwrap_or(None).unwrap_or(0.0035)),
            })
        },
    )
}

pub fn save_settings(conn: &Connection, set: &DbSettings) -> Result<()> {
    conn.execute(
        "UPDATE settings SET
            sell_tip_fee = ?1,
            sell_prio_fee = ?2,
            sell_slippage = ?3,
            preset_kecil_tip = ?4,
            preset_kecil_prio = ?5,
            preset_sedang_tip = ?6,
            preset_sedang_prio = ?7,
            preset_besar_tip = ?8,
            preset_besar_prio = ?9,
            preset_mega_tip = ?10,
            preset_mega_prio = ?11,
            bought_token = ?12,
            second_dip_active = ?13,
            second_dip_target_wallet = ?14,
            second_dip_trigger1_pct = ?15,
            second_dip_trigger2_pct = ?16,
            second_dip_tip = ?17,
            second_dip_prio = ?18,
            second_dip_amount_usd = ?19,
            dip_mode = ?20,
            second_dip_dry_run = ?21,
            second_dip_trigger1_timeout_mins = ?22,
            second_dip_trigger2_timeout_mins = ?23,
            second_dip_trigger2_mode = ?24,
            second_dip_trigger2_usd = ?25,
            second_dip_trigger2_mcap = ?26,
            second_dip_trigger1_mode = ?27,
            second_dip_trigger1_usd = ?28,
            auto_sell_active = ?29,
            auto_sell_tp_mode = ?30,
            auto_sell_tp_pct = ?31,
            auto_sell_tp_usd = ?32,
            auto_sell_tp_mcap = ?33,
            auto_sell_sell_pct = ?34,
            auto_sell_sd_active = ?35,
            auto_sell_sd_tp_mode = ?36,
            auto_sell_sd_tp_pct = ?37,
            auto_sell_sd_tp_usd = ?38,
            auto_sell_sd_tp_mcap = ?39,
            auto_sell_sd_sell_pct = ?40,
            auto_sell_tip = ?41,
            auto_sell_prio = ?42,
            auto_sell_sd_tip = ?43,
            auto_sell_sd_prio = ?44
         WHERE id = 1",
        params![
            set.sell_tip_fee,
            set.sell_prio_fee,
            set.sell_slippage,
            set.preset_kecil_tip,
            set.preset_kecil_prio,
            set.preset_sedang_tip,
            set.preset_sedang_prio,
            set.preset_besar_tip,
            set.preset_besar_prio,
            set.preset_mega_tip,
            set.preset_mega_prio,
            set.bought_token,
            set.second_dip_active as i32,
            set.second_dip_target_wallet,
            set.second_dip_trigger1_pct,
            set.second_dip_trigger2_pct,
            set.second_dip_tip,
            set.second_dip_prio,
            set.second_dip_amount_usd,
            set.dip_mode,
            set.second_dip_dry_run as i32,
            set.second_dip_trigger1_timeout_mins as i64,
            set.second_dip_trigger2_timeout_mins as i64,
            set.second_dip_trigger2_mode,
            set.second_dip_trigger2_usd,
            set.second_dip_trigger2_mcap,
            set.second_dip_trigger1_mode,
            set.second_dip_trigger1_usd,
            set.auto_sell_active as i32,
            set.auto_sell_tp_mode,
            set.auto_sell_tp_pct,
            set.auto_sell_tp_usd,
            set.auto_sell_tp_mcap,
            set.auto_sell_sell_pct,
            set.auto_sell_sd_active as i32,
            set.auto_sell_sd_tp_mode,
            set.auto_sell_sd_tp_pct,
            set.auto_sell_sd_tp_usd,
            set.auto_sell_sd_tp_mcap,
            set.auto_sell_sd_sell_pct,
            set.auto_sell_tip,
            set.auto_sell_prio,
            set.auto_sell_sd_tip,
            set.auto_sell_sd_prio,
        ],
    )?;
    Ok(())
}

// ─── Second Dip Targets (multi-wallet) ───────────────────────────────────────

pub fn create_second_dip_targets_table(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS second_dip_targets (
            wallet TEXT PRIMARY KEY,
            added_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        )",
        [],
    )?;
    Ok(())
}

/// Daftar wallet target, urut sesuai waktu ditambahkan (rowid).
pub fn load_second_dip_targets(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT wallet FROM second_dip_targets ORDER BY rowid ASC")?;
    let iter = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut out = Vec::new();
    for w in iter {
        out.push(w?);
    }
    Ok(out)
}

// ─── Auto Sell: lacak order beli yang menunggu Take Profit ──────────────────

pub fn create_auto_sell_tracked_table(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS auto_sell_tracked (
            aura_order_id INTEGER PRIMARY KEY,
            mint TEXT NOT NULL,
            buy_price REAL NOT NULL,
            amount_usd REAL NOT NULL,
            tip REAL NOT NULL,
            prio REAL NOT NULL,
            source TEXT NOT NULL,
            sell_placed INTEGER DEFAULT 0,
            sell_order_id INTEGER,
            created_at INTEGER NOT NULL
        )",
        [],
    )?;
    Ok(())
}

/// Catat order beli untuk dilacak Auto Sell. INSERT OR REPLACE supaya
/// pencatatan ulang order yang sama tidak duplikat.
/// Apakah order ini sudah dilacak Auto Sell? (hindari duplikat / menimpa
/// source + harga entry order yang sudah dilacak saat dipasang.)
pub fn is_auto_sell_tracked(conn: &Connection, aura_order_id: i64) -> Result<bool> {
    let n: i64 = conn.query_row(
        "SELECT COUNT(*) FROM auto_sell_tracked WHERE aura_order_id = ?1",
        params![aura_order_id],
        |row| row.get(0),
    )?;
    Ok(n > 0)
}

/// Ambil 1 baris lacak Auto Sell: (source, sell_placed).
/// Dipakai fast path (place_tp_now) untuk tahu config mana yang dipakai
/// (manual/second_dip) dan apakah TP sudah terpasang (hindari dobel).
pub fn get_auto_sell_tracked(
    conn: &Connection,
    aura_order_id: i64,
) -> Result<Option<(String, i64)>> {
    let mut stmt = conn.prepare(
        "SELECT source, sell_placed FROM auto_sell_tracked WHERE aura_order_id = ?1",
    )?;
    let mut rows = stmt.query(params![aura_order_id])?;
    if let Some(row) = rows.next()? {
        let source: String = row.get(0)?;
        let sell_placed: i64 = row.get(1)?;
        Ok(Some((source, sell_placed)))
    } else {
        Ok(None)
    }
}

pub fn insert_auto_sell_tracked(
    conn: &Connection,
    aura_order_id: i64,
    mint: &str,
    buy_price: f64,
    amount_usd: f64,
    tip: f64,
    prio: f64,
    source: &str,
) -> Result<()> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    conn.execute(
        "INSERT OR REPLACE INTO auto_sell_tracked
            (aura_order_id, mint, buy_price, amount_usd, tip, prio, source, sell_placed, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, ?8)",
        params![aura_order_id, mint, buy_price, amount_usd, tip, prio, source, now],
    )?;
    Ok(())
}

/// Tandai order jual TP sudah dipasang untuk order beli ini.
pub fn mark_auto_sell_placed(conn: &Connection, aura_order_id: i64, sell_order_id: i64) -> Result<()> {
    conn.execute(
        "UPDATE auto_sell_tracked SET sell_placed = 1, sell_order_id = ?2 WHERE aura_order_id = ?1",
        params![aura_order_id, sell_order_id],
    )?;
    Ok(())
}

/// Hapus dari lacak (order beli dibatalkan / tidak terisi).
pub fn remove_auto_sell_tracked(conn: &Connection, aura_order_id: i64) -> Result<()> {
    conn.execute(
        "DELETE FROM auto_sell_tracked WHERE aura_order_id = ?1",
        params![aura_order_id],
    )?;
    Ok(())
}

/// Tambah wallet target. Ok(true) = berhasil ditambahkan,
/// Ok(false) = wallet sudah ada di daftar.
pub fn add_second_dip_target(conn: &Connection, wallet: &str) -> Result<bool> {
    let n = conn.execute(
        "INSERT OR IGNORE INTO second_dip_targets (wallet) VALUES (?1)",
        [wallet],
    )?;
    Ok(n > 0)
}

/// Hapus wallet target. Ok(true) = ada & dihapus,
/// Ok(false) = wallet tidak ada di daftar.
pub fn remove_second_dip_target(conn: &Connection, wallet: &str) -> Result<bool> {
    let n = conn.execute(
        "DELETE FROM second_dip_targets WHERE wallet = ?1",
        [wallet],
    )?;
    Ok(n > 0)
}

// ─── Chats CRUD ────────────────────────────────────────────────────────────────

pub fn load_chats(conn: &Connection) -> Result<HashSet<ChatId>> {
    let mut stmt = conn.prepare("SELECT chat_id FROM chats")?;
    let chat_iter = stmt.query_map([], |row| {
        let id: i64 = row.get(0)?;
        Ok(ChatId(id))
    })?;
    let mut chats = HashSet::new();
    for c in chat_iter {
        chats.insert(c?);
    }
    Ok(chats)
}

pub fn save_chat(conn: &Connection, chat_id: ChatId) -> Result<()> {
    conn.execute("INSERT OR IGNORE INTO chats (chat_id) VALUES (?1)", [chat_id.0])?;
    Ok(())
}

// ─── Limit Order History CRUD ─────────────────────────────────────────────────

pub fn insert_limit_order(conn: &Connection, order_type: &str, token: &str, target: &str, tip: f64, prio: f64, amount_sol: f64) -> Result<i64> {
    conn.execute(
        "INSERT INTO limit_orders (order_type, token, target, tip_fee, prio_fee, amount_sol) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![order_type, token, target, tip, prio, amount_sol],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Insert limit order yang sudah punya aura_order_id (dipakai sync dari Aura).
pub fn insert_limit_order_with_aura_id(
    conn: &Connection,
    order_type: &str,
    token: &str,
    target: &str,
    tip: f64,
    prio: f64,
    amount_sol: f64,
    aura_order_id: i64,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO limit_orders (order_type, token, target, tip_fee, prio_fee, amount_sol, aura_order_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![order_type, token, target, tip, prio, amount_sol, aura_order_id],
    )?;
    Ok(conn.last_insert_rowid())
}

pub fn load_limit_orders(conn: &Connection) -> Result<Vec<DbLimitOrder>> {
    let mut stmt = conn.prepare(
        "SELECT id, order_type, token, target, tip_fee, prio_fee, created_at, aura_order_id, amount_sol FROM limit_orders ORDER BY id ASC"
    )?;
    let iter = stmt.query_map([], |row| {
        Ok(DbLimitOrder {
            id: row.get(0)?,
            order_type: row.get(1)?,
            token: row.get(2)?,
            target: row.get(3)?,
            tip_fee: row.get(4)?,
            prio_fee: row.get(5)?,
            created_at: row.get(6)?,
            aura_order_id: row.get(7)?,
            amount_sol: row.get::<_, Option<f64>>(8)?.unwrap_or(0.0),
        })
    })?;
    let mut orders = Vec::new();
    for o in iter { orders.push(o?); }
    Ok(orders)
}

pub fn update_limit_order(conn: &Connection, id: i64, target: &str, tip: f64, prio: f64) -> Result<()> {
    conn.execute(
        "UPDATE limit_orders SET target = ?1, tip_fee = ?2, prio_fee = ?3 WHERE id = ?4",
        params![target, tip, prio, id],
    )?;
    Ok(())
}

pub fn update_limit_order_amount(conn: &Connection, id: i64, amount: f64) -> Result<()> {
    conn.execute(
        "UPDATE limit_orders SET amount_sol = ?1 WHERE id = ?2",
        params![amount, id],
    )?;
    Ok(())
}

/// Hapus semua limit order lokal untuk satu token (mint).
pub fn delete_limit_orders_by_token(conn: &Connection, token: &str) -> Result<()> {
    conn.execute("DELETE FROM limit_orders WHERE token = ?1", params![token])?;
    Ok(())
}

/// Hapus SEMUA limit order lokal.
pub fn delete_all_limit_orders(conn: &Connection) -> Result<()> {
    conn.execute("DELETE FROM limit_orders", [])?;
    Ok(())
}

pub fn delete_limit_order(conn: &Connection, id: i64) -> Result<()> {
    conn.execute("DELETE FROM limit_orders WHERE id = ?1", [id])?;
    Ok(())
}

// ─── Error Log CRUD ────────────────────────────────────────────────────────────

pub fn insert_error_log(conn: &Connection, order_id: i64, token: &str, error_msg: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO error_logs (order_id, token, error_msg) VALUES (?1, ?2, ?3)",
        params![order_id, token, error_msg],
    )?;
    Ok(())
}

pub fn load_error_logs(conn: &Connection) -> Result<Vec<DbErrorLog>> {
    let mut stmt = conn.prepare(
        "SELECT id, order_id, token, error_msg, created_at FROM error_logs ORDER BY id DESC LIMIT 50"
    )?;
    let iter = stmt.query_map([], |row| {
        Ok(DbErrorLog {
            id: row.get(0)?,
            order_id: row.get(1)?,
            token: row.get(2)?,
            error_msg: row.get(3)?,
            created_at: row.get(4)?,
        })
    })?;
    let mut logs = Vec::new();
    for l in iter { logs.push(l?); }
    Ok(logs)
}

pub fn clear_error_logs(conn: &Connection) -> Result<()> {
    conn.execute("DELETE FROM error_logs", [])?;
    Ok(())
}

pub fn update_aura_order_id(conn: &Connection, id: i64, aura_order_id: i64) -> Result<()> {
    conn.execute(
        "UPDATE limit_orders SET aura_order_id = ?1 WHERE id = ?2",
        params![aura_order_id, id],
    )?;
    Ok(())
}

pub fn get_limit_order(conn: &Connection, id: i64) -> Result<DbLimitOrder> {
    conn.query_row(
        "SELECT id, order_type, token, target, tip_fee, prio_fee, created_at, aura_order_id, amount_sol FROM limit_orders WHERE id = ?1",
        params![id],
        |row| {
            Ok(DbLimitOrder {
                id: row.get(0)?,
                order_type: row.get(1)?,
                token: row.get(2)?,
                target: row.get(3)?,
                tip_fee: row.get(4)?,
                prio_fee: row.get(5)?,
                created_at: row.get(6)?,
                aura_order_id: row.get(7)?,
                amount_sol: row.get::<_, Option<f64>>(8)?.unwrap_or(0.0),
            })
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn mem_conn() -> Connection {
        let conn = Connection::open(":memory:").unwrap();
        create_second_dip_targets_table(&conn).unwrap();
        conn
    }

    #[test]
    fn test_is_valid_solana_address() {
        // Valid: default 9S7hhP (44 karakter) + wSOL mint (44 karakter).
        assert!(is_valid_solana_address(
            "AURARwy7F1sfbxeRkdcUCVg2eG1mpeHG1jChRU9S7hhP"
        ));
        assert!(is_valid_solana_address(
            "So11111111111111111111111111111111111111112"
        ));
        assert!(is_valid_solana_address(&"A".repeat(32)));
        assert!(is_valid_solana_address(&"A".repeat(44)));
        // Terlalu pendek / panjang.
        assert!(!is_valid_solana_address(&"A".repeat(31)));
        assert!(!is_valid_solana_address(&"A".repeat(45)));
        assert!(!is_valid_solana_address(""));
        assert!(!is_valid_solana_address("pendek"));
        // Karakter yang BUKAN base58: 0, O, I, l, spasi.
        let base = "A".repeat(43);
        assert!(!is_valid_solana_address(&format!("{}0", base)));
        assert!(!is_valid_solana_address(&format!("{}O", base)));
        assert!(!is_valid_solana_address(&format!("{}I", base)));
        assert!(!is_valid_solana_address(&format!("{}l", base)));
        assert!(!is_valid_solana_address(
            "AURARwy7F1sfbxeRkdcUCVg2eG1mpeHG1jChRU9S7hhP "
        ));
    }

    #[test]
    fn test_add_remove_targets() {
        let conn = mem_conn();
        assert!(add_second_dip_target(
            &conn,
            "AURARwy7F1sfbxeRkdcUCVg2eG1mpeHG1jChRU9S7hhP"
        )
        .unwrap());
        // Duplikat ditolak (INSERT OR IGNORE -> 0 baris berubah).
        assert!(!add_second_dip_target(
            &conn,
            "AURARwy7F1sfbxeRkdcUCVg2eG1mpeHG1jChRU9S7hhP"
        )
        .unwrap());
        assert!(add_second_dip_target(
            &conn,
            "So11111111111111111111111111111111111111112"
        )
        .unwrap());
        let list = load_second_dip_targets(&conn).unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0], "AURARwy7F1sfbxeRkdcUCVg2eG1mpeHG1jChRU9S7hhP");
        assert!(remove_second_dip_target(
            &conn,
            "So11111111111111111111111111111111111111112"
        )
        .unwrap());
        // Hapus yang sudah tidak ada -> false.
        assert!(!remove_second_dip_target(
            &conn,
            "So11111111111111111111111111111111111111112"
        )
        .unwrap());
        assert_eq!(load_second_dip_targets(&conn).unwrap().len(), 1);
    }
}

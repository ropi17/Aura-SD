// Hapus Aura limit order berdasarkan mint + order ID.
// Dipakai oleh monitor FOTF: kalau harga <= threshold, hapus order beli.
// Usage: delete_orders <mint> <order_id_1,order_id_2,...>
// Env: AURA_API_KEY (dari .env di working directory).
// TIDAK pernah mencetak API key. Keluar 0 kalau gRPC sukses, 1 kalau gagal.
use std::env;
use std::str::FromStr;
use tonic::{Request, Status};
use tonic::transport::Endpoint;
use aura_api_client::client::AuraClients;
use aura_api_client::client_ext::UserCtxInterceptor;
use aura_api_client::types::{DeleteOrders, OrderId};
use solana_address::Address;

#[derive(Clone, Copy)]
struct NoCtx;

impl UserCtxInterceptor for NoCtx {
    type Payload = ();
    fn intercept<T>(_payload: (), _req: &mut tonic::Request<T>) -> Result<(), tonic::Status> {
        Ok(())
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
    dotenvy::dotenv().ok();
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: delete_orders <mint> <order_id_1,order_id_2,...>");
        std::process::exit(2);
    }
    let mint = match Address::from_str(&args[1]) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("Mint tidak valid: {}", e);
            std::process::exit(2);
        }
    };
    let mut ids: Vec<OrderId> = Vec::new();
    for part in args[2].split(',') {
        match part.trim().parse::<i64>() {
            Ok(n) => ids.push(OrderId(n)),
            Err(_) => {
                eprintln!("Order ID tidak valid: {}", part);
                std::process::exit(2);
            }
        }
    }
    if ids.is_empty() {
        eprintln!("Tidak ada order ID.");
        std::process::exit(2);
    }

    let api_key = match env::var("AURA_API_KEY") {
        Ok(k) if !k.is_empty() => k,
        _ => {
            eprintln!("AURA_API_KEY tidak diset.");
            std::process::exit(1);
        }
    };
    let _ = API_KEY.set(api_key);

    let endpoint = Endpoint::from_static("http://trade.aura.rehab:40051");
    let channel = match endpoint.connect().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Gagal konek gRPC Aura: {}", e);
            std::process::exit(1);
        }
    };
    let interceptor: fn(Request<()>) -> Result<Request<()>, Status> = auth_interceptor;
    let clients = AuraClients::<_, NoCtx>::new(channel, interceptor);
    let mut lo_client = clients.limit_orders();

    let req = Request::new(DeleteOrders {
        mint,
        all: false,
        ids: ids.clone(),
    });
    match lo_client.delete_limit_orders((), req).await {
        Ok(resp) => {
            let inner = resp.into_inner();
            println!(
                "OK delete orders {:?} mint {} total_orders_after={}",
                args[2], args[1], inner.total_orders_after
            );
        }
        Err(e) => {
            eprintln!("Gagal hapus order: {} ({})", e.message(), e.code());
            std::process::exit(1);
        }
    }
}

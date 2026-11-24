// List semua limit order aktif di Aura (buat verifikasi).
// Usage: list_orders
// Env: AURA_API_KEY (dari .env di working directory).
use std::env;
use tonic::{Request, Status};
use tonic::transport::Endpoint;
use aura_api_client::client::AuraClients;
use aura_api_client::client_ext::UserCtxInterceptor;
use aura_api_client::types::GetLimitOrders;

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
    match lo_client.get_limit_orders((), Request::new(GetLimitOrders)).await {
        Ok(resp) => {
            let orders = resp.into_inner().orders;
            println!("TOTAL_ORDERS {}", orders.len());
            for o in orders {
                println!("ORDER {:?}", o);
            }
        }
        Err(e) => {
            eprintln!("Gagal ambil order: {} ({})", e.message(), e.code());
            std::process::exit(1);
        }
    }
}

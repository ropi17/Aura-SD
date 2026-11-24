use aura_api_client::client::types::GetLimitOrders;
use aura_bot::setup_aura_client;

#[tokio::main]
async fn main() {
    let _ = dotenvy::dotenv();
    let api_key = std::env::var("AURA_API_KEY").expect("AURA_API_KEY not set");
    if let Some(client) = setup_aura_client(&api_key).await {
        println!("Client setup done. Getting limit orders...");
        let req = tonic::Request::new(GetLimitOrders);
        match client.limit_orders().get_limit_orders((), req).await {
            Ok(resp) => {
                let orders = resp.into_inner().orders;
                println!("Got {} orders", orders.len());
                for o in orders {
                    println!("Order state: {:?}", o.state);
                }
            }
            Err(e) => {
                println!("Error: {}", e.message());
            }
        }
    } else {
        println!("Failed to setup client");
    }
}

use dotenv::dotenv;
use relayer_core::config::{config_from_yaml, Config};
use relayer_core::heartbeat::heartbeats_loop;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    dotenv().ok();
    let network = std::env::var("NETWORK").expect("NETWORK must be set");
    let config: Config = config_from_yaml(&format!("config.{}.yaml", network))?;

    heartbeats_loop(&config).await;
}

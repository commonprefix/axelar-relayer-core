use dotenv::dotenv;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};

use relayer_core::config::{config_from_yaml, Config};
use relayer_core::logging::setup_logging;
use relayer_core::logging_ctx_cache::RedisLoggingCtxCache;
use relayer_core::redis::connection_manager;
use relayer_core::{
    database::PostgresDB,
    distributor::Distributor,
    gmp_api,
    queue::{Queue, QueueTrait},
    utils::setup_heartbeat,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    let network = std::env::var("NETWORK").expect("NETWORK must be set");
    let config: Config = config_from_yaml(&format!("config.{}.yaml", network))?;

    let (_sentry_guard, otel_guard) = setup_logging(&config);

    let includer_tasks_queue: Arc<dyn QueueTrait> =
        Queue::new(&config.queue_address, "includer_tasks", config.num_workers).await;
    let ingestor_tasks_queue: Arc<dyn QueueTrait> =
        Queue::new(&config.queue_address, "ingestor_tasks", config.num_workers).await;
    let postgres_db = PostgresDB::new(&config.postgres_url)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create PostgresDB: {}", e))?;

    let pg_pool = PgPool::connect(&config.postgres_url).await?;
    let gmp_api = gmp_api::construct_gmp_api(pg_pool, &config, true)?;
    let redis_client = redis::Client::open(config.redis_server.clone())?;
    let redis_conn = connection_manager(redis_client, None, None, None, None).await?;
    let logging_ctx_cache = RedisLoggingCtxCache::new(redis_conn.clone());

    let mut distributor = Distributor::new(
        postgres_db,
        "default".to_string(),
        gmp_api,
        config.refunds_enabled,
        Arc::new(logging_ctx_cache),
    )
    .await;

    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;

    setup_heartbeat("heartbeat:distributor".to_owned(), redis_conn, None);

    tokio::select! {
        _ = sigint.recv()  => {},
        _ = sigterm.recv() => {},
        _ = distributor.run(
            Arc::clone(&includer_tasks_queue),
            Arc::clone(&ingestor_tasks_queue),
        ) => {},
    }

    ingestor_tasks_queue.close().await;
    includer_tasks_queue.close().await;

    otel_guard
        .force_flush()
        .expect("Failed to flush OTEL messages");

    Ok(())
}

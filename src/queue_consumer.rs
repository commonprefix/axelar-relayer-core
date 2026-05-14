use crate::queue::QueueTrait;
use async_std::stream::StreamExt;
use async_trait::async_trait;
use lapin::message::Delivery;
use lapin::Consumer;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

const CONSUMER_RECREATE_DELAYS_SECS: [u64; 3] = [1, 3, 5];

#[async_trait]
pub trait QueueConsumer {
    async fn on_delivery(
        &self,
        delivery: Delivery,
        queue: Arc<dyn QueueTrait>,
        tracker: &TaskTracker,
    );

    async fn work(
        &self,
        consumer: &mut Consumer,
        queue: Arc<dyn QueueTrait>,
        token: CancellationToken,
    ) {
        let tracker = TaskTracker::new();

        'outer: loop {
            debug!("Task tracker size: {}", tracker.len());
            debug!("Waiting for messages from {}", consumer.queue());
            select! {
                _ = token.cancelled() => {
                    info!("Cancellation requested; no longer awaiting consumer.next()");
                    break;
                }
                maybe_msg = consumer.next() => {
                    match maybe_msg {
                        Some(Ok(delivery)) => {
                            self.on_delivery(delivery, Arc::clone(&queue), &tracker).await;
                        }
                        Some(Err(e)) => {
                            error!("Failed to receive delivery: {:?}. Attempting to recreate consumer.", e);
                            if !recreate_consumer(consumer, &queue, &token).await {
                                break 'outer;
                            }
                        }
                        None => {
                            warn!("Consumer stream ended. Attempting to recreate consumer.");
                            if !recreate_consumer(consumer, &queue, &token).await {
                                break 'outer;
                            }
                        }
                    }
                }
            }
        }

        info!("Task tracker size: {}", tracker.len());
        tracker.close();
        tracker.wait().await;
    }
}

/// Attempts to recreate the consumer with bounded backoff. Returns `true` if a
/// new consumer was installed in `*consumer`, `false` if every attempt failed
/// or the token was cancelled mid-retry (in which case the caller should exit
/// the work loop).
async fn recreate_consumer(
    consumer: &mut Consumer,
    queue: &Arc<dyn QueueTrait>,
    token: &CancellationToken,
) -> bool {
    for (idx, delay_secs) in CONSUMER_RECREATE_DELAYS_SECS.iter().enumerate() {
        let attempt = idx + 1;
        let max = CONSUMER_RECREATE_DELAYS_SECS.len();

        select! {
            _ = token.cancelled() => {
                info!("Cancellation during consumer recreation; aborting retries.");
                return false;
            }
            _ = tokio::time::sleep(Duration::from_secs(*delay_secs)) => {}
        }

        match queue.consumer().await {
            Ok(new_consumer) => {
                info!(
                    "Consumer recreated successfully on attempt {}/{}",
                    attempt, max
                );
                *consumer = new_consumer;
                return true;
            }
            Err(e) => {
                warn!(
                    "Consumer recreation attempt {}/{} failed: {:?}",
                    attempt, max, e
                );
            }
        }
    }

    error!(
        "Failed to recreate consumer after {} attempts; exiting work loop",
        CONSUMER_RECREATE_DELAYS_SECS.len()
    );
    false
}

use async_trait::async_trait;
use lapin::message::Delivery;
use lapin::options::BasicAckOptions;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, info_span, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::gmp_api::GmpApiTrait;
use crate::includer_worker::{IncluderTrait, IncluderWorker, IncluderWorkerTrait};
use crate::logging::distributed_tracing_extract_parent_context;
use crate::queue_consumer::QueueConsumer;
use crate::utils::ThreadSafe;
use crate::{
    database::Database,
    error::{IncluderError, RefundManagerError},
    gmp_api::gmp_types::RefundTask,
    queue::{QueueItem, QueueTrait},
};

#[async_trait]
pub trait RefundManager
where
    Self::Wallet: Send,
{
    type Wallet;
    fn is_refund_manager_managed(&self) -> bool;

    async fn build_refund_tx(
        &self,
        recipient: String,
        amount: String,
        refund_id: &str,
        wallet: &Self::Wallet,
    ) -> Result<Option<(String, String, String)>, RefundManagerError>;
    async fn is_refund_processed(
        &self,
        refund_task: &RefundTask,
        refund_id: &str,
    ) -> Result<bool, RefundManagerError>;
    async fn get_wallet_lock(&self) -> Result<Self::Wallet, RefundManagerError>;
    async fn release_wallet_lock(&self, wallet: Self::Wallet) -> Result<(), RefundManagerError>;
}

pub struct Includer<C, R, DB, G, I>
where
    C: ThreadSafe + Clone,
    R: RefundManager + ThreadSafe + Clone,
    DB: Database + ThreadSafe + Clone,
    G: GmpApiTrait + ThreadSafe + Clone,
    I: IncluderTrait + ThreadSafe + Clone,
{
    worker: IncluderWorker<C, R, DB, G, I>,
}

#[async_trait]
impl<C, R, DB, G, I> QueueConsumer for Includer<C, R, DB, G, I>
where
    C: ThreadSafe + Clone,
    R: RefundManager + ThreadSafe + Clone,
    DB: Database + ThreadSafe + Clone,
    G: GmpApiTrait + ThreadSafe + Clone,
    I: IncluderTrait + ThreadSafe + Clone,
{
    async fn on_delivery(
        &self,
        delivery: Delivery,
        queue: Arc<dyn QueueTrait>,
        tracker: &TaskTracker,
    ) {
        let worker = self.worker.clone();
        let queue_clone = Arc::clone(&queue);
        tracker.spawn(async move {
            debug!("Spawned new includer task");
            let parent_cx = distributed_tracing_extract_parent_context(&delivery);
            let span = info_span!("consume_queue_task");
            span.set_parent(parent_cx);

            let data = delivery.data.clone();
            if let Err(e) = worker
                .process_delivery(&data)
                .instrument(span.clone())
                .await
            {
                let mut force_requeue = false;
                match e {
                    IncluderError::IrrelevantTask => {
                        debug!("Skipping irrelevant task");
                        force_requeue = true;
                    }
                    _ => {
                        error!("Failed to consume delivery: {:?}", e);
                    }
                }

                if let Err(nack_err) = queue_clone
                    .republish(delivery, force_requeue)
                    .instrument(span.clone())
                    .await
                {
                    error!("Failed to republish message: {:?}", nack_err);
                }
            } else if let Err(ack_err) = delivery
                .ack(BasicAckOptions::default())
                .instrument(span.clone())
                .await
            {
                let item = serde_json::from_slice::<QueueItem>(&delivery.data);
                error!("Failed to ack item {:?}: {:?}", item, ack_err);
            }
            debug!("Includer task finished");
        });
    }
}

impl<C, R, DB, G, I> Includer<C, R, DB, G, I>
where
    C: ThreadSafe + Clone,
    R: RefundManager + ThreadSafe + Clone,
    DB: Database + ThreadSafe + Clone,
    G: GmpApiTrait + ThreadSafe + Clone,
    I: IncluderTrait + ThreadSafe + Clone,
{
    pub fn new(worker: IncluderWorker<C, R, DB, G, I>) -> Self {
        Self { worker }
    }
    pub async fn run(&self, queue: Arc<dyn QueueTrait>, token: CancellationToken) {
        if let Ok(mut consumer) = queue.consumer().await {
            info!("Includer is alive.");
            self.work(&mut consumer, Arc::clone(&queue), token.clone())
                .await;
            info!("Includer is done.");
        } else {
            error!("Failed to create consumer");
        }
    }
}

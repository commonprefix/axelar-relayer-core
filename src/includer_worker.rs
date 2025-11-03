use crate::database::Database;
use crate::error::IncluderError;
use crate::gmp_api::gmp_types::{Event, ExecuteTask, GatewayTxTask, RefundTask, Task};
use crate::gmp_api::GmpApiTrait;
use crate::includer::RefundManager;
use crate::payload_cache::PayloadCache;
use crate::queue::Queue;
use crate::queue::QueueItem;
use crate::utils::ThreadSafe;
use async_trait::async_trait;
use redis::aio::ConnectionManager;
use std::sync::Arc;
use tracing::info;

#[derive(Clone)]
pub struct IncluderWorker<C, R, DB, G, I>
where
    C: ThreadSafe + Clone,
    R: RefundManager + ThreadSafe + Clone,
    DB: Database + ThreadSafe + Clone,
    G: GmpApiTrait + ThreadSafe + Clone,
    I: IncluderTrait + ThreadSafe + Clone,
{
    pub chain_client: C,
    pub refund_manager: R,
    pub gmp_api: Arc<G>,
    pub payload_cache: PayloadCache<DB>,
    pub construct_proof_queue: Arc<Queue>,
    pub redis_conn: ConnectionManager,
    pub includer: I,
}

#[async_trait]
#[cfg_attr(any(test), mockall::automock)]
pub trait IncluderWorkerTrait {
    async fn process_delivery(&self, data: &[u8]) -> Result<(), IncluderError>;
}

#[async_trait]
trait IncluderWorkerPrivateTrait {
    async fn consume(&self, item: QueueItem) -> Result<(), IncluderError>;
}

impl<C, R, DB, G, I> IncluderWorker<C, R, DB, G, I>
where
    C: ThreadSafe + Clone,
    R: RefundManager + ThreadSafe + Clone,
    DB: Database + ThreadSafe + Clone,
    G: GmpApiTrait + ThreadSafe + Clone,
    I: IncluderTrait + ThreadSafe + Clone,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chain_client: C,
        refund_manager: R,
        gmp_api: Arc<G>,
        payload_cache: PayloadCache<DB>,
        construct_proof_queue: Arc<Queue>,
        redis_conn: ConnectionManager,
        includer: I,
    ) -> Self {
        Self {
            chain_client,
            refund_manager,
            gmp_api,
            payload_cache,
            construct_proof_queue,
            redis_conn,
            includer,
        }
    }
}

#[async_trait]
pub trait IncluderTrait {
    async fn handle_execute_task(&self, task: ExecuteTask) -> Result<Vec<Event>, IncluderError>;
    async fn handle_gateway_tx_task(
        &self,
        task: GatewayTxTask,
    ) -> Result<Vec<Event>, IncluderError>;
    async fn handle_refund_task(&self, task: RefundTask) -> Result<(), IncluderError>;
}

#[async_trait]
impl<C, R, DB, G, I> IncluderWorkerPrivateTrait for IncluderWorker<C, R, DB, G, I>
where
    C: ThreadSafe + Clone,
    R: RefundManager + ThreadSafe + Clone,
    DB: Database + ThreadSafe + Clone,
    G: GmpApiTrait + ThreadSafe + Clone,
    I: IncluderTrait + ThreadSafe + Clone,
{
    #[tracing::instrument(skip(self))]
    async fn consume(&self, task: QueueItem) -> Result<(), IncluderError> {
        match task {
            QueueItem::Task(task) => match *task {
                Task::Execute(execute_task) => {
                    info!("Consuming execute task: {:?}", execute_task);
                    let events = self.includer.handle_execute_task(execute_task).await?;
                    self.gmp_api
                        .post_events(events)
                        .await
                        .map_err(|e| IncluderError::ConsumerError(e.to_string()))?;

                    Ok(())
                }
                Task::GatewayTx(gateway_tx_task) => {
                    info!("Consuming task: {:?}", gateway_tx_task);
                    let events = self
                        .includer
                        .handle_gateway_tx_task(gateway_tx_task.clone())
                        .await
                        .map_err(|e| IncluderError::GatewayTxTaskError(e.to_string()))?;

                    self.gmp_api
                        .post_events(events)
                        .await
                        .map_err(|e| IncluderError::ConsumerError(e.to_string()))?;

                    Ok(())
                }
                Task::Refund(refund_task) => {
                    info!("Consuming task: {:?}", refund_task);
                    self.includer
                        .handle_refund_task(refund_task)
                        .await
                        .map_err(|e| IncluderError::RefundTaskError(e.to_string()))?;

                    Ok(())
                }
                _ => Err(IncluderError::IrrelevantTask),
            },
            _ => Err(IncluderError::GenericError(
                "Invalid queue item".to_string(),
            )),
        }
    }
}

#[async_trait]
impl<C, R, DB, G, I> IncluderWorkerTrait for IncluderWorker<C, R, DB, G, I>
where
    C: ThreadSafe + Clone,
    R: RefundManager + ThreadSafe + Clone,
    DB: Database + ThreadSafe + Clone,
    G: GmpApiTrait + ThreadSafe + Clone,
    I: IncluderTrait + ThreadSafe + Clone,
{
    #[tracing::instrument(skip(self))]
    async fn process_delivery(&self, data: &[u8]) -> Result<(), IncluderError> {
        let item = serde_json::from_slice::<QueueItem>(data)
            .map_err(|e| IncluderError::GenericError(format!("Invalid JSON: {}", e)))?;

        self.consume(item).await
    }
}

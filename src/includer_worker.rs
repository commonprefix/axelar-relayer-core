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
                    // if !self.refund_manager.is_refund_manager_managed() {
                    //     return match self
                    //         .broadcaster
                    //         .broadcast_refund_message(refund_task.task)
                    //         .await
                    //     {
                    //         Ok(_) => Ok(()),
                    //         Err(BroadcasterError::InsufficientGas(_)) => Ok(()),
                    //         Err(e) => Err(IncluderError::GenericError(e.to_string())),
                    //     };
                    // }

                    // if self
                    //     .refund_manager
                    //     .is_refund_processed(&refund_task, &refund_task.common.id)
                    //     .await
                    //     .map_err(|e| IncluderError::ConsumerError(e.to_string()))?
                    // {
                    //     warn!("Refund already processed");
                    //     return Ok(());
                    // }

                    // if refund_task.task.remaining_gas_balance.token_id.is_some() {
                    //     return Err(IncluderError::GenericError(
                    //         "Refund task with token_id is not supported".to_string(),
                    //     ));
                    // }

                    // let wallet = self
                    //     .refund_manager
                    //     .get_wallet_lock()
                    //     .await
                    //     .map_err(|e| IncluderError::ConsumerError(e.to_string()))?;

                    // let refund_info = self
                    //     .refund_manager
                    //     .build_refund_tx(
                    //         refund_task.task.refund_recipient_address.clone(),
                    //         refund_task.task.remaining_gas_balance.amount.clone(),
                    //         &refund_task.common.id,
                    //         &wallet,
                    //     )
                    //     .await
                    //     .map_err(|e| IncluderError::ConsumerError(e.to_string()))?;

                    // if let Some((tx_blob, refunded_amount, fee)) = refund_info {
                    //     let tx_hash = match self
                    //         .broadcaster
                    //         .broadcast_refund(tx_blob)
                    //         .await
                    //         .map_err(|e| IncluderError::ConsumerError(e.to_string()))
                    //     {
                    //         Ok(hash) => hash, // bind the successful tx_hash here…
                    //         Err(err) => {
                    //             // …or on error, release the lock and return
                    //             self.refund_manager
                    //                 .release_wallet_lock(wallet)
                    //                 .await
                    //                 .map_err(|e| IncluderError::ConsumerError(e.to_string()))?;
                    //             return Err(err);
                    //         }
                    //     };

                    //     let gas_refunded = Event::GasRefunded {
                    //         common: CommonEventFields {
                    //             r#type: "GAS_REFUNDED".to_owned(),
                    //             event_id: tx_hash.clone(),
                    //             meta: None,
                    //         },
                    //         message_id: refund_task.task.message.message_id,
                    //         recipient_address: refund_task.task.refund_recipient_address,
                    //         refunded_amount: Amount {
                    //             token_id: None,
                    //             amount: refunded_amount,
                    //         },
                    //         cost: Amount {
                    //             token_id: None,
                    //             amount: fee,
                    //         },
                    //     };

                    //     let gas_refunded_post = self.gmp_api.post_events(vec![gas_refunded]).await;
                    //     if let Err(e) = gas_refunded_post {
                    //         // TODO: should retry somehow
                    //         warn!("Failed to post event: {:?}", e);
                    //     }
                    // } else {
                    //     warn!("Refund not executed: refund amount is not enough to cover tx fees");
                    // }
                    // self.refund_manager
                    //     .release_wallet_lock(wallet)
                    //     .await
                    //     .map_err(|e| IncluderError::ConsumerError(e.to_string()))?;
                    // Ok(())
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

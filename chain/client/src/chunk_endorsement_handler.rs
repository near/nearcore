use std::sync::Arc;

use near_async::messaging::Handler;
use near_async::tokio::TokioRuntimeHandle;
use near_async::{ActorSystem, messaging};
use near_network::client::ChunkEndorsementMessage;
use near_performance_metrics_macros::perf;

use crate::stateless_validation::chunk_endorsement::ChunkEndorsementTracker;

impl Handler<ChunkEndorsementMessage> for ChunkEndorsementHandler {
    #[perf]
    fn handle(&mut self, msg: ChunkEndorsementMessage) {
        if let Err(err) = self.chunk_endorsement_tracker.process_chunk_endorsement(msg.0) {
            tracing::error!(target: "client", ?err, "Error processing chunk endorsement");
        }
    }
}

impl messaging::Actor for ChunkEndorsementHandler {}

pub fn spawn_chunk_endorsement_handler_actor(
    actor_system: ActorSystem,
    chunk_endorsement_tracker: Arc<ChunkEndorsementTracker>,
) -> TokioRuntimeHandle<ChunkEndorsementHandler> {
    actor_system.spawn_tokio_actor(ChunkEndorsementHandler::new(chunk_endorsement_tracker.clone()))
}

#[derive(Clone)]
pub struct ChunkEndorsementHandler {
    chunk_endorsement_tracker: Arc<ChunkEndorsementTracker>,
}

impl ChunkEndorsementHandler {
    pub fn new(chunk_endorsement_tracker: Arc<ChunkEndorsementTracker>) -> Self {
        Self { chunk_endorsement_tracker }
    }
}

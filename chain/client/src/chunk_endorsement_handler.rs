use std::sync::Arc;

use near_async::messaging::Handler;
use near_async::multithread::MultithreadRuntimeHandle;
use near_async::{ActorSystem, messaging};
use near_network::client::ChunkEndorsementMessage;
use near_performance_metrics_macros::perf;

use crate::stateless_validation::chunk_endorsement::ChunkEndorsementTracker;

impl Handler<ChunkEndorsementMessage> for ChunkEndorsementHandlerActor {
    #[perf]
    fn handle(&mut self, msg: ChunkEndorsementMessage) {
        if let Err(err) = self.chunk_endorsement_tracker.process_chunk_endorsement(msg.0) {
            tracing::error!(target: "client", ?err, "error processing chunk endorsement");
        }
    }
}

impl messaging::Actor for ChunkEndorsementHandlerActor {}

pub fn spawn_chunk_endorsement_handler_actor(
    actor_system: ActorSystem,
    chunk_endorsement_tracker: Arc<ChunkEndorsementTracker>,
) -> MultithreadRuntimeHandle<ChunkEndorsementHandlerActor> {
    let actor = ChunkEndorsementHandlerActor::new(chunk_endorsement_tracker);
    actor_system.spawn_multithread_actor(4, move || actor.clone())
}

#[derive(Clone)]
pub struct ChunkEndorsementHandlerActor {
    chunk_endorsement_tracker: Arc<ChunkEndorsementTracker>,
}

impl ChunkEndorsementHandlerActor {
    pub fn new(chunk_endorsement_tracker: Arc<ChunkEndorsementTracker>) -> Self {
        Self { chunk_endorsement_tracker }
    }
}

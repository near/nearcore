use crate::stateless_validation::chunk_endorsement::ChunkEndorsementTracker;
use near_async::messaging::Handler;
use near_async::multithread::MultithreadRuntimeHandle;
use near_async::{ActorSystem, messaging};
use near_network::client::ChunkEndorsementMessage;
use std::sync::Arc;

impl Handler<ChunkEndorsementMessage> for ChunkEndorsementHandlerActor {
    fn handle(&mut self, msg: ChunkEndorsementMessage) {
        let endorsement = msg.0;
        if let Err(err) = self.chunk_endorsement_tracker.process_chunk_endorsement(&endorsement) {
            tracing::error!(
                target: "client",
                ?err,
                account_id = %endorsement.account_id(),
                key = ?endorsement.chunk_production_key(),
                "error processing chunk endorsement",
            );
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

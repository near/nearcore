use std::sync::Arc;

use actix::Actor;
use near_async::messaging::Sender;
use near_async::time::Clock;
use near_async::{MultiSend, MultiSendMessage, MultiSenderFrom};
use near_epoch_manager::EpochManagerAdapter;
use near_network::state_witness::ChunkStateWitnessAckMessage;
use near_network::types::PeerManagerAdapter;
use near_o11y::{handler_debug_span, WithSpanContext};
use near_performance_metrics_macros::perf;
use near_primitives::stateless_validation::ChunkStateWitness;
use near_primitives::validator_signer::ValidatorSigner;

use super::state_witness_actions::StateWitnessActions;

pub struct StateWitnessActor {
    pub actions: StateWitnessActions,
}

impl StateWitnessActor {
    pub fn spawn(
        clock: Clock,
        network_adapter: PeerManagerAdapter,
        my_signer: Arc<dyn ValidatorSigner>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
    ) -> (actix::Addr<Self>, actix::ArbiterHandle) {
        let arbiter = actix::Arbiter::new().handle();
        let addr = Self::start_in_arbiter(&arbiter, |_ctx| Self {
            actions: StateWitnessActions::new(clock, network_adapter, my_signer, epoch_manager),
        });
        (addr, arbiter)
    }
}

impl actix::Actor for StateWitnessActor {
    type Context = actix::Context<Self>;
}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct DistributeStateWitnessRequest {
    pub state_witness: ChunkStateWitness,
}

#[derive(Clone, MultiSend, MultiSenderFrom, MultiSendMessage)]
#[multi_send_message_derive(Debug)]
pub struct StateWitnessSenderForClient {
    pub distribute_chunk_state_witness: Sender<DistributeStateWitnessRequest>,
}

impl actix::Handler<WithSpanContext<DistributeStateWitnessRequest>> for StateWitnessActor {
    type Result = ();

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<DistributeStateWitnessRequest>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "stateless_validation", msg);
        if let Err(err) = self.actions.handle_distribute_state_witness_request(msg) {
            tracing::error!(target: "stateless_validation", ?err, "Failed to handle distribute chunk state witness request");
        }
    }
}

impl actix::Handler<WithSpanContext<ChunkStateWitnessAckMessage>> for StateWitnessActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: WithSpanContext<ChunkStateWitnessAckMessage>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "stateless_validation", msg);
        self.actions.handle_chunk_state_witness_ack(msg.0);
    }
}

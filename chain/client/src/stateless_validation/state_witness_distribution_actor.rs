use std::sync::Arc;

use actix::Actor;
use near_async::messaging::Sender;
use near_async::time::Clock;
use near_async::{MultiSend, MultiSendMessage, MultiSenderFrom};
use near_network::state_witness_distribution::ChunkStateWitnessAckMessage;
use near_network::types::PeerManagerAdapter;
use near_o11y::{handler_debug_span, WithSpanContext};
use near_performance_metrics_macros::perf;
use near_primitives::stateless_validation::ChunkStateWitness;
use near_primitives::types::AccountId;
use near_primitives::validator_signer::ValidatorSigner;

use super::state_witness_distribution_actions::StateWitnessDistributionActions;

pub struct StateWitnessDistributionActor {
    pub actions: StateWitnessDistributionActions,
}

impl StateWitnessDistributionActor {
    pub fn spawn(
        clock: Clock,
        network_adapter: PeerManagerAdapter,
        my_signer: Arc<dyn ValidatorSigner>,
    ) -> (actix::Addr<Self>, actix::ArbiterHandle) {
        let arbiter = actix::Arbiter::new().handle();
        let addr = Self::start_in_arbiter(&arbiter, |_ctx| Self {
            actions: StateWitnessDistributionActions::new(clock, network_adapter, my_signer),
        });
        (addr, arbiter)
    }
}

impl actix::Actor for StateWitnessDistributionActor {
    type Context = actix::Context<Self>;
}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct DistributeChunkStateWitnessRequest {
    pub chunk_validators: Vec<AccountId>,
    pub state_witness: ChunkStateWitness,
}

#[derive(Clone, MultiSend, MultiSenderFrom, MultiSendMessage)]
#[multi_send_message_derive(Debug)]
pub struct StateWitnessDistributionSenderForClient {
    pub distribute_chunk_state_witness: Sender<DistributeChunkStateWitnessRequest>,
}

impl actix::Handler<WithSpanContext<DistributeChunkStateWitnessRequest>>
    for StateWitnessDistributionActor
{
    type Result = ();

    #[perf]
    fn handle(
        &mut self,
        msg: WithSpanContext<DistributeChunkStateWitnessRequest>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "stateless_validation", msg);
        if let Err(err) = self.actions.handle_distribute_chunk_state_witness_request(msg) {
            tracing::error!(target: "stateless_validation", ?err, "Failed to handle distribute chunk state witness request");
        }
    }
}

impl actix::Handler<WithSpanContext<ChunkStateWitnessAckMessage>>
    for StateWitnessDistributionActor
{
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

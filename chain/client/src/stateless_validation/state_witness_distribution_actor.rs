use actix::Actor;
use near_network::types::PeerManagerAdapter;
use near_o11y::{handler_debug_span, WithSpanContext};
use near_performance_metrics_macros::perf;
use near_primitives::stateless_validation::SignedEncodedChunkStateWitness;
use near_primitives::types::AccountId;

use super::state_witness_distribution_actions::StateWitnessDistributionActions;

pub struct StateWitnessDistributionActor {
    pub actions: StateWitnessDistributionActions,
}

impl StateWitnessDistributionActor {
    pub fn spawn(network_adapter: PeerManagerAdapter) -> (actix::Addr<Self>, actix::ArbiterHandle) {
        let arbiter = actix::Arbiter::new().handle();
        let addr = Self::start_in_arbiter(&arbiter, |_ctx| Self {
            actions: StateWitnessDistributionActions::new(network_adapter),
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
    pub signed_witness: SignedEncodedChunkStateWitness,
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
        let (_span, msg) = handler_debug_span!(target: "client", msg);
        if let Err(err) = self.actions.handle_distribute_chunk_state_witness_request(msg) {
            tracing::error!(target: "stateless_validation", ?err, "Failed to handle distribute chunk state witness request");
        }
    }
}

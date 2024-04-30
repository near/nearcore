use std::sync::Arc;

use actix::Actor;
use near_async::messaging::Sender;
use near_async::time::Clock;
use near_async::{MultiSend, MultiSendMessage, MultiSenderFrom};
use near_epoch_manager::EpochManagerAdapter;
use near_network::state_witness::{
    ChunkStateWitnessAckMessage, PartialEncodedStateWitnessForwardMessage,
    PartialEncodedStateWitnessMessage,
};
use near_network::types::PeerManagerAdapter;
use near_o11y::{handler_debug_span, WithSpanContext};
use near_performance_metrics_macros::perf;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::stateless_validation::ChunkStateWitness;
use near_primitives::types::EpochId;
use near_primitives::validator_signer::ValidatorSigner;

use crate::client_actions::ClientSenderForPartialWitness;

use super::partial_witness_actions::PartialWitnessActions;

pub struct PartialWitnessActor {
    pub actions: PartialWitnessActions,
}

impl PartialWitnessActor {
    pub fn spawn(
        clock: Clock,
        network_adapter: PeerManagerAdapter,
        client_sender: ClientSenderForPartialWitness,
        my_signer: Arc<dyn ValidatorSigner>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
    ) -> (actix::Addr<Self>, actix::ArbiterHandle) {
        let arbiter = actix::Arbiter::new().handle();
        let addr = Self::start_in_arbiter(&arbiter, |_ctx| Self {
            actions: PartialWitnessActions::new(
                clock,
                network_adapter,
                client_sender,
                my_signer,
                epoch_manager,
            ),
        });
        (addr, arbiter)
    }
}

impl actix::Actor for PartialWitnessActor {
    type Context = actix::Context<Self>;
}

#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct DistributeStateWitnessRequest {
    pub epoch_id: EpochId,
    pub chunk_header: ShardChunkHeader,
    pub state_witness: ChunkStateWitness,
}

#[derive(Clone, MultiSend, MultiSenderFrom, MultiSendMessage)]
#[multi_send_message_derive(Debug)]
pub struct PartialWitnessSenderForClient {
    pub distribute_chunk_state_witness: Sender<DistributeStateWitnessRequest>,
}

impl actix::Handler<WithSpanContext<DistributeStateWitnessRequest>> for PartialWitnessActor {
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

impl actix::Handler<WithSpanContext<ChunkStateWitnessAckMessage>> for PartialWitnessActor {
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

impl actix::Handler<WithSpanContext<PartialEncodedStateWitnessMessage>> for PartialWitnessActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: WithSpanContext<PartialEncodedStateWitnessMessage>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "stateless_validation", msg);
        if let Err(err) = self.actions.handle_partial_encoded_state_witness(msg.0) {
            tracing::error!(target: "stateless_validation", ?err, "Failed to handle PartialEncodedStateWitnessMessage");
        }
    }
}

impl actix::Handler<WithSpanContext<PartialEncodedStateWitnessForwardMessage>>
    for PartialWitnessActor
{
    type Result = ();

    fn handle(
        &mut self,
        msg: WithSpanContext<PartialEncodedStateWitnessForwardMessage>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "stateless_validation", msg);
        if let Err(err) = self.actions.handle_partial_encoded_state_witness_forward(msg.0) {
            tracing::error!(target: "stateless_validation", ?err, "Failed to handle PartialEncodedStateWitnessForwardMessage");
        }
    }
}

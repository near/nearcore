use crate::{PartialWitnessActor, PartialWitnessSenderForClientMessage};
use near_async::messaging::Handler;
use near_async::test_loop::event_handler::LoopEventHandler;
use near_network::state_witness::PartialWitnessSenderForNetworkMessage;

pub fn forward_messages_from_network_to_partial_witness_actor(
) -> LoopEventHandler<PartialWitnessActor, PartialWitnessSenderForNetworkMessage> {
    LoopEventHandler::new_simple(|msg, partial_witness_actor: &mut PartialWitnessActor| match msg {
        PartialWitnessSenderForNetworkMessage::_chunk_state_witness_ack(msg) => {
            partial_witness_actor.handle(msg);
        }
        PartialWitnessSenderForNetworkMessage::_partial_encoded_state_witness(msg) => {
            partial_witness_actor.handle(msg);
        }
        PartialWitnessSenderForNetworkMessage::_partial_encoded_state_witness_forward(msg) => {
            partial_witness_actor.handle(msg);
        }
    })
}

pub fn forward_messages_from_client_to_partial_witness_actor(
) -> LoopEventHandler<PartialWitnessActor, PartialWitnessSenderForClientMessage> {
    LoopEventHandler::new_simple(|msg, partial_witness_actor: &mut PartialWitnessActor| match msg {
        PartialWitnessSenderForClientMessage::_distribute_chunk_state_witness(msg) => {
            partial_witness_actor.handle(msg);
        }
        PartialWitnessSenderForClientMessage::_update_chain_head(msg) => {
            partial_witness_actor.handle(msg);
        }
    })
}

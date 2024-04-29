use crate::{StateWitnessActions, StateWitnessSenderForClientMessage};
use near_async::test_loop::event_handler::LoopEventHandler;
use near_network::state_witness::StateWitnessSenderForNetworkMessage;

pub fn forward_messages_from_network_to_state_witness_actor(
) -> LoopEventHandler<StateWitnessActions, StateWitnessSenderForNetworkMessage> {
    LoopEventHandler::new_simple(|msg, state_witness_actions: &mut StateWitnessActions| match msg {
        StateWitnessSenderForNetworkMessage::_chunk_state_witness_ack(msg) => {
            state_witness_actions.handle_chunk_state_witness_ack(msg.0);
        }
        StateWitnessSenderForNetworkMessage::_partial_encoded_state_witness(msg) => {
            state_witness_actions.handle_partial_encoded_state_witness(msg.0).unwrap();
        }
        StateWitnessSenderForNetworkMessage::_partial_encoded_state_witness_forward(msg) => {
            state_witness_actions.handle_partial_encoded_state_witness_forward(msg.0).unwrap();
        }
    })
}

pub fn forward_messages_from_client_to_state_witness_actor(
) -> LoopEventHandler<StateWitnessActions, StateWitnessSenderForClientMessage> {
    LoopEventHandler::new_simple(|msg, state_witness_actions: &mut StateWitnessActions| match msg {
        StateWitnessSenderForClientMessage::_distribute_chunk_state_witness(msg) => {
            state_witness_actions.handle_distribute_state_witness_request(msg).unwrap();
        }
    })
}

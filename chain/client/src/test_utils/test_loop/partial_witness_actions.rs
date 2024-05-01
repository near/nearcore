use crate::{PartialWitnessActions, PartialWitnessSenderForClientMessage};
use near_async::test_loop::event_handler::LoopEventHandler;
use near_network::state_witness::PartialWitnessSenderForNetworkMessage;

pub fn forward_messages_from_network_to_partial_witness_actor(
) -> LoopEventHandler<PartialWitnessActions, PartialWitnessSenderForNetworkMessage> {
    LoopEventHandler::new_simple(|msg, partial_witness_actions: &mut PartialWitnessActions| {
        match msg {
            PartialWitnessSenderForNetworkMessage::_chunk_state_witness_ack(msg) => {
                partial_witness_actions.handle_chunk_state_witness_ack(msg.0);
            }
            PartialWitnessSenderForNetworkMessage::_partial_encoded_state_witness(msg) => {
                partial_witness_actions.handle_partial_encoded_state_witness(msg.0).unwrap();
            }
            PartialWitnessSenderForNetworkMessage::_partial_encoded_state_witness_forward(msg) => {
                partial_witness_actions
                    .handle_partial_encoded_state_witness_forward(msg.0)
                    .unwrap();
            }
        }
    })
}

pub fn forward_messages_from_client_to_partial_witness_actor(
) -> LoopEventHandler<PartialWitnessActions, PartialWitnessSenderForClientMessage> {
    LoopEventHandler::new_simple(|msg, state_partial_actions: &mut PartialWitnessActions| match msg
    {
        PartialWitnessSenderForClientMessage::_distribute_chunk_state_witness(msg) => {
            state_partial_actions.handle_distribute_state_witness_request(msg).unwrap();
        }
    })
}

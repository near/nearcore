use near_async::messaging::Sender;
use near_async::{MultiSend, MultiSendMessage, MultiSenderFrom};
use near_primitives::stateless_validation::ChunkStateWitnessAck;

#[derive(actix::Message, Clone, Debug, PartialEq, Eq)]
#[rtype(result = "()")]
pub struct ChunkStateWitnessAckMessage(pub ChunkStateWitnessAck);

#[derive(Clone, MultiSend, MultiSenderFrom, MultiSendMessage)]
#[multi_send_message_derive(Debug)]
#[multi_send_input_derive(Debug, Clone, PartialEq, Eq)]
pub struct StateWitnessSenderForNetwork {
    pub chunk_state_witness_ack: Sender<ChunkStateWitnessAckMessage>,
}

use near_async::messaging::Sender;
use near_async::{MultiSend, MultiSendMessage, MultiSenderFrom};
use near_primitives::stateless_validation::{ChunkStateWitnessAck, PartialEncodedStateWitness};

#[derive(actix::Message, Clone, Debug, PartialEq, Eq)]
#[rtype(result = "()")]
pub struct ChunkStateWitnessAckMessage(pub ChunkStateWitnessAck);

#[derive(actix::Message, Clone, Debug, PartialEq, Eq)]
#[rtype(result = "()")]
pub struct PartialEncodedStateWitnessMessage(pub PartialEncodedStateWitness);

#[derive(actix::Message, Clone, Debug, PartialEq, Eq)]
#[rtype(result = "()")]
pub struct PartialEncodedStateWitnessForwardMessage(pub PartialEncodedStateWitness);

#[derive(Clone, MultiSend, MultiSenderFrom, MultiSendMessage)]
#[multi_send_message_derive(Debug)]
#[multi_send_input_derive(Debug, Clone, PartialEq, Eq)]
pub struct PartialWitnessSenderForNetwork {
    pub chunk_state_witness_ack: Sender<ChunkStateWitnessAckMessage>,
    pub partial_encoded_state_witness: Sender<PartialEncodedStateWitnessMessage>,
    pub partial_encoded_state_witness_forward: Sender<PartialEncodedStateWitnessForwardMessage>,
}

use near_async::messaging::Sender;
use near_async::{Message, MultiSend, MultiSendMessage, MultiSenderFrom};
use near_primitives::spice_partial_data::SpicePartialData;

#[derive(Message, Debug, Clone, PartialEq, Eq)]
pub struct SpiceIncomingPartialData {
    pub data: SpicePartialData,
}

#[derive(Clone, MultiSend, MultiSenderFrom, MultiSendMessage)]
#[multi_send_message_derive(Debug)]
#[multi_send_input_derive(Debug, Clone, PartialEq, Eq)]
pub struct SpiceDataDistributorSenderForNetwork {
    pub incoming: Sender<SpiceIncomingPartialData>,
}

use near_async::messaging::Sender;
use near_async::{Message, MultiSend, MultiSenderFrom};
use near_primitives::spice_partial_data::SpicePartialData;

#[derive(Message, Debug, Clone, PartialEq, Eq)]
pub struct SpiceIncomingPartialData {
    pub data: SpicePartialData,
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct SpiceDataDistributorSenderForNetwork {
    pub incoming: Sender<SpiceIncomingPartialData>,
}

use near_async::messaging::Sender;
use near_async::{MultiSend, MultiSenderFrom};
use near_primitives::spice_partial_data::{SpiceDataIdentifier, SpicePartialData};
use near_primitives::types::AccountId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpiceIncomingPartialData {
    pub data: SpicePartialData,
}

#[derive(Debug, Clone, PartialEq, Eq, borsh::BorshSerialize, borsh::BorshDeserialize)]
pub struct SpicePartialDataRequest {
    pub data_id: SpiceDataIdentifier,
    pub requester: AccountId,
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct SpiceDataDistributorSenderForNetwork {
    pub incoming: Sender<SpiceIncomingPartialData>,
    pub request: Sender<SpicePartialDataRequest>,
}

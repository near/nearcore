use crate::client::ClientSenderForNetworkInput;
use crate::shards_manager::ShardsManagerRequestFromNetwork;

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum Event {
    ShardsManager(ShardsManagerRequestFromNetwork),
    Client(ClientSenderForNetworkInput),
}

use serde::{Deserialize, Serialize};

pub mod blocks;
pub mod changes;
pub mod chunks;
pub mod config;
pub mod gas_price;
pub mod light_client;
pub mod network_info;
pub mod query;
pub mod receipts;
pub mod sandbox;
pub mod status;
pub mod transactions;
pub mod validator;

pub type ProtocolVersion = u32;
/// Height of the block.
pub type BlockHeight = u64;
/// Balance is type for storing amounts of tokens.
pub type Balance = u128;
/// Gas is a type for storing amount of gas.
pub type Gas = u64;
/// Number of blocks in current group.
pub type NumBlocks = u64;
/// Number of seats of validators (block producer or hidden ones) in current group (settlement).
pub type NumSeats = u64;
/// Block height delta that measures the difference between `BlockHeight`s.
pub type BlockHeightDelta = u64;

/// Costs associated with an object that can only be sent over the network (and executed
/// by the receiver).
/// NOTE: `send_sir` or `send_not_sir` fees are usually burned when the item is being created.
/// And `execution` fee is burned when the item is being executed.
#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct Fee {
    /// Fee for sending an object from the sender to itself, guaranteeing that it does not leave
    /// the shard.
    pub send_sir: Gas,
    /// Fee for sending an object potentially across the shards.
    pub send_not_sir: Gas,
    /// Fee for executing the object.
    pub execution: Gas,
}

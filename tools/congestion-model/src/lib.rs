mod evaluation;
mod model;
pub mod strategy;
pub mod workload;

pub use evaluation::{
    summary_table, QueueStats, ShardQueueLengths, StatsWriter, TransactionStatus,
};
pub use model::{Model, Queue, QueueId, Receipt, ShardId, TransactionId};
pub use strategy::CongestionStrategy;
pub use workload::{ReceiptDefinition, ReceiptId, TransactionBuilder};

pub(crate) use model::Transaction;

/// Gas is measured in Giga Gas as the smallest unit. This way, it fits in a u64
/// even for long simulations.
type GGas = u64;
type Round = u64;

pub const GGAS: GGas = 10u64.pow(0);
pub const TGAS: GGas = 10u64.pow(3);
pub const PGAS: GGas = 10u64.pow(6);

/// How much gas can be executed in a chunk. (Soft limit, one additional receipt is allowed.)
///
/// We assume chunk.gas_limit() is constant. In fact, validators are allowed to
/// dynamically adjust it in small steps per chunk. But in genesis it was set to
/// 1000 TGas and no known validator client ever changes it.
pub const GAS_LIMIT: GGas = 1000 * TGAS;

/// How much gas can be spent for converting transactions, in the original
/// design of Near Protocol.
///
/// The TX gas limit has been hard-coded to gas_limit / 2 for years.
/// <https://github.com/near/nearcore/blob/ac5cba2e7a7507aecce09cbd0152641e986ea381/chain/chain/src/runtime/mod.rs#L709>
///
/// Changing this could be part of a congestion strategy.
pub const TX_GAS_LIMIT: GGas = 500 * TGAS;

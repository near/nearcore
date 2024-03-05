mod evaluation;
mod model;
pub mod strategy;
pub mod workload;

pub use evaluation::{summary_table, TransactionStatus};
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

pub const TX_GAS_LIMIT: GGas = 500 * TGAS;
pub const GAS_LIMIT: GGas = 1000 * TGAS;

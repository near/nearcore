mod all_for_one;
mod balanced;
mod fairness_benchmark;
mod linear_imbalance;
mod transaction_builder;
mod utils;

pub use all_for_one::AllForOneProducer;
pub use balanced::BalancedProducer;
pub use fairness_benchmark::FairnessBenchmarkProducer;
pub use linear_imbalance::LinearImbalanceProducer;
pub use transaction_builder::{ReceiptDefinition, ReceiptId, TransactionBuilder};

use crate::{Round, ShardId};

/// Produces workload in the form of transactions.
///
/// Describes how many messages by which producer should be created in a model
/// execution.
pub trait Producer {
    /// Set up initial state of the producer if necessary.
    fn init(&mut self, shards: &[ShardId]);

    /// Create transactions for a round.
    fn produce_transactions(
        &mut self,
        round: Round,
        shards: &[ShardId],
        tx_factory: &mut dyn FnMut(ShardId) -> TransactionBuilder,
    ) -> Vec<TransactionBuilder>;
}

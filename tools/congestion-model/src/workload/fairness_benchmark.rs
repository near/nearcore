use crate::{GGas, ReceiptDefinition, ShardId, TransactionBuilder, GAS_LIMIT, TGAS};

use super::Producer;

/// Transaction producer that tests fairness for shards.
///
/// One shard has to execute receipts from all others. But it is also trying to
/// send new transactions to all others.
///
/// A fair strategy should allow to send the busy shard at least some work to
/// other shards, since those are not above capacity. Thus, by looking at
/// utilization, we can get a measure of fairness between shards.
pub struct FairnessBenchmarkProducer {
    pub receipt_size: u64,
    pub attached_gas: GGas,
    pub execution_gas: GGas,
    pub conversion_gas: GGas,
}

impl Producer for FairnessBenchmarkProducer {
    fn init(&mut self, _shards: &[ShardId]) {}

    fn produce_transactions(
        &mut self,
        _round: crate::Round,
        shards: &[ShardId],
        tx_factory: &mut dyn FnMut(ShardId) -> TransactionBuilder,
    ) -> Vec<TransactionBuilder> {
        let mut out = vec![];

        let busy_shard = shards[0];
        let other_shards = &shards[1..];

        let mut gas_to_busy = 0;
        let mut gas_to_non_busy = 0;

        // Send from all to the busy shard until it receives at least 4 times
        // what it can handle
        while gas_to_busy < GAS_LIMIT * 4 {
            for &other_shard in other_shards {
                let mut tx_to_busy_shard = tx_factory(other_shard);
                self.produce_tx(busy_shard, &mut tx_to_busy_shard);
                out.push(tx_to_busy_shard);
                gas_to_busy += self.execution_gas;
            }
        }
        
        // Send from the busy shard to all other shards, using around 90% of
        // their capacity.
        while gas_to_non_busy < GAS_LIMIT * 10 / 9 {
            for &other_shard in other_shards {
                let mut tx_from_busy_shard = tx_factory(busy_shard);
                self.produce_tx(other_shard, &mut tx_from_busy_shard);
                out.push(tx_from_busy_shard);
            }
            gas_to_non_busy += self.execution_gas;
        }
        out
    }
}

impl FairnessBenchmarkProducer {
    fn produce_tx(&self, receiver: ShardId, tx: &mut TransactionBuilder) {
        let receipt = ReceiptDefinition {
            receiver,
            size: self.receipt_size,
            attached_gas: self.attached_gas,
            execution_gas: self.execution_gas,
        };
        tx.add_first_receipt(receipt, self.conversion_gas);
        // no refund for this workload
    }
}

impl Default for FairnessBenchmarkProducer {
    fn default() -> Self {
        Self {
            receipt_size: 10_000,
            attached_gas: 200 * TGAS,
            execution_gas: 100 * TGAS,
            conversion_gas: 1 * TGAS,
        }
    }
}

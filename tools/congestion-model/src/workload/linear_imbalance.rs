use crate::{GAS_LIMIT, GGas, ReceiptDefinition, ShardId, TGAS, TransactionBuilder};

use super::{Producer, utils};

/// Shards X sends transactions to all shards >= X.
///
/// By sending transactions to all shards with a higher shard id, we get a
/// linear progression of how busy each shard is. Shard 1 only processes 1
/// receipt per round. Shard 2 processes 2 and so on. With enough shards, we get
/// many different levels of congestion and we get to see how many shards are
/// still able to function undisturbed.
///
/// The work intensity is scaled such that half the shard are below max capacity
/// and half are above (forced to be congested). And the busiest shard (highest
/// shard id) receives twice its capacity.
///
/// As an actual formula, it looks like this:
///
/// ```ignore
/// receipts_per_round(shard_id) =
///     2 * (shard_id + 1) * (GAS_LIMIT / GAS_PER_RECEIPT) / NUM_SHARDS
/// ```
pub struct LinearImbalanceProducer {
    pub receipt_size: u64,
    pub attached_gas: GGas,
    pub execution_gas: GGas,
    pub conversion_gas: GGas,

    /// How many messages are sent for each directed shard pair (a,b) where a <= b.
    ///
    /// This value is computed in `init` according to the number of shards and
    /// the configured gas per receipt.
    message_multiplier: usize,
}

impl Producer for LinearImbalanceProducer {
    fn init(&mut self, shards: &[ShardId]) {
        let factor = 2.0 * (GAS_LIMIT as f32 / self.execution_gas as f32) / shards.len() as f32;
        self.message_multiplier = factor.round() as usize;
    }

    fn produce_transactions(
        &mut self,
        _round: crate::Round,
        shards: &[ShardId],
        tx_factory: &mut dyn FnMut(ShardId) -> TransactionBuilder,
    ) -> Vec<TransactionBuilder> {
        let mut all_tx = vec![];
        for (sender_index, &sender_id) in shards.iter().enumerate() {
            // send to all shards after or equal to the own shard id
            for _ in 0..self.message_multiplier {
                for &receiver in &shards[sender_index..] {
                    let main_receipt = ReceiptDefinition {
                        receiver,
                        size: self.receipt_size,
                        attached_gas: self.attached_gas,
                        execution_gas: self.execution_gas,
                    };
                    let mut tx = tx_factory(sender_id);
                    let main_receipt_id = tx.add_first_receipt(main_receipt, self.conversion_gas);
                    // also add a refund receipt going back
                    tx.new_outgoing_receipt(main_receipt_id, utils::refund_receipt(sender_id));
                    all_tx.push(tx);
                }
            }
        }
        all_tx
    }
}

impl LinearImbalanceProducer {
    pub fn big_receipts() -> Self {
        Self {
            receipt_size: 2_000_000,
            attached_gas: 5 * TGAS,
            execution_gas: 5 * TGAS,
            ..Self::default()
        }
    }
}

impl Default for LinearImbalanceProducer {
    fn default() -> Self {
        Self {
            receipt_size: 512,
            attached_gas: 50 * TGAS,
            execution_gas: 10 * TGAS,
            conversion_gas: 2 * TGAS,
            // overwritten in `init`
            message_multiplier: 1,
        }
    }
}

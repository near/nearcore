use super::{utils, Producer};
use crate::{GGas, ReceiptDefinition, ReceiptId, ShardId, TransactionBuilder, TGAS};

/// Transaction producer that sends N receipts from each shard to all other
/// shards every round.
///
/// Transactions can be configured with a depth. At depth 1, the receipt is sent
/// directly to the receiver shard. At depth N, there are N-1 rounds on the
/// sender shard, where the receipt is executed locally and produces another
/// receipt. Only the last receipt leaves the local shard.
/// The execution gas is split evenly across all receipts in the transaction.
///
/// Another configuration option is fan-out. It allows to multiply the number of
/// receipts on each round of local execution. This only has an effect if the
/// depth is larger than 1.
///
/// The last configuration option allows to set receipt sizes. By default, they
/// are all th same fixed size. But one can override the size per depth level of
/// the receipt.
pub struct BalancedProducer {
    // direct config inputs
    conversion_gas: GGas,
    num_tx_per_shard_pair: usize,
    receipt_sizes: Vec<u64>,
    fan_out: usize,

    // computed
    depth: usize,
    execution_gas_per_receipt: GGas,
    attached_gas_per_depth: Vec<GGas>,
}

impl Producer for BalancedProducer {
    fn init(&mut self, _shards: &[ShardId]) {}

    fn produce_transactions(
        &mut self,
        _round: crate::Round,
        shards: &[ShardId],
        tx_factory: &mut dyn FnMut(ShardId) -> TransactionBuilder,
    ) -> Vec<TransactionBuilder> {
        let mut out = vec![];
        for _ in 0..self.num_tx_per_shard_pair {
            for sender in shards {
                for receiver in shards {
                    let mut tx = tx_factory(*sender);

                    let first_receipt = self.produce_first_receipt(*receiver, &mut tx);
                    let mut prev_depth_receipts = vec![first_receipt];
                    let mut next_depth_receipts = vec![];
                    // if depth >= 2, generate receipts for each additional layer
                    for depth in 2..=self.depth {
                        // if fan_out is >= 2, layers have an increasing number of receipts
                        for parent in &prev_depth_receipts {
                            for _ in 0..self.fan_out {
                                let child = self.add_receipt(*receiver, &mut tx, *parent, depth);
                                next_depth_receipts.push(child);
                            }
                        }
                        // swap children of this layer to become parents of next layer
                        std::mem::swap(&mut prev_depth_receipts, &mut next_depth_receipts);
                        next_depth_receipts.clear();
                    }
                    out.push(tx);
                }
            }
        }
        out
    }
}

impl BalancedProducer {
    pub fn with_sizes_and_fan_out(receipt_sizes: Vec<u64>, fan_out: usize) -> Self {
        let attached_gas = 300 * TGAS;
        let execution_gas = 100 * TGAS;
        let conversion_gas = 5 * TGAS;
        let num_tx_per_shard_pair = 4;
        Self::new(
            attached_gas,
            execution_gas,
            conversion_gas,
            num_tx_per_shard_pair,
            receipt_sizes,
            fan_out,
        )
    }

    pub fn new(
        attached_gas: GGas,
        execution_gas: GGas,
        conversion_gas: GGas,
        num_tx_per_shard_pair: usize,
        receipt_sizes: Vec<u64>,
        fan_out: usize,
    ) -> Self {
        assert!(!receipt_sizes.is_empty(), "must have at least one receipt size");
        assert!(execution_gas <= attached_gas, "must attach more gas than is needed for execution");
        let depth = receipt_sizes.len();
        let mut num_receipts = 1;
        for d in 1..depth {
            num_receipts += fan_out.pow(d as u32);
        }
        let execution_gas_per_receipt = execution_gas / num_receipts as u64;

        let mut attached_gas_per_depth = vec![attached_gas];
        // Pre-calculate how much gas will be attached to each receipt depending on its depth.
        // Each round, `execution_gas_per_receipt` is burnt and the rest divided by `fan_out`.
        for _ in 1..depth {
            let prev_gas = attached_gas_per_depth.last().unwrap();
            attached_gas_per_depth.push((prev_gas - execution_gas_per_receipt) / fan_out as u64);
        }

        let smallest_attached = *attached_gas_per_depth.last().unwrap();
        assert!(
            execution_gas_per_receipt <= smallest_attached,
            "impossible workload config detected, receipt require {execution_gas_per_receipt} Ggas but only have {smallest_attached} attached"
        );

        Self {
            depth,
            execution_gas_per_receipt,
            attached_gas_per_depth,
            conversion_gas,
            num_tx_per_shard_pair,
            receipt_sizes,
            fan_out,
        }
    }

    fn produce_first_receipt(&self, receiver: ShardId, tx: &mut TransactionBuilder) -> ReceiptId {
        let first_receiver = if self.depth == 1 { receiver } else { tx.sender_shard() };

        let receipt = self.receipt(first_receiver, 1);
        let first = tx.add_first_receipt(receipt, self.conversion_gas);
        tx.new_outgoing_receipt(first, utils::refund_receipt(tx.sender_shard()));
        first
    }

    fn add_receipt(
        &mut self,
        receiver: ShardId,
        tx: &mut TransactionBuilder,
        prev: ReceiptId,
        depth: usize,
    ) -> ReceiptId {
        let next_receiver = if self.depth == depth { receiver } else { tx.sender_shard() };
        let next_receipt = tx.new_outgoing_receipt(prev, self.receipt(next_receiver, depth));
        tx.new_outgoing_receipt(prev, utils::refund_receipt(tx.sender_shard()));
        next_receipt
    }

    fn receipt(&self, receiver: ShardId, depth: usize) -> ReceiptDefinition {
        ReceiptDefinition {
            receiver,
            size: self.receipt_sizes[depth - 1],
            attached_gas: self.attached_gas_per_depth[depth - 1],
            execution_gas: self.execution_gas_per_receipt,
        }
    }
}

impl Default for BalancedProducer {
    fn default() -> Self {
        // as a default, use 1kb sized receipts with 300 Tgas attached that end
        // up actually executing 5 TGas + 100 TGas.
        let receipt_sizes = vec![1024];
        let attached_gas = 300 * TGAS;
        let execution_gas = 100 * TGAS;
        let conversion_gas = 5 * TGAS;
        let num_tx_per_shard_pair = 6;
        let fan_out = 1;
        Self::new(
            attached_gas,
            execution_gas,
            conversion_gas,
            num_tx_per_shard_pair,
            receipt_sizes,
            fan_out,
        )
    }
}

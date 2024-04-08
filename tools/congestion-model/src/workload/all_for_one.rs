use crate::{GGas, ReceiptDefinition, ShardId, TransactionBuilder, TGAS};

use super::{utils, Producer};

/// Transaction producer that sends receipts from all shards to shard 0.
///
/// A third of the transactions contain a single receipt, going directly to
/// shard 0. Those are easy to manage by congestion control strategies, as the
/// receiver is known up front and therefore it can easily be dropped when that
/// shard is congested.
///
/// Another third of transactions contains two receipts, with most of the gas
/// spent on the second receipt. The first receipt executes locally, the second
/// receipt is in shard 0 where the congestion should happen.
///
/// The last third of transactions has three receipts. The first two receipts
/// execute at shard + 1 and shard + 2 (mod N). The third receipt again executes
/// at shard 0.
///
/// Each of the three types of receipts can also be disabled individually. But
/// by default, all three are produced.
///
/// All receipts have the same size, to keep the workload relatively simple.
pub struct AllForOneProducer {
    // config options
    pub receipt_size: u64,
    pub attached_gas: GGas,
    pub light_execution_gas: GGas,
    pub last_execution_gas: GGas,
    pub conversion_gas: GGas,
    pub messages_per_round: usize,
    pub enable_one_hop: bool,
    pub enable_two_hops: bool,
    pub enable_three_hops: bool,

    // state
    pub round_robin_shards: Box<dyn Iterator<Item = ShardId>>,
}

impl Producer for AllForOneProducer {
    fn init(&mut self, shards: &[ShardId]) {
        // note: the `to_vec` is required to satisfy lifetimes
        #[allow(clippy::unnecessary_to_owned)]
        let iter = shards.to_vec().into_iter().cycle();
        self.round_robin_shards = Box::new(iter);
    }

    fn produce_transactions(
        &mut self,
        _round: crate::Round,
        shards: &[ShardId],
        tx_factory: &mut dyn FnMut(ShardId) -> TransactionBuilder,
    ) -> Vec<TransactionBuilder> {
        let mut hops_enabled = vec![];
        if self.enable_one_hop {
            hops_enabled.push(1);
        }
        if self.enable_two_hops {
            hops_enabled.push(2);
        }
        if self.enable_three_hops {
            hops_enabled.push(3);
        }

        let mut out = vec![];
        for i in 0..self.messages_per_round {
            let hops = hops_enabled[i % hops_enabled.len()];
            let shard = self.round_robin_shards.next().unwrap();
            let mut tx_builder = tx_factory(shard);
            match hops {
                1 => self.produce_one_hop_tx(shards, &mut tx_builder),
                2 => self.produce_two_hops_tx(shards, &mut tx_builder),
                3 => self.produce_three_hops_tx(shards, &mut tx_builder),
                _ => unreachable!(),
            }
            out.push(tx_builder);
        }
        out
    }
}

impl AllForOneProducer {
    /// Transaction with a single receipt, going directly to shard 0.
    fn produce_one_hop_tx(&self, shards: &[ShardId], tx: &mut TransactionBuilder) {
        let heavy_receipt = self.receipt_to_shard_0(shards, 1);
        let first = tx.add_first_receipt(heavy_receipt, self.conversion_gas);
        tx.new_outgoing_receipt(first, utils::refund_receipt(tx.sender_shard()));
    }

    /// Transaction with two receipts, a one executing locally with small
    /// execution gas, followed by one going to shard 0 spending more gas.
    fn produce_two_hops_tx(&self, shards: &[ShardId], tx: &mut TransactionBuilder) {
        let light_receipt = self.light_receipt(tx.sender_shard(), 0);
        let heavy_receipt = self.receipt_to_shard_0(shards, 1);
        let first = tx.add_first_receipt(light_receipt, self.conversion_gas);
        let second = tx.new_outgoing_receipt(first, heavy_receipt);

        tx.new_outgoing_receipt(first, utils::refund_receipt(tx.sender_shard()));
        tx.new_outgoing_receipt(second, utils::refund_receipt(tx.sender_shard()));
    }

    /// Transaction with a chain of three receipts, the last one spending the
    /// most gas. The last receipt always has shard 0 as receiver.
    fn produce_three_hops_tx(&self, shards: &[ShardId], tx: &mut TransactionBuilder) {
        let my_index = shards
            .iter()
            .position(|&id| id == tx.sender_shard())
            .expect("sender must be in shards");
        let hop_1 = shards[(my_index + 1) % shards.len()];
        let hop_2 = shards[(my_index + 2) % shards.len()];

        let light_receipt_1 = self.light_receipt(hop_1, 0);
        let light_receipt_2 = self.light_receipt(hop_2, 1);
        let heavy_receipt = self.receipt_to_shard_0(shards, 2);

        let first = tx.add_first_receipt(light_receipt_1, self.conversion_gas);
        let second = tx.new_outgoing_receipt(first, light_receipt_2);
        let third = tx.new_outgoing_receipt(second, heavy_receipt);

        tx.new_outgoing_receipt(first, utils::refund_receipt(tx.sender_shard()));
        tx.new_outgoing_receipt(second, utils::refund_receipt(tx.sender_shard()));
        tx.new_outgoing_receipt(third, utils::refund_receipt(tx.sender_shard()));
    }

    fn receipt_to_shard_0(&self, shards: &[ShardId], prior_hops: u64) -> ReceiptDefinition {
        ReceiptDefinition {
            receiver: shards[0],
            size: self.receipt_size,
            attached_gas: self.attached_gas - self.light_execution_gas * prior_hops,
            execution_gas: self.last_execution_gas,
        }
    }

    fn light_receipt(&self, shard: ShardId, prior_hops: u64) -> ReceiptDefinition {
        ReceiptDefinition {
            receiver: shard,
            size: self.receipt_size,
            attached_gas: self.attached_gas - self.light_execution_gas * prior_hops,
            execution_gas: self.light_execution_gas,
        }
    }

    pub fn new(enable_one_hop: bool, enable_two_hops: bool, enable_three_hops: bool) -> Self {
        Self { enable_one_hop, enable_two_hops, enable_three_hops, ..Default::default() }
    }
}

impl Default for AllForOneProducer {
    fn default() -> Self {
        // for this simple scenario, as a default, use 1kb sized receipts with 300 Tgas attached that
        // end up actually executing 5 TGas + 100 TGas (+ optionally 2x 10 Tgas).
        Self {
            receipt_size: 1024,
            attached_gas: 300 * TGAS,
            light_execution_gas: 10 * TGAS,
            last_execution_gas: 100 * TGAS,
            conversion_gas: 5 * TGAS,
            // ideally a number divisible by 3
            messages_per_round: 42,
            // empty iterator overwritten in init
            round_robin_shards: Box::new(std::iter::empty()),
            enable_one_hop: true,
            enable_two_hops: true,
            enable_three_hops: true,
        }
    }
}

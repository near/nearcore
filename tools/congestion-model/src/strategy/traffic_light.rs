use super::QueueFactory;
use crate::model::ChunkExecutionContext;
use crate::{
    CongestionStrategy, GGas, QueueId, Receipt, Round, ShardId, GAS_LIMIT, TGAS, TX_GAS_LIMIT,
};
use std::collections::HashMap;

/// A congestion strategy featuring 3 levels of traffic control.
///
/// This strategy is designed to guarantee:
/// - Hard limit on incoming queue length measured in attached gas.
/// - Soft limit on total size of receipts in incoming + outgoing receipts.
/// - Provably no deadlocks.
///
/// In the traffic light strategy, each shard set its own traffic signal to
/// green (full-speed), amber (senders must stop accepting more load for this
/// shard), red (senders must stop all load to this shard).
///
/// When a shard turns on the red light, manual traffic control ensures no
/// deadlocks occur. The congested shard will assign one shard each turn that is
/// allowed to send a limited amount of traffic.
///
pub struct TrafficLight {
    // config
    amber_gas_queue_limit: GGas,
    red_gas_queue_limit: GGas,
    red_queue_size_limit: u64,

    green_outgoing_receipts_gas: GGas,
    amber_outgoing_receipts_gas: GGas,
    red_outgoing_receipts_gas_single_shard: GGas,

    green_new_tx_gas: GGas,
    amber_new_tx_gas: GGas,
    red_new_tx_gas: GGas,

    // state
    shard_id: Option<ShardId>,
    outgoing_buffer: HashMap<ShardId, QueueId>,
    all_shards: Vec<ShardId>,
    outgoing_gas_allowance: HashMap<ShardId, GGas>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TrafficLightStatus {
    Green,
    Amber,
    Red { allowed_shard: ShardId },
}

impl Default for TrafficLight {
    fn default() -> Self {
        Self {
            // at which levels the status color is switched
            amber_gas_queue_limit: 50_000 * TGAS, // ~50 chunks
            red_gas_queue_limit: 100_000 * TGAS,  // ~100 chunks
            red_queue_size_limit: 500 * 10u64.pow(6), // 500 MB

            // how much gas each shard can send depending on the receiver
            // shard's color note that this is "attached gas" and the actual gas
            // the receiver will need to burn while executing can be much lower
            green_outgoing_receipts_gas: 30_000 * TGAS, // ~30 chunks
            amber_outgoing_receipts_gas: 5_000 * TGAS,  // ~5 chunks
            // RED: only the selected shard can send this amount. This number
            // should be quite low. The receiving shard is not running out of
            // executable receipts. But it should also not be too slow, since
            // this is all the progress we get on the sending shard in case it
            // is also itself full .
            red_outgoing_receipts_gas_single_shard: 1_000 * TGAS, // < 1 chunk

            // how much gas is assigned to new transactions on a shard depending on it color
            green_new_tx_gas: TX_GAS_LIMIT,
            amber_new_tx_gas: 100 * TGAS,
            red_new_tx_gas: 20 * TGAS,

            // state to be initialized in `init`
            shard_id: None,
            outgoing_buffer: HashMap::new(),
            all_shards: vec![],

            // state to update every round
            outgoing_gas_allowance: HashMap::new(),
        }
    }
}

impl CongestionStrategy for TrafficLight {
    fn init(
        &mut self,
        own_id: crate::ShardId,
        all_shards: &[crate::ShardId],
        queue_factory: &mut dyn QueueFactory,
    ) {
        self.shard_id = Some(own_id);
        self.outgoing_buffer = all_shards
            .iter()
            .map(|&id| (id, queue_factory.register_queue(own_id, &format!("out_buffer_{id}"))))
            .collect();
        self.all_shards = all_shards.to_vec();
    }

    fn compute_chunk(&mut self, ctx: &mut ChunkExecutionContext) {
        let tx_gas = match self.my_prev_status(ctx) {
            TrafficLightStatus::Green => self.green_new_tx_gas,
            TrafficLightStatus::Amber => self.amber_new_tx_gas,
            TrafficLightStatus::Red { .. } => self.red_new_tx_gas,
        };
        self.reset_outgoing_gas_allowance(ctx);

        // try forwarding buffered outgoing receipts
        for (shard, &q) in &self.outgoing_buffer {
            while !(&mut ctx.queue(q)).is_empty()
                && self.outgoing_gas_allowance[&shard]
                    >= (&mut ctx.queue(q)).front().unwrap().attached_gas
            {
                let receipt = (&mut ctx.queue(q)).pop_front().unwrap();
                let allowance = self.outgoing_gas_allowance.get_mut(&shard).unwrap();
                *allowance = allowance.saturating_sub(receipt.attached_gas);
                ctx.forward_receipt(receipt);
            }
        }

        while ctx.gas_burnt() < tx_gas {
            let Some(tx) = ctx.incoming_transactions().pop_front() else {
                // no more transaction incoming
                break;
            };
            let receiver = ctx.tx_receiver(tx);
            if status(ctx, receiver) != TrafficLightStatus::Green {
                // dropping transaction, the receiver is already too busy
                // (dropping simply means not accepting it, it's already
                // removed from the incoming queue)
            } else {
                let receipt = ctx.accept_transaction(tx);
                self.forward_or_buffer(receipt, ctx);
            }
        }

        while ctx.gas_burnt() < GAS_LIMIT {
            if let Some(receipt) = ctx.incoming_receipts().pop_front() {
                let outgoing = ctx.execute_receipt(receipt);
                for receipt in outgoing {
                    self.forward_or_buffer(receipt, ctx);
                }
            } else {
                // no more receipts to execute
                break;
            }
        }

        let my_status = self.my_new_status(ctx);
        ctx.current_block_info().insert(my_status);
    }
}

impl TrafficLight {
    fn reset_outgoing_gas_allowance(&mut self, ctx: &mut ChunkExecutionContext) {
        self.outgoing_gas_allowance.clear();
        for &shard in &self.all_shards {
            let gas = match status(ctx, shard) {
                TrafficLightStatus::Green => self.green_outgoing_receipts_gas,
                TrafficLightStatus::Amber => self.amber_outgoing_receipts_gas,
                TrafficLightStatus::Red { allowed_shard }
                    if allowed_shard == self.shard_id.unwrap() =>
                {
                    self.red_outgoing_receipts_gas_single_shard
                }
                _else => 0,
            };
            self.outgoing_gas_allowance.insert(shard, gas);
        }
    }

    fn forward_or_buffer(&mut self, receipt: Receipt, ctx: &mut ChunkExecutionContext) {
        let allowance = self.outgoing_gas_allowance.get_mut(&receipt.receiver).unwrap();
        if *allowance >= receipt.attached_gas {
            *allowance = allowance.saturating_sub(receipt.attached_gas);
            ctx.forward_receipt(receipt);
        } else {
            let q = self.outgoing_buffer[&receipt.receiver];
            ctx.queue(q).push_back(receipt);
        }
    }

    fn my_prev_status(&self, ctx: &mut ChunkExecutionContext) -> TrafficLightStatus {
        status(ctx, self.shard_id.unwrap())
    }

    fn my_new_status(&self, ctx: &mut ChunkExecutionContext) -> TrafficLightStatus {
        let incoming_gas_queued: GGas =
            ctx.incoming_receipts().iter().map(|r| r.attached_gas).sum();
        let incoming_bytes_queued: u64 = ctx.incoming_receipts().iter().map(|r| r.size).sum();
        let outgoing_bytes_queued: u64 = self
            .outgoing_buffer
            .values()
            .map(|&q| ctx.queue(q).iter().map(|r| r.size).sum::<u64>())
            .sum();

        // If we are reaching the memory limit, stop as much traffic as we can.
        if incoming_bytes_queued + outgoing_bytes_queued >= self.red_queue_size_limit {
            return self.red(ctx.block_height());
        };

        let amber = self.amber_gas_queue_limit;
        let red = self.red_gas_queue_limit;
        if incoming_gas_queued < amber {
            TrafficLightStatus::Green
        } else if incoming_gas_queued < red {
            TrafficLightStatus::Amber
        } else {
            self.red(ctx.block_height())
        }
    }

    fn red(&self, round: Round) -> TrafficLightStatus {
        // use simple round-robin, it gives good fairness and is stateless
        TrafficLightStatus::Red {
            allowed_shard: self.all_shards
                [(round as usize + *self.shard_id.unwrap()) % self.all_shards.len()],
        }
    }
}

fn status(ctx: &mut ChunkExecutionContext, shard_id: ShardId) -> TrafficLightStatus {
    ctx.prev_block_info()
        .get(&shard_id)
        .and_then(|block| block.get::<TrafficLightStatus>())
        .copied()
        .unwrap_or(TrafficLightStatus::Green)
}

use crate::model::ChunkExecutionContext;
use crate::strategy::QueueFactory;
use crate::{GGas, ShardId, GAS_LIMIT, TGAS, TX_GAS_LIMIT};
use std::collections::{BTreeMap, HashMap};

#[derive(Default)]
pub struct SoftBackpressure {
    shard_id: Option<ShardId>,
    all_shards: Vec<ShardId>,
}

#[derive(Default, Clone, Debug)]
struct ShardState {
    orig_rec_txn_input_gas: HashMap<(ShardId, ShardId), GGas>,
    shard_current_load: GGas,
}

const MAX_DESIRED_QUEUE_SIZE: GGas = 4000 * TGAS;

impl crate::CongestionStrategy for SoftBackpressure {
    fn init(&mut self, id: ShardId, all_shards: &[ShardId], _queue_factory: &mut dyn QueueFactory) {
        self.shard_id = Some(id);
        self.all_shards = all_shards.to_vec();
    }

    fn compute_chunk(&mut self, ctx: &mut ChunkExecutionContext) {
        let prev_state = self
            .all_shards
            .iter()
            .map(|shard_id| {
                (
                    *shard_id,
                    ctx.prev_block_info()
                        .get(shard_id)
                        .and_then(|block_info| block_info.get::<ShardState>().cloned())
                        .unwrap_or_default(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut bucket_limits = calculate_tx_limits(&prev_state, &self.all_shards);
        // if self.shard_id == Some(self.all_shards[0]) {
        //     println!("Tx bucket limits: {:?}", bucket_limits);
        // }

        let mut dropped_txs = Vec::new();
        while ctx.gas_burnt() < TX_GAS_LIMIT {
            if let Some(tx) = ctx.incoming_transactions().pop_front() {
                let sender = ctx.tx_sender(tx);
                let receiver = ctx.tx_receiver(tx);
                let gas = ctx.tx_attached_gas(tx);
                let bucket = (sender, receiver);
                let limit = bucket_limits.get_mut(&bucket).unwrap();
                if gas <= *limit {
                    let receipt = ctx.accept_transaction(tx);
                    ctx.forward_receipt(receipt);
                    *limit -= gas;
                } else {
                    dropped_txs.push(tx);
                }
            } else {
                break;
            }
        }
        for tx in dropped_txs.drain(..).rev() {
            ctx.incoming_transactions().push_front(tx);
        }

        let mut orig_rec_txn_input_gas = HashMap::new();
        let orig_tx_ids = ctx
            .incoming_receipts()
            .iter()
            .map(|receipt| receipt.transaction_id())
            .collect::<Vec<_>>();
        for tx in orig_tx_ids {
            let tx_sender = ctx.tx_sender(tx);
            let tx_receiver = ctx.tx_receiver(tx);
            let tx_gas = ctx.tx_attached_gas(tx);
            *orig_rec_txn_input_gas.entry((tx_sender, tx_receiver)).or_default() += tx_gas;
        }

        let incoming_receipt_gas: GGas =
            ctx.incoming_receipts().iter().map(|receipt| receipt.attached_gas).sum();
        let prev_gas_burnt = ctx.gas_burnt();
        while ctx.gas_burnt() < GAS_LIMIT {
            if let Some(receipt) = ctx.incoming_receipts().pop_front() {
                let outgoing = ctx.execute_receipt(receipt);
                for receipt in outgoing {
                    ctx.forward_receipt(receipt);
                }
            } else {
                break;
            }
        }
        let execution_gas = if ctx.gas_burnt() >= GAS_LIMIT {
            let remaining_receipt_gas: GGas =
                ctx.incoming_receipts().iter().map(|receipt| receipt.attached_gas).sum();
            let estimated_execution_gas = incoming_receipt_gas * (ctx.gas_burnt() - prev_gas_burnt)
                / (incoming_receipt_gas - remaining_receipt_gas);
            estimated_execution_gas
        } else {
            ctx.gas_burnt() - prev_gas_burnt
        };
        let shard_state = ShardState { orig_rec_txn_input_gas, shard_current_load: execution_gas };
        // println!("Shard {:?} state: {:?}", self.shard_id.unwrap(), shard_state);
        ctx.current_block_info().insert(shard_state);
    }
}

struct BucketCalc {
    contribution: HashMap<ShardId, GGas>,
    total_contribution: GGas,
    allowance: GGas,
}

impl BucketCalc {
    fn new() -> Self {
        Self { total_contribution: 0, contribution: HashMap::new(), allowance: 10 * GAS_LIMIT }
    }

    fn add_input_gas(&mut self, shard: ShardId, gas: GGas) {
        self.contribution.insert(shard, gas);
        self.total_contribution += gas;
    }

    fn limit_to(&mut self, shard: ShardId, gas: GGas) {
        let shard_contribution = self.contribution.get(&shard).unwrap();
        let limit = gas * self.total_contribution / shard_contribution;
        self.allowance = self.allowance.min(limit);
    }
}

fn calculate_tx_limits(
    shards: &BTreeMap<ShardId, ShardState>,
    all_shards: &[ShardId],
) -> HashMap<(ShardId, ShardId), GGas> {
    let mut buckets: HashMap<(ShardId, ShardId), BucketCalc> = HashMap::new();
    for i in all_shards {
        for j in all_shards {
            buckets.insert((*i, *j), BucketCalc::new());
        }
    }
    for shard in all_shards {
        let shard_state = shards.get(shard).unwrap();
        for (bucket, gas) in &shard_state.orig_rec_txn_input_gas {
            buckets.get_mut(bucket).unwrap().add_input_gas(*shard, *gas);
        }
    }
    for shard_id in all_shards {
        let shard_state = shards.get(shard_id).unwrap();

        let total_previous_intake: GGas =
            shard_state.orig_rec_txn_input_gas.values().copied().sum();
        let intake_per_gas = if shard_state.shard_current_load == 0 {
            1.0
        } else {
            total_previous_intake as f64 / shard_state.shard_current_load as f64
        };

        let available_capacity =
            MAX_DESIRED_QUEUE_SIZE.checked_sub(shard_state.shard_current_load).unwrap_or_default();
        let desired_total_intake = (available_capacity as f64 * intake_per_gas) as u64;

        // Apply backpressure to the top buckets until the remaining sum is enough.
        let mut bucket_inputs =
            shard_state.orig_rec_txn_input_gas.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>();
        bucket_inputs.sort_by_key(|(_, v)| -(*v as i64));
        let (limit_count, limit_cutoff) = limit_top(
            &bucket_inputs.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
            desired_total_intake,
        );
        for i in 0..limit_count {
            let bucket = bucket_inputs[i].0;
            buckets.get_mut(&bucket).unwrap().limit_to(*shard_id, limit_cutoff);
        }
        // if limit_count > 0 {
        //     println!("Limiting the first {} buckets to {}", limit_count, limit_cutoff);
        // }
    }
    buckets.into_iter().map(|(k, v)| (k, v.allowance)).collect()
}

fn limit_top(amounts: &[u64], limit: u64) -> (usize, u64) {
    if amounts.is_empty() {
        return (0, 0);
    }
    let mut sum: u64 = amounts.iter().sum();
    let mut index = 0;
    let mut cutoff = amounts[0];
    while sum > limit {
        index += 1;
        let mut next_cutoff = if index == amounts.len() { 0 } else { amounts[index] };
        if index as u64 * (cutoff - next_cutoff) > sum - limit {
            next_cutoff = cutoff - (sum - limit + index as u64 - 1) / index as u64;
        }
        sum -= (cutoff - next_cutoff) * index as u64;
        cutoff = next_cutoff;
    }
    (index, cutoff)
}

#[cfg(test)]
mod tests {
    use crate::strategy::soft_backpressure::limit_top;

    #[test]
    fn test_limit_top() {
        assert_eq!(limit_top(&[], 1234), (0, 0));
        assert_eq!(limit_top(&[60, 50, 40, 30, 20, 10], 165), (3, 35));
    }
}

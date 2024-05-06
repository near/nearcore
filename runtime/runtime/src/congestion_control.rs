use crate::config::{
    safe_add_gas, total_prepaid_exec_fees, total_prepaid_gas, total_prepaid_send_fees,
};
use crate::ApplyState;
use near_parameters::{ActionCosts, RuntimeConfig};
use near_primitives::congestion_info::{CongestionInfo, CongestionInfoV1};
use near_primitives::errors::{IntegerOverflowError, RuntimeError};
use near_primitives::receipt::{Receipt, ReceiptEnum};
use near_primitives::types::{EpochInfoProvider, Gas, ShardId};
use near_primitives::version::ProtocolFeature;
use near_store::trie::receipts_column_helper::{
    DelayedReceiptQueue, ShardsOutgoingReceiptBuffer, TrieQueue,
};
use near_store::{StorageError, TrieAccess, TrieUpdate};
use near_vm_runner::logic::ProtocolVersion;
use std::collections::HashMap;

/// Handle receipt forwarding for different protocol versions.
pub(crate) enum ReceiptSink<'a> {
    V1(ReceiptSinkV1<'a>),
    V2(ReceiptSinkV2<'a>),
}

/// Always put receipt to the outgoing receipts.
pub(crate) struct ReceiptSinkV1<'a> {
    pub(crate) outgoing_receipts: &'a mut Vec<Receipt>,
}

/// A helper struct to buffer or forward receipts.
///
/// When the runtime produces receipts for other shards, a `ReceiptSink` accept
/// them and either puts them in the outgoing receipts list, or puts them in a
/// buffer which is stored in the trie.
///
/// This is for congestion control, allowing to apply backpressure from the
/// receiving shard and stopping us from sending more receipts to it than its
/// nodes can keep in memory.
pub(crate) struct ReceiptSinkV2<'a> {
    pub(crate) congestion_info: &'a mut CongestionInfo,
    pub(crate) outgoing_receipts: &'a mut Vec<Receipt>,
    pub(crate) outgoing_limit: HashMap<ShardId, Gas>,
    pub(crate) outgoing_buffers: ShardsOutgoingReceiptBuffer,
}

enum ReceiptForwarding {
    Forwarded,
    NotForwarded(Receipt),
}

impl<'a> ReceiptSink<'a> {
    pub(crate) fn new(
        protocol_version: ProtocolVersion,
        trie: &dyn TrieAccess,
        apply_state: &ApplyState,
        congestion_info: &'a mut Option<CongestionInfo>,
        outgoing_receipts: &'a mut Vec<Receipt>,
    ) -> Result<Self, StorageError> {
        if let Some(ref mut congestion_info) = congestion_info {
            debug_assert!(ProtocolFeature::CongestionControl.enabled(protocol_version));
            let outgoing_buffers = ShardsOutgoingReceiptBuffer::load(trie)?;

            let outgoing_limit: HashMap<ShardId, Gas> = apply_state
                .congestion_info
                .iter()
                .map(|(&shard_id, congestion)| {
                    (shard_id, congestion.outgoing_limit(apply_state.shard_id))
                })
                .collect();

            Ok(ReceiptSink::V2(ReceiptSinkV2 {
                congestion_info: congestion_info,
                outgoing_receipts: outgoing_receipts,
                outgoing_limit,
                outgoing_buffers,
            }))
        } else {
            debug_assert!(!ProtocolFeature::CongestionControl.enabled(protocol_version));
            Ok(ReceiptSink::V1(ReceiptSinkV1 { outgoing_receipts: outgoing_receipts }))
        }
    }

    /// Forward receipts already in the buffer to the outgoing receipts vector, as
    /// much as the gas limits allow.
    pub(crate) fn forward_from_buffer(
        &mut self,
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
    ) -> Result<(), RuntimeError> {
        match self {
            ReceiptSink::V1(_inner) => Ok(()),
            ReceiptSink::V2(inner) => inner.forward_from_buffer(state_update, apply_state),
        }
    }

    /// Put a receipt in the outgoing receipts vector (=forward) if the
    /// congestion preventing limits allow it. Put it in the buffered receipts
    /// queue otherwise.
    pub(crate) fn forward_or_buffer_receipt(
        &mut self,
        receipt: Receipt,
        apply_state: &ApplyState,
        state_update: &mut TrieUpdate,
        epoch_info_provider: &dyn EpochInfoProvider,
    ) -> Result<(), RuntimeError> {
        match self {
            ReceiptSink::V1(inner) => {
                inner.forward(receipt);
                Ok(())
            }
            ReceiptSink::V2(inner) => inner.forward_or_buffer_receipt(
                receipt,
                apply_state,
                state_update,
                epoch_info_provider,
            ),
        }
    }
}

impl ReceiptSinkV1<'_> {
    /// V1 can only forward receipts.
    pub(crate) fn forward(&mut self, receipt: Receipt) {
        self.outgoing_receipts.push(receipt);
    }
}

impl ReceiptSinkV2<'_> {
    /// Forward receipts already in the buffer to the outgoing receipts vector, as
    /// much as the gas limits allow.
    pub(crate) fn forward_from_buffer(
        &mut self,
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
    ) -> Result<(), RuntimeError> {
        // store shards in vec to avoid borrowing self.outgoing_limit
        let shards: Vec<_> = self.outgoing_limit.keys().copied().collect();
        for shard_id in shards {
            self.forward_from_buffer_to_shard(shard_id, state_update, apply_state)?;
        }
        Ok(())
    }

    fn forward_from_buffer_to_shard(
        &mut self,
        shard_id: u64,
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
    ) -> Result<(), RuntimeError> {
        let mut num_forwarded = 0;
        for receipt_result in self.outgoing_buffers.to_shard(shard_id).iter(&state_update.trie) {
            let receipt = receipt_result?;
            let bytes = receipt_size(&receipt)?;
            let gas = receipt_congestion_gas(&receipt, &apply_state.config)?;
            match Self::try_forward(
                receipt,
                shard_id,
                &mut self.outgoing_limit,
                self.outgoing_receipts,
                apply_state,
            )? {
                ReceiptForwarding::Forwarded => {
                    self.congestion_info.remove_receipt_bytes(bytes as u64)?;
                    self.congestion_info.remove_buffered_receipt_gas(gas)?;
                    // count how many to release later to avoid modifying
                    // `state_update` while iterating based on
                    // `state_update.trie`.
                    num_forwarded += 1;
                }
                ReceiptForwarding::NotForwarded(_) => {
                    break;
                }
            }
        }
        self.outgoing_buffers.to_shard(shard_id).pop_n(state_update, num_forwarded)?;
        Ok(())
    }

    /// Put a receipt in the outgoing receipts vector (=forward) if the
    /// congestion preventing limits allow it. Put it in the buffered receipts
    /// queue otherwise.
    pub(crate) fn forward_or_buffer_receipt(
        &mut self,
        receipt: Receipt,
        apply_state: &ApplyState,
        state_update: &mut TrieUpdate,
        epoch_info_provider: &dyn EpochInfoProvider,
    ) -> Result<(), RuntimeError> {
        let shard = epoch_info_provider
            .account_id_to_shard_id(&receipt.receiver_id, &apply_state.epoch_id)?;
        if shard == apply_state.shard_id {
            // No limits on receipts that stay on the same shard. Backpressure
            // wouldn't help, the receipt takes the same memory if buffered or
            // in the delayed receipts queue.
            self.outgoing_receipts.push(receipt);
            return Ok(());
        }
        match Self::try_forward(
            receipt,
            shard,
            &mut self.outgoing_limit,
            self.outgoing_receipts,
            apply_state,
        )? {
            ReceiptForwarding::Forwarded => (),
            ReceiptForwarding::NotForwarded(receipt) => {
                self.buffer_receipt(&receipt, state_update, shard, &apply_state.config)?;
            }
        }
        Ok(())
    }

    /// Forward a receipt if possible and return whether it was forwarded or
    /// not.
    ///
    /// This does not take `&mut self` as first argument to make lifetime
    /// management easier. Instead it takes exactly the fields it requires,
    /// namely `outgoing_limit` and `outgoing_receipt`.
    fn try_forward(
        receipt: Receipt,
        shard: ShardId,
        outgoing_limit: &mut HashMap<ShardId, Gas>,
        outgoing_receipts: &mut Vec<Receipt>,
        apply_state: &ApplyState,
    ) -> Result<ReceiptForwarding, RuntimeError> {
        // Default case set to `Gas::MAX`: If no outgoing limit was defined for the receiving
        // shard, this usually just means the feature is not enabled. Or, it
        // could be a special case during resharding events. Or even a bug. In
        // any case, if we cannot know a limit, treating it as literally "no
        // limit" is the safest approach to ensure availability.
        let forward_limit = outgoing_limit.entry(shard).or_insert(Gas::MAX);
        let gas_to_forward = receipt_congestion_gas(&receipt, &apply_state.config)?;
        if *forward_limit > gas_to_forward {
            outgoing_receipts.push(receipt);
            // underflow impossible: checked forward_limit > gas_to_forward above
            *forward_limit -= gas_to_forward;
            Ok(ReceiptForwarding::Forwarded)
        } else {
            Ok(ReceiptForwarding::NotForwarded(receipt))
        }
    }

    /// Put a receipt in the outgoing receipt buffer of a shard.
    fn buffer_receipt(
        &mut self,
        receipt: &Receipt,
        state_update: &mut TrieUpdate,
        shard: u64,
        config: &RuntimeConfig,
    ) -> Result<(), RuntimeError> {
        let bytes = receipt_size(&receipt)?;
        let gas = receipt_congestion_gas(&receipt, config)?;
        self.congestion_info.add_receipt_bytes(bytes as u64)?;
        self.congestion_info.add_buffered_receipt_gas(gas)?;
        self.outgoing_buffers.to_shard(shard).push(state_update, &receipt)?;
        Ok(())
    }
}

fn receipt_congestion_gas(
    receipt: &Receipt,
    config: &RuntimeConfig,
) -> Result<Gas, IntegerOverflowError> {
    match &receipt.receipt {
        ReceiptEnum::Action(action_receipt) => {
            // account for gas guaranteed to be used for executing the receipts
            let prepaid_exec_gas = safe_add_gas(
                total_prepaid_exec_fees(config, &action_receipt.actions, &receipt.receiver_id)?,
                config.fees.fee(ActionCosts::new_action_receipt).exec_fee(),
            )?;
            // account for gas guaranteed to be used for creating new receipts
            let prepaid_send_gas = total_prepaid_send_fees(config, &action_receipt.actions)?;
            let prepaid_gas = safe_add_gas(prepaid_exec_gas, prepaid_send_gas)?;

            // account for gas potentially used for dynamic execution
            let gas_attached_to_fns = total_prepaid_gas(&action_receipt.actions)?;
            let gas = safe_add_gas(gas_attached_to_fns, prepaid_gas)?;

            Ok(gas)
        }
        ReceiptEnum::Data(_data_receipt) => {
            // Data receipts themselves don't cost gas to execute, their cost is
            // burnt at creation. What we should count, is the gas of the
            // postponed action receipt. But looking that up would require
            // reading the postponed receipt from the trie.
            // Thus, the congestion control MVP does not account for data
            // receipts or postponed receipts.
            Ok(0)
        }
        ReceiptEnum::PromiseYield(_) => {
            // The congestion control MVP does not account for yielding a
            // promise. Yielded promises are confined to a single account, hence
            // they never cross the shard boundaries. This makes it irrelevant
            // for the congestion MVP, which only counts gas in the outgoing
            // buffers and delayed receipts queue.
            tracing::error!(target: "congestion_control", "Attempting to calculate congestion gas for a `PromiseYield`.");
            Ok(0)
        }
        ReceiptEnum::PromiseResume(_) => {
            // The congestion control MVP does not account for resuming a promise.
            // Unlike `PromiseYield`, it is possible that a promise-resume ends
            // up in the delayed receipts queue.
            // But similar to a data receipt, it would be difficult to find the cost
            // of it without expensive state lookups.
            Ok(0)
        }
    }
}

/// Iterate all columns in the trie holding unprocessed receipts and
/// computes the storage consumption as well as attached gas.
///
/// This is an IO intensive operation! Only do it to bootstrap the
/// `CongestionInfo`. In normal operation, this information is kept up
/// to date and passed from chunk to chunk through chunk header fields.
pub fn compute_congestion_info(
    trie: &dyn near_store::TrieAccess,
    config: &RuntimeConfig,
    shard_id: ShardId,
    other_shard_ids: &[ShardId],
    congestion_seed: u64,
) -> Result<CongestionInfo, StorageError> {
    let mut receipt_bytes: u64 = 0;
    let mut delayed_receipts_gas: u128 = 0;
    let mut buffered_receipts_gas: u128 = 0;

    let delayed_receipt_queue = &DelayedReceiptQueue::load(trie)?;
    for receipt_result in delayed_receipt_queue.iter(trie) {
        let receipt = receipt_result?;
        let gas = receipt_congestion_gas(&receipt, config).map_err(int_overflow_to_storage_err)?;
        delayed_receipts_gas =
            safe_add_gas_to_u128(delayed_receipts_gas, gas).map_err(int_overflow_to_storage_err)?;

        let memory = receipt_size(&receipt).map_err(int_overflow_to_storage_err)? as u64;
        receipt_bytes = receipt_bytes.checked_add(memory).ok_or_else(overflow_storage_err)?;
    }

    let mut outgoing_buffers = ShardsOutgoingReceiptBuffer::load(trie)?;
    for shard in outgoing_buffers.shards() {
        for receipt_result in outgoing_buffers.to_shard(shard).iter(trie) {
            let receipt = receipt_result?;
            let gas =
                receipt_congestion_gas(&receipt, config).map_err(int_overflow_to_storage_err)?;
            buffered_receipts_gas = safe_add_gas_to_u128(buffered_receipts_gas, gas)
                .map_err(int_overflow_to_storage_err)?;
            let memory = receipt_size(&receipt).map_err(int_overflow_to_storage_err)? as u64;
            receipt_bytes = receipt_bytes.checked_add(memory).ok_or_else(overflow_storage_err)?;
        }
    }

    let mut congestion_info = CongestionInfo::V1(CongestionInfoV1 {
        delayed_receipts_gas: delayed_receipts_gas as u128,
        buffered_receipts_gas: buffered_receipts_gas as u128,
        receipt_bytes,
        // will be overwritten in a moment
        allowed_shard: 0,
    });
    congestion_info.finalize_allowed_shard(shard_id, other_shard_ids, congestion_seed);

    Ok(congestion_info)
}

fn receipt_size(receipt: &Receipt) -> Result<usize, IntegerOverflowError> {
    // `borsh::object_length` may only fail when the total size overflows u32
    borsh::object_length(&receipt).map_err(|_| IntegerOverflowError)
}

fn int_overflow_to_storage_err(_err: IntegerOverflowError) -> StorageError {
    overflow_storage_err()
}

fn overflow_storage_err() -> StorageError {
    StorageError::StorageInconsistentState(
        "Calculations on stored receipt overflows calculations".to_owned(),
    )
}

// we use u128 for accumulated gas because congestion may deal with a lot of gas
fn safe_add_gas_to_u128(a: u128, b: Gas) -> Result<u128, IntegerOverflowError> {
    a.checked_add(b as u128).ok_or(IntegerOverflowError {})
}

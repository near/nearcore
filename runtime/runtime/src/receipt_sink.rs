use super::{receipt_congestion_gas, receipt_size};
use crate::ApplyState;
use near_parameters::RuntimeConfig;
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::errors::RuntimeError;
use near_primitives::receipt::{BufferedReceiptIndices, Receipt};
use near_primitives::types::{EpochInfoProvider, Gas, ShardId};
use near_store::remove_buffered_receipt;
use near_store::{
    push_buffered_receipt, trie::receipts_column_helper::ReceiptIterator, TrieUpdate,
};
use std::collections::HashMap;

/// A helper struct to buffer or forward receipts.
///
/// When the runtime produces receipts for other shards, a `ReceiptSink` accept
/// them and either puts them in the outgoing receipts list, or puts them in a
/// buffer which is stored in the trie.
///
/// This is for congestion control, allowing to apply backpressure from the
/// receiving shard and stopping us from sending more receipts to it than its
/// nodes can keep in memory.
pub(crate) struct ReceiptSink<'a> {
    pub(crate) congestion_info: &'a mut CongestionInfo,
    pub(crate) outgoing_limit: &'a mut HashMap<ShardId, Gas>,
    pub(crate) buffered_receipts_indices: &'a mut BufferedReceiptIndices,
    pub(crate) outgoing_receipts: &'a mut Vec<Receipt>,
}

enum ReceiptForwarding {
    Forwarded,
    NotForwarded(Receipt),
}

impl ReceiptSink<'_> {
    /// Forward receipts already in the buffer to the outgoing receipts vector, as
    /// much as the gas limits allow.
    pub(crate) fn forward_from_buffer(
        &mut self,
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
    ) -> Result<(), RuntimeError> {
        for (&shard, buffer_indices) in &mut self.buffered_receipts_indices.shard_buffer_indices {
            let index_before = buffer_indices.first_index;
            for receipt_result in
                ReceiptIterator::shard_buffered_receipts(&state_update.trie, shard, buffer_indices)?
            {
                let receipt = receipt_result?;
                let bytes = receipt_size(&receipt)?;
                let gas = receipt_congestion_gas(&receipt, &apply_state.config)?;
                match Self::try_forward(
                    receipt,
                    shard,
                    self.outgoing_limit,
                    self.outgoing_receipts,
                    apply_state,
                )? {
                    ReceiptForwarding::Forwarded => {
                        self.congestion_info.receipt_bytes = self
                            .congestion_info
                            .receipt_bytes
                            .checked_sub(bytes as u64)
                            .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?;
                        self.congestion_info.buffered_receipts_gas = self
                            .congestion_info
                            .buffered_receipts_gas
                            .checked_sub(gas as u128)
                            .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?;
                        buffer_indices.first_index = buffer_indices
                            .first_index
                            .checked_add(1)
                            .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?;
                    }
                    ReceiptForwarding::NotForwarded(_) => {
                        break;
                    }
                }
            }
            // removing receipts from state here to avoid double borrow of `state_update`
            for index in index_before..buffer_indices.first_index {
                remove_buffered_receipt(state_update, index, shard);
            }
        }
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
            self.outgoing_limit,
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
        self.congestion_info.receipt_bytes = self
            .congestion_info
            .receipt_bytes
            .checked_add(bytes as u64)
            .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?;
        self.congestion_info.buffered_receipts_gas = self
            .congestion_info
            .buffered_receipts_gas
            .checked_add(gas as u128)
            .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?;
        push_buffered_receipt(
            state_update,
            self.buffered_receipts_indices.shard_buffer_indices.entry(shard).or_default(),
            &receipt,
            shard,
        )?;
        Ok(())
    }
}

use crate::ApplyState;
use crate::bandwidth_scheduler::BandwidthSchedulerOutput;
use crate::config::{
    safe_add_gas, total_prepaid_exec_fees, total_prepaid_gas, total_prepaid_send_fees,
};
use bytesize::ByteSize;
use itertools::Itertools;
use near_parameters::{ActionCosts, RuntimeConfig};
use near_primitives::bandwidth_scheduler::{
    BandwidthRequest, BandwidthRequests, BandwidthRequestsV1, BandwidthSchedulerParams,
};
use near_primitives::chunk_apply_stats::{ChunkApplyStatsV0, ReceiptSinkStats, ReceiptsStats};
use near_primitives::congestion_info::{CongestionControl, CongestionInfo, CongestionInfoV1};
use near_primitives::errors::{EpochError, IntegerOverflowError, RuntimeError};
use near_primitives::receipt::{
    Receipt, ReceiptEnum, ReceiptOrStateStoredReceipt, StateStoredReceipt,
    StateStoredReceiptMetadata,
};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{EpochId, EpochInfoProvider, Gas, ShardId};
use near_primitives::version::ProtocolFeature;
use near_store::trie::outgoing_metadata::{OutgoingMetadatas, ReceiptGroupsConfig};
use near_store::trie::receipts_column_helper::{
    DelayedReceiptQueue, ShardsOutgoingReceiptBuffer, TrieQueue,
};
use near_store::{StorageError, TrieAccess, TrieUpdate};
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};

pub enum ReceiptSink {
    V2(ReceiptSinkV2),
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
pub struct ReceiptSinkV2 {
    /// Keeps track of the local shard's congestion info while adding and
    /// removing buffered or delayed receipts. At the end of applying receipts,
    /// it will be a field in the [`ApplyResult`]. For this chunk, it is not
    /// used to make forwarding decisions.
    pub(crate) own_congestion_info: CongestionInfo,
    pub(crate) outgoing_receipts: Vec<Receipt>,
    pub(crate) outgoing_limit: HashMap<ShardId, OutgoingLimit>,
    pub(crate) outgoing_buffers: ShardsOutgoingReceiptBuffer,
    pub(crate) outgoing_metadatas: OutgoingMetadatas,
    pub(crate) bandwidth_scheduler_output: BandwidthSchedulerOutput,
    pub(crate) stats: ReceiptSinkStats,
}

/// Limits for outgoing receipts to a shard.
/// Receipts are sent out until the limit is hit, after that they're buffered.
pub(crate) struct OutgoingLimit {
    pub gas: Gas,
    pub size: u64,
}

#[allow(clippy::large_enum_variant)]
enum ReceiptForwarding {
    Forwarded,
    NotForwarded(Receipt),
}

impl ReceiptSink {
    pub(crate) fn new(
        trie: &dyn TrieAccess,
        apply_state: &ApplyState,
        prev_own_congestion_info: CongestionInfo,
        bandwidth_scheduler_output: BandwidthSchedulerOutput,
    ) -> Result<Self, StorageError> {
        let outgoing_buffers = ShardsOutgoingReceiptBuffer::load(trie)?;

        let outgoing_limit: HashMap<ShardId, OutgoingLimit> = apply_state
            .congestion_info
            .iter()
            .map(|(&shard_id, congestion)| {
                let other_congestion_control = CongestionControl::new(
                    apply_state.config.congestion_control_config,
                    congestion.congestion_info,
                    congestion.missed_chunks_count,
                );
                let gas_limit = if shard_id != apply_state.shard_id {
                    other_congestion_control.outgoing_gas_limit(apply_state.shard_id)
                } else {
                    // No gas limits on receipts that stay on the same shard. Backpressure
                    // wouldn't help, the receipt takes the same memory if buffered or
                    // in the delayed receipts queue.
                    Gas::MAX
                };

                let size_limit = bandwidth_scheduler_output
                    .granted_bandwidth
                    .get_granted_bandwidth(apply_state.shard_id, shard_id);

                (shard_id, OutgoingLimit { gas: gas_limit, size: size_limit })
            })
            .collect();

        let outgoing_metadatas = OutgoingMetadatas::load(
            trie,
            outgoing_buffers.shards(),
            ReceiptGroupsConfig::default_config(),
        )?;

        let mut stats = ReceiptSinkStats::default();
        stats.set_outgoing_limits(
            outgoing_limit.iter().map(|(shard_id, limit)| (*shard_id, (limit.size, limit.gas))),
        );

        Ok(ReceiptSink::V2(ReceiptSinkV2 {
            own_congestion_info: prev_own_congestion_info,
            outgoing_receipts: Vec::new(),
            outgoing_limit,
            outgoing_buffers,
            outgoing_metadatas,
            bandwidth_scheduler_output,
            stats,
        }))
    }

    /// Forward receipts already in the buffer to the outgoing receipts vector, as
    /// much as the gas limits allow.
    pub(crate) fn forward_from_buffer(
        &mut self,
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
        epoch_info_provider: &dyn EpochInfoProvider,
    ) -> Result<(), RuntimeError> {
        match self {
            ReceiptSink::V2(inner) => {
                inner.forward_from_buffer(state_update, apply_state, epoch_info_provider)
            }
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
            ReceiptSink::V2(inner) => inner.forward_or_buffer_receipt(
                receipt,
                apply_state,
                state_update,
                epoch_info_provider,
            ),
        }
    }

    /// Consumes receipt sink, finalizes ReceiptSinkStats and returns the outgoing receipts.
    /// Called at the end of chunk application.
    pub(crate) fn finalize_stats_get_outgoing_receipts(
        self,
        stats: &mut ReceiptSinkStats,
    ) -> Vec<Receipt> {
        match self {
            ReceiptSink::V2(mut inner) => {
                inner.record_outgoing_buffer_stats();
                *stats = inner.stats;
                inner.outgoing_receipts
            }
        }
    }

    pub(crate) fn own_congestion_info(&self) -> CongestionInfo {
        match self {
            ReceiptSink::V2(inner) => inner.own_congestion_info,
        }
    }

    pub(crate) fn bandwidth_scheduler_output(&self) -> &BandwidthSchedulerOutput {
        match self {
            ReceiptSink::V2(inner) => &inner.bandwidth_scheduler_output,
        }
    }

    /// Generate bandwidth requests based on the receipts stored in the outgoing buffers.
    pub(crate) fn generate_bandwidth_requests(
        &self,
        trie: &dyn TrieAccess,
        shard_layout: &ShardLayout,
        side_effects: bool,
        stats: &mut ChunkApplyStatsV0,
    ) -> Result<BandwidthRequests, StorageError> {
        match self {
            ReceiptSink::V2(inner) => {
                inner.generate_bandwidth_requests(trie, shard_layout, side_effects, stats)
            }
        }
    }
}

impl ReceiptSinkV2 {
    /// Forward receipts already in the buffer to the outgoing receipts vector, as
    /// much as the gas limits allow.
    pub(crate) fn forward_from_buffer(
        &mut self,
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
        epoch_info_provider: &dyn EpochInfoProvider,
    ) -> Result<(), RuntimeError> {
        tracing::debug!(target: "runtime", "forwarding receipts from outgoing buffers");

        let protocol_version = apply_state.current_protocol_version;
        let shard_layout = epoch_info_provider.shard_layout(&apply_state.epoch_id)?;
        let (shard_ids, parent_shard_ids) =
            if ProtocolFeature::SimpleNightshadeV4.enabled(protocol_version) {
                (
                    shard_layout.shard_ids().collect_vec(),
                    shard_layout.get_split_parent_shard_ids().map_err(Into::<EpochError>::into)?,
                )
            } else {
                (self.outgoing_limit.keys().copied().collect_vec(), BTreeSet::new())
            };

        // There mustn't be any shard ids in both the parents and the current
        // shard ids. If this happens the same buffer will be processed twice.
        debug_assert!(
            parent_shard_ids.intersection(&shard_ids.clone().into_iter().collect()).count() == 0
        );

        let mut all_buffers_empty = true;

        // First forward any receipts that may still be in the outgoing buffers
        // of the parent shards.
        for &shard_id in &parent_shard_ids {
            self.forward_from_buffer_to_shard(shard_id, state_update, apply_state, &shard_layout)?;
            let is_buffer_empty = self.outgoing_buffers.to_shard(shard_id).len() == 0;
            all_buffers_empty &= is_buffer_empty;
        }

        // Then forward receipts from the outgoing buffers of the shard in the
        // current shard layout.
        for &shard_id in &shard_ids {
            self.forward_from_buffer_to_shard(shard_id, state_update, apply_state, &shard_layout)?;
            let is_buffer_empty = self.outgoing_buffers.to_shard(shard_id).len() == 0;
            all_buffers_empty &= is_buffer_empty;
        }

        // Assert that empty buffers match zero buffered gas.
        if all_buffers_empty {
            assert_eq!(self.own_congestion_info.buffered_receipts_gas(), 0);
        }

        Ok(())
    }

    /// Forward receipts from the outgoing buffer of buffer_shard_id to the
    /// outgoing receipts as much as the limits allow.
    ///
    /// Please note that the buffer shard id may be different than the target
    /// shard if for a short period of time after resharding. That is because
    /// some shards may have receipts for the parent shard that no longer exists
    /// and those receipts need to be forwarded to either of the child shards.
    ///
    /// TODO(resharding) - remove the parent outgoing buffer once it's empty.
    fn forward_from_buffer_to_shard(
        &mut self,
        buffer_shard_id: ShardId,
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
        shard_layout: &ShardLayout,
    ) -> Result<(), RuntimeError> {
        let mut num_forwarded = 0;
        let mut outgoing_metadatas_updates: Vec<(ByteSize, Gas)> = Vec::new();
        for receipt_result in
            self.outgoing_buffers.to_shard(buffer_shard_id).iter(&state_update.trie, true)
        {
            let receipt = receipt_result?;
            let gas = receipt_congestion_gas(&receipt, &apply_state.config)?;
            let size = receipt_size(&receipt)?;
            let should_update_outgoing_metadatas = receipt.should_update_outgoing_metadatas();
            let receipt = receipt.into_receipt();
            let target_shard_id = receipt.receiver_shard_id(&shard_layout)?;

            match Self::try_forward(
                receipt,
                gas,
                size,
                target_shard_id,
                &mut self.outgoing_limit,
                &mut self.outgoing_receipts,
                apply_state,
                &mut self.stats,
            )? {
                ReceiptForwarding::Forwarded => {
                    self.own_congestion_info.remove_receipt_bytes(size)?;
                    self.own_congestion_info.remove_buffered_receipt_gas(gas.into())?;
                    if should_update_outgoing_metadatas {
                        // Can't update metadatas immediately because state_update is borrowed by iterator.
                        outgoing_metadatas_updates.push((ByteSize::b(size), gas));
                    }
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

        self.outgoing_buffers.to_shard(buffer_shard_id).pop_n(state_update, num_forwarded)?;
        for (size, gas) in outgoing_metadatas_updates {
            self.outgoing_metadatas.update_on_receipt_popped(
                buffer_shard_id,
                size,
                gas,
                state_update,
            )?;
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
        let shard_layout = epoch_info_provider.shard_layout(&apply_state.epoch_id)?;
        let shard = receipt.receiver_shard_id(&shard_layout)?;
        let size = compute_receipt_size(&receipt)?;
        let gas = compute_receipt_congestion_gas(&receipt, &apply_state.config)?;

        match Self::try_forward(
            receipt,
            gas,
            size,
            shard,
            &mut self.outgoing_limit,
            &mut self.outgoing_receipts,
            apply_state,
            &mut self.stats,
        )? {
            ReceiptForwarding::Forwarded => (),
            ReceiptForwarding::NotForwarded(receipt) => {
                self.buffer_receipt(
                    receipt,
                    size,
                    gas,
                    state_update,
                    shard,
                    apply_state.config.use_state_stored_receipt,
                )?;
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
        gas: u64,
        mut size: u64,
        shard: ShardId,
        outgoing_limit: &mut HashMap<ShardId, OutgoingLimit>,
        outgoing_receipts: &mut Vec<Receipt>,
        apply_state: &ApplyState,
        stats: &mut ReceiptSinkStats,
    ) -> Result<ReceiptForwarding, RuntimeError> {
        // There is a bug which allows to create receipts that are above the size limit. Receipts
        // above the size limit might not fit under the maximum outgoing size limit. Let's pretend
        // that all receipts are at most `max_receipt_size` to avoid receipts getting stuck.
        // See https://github.com/near/nearcore/issues/12606
        let max_receipt_size = apply_state.config.wasm_config.limit_config.max_receipt_size;
        if size > max_receipt_size {
            tracing::debug!(
                target: "runtime",
                receipt_id=?receipt.receipt_id(),
                size,
                max_receipt_size,
                "try_forward observed a receipt with size exceeding the size limit!",
            );
            size = max_receipt_size;
        }

        // Default case set to `Gas::MAX`: If no outgoing limit was defined for the receiving
        // shard, this usually just means the feature is not enabled. Or, it
        // could be a special case during resharding events. Or even a bug. In
        // any case, if we cannot know a limit, treating it as literally "no
        // limit" is the safest approach to ensure availability.
        let default_gas_limit = Gas::MAX;

        // Since bandwidth scheduler, a shard is not allowed to send any receipts if it doesn't have a grant.
        let default_size_limit = 0;

        let default_outgoing_limit =
            OutgoingLimit { gas: default_gas_limit, size: default_size_limit };
        let forward_limit = outgoing_limit.entry(shard).or_insert(default_outgoing_limit);

        if forward_limit.gas >= gas && forward_limit.size >= size {
            tracing::trace!(target: "runtime", ?shard, receipt_id=?receipt.receipt_id(), "forwarding buffered receipt");
            outgoing_receipts.push(receipt);
            // underflow impossible: checked forward_limit > gas/size_to_forward above
            forward_limit.gas -= gas;
            forward_limit.size -= size;
            stats.forwarded_receipts.entry(shard).or_default().add_receipt(size, gas);

            Ok(ReceiptForwarding::Forwarded)
        } else {
            tracing::trace!(target: "runtime", ?shard, receipt_id=?receipt.receipt_id(), "not forwarding buffered receipt");
            Ok(ReceiptForwarding::NotForwarded(receipt))
        }
    }

    /// Put a receipt in the outgoing receipt buffer of a shard.
    fn buffer_receipt(
        &mut self,
        receipt: Receipt,
        size: u64,
        gas: u64,
        state_update: &mut TrieUpdate,
        shard: ShardId,
        use_state_stored_receipt: bool,
    ) -> Result<(), RuntimeError> {
        let receipt = match use_state_stored_receipt {
            true => {
                let metadata =
                    StateStoredReceiptMetadata { congestion_gas: gas, congestion_size: size };
                let receipt = StateStoredReceipt::new_owned(receipt, metadata);
                let receipt = ReceiptOrStateStoredReceipt::StateStoredReceipt(receipt);
                receipt
            }
            false => ReceiptOrStateStoredReceipt::Receipt(std::borrow::Cow::Owned(receipt)),
        };

        self.own_congestion_info.add_receipt_bytes(size)?;
        self.own_congestion_info.add_buffered_receipt_gas(gas)?;

        if receipt.should_update_outgoing_metadatas() {
            self.outgoing_metadatas.update_on_receipt_pushed(
                shard,
                ByteSize::b(size),
                gas,
                state_update,
            )?;
        }

        self.outgoing_buffers.to_shard(shard).push_back(state_update, &receipt)?;
        self.stats.buffered_receipts.entry(shard).or_default().add_receipt(size, gas);
        Ok(())
    }

    fn generate_bandwidth_requests(
        &self,
        trie: &dyn TrieAccess,
        shard_layout: &ShardLayout,
        side_effects: bool,
        stats: &mut ChunkApplyStatsV0,
    ) -> Result<BandwidthRequests, StorageError> {
        let params = &self.bandwidth_scheduler_output.params;

        let mut requests = Vec::new();
        for shard_id in shard_layout.shard_ids() {
            if let Some(request) =
                self.generate_bandwidth_request(shard_id, trie, shard_layout, side_effects, params)?
            {
                requests.push(request);
            }
        }

        let bandwidth_requests = BandwidthRequests::V1(BandwidthRequestsV1 { requests });
        stats.set_new_bandwidth_requests(&bandwidth_requests, &params);
        Ok(bandwidth_requests)
    }

    fn generate_bandwidth_request(
        &self,
        to_shard: ShardId,
        trie: &dyn TrieAccess,
        shard_layout: &ShardLayout,
        side_effects: bool,
        params: &BandwidthSchedulerParams,
    ) -> Result<Option<BandwidthRequest>, StorageError> {
        // Get (group) sizes of receipts stored in outgoing buffer to the shard.
        let mut receipt_sizes_iter =
            self.get_receipt_group_sizes_for_buffer_to_shard(to_shard, trie, side_effects, params);

        // When making a bandwidth request to a child shard which has been split from a parent
        // shard, we have to include the receipts stored in the outgoing buffer to the parent shard
        // in the bandwidth request for sending receipts to the child shard. Forwarding receipts
        // from the buffer to parent uses bandwidth granted for sending receipts to one of the
        // children. Not including the parent receipts in the bandwidth request could lead to a
        // situation where a receipt can't be sent because the grant for sending receipts to a child
        // is too small to send out a receipt from a buffer aimed at a parent.
        if let Ok(parent_shard_id) = shard_layout.get_parent_shard_id(to_shard) {
            let parent_receipt_sizes_iter = self.get_receipt_group_sizes_for_buffer_to_shard(
                parent_shard_id,
                trie,
                side_effects,
                params,
            );

            receipt_sizes_iter = Box::new(parent_receipt_sizes_iter.chain(receipt_sizes_iter));
        }

        // There's a bug which allows to create receipts above `max_receipt_size` (https://github.com/near/nearcore/issues/12606).
        // This could cause problems with bandwidth scheduler which would generate requests for size above max size, and these
        // requests would never be fulfilled. For bandwidth requests let's pretend that all sizes are below `max_receipt_size`.
        // The same pretending logic is also present in `try_forward` which compares receipt size with outgoing limit.
        // This logic should also make it possible to do protocol upgrades that lower `max_receipt_size` without too much trouble.
        let sizes_iter = receipt_sizes_iter
            .map_ok(|group_size| std::cmp::min(group_size, params.max_receipt_size));

        // Create the bandwidth request based on buffered receipt (group) sizes
        BandwidthRequest::make_from_receipt_sizes(to_shard, sizes_iter, params)
    }

    /// Get iterator over receipt group sizes for receipts stored in the outgoing buffer to some shard.
    /// If outgoing buffer metadata isn't fully initialized yet, returns an iterator where the only
    /// item is `max_receipt_size`.
    fn get_receipt_group_sizes_for_buffer_to_shard<'a>(
        &'a self,
        to_shard: ShardId,
        trie: &'a dyn TrieAccess,
        side_effects: bool,
        params: &BandwidthSchedulerParams,
    ) -> Box<dyn Iterator<Item = Result<u64, StorageError>> + 'a> {
        let outgoing_receipts_buffer_len = self.outgoing_buffers.buffer_len(to_shard).unwrap_or(0);

        if outgoing_receipts_buffer_len == 0 {
            // No receipts in the outgoing buffer, return an empty iterator.
            return Box::new(std::iter::empty());
        }

        // To make a proper bandwidth request we need the metadata for the outgoing buffer to be fully initialized
        // (i.e. contain data about all of the receipts in the outgoing buffer). There is a moment right after the
        // protocol upgrade where the outgoing buffer contains receipts which were buffered in the previous protocol
        // version where metadata was not enabled. Metadata doesn't contain information about them.
        // We can't make a proper request in this case, so we make a basic request while we wait for
        // metadata to become fully initialized. The basic request requests just `max_receipt_size`. This is enough to
        // ensure liveness, as all receipts are smaller than `max_receipt_size`. The resulting behavior is similar
        // to the previous approach where the `allowed_shard` was assigned most of the bandwidth.
        // Over time these old receipts will be removed from the outgoing buffer and eventually metadata will contain
        // information about every receipt in the buffer. From that point on we will be able to make
        // proper bandwidth requests.

        match self.outgoing_metadatas.get_metadata_for_shard(&to_shard) {
            Some(metadata) if metadata.total_receipts_num() == outgoing_receipts_buffer_len => {
                // Metadata fully initialized, use it to read receipt group sizes.
                Box::new(metadata.iter_receipt_group_sizes(trie, side_effects))
            }
            _ => {
                // Metadata not initialized. Make a basic request which requests only `max_receipt_size`.
                Box::new([Ok(params.max_receipt_size)].into_iter())
            }
        }
    }

    /// Record information about the outgoing buffer in ReceiptSinkStats.
    fn record_outgoing_buffer_stats(&mut self) {
        for shard in self.outgoing_buffers.shards() {
            let buffer = self.outgoing_buffers.to_shard(shard);

            if buffer.len() == 0 {
                self.stats
                    .final_outgoing_buffers
                    .insert(shard, ReceiptsStats { num: 0, total_size: 0, total_gas: 0 });
                self.stats.is_outgoing_metadata_ready.insert(shard, true);
                continue;
            }

            // If the outgoing buffer metadata is fully initialized, record the total size and gas
            // of the receipts in the buffer. Otherwise, record the number of receipts in the buffer
            // and set the total size and gas to 0. See
            // `get_receipt_group_sizes_for_buffer_to_shard` for more information about the metadata
            // initialization.
            match self.outgoing_metadatas.get_metadata_for_shard(&shard) {
                Some(metadata) if metadata.total_receipts_num() == buffer.len() => {
                    self.stats.final_outgoing_buffers.insert(
                        shard,
                        ReceiptsStats {
                            num: buffer.len(),
                            total_size: metadata.total_size(),
                            total_gas: metadata.total_gas(),
                        },
                    );
                    self.stats.is_outgoing_metadata_ready.insert(shard, true);
                }
                _ => {
                    self.stats.final_outgoing_buffers.insert(
                        shard,
                        ReceiptsStats { num: buffer.len(), total_size: 0, total_gas: 0 },
                    );
                    self.stats.is_outgoing_metadata_ready.insert(shard, false);
                }
            }
        }

        self.stats.all_outgoing_metadatas_ready =
            self.stats.is_outgoing_metadata_ready.values().all(|&ready| ready);
    }
}

/// Get the receipt gas from the receipt that was retrieved from the state.
/// If it is a [Receipt], the gas will be computed.
/// If it s the [StateStoredReceipt], the size will be read from the metadata.
pub(crate) fn receipt_congestion_gas(
    receipt: &ReceiptOrStateStoredReceipt,
    config: &RuntimeConfig,
) -> Result<Gas, IntegerOverflowError> {
    match receipt {
        ReceiptOrStateStoredReceipt::Receipt(receipt) => {
            compute_receipt_congestion_gas(receipt, config)
        }
        ReceiptOrStateStoredReceipt::StateStoredReceipt(receipt) => {
            Ok(receipt.metadata().congestion_gas)
        }
    }
}

/// Calculate the gas of a receipt before it is pushed into a state queue or
/// buffer. Please note that this method should only be used when storing
/// receipts into state. It should not be used for retrieving receipts from the
/// state.
///
/// The calculation is part of protocol and should only be modified with a
/// protocol upgrade.
pub(crate) fn compute_receipt_congestion_gas(
    receipt: &Receipt,
    config: &RuntimeConfig,
) -> Result<u64, IntegerOverflowError> {
    match receipt.receipt() {
        ReceiptEnum::Action(action_receipt) => {
            // account for gas guaranteed to be used for executing the receipts
            let prepaid_exec_gas = safe_add_gas(
                total_prepaid_exec_fees(config, &action_receipt.actions, receipt.receiver_id())?,
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
        ReceiptEnum::GlobalContractDistribution(_) => Ok(0),
    }
}

/// Iterate all columns in the trie holding unprocessed receipts and
/// computes the storage consumption as well as attached gas.
///
/// This is an IO intensive operation! Only do it to bootstrap the
/// `CongestionInfo`. In normal operation, this information is kept up
/// to date and passed from chunk to chunk through chunk header fields.
pub fn bootstrap_congestion_info(
    trie: &dyn near_store::TrieAccess,
    config: &RuntimeConfig,
    shard_id: ShardId,
) -> Result<CongestionInfo, StorageError> {
    let mut receipt_bytes: u64 = 0;
    let mut delayed_receipts_gas: u128 = 0;
    let mut buffered_receipts_gas: u128 = 0;

    let delayed_receipt_queue = &DelayedReceiptQueue::load(trie)?;
    for receipt_result in delayed_receipt_queue.iter(trie, true) {
        let receipt = receipt_result?;
        let gas = receipt_congestion_gas(&receipt, config).map_err(int_overflow_to_storage_err)?;
        delayed_receipts_gas =
            safe_add_gas_to_u128(delayed_receipts_gas, gas).map_err(int_overflow_to_storage_err)?;

        let memory = receipt_size(&receipt).map_err(int_overflow_to_storage_err)? as u64;
        receipt_bytes = receipt_bytes.checked_add(memory).ok_or_else(overflow_storage_err)?;
    }

    let mut outgoing_buffers = ShardsOutgoingReceiptBuffer::load(trie)?;
    for shard in outgoing_buffers.shards() {
        for receipt_result in outgoing_buffers.to_shard(shard).iter(trie, true) {
            let receipt = receipt_result?;
            let gas =
                receipt_congestion_gas(&receipt, config).map_err(int_overflow_to_storage_err)?;
            buffered_receipts_gas = safe_add_gas_to_u128(buffered_receipts_gas, gas)
                .map_err(int_overflow_to_storage_err)?;
            let memory = receipt_size(&receipt).map_err(int_overflow_to_storage_err)? as u64;
            receipt_bytes = receipt_bytes.checked_add(memory).ok_or_else(overflow_storage_err)?;
        }
    }

    Ok(CongestionInfo::V1(CongestionInfoV1 {
        delayed_receipts_gas: delayed_receipts_gas as u128,
        buffered_receipts_gas: buffered_receipts_gas as u128,
        receipt_bytes,
        // For the first chunk, set this to the own id.
        // This allows bootstrapping without knowing all other shards.
        // It is also irrelevant, since the bootstrapped value is only used at
        // the start of applying a chunk on this shard. Other shards will only
        // see and act on the first congestion info after that.
        allowed_shard: shard_id.into(),
    }))
}

/// A wrapper around `DelayedReceiptQueue` to accumulate changes in gas and
/// bytes.
///
/// This struct exists for two reasons. One, to encapsulate the accounting of
/// gas and bytes in functions that can be called in all necessary places. Two,
/// to accumulate changes and only apply them to `CongestionInfo` in the end,
/// which avoids problems with multiple mutable borrows with the closure
/// approach we currently have in receipt processing code.
///
/// We use positive added and removed values to avoid integer conversions with
/// the associated additional overflow conditions.
pub(crate) struct DelayedReceiptQueueWrapper<'a> {
    // The delayed receipt queue.
    queue: DelayedReceiptQueue,

    // Epoch_info_provider, shard_id, and epoch_id are used to determine
    // if a receipt belongs to the current shard or not after a resharding event.
    epoch_info_provider: &'a dyn EpochInfoProvider,
    shard_id: ShardId,
    epoch_id: EpochId,

    // Accumulated changes in gas and bytes for congestion info calculations.
    new_delayed_gas: Gas,
    new_delayed_bytes: u64,
    removed_delayed_gas: Gas,
    removed_delayed_bytes: u64,
}

impl<'a> DelayedReceiptQueueWrapper<'a> {
    pub fn new(
        queue: DelayedReceiptQueue,
        epoch_info_provider: &'a dyn EpochInfoProvider,
        shard_id: ShardId,
        epoch_id: EpochId,
    ) -> Self {
        Self {
            queue,
            epoch_info_provider,
            shard_id,
            epoch_id,
            new_delayed_gas: 0,
            new_delayed_bytes: 0,
            removed_delayed_gas: 0,
            removed_delayed_bytes: 0,
        }
    }

    pub(crate) fn push(
        &mut self,
        trie_update: &mut TrieUpdate,
        receipt: &Receipt,
        apply_state: &ApplyState,
    ) -> Result<(), RuntimeError> {
        let config = &apply_state.config;

        let gas = compute_receipt_congestion_gas(&receipt, &config)?;
        let size = compute_receipt_size(&receipt)? as u64;

        // TODO It would be great to have this method take owned Receipt and
        // get rid of the Cow from the Receipt and StateStoredReceipt.
        let receipt = match config.use_state_stored_receipt {
            true => {
                let metadata =
                    StateStoredReceiptMetadata { congestion_gas: gas, congestion_size: size };
                let receipt = StateStoredReceipt::new_borrowed(receipt, metadata);
                ReceiptOrStateStoredReceipt::StateStoredReceipt(receipt)
            }
            false => ReceiptOrStateStoredReceipt::Receipt(Cow::Borrowed(receipt)),
        };

        self.new_delayed_gas = safe_add_gas(self.new_delayed_gas, gas)?;
        self.new_delayed_bytes = safe_add_gas(self.new_delayed_bytes, size)?;
        self.queue.push_back(trie_update, &receipt)?;
        Ok(())
    }

    // With ReshardingV3, it's possible for a chunk to have delayed receipts that technically
    // belong to the sibling shard before a resharding event.
    // Here, we filter all the receipts that don't belong to the current shard_id.
    //
    // The function follows the guidelines of standard iterator filter function
    // We return true if we should retain the receipt and false if we should filter it.
    fn receipt_filter_fn(&self, receipt: &ReceiptOrStateStoredReceipt) -> bool {
        let shard_layout = self.epoch_info_provider.shard_layout(&self.epoch_id).unwrap();
        let receipt_shard_id = receipt.get_receipt().receiver_shard_id(&shard_layout).unwrap();
        receipt_shard_id == self.shard_id
    }

    pub(crate) fn pop(
        &mut self,
        trie_update: &mut TrieUpdate,
        config: &RuntimeConfig,
    ) -> Result<Option<ReceiptOrStateStoredReceipt>, RuntimeError> {
        // While processing receipts, we need to keep track of the gas and bytes
        // even for receipts that may be filtered out due to a resharding event
        loop {
            // Check proof size limit before each receipt is popped.
            if trie_update.trie.check_proof_size_limit_exceed() {
                break;
            }
            let Some(receipt) = self.queue.pop_front(trie_update)? else {
                break;
            };
            let delayed_gas = receipt_congestion_gas(&receipt, &config)?;
            let delayed_bytes = receipt_size(&receipt)? as u64;
            self.removed_delayed_gas = safe_add_gas(self.removed_delayed_gas, delayed_gas)?;
            self.removed_delayed_bytes = safe_add_gas(self.removed_delayed_bytes, delayed_bytes)?;

            // Track gas and bytes for receipt above and return only receipt that belong to the shard.
            if self.receipt_filter_fn(&receipt) {
                return Ok(Some(receipt));
            }
        }
        Ok(None)
    }

    pub(crate) fn peek_iter(
        &'a self,
        trie_update: &'a TrieUpdate,
    ) -> impl Iterator<Item = ReceiptOrStateStoredReceipt<'static>> + 'a {
        self.queue
            .iter(trie_update, false)
            .map_while(Result::ok)
            .filter(|receipt| self.receipt_filter_fn(receipt))
    }

    /// This function returns the maximum length of the delayed receipt queue.
    /// The only time the real number of delayed receipts differ from the returned value is right
    /// after a resharding event. During resharding, we duplicate the delayed receipt queue across
    /// both child shards, which means it's possible that the child shards contain delayed receipts
    /// that don't belong to them.
    pub(crate) fn upper_bound_len(&self) -> u64 {
        self.queue.len()
    }

    pub(crate) fn apply_congestion_changes(
        self,
        congestion: &mut CongestionInfo,
    ) -> Result<(), RuntimeError> {
        congestion.add_delayed_receipt_gas(self.new_delayed_gas)?;
        congestion.remove_delayed_receipt_gas(self.removed_delayed_gas)?;
        congestion.add_receipt_bytes(self.new_delayed_bytes)?;
        congestion.remove_receipt_bytes(self.removed_delayed_bytes)?;
        Ok(())
    }
}

/// Get the receipt size from the receipt that was retrieved from the state.
/// If it is a [Receipt], the size will be computed.
/// If it s the [StateStoredReceipt], the size will be read from the metadata.
pub(crate) fn receipt_size(
    receipt: &ReceiptOrStateStoredReceipt,
) -> Result<u64, IntegerOverflowError> {
    match receipt {
        ReceiptOrStateStoredReceipt::Receipt(receipt) => compute_receipt_size(receipt),
        ReceiptOrStateStoredReceipt::StateStoredReceipt(receipt) => {
            Ok(receipt.metadata().congestion_size)
        }
    }
}

/// Calculate the size of a receipt before it is pushed into a state queue or
/// buffer. Please note that this method should only be used when storing
/// receipts into state. It should not be used for retrieving receipts from the
/// state.
///
/// The calculation is part of protocol and should only be modified with a
/// protocol upgrade.
pub(crate) fn compute_receipt_size(receipt: &Receipt) -> Result<u64, IntegerOverflowError> {
    let size = borsh::object_length(&receipt).map_err(|_| IntegerOverflowError)?;
    size.try_into().map_err(|_| IntegerOverflowError)
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

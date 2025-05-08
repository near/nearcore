use crate::types::{Block, BlockHeader, LatestKnown};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::Utc;
pub use latest_witnesses::LatestWitnessesInfo;
pub use merkle_proof::MerkleProofAccess;
use near_chain_primitives::error::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::Tip;
use near_primitives::chunk_apply_stats::{ChunkApplyStats, ChunkApplyStatsV0};
use near_primitives::errors::{EpochError, InvalidTxError};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{MerklePath, PartialMerkleTree};
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::{ShardLayout, ShardUId, get_block_shard_uid};
use near_primitives::sharding::{
    ChunkHash, EncodedShardChunk, PartialEncodedChunk, ReceiptProof, ShardChunk, StateSyncInfo,
};
use near_primitives::state_sync::StateSyncDumpProgress;
use near_primitives::stateless_validation::contract_distribution::ContractUpdates;
use near_primitives::stateless_validation::stored_chunk_state_transition_data::{
    StoredChunkStateTransitionData, StoredChunkStateTransitionDataV1,
};
use near_primitives::transaction::{
    ExecutionOutcomeWithId, ExecutionOutcomeWithIdAndProof, ExecutionOutcomeWithProof,
    SignedTransaction,
};
use near_primitives::trie_key::{TrieKey, trie_key_parsers};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{
    BlockHeight, BlockHeightDelta, EpochId, NumBlocks, ShardId, StateChanges, StateChangesExt,
    StateChangesKinds, StateChangesKindsExt, StateChangesRequest,
};
use near_primitives::utils::{
    get_block_shard_id, get_outcome_id_block_hash, get_outcome_id_block_hash_rev, index_to_bytes,
    to_timestamp,
};
use near_primitives::version::{ProtocolFeature, ProtocolVersion};
use near_primitives::views::LightClientBlockView;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::db::{STATE_SYNC_DUMP_KEY, StoreStatistics};
use near_store::{
    CHUNK_TAIL_KEY, DBCol, FINAL_HEAD_KEY, FORK_TAIL_KEY, HEAD_KEY, HEADER_HEAD_KEY,
    KeyForStateChanges, LARGEST_TARGET_HEIGHT_KEY, LATEST_KNOWN_KEY, PartialStorage, Store,
    StoreUpdate, TAIL_KEY, WrappedTrieChanges,
};
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::ops::Deref;
use std::sync::Arc;
use utils::check_transaction_validity_period;

mod latest_witnesses;
mod merkle_proof;
pub mod utils;

/// Filter receipts mode for incoming receipts collection.
pub enum ReceiptFilter {
    /// Leave receipts unchanged. Needed for receipt proof generation, because
    /// even if we filter some receipts out, they still must be proven based on
    /// the original incoming receipts set.
    All,
    /// Leave only receipts which receiver id belongs to the target shard.
    /// Is non-trivial when shard layout changes and receipt was sent before
    /// the change, so that target shard must be recomputed.
    TargetShard,
}

/// Accesses the chain store. Used to create atomic editable views that can be reverted.
pub trait ChainStoreAccess {
    /// Returns underlying chain store
    fn chain_store(&self) -> &ChainStore;
    /// Returns underlying store.
    fn store(&self) -> Store;
    /// The chain head.
    fn head(&self) -> Result<Tip, Error>;
    /// The chain Blocks Tail height.
    fn tail(&self) -> Result<BlockHeight, Error>;
    /// The chain Chunks Tail height.
    fn chunk_tail(&self) -> Result<BlockHeight, Error>;
    /// Tail height of the fork cleaning process.
    fn fork_tail(&self) -> Result<BlockHeight, Error>;
    /// Head of the header chain (not the same thing as head_header).
    fn header_head(&self) -> Result<Tip, Error>;
    /// Header of the block at the head of the block chain (not the same thing as header_head).
    fn head_header(&self) -> Result<BlockHeader, Error>;
    /// The chain final head. It is guaranteed to be monotonically increasing.
    fn final_head(&self) -> Result<Tip, Error>;
    /// Largest approval target height sent by us
    fn largest_target_height(&self) -> Result<BlockHeight, Error>;
    /// Get full block.
    fn get_block(&self, h: &CryptoHash) -> Result<Block, Error>;
    /// Get partial chunk.
    fn get_partial_chunk(&self, chunk_hash: &ChunkHash) -> Result<Arc<PartialEncodedChunk>, Error>;
    /// Does this full block exist?
    fn block_exists(&self, h: &CryptoHash) -> Result<bool, Error>;
    /// Does this chunk exist?
    fn chunk_exists(&self, h: &ChunkHash) -> Result<bool, Error>;
    /// Get previous header.
    fn get_previous_header(&self, header: &BlockHeader) -> Result<BlockHeader, Error>;
    /// Get chunk extra info for given block hash + shard id.
    fn get_chunk_extra(
        &self,
        block_hash: &CryptoHash,
        shard_uid: &ShardUId,
    ) -> Result<Arc<ChunkExtra>, Error>;
    fn get_chunk_apply_stats(
        &self,
        block_hash: &CryptoHash,
        shard_id: &ShardId,
    ) -> Result<Option<ChunkApplyStats>, Error>;
    /// Get block header.
    fn get_block_header(&self, h: &CryptoHash) -> Result<BlockHeader, Error>;
    /// Returns hash of the block on the main chain for given height.
    fn get_block_hash_by_height(&self, height: BlockHeight) -> Result<CryptoHash, Error>;
    /// Returns hash of the first available block after genesis.
    fn get_earliest_block_hash(&self) -> Result<Option<CryptoHash>, Error> {
        // To find the earliest available block we use the `tail` marker primarily
        // used by garbage collection system.
        // NOTE: `tail` is the block height at which we can say that there is
        // at most 1 block available in the range from the genesis height to
        // the tail. Thus, the strategy is to find the first block AFTER the tail
        // height, and use the `prev_hash` to get the reference to the earliest
        // block.
        // The earliest block can be the genesis block.
        let head_header_height = self.head_header()?.height();
        let tail = self.tail()?;

        // There is a corner case when there are no blocks after the tail, and
        // the tail is in fact the earliest block available on the chain.
        if let Ok(block_hash) = self.get_block_hash_by_height(tail) {
            return Ok(Some(block_hash));
        }
        for height in tail + 1..=head_header_height {
            if let Ok(block_hash) = self.get_block_hash_by_height(height) {
                let earliest_block_hash = *self.get_block_header(&block_hash)?.prev_hash();
                debug_assert!(matches!(self.block_exists(&earliest_block_hash), Ok(true)));
                return Ok(Some(earliest_block_hash));
            }
        }
        Ok(None)
    }
    /// Returns block header from the current chain for given height if present.
    fn get_block_header_by_height(&self, height: BlockHeight) -> Result<BlockHeader, Error> {
        let hash = self.get_block_hash_by_height(height)?;
        self.get_block_header(&hash)
    }
    fn get_next_block_hash(&self, hash: &CryptoHash) -> Result<CryptoHash, Error>;
    fn get_epoch_light_client_block(
        &self,
        hash: &CryptoHash,
    ) -> Result<Arc<LightClientBlockView>, Error>;
    /// Returns a number of references for Block with `block_hash`
    fn get_block_refcount(&self, block_hash: &CryptoHash) -> Result<u64, Error>;
    /// Returns resulting receipt for given block.
    fn get_outgoing_receipts(
        &self,
        hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Arc<Vec<Receipt>>, Error>;

    fn get_incoming_receipts(
        &self,
        hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Arc<Vec<ReceiptProof>>, Error>;

    fn get_blocks_to_catchup(&self, prev_hash: &CryptoHash) -> Result<Vec<CryptoHash>, Error>;

    /// Returns encoded chunk if it's invalid otherwise None.
    fn is_invalid_chunk(
        &self,
        chunk_hash: &ChunkHash,
    ) -> Result<Option<Arc<EncodedShardChunk>>, Error>;

    fn get_transaction(
        &self,
        tx_hash: &CryptoHash,
    ) -> Result<Option<Arc<SignedTransaction>>, Error>;

    /// Fetch a receipt by id, if it is stored in the store.
    ///
    /// Note that not _all_ receipts are persisted. Some receipts are ephemeral,
    /// get processed immediately after creation and don't even get to the
    /// database.
    fn get_receipt(&self, receipt_id: &CryptoHash) -> Result<Option<Arc<Receipt>>, Error>;

    fn get_genesis_height(&self) -> BlockHeight;

    fn get_block_merkle_tree(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Arc<PartialMerkleTree>, Error>;

    fn get_block_hash_from_ordinal(&self, block_ordinal: NumBlocks) -> Result<CryptoHash, Error>;

    fn is_height_processed(&self, height: BlockHeight) -> Result<bool, Error>;

    fn get_block_height(&self, hash: &CryptoHash) -> Result<BlockHeight, Error> {
        if hash == &CryptoHash::default() {
            Ok(self.get_genesis_height())
        } else {
            Ok(self.get_block_header(hash)?.height())
        }
    }

    /// Get epoch id of the last block with existing chunk for the given shard id.
    fn get_epoch_id_of_last_block_with_chunk(
        &self,
        epoch_manager: &dyn EpochManagerAdapter,
        hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<EpochId, Error> {
        let mut candidate_hash = *hash;
        let block_header = self.get_block_header(&candidate_hash)?;
        let shard_layout = epoch_manager.get_shard_layout(block_header.epoch_id())?;
        let mut shard_id = shard_id;
        let mut shard_index = shard_layout.get_shard_index(shard_id)?;
        loop {
            let block_header = self.get_block_header(&candidate_hash)?;
            if *block_header.chunk_mask().get(shard_index).ok_or(Error::InvalidShardId(shard_id))? {
                break Ok(*block_header.epoch_id());
            }
            candidate_hash = *block_header.prev_hash();
            (shard_id, shard_index) =
                epoch_manager.get_prev_shard_ids(&candidate_hash, vec![shard_id])?[0];
        }
    }

    fn get_current_epoch_sync_hash(&self, epoch_id: &EpochId) -> Result<Option<CryptoHash>, Error>;
}

/// Given a vector of receipts return only the receipts that should be assigned
/// to the target shard id in the target shard layout. Used when collecting the
/// incoming receipts and the shard layout changed.
pub fn filter_incoming_receipts_for_shard(
    target_shard_layout: &ShardLayout,
    target_shard_id: ShardId,
    receipt_proofs: Arc<Vec<ReceiptProof>>,
) -> Result<Vec<ReceiptProof>, EpochError> {
    let mut filtered_receipt_proofs = vec![];
    for receipt_proof in receipt_proofs.iter() {
        let mut filtered_receipts = vec![];
        let ReceiptProof(receipts, shard_proof) = receipt_proof.clone();
        for receipt in receipts {
            if receipt.receiver_shard_id(target_shard_layout)? == target_shard_id {
                tracing::trace!(target: "chain", receipt_id=?receipt.receipt_id(), "including receipt");
                filtered_receipts.push(receipt);
            } else {
                tracing::trace!(target: "chain", receipt_id=?receipt.receipt_id(), "excluding receipt");
            }
        }
        let receipt_proof = ReceiptProof(filtered_receipts, shard_proof);
        filtered_receipt_proofs.push(receipt_proof);
    }
    Ok(filtered_receipt_proofs)
}

/// All chain-related database operations.
pub struct ChainStore {
    store: ChainStoreAdapter,
    // TODO(store): Use std::cell::OnceCell once OnceCell::get_or_try_init stabilizes.
    latest_known: std::cell::Cell<Option<LatestKnown>>,
    /// save_trie_changes should be set to true iff
    /// - archive is false - non-archival nodes need trie changes to perform garbage collection
    /// - archive is true, cold_store is configured and migration to split_storage is finished - node
    /// working in split storage mode needs trie changes in order to do garbage collection on hot.
    save_trie_changes: bool,
    /// The maximum number of blocks for which a transaction is valid since its creation.
    pub(super) transaction_validity_period: BlockHeightDelta,
}

impl Deref for ChainStore {
    type Target = ChainStoreAdapter;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

fn option_to_not_found<T, F>(res: io::Result<Option<T>>, field_name: F) -> Result<T, Error>
where
    F: std::string::ToString,
{
    match res {
        Ok(Some(o)) => Ok(o),
        Ok(None) => Err(Error::DBNotFoundErr(field_name.to_string())),
        Err(e) => Err(e.into()),
    }
}

impl ChainStore {
    pub fn new(
        store: Store,
        save_trie_changes: bool,
        transaction_validity_period: BlockHeightDelta,
    ) -> ChainStore {
        ChainStore {
            store: store.chain_store(),
            latest_known: std::cell::Cell::new(None),
            save_trie_changes,
            transaction_validity_period,
        }
    }

    pub fn store_update(&mut self) -> ChainStoreUpdate<'_> {
        ChainStoreUpdate::new(self)
    }

    pub fn iterate_state_sync_infos(&self) -> Result<Vec<(CryptoHash, StateSyncInfo)>, Error> {
        self.store
            .store()
            .iter(DBCol::StateDlInfos)
            .map(|item| match item {
                Ok((k, v)) => Ok((
                    CryptoHash::try_from(k.as_ref()).map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("wrong key length: {k:?}"),
                        )
                    })?,
                    StateSyncInfo::try_from_slice(v.as_ref())?,
                )),
                Err(err) => Err(err.into()),
            })
            .collect()
    }

    pub fn get_outgoing_receipts_for_shard(
        &self,
        epoch_manager: &dyn EpochManagerAdapter,
        prev_block_hash: CryptoHash,
        shard_id: ShardId,
        last_included_height: BlockHeight,
    ) -> Result<Vec<Receipt>, Error> {
        Self::get_outgoing_receipts_for_shard_from_store(
            &self.chain_store(),
            epoch_manager,
            prev_block_hash,
            shard_id,
            last_included_height,
        )
    }

    /// Get outgoing receipts that will be *sent* from shard `shard_id` from block whose prev block
    /// is `prev_block_hash`
    /// Note that the meaning of outgoing receipts here are slightly different from
    /// `save_outgoing_receipts` or `get_outgoing_receipts`.
    /// There, outgoing receipts for a shard refers to receipts that are generated
    /// from the shard from block `prev_block_hash`.
    /// Here, outgoing receipts for a shard refers to receipts that will be sent from this shard
    /// to other shards in the block after `prev_block_hash`
    /// The difference of one block is important because shard layout may change between the previous
    /// block and the current block and the meaning of `shard_id` will change.
    ///
    /// Note, the current way of implementation assumes that at least one chunk is generated before
    /// shard layout are changed twice. This is not a problem right now because we are changing shard
    /// layout for the first time for simple nightshade and generally not a problem if shard layout
    /// changes very rarely.
    /// But we need to implement a more theoretically correct algorithm if shard layouts will change
    /// more often in the future
    /// <https://github.com/near/nearcore/issues/4877>
    pub fn get_outgoing_receipts_for_shard_from_store(
        chain_store: &ChainStoreAdapter,
        epoch_manager: &dyn EpochManagerAdapter,
        prev_block_hash: CryptoHash,
        shard_id: ShardId,
        last_included_height: BlockHeight,
    ) -> Result<Vec<Receipt>, Error> {
        let shard_layout = epoch_manager.get_shard_layout_from_prev_block(&prev_block_hash)?;
        let mut receipts_block_hash = prev_block_hash;
        loop {
            let block_header = chain_store.get_block_header(&receipts_block_hash)?;

            if block_header.height() != last_included_height {
                receipts_block_hash = *block_header.prev_hash();
                continue;
            }
            let receipts_shard_layout = epoch_manager.get_shard_layout(block_header.epoch_id())?;

            // get the shard from which the outgoing receipt were generated
            let receipts_shard_id = if shard_layout != receipts_shard_layout {
                shard_layout.get_parent_shard_id(shard_id)?
            } else {
                shard_id
            };

            let mut receipts = chain_store
                .get_outgoing_receipts(&receipts_block_hash, receipts_shard_id)
                .map(|v| v.to_vec())
                .unwrap_or_default();

            if shard_layout != receipts_shard_layout {
                // the shard layout has changed so we need to reassign the outgoing receipts
                let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_block_hash)?;
                let protocol_version = epoch_manager.get_epoch_protocol_version(&epoch_id)?;
                Self::reassign_outgoing_receipts_for_resharding(
                    &mut receipts,
                    protocol_version,
                    &shard_layout,
                    shard_id,
                    receipts_shard_id,
                )?;
            }

            return Ok(receipts);
        }
    }

    pub fn reassign_outgoing_receipts_for_resharding(
        receipts: &mut Vec<Receipt>,
        protocol_version: ProtocolVersion,
        shard_layout: &ShardLayout,
        shard_id: ShardId,
        receipts_shard_id: ShardId,
    ) -> Result<(), Error> {
        tracing::trace!(target: "resharding", ?protocol_version, ?shard_id, ?receipts_shard_id, "reassign_outgoing_receipts_for_resharding");
        // If simple nightshade v2 is enabled and stable use that.
        // Same reassignment of outgoing receipts works for simple nightshade v3
        if ProtocolFeature::SimpleNightshadeV2.enabled(protocol_version) {
            Self::reassign_outgoing_receipts_for_resharding_impl(
                receipts,
                shard_layout,
                shard_id,
                receipts_shard_id,
            )?;
            return Ok(());
        }
        Ok(())
    }

    /// Reassign the outgoing receipts from the parent shard to the children
    /// shards.
    ///
    /// This method does it based on the "lowest child index" approach where it
    /// assigns all the receipts from parent to the child shard with the lowest
    /// index. It's meant to be used for the resharding from simple nightshade
    /// with 4 shards to simple nightshade v2 with 5 shards and subsequent
    /// reshardings.
    ///
    /// e.g. in the following resharding
    /// 0->0', 1->1', 2->2', 3->3',4'
    /// 0' will get all outgoing receipts from its parent 0
    /// 1' will get all outgoing receipts from its parent 1
    /// 2' will get all outgoing receipts from its parent 2
    /// 3' will get all outgoing receipts from its parent 3
    /// 4' will get no outgoing receipts from its parent 3
    /// All receipts are distributed to children, each exactly once.
    fn reassign_outgoing_receipts_for_resharding_impl(
        receipts: &mut Vec<Receipt>,
        shard_layout: &ShardLayout,
        shard_id: ShardId,
        receipts_shard_id: ShardId,
    ) -> Result<(), Error> {
        let split_shard_ids = shard_layout.get_children_shards_ids(receipts_shard_id);
        let split_shard_ids =
            split_shard_ids.ok_or(Error::InvalidSplitShardsIds(shard_id, receipts_shard_id))?;

        // The target shard id is the split shard with the lowest shard id.
        let target_shard_id = split_shard_ids.iter().min();
        let target_shard_id =
            *target_shard_id.ok_or(Error::InvalidSplitShardsIds(shard_id, receipts_shard_id))?;

        if shard_id == target_shard_id {
            // This shard_id is the lowest index child, it gets all the receipts.
            Ok(())
        } else {
            // This shard_id is not the lowest index child, it gets no receipts.
            receipts.clear();
            Ok(())
        }
    }

    /// For a given transaction, it expires if the block that the chunk points to is more than `validity_period`
    /// ahead of the block that has `base_block_hash`.
    pub fn check_transaction_validity_period(
        &self,
        prev_block_header: &BlockHeader,
        base_block_hash: &CryptoHash,
    ) -> Result<(), InvalidTxError> {
        check_transaction_validity_period(
            &self.store,
            prev_block_header,
            base_block_hash,
            self.transaction_validity_period,
        )
    }

    pub fn compute_transaction_validity(
        &self,
        prev_block_header: &BlockHeader,
        chunk: &ShardChunk,
    ) -> Vec<bool> {
        chunk
            .to_transactions()
            .into_iter()
            .map(|signed_tx| {
                self.check_transaction_validity_period(
                    prev_block_header,
                    signed_tx.transaction.block_hash(),
                )
                .is_ok()
            })
            .collect()
    }
}

impl ChainStore {
    /// Returns outcomes on all forks generated by applying transaction or
    /// receipt with the given id.
    pub fn get_outcomes_by_id(
        &self,
        id: &CryptoHash,
    ) -> Result<Vec<ExecutionOutcomeWithIdAndProof>, Error> {
        self.store
            .store()
            .iter_prefix_ser::<ExecutionOutcomeWithProof>(
                DBCol::TransactionResultForBlock,
                id.as_ref(),
            )
            .map(|item| {
                let (key, outcome_with_proof) = item?;
                let (_, block_hash) = get_outcome_id_block_hash_rev(key.as_ref())?;
                Ok(ExecutionOutcomeWithIdAndProof {
                    proof: outcome_with_proof.proof,
                    block_hash,
                    outcome_with_id: ExecutionOutcomeWithId {
                        id: *id,
                        outcome: outcome_with_proof.outcome,
                    },
                })
            })
            .collect()
    }

    /// Get all execution outcomes generated when the chunk are applied
    pub fn get_block_execution_outcomes(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<HashMap<ShardId, Vec<ExecutionOutcomeWithIdAndProof>>, Error> {
        let block = self.get_block(block_hash)?;
        let chunk_headers = block.chunks().iter_deprecated().cloned().collect::<Vec<_>>();

        let mut res = HashMap::new();
        for chunk_header in chunk_headers {
            let shard_id = chunk_header.shard_id();
            let outcomes = self
                .get_outcomes_by_block_hash_and_shard_id(block_hash, shard_id)?
                .into_iter()
                .filter_map(|id| {
                    let outcome_with_proof =
                        self.get_outcome_by_id_and_block_hash(&id, block_hash).ok()??;
                    Some(ExecutionOutcomeWithIdAndProof {
                        proof: outcome_with_proof.proof,
                        block_hash: *block_hash,
                        outcome_with_id: ExecutionOutcomeWithId {
                            id,
                            outcome: outcome_with_proof.outcome,
                        },
                    })
                })
                .collect::<Vec<_>>();
            res.insert(shard_id, outcomes);
        }
        Ok(res)
    }

    /// Returns latest known height and time it was seen.
    /// TODO(store): What is this doing here? Cleanup
    pub fn get_latest_known(&self) -> Result<LatestKnown, Error> {
        if let Some(latest_known) = self.latest_known.get() {
            return Ok(latest_known);
        }
        let latest_known: LatestKnown = option_to_not_found(
            self.store.store().get_ser(DBCol::BlockMisc, LATEST_KNOWN_KEY),
            "LATEST_KNOWN_KEY",
        )?;
        self.latest_known.set(Some(latest_known));
        Ok(latest_known)
    }

    /// Save the latest known.
    /// TODO(store): What is this doing here? Cleanup
    pub fn save_latest_known(&mut self, latest_known: LatestKnown) -> Result<(), Error> {
        let mut store_update = self.store.store().store_update();
        store_update.set_ser(DBCol::BlockMisc, LATEST_KNOWN_KEY, &latest_known)?;
        self.latest_known.set(Some(latest_known));
        store_update.commit().map_err(|err| err.into())
    }

    /// Retrieve the kinds of state changes occurred in a given block.
    ///
    /// We store different types of data, so we prefer to only expose minimal information about the
    /// changes (i.e. a kind of the change and an account id).
    pub fn get_state_changes_in_block(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<StateChangesKinds, Error> {
        // We store the trie changes under a compound key: `block_hash + trie_key`, so when we
        // query the changes, we reverse the process by splitting the key using simple slicing of an
        // array of bytes, essentially, extracting `trie_key`.
        //
        // Example: data changes are stored under a key:
        //
        //     block_hash + (col::ACCOUNT + account_id + ACCOUNT_DATA_SEPARATOR + user_specified_key)
        //
        // Thus, to query the list of touched accounts we do the following:
        // 1. Query RocksDB for `block_hash` prefix.
        // 2. Extract the original Trie key out of the keys returned by RocksDB
        // 3. Try extracting `account_id` from the key using KeyFor* implementations

        let storage_key = KeyForStateChanges::for_block(block_hash);

        let store = self.store.store();
        let mut block_changes = storage_key.find_iter(&store);

        Ok(StateChangesKinds::from_changes(&mut block_changes)?)
    }

    pub fn get_state_changes_with_cause_in_block(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<StateChanges, Error> {
        let storage_key = KeyForStateChanges::for_block(block_hash);

        let store = self.store.store();
        let mut block_changes = storage_key.find_iter(&store);

        Ok(StateChanges::from_changes(&mut block_changes)?)
    }

    /// Retrieve the key-value changes from the store and decode them appropriately.
    ///
    /// We store different types of data, so we need to take care of all the types. That is, the
    /// account data and the access keys are internally-serialized and we have to deserialize those
    /// values appropriately. Code and data changes are simple blobs of data, so we return them as
    /// base64-encoded blobs.
    pub fn get_state_changes(
        &self,
        block_hash: &CryptoHash,
        state_changes_request: &StateChangesRequest,
    ) -> Result<StateChanges, Error> {
        // We store the trie changes under a compound key: `block_hash + trie_key`, so when we
        // query the changes, we reverse the process by splitting the key using simple slicing of an
        // array of bytes, essentially, extracting `trie_key`.
        //
        // Example: data changes are stored under a key:
        //
        //     block_hash + (col::ACCOUNT + account_id + ACCOUNT_DATA_SEPARATOR + user_specified_key)
        //
        // Thus, to query all the changes by a user-specified key prefix, we do the following:
        // 1. Query RocksDB for
        //     block_hash + (col::ACCOUNT + account_id + ACCOUNT_DATA_SEPARATOR + user_specified_key_prefix)
        //
        // 2. In the simplest case, to extract the full key we need to slice the RocksDB key by a length of
        //     block_hash + (col::ACCOUNT + account_id + ACCOUNT_DATA_SEPARATOR)
        //
        //    In this implementation, however, we decoupled this process into two steps:
        //
        //    2.1. Split off the `block_hash` (internally in `KeyForStateChanges`), thus we are
        //         left working with a key that was used in the trie.
        //    2.2. Parse the trie key with a relevant KeyFor* implementation to ensure consistency

        let store = self.store.store();
        Ok(match state_changes_request {
            StateChangesRequest::AccountChanges { account_ids } => {
                let mut changes = StateChanges::new();
                for account_id in account_ids {
                    let data_key = TrieKey::Account { account_id: account_id.clone() };
                    let storage_key = KeyForStateChanges::from_trie_key(block_hash, &data_key);
                    let changes_per_key = storage_key.find_exact_iter(&store);
                    changes.extend(StateChanges::from_account_changes(changes_per_key)?);
                }
                changes
            }
            StateChangesRequest::SingleAccessKeyChanges { keys } => {
                let mut changes = StateChanges::new();
                for key in keys {
                    let data_key = TrieKey::AccessKey {
                        account_id: key.account_id.clone(),
                        public_key: key.public_key.clone(),
                    };
                    let storage_key = KeyForStateChanges::from_trie_key(block_hash, &data_key);
                    let changes_per_key = storage_key.find_exact_iter(&store);
                    changes.extend(StateChanges::from_access_key_changes(changes_per_key)?);
                }
                changes
            }
            StateChangesRequest::AllAccessKeyChanges { account_ids } => {
                let mut changes = StateChanges::new();
                for account_id in account_ids {
                    let data_key = trie_key_parsers::get_raw_prefix_for_access_keys(account_id);
                    let storage_key = KeyForStateChanges::from_raw_key(block_hash, &data_key);
                    let changes_per_key_prefix = storage_key.find_iter(&store);
                    changes.extend(StateChanges::from_access_key_changes(changes_per_key_prefix)?);
                }
                changes
            }
            StateChangesRequest::ContractCodeChanges { account_ids } => {
                let mut changes = StateChanges::new();
                for account_id in account_ids {
                    let data_key = TrieKey::ContractCode { account_id: account_id.clone() };
                    let storage_key = KeyForStateChanges::from_trie_key(block_hash, &data_key);
                    let changes_per_key = storage_key.find_exact_iter(&store);
                    changes.extend(StateChanges::from_contract_code_changes(changes_per_key)?);
                }
                changes
            }
            StateChangesRequest::DataChanges { account_ids, key_prefix } => {
                let mut changes = StateChanges::new();
                for account_id in account_ids {
                    let data_key = trie_key_parsers::get_raw_prefix_for_contract_data(
                        account_id,
                        key_prefix.as_ref(),
                    );
                    let storage_key = KeyForStateChanges::from_raw_key(block_hash, &data_key);
                    let changes_per_key_prefix = storage_key.find_iter(&store);
                    changes.extend(StateChanges::from_data_changes(changes_per_key_prefix)?);
                }
                changes
            }
        })
    }

    pub fn get_store_statistics(&self) -> Option<StoreStatistics> {
        self.store.store().get_store_statistics()
    }

    /// Constructs key 'STATE_SYNC_DUMP:<ShardId>',
    /// for example 'STATE_SYNC_DUMP:2' for shard_id=2.
    /// Doesn't contain epoch_id, because only one dump process per shard is allowed.
    fn state_sync_dump_progress_key(shard_id: ShardId) -> Vec<u8> {
        let mut key = STATE_SYNC_DUMP_KEY.to_vec();
        key.extend(b":".to_vec());
        key.extend(shard_id.to_le_bytes());
        key
    }

    /// Retrieves STATE_SYNC_DUMP for the given shard.
    /// TODO(store): What is this doing here? Cleanup
    pub fn get_state_sync_dump_progress(
        &self,
        shard_id: ShardId,
    ) -> Result<StateSyncDumpProgress, Error> {
        option_to_not_found(
            self.store
                .store()
                .get_ser(DBCol::BlockMisc, &ChainStore::state_sync_dump_progress_key(shard_id)),
            format!("STATE_SYNC_DUMP:{}", shard_id),
        )
    }

    /// For each value stored, this returns an (EpochId, bool), where the bool tells whether it's finished
    /// because those are the only fields we really care about.
    pub fn iter_state_sync_dump_progress<'a>(
        &'a self,
    ) -> impl Iterator<Item = io::Result<(ShardId, (EpochId, bool))>> + 'a {
        self.store
            .store_ref()
            .iter_prefix_ser::<StateSyncDumpProgress>(DBCol::BlockMisc, STATE_SYNC_DUMP_KEY)
            .map(|item| {
                item.and_then(|(key, progress)| {
                    // + 1 for the ':'
                    let prefix_len = STATE_SYNC_DUMP_KEY.len() + 1;
                    let int_part = &key[prefix_len..];
                    let int_part = int_part.try_into().map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Bad StateSyncDump column key length: {}", key.len()),
                        )
                    })?;
                    let shard_id = ShardId::from_le_bytes(int_part);
                    Ok((
                        shard_id,
                        match progress {
                            StateSyncDumpProgress::AllDumped { epoch_id, .. } => (epoch_id, true),
                            StateSyncDumpProgress::InProgress { epoch_id, .. } => (epoch_id, false),
                            StateSyncDumpProgress::Skipped { epoch_id, .. } => (epoch_id, true),
                        },
                    ))
                })
            })
    }

    /// Updates STATE_SYNC_DUMP for the given shard.
    /// TODO(store): What is this doing here? Cleanup
    pub fn set_state_sync_dump_progress(
        &self,
        shard_id: ShardId,
        value: Option<StateSyncDumpProgress>,
    ) -> Result<(), Error> {
        let mut store_update = self.store.store().store_update();
        let key = ChainStore::state_sync_dump_progress_key(shard_id);
        match value {
            None => store_update.delete(DBCol::BlockMisc, &key),
            Some(value) => store_update.set_ser(DBCol::BlockMisc, &key, &value)?,
        }
        store_update.commit().map_err(|err| err.into())
    }

    pub fn prev_block_is_caught_up(
        chain_store: &ChainStoreAdapter,
        prev_prev_hash: &CryptoHash,
        prev_hash: &CryptoHash,
    ) -> Result<bool, Error> {
        // Needs to be used with care: for the first block of each epoch the semantic is slightly
        // different, since the prev_block is in a different epoch. So for all the blocks but the
        // first one in each epoch this method returns true if the block is ready to have state
        // applied for the next epoch, while for the first block in a particular epoch this method
        // returns true if the block is ready to have state applied for the current epoch (and
        // otherwise should be orphaned)
        Ok(!chain_store.get_blocks_to_catchup(prev_prev_hash)?.contains(prev_hash))
    }
}

impl ChainStoreAccess for ChainStore {
    fn chain_store(&self) -> &ChainStore {
        &self
    }

    fn store(&self) -> Store {
        self.store.store()
    }
    /// The chain head.
    fn head(&self) -> Result<Tip, Error> {
        ChainStoreAdapter::head(self)
    }

    /// The chain Blocks Tail height, used by GC.
    fn tail(&self) -> Result<BlockHeight, Error> {
        ChainStoreAdapter::tail(self)
    }

    /// The chain Chunks Tail height, used by GC.
    fn chunk_tail(&self) -> Result<BlockHeight, Error> {
        ChainStoreAdapter::chunk_tail(self)
    }

    fn fork_tail(&self) -> Result<BlockHeight, Error> {
        ChainStoreAdapter::fork_tail(self)
    }

    /// Header of the block at the head of the block chain (not the same thing as header_head).
    fn head_header(&self) -> Result<BlockHeader, Error> {
        ChainStoreAdapter::head_header(self)
    }

    /// Largest height for which we created a doomslug endorsement
    fn largest_target_height(&self) -> Result<BlockHeight, Error> {
        ChainStoreAdapter::largest_target_height(self)
    }

    /// Head of the header chain (not the same thing as head_header).
    fn header_head(&self) -> Result<Tip, Error> {
        ChainStoreAdapter::header_head(self)
    }

    /// Final head of the chain.
    fn final_head(&self) -> Result<Tip, Error> {
        ChainStoreAdapter::final_head(self)
    }

    /// Get full block.
    fn get_block(&self, h: &CryptoHash) -> Result<Block, Error> {
        ChainStoreAdapter::get_block(self, h)
    }

    /// Get partial chunk.
    fn get_partial_chunk(&self, chunk_hash: &ChunkHash) -> Result<Arc<PartialEncodedChunk>, Error> {
        ChainStoreAdapter::get_partial_chunk(self, chunk_hash)
    }

    /// Does this full block exist?
    fn block_exists(&self, h: &CryptoHash) -> Result<bool, Error> {
        ChainStoreAdapter::block_exists(self, h)
    }

    fn chunk_exists(&self, h: &ChunkHash) -> Result<bool, Error> {
        ChainStoreAdapter::chunk_exists(self, h)
    }

    /// Get previous header.
    fn get_previous_header(&self, header: &BlockHeader) -> Result<BlockHeader, Error> {
        ChainStoreAdapter::get_previous_header(self, header)
    }

    /// Information from applying chunk.
    fn get_chunk_extra(
        &self,
        block_hash: &CryptoHash,
        shard_uid: &ShardUId,
    ) -> Result<Arc<ChunkExtra>, Error> {
        ChainStoreAdapter::get_chunk_extra(self, block_hash, shard_uid)
    }

    fn get_chunk_apply_stats(
        &self,
        block_hash: &CryptoHash,
        shard_id: &ShardId,
    ) -> Result<Option<ChunkApplyStats>, Error> {
        ChainStoreAdapter::get_chunk_apply_stats(&self, block_hash, shard_id)
    }

    /// Get block header.
    fn get_block_header(&self, h: &CryptoHash) -> Result<BlockHeader, Error> {
        ChainStoreAdapter::get_block_header(self, h)
    }

    /// Returns hash of the block on the main chain for given height.
    fn get_block_hash_by_height(&self, height: BlockHeight) -> Result<CryptoHash, Error> {
        ChainStoreAdapter::get_block_hash_by_height(self, height)
    }

    fn get_next_block_hash(&self, hash: &CryptoHash) -> Result<CryptoHash, Error> {
        ChainStoreAdapter::get_next_block_hash(self, hash)
    }

    fn get_epoch_light_client_block(
        &self,
        hash: &CryptoHash,
    ) -> Result<Arc<LightClientBlockView>, Error> {
        ChainStoreAdapter::get_epoch_light_client_block(self, hash)
    }

    fn get_block_refcount(&self, block_hash: &CryptoHash) -> Result<u64, Error> {
        ChainStoreAdapter::get_block_refcount(self, block_hash)
    }

    /// Get outgoing receipts *generated* from shard `shard_id` in block `prev_hash`
    /// Note that this function is different from get_outgoing_receipts_for_shard, see comments there
    fn get_outgoing_receipts(
        &self,
        prev_block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Arc<Vec<Receipt>>, Error> {
        ChainStoreAdapter::get_outgoing_receipts(self, prev_block_hash, shard_id)
    }

    fn get_incoming_receipts(
        &self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Arc<Vec<ReceiptProof>>, Error> {
        ChainStoreAdapter::get_incoming_receipts(self, block_hash, shard_id)
    }

    fn get_blocks_to_catchup(&self, hash: &CryptoHash) -> Result<Vec<CryptoHash>, Error> {
        ChainStoreAdapter::get_blocks_to_catchup(self, hash)
    }

    fn is_invalid_chunk(
        &self,
        chunk_hash: &ChunkHash,
    ) -> Result<Option<Arc<EncodedShardChunk>>, Error> {
        ChainStoreAdapter::is_invalid_chunk(self, chunk_hash)
    }

    fn get_transaction(
        &self,
        tx_hash: &CryptoHash,
    ) -> Result<Option<Arc<SignedTransaction>>, Error> {
        ChainStoreAdapter::get_transaction(self, tx_hash)
    }

    fn get_receipt(&self, receipt_id: &CryptoHash) -> Result<Option<Arc<Receipt>>, Error> {
        ChainStoreAdapter::get_receipt(self, receipt_id)
    }

    fn get_genesis_height(&self) -> BlockHeight {
        ChainStoreAdapter::get_genesis_height(self)
    }

    fn get_block_merkle_tree(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Arc<PartialMerkleTree>, Error> {
        ChainStoreAdapter::get_block_merkle_tree(self, block_hash).map(Arc::new)
    }

    fn get_block_hash_from_ordinal(&self, block_ordinal: NumBlocks) -> Result<CryptoHash, Error> {
        ChainStoreAdapter::get_block_hash_from_ordinal(self, block_ordinal)
    }

    fn is_height_processed(&self, height: BlockHeight) -> Result<bool, Error> {
        ChainStoreAdapter::is_height_processed(self, height)
    }

    fn get_current_epoch_sync_hash(&self, epoch_id: &EpochId) -> Result<Option<CryptoHash>, Error> {
        ChainStoreAdapter::get_current_epoch_sync_hash(self, epoch_id)
    }
}

/// Cache update for ChainStore
#[derive(Default)]
pub(crate) struct ChainStoreCacheUpdate {
    block: Option<Block>,
    headers: HashMap<CryptoHash, BlockHeader>,
    chunk_extras: HashMap<(CryptoHash, ShardUId), Arc<ChunkExtra>>,
    chunks: HashMap<ChunkHash, Arc<ShardChunk>>,
    partial_chunks: HashMap<ChunkHash, Arc<PartialEncodedChunk>>,
    block_hash_per_height: HashMap<BlockHeight, HashMap<EpochId, HashSet<CryptoHash>>>,
    pub(crate) height_to_hashes: HashMap<BlockHeight, Option<CryptoHash>>,
    next_block_hashes: HashMap<CryptoHash, CryptoHash>,
    epoch_light_client_blocks: HashMap<CryptoHash, Arc<LightClientBlockView>>,
    outgoing_receipts: HashMap<(CryptoHash, ShardId), Arc<Vec<Receipt>>>,
    incoming_receipts: HashMap<(CryptoHash, ShardId), Arc<Vec<ReceiptProof>>>,
    outcomes: HashMap<(CryptoHash, CryptoHash), ExecutionOutcomeWithProof>,
    outcome_ids: HashMap<(CryptoHash, ShardId), Vec<CryptoHash>>,
    invalid_chunks: HashMap<ChunkHash, Arc<EncodedShardChunk>>,
    transactions: HashMap<CryptoHash, Arc<SignedTransaction>>,
    receipts: HashMap<CryptoHash, Arc<Receipt>>,
    block_refcounts: HashMap<CryptoHash, u64>,
    block_merkle_tree: HashMap<CryptoHash, Arc<PartialMerkleTree>>,
    block_ordinal_to_hash: HashMap<NumBlocks, CryptoHash>,
    processed_block_heights: HashSet<BlockHeight>,
}

/// Provides layer to update chain without touching the underlying database.
/// This serves few purposes, main one is that even if executable exists/fails during update the database is in consistent state.
pub struct ChainStoreUpdate<'a> {
    chain_store: &'a mut ChainStore,
    store_updates: Vec<StoreUpdate>,
    /// Blocks added during this update. Takes ownership (unclear how to not do it because of failure exists).
    pub(crate) chain_store_cache_update: ChainStoreCacheUpdate,
    head: Option<Tip>,
    tail: Option<BlockHeight>,
    chunk_tail: Option<BlockHeight>,
    fork_tail: Option<BlockHeight>,
    header_head: Option<Tip>,
    final_head: Option<Tip>,
    largest_target_height: Option<BlockHeight>,
    trie_changes: Vec<(CryptoHash, WrappedTrieChanges)>,
    state_transition_data: HashMap<(CryptoHash, ShardId), StoredChunkStateTransitionData>,
    add_blocks_to_catchup: Vec<(CryptoHash, CryptoHash)>,
    // A pair (prev_hash, hash) to be removed from blocks to catchup
    remove_blocks_to_catchup: Vec<(CryptoHash, CryptoHash)>,
    // A prev_hash to be removed with all the hashes associated with it
    remove_prev_blocks_to_catchup: Vec<CryptoHash>,
    add_state_sync_infos: Vec<StateSyncInfo>,
    remove_state_sync_infos: Vec<CryptoHash>,
    chunk_apply_stats: HashMap<(CryptoHash, ShardId), ChunkApplyStats>,
}

impl<'a> ChainStoreUpdate<'a> {
    pub fn new(chain_store: &'a mut ChainStore) -> Self {
        ChainStoreUpdate {
            chain_store,
            store_updates: vec![],
            chain_store_cache_update: ChainStoreCacheUpdate::default(),
            head: None,
            tail: None,
            chunk_tail: None,
            fork_tail: None,
            header_head: None,
            final_head: None,
            largest_target_height: None,
            trie_changes: vec![],
            state_transition_data: Default::default(),
            add_blocks_to_catchup: vec![],
            remove_blocks_to_catchup: vec![],
            remove_prev_blocks_to_catchup: vec![],
            add_state_sync_infos: vec![],
            remove_state_sync_infos: vec![],
            chunk_apply_stats: HashMap::default(),
        }
    }

    pub fn get_chunk(&self, chunk_hash: &ChunkHash) -> Result<Arc<ShardChunk>, Error> {
        if let Some(chunk) = self.chain_store_cache_update.chunks.get(chunk_hash) {
            Ok(Arc::clone(chunk))
        } else {
            self.chain_store.get_chunk(chunk_hash).map(Arc::new)
        }
    }
}

impl<'a> ChainStoreAccess for ChainStoreUpdate<'a> {
    fn chain_store(&self) -> &ChainStore {
        &self.chain_store
    }

    fn store(&self) -> Store {
        self.chain_store.store.store()
    }

    /// The chain head.
    fn head(&self) -> Result<Tip, Error> {
        if let Some(head) = &self.head { Ok(head.clone()) } else { self.chain_store.head() }
    }

    /// The chain Block Tail height, used by GC.
    fn tail(&self) -> Result<BlockHeight, Error> {
        if let Some(tail) = &self.tail { Ok(*tail) } else { self.chain_store.tail() }
    }

    /// The chain Chunks Tail height, used by GC.
    fn chunk_tail(&self) -> Result<BlockHeight, Error> {
        if let Some(chunk_tail) = &self.chunk_tail {
            Ok(*chunk_tail)
        } else {
            self.chain_store.chunk_tail()
        }
    }

    /// Fork tail used by GC
    fn fork_tail(&self) -> Result<BlockHeight, Error> {
        if let Some(fork_tail) = &self.fork_tail {
            Ok(*fork_tail)
        } else {
            self.chain_store.fork_tail()
        }
    }

    /// Head of the header chain (not the same thing as head_header).
    fn header_head(&self) -> Result<Tip, Error> {
        if let Some(header_head) = &self.header_head {
            Ok(header_head.clone())
        } else {
            self.chain_store.header_head()
        }
    }

    fn final_head(&self) -> Result<Tip, Error> {
        if let Some(final_head) = self.final_head.as_ref() {
            Ok(final_head.clone())
        } else {
            self.chain_store.final_head()
        }
    }

    fn largest_target_height(&self) -> Result<BlockHeight, Error> {
        if let Some(largest_target_height) = &self.largest_target_height {
            Ok(*largest_target_height)
        } else {
            self.chain_store.largest_target_height()
        }
    }

    /// Header of the block at the head of the block chain (not the same thing as header_head).
    fn head_header(&self) -> Result<BlockHeader, Error> {
        self.get_block_header(&(self.head()?.last_block_hash))
    }

    /// Get full block.
    fn get_block(&self, h: &CryptoHash) -> Result<Block, Error> {
        if let Some(block) = &self.chain_store_cache_update.block {
            if block.hash() == h {
                return Ok(block.clone());
            }
        }
        self.chain_store.get_block(h)
    }

    /// Does this full block exist?
    fn block_exists(&self, h: &CryptoHash) -> Result<bool, Error> {
        if let Some(block) = &self.chain_store_cache_update.block {
            if block.hash() == h {
                return Ok(true);
            }
        }
        self.chain_store.block_exists(h)
    }

    fn chunk_exists(&self, h: &ChunkHash) -> Result<bool, Error> {
        Ok(self.chain_store_cache_update.chunks.contains_key(h)
            || self.chain_store.chunk_exists(h)?)
    }

    /// Get previous header.
    fn get_previous_header(&self, header: &BlockHeader) -> Result<BlockHeader, Error> {
        self.get_block_header(header.prev_hash())
    }

    /// Get state root hash after applying header with given hash.
    fn get_chunk_extra(
        &self,
        block_hash: &CryptoHash,
        shard_uid: &ShardUId,
    ) -> Result<Arc<ChunkExtra>, Error> {
        if let Some(chunk_extra) =
            self.chain_store_cache_update.chunk_extras.get(&(*block_hash, *shard_uid))
        {
            Ok(Arc::clone(chunk_extra))
        } else {
            self.chain_store.get_chunk_extra(block_hash, shard_uid)
        }
    }

    fn get_chunk_apply_stats(
        &self,
        block_hash: &CryptoHash,
        shard_id: &ShardId,
    ) -> Result<Option<ChunkApplyStats>, Error> {
        if let Some(stats) = self.chunk_apply_stats.get(&(*block_hash, *shard_id)) {
            Ok(Some(stats.clone()))
        } else {
            self.chain_store.get_chunk_apply_stats(block_hash, shard_id)
        }
    }

    /// Get block header.
    fn get_block_header(&self, hash: &CryptoHash) -> Result<BlockHeader, Error> {
        if let Some(header) = self.chain_store_cache_update.headers.get(hash).cloned() {
            Ok(header)
        } else {
            self.chain_store.get_block_header(hash)
        }
    }

    /// Get block header from the current chain by height.
    fn get_block_hash_by_height(&self, height: BlockHeight) -> Result<CryptoHash, Error> {
        match self.chain_store_cache_update.height_to_hashes.get(&height) {
            Some(Some(hash)) => Ok(*hash),
            Some(None) => Err(Error::DBNotFoundErr(format!("BLOCK HEIGHT: {}", height))),
            None => self.chain_store.get_block_hash_by_height(height),
        }
    }

    fn get_block_refcount(&self, block_hash: &CryptoHash) -> Result<u64, Error> {
        if let Some(refcount) = self.chain_store_cache_update.block_refcounts.get(block_hash) {
            Ok(*refcount)
        } else {
            let refcount = match self.chain_store.get_block_refcount(block_hash) {
                Ok(refcount) => refcount,
                Err(e) => match e {
                    Error::DBNotFoundErr(_) => 0,
                    _ => return Err(e),
                },
            };
            Ok(refcount)
        }
    }

    fn get_next_block_hash(&self, hash: &CryptoHash) -> Result<CryptoHash, Error> {
        if let Some(next_hash) = self.chain_store_cache_update.next_block_hashes.get(hash) {
            Ok(*next_hash)
        } else {
            self.chain_store.get_next_block_hash(hash)
        }
    }

    fn get_epoch_light_client_block(
        &self,
        hash: &CryptoHash,
    ) -> Result<Arc<LightClientBlockView>, Error> {
        if let Some(light_client_block) =
            self.chain_store_cache_update.epoch_light_client_blocks.get(hash)
        {
            Ok(Arc::clone(light_client_block))
        } else {
            self.chain_store.get_epoch_light_client_block(hash)
        }
    }

    /// Get receipts produced for block with given hash.
    fn get_outgoing_receipts(
        &self,
        hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Arc<Vec<Receipt>>, Error> {
        if let Some(receipts) =
            self.chain_store_cache_update.outgoing_receipts.get(&(*hash, shard_id))
        {
            Ok(Arc::clone(receipts))
        } else {
            self.chain_store.get_outgoing_receipts(hash, shard_id)
        }
    }

    /// Get receipts produced for block with given hash.
    fn get_incoming_receipts(
        &self,
        hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Arc<Vec<ReceiptProof>>, Error> {
        if let Some(receipt_proofs) =
            self.chain_store_cache_update.incoming_receipts.get(&(*hash, shard_id))
        {
            Ok(Arc::clone(receipt_proofs))
        } else {
            self.chain_store.get_incoming_receipts(hash, shard_id)
        }
    }

    fn get_partial_chunk(&self, chunk_hash: &ChunkHash) -> Result<Arc<PartialEncodedChunk>, Error> {
        if let Some(partial_chunk) = self.chain_store_cache_update.partial_chunks.get(chunk_hash) {
            Ok(Arc::clone(partial_chunk))
        } else {
            self.chain_store.get_partial_chunk(chunk_hash)
        }
    }

    fn get_blocks_to_catchup(&self, prev_hash: &CryptoHash) -> Result<Vec<CryptoHash>, Error> {
        // Make sure we never request a block to catchup after altering the data structure
        assert_eq!(self.add_blocks_to_catchup.len(), 0);
        assert_eq!(self.remove_blocks_to_catchup.len(), 0);
        assert_eq!(self.remove_prev_blocks_to_catchup.len(), 0);

        self.chain_store.get_blocks_to_catchup(prev_hash)
    }

    fn is_invalid_chunk(
        &self,
        chunk_hash: &ChunkHash,
    ) -> Result<Option<Arc<EncodedShardChunk>>, Error> {
        if let Some(chunk) = self.chain_store_cache_update.invalid_chunks.get(chunk_hash) {
            Ok(Some(Arc::clone(chunk)))
        } else {
            self.chain_store.is_invalid_chunk(chunk_hash)
        }
    }

    fn get_transaction(
        &self,
        tx_hash: &CryptoHash,
    ) -> Result<Option<Arc<SignedTransaction>>, Error> {
        if let Some(tx) = self.chain_store_cache_update.transactions.get(tx_hash) {
            Ok(Some(Arc::clone(tx)))
        } else {
            self.chain_store.get_transaction(tx_hash)
        }
    }

    fn get_receipt(&self, receipt_id: &CryptoHash) -> Result<Option<Arc<Receipt>>, Error> {
        if let Some(receipt) = self.chain_store_cache_update.receipts.get(receipt_id) {
            Ok(Some(Arc::clone(receipt)))
        } else {
            self.chain_store.get_receipt(receipt_id)
        }
    }

    fn get_genesis_height(&self) -> BlockHeight {
        self.chain_store.get_genesis_height()
    }

    fn get_block_merkle_tree(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Arc<PartialMerkleTree>, Error> {
        if let Some(merkle_tree) = self.chain_store_cache_update.block_merkle_tree.get(block_hash) {
            Ok(Arc::clone(&merkle_tree))
        } else {
            self.chain_store.get_block_merkle_tree(block_hash)
        }
    }

    fn get_block_hash_from_ordinal(&self, block_ordinal: NumBlocks) -> Result<CryptoHash, Error> {
        if let Some(block_hash) =
            self.chain_store_cache_update.block_ordinal_to_hash.get(&block_ordinal)
        {
            Ok(*block_hash)
        } else {
            self.chain_store.get_block_hash_from_ordinal(block_ordinal)
        }
    }

    fn is_height_processed(&self, height: BlockHeight) -> Result<bool, Error> {
        if self.chain_store_cache_update.processed_block_heights.contains(&height) {
            Ok(true)
        } else {
            self.chain_store.is_height_processed(height)
        }
    }

    fn get_current_epoch_sync_hash(&self, epoch_id: &EpochId) -> Result<Option<CryptoHash>, Error> {
        self.chain_store.get_current_epoch_sync_hash(epoch_id)
    }
}

impl<'a> ChainStoreUpdate<'a> {
    /// Update both header and block body head.
    pub fn save_head(&mut self, t: &Tip) -> Result<(), Error> {
        self.save_body_head(t)?;
        self.save_header_head(t)
    }

    /// Update block body head and latest known height.
    pub fn save_body_head(&mut self, t: &Tip) -> Result<(), Error> {
        self.try_save_latest_known(t.height)?;
        self.head = Some(t.clone());
        Ok(())
    }

    pub fn save_final_head(&mut self, t: &Tip) -> Result<(), Error> {
        self.final_head = Some(t.clone());
        Ok(())
    }

    /// Updates fields in the ChainStore that store information about the
    /// canonical chain.
    fn update_height(&mut self, height: BlockHeight, hash: CryptoHash) -> Result<(), Error> {
        let mut prev_hash = hash;
        let mut prev_height = height;
        loop {
            let header = self.get_block_header(&prev_hash)?;
            let (header_height, header_hash, header_prev_hash) =
                (header.height(), *header.hash(), *header.prev_hash());
            // Clean up block indices between blocks.
            for height in (header_height + 1)..prev_height {
                self.chain_store_cache_update.height_to_hashes.insert(height, None);
            }
            // Override block ordinal to hash mapping for blocks in between.
            // At this point block_merkle_tree for header is already saved.
            let block_ordinal = self.get_block_merkle_tree(&header_hash)?.size();
            self.chain_store_cache_update.block_ordinal_to_hash.insert(block_ordinal, header_hash);
            match self.get_block_hash_by_height(header_height) {
                Ok(cur_hash) if cur_hash == header_hash => {
                    // Found common ancestor.
                    return Ok(());
                }
                _ => {
                    self.chain_store_cache_update
                        .height_to_hashes
                        .insert(header_height, Some(header_hash));
                    self.chain_store_cache_update
                        .next_block_hashes
                        .insert(header_prev_hash, header_hash);
                    prev_hash = header_prev_hash;
                    prev_height = header_height;
                }
            };
        }
    }

    /// Update header head and height to hash index for this branch.
    pub fn save_header_head(&mut self, t: &Tip) -> Result<(), Error> {
        if t.height > self.chain_store.get_genesis_height() {
            self.update_height(t.height, t.prev_block_hash)?;
        }
        self.try_save_latest_known(t.height)?;

        match self.header_head() {
            Ok(prev_tip) => {
                if prev_tip.height > t.height {
                    for height in (t.height + 1)..=prev_tip.height {
                        self.chain_store_cache_update.height_to_hashes.insert(height, None);
                    }
                }
            }
            Err(err) => match err {
                Error::DBNotFoundErr(_) => {}
                e => return Err(e),
            },
        }

        // save block ordinal and height if we need to update header head
        let block_ordinal = self.get_block_merkle_tree(&t.last_block_hash)?.size();
        self.chain_store_cache_update
            .block_ordinal_to_hash
            .insert(block_ordinal, t.last_block_hash);
        self.chain_store_cache_update.height_to_hashes.insert(t.height, Some(t.last_block_hash));
        self.chain_store_cache_update
            .next_block_hashes
            .insert(t.prev_block_hash, t.last_block_hash);
        self.header_head = Some(t.clone());
        Ok(())
    }

    pub fn save_largest_target_height(&mut self, height: BlockHeight) {
        self.largest_target_height = Some(height);
    }

    /// Save new height if it's above currently latest known.
    pub fn try_save_latest_known(&mut self, height: BlockHeight) -> Result<(), Error> {
        let latest_known = self.chain_store.get_latest_known().ok();
        if latest_known.is_none() || height > latest_known.unwrap().height {
            self.chain_store
                .save_latest_known(LatestKnown { height, seen: to_timestamp(Utc::now()) })?;
        }
        Ok(())
    }

    #[cfg(feature = "test_features")]
    pub fn adv_save_latest_known(&mut self, height: BlockHeight) -> Result<(), Error> {
        let header = self.get_block_header_by_height(height)?;
        let tip = Tip::from_header(&header);
        self.chain_store
            .save_latest_known(LatestKnown { height, seen: to_timestamp(Utc::now()) })?;
        self.save_head(&tip)?;
        Ok(())
    }

    /// Save block.
    pub fn save_block(&mut self, block: Block) {
        debug_assert!(self.chain_store_cache_update.block.is_none());
        self.chain_store_cache_update.block = Some(block);
    }

    /// Save post applying chunk extra info.
    pub fn save_chunk_extra(
        &mut self,
        block_hash: &CryptoHash,
        shard_uid: &ShardUId,
        chunk_extra: ChunkExtra,
    ) {
        self.chain_store_cache_update
            .chunk_extras
            .insert((*block_hash, *shard_uid), Arc::new(chunk_extra));
    }

    pub fn save_chunk(&mut self, chunk: ShardChunk) {
        for transaction in chunk.to_transactions() {
            self.chain_store_cache_update
                .transactions
                .insert(transaction.get_hash(), Arc::new(transaction.clone()));
        }
        for receipt in chunk.prev_outgoing_receipts() {
            self.chain_store_cache_update
                .receipts
                .insert(*receipt.receipt_id(), Arc::new(receipt.clone()));
        }
        self.chain_store_cache_update.chunks.insert(chunk.chunk_hash(), Arc::new(chunk));
    }

    pub fn save_partial_chunk(&mut self, partial_chunk: Arc<PartialEncodedChunk>) {
        self.chain_store_cache_update
            .partial_chunks
            .insert(partial_chunk.chunk_hash(), partial_chunk);
    }

    pub fn save_block_merkle_tree(
        &mut self,
        block_hash: CryptoHash,
        block_merkle_tree: PartialMerkleTree,
    ) {
        self.chain_store_cache_update
            .block_merkle_tree
            .insert(block_hash, Arc::new(block_merkle_tree));
    }

    fn update_and_save_block_merkle_tree(&mut self, header: &BlockHeader) -> Result<(), Error> {
        if header.is_genesis() {
            self.save_block_merkle_tree(*header.hash(), PartialMerkleTree::default());
        } else {
            let prev_hash = header.prev_hash();
            let old_merkle_tree = self.get_block_merkle_tree(prev_hash)?;
            let mut new_merkle_tree = PartialMerkleTree::clone(&old_merkle_tree);
            new_merkle_tree.insert(*prev_hash);
            self.save_block_merkle_tree(*header.hash(), new_merkle_tree);
        }
        Ok(())
    }

    pub fn save_block_header(&mut self, header: BlockHeader) -> Result<(), Error> {
        self.update_and_save_block_merkle_tree(&header)?;
        self.chain_store_cache_update.headers.insert(*header.hash(), header);
        Ok(())
    }

    pub fn save_next_block_hash(&mut self, hash: &CryptoHash, next_hash: CryptoHash) {
        self.chain_store_cache_update.next_block_hashes.insert(*hash, next_hash);
    }

    pub fn save_epoch_light_client_block(
        &mut self,
        epoch_hash: &CryptoHash,
        light_client_block: LightClientBlockView,
    ) {
        self.chain_store_cache_update
            .epoch_light_client_blocks
            .insert(*epoch_hash, Arc::new(light_client_block));
    }

    // save the outgoing receipts generated by chunk from block `hash` for shard `shard_id`
    pub fn save_outgoing_receipt(
        &mut self,
        hash: &CryptoHash,
        shard_id: ShardId,
        outgoing_receipts: Vec<Receipt>,
    ) {
        self.chain_store_cache_update
            .outgoing_receipts
            .insert((*hash, shard_id), Arc::new(outgoing_receipts));
    }

    pub fn save_incoming_receipt(
        &mut self,
        hash: &CryptoHash,
        shard_id: ShardId,
        receipt_proof: Arc<Vec<ReceiptProof>>,
    ) {
        self.chain_store_cache_update.incoming_receipts.insert((*hash, shard_id), receipt_proof);
    }

    pub fn save_outcomes_with_proofs(
        &mut self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
        outcomes: Vec<ExecutionOutcomeWithId>,
        proofs: Vec<MerklePath>,
    ) {
        let mut outcome_ids = Vec::with_capacity(outcomes.len());
        for (outcome_with_id, proof) in outcomes.into_iter().zip(proofs.into_iter()) {
            outcome_ids.push(outcome_with_id.id);
            self.chain_store_cache_update.outcomes.insert(
                (outcome_with_id.id, *block_hash),
                ExecutionOutcomeWithProof { outcome: outcome_with_id.outcome, proof },
            );
        }
        self.chain_store_cache_update.outcome_ids.insert((*block_hash, shard_id), outcome_ids);
    }

    pub fn save_trie_changes(&mut self, block_hash: CryptoHash, trie_changes: WrappedTrieChanges) {
        self.trie_changes.push((block_hash, trie_changes));
    }

    pub fn save_state_transition_data(
        &mut self,
        block_hash: CryptoHash,
        shard_id: ShardId,
        partial_storage: Option<PartialStorage>,
        applied_receipts_hash: CryptoHash,
        contract_updates: ContractUpdates,
    ) {
        if let Some(partial_storage) = partial_storage {
            let ContractUpdates { contract_accesses, contract_deploys } = contract_updates;
            self.state_transition_data.insert(
                (block_hash, shard_id),
                StoredChunkStateTransitionData::V1(StoredChunkStateTransitionDataV1 {
                    base_state: partial_storage.nodes,
                    receipts_hash: applied_receipts_hash,
                    contract_accesses: contract_accesses.into_iter().collect(),
                    contract_deploys: contract_deploys.into_iter().map(|c| c.into()).collect(),
                }),
            );
        }
    }

    pub fn add_block_to_catchup(&mut self, prev_hash: CryptoHash, block_hash: CryptoHash) {
        self.add_blocks_to_catchup.push((prev_hash, block_hash));
    }

    pub fn remove_block_to_catchup(&mut self, prev_hash: CryptoHash, hash: CryptoHash) {
        self.remove_blocks_to_catchup.push((prev_hash, hash));
    }

    pub fn remove_prev_block_to_catchup(&mut self, hash: CryptoHash) {
        self.remove_prev_blocks_to_catchup.push(hash);
    }

    pub fn add_state_sync_info(&mut self, info: StateSyncInfo) {
        self.add_state_sync_infos.push(info);
    }

    pub fn remove_state_sync_info(&mut self, hash: CryptoHash) {
        self.remove_state_sync_infos.push(hash);
    }

    pub fn save_invalid_chunk(&mut self, chunk: EncodedShardChunk) {
        self.chain_store_cache_update.invalid_chunks.insert(chunk.chunk_hash(), Arc::new(chunk));
    }

    pub fn save_block_height_processed(&mut self, height: BlockHeight) {
        self.chain_store_cache_update.processed_block_heights.insert(height);
    }

    pub fn save_chunk_apply_stats(
        &mut self,
        block_hash: CryptoHash,
        shard_id: ShardId,
        stats: ChunkApplyStatsV0,
    ) {
        self.chunk_apply_stats.insert((block_hash, shard_id), ChunkApplyStats::V0(stats));
    }

    pub fn inc_block_refcount(&mut self, block_hash: &CryptoHash) -> Result<(), Error> {
        let refcount = match self.get_block_refcount(block_hash) {
            Ok(refcount) => refcount,
            Err(e) => match e {
                Error::DBNotFoundErr(_) => 0,
                _ => return Err(e),
            },
        };
        self.chain_store_cache_update.block_refcounts.insert(*block_hash, refcount + 1);
        Ok(())
    }

    pub fn dec_block_refcount(&mut self, block_hash: &CryptoHash) -> Result<(), Error> {
        let refcount = self.get_block_refcount(block_hash)?;
        if refcount > 0 {
            self.chain_store_cache_update.block_refcounts.insert(*block_hash, refcount - 1);
            Ok(())
        } else {
            debug_assert!(false, "refcount can not be negative");
            Err(Error::Other(format!("cannot decrease refcount for {:?}", block_hash)))
        }
    }

    pub fn reset_tail(&mut self) {
        self.tail = None;
        self.chunk_tail = None;
        self.fork_tail = None;
    }

    pub fn update_tail(&mut self, height: BlockHeight) -> Result<(), Error> {
        self.tail = Some(height);
        let genesis_height = self.get_genesis_height();
        // When fork tail is behind tail, it doesn't hurt to set it to tail for consistency.
        if self.fork_tail()? < height {
            self.fork_tail = Some(height);
        }

        let chunk_tail = self.chunk_tail()?;
        if chunk_tail == genesis_height {
            // For consistency, Chunk Tail should be set if Tail is set
            self.chunk_tail = Some(self.get_genesis_height());
        }
        Ok(())
    }

    pub fn update_fork_tail(&mut self, height: BlockHeight) {
        self.fork_tail = Some(height);
    }

    pub fn update_chunk_tail(&mut self, height: BlockHeight) {
        self.chunk_tail = Some(height);
    }

    /// Merge another StoreUpdate into this one
    pub fn merge(&mut self, store_update: StoreUpdate) {
        self.store_updates.push(store_update);
    }

    fn write_col_misc<T: BorshSerialize>(
        store_update: &mut StoreUpdate,
        key: &[u8],
        value: &mut Option<T>,
    ) -> Result<(), Error> {
        if let Some(t) = value.take() {
            store_update.set_ser(DBCol::BlockMisc, key, &t)?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", target = "store", "ChainUpdate::finalize", skip_all)]
    fn finalize(&mut self) -> Result<StoreUpdate, Error> {
        let mut store_update = self.store().store_update();
        {
            let _span = tracing::trace_span!(target: "store", "write_col_misc").entered();
            Self::write_col_misc(&mut store_update, HEAD_KEY, &mut self.head)?;
            Self::write_col_misc(&mut store_update, TAIL_KEY, &mut self.tail)?;
            Self::write_col_misc(&mut store_update, CHUNK_TAIL_KEY, &mut self.chunk_tail)?;
            Self::write_col_misc(&mut store_update, FORK_TAIL_KEY, &mut self.fork_tail)?;
            Self::write_col_misc(&mut store_update, HEADER_HEAD_KEY, &mut self.header_head)?;
            Self::write_col_misc(&mut store_update, FINAL_HEAD_KEY, &mut self.final_head)?;
            Self::write_col_misc(
                &mut store_update,
                LARGEST_TARGET_HEIGHT_KEY,
                &mut self.largest_target_height,
            )?;
        }
        {
            let _span = tracing::trace_span!(target: "store", "write_block").entered();
            if let Some(block) = &self.chain_store_cache_update.block {
                let mut map = HashMap::clone(
                    self.chain_store
                        .get_all_block_hashes_by_height(block.header().height())?
                        .as_ref(),
                );
                map.entry(*block.header().epoch_id())
                    .or_insert_with(|| HashSet::new())
                    .insert(*block.hash());
                store_update.set_ser(
                    DBCol::BlockPerHeight,
                    &index_to_bytes(block.header().height()),
                    &map,
                )?;
                self.chain_store_cache_update
                    .block_hash_per_height
                    .insert(block.header().height(), map);
                store_update.insert_ser(DBCol::Block, block.hash().as_ref(), block)?;
            }
            // This is a BTreeMap because the update_sync_hashes() calls below must be done in order of height
            let mut headers_by_height: BTreeMap<BlockHeight, Vec<&BlockHeader>> = BTreeMap::new();
            for (hash, header) in &self.chain_store_cache_update.headers {
                if self.chain_store.get_block_header(hash).is_ok() {
                    // No need to add same Header once again
                    continue;
                }
                headers_by_height.entry(header.height()).or_default().push(header);
                store_update.insert_ser(DBCol::BlockHeader, hash.as_ref(), header)?;
            }
            for (height, headers) in headers_by_height {
                let mut hash_set = match self.chain_store.get_all_header_hashes_by_height(height) {
                    Ok(hashes) => hashes,
                    Err(Error::DBNotFoundErr(_)) => HashSet::with_capacity(headers.len()),
                    Err(e) => return Err(e),
                };
                hash_set.extend(headers.iter().map(|header| *header.hash()));
                store_update.set_ser(
                    DBCol::HeaderHashesByHeight,
                    &index_to_bytes(height),
                    &hash_set,
                )?;
                for header in headers {
                    crate::state_sync::update_sync_hashes(self, &mut store_update, header)?;
                }
            }
            for ((block_hash, shard_uid), chunk_extra) in
                &self.chain_store_cache_update.chunk_extras
            {
                store_update.set_ser(
                    DBCol::ChunkExtra,
                    &get_block_shard_uid(block_hash, shard_uid),
                    chunk_extra,
                )?;
            }
        }

        {
            let _span = tracing::trace_span!(target: "store", "write_chunk").entered();

            let mut chunk_hashes_by_height: HashMap<BlockHeight, HashSet<ChunkHash>> =
                HashMap::new();
            for (chunk_hash, chunk) in &self.chain_store_cache_update.chunks {
                if self.chain_store.chunk_exists(chunk_hash)? {
                    // No need to add same Chunk once again
                    continue;
                }

                let height_created = chunk.height_created();
                match chunk_hashes_by_height.entry(height_created) {
                    Entry::Occupied(mut entry) => {
                        entry.get_mut().insert(chunk_hash.clone());
                    }
                    Entry::Vacant(entry) => {
                        let mut hash_set =
                            match self.chain_store.get_all_chunk_hashes_by_height(height_created) {
                                Ok(hash_set) => hash_set.clone(),
                                Err(_) => HashSet::new(),
                            };
                        hash_set.insert(chunk_hash.clone());
                        entry.insert(hash_set);
                    }
                };

                // Increase transaction refcounts for all included txs
                for tx in chunk.to_transactions() {
                    let bytes = borsh::to_vec(&tx).expect("Borsh cannot fail");
                    store_update.increment_refcount(
                        DBCol::Transactions,
                        tx.get_hash().as_ref(),
                        &bytes,
                    );
                }

                store_update.insert_ser(DBCol::Chunks, chunk_hash.as_ref(), chunk)?;
            }
            for (height, hash_set) in chunk_hashes_by_height {
                store_update.set_ser(
                    DBCol::ChunkHashesByHeight,
                    &index_to_bytes(height),
                    &hash_set,
                )?;
            }
            for (chunk_hash, partial_chunk) in &self.chain_store_cache_update.partial_chunks {
                store_update.insert_ser(
                    DBCol::PartialChunks,
                    chunk_hash.as_ref(),
                    partial_chunk,
                )?;

                // We'd like the Receipts column to be exactly the same collection of receipts as
                // the partial encoded chunks. This way, if we only track a subset of shards, we
                // can still have all the incoming receipts for the shards we do track.
                for receipts in partial_chunk.prev_outgoing_receipts() {
                    for receipt in &receipts.0 {
                        let bytes = borsh::to_vec(&receipt).expect("Borsh cannot fail");
                        store_update.increment_refcount(
                            DBCol::Receipts,
                            receipt.get_hash().as_ref(),
                            &bytes,
                        );
                    }
                }
            }
        }

        for (height, hash) in &self.chain_store_cache_update.height_to_hashes {
            if let Some(hash) = hash {
                store_update.set_ser(DBCol::BlockHeight, &index_to_bytes(*height), hash)?;
            } else {
                store_update.delete(DBCol::BlockHeight, &index_to_bytes(*height));
            }
        }
        for (block_hash, next_hash) in &self.chain_store_cache_update.next_block_hashes {
            store_update.set_ser(DBCol::NextBlockHashes, block_hash.as_ref(), next_hash)?;
        }
        for (epoch_hash, light_client_block) in
            &self.chain_store_cache_update.epoch_light_client_blocks
        {
            store_update.set_ser(
                DBCol::EpochLightClientBlocks,
                epoch_hash.as_ref(),
                light_client_block,
            )?;
        }
        {
            let _span =
                tracing::trace_span!(target: "store", "write_incoming_and_outgoing_receipts")
                    .entered();

            for ((block_hash, shard_id), receipt) in
                &self.chain_store_cache_update.outgoing_receipts
            {
                store_update.set_ser(
                    DBCol::OutgoingReceipts,
                    &get_block_shard_id(block_hash, *shard_id),
                    receipt,
                )?;
            }
            for ((block_hash, shard_id), receipt) in
                &self.chain_store_cache_update.incoming_receipts
            {
                store_update.set_ser(
                    DBCol::IncomingReceipts,
                    &get_block_shard_id(block_hash, *shard_id),
                    receipt,
                )?;
            }
        }

        {
            let _span = tracing::trace_span!(target: "store", "write_outcomes").entered();

            for ((outcome_id, block_hash), outcome_with_proof) in
                &self.chain_store_cache_update.outcomes
            {
                store_update.insert_ser(
                    DBCol::TransactionResultForBlock,
                    &get_outcome_id_block_hash(outcome_id, block_hash),
                    &outcome_with_proof,
                )?;
            }
            for ((block_hash, shard_id), ids) in &self.chain_store_cache_update.outcome_ids {
                store_update.set_ser(
                    DBCol::OutcomeIds,
                    &get_block_shard_id(block_hash, *shard_id),
                    &ids,
                )?;
            }
        }

        for (block_hash, refcount) in &self.chain_store_cache_update.block_refcounts {
            store_update.set_ser(DBCol::BlockRefCount, block_hash.as_ref(), refcount)?;
        }
        for (block_hash, block_merkle_tree) in &self.chain_store_cache_update.block_merkle_tree {
            store_update.set_ser(DBCol::BlockMerkleTree, block_hash.as_ref(), block_merkle_tree)?;
        }
        for (block_ordinal, block_hash) in &self.chain_store_cache_update.block_ordinal_to_hash {
            store_update.set_ser(
                DBCol::BlockOrdinal,
                &index_to_bytes(*block_ordinal),
                block_hash,
            )?;
        }

        // Convert trie changes to database ops for trie nodes.
        // Create separate store update for deletions, because we want to update cache and don't want to remove nodes
        // from the store.
        {
            let _span = tracing::trace_span!(target: "store", "write_trie_changes").entered();
            let mut deletions_store_update = self.store().trie_store().store_update();
            for (block_hash, mut wrapped_trie_changes) in self.trie_changes.drain(..) {
                wrapped_trie_changes.apply_mem_changes();
                wrapped_trie_changes.insertions_into(&mut store_update.trie_store_update());
                wrapped_trie_changes.deletions_into(&mut deletions_store_update);
                wrapped_trie_changes
                    .state_changes_into(&block_hash, &mut store_update.trie_store_update());

                if self.chain_store.save_trie_changes {
                    wrapped_trie_changes
                        .trie_changes_into(&block_hash, &mut store_update.trie_store_update());
                }
            }

            for ((block_hash, shard_id), state_transition_data) in
                self.state_transition_data.drain()
            {
                store_update.set_ser(
                    DBCol::StateTransitionData,
                    &get_block_shard_id(&block_hash, shard_id),
                    &state_transition_data,
                )?;
            }
        }
        {
            let _span = tracing::trace_span!(target: "store", "write_catchup").entered();

            let mut affected_catchup_blocks = HashSet::new();
            for (prev_hash, hash) in self.remove_blocks_to_catchup.drain(..) {
                assert!(!affected_catchup_blocks.contains(&prev_hash));
                if affected_catchup_blocks.contains(&prev_hash) {
                    return Err(Error::Other(
                        "Multiple changes to the store affect the same catchup block".to_string(),
                    ));
                }
                affected_catchup_blocks.insert(prev_hash);

                let mut prev_table =
                    self.chain_store.get_blocks_to_catchup(&prev_hash).unwrap_or_else(|_| vec![]);

                let mut remove_idx = prev_table.len();
                for (i, val) in prev_table.iter().enumerate() {
                    if *val == hash {
                        remove_idx = i;
                    }
                }

                assert_ne!(remove_idx, prev_table.len());
                prev_table.swap_remove(remove_idx);

                if !prev_table.is_empty() {
                    store_update.set_ser(
                        DBCol::BlocksToCatchup,
                        prev_hash.as_ref(),
                        &prev_table,
                    )?;
                } else {
                    store_update.delete(DBCol::BlocksToCatchup, prev_hash.as_ref());
                }
            }
            for prev_hash in self.remove_prev_blocks_to_catchup.drain(..) {
                assert!(!affected_catchup_blocks.contains(&prev_hash));
                if affected_catchup_blocks.contains(&prev_hash) {
                    return Err(Error::Other(
                        "Multiple changes to the store affect the same catchup block".to_string(),
                    ));
                }
                affected_catchup_blocks.insert(prev_hash);

                store_update.delete(DBCol::BlocksToCatchup, prev_hash.as_ref());
            }
            for (prev_hash, new_hash) in self.add_blocks_to_catchup.drain(..) {
                assert!(!affected_catchup_blocks.contains(&prev_hash));
                if affected_catchup_blocks.contains(&prev_hash) {
                    return Err(Error::Other(
                        "Multiple changes to the store affect the same catchup block".to_string(),
                    ));
                }
                affected_catchup_blocks.insert(prev_hash);

                let mut prev_table =
                    self.chain_store.get_blocks_to_catchup(&prev_hash).unwrap_or_else(|_| vec![]);
                prev_table.push(new_hash);
                store_update.set_ser(DBCol::BlocksToCatchup, prev_hash.as_ref(), &prev_table)?;
            }
        }

        for state_sync_info in self.add_state_sync_infos.drain(..) {
            store_update.set_ser(
                DBCol::StateDlInfos,
                state_sync_info.epoch_first_block().as_ref(),
                &state_sync_info,
            )?;
        }
        for hash in self.remove_state_sync_infos.drain(..) {
            store_update.delete(DBCol::StateDlInfos, hash.as_ref());
        }
        for (chunk_hash, chunk) in &self.chain_store_cache_update.invalid_chunks {
            store_update.insert_ser(DBCol::InvalidChunks, chunk_hash.as_ref(), chunk)?;
        }
        for block_height in &self.chain_store_cache_update.processed_block_heights {
            store_update.set_ser(
                DBCol::ProcessedBlockHeights,
                &index_to_bytes(*block_height),
                &(),
            )?;
        }
        for ((block_hash, shard_id), stats) in &self.chunk_apply_stats {
            store_update.set_ser(
                DBCol::ChunkApplyStats,
                &get_block_shard_id(block_hash, *shard_id),
                stats,
            )?;
        }
        for other in self.store_updates.drain(..) {
            store_update.merge(other);
        }
        Ok(store_update)
    }

    #[tracing::instrument(level = "debug", target = "store", "ChainStoreUpdate::commit", skip_all)]
    pub fn commit(mut self) -> Result<(), Error> {
        let store_update = self.finalize()?;
        store_update.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use near_async::time::Clock;
    use std::sync::Arc;

    use crate::test_utils::get_chain;
    use near_primitives::errors::InvalidTxError;
    use near_primitives::test_utils::TestBlockBuilder;
    use near_primitives::test_utils::create_test_signer;

    #[test]
    fn test_tx_validity_long_fork() {
        let mut chain = get_chain(Clock::real());
        chain.set_transaction_validity_period(5);
        let genesis = chain.get_block_by_height(0).unwrap();
        let signer = Arc::new(create_test_signer("test1"));
        let short_fork = [TestBlockBuilder::new(Clock::real(), &genesis, signer.clone()).build()];
        let mut store_update = chain.mut_chain_store().store_update();
        store_update.save_block_header(short_fork[0].header().clone()).unwrap();
        store_update.commit().unwrap();

        let short_fork_head = short_fork[0].header().clone();
        assert!(
            chain
                .mut_chain_store()
                .check_transaction_validity_period(&short_fork_head, genesis.hash(),)
                .is_ok()
        );
        let mut long_fork = vec![];
        let mut prev_block = genesis;
        for i in 1..(chain.transaction_validity_period() + 3) {
            let mut store_update = chain.mut_chain_store().store_update();
            let block =
                TestBlockBuilder::new(Clock::real(), &prev_block, signer.clone()).height(i).build();
            prev_block = block.clone();
            store_update.save_block_header(block.header().clone()).unwrap();
            store_update.update_height(block.header().height(), *block.hash()).unwrap();
            long_fork.push(block);
            store_update.commit().unwrap();
        }
        let valid_base_hash = long_fork[1].hash();
        let cur_header = &long_fork.last().unwrap().header();
        assert!(
            chain
                .mut_chain_store()
                .check_transaction_validity_period(cur_header, valid_base_hash)
                .is_ok()
        );
        let invalid_base_hash = long_fork[0].hash();
        assert_eq!(
            chain
                .mut_chain_store()
                .check_transaction_validity_period(cur_header, invalid_base_hash),
            Err(InvalidTxError::Expired)
        );
    }

    #[test]
    fn test_tx_validity_normal_case() {
        let mut chain = get_chain(Clock::real());
        chain.set_transaction_validity_period(5);
        let genesis = chain.get_block_by_height(0).unwrap();
        let signer = Arc::new(create_test_signer("test1"));
        let mut blocks = vec![];
        let mut prev_block = genesis;
        for i in 1..(chain.transaction_validity_period() + 2) {
            let mut store_update = chain.mut_chain_store().store_update();
            let block =
                TestBlockBuilder::new(Clock::real(), &prev_block, signer.clone()).height(i).build();
            prev_block = block.clone();
            store_update.save_block_header(block.header().clone()).unwrap();
            store_update.update_height(block.header().height(), *block.hash()).unwrap();
            blocks.push(block);
            store_update.commit().unwrap();
        }
        let valid_base_hash = blocks[1].hash();
        let cur_header = &blocks.last().unwrap().header();
        assert!(
            chain
                .chain_store()
                .check_transaction_validity_period(cur_header, valid_base_hash,)
                .is_ok()
        );
        let new_block = TestBlockBuilder::new(Clock::real(), &blocks.last().unwrap(), signer)
            .height(chain.transaction_validity_period() + 3)
            .build();

        let mut store_update = chain.mut_chain_store().store_update();
        store_update.save_block_header(new_block.header().clone()).unwrap();
        store_update.update_height(new_block.header().height(), *new_block.hash()).unwrap();
        store_update.commit().unwrap();
        assert_eq!(
            chain
                .chain_store()
                .check_transaction_validity_period(new_block.header(), valid_base_hash,),
            Err(InvalidTxError::Expired)
        );
    }

    #[test]
    fn test_tx_validity_off_by_one() {
        let mut chain = get_chain(Clock::real());
        chain.set_transaction_validity_period(5);
        let genesis = chain.get_block_by_height(0).unwrap();
        let genesis_hash = *genesis.hash();
        let signer = Arc::new(create_test_signer("test1"));
        let mut short_fork = vec![];
        let mut prev_block = genesis.clone();
        for i in 1..(chain.transaction_validity_period() + 2) {
            let mut store_update = chain.mut_chain_store().store_update();
            let block =
                TestBlockBuilder::new(Clock::real(), &prev_block, signer.clone()).height(i).build();
            prev_block = block.clone();
            store_update.save_block_header(block.header().clone()).unwrap();
            short_fork.push(block);
            store_update.commit().unwrap();
        }

        let short_fork_head = short_fork.last().unwrap().header().clone();
        assert_eq!(
            chain
                .mut_chain_store()
                .check_transaction_validity_period(&short_fork_head, &genesis_hash),
            Err(InvalidTxError::Expired)
        );
        let mut long_fork = vec![];
        let mut prev_block = genesis;
        for i in 1..(chain.transaction_validity_period() * 5) {
            let mut store_update = chain.mut_chain_store().store_update();
            let block =
                TestBlockBuilder::new(Clock::real(), &prev_block, signer.clone()).height(i).build();
            prev_block = block.clone();
            store_update.save_block_header(block.header().clone()).unwrap();
            long_fork.push(block);
            store_update.commit().unwrap();
        }
        let long_fork_head = &long_fork.last().unwrap().header();
        assert_eq!(
            chain
                .mut_chain_store()
                .check_transaction_validity_period(long_fork_head, &genesis_hash,),
            Err(InvalidTxError::Expired)
        );
    }
}

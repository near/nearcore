use crate::client_actor::ClientActorInner;
use crate::metrics;
use near_async::futures::{AsyncComputationSpawner, AsyncComputationSpawnerExt};
use near_async::messaging::{CanSend, Handler};
use near_async::time::Clock;
use near_chain::types::Tip;
use near_chain::{BlockHeader, Chain, ChainStoreAccess, Error, MerkleProofAccess};
use near_chain_configs::EpochSyncConfig;
use near_client_primitives::types::{EpochSyncStatus, SyncStatus};
use near_crypto::Signature;
use near_epoch_manager::EpochManagerAdapter;
use near_network::client::{EpochSyncRequestMessage, EpochSyncResponseMessage};
use near_network::types::{
    HighestHeightPeerInfo, NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest,
};
use near_performance_metrics_macros::perf;
use near_primitives::block::{Approval, ApprovalInner, compute_bp_hash_from_validator_stakes};
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::epoch_sync::{
    CompressedEpochSyncProof, EpochSyncProof, EpochSyncProofCurrentEpochData,
    EpochSyncProofEpochData, EpochSyncProofLastEpochData, EpochSyncProofV1,
    should_use_versioned_bp_hash_format,
};
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, ApprovalStake, Balance, BlockHeight, BlockHeightDelta, EpochId,
};
use near_primitives::utils::compression::CompressedData;
use near_store::Store;
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::instrument;

pub struct EpochSync {
    clock: Clock,
    network_adapter: PeerManagerAdapter,
    genesis: BlockHeader,
    async_computation_spawner: Arc<dyn AsyncComputationSpawner>,
    config: EpochSyncConfig,
    /// The last epoch sync proof and the epoch ID it was computed for.
    /// We reuse the same proof as long as the current epoch ID is the same.
    last_epoch_sync_response_cache: Arc<Mutex<Option<(EpochId, CompressedEpochSyncProof)>>>,
    // See `my_own_epoch_sync_boundary_block_header()`.
    my_own_epoch_sync_boundary_block_header: Option<BlockHeader>,
}

impl EpochSync {
    pub fn new(
        clock: Clock,
        network_adapter: PeerManagerAdapter,
        genesis: BlockHeader,
        async_computation_spawner: Arc<dyn AsyncComputationSpawner>,
        config: EpochSyncConfig,
        store: &Store,
    ) -> Self {
        let my_own_epoch_sync_boundary_block_header = store
            .epoch_store()
            .get_epoch_sync_proof()
            .expect("IO error querying epoch sync proof")
            .map(|proof| proof.current_epoch.first_block_header_in_epoch);

        Self {
            clock,
            network_adapter,
            genesis,
            async_computation_spawner,
            config,
            last_epoch_sync_response_cache: Arc::new(Mutex::new(None)),
            my_own_epoch_sync_boundary_block_header,
        }
    }

    /// Returns the block hash of the first block of the epoch that this node was initially
    /// bootstrapped to using epoch sync, or None if this node was not bootstrapped using
    /// epoch sync.
    pub fn my_own_epoch_sync_boundary_block_header(&self) -> Option<&BlockHeader> {
        self.my_own_epoch_sync_boundary_block_header.as_ref()
    }

    /// Derives an epoch sync proof for a recent epoch, that can be directly used to bootstrap
    /// a new node or bring a far-behind node to a recent epoch.
    #[instrument(skip(store, cache))]
    pub fn derive_epoch_sync_proof(
        store: Store,
        transaction_validity_period: BlockHeightDelta,
        cache: Arc<Mutex<Option<(EpochId, CompressedEpochSyncProof)>>>,
    ) -> Result<CompressedEpochSyncProof, Error> {
        // Epoch sync initializes a new node with the first block of some epoch; we call that
        // epoch the "target epoch". In the context of talking about the proof or the newly
        // bootstrapped node, it is also called the "current epoch".
        let target_epoch_last_block_hash =
            Self::find_target_epoch_to_produce_proof_for(&store, transaction_validity_period)?;

        let chain_store = store.chain_store();
        let target_epoch_last_block_header =
            chain_store.get_block_header(&target_epoch_last_block_hash)?;
        let target_epoch_second_last_block_header =
            chain_store.get_block_header(target_epoch_last_block_header.prev_hash())?;

        let mut guard = cache.lock();
        if let Some((epoch_id, proof)) = &*guard {
            if epoch_id == target_epoch_last_block_header.epoch_id() {
                return Ok(proof.clone());
            }
        }
        // We're purposefully not releasing the lock here. This is so that if the cache
        // is out of date, only one thread should be doing the computation.
        let proof = Self::derive_epoch_sync_proof_from_last_final_block(
            store,
            target_epoch_second_last_block_header,
        );
        let (proof, _) = match CompressedEpochSyncProof::encode(&proof?) {
            Ok(proof) => proof,
            Err(err) => {
                return Err(Error::Other(format!(
                    "Failed to compress epoch sync proof: {:?}",
                    err
                )));
            }
        };
        metrics::EPOCH_SYNC_LAST_GENERATED_COMPRESSED_PROOF_SIZE.set(proof.size_bytes() as i64);
        *guard = Some((*target_epoch_last_block_header.epoch_id(), proof.clone()));
        Ok(proof)
    }

    /// Figures out which target epoch we should produce a proof for, based on the current
    /// state of the blockchain.
    ///
    /// The basic requirement for picking the target epoch is that its first block must be
    /// final. That's just so that we don't have to deal with any forks. Therefore, it is
    /// sufficient to pick whatever epoch the current final block is in. However, there are
    /// additional considerations:
    ///  - Because state sync also requires some previous headers to be available (depending
    ///    on how many chunks were missing), if we pick the most recent epoch, after the node
    ///    finishes epoch sync and transitions to state sync, it would not have these headers
    ///    before the epoch. Therefore, for this purpose, it's convenient for epoch sync to not
    ///    pick the most recent epoch. This would ensure that state sync has a whole epoch of
    ///    headers before the epoch it's syncing to.
    ///  - We also need to have enough block headers to check for transaction_validity_period.
    ///    Therefore, we need find the latest epoch for which we would have at least that many
    ///    headers.
    ///
    /// This function returns the hash of the last final block (third last block) of the target
    /// epoch.
    fn find_target_epoch_to_produce_proof_for(
        store: &Store,
        transaction_validity_period: BlockHeightDelta,
    ) -> Result<CryptoHash, Error> {
        let chain_store = store.chain_store();
        let epoch_store = store.epoch_store();

        let tip = chain_store.final_head()?;
        let current_epoch_start_height = epoch_store.get_epoch_start(&tip.epoch_id)?;
        let next_next_epoch_id = tip.next_epoch_id;
        // Last block hash of the target epoch is the same as the next next EpochId.
        // That's a general property for Near's epochs.
        let mut target_epoch_last_block_hash = next_next_epoch_id.0;
        Ok(loop {
            let block_info = epoch_store.get_block_info(&target_epoch_last_block_hash)?;
            let target_epoch_first_block_header =
                chain_store.get_block_header(block_info.epoch_first_block())?;
            // Check that we have enough headers to check for transaction_validity_period.
            // We check this against the current epoch's start height, because when we state
            // sync, we will sync against the current epoch, and starting from the point we
            // state sync is when we'll need to be able to check for transaction validity.
            if target_epoch_first_block_header.height() + transaction_validity_period
                > current_epoch_start_height
            {
                target_epoch_last_block_hash = *target_epoch_first_block_header.prev_hash();
            } else {
                break target_epoch_last_block_hash;
            }
        })
    }

    /// Derives an epoch sync proof using a target epoch whose last final block is given
    /// (actually it's the block after that, so that we can find the approvals).
    fn derive_epoch_sync_proof_from_last_final_block(
        store: Store,
        next_block_header_after_last_final_block_of_current_epoch: BlockHeader,
    ) -> Result<EpochSyncProof, Error> {
        let chain_store = store.chain_store();
        let epoch_store = store.epoch_store();

        let last_final_block_header_in_current_epoch = chain_store.get_block_header(
            next_block_header_after_last_final_block_of_current_epoch.prev_hash(),
        )?;
        let current_epoch = *last_final_block_header_in_current_epoch.epoch_id();
        let current_epoch_info = epoch_store.get_epoch_info(&current_epoch)?;
        let next_epoch = *last_final_block_header_in_current_epoch.next_epoch_id();
        let next_epoch_info = epoch_store.get_epoch_info(&next_epoch)?;
        let genesis_epoch_info = epoch_store.get_epoch_info(&EpochId::default())?;

        // If we have an existing (possibly and likely outdated) EpochSyncProof stored on disk,
        // the last epoch we have a proof for is the "previous epoch" included in that EpochSyncProof.
        // Otherwise, the last epoch we have a "proof" for is the genesis epoch.
        let existing_epoch_sync_proof = epoch_store.get_epoch_sync_proof()?;
        let last_epoch_we_have_proof_for = existing_epoch_sync_proof
            .as_ref()
            .and_then(|existing_proof| {
                existing_proof
                    .all_epochs
                    .last()
                    .map(|last_epoch| *last_epoch.last_final_block_header.epoch_id())
            })
            .unwrap_or_else(EpochId::default);
        let last_epoch_height_we_have_proof_for = existing_epoch_sync_proof
            .as_ref()
            .map(|existing_proof| existing_proof.last_epoch.next_epoch_info.epoch_height())
            .unwrap_or_else(|| genesis_epoch_info.epoch_height() + 1);

        // If the proof we stored is for the same epoch as current or older, then just return that.
        if current_epoch_info.epoch_height() <= last_epoch_height_we_have_proof_for {
            if let Some(existing_proof) = existing_epoch_sync_proof {
                return Ok(EpochSyncProof::V1(existing_proof));
            }
            // Corner case for if the current epoch is genesis or right after genesis.
            return Err(Error::Other("Not enough epochs after genesis to epoch sync".to_string()));
        }

        let all_epochs_since_last_proof = Self::get_all_epoch_proofs_in_range(
            &store,
            last_epoch_we_have_proof_for,
            next_epoch,
            &last_final_block_header_in_current_epoch,
            next_block_header_after_last_final_block_of_current_epoch.approvals().to_vec(),
        )?;
        if all_epochs_since_last_proof.len() < 2 {
            return Err(Error::Other("Not enough epochs after genesis to epoch sync".to_string()));
        }
        let prev_epoch = *all_epochs_since_last_proof
            .get(all_epochs_since_last_proof.len() - 2)
            .unwrap()
            .last_final_block_header
            .epoch_id();
        let prev_epoch_info = epoch_store.get_epoch_info(&prev_epoch)?;
        let last_block_of_prev_epoch = chain_store.get_block_header(&next_epoch.0)?;
        let last_block_info_of_prev_epoch =
            epoch_store.get_block_info(last_block_of_prev_epoch.hash())?;
        let second_last_block_of_prev_epoch =
            chain_store.get_block_header(last_block_of_prev_epoch.prev_hash())?;
        let second_last_block_info_of_prev_epoch =
            epoch_store.get_block_info(last_block_of_prev_epoch.prev_hash())?;
        let first_block_info_of_prev_epoch =
            epoch_store.get_block_info(last_block_info_of_prev_epoch.epoch_first_block())?;
        let block_info_for_final_block_of_current_epoch =
            epoch_store.get_block_info(last_final_block_header_in_current_epoch.hash())?;
        let first_block_of_current_epoch = chain_store
            .get_block_header(block_info_for_final_block_of_current_epoch.epoch_first_block())?;

        let merkle_proof_for_first_block_of_current_epoch = store
            .compute_past_block_proof_in_merkle_tree_of_later_block(
                first_block_of_current_epoch.hash(),
                last_final_block_header_in_current_epoch.hash(),
            )?;
        let partial_merkle_tree_for_first_block_of_current_epoch =
            chain_store.get_block_merkle_tree(first_block_of_current_epoch.hash())?;

        let all_epochs_including_old_proof = existing_epoch_sync_proof
            .map(|proof| proof.all_epochs)
            .unwrap_or_else(Vec::new)
            .into_iter()
            .chain(all_epochs_since_last_proof.into_iter())
            .collect();
        let proof = EpochSyncProofV1 {
            all_epochs: all_epochs_including_old_proof,
            last_epoch: EpochSyncProofLastEpochData {
                epoch_info: prev_epoch_info,
                next_epoch_info: current_epoch_info,
                next_next_epoch_info: next_epoch_info,
                first_block_in_epoch: first_block_info_of_prev_epoch,
                last_block_in_epoch: last_block_info_of_prev_epoch,
                second_last_block_in_epoch: second_last_block_info_of_prev_epoch,
            },
            current_epoch: EpochSyncProofCurrentEpochData {
                first_block_header_in_epoch: first_block_of_current_epoch,
                last_block_header_in_prev_epoch: last_block_of_prev_epoch,
                second_last_block_header_in_prev_epoch: second_last_block_of_prev_epoch,
                merkle_proof_for_first_block: merkle_proof_for_first_block_of_current_epoch,
                partial_merkle_tree_for_first_block:
                    partial_merkle_tree_for_first_block_of_current_epoch,
            },
        };

        Ok(EpochSyncProof::V1(proof))
    }

    /// Get all the past epoch data needed for epoch sync, between `after_epoch` and `next_epoch`
    /// (both exclusive). `current_epoch_any_header` is any block header in the current epoch,
    /// which is the epoch before `next_epoch`.
    #[instrument(skip(
        store,
        current_epoch_last_final_block_header,
        current_epoch_second_last_block_approvals
    ))]
    fn get_all_epoch_proofs_in_range(
        store: &Store,
        after_epoch: EpochId,
        next_epoch: EpochId,
        current_epoch_last_final_block_header: &BlockHeader,
        current_epoch_second_last_block_approvals: Vec<Option<Box<Signature>>>,
    ) -> Result<Vec<EpochSyncProofEpochData>, Error> {
        let chain_store = store.chain_store();
        let epoch_store = store.epoch_store();

        // We're going to get all the epochs and then figure out the correct chain of
        // epochs. The reason is that (1) epochs may, in very rare cases, have forks,
        // so we cannot just take all the epochs and assume their heights do not collide;
        // and (2) it is not easy to walk backwards from the last epoch; there's no
        // "give me the previous epoch" query. So instead, we use block header's
        // `next_epoch_id` to establish an epoch chain.
        let all_epoch_infos = epoch_store.iter_epoch_info().collect::<HashMap<_, _>>();

        // Collect the previous-epoch relationship based on block headers.
        // To get block headers for past epochs, we use the fact that the EpochId is the
        // same as the block hash of the last block two epochs ago. That works except for
        // the current epoch, whose last block doesn't exist yet, which is why we need
        // any arbitrary block header in the current epoch as a special case.
        let mut epoch_to_prev_epoch = HashMap::new();
        epoch_to_prev_epoch.insert(
            *current_epoch_last_final_block_header.next_epoch_id(),
            *current_epoch_last_final_block_header.epoch_id(),
        );
        for (epoch_id, _) in &all_epoch_infos {
            if let Ok(block) = chain_store.get_block_header(&epoch_id.0) {
                epoch_to_prev_epoch.insert(*block.next_epoch_id(), *block.epoch_id());
            }
        }

        // Now that we have the chain of previous epochs, walk from the last epoch backwards
        // towards the first epoch.
        let mut epoch_ids = vec![];
        let mut current_epoch = next_epoch;
        while current_epoch != after_epoch {
            let prev_epoch = epoch_to_prev_epoch.get(&current_epoch).ok_or_else(|| {
                Error::Other(format!("Could not find prev epoch for {:?}", current_epoch))
            })?;
            epoch_ids.push(current_epoch);
            current_epoch = *prev_epoch;
        }
        epoch_ids.reverse();

        // Now that we have all epochs, we can fetch the data needed for each epoch.
        let epochs = (0..epoch_ids.len() - 1)
            .into_par_iter()
            .map(|index| -> Result<EpochSyncProofEpochData, Error> {
                let next_epoch_id = epoch_ids[index + 1];
                let epoch_id = epoch_ids[index];
                let prev_epoch_id = if index == 0 { after_epoch } else { epoch_ids[index - 1] };

                let (last_final_block_header, approvals_for_last_final_block) = if index + 2
                    < epoch_ids.len()
                {
                    let next_next_epoch_id = epoch_ids[index + 2];
                    let last_block_header = chain_store.get_block_header(&next_next_epoch_id.0)?;
                    let second_last_block_header =
                        chain_store.get_block_header(last_block_header.prev_hash())?;
                    let third_last_block_header =
                        chain_store.get_block_header(second_last_block_header.prev_hash())?;
                    (third_last_block_header, second_last_block_header.approvals().to_vec())
                } else {
                    (
                        current_epoch_last_final_block_header.clone(),
                        current_epoch_second_last_block_approvals.clone(),
                    )
                };
                let prev_epoch_info = all_epoch_infos.get(&prev_epoch_id).ok_or_else(|| {
                    Error::Other(format!("Could not find epoch info for epoch {:?}", prev_epoch_id))
                })?;
                let epoch_info = all_epoch_infos.get(&epoch_id).ok_or_else(|| {
                    Error::Other(format!("Could not find epoch info for epoch {:?}", epoch_id))
                })?;
                let next_epoch_info = all_epoch_infos.get(&next_epoch_id).ok_or_else(|| {
                    Error::Other(format!("Could not find epoch info for epoch {:?}", next_epoch_id))
                })?;

                let this_epoch_block_producers = Self::get_epoch_info_block_producers(epoch_info);
                let next_epoch_block_producers =
                    Self::get_epoch_info_block_producers(next_epoch_info);
                let approvals_for_this_epoch_block_producers =
                    Self::get_approvals_for_this_epoch_block_producers(
                        &approvals_for_last_final_block,
                        &this_epoch_block_producers,
                        &next_epoch_block_producers,
                    );
                let use_versioned_bp_hash_format =
                    should_use_versioned_bp_hash_format(prev_epoch_info.protocol_version());

                Ok(EpochSyncProofEpochData {
                    block_producers: Self::get_epoch_info_block_producers(epoch_info),
                    use_versioned_bp_hash_format,
                    last_final_block_header,
                    this_epoch_endorsements_for_last_final_block:
                        approvals_for_this_epoch_block_producers,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(epochs)
    }

    /// Gets the ordered list of block producers and their stake from the EpochInfo.
    fn get_epoch_info_block_producers(epoch_info: &EpochInfo) -> Vec<ValidatorStake> {
        // The block producers settlement can actually have duplicates.
        // The list of block producers used to compute bp_hash is the deduplicated version
        // of this list, keeping the order of first appearance.
        let mut block_producers = Vec::new();
        let mut seen_validators = HashSet::new();
        for bp_index in epoch_info.block_producers_settlement() {
            if seen_validators.insert(*bp_index) {
                block_producers.push(epoch_info.get_validator(*bp_index));
            }
        }
        block_producers
    }

    /// Gets the ordered list of signatures within the approvals list in a block that correspond
    /// to this epoch's block producers. The given block is expected to require both the current
    /// and the next epoch's signatures. The returned list has the exact same length as this
    /// epoch's block producers (unlike the block's approvals list which may be shorter).
    fn get_approvals_for_this_epoch_block_producers(
        approvals: &[Option<Box<Signature>>],
        this_epoch_block_producers: &[ValidatorStake],
        next_epoch_block_producers: &[ValidatorStake],
    ) -> Vec<Option<Box<Signature>>> {
        let approvers = Self::get_dual_epoch_block_approvers_ordered(
            this_epoch_block_producers,
            next_epoch_block_producers,
        );
        let mut approver_to_signature = HashMap::new();
        for (index, approver) in approvers.into_iter().enumerate() {
            if let Some(Some(approval)) = approvals.get(index) {
                approver_to_signature.insert(approver.account_id, approval.clone());
            }
        }
        this_epoch_block_producers
            .iter()
            .map(|validator| approver_to_signature.get(validator.account_id()).cloned())
            .collect()
    }

    /// Computes the ordered list of block approvers for a block that requires signatures from both
    /// the current epoch's block producers and the next epoch's block producers.
    fn get_dual_epoch_block_approvers_ordered(
        current_block_producers: &[ValidatorStake],
        next_block_producers: &[ValidatorStake],
    ) -> Vec<ApprovalStake> {
        let mut settlement = current_block_producers.to_vec();
        let settlement_epoch_boundary = settlement.len();

        settlement.extend_from_slice(next_block_producers);

        let mut result = vec![];
        let mut validators: HashMap<AccountId, usize> = HashMap::default();
        for (ord, validator_stake) in settlement.into_iter().enumerate() {
            let account_id = validator_stake.account_id();
            match validators.get(account_id) {
                None => {
                    validators.insert(account_id.clone(), result.len());
                    result
                        .push(validator_stake.get_approval_stake(ord >= settlement_epoch_boundary));
                }
                Some(old_ord) => {
                    if ord >= settlement_epoch_boundary {
                        result[*old_ord].stake_next_epoch = validator_stake.stake();
                    };
                }
            };
        }
        result
    }

    /// Performs the epoch sync logic if applicable in the current state of the blockchain.
    /// This is periodically called by the client actor.
    pub fn run(
        &self,
        status: &mut SyncStatus,
        chain: &Chain,
        highest_height: BlockHeight,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Result<(), Error> {
        if self.config.disable_epoch_sync_for_bootstrapping {
            return Ok(());
        }
        let tip_height = chain.chain_store().header_head()?.height;
        if tip_height != chain.genesis().height() {
            // Epoch Sync only supports bootstrapping at genesis. This is because there is no reason
            // to use Epoch Sync on an already existing node; we would have to carefully delete old
            // data and then the result would be the same as if we just started the node from
            // scratch.
            return Ok(());
        }
        if tip_height + self.config.epoch_sync_horizon >= highest_height {
            return Ok(());
        }
        match status {
            SyncStatus::EpochSync(status) => {
                if status.attempt_time + self.config.timeout_for_epoch_sync < self.clock.now_utc() {
                    tracing::warn!("Epoch sync from {} timed out; retrying", status.source_peer_id);
                } else {
                    return Ok(());
                }
            }
            _ => {}
        }

        // TODO(#11976): Implement a more robust logic for picking a peer to request epoch sync from.
        let peer = highest_height_peers
            .choose(&mut rand::thread_rng())
            .ok_or_else(|| Error::Other("No peers to request epoch sync from".to_string()))?;

        tracing::info!(peer_id=?peer.peer_info.id, "Bootstrapping node via epoch sync");

        *status = SyncStatus::EpochSync(EpochSyncStatus {
            source_peer_id: peer.peer_info.id.clone(),
            source_peer_height: peer.highest_block_height,
            attempt_time: self.clock.now_utc(),
        });

        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::EpochSyncRequest { peer_id: peer.peer_info.id.clone() },
        ));

        Ok(())
    }

    pub fn apply_proof(
        &self,
        status: &mut SyncStatus,
        chain: &mut Chain,
        proof: EpochSyncProof,
        source_peer: PeerId,
        epoch_manager: &dyn EpochManagerAdapter,
    ) -> Result<(), Error> {
        let proof = proof.into_v1();
        if let SyncStatus::EpochSync(status) = status {
            if status.source_peer_id != source_peer {
                tracing::warn!("Ignoring epoch sync proof from unexpected peer: {}", source_peer);
                return Ok(());
            }
            if proof
                .current_epoch
                .first_block_header_in_epoch
                .height()
                .saturating_add(chain.epoch_length.max(chain.transaction_validity_period()))
                >= status.source_peer_height
            {
                tracing::error!(
                    "Ignoring epoch sync proof from peer {} that is too recent",
                    source_peer
                );
                return Ok(());
            }
            if proof
                .current_epoch
                .first_block_header_in_epoch
                .height()
                .saturating_add(self.config.epoch_sync_horizon)
                < status.source_peer_height
            {
                tracing::error!(
                    "Ignoring epoch sync proof from peer {} that is too old",
                    source_peer
                );
                return Ok(());
            }
        } else {
            tracing::warn!("Ignoring unexpected epoch sync proof from peer: {}", source_peer);
            return Ok(());
        }

        self.verify_proof(&proof, epoch_manager)?;

        let store = chain.chain_store.store();
        let mut store_update = store.store_update();

        // Store the EpochSyncProof, so that this node can derive a more recent EpochSyncProof
        // to facilitate epoch sync of other nodes.
        let proof = EpochSyncProof::V1(proof); // convert to avoid cloning
        store_update.epoch_store_update().set_epoch_sync_proof(&proof);
        let proof = proof.into_v1();

        let last_header = proof.current_epoch.first_block_header_in_epoch;

        // Save blocks and headers to the store.
        // Set the header head and final head.
        let mut chain_store_update = store.chain_store().store_update();

        for block_header in [
            &last_header,
            &proof.current_epoch.last_block_header_in_prev_epoch,
            &proof.current_epoch.second_last_block_header_in_prev_epoch,
            &proof.all_epochs.get(proof.all_epochs.len() - 2).unwrap().last_final_block_header,
        ] {
            chain_store_update.set_block_header_only(block_header);
            chain_store_update.update_block_header_hashes_by_height(block_header);
        }

        chain_store_update.set_header_head(&Tip::from_header(&last_header));
        chain_store_update.set_final_head(&Tip::from_header(&self.genesis));

        chain_store_update.commit()?;

        // Initialize the epoch manager with the last epoch.
        epoch_manager.init_after_epoch_sync(
            &mut store_update,
            proof.last_epoch.first_block_in_epoch,
            proof.last_epoch.second_last_block_in_epoch,
            proof.last_epoch.last_block_in_epoch.clone(),
            proof.last_epoch.last_block_in_epoch.epoch_id(),
            proof.last_epoch.epoch_info,
            last_header.epoch_id(),
            proof.last_epoch.next_epoch_info,
            last_header.next_epoch_id(),
            proof.last_epoch.next_next_epoch_info,
        )?;

        // At this point store contains headers of 3 last blocks of last past epoch
        // and header of the first block of current epoch.
        // At least the third last block of last past epoch is final.
        // It means that store contains header of last final block of the first block of current epoch.
        let last_header_last_finalized_height =
            store.chain_store().get_block_header(last_header.last_final_block())?.height();
        let mut first_block_info_in_epoch =
            BlockInfo::from_header(&last_header, last_header_last_finalized_height);
        // We need to populate fields below manually, as they are set to defaults by `BlockInfo::from_header`.
        *first_block_info_in_epoch.epoch_first_block_mut() = *last_header.hash();
        *first_block_info_in_epoch.epoch_id_mut() = *last_header.epoch_id();

        store_update.epoch_store_update().set_block_info(&first_block_info_in_epoch);
        store_update.chain_store_update().set_block_ordinal(
            proof.current_epoch.partial_merkle_tree_for_first_block.size(),
            last_header.hash(),
        );
        store_update
            .chain_store_update()
            .set_block_height(last_header.hash(), last_header.height());
        store_update.chain_store_update().set_block_merkle_tree(
            last_header.hash(),
            &proof.current_epoch.partial_merkle_tree_for_first_block,
        );

        store_update.commit()?;

        *status = SyncStatus::EpochSyncDone;
        tracing::info!(epoch_id=?last_header.epoch_id(), "Bootstrapped from epoch sync");

        Ok(())
    }

    fn verify_proof(
        &self,
        proof: &EpochSyncProofV1,
        epoch_manager: &dyn EpochManagerAdapter,
    ) -> Result<(), Error> {
        let EpochSyncProofV1 { all_epochs, last_epoch, current_epoch } = proof;
        if all_epochs.len() < 2 {
            return Err(Error::InvalidEpochSyncProof(
                "need at least two epochs in all_epochs".to_string(),
            ));
        }

        // Verify block producer handoff to the second epoch after genesis.
        let second_next_epoch_id_after_genesis = EpochId(*self.genesis.hash());
        let second_next_epoch_info_after_genesis =
            epoch_manager.get_epoch_info(&second_next_epoch_id_after_genesis)?;
        if all_epochs[0].block_producers
            != Self::get_epoch_info_block_producers(&second_next_epoch_info_after_genesis)
        {
            return Err(Error::InvalidEpochSyncProof(
                "invalid block producers for second epoch after genesis".to_string(),
            ));
        }
        Self::verify_final_block_endorsement(&all_epochs[0])?;

        // Verify the data of each epoch, in chronological order. When verifying each epoch,
        // we assume that the previous epoch has been verified (thereby giving correctness of all
        // epochs by induction.) For each epoch, we need to verify the following:
        //
        // - Its block producers. To verify this, we compare the previous epoch's last final block's
        //   next_bp_hash against the hash of the current epoch's block producers, taking into
        //   account the use_versioned_bp_hash_format flag.
        // - Its last final block. To verify this, we use the endorsements provided for the final
        //   block. What we verify is that more than 2/3 of the block producers of the current epoch
        //   have endorsed the final block.
        //
        // See the comments in `EpochSyncProofEpochData` for more detailed information.
        for epoch_index in 1..all_epochs.len() {
            let epoch = &all_epochs[epoch_index];
            let prev_epoch = &all_epochs[epoch_index - 1];
            if !Self::verify_block_producer_handoff(
                &epoch.block_producers,
                epoch.use_versioned_bp_hash_format,
                prev_epoch.last_final_block_header.next_bp_hash(),
            )? {
                return Err(Error::InvalidEpochSyncProof(format!(
                    "invalid block producer handoff to epoch index {}",
                    epoch_index
                )));
            }
            Self::verify_final_block_endorsement(epoch)?;
        }

        Self::verify_epoch_sync_data_hash(&last_epoch, &current_epoch.first_block_header_in_epoch)?;

        Self::verify_current_epoch_data(
            current_epoch,
            &all_epochs.last().unwrap().last_final_block_header,
        )?;
        Ok(())
    }

    fn verify_current_epoch_data(
        current_epoch: &EpochSyncProofCurrentEpochData,
        current_epoch_final_block_header: &BlockHeader,
    ) -> Result<(), Error> {
        // Verify first_block_header_in_epoch
        let first_block_header = &current_epoch.first_block_header_in_epoch;
        if !near_primitives::merkle::verify_hash(
            *current_epoch_final_block_header.block_merkle_root(),
            &current_epoch.merkle_proof_for_first_block,
            *first_block_header.hash(),
        ) {
            return Err(Error::InvalidEpochSyncProof(
                "invalid merkle_proof_for_first_block".to_string(),
            ));
        }

        // Verify partial_merkle_tree_for_first_block. The size needs to match to ensure that
        // the partial merkle tree is for the right block ordinal, and the partial tree itself
        // needs to be valid and have the correct root.
        //
        // Note that the block_ordinal in the header is 1-based, so we need to add 1 to the size.
        if current_epoch.partial_merkle_tree_for_first_block.size() + 1
            != first_block_header.block_ordinal()
        {
            return Err(Error::InvalidEpochSyncProof(
                "invalid size in partial_merkle_tree_for_first_block".to_string(),
            ));
        }

        if !current_epoch.partial_merkle_tree_for_first_block.is_well_formed()
            || current_epoch.partial_merkle_tree_for_first_block.root()
                != *first_block_header.block_merkle_root()
        {
            return Err(Error::InvalidEpochSyncProof(
                "invalid path in partial_merkle_tree_for_first_block".to_string(),
            ));
        }

        // Verify the two headers before the first block.
        if current_epoch.last_block_header_in_prev_epoch.hash()
            != current_epoch.first_block_header_in_epoch.prev_hash()
        {
            return Err(Error::InvalidEpochSyncProof(
                "invalid last_block_header_in_prev_epoch".to_string(),
            ));
        }
        if current_epoch.second_last_block_header_in_prev_epoch.hash()
            != current_epoch.last_block_header_in_prev_epoch.prev_hash()
        {
            return Err(Error::InvalidEpochSyncProof(
                "invalid second_last_block_header_in_prev_epoch".to_string(),
            ));
        }

        Ok(())
    }

    /// Verify epoch_sync_data_hash matches current_epoch_first_block_header's epoch_sync_data_hash.
    fn verify_epoch_sync_data_hash(
        last_epoch: &EpochSyncProofLastEpochData,
        current_epoch_first_block_header: &BlockHeader,
    ) -> Result<(), Error> {
        let epoch_sync_data_hash = CryptoHash::hash_borsh(&(
            &last_epoch.first_block_in_epoch,
            &last_epoch.second_last_block_in_epoch,
            &last_epoch.last_block_in_epoch,
            &last_epoch.epoch_info,
            &last_epoch.next_epoch_info,
            &last_epoch.next_next_epoch_info,
        ));
        let expected_epoch_sync_data_hash =
            current_epoch_first_block_header.epoch_sync_data_hash().ok_or_else(|| {
                Error::InvalidEpochSyncProof("missing epoch_sync_data_hash".to_string())
            })?;
        if epoch_sync_data_hash != expected_epoch_sync_data_hash {
            return Err(Error::InvalidEpochSyncProof("invalid epoch_sync_data_hash".to_string()));
        }

        Ok(())
    }

    /// Verifies that EpochSyncProofPastEpochData's block_producers is valid,
    /// returning true if it is.
    fn verify_block_producer_handoff(
        block_producers: &Vec<ValidatorStake>,
        use_versioned_bp_hash_format: bool,
        prev_epoch_next_bp_hash: &CryptoHash,
    ) -> Result<bool, Error> {
        let bp_hash =
            compute_bp_hash_from_validator_stakes(block_producers, use_versioned_bp_hash_format);
        Ok(bp_hash == *prev_epoch_next_bp_hash)
    }

    /// Verifies that the epoch's last_final_block_header is sufficiently endorsed by the current
    /// epoch's block producers.
    fn verify_final_block_endorsement(epoch: &EpochSyncProofEpochData) -> Result<(), Error> {
        Self::verify_block_endorsements(
            *(&epoch.last_final_block_header).hash(),
            (&epoch.last_final_block_header).height(),
            &epoch.block_producers,
            &epoch.this_epoch_endorsements_for_last_final_block,
        )
    }

    /// Verifies that the given block is endorsed properly, and with enough stake.
    fn verify_block_endorsements(
        prev_block_hash: CryptoHash,
        block_height: BlockHeight,
        block_producers: &[ValidatorStake],
        endorsements: &[Option<Box<Signature>>],
    ) -> Result<(), near_chain::Error> {
        if endorsements.len() != block_producers.len() {
            return Err(near_chain::Error::InvalidEpochSyncProof(format!(
                "Block {} should be provided with {} endorsements but has {}",
                block_height,
                block_producers.len(),
                endorsements.len()
            )));
        }

        let message_to_sign = Approval::get_data_for_sig(
            &ApprovalInner::Endorsement(prev_block_hash),
            block_height + 1,
        );

        let mut total_stake: Balance = 0;
        let mut endorsed_stake: Balance = 0;

        for (validator, may_be_signature) in block_producers.iter().zip(endorsements.iter()) {
            if let Some(signature) = may_be_signature {
                if !signature.verify(&message_to_sign, validator.public_key()) {
                    return Err(near_chain::Error::InvalidEpochSyncProof(format!(
                        "Invalid signature for block {} from validator {:?}",
                        block_height,
                        validator.account_id()
                    )));
                }
                endorsed_stake += validator.stake();
            }
            total_stake += validator.stake();
        }

        if endorsed_stake <= total_stake * 2 / 3 {
            return Err(near_chain::Error::InvalidEpochSyncProof(format!(
                "Block {} does not have enough endorsements",
                block_height
            )));
        }

        Ok(())
    }
}

impl Handler<EpochSyncRequestMessage> for ClientActorInner {
    #[perf]
    fn handle(&mut self, msg: EpochSyncRequestMessage) {
        if self.client.sync_handler.epoch_sync.config.ignore_epoch_sync_network_requests {
            // Temporary kill switch for the rare case there were issues with this network request.
            return;
        }
        let store = self.client.chain.chain_store.store();
        let network_adapter = self.client.network_adapter.clone();
        let requester_peer_id = msg.from_peer;
        let cache = self.client.sync_handler.epoch_sync.last_epoch_sync_response_cache.clone();
        let transaction_validity_period = self.client.chain.transaction_validity_period();
        self.client.sync_handler.epoch_sync.async_computation_spawner.spawn(
            "respond to epoch sync request",
            move || {
                let proof = match EpochSync::derive_epoch_sync_proof(
                    store,
                    transaction_validity_period,
                    cache,
                ) {
                    Ok(epoch_sync_proof) => epoch_sync_proof,
                    Err(err) => {
                        tracing::error!(?err, "Failed to derive epoch sync proof");
                        return;
                    }
                };
                network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::EpochSyncResponse { peer_id: requester_peer_id, proof },
                ));
            },
        )
    }
}

impl Handler<EpochSyncResponseMessage> for ClientActorInner {
    #[perf]
    fn handle(&mut self, msg: EpochSyncResponseMessage) {
        let (proof, _) = match msg.proof.decode() {
            Ok(proof) => proof,
            Err(err) => {
                tracing::error!(?err, "Failed to uncompress epoch sync proof");
                return;
            }
        };
        if let Err(err) = self.client.sync_handler.epoch_sync.apply_proof(
            &mut self.client.sync_handler.sync_status,
            &mut self.client.chain,
            proof,
            msg.from_peer,
            self.client.epoch_manager.as_ref(),
        ) {
            tracing::error!(?err, "Failed to apply epoch sync proof");
        }
    }
}

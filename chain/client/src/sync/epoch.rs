use crate::client_actor::ClientActorInner;
use crate::metrics;
use borsh::BorshDeserialize;
use near_async::futures::{AsyncComputationSpawner, AsyncComputationSpawnerExt};
use near_async::messaging::{CanSend, Handler};
use near_async::time::Clock;
use near_chain::types::Tip;
use near_chain::{BlockHeader, Chain, ChainStoreAccess, Doomslug, DoomslugThresholdMode, Error};
use near_chain_configs::EpochSyncConfig;
use near_client_primitives::types::{EpochSyncStatus, SyncStatus};
use near_crypto::Signature;
use near_epoch_manager::EpochManagerAdapter;
use near_network::client::{EpochSyncRequestMessage, EpochSyncResponseMessage};
use near_network::types::{
    HighestHeightPeerInfo, NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest,
};
use near_performance_metrics_macros::perf;
use near_primitives::block::{Approval, ApprovalInner};
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::epoch_manager::AGGREGATOR_KEY;
use near_primitives::epoch_sync::{
    CompressedEpochSyncProof, EpochSyncProof, EpochSyncProofCurrentEpochData,
    EpochSyncProofEpochData, EpochSyncProofLastEpochData,
};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::network::PeerId;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{AccountId, ApprovalStake, BlockHeight, BlockHeightDelta, EpochId};
use near_primitives::utils::{compression::CompressedData, index_to_bytes};
use near_store::{DBCol, Store, FINAL_HEAD_KEY};
use near_vm_runner::logic::ProtocolVersion;
use rand::seq::SliceRandom;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
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
        let epoch_sync_proof_we_used_to_bootstrap = store
            .get_ser::<EpochSyncProof>(DBCol::EpochSyncProof, &[])
            .expect("IO error querying epoch sync proof");
        let my_own_epoch_sync_boundary_block_header = epoch_sync_proof_we_used_to_bootstrap
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

        let target_epoch_last_block_header = store
            .get_ser::<BlockHeader>(DBCol::BlockHeader, target_epoch_last_block_hash.as_bytes())?
            .ok_or_else(|| Error::Other("Could not find last block of target epoch".to_string()))?;
        let target_epoch_second_last_block_header = store
            .get_ser::<BlockHeader>(
                DBCol::BlockHeader,
                target_epoch_last_block_header.prev_hash().as_bytes(),
            )?
            .ok_or_else(|| {
                Error::Other("Could not find second last block of target epoch".to_string())
            })?;

        let mut guard = cache.lock().unwrap();
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
                return Err(Error::Other(format!("Failed to compress epoch sync proof: {:?}", err)))
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
        let tip = store
            .get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?
            .ok_or_else(|| Error::Other("Could not find tip".to_string()))?;
        let current_epoch_start_height = store
            .get_ser::<BlockHeight>(DBCol::EpochStart, tip.epoch_id.0.as_bytes())?
            .ok_or_else(|| Error::EpochOutOfBounds(tip.epoch_id))?;
        let next_next_epoch_id = tip.next_epoch_id;
        // Last block hash of the target epoch is the same as the next next EpochId.
        // That's a general property for Near's epochs.
        let mut target_epoch_last_block_hash = next_next_epoch_id.0;
        Ok(loop {
            let block_info = store
                .get_ser::<BlockInfo>(DBCol::BlockInfo, target_epoch_last_block_hash.as_bytes())?
                .ok_or_else(|| Error::Other("Could not find block info".to_string()))?;
            let target_epoch_first_block_hash = block_info.epoch_first_block();
            let target_epoch_first_block_header = store
                .get_ser::<BlockHeader>(
                    DBCol::BlockHeader,
                    target_epoch_first_block_hash.as_bytes(),
                )?
                .ok_or_else(|| {
                    Error::Other("Could not find first block of target epoch".to_string())
                })?;
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
        let last_final_block_header_in_current_epoch = store
            .get_ser::<BlockHeader>(
                DBCol::BlockHeader,
                next_block_header_after_last_final_block_of_current_epoch.prev_hash().as_bytes(),
            )?
            .ok_or_else(|| Error::Other("Could not find final block header".to_string()))?;

        let current_epoch = *last_final_block_header_in_current_epoch.epoch_id();
        let current_epoch_info = store
            .get_ser::<EpochInfo>(DBCol::EpochInfo, current_epoch.0.as_bytes())?
            .ok_or_else(|| Error::EpochOutOfBounds(current_epoch))?;
        let next_epoch = *last_final_block_header_in_current_epoch.next_epoch_id();
        let next_epoch_info = store
            .get_ser::<EpochInfo>(DBCol::EpochInfo, next_epoch.0.as_bytes())?
            .ok_or_else(|| Error::EpochOutOfBounds(next_epoch))?;

        let genesis_epoch_info = store
            .get_ser::<EpochInfo>(DBCol::EpochInfo, EpochId::default().0.as_bytes())?
            .ok_or_else(|| Error::EpochOutOfBounds(EpochId::default()))?;

        // If we have an existing (possibly and likely outdated) EpochSyncProof stored on disk,
        // the last epoch we have a proof for is the "previous epoch" included in that EpochSyncProof.
        // Otherwise, the last epoch we have a "proof" for is the genesis epoch.
        let existing_epoch_sync_proof =
            store.get_ser::<EpochSyncProof>(DBCol::EpochSyncProof, &[])?;
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
                return Ok(existing_proof);
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
        let prev_epoch_info = store
            .get_ser::<EpochInfo>(DBCol::EpochInfo, prev_epoch.0.as_bytes())?
            .ok_or_else(|| Error::EpochOutOfBounds(prev_epoch))?;

        let last_block_of_prev_epoch = store
            .get_ser::<BlockHeader>(DBCol::BlockHeader, next_epoch.0.as_bytes())?
            .ok_or_else(|| Error::Other("Could not find last block of target epoch".to_string()))?;

        let last_block_info_of_prev_epoch = store
            .get_ser::<BlockInfo>(DBCol::BlockInfo, last_block_of_prev_epoch.hash().as_bytes())?
            .ok_or_else(|| Error::Other("Could not find last block info".to_string()))?;

        let second_last_block_of_prev_epoch = store
            .get_ser::<BlockHeader>(
                DBCol::BlockHeader,
                last_block_of_prev_epoch.prev_hash().as_bytes(),
            )?
            .ok_or_else(|| {
                Error::Other("Could not find second last block of target epoch".to_string())
            })?;

        let second_last_block_info_of_prev_epoch = store
            .get_ser::<BlockInfo>(
                DBCol::BlockInfo,
                last_block_of_prev_epoch.prev_hash().as_bytes(),
            )?
            .ok_or_else(|| Error::Other("Could not find second last block info".to_string()))?;

        let first_block_info_of_prev_epoch = store
            .get_ser::<BlockInfo>(
                DBCol::BlockInfo,
                last_block_info_of_prev_epoch.epoch_first_block().as_bytes(),
            )?
            .ok_or_else(|| Error::Other("Could not find first block info".to_string()))?;

        let block_info_for_final_block_of_current_epoch = store
            .get_ser::<BlockInfo>(
                DBCol::BlockInfo,
                last_final_block_header_in_current_epoch.hash().as_bytes(),
            )?
            .ok_or_else(|| {
                Error::Other("Could not find block info for latest final block".to_string())
            })?;

        let first_block_of_current_epoch = store
            .get_ser::<BlockHeader>(
                DBCol::BlockHeader,
                block_info_for_final_block_of_current_epoch.epoch_first_block().as_bytes(),
            )?
            .ok_or_else(|| Error::Other("Could not find first block of next epoch".to_string()))?;

        // TODO(#12255) That currently does not work because we might need some old block hashes
        // in order to build the merkle proof.
        // let merkle_proof_for_first_block_of_current_epoch = store
        //     .compute_past_block_proof_in_merkle_tree_of_later_block(
        //         first_block_of_current_epoch.hash(),
        //         final_block_header_in_current_epoch.hash(),
        //     )?;
        let merkle_proof_for_first_block_of_current_epoch = Default::default();

        let partial_merkle_tree_for_first_block_of_current_epoch = store
            .get_ser::<PartialMerkleTree>(
                DBCol::BlockMerkleTree,
                first_block_of_current_epoch.hash().as_bytes(),
            )?
            .ok_or_else(|| {
                Error::Other(
                    "Could not find partial merkle tree for first block of next epoch".to_string(),
                )
            })?;

        let all_epochs_including_old_proof = existing_epoch_sync_proof
            .map(|proof| proof.all_epochs)
            .unwrap_or_else(Vec::new)
            .into_iter()
            .chain(all_epochs_since_last_proof.into_iter())
            .collect();
        let proof = EpochSyncProof {
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

        Ok(proof)
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
        // We're going to get all the epochs and then figure out the correct chain of
        // epochs. The reason is that (1) epochs may, in very rare cases, have forks,
        // so we cannot just take all the epochs and assume their heights do not collide;
        // and (2) it is not easy to walk backwards from the last epoch; there's no
        // "give me the previous epoch" query. So instead, we use block header's
        // `next_epoch_id` to establish an epoch chain.
        let mut all_epoch_infos = HashMap::new();
        for item in store.iter(DBCol::EpochInfo) {
            let (key, value) = item?;
            if key.as_ref() == AGGREGATOR_KEY {
                continue;
            }
            let epoch_id = EpochId::try_from_slice(key.as_ref())?;
            let epoch_info = EpochInfo::try_from_slice(value.as_ref())?;
            all_epoch_infos.insert(epoch_id, epoch_info);
        }

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
            if let Ok(Some(block)) =
                store.get_ser::<BlockHeader>(DBCol::BlockHeader, epoch_id.0.as_bytes())
            {
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
        let mut epochs = (0..epoch_ids.len() - 2)
            .into_par_iter()
            .map(|index| -> Result<EpochSyncProofEpochData, Error> {
                let next_next_epoch_id = epoch_ids[index + 2];
                let epoch_id = epoch_ids[index];
                let last_block_header = store
                    .get_ser::<BlockHeader>(DBCol::BlockHeader, next_next_epoch_id.0.as_bytes())?
                    .ok_or_else(|| {
                        Error::Other(format!(
                            "Could not find last block header for epoch {:?}",
                            epoch_id
                        ))
                    })?;
                let second_last_block_header = store
                    .get_ser::<BlockHeader>(
                        DBCol::BlockHeader,
                        last_block_header.prev_hash().as_bytes(),
                    )?
                    .ok_or_else(|| {
                        Error::Other(format!(
                            "Could not find second last block header for epoch {:?}",
                            epoch_id
                        ))
                    })?;
                let third_last_block_header = store
                    .get_ser::<BlockHeader>(
                        DBCol::BlockHeader,
                        second_last_block_header.prev_hash().as_bytes(),
                    )?
                    .ok_or_else(|| {
                        Error::Other(format!(
                            "Could not find third last block header for epoch {:?}",
                            epoch_id
                        ))
                    })?;
                let epoch_info = all_epoch_infos.get(&epoch_id).ok_or_else(|| {
                    Error::Other(format!("Could not find epoch info for epoch {:?}", epoch_id))
                })?;
                Ok(EpochSyncProofEpochData {
                    block_producers: Self::get_epoch_info_block_producers(epoch_info),
                    last_final_block_header: third_last_block_header,
                    approvals_for_last_final_block: second_last_block_header.approvals().to_vec(),
                    protocol_version: epoch_info.protocol_version(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let current_epoch_info = all_epoch_infos
            .get(current_epoch_last_final_block_header.epoch_id())
            .ok_or_else(|| {
                Error::Other(format!(
                    "Could not find epoch info for epoch {:?}",
                    current_epoch_last_final_block_header.epoch_id()
                ))
            })?;
        epochs.push(EpochSyncProofEpochData {
            block_producers: Self::get_epoch_info_block_producers(current_epoch_info),
            last_final_block_header: current_epoch_last_final_block_header.clone(),
            approvals_for_last_final_block: current_epoch_second_last_block_approvals,
            protocol_version: current_epoch_info.protocol_version(),
        });
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

    /// Performs the epoch sync logic if applicable in the current state of the blockchain.
    /// This is periodically called by the client actor.
    pub fn run(
        &self,
        status: &mut SyncStatus,
        chain: &Chain,
        highest_height: BlockHeight,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Result<(), Error> {
        if !self.config.enabled {
            return Ok(());
        }
        let tip_height = chain.chain_store().header_head()?.height;
        if tip_height + self.config.epoch_sync_horizon >= highest_height {
            return Ok(());
        }
        match status {
            SyncStatus::AwaitingPeers | SyncStatus::StateSync(_) => {
                return Ok(());
            }
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
        if let SyncStatus::EpochSync(status) = status {
            if status.source_peer_id != source_peer {
                tracing::warn!("Ignoring epoch sync proof from unexpected peer: {}", source_peer);
                return Ok(());
            }
            if proof
                .current_epoch
                .first_block_header_in_epoch
                .height()
                .saturating_add(chain.epoch_length.max(chain.transaction_validity_period))
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

        let mut store_update = chain.chain_store.store().store_update();

        // Store the EpochSyncProof, so that this node can derive a more recent EpochSyncProof
        // to faciliate epoch sync of other nodes.
        store_update.set_ser(DBCol::EpochSyncProof, &[], &proof)?;

        let last_header = proof.current_epoch.first_block_header_in_epoch;
        let mut update = chain.mut_chain_store().store_update();
        update.save_block_header_no_update_tree(last_header.clone())?;
        update.save_block_header_no_update_tree(
            proof.current_epoch.last_block_header_in_prev_epoch,
        )?;
        update.save_block_header_no_update_tree(
            proof.current_epoch.second_last_block_header_in_prev_epoch.clone(),
        )?;
        update.save_block_header_no_update_tree(
            proof
                .all_epochs
                .get(proof.all_epochs.len() - 2)
                .unwrap()
                .last_final_block_header
                .clone(),
        )?;
        update.force_save_header_head(&Tip::from_header(&last_header))?;
        update.save_final_head(&Tip::from_header(&self.genesis))?;

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

        // At this point `update` contains headers of 3 last blocks of last past epoch
        // and header of the first block of current epoch.
        // At least the third last block of last past epoch is final.
        // It means that `update` contains header of last final block of the first block of current epoch.
        let last_header_last_finalized_height =
            update.get_block_header(last_header.last_final_block())?.height();
        let mut first_block_info_in_epoch =
            BlockInfo::from_header(&last_header, last_header_last_finalized_height);
        // We need to populate fields below manually, as they are set to defaults by `BlockInfo::from_header`.
        *first_block_info_in_epoch.epoch_first_block_mut() = *last_header.hash();
        *first_block_info_in_epoch.epoch_id_mut() = *last_header.epoch_id();

        store_update.insert_ser(
            DBCol::BlockInfo,
            &borsh::to_vec(first_block_info_in_epoch.hash()).unwrap(),
            &first_block_info_in_epoch,
        )?;

        store_update.set_ser(
            DBCol::BlockOrdinal,
            &index_to_bytes(proof.current_epoch.partial_merkle_tree_for_first_block.size()),
            last_header.hash(),
        )?;

        store_update.set_ser(
            DBCol::BlockHeight,
            &borsh::to_vec(&last_header.height()).unwrap(),
            last_header.hash(),
        )?;

        store_update.set_ser(
            DBCol::BlockMerkleTree,
            last_header.hash().as_bytes(),
            &proof.current_epoch.partial_merkle_tree_for_first_block,
        )?;

        update.merge(store_update);
        update.commit()?;

        *status = SyncStatus::EpochSyncDone;
        tracing::info!(epoch_id=?last_header.epoch_id(), "Bootstrapped from epoch sync");

        Ok(())
    }

    fn verify_proof(
        &self,
        proof: &EpochSyncProof,
        epoch_manager: &dyn EpochManagerAdapter,
    ) -> Result<(), Error> {
        let EpochSyncProof { all_epochs, last_epoch, current_epoch } = proof;
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

        Self::verify_final_block_endorsement(&all_epochs[0], &all_epochs[1].block_producers)?;

        // Verify block producer handoff between all past epochs
        for epoch_index in 1..all_epochs.len() {
            let epoch = &all_epochs[epoch_index];
            let prev_epoch = &all_epochs[epoch_index - 1];
            let next_epoch_block_producers = if epoch_index + 1 < all_epochs.len() {
                all_epochs[epoch_index + 1].block_producers.clone()
            } else {
                Self::get_epoch_info_block_producers(&proof.last_epoch.next_next_epoch_info)
            };
            Self::verify_block_producer_handoff(
                epoch_index,
                &epoch.block_producers,
                prev_epoch.protocol_version,
                prev_epoch.last_final_block_header.next_bp_hash(),
            )?;
            Self::verify_final_block_endorsement(epoch, &next_epoch_block_producers)?;
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
        _current_epoch_final_block_header: &BlockHeader,
    ) -> Result<(), Error> {
        // Verify first_block_header_in_epoch
        let first_block_header = &current_epoch.first_block_header_in_epoch;
        // TODO(#12255) Uncomment the check below when `merkle_proof_for_first_block` is generated.
        // if !merkle::verify_hash(
        //     *current_epoch_final_block_header.block_merkle_root(),
        //     &current_epoch.merkle_proof_for_first_block,
        //     *first_block_header.hash(),
        // ) {
        //     return Err(Error::InvalidEpochSyncProof(
        //         "invalid merkle_proof_for_first_block".to_string(),
        //     ));
        // }

        // Verify partial_merkle_tree_for_first_block
        if current_epoch.partial_merkle_tree_for_first_block.root()
            != *first_block_header.block_merkle_root()
        {
            return Err(Error::InvalidEpochSyncProof(
                "invalid path in partial_merkle_tree_for_first_block".to_string(),
            ));
        }
        // TODO(#12256) Investigate why "+1" was needed here, looks like it should not be there.
        if current_epoch.partial_merkle_tree_for_first_block.size() + 1
            != first_block_header.block_ordinal()
        {
            return Err(Error::InvalidEpochSyncProof(
                "invalid size in partial_merkle_tree_for_first_block".to_string(),
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
        let expected_epoch_sync_data_hash = current_epoch_first_block_header
            .epoch_sync_data_hash()
            .ok_or(Error::InvalidEpochSyncProof("missing epoch_sync_data_hash".to_string()))?;
        if epoch_sync_data_hash != expected_epoch_sync_data_hash {
            return Err(Error::InvalidEpochSyncProof("invalid epoch_sync_data_hash".to_string()));
        }

        Ok(())
    }

    // Computes the ordered list of block approvers. For any of the last three blocks of an
    // epoch, the approvers are the combined block producers of the epoch and its next epoch.
    // For other blocks, the block approvers are just the block producers.
    fn get_all_block_approvers_ordered(
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

    /// Verifies that EpochSyncProofPastEpochData's block_producers is valid
    fn verify_block_producer_handoff(
        epoch_index: usize,
        block_producers: &Vec<ValidatorStake>,
        prev_epoch_protocol_version: ProtocolVersion,
        prev_epoch_next_bp_hash: &CryptoHash,
    ) -> Result<(), Error> {
        let bp_hash = Chain::compute_bp_hash_from_validator_stakes(
            block_producers,
            prev_epoch_protocol_version,
        )?;
        if bp_hash != *prev_epoch_next_bp_hash {
            return Err(Error::InvalidEpochSyncProof(format!(
                "invalid block_producers for epoch index {}",
                epoch_index
            )));
        }
        Ok(())
    }

    /// Verifies that the epoch's last_final_block_header is correctly endorsed.
    ///
    /// Note that we do not trust next_epoch_block_producers. It doesn't matter what's given there;
    /// it's just so that we can verify the signatures of the last final block of the epoch.
    /// This block requires signatures from both this epoch's block producers and the next epoch's,
    /// so we need to know both to verify the signatures; however, we only need this epoch's block
    /// producers to be valid to verify that the signatures are valid. It is possible to to invent
    /// an incorrect list (such as one with incorrect stake numbers) for the next epoch's block
    /// producers so that we still pass the signature verification, but that's fine, as we still do
    /// not trust the next epoch block producers; those will be verified later.
    fn verify_final_block_endorsement(
        epoch: &EpochSyncProofEpochData,
        next_epoch_block_producers: &[ValidatorStake],
    ) -> Result<(), Error> {
        let last_final_block_header = &epoch.last_final_block_header;
        let approvers_info = Self::get_all_block_approvers_ordered(
            &epoch.block_producers,
            next_epoch_block_producers,
        );
        Self::verify_block_endorsements(
            *last_final_block_header.hash(),
            last_final_block_header.height(),
            &approvers_info,
            &epoch.approvals_for_last_final_block,
        )?;
        Ok(())
    }

    /// Verifies that the given block is endorsed properly, and with enough stake.
    fn verify_block_endorsements(
        prev_block_hash: CryptoHash,
        block_height: BlockHeight,
        approvers: &[ApprovalStake],
        approvals: &[Option<Box<Signature>>],
    ) -> Result<(), near_chain::Error> {
        if approvals.len() > approvers.len() {
            return Err(near_chain::Error::InvalidEpochSyncProof(format!(
                "Block {} should have {} approvers but has {} approvals",
                block_height,
                approvers.len(),
                approvals.len()
            )));
        }

        let message_to_sign = Approval::get_data_for_sig(
            &ApprovalInner::Endorsement(prev_block_hash),
            block_height + 1,
        );

        for (validator, may_be_signature) in approvers.iter().zip(approvals.iter()) {
            if let Some(signature) = may_be_signature {
                if !signature.verify(&message_to_sign, &validator.public_key) {
                    return Err(near_chain::Error::InvalidEpochSyncProof(format!(
                        "Invalid signature for block {} from validator {:?}",
                        block_height, validator.account_id
                    )));
                }
            }
        }

        let stakes = approvers
            .iter()
            .map(|x| (x.stake_this_epoch, x.stake_next_epoch, false))
            .collect::<Vec<_>>();
        if !Doomslug::can_approved_block_be_produced(
            DoomslugThresholdMode::TwoThirds,
            approvals,
            &stakes,
        ) {
            return Err(near_chain::Error::InvalidEpochSyncProof(format!(
                "Block {} does not have enough approvals",
                block_height
            )));
        }

        Ok(())
    }
}

impl Handler<EpochSyncRequestMessage> for ClientActorInner {
    #[perf]
    fn handle(&mut self, msg: EpochSyncRequestMessage) {
        if !self.client.epoch_sync.config.enabled {
            // TODO(#11937): before we have rate limiting, don't respond to epoch sync requests
            // unless config is enabled.
            return;
        }
        let store = self.client.chain.chain_store.store().clone();
        let network_adapter = self.client.network_adapter.clone();
        let requester_peer_id = msg.from_peer;
        let cache = self.client.epoch_sync.last_epoch_sync_response_cache.clone();
        let transaction_validity_period = self.client.chain.transaction_validity_period;
        self.client.epoch_sync.async_computation_spawner.spawn(
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
        if let Err(err) = self.client.epoch_sync.apply_proof(
            &mut self.client.sync_status,
            &mut self.client.chain,
            proof,
            msg.from_peer,
            self.client.epoch_manager.as_ref(),
        ) {
            tracing::error!(?err, "Failed to apply epoch sync proof");
        }
    }
}

use crate::client_actor::{ClientActor, ShutdownReason};
use crate::sync::handler::SyncHandler;
use near_async::futures::{AsyncComputationSpawner, AsyncComputationSpawnerExt};
use near_async::messaging::{CanSend, Handler};
use near_async::time::Clock;
use near_chain::types::Tip;
use near_chain::{BlockHeader, Chain, ChainStoreAccess, Error};
use near_chain_configs::EpochSyncConfig;
use near_client_primitives::types::{EpochSyncStatus, FetchingEpochSyncBatchesState, SyncStatus};
use near_crypto::Signature;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::epoch_sync::{
    derive_epoch_sync_proof_from_last_block, find_target_epoch_to_produce_proof_for,
    get_epoch_info_block_producers,
};
use near_network::client::{
    EpochSyncBatchRequestMessage, EpochSyncBatchResponseMessage, EpochSyncManifestRequestMessage,
    EpochSyncManifestResponseMessage, EpochSyncRequestMessage, EpochSyncResponseMessage,
};
use near_network::concurrency::outgoing_queue_limiter::OutgoingPermit;
use near_network::types::{
    HighestHeightPeerInfo, NetworkRequestWithPermit, NetworkRequests, PeerManagerAdapter,
    PeerManagerMessageRequest,
};
use near_primitives::block::{Approval, ApprovalInner, compute_bp_hash_from_validator_stakes};
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_sync::{
    CompressedEpochSyncProof, CompressedEpochSyncProofBatch, CompressedEpochSyncProofManifest,
    EPOCHS_PER_BATCH_V1, EpochSyncProof, EpochSyncProofBatch, EpochSyncProofBatchV1,
    EpochSyncProofCurrentEpochData, EpochSyncProofEpochData, EpochSyncProofLastEpochData,
    EpochSyncProofManifest, EpochSyncProofManifestV1, EpochSyncProofV1, MAX_NUMBER_OF_BATCHES,
};
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{Balance, BlockHeight, BlockHeightDelta, EpochId};
use near_primitives::utils::compression::CompressedData;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::{Store, metrics};
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use std::sync::Arc;
use std::time::Duration;
use tracing::instrument;

/// Maximum age of an epoch sync proof, in number of epochs.
/// Proofs older than this are rejected as too stale.
/// This is intentionally larger than the epoch sync horizon because
/// proofs are inherently ~2 epochs old by design (the target epoch
/// must be finalized before a proof can be derived).
const EPOCH_SYNC_PROOF_MAX_AGE_NUM_EPOCHS: u64 = {
    assert!(
        near_chain_configs::MIN_GC_NUM_EPOCHS_TO_KEEP == 3,
        "EPOCH_SYNC_PROOF_MAX_AGE_NUM_EPOCHS must match MIN_GC_NUM_EPOCHS_TO_KEEP"
    );
    3
};

/// Timeout for a single batch request.
const EPOCH_SYNC_BATCH_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum number of batch requests outstanding at any time.
const MAX_IN_FLIGHT_BATCH_REQUESTS: u64 = 8;

pub enum EpochDataBatchStatus {
    Missing,
    Requested { attempt_time: near_time::Utc },
    Verified(Vec<EpochSyncProofEpochData>),
}

pub struct EpochSyncProofAssembler {
    manifest: EpochSyncProofManifestV1,
    batches: Vec<EpochDataBatchStatus>,
}

impl EpochSyncProofAssembler {
    pub fn new(manifest: EpochSyncProofManifestV1) -> Result<Self, Error> {
        if manifest.total_epochs == 0 {
            return Err(Error::Other(String::from("manifest covers no epochs")));
        }

        let expected_num_batches = manifest.expected_num_batches();
        if expected_num_batches > MAX_NUMBER_OF_BATCHES {
            return Err(Error::Other(format!(
                "manifest declares {expected_num_batches} batches, at most {MAX_NUMBER_OF_BATCHES} allowed",
            )));
        }

        if manifest.batches_metadata.len() as u64 != expected_num_batches {
            return Err(Error::Other(format!(
                "manifest has {} batch metadata entries but declares {expected_num_batches} batches",
                manifest.batches_metadata.len(),
            )));
        }

        let batches = (0..expected_num_batches).map(|_| EpochDataBatchStatus::Missing).collect();

        Ok(Self { manifest, batches })
    }

    pub fn total_batches(&self) -> usize {
        self.batches.len()
    }

    pub fn is_awaiting_batches(&self, batch_index: u64) -> bool {
        self.batches
            .get(batch_index as usize)
            .is_some_and(|status| matches!(status, EpochDataBatchStatus::Requested { .. }))
    }

    pub fn try_add_batch(
        &mut self,
        batch_index: u64,
        batch: EpochSyncProofBatchV1,
    ) -> Result<(), Error> {
        let entry = self
            .batches
            .get_mut(batch_index as usize)
            .ok_or_else(|| Error::Other(String::from("invalid batch index")))?;

        let metadata = self
            .manifest
            .batches_metadata
            .get(batch_index as usize)
            .ok_or_else(|| Error::Other(String::from("invalid batch index")))?;

        match entry {
            EpochDataBatchStatus::Requested { .. } => {
                // We expect that every batch is full except the last one.
                let epochs_before_batch = batch_index.saturating_mul(EPOCHS_PER_BATCH_V1);
                let expected_epochs = self
                    .manifest
                    .total_epochs
                    .saturating_sub(epochs_before_batch)
                    .min(EPOCHS_PER_BATCH_V1);
                if batch.epochs.len() as u64 != expected_epochs {
                    return Err(Error::Other(format!(
                        "batch {batch_index} carries {} epochs, expected {expected_epochs}",
                        batch.epochs.len(),
                    )));
                }

                let Some(first_epoch) = &batch.epochs.first() else {
                    return Err(Error::Other(String::from("Batch was empty")));
                };

                if batch_index > 0 {
                    if first_epoch.last_final_block_header.epoch_id() != &metadata.first_epoch_id {
                        return Err(Error::Other(format!(
                            "batch {batch_index} starts at epoch {:?}, manifest declares {:?}",
                            first_epoch.last_final_block_header.epoch_id(),
                            metadata.first_epoch_id,
                        )));
                    }
                    if !EpochSync::verify_block_producer_handoff(
                        &first_epoch.block_producers,
                        first_epoch.use_versioned_bp_hash_format,
                        &metadata.first_bp_hash,
                    )? {
                        return Err(Error::Other(format!(
                            "block producers of batch {batch_index}'s first epoch do not match the manifest",
                        )));
                    }
                }

                *entry = EpochDataBatchStatus::Verified(batch.epochs);
                Ok(())
            }
            EpochDataBatchStatus::Missing => {
                Err(Error::Other(String::from("batch was not requested")))
            }
            EpochDataBatchStatus::Verified(_) => {
                Err(Error::Other(String::from("batch already exists")))
            }
        }
    }

    #[cfg(test)]
    fn mark_requested_for_test(&mut self, batch_index: u64) {
        self.batches[batch_index as usize] =
            EpochDataBatchStatus::Requested { attempt_time: near_time::Utc::UNIX_EPOCH };
    }

    pub fn is_ready(&self) -> bool {
        self.batches.iter().all(|batch| matches!(batch, EpochDataBatchStatus::Verified(_)))
    }

    pub fn try_build(&self) -> Result<EpochSyncProofV1, Error> {
        let all_epochs = self
            .batches
            .iter()
            .map(|batch| {
                if let EpochDataBatchStatus::Verified(epochs) = batch {
                    Ok(epochs)
                } else {
                    Err(Error::Other(String::from("Not all batch are ready")))
                }
            })
            .collect::<Result<Vec<_>, Error>>()?;

        Ok(EpochSyncProofV1 {
            all_epochs: all_epochs.into_iter().flatten().cloned().collect(),
            last_epoch: self.manifest.last_epoch.clone(),
            current_epoch: self.manifest.current_epoch.clone(),
        })
    }
}

struct BatchedEpochSyncProof {
    manifest: CompressedEpochSyncProofManifest,
    batches: Vec<CompressedEpochSyncProofBatch>,
}

pub struct EpochSync {
    clock: Clock,
    network_adapter: PeerManagerAdapter,
    genesis: BlockHeader,
    async_computation_spawner: Arc<dyn AsyncComputationSpawner>,
    config: EpochSyncConfig,
    /// The last epoch sync proof and the epoch ID it was computed for.
    /// We reuse the same proof as long as the current epoch ID is the same.
    last_epoch_sync_response_cache: Arc<Mutex<Option<(EpochId, CompressedEpochSyncProof)>>>,
    last_batched_response_cache: Arc<Mutex<Option<(EpochId, Arc<BatchedEpochSyncProof>)>>>,
    proof_assembler: Option<EpochSyncProofAssembler>,
}

impl EpochSync {
    pub fn new(
        clock: Clock,
        network_adapter: PeerManagerAdapter,
        genesis: BlockHeader,
        async_computation_spawner: Arc<dyn AsyncComputationSpawner>,
        config: EpochSyncConfig,
    ) -> Self {
        Self {
            clock,
            network_adapter,
            genesis,
            async_computation_spawner,
            config,
            last_epoch_sync_response_cache: Arc::new(Mutex::new(None)),
            last_batched_response_cache: Arc::new(Mutex::new(None)),
            proof_assembler: None,
        }
    }

    /// Derives an epoch sync proof for a recent epoch, that can be directly used to bootstrap
    /// a new node or bring a far-behind node to a recent epoch.
    #[instrument(skip(store, cache))]
    fn derive_epoch_sync_proof(
        store: Store,
        transaction_validity_period: BlockHeightDelta,
        cache: Arc<Mutex<Option<(EpochId, CompressedEpochSyncProof)>>>,
    ) -> Result<CompressedEpochSyncProof, Error> {
        // Epoch sync initializes a new node with the first block of some epoch; we call that
        // epoch the "target epoch". In the context of talking about the proof or the newly
        // bootstrapped node, it is also called the "current epoch".
        let target_epoch_last_block_hash =
            find_target_epoch_to_produce_proof_for(&store, transaction_validity_period)?;

        let chain_store = store.chain_store();
        let target_epoch_last_block_header =
            chain_store.get_block_header(&target_epoch_last_block_hash)?;

        let mut guard = cache.lock();
        if let Some((epoch_id, proof)) = &*guard {
            if epoch_id == target_epoch_last_block_header.epoch_id() {
                return Ok(proof.clone());
            }
        }
        // We're purposefully not releasing the lock here. This is so that if the cache
        // is out of date, only one thread should be doing the computation.
        let proof = derive_epoch_sync_proof_from_last_block(
            &store.epoch_store(),
            &target_epoch_last_block_hash,
            true,
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

    /// Sends an epoch sync request to a random peer, or waits if a previous
    /// request is still in flight. Handles both initial send (NotStarted) and
    /// retry on timeout (InProgress).
    pub fn run(
        &mut self,
        status: &mut EpochSyncStatus,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Result<(), Error> {
        match status {
            EpochSyncStatus::InProgress { attempt_time, source_peer_id, .. } => {
                if *attempt_time + self.config.timeout_for_epoch_sync < self.clock.now_utc() {
                    tracing::warn!(target: "sync", %source_peer_id, "epoch sync from peer timed out, retrying");
                    self.request_full_proof(status, highest_height_peers)
                } else {
                    Ok(())
                }
            }
            EpochSyncStatus::FetchingManifest {
                source_peer_id,
                source_peer_height: _,
                attempt_time,
            } => {
                if *attempt_time + self.config.timeout_for_epoch_sync < self.clock.now_utc() {
                    tracing::warn!(target: "sync", %source_peer_id, "epoch sync from peer timed out, retrying");
                    self.request_manifest(status, highest_height_peers)
                } else {
                    Ok(())
                }
            }
            EpochSyncStatus::FetchingBatches(state) => {
                *status = self.check_batches_status(state)?;
                Ok(())
            }
            EpochSyncStatus::NotStarted => {
                if ProtocolFeature::BatchedEpochSync.enabled(PROTOCOL_VERSION) {
                    self.request_manifest(status, highest_height_peers)
                } else {
                    self.request_full_proof(status, highest_height_peers)
                }
            }
            EpochSyncStatus::Done => Ok(()),
        }
    }

    fn check_batches_status(
        &mut self,
        FetchingEpochSyncBatchesState {
            manifest_source_peer_id,
            manifest_source_peer_height,
            total_batches,
            verified_batches,
            attempt_time,
            in_flight: _,
        }: &FetchingEpochSyncBatchesState,
    ) -> Result<EpochSyncStatus, Error> {
        if *attempt_time + self.config.timeout_for_epoch_sync < self.clock.now_utc() {
            tracing::warn!(target: "sync", %manifest_source_peer_id, "epoch sync from peer timed out, restarting epoch sync");
            self.proof_assembler = None;
            return Ok(EpochSyncStatus::NotStarted);
        }

        let Some(assembler) = self.proof_assembler.as_mut() else {
            tracing::error!(target: "sync", "no proof assembler while fetching batches, this is a bug, restarting epoch sync");
            return Ok(EpochSyncStatus::NotStarted);
        };

        let mut in_flight = assembler
            .batches
            .iter()
            .filter(|status| matches!(status, EpochDataBatchStatus::Requested { .. }))
            .count() as u64;

        for (batch_index, status) in assembler.batches.iter_mut().enumerate() {
            match status {
                EpochDataBatchStatus::Verified(_) => {}
                EpochDataBatchStatus::Missing => {
                    if in_flight < MAX_IN_FLIGHT_BATCH_REQUESTS {
                        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                            // TODO: consider requesting batches from multiple peers in parallel
                            NetworkRequests::EpochSyncBatchRequest {
                                peer_id: manifest_source_peer_id.clone(),
                                batch_index: batch_index as u64,
                            },
                        ));
                        *status =
                            EpochDataBatchStatus::Requested { attempt_time: self.clock.now_utc() };
                        in_flight += 1;
                    }
                }
                EpochDataBatchStatus::Requested { attempt_time } => {
                    // retry we haven't gotten back a response yet
                    if *attempt_time + EPOCH_SYNC_BATCH_REQUEST_TIMEOUT < self.clock.now_utc() {
                        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                            // TODO: consider requesting batches from multiple peers in parallel
                            NetworkRequests::EpochSyncBatchRequest {
                                peer_id: manifest_source_peer_id.clone(),
                                batch_index: batch_index as u64,
                            },
                        ));
                        *status =
                            EpochDataBatchStatus::Requested { attempt_time: self.clock.now_utc() };
                    }
                }
            }
        }

        Ok(EpochSyncStatus::FetchingBatches(FetchingEpochSyncBatchesState {
            manifest_source_peer_id: manifest_source_peer_id.clone(),
            manifest_source_peer_height: *manifest_source_peer_height,
            total_batches: *total_batches,
            verified_batches: *verified_batches,
            attempt_time: *attempt_time,
            in_flight,
        }))
    }

    fn request_manifest(
        &self,
        status: &mut EpochSyncStatus,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Result<(), Error> {
        // TODO(#11976): Implement a more robust logic for picking a peer to request epoch sync from.
        let peer = highest_height_peers
            .choose(&mut rand::thread_rng())
            .ok_or_else(|| Error::Other("No peers to request epoch sync from".to_string()))?;

        tracing::info!(target: "sync", peer_id=?peer.peer_info.id, "bootstrapping node via epoch sync");

        *status = EpochSyncStatus::FetchingManifest {
            source_peer_id: peer.peer_info.id.clone(),
            source_peer_height: peer.highest_block_height,
            attempt_time: self.clock.now_utc(),
        };

        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::EpochSyncManifestRequest { peer_id: peer.peer_info.id.clone() },
        ));

        Ok(())
    }

    fn request_full_proof(
        &self,
        status: &mut EpochSyncStatus,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Result<(), Error> {
        // TODO(#11976): Implement a more robust logic for picking a peer to request epoch sync from.
        let peer = highest_height_peers
            .choose(&mut rand::thread_rng())
            .ok_or_else(|| Error::Other("No peers to request epoch sync from".to_string()))?;

        tracing::info!(target: "sync", peer_id=?peer.peer_info.id, "bootstrapping node via epoch sync");

        *status = EpochSyncStatus::InProgress {
            source_peer_id: peer.peer_info.id.clone(),
            source_peer_height: peer.highest_block_height,
            attempt_time: self.clock.now_utc(),
        };

        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::EpochSyncRequest { peer_id: peer.peer_info.id.clone() },
        ));

        Ok(())
    }

    #[instrument(skip(store, cache))]
    fn derive_batched_epoch_sync_proof(
        store: Store,
        transaction_validity_period: BlockHeightDelta,
        cache: Arc<Mutex<Option<(EpochId, Arc<BatchedEpochSyncProof>)>>>,
    ) -> Result<Arc<BatchedEpochSyncProof>, Error> {
        let target_epoch_last_block_hash =
            find_target_epoch_to_produce_proof_for(&store, transaction_validity_period)?;
        let chain_store = store.chain_store();
        let target_epoch_last_block_header =
            chain_store.get_block_header(&target_epoch_last_block_hash)?;

        let mut guard = cache.lock();
        if let Some((epoch_id, response)) = &*guard {
            if epoch_id == target_epoch_last_block_header.epoch_id() {
                return Ok(response.clone());
            }
        }
        let proof = derive_epoch_sync_proof_from_last_block(
            &store.epoch_store(),
            &target_epoch_last_block_hash,
            true,
        )?;
        let (manifest, batches) = proof.into_v1().split_into_batches();

        let (manifest, _) =
            CompressedEpochSyncProofManifest::encode(&EpochSyncProofManifest::V1(manifest))
                .map_err(|err| {
                    Error::Other(format!("failed to compress epoch sync manifest: {err:?}"))
                })?;
        let batches = batches
            .into_iter()
            .map(|batches| {
                CompressedEpochSyncProofBatch::encode(&EpochSyncProofBatch::V1(batches))
                    .map(|(batches, _)| batches)
                    .map_err(|err| {
                        Error::Other(format!("failed to compress epoch sync proof batch: {err:?}"))
                    })
            })
            .collect::<Result<Vec<_>, Error>>()?;

        let response = Arc::new(BatchedEpochSyncProof { manifest, batches });
        *guard = Some((*target_epoch_last_block_header.epoch_id(), response.clone()));
        Ok(response)
    }

    /// Validates an epoch sync proof: checks peer identity, proof freshness,
    /// and cryptographic correctness. Does not write any data to the store.
    /// Returns `Ok(true)` if the proof is valid, `Ok(false)` if the proof
    /// should be silently ignored (wrong peer, too recent, too old, unexpected).
    fn validate_proof(
        &self,
        status: &SyncStatus,
        chain: &Chain,
        proof: &EpochSyncProofV1,
        source_peer: &PeerId,
        epoch_manager: &dyn EpochManagerAdapter,
    ) -> Result<bool, Error> {
        let (source_peer_id, source_peer_height) = match status {
            SyncStatus::EpochSync(EpochSyncStatus::InProgress {
                source_peer_id,
                source_peer_height,
                ..
            }) => (source_peer_id, source_peer_height),
            SyncStatus::EpochSync(EpochSyncStatus::FetchingBatches(
                FetchingEpochSyncBatchesState {
                    manifest_source_peer_id,
                    manifest_source_peer_height,
                    ..
                },
            )) => (manifest_source_peer_id, manifest_source_peer_height),
            _ => {
                tracing::warn!(target: "sync", %source_peer, "ignoring unexpected epoch sync proof");
                return Ok(false);
            }
        };

        if *source_peer_id != *source_peer {
            tracing::warn!(target: "sync", %source_peer, expected_peer = %source_peer_id, "ignoring epoch sync proof from unexpected peer");
            return Ok(false);
        }
        if proof
            .current_epoch
            .first_block_header_in_epoch
            .height()
            .saturating_add(chain.epoch_length.max(chain.transaction_validity_period()))
            >= *source_peer_height
        {
            tracing::error!(
                target: "sync",
                %source_peer,
                "ignoring epoch sync proof from peer that is too recent"
            );
            return Ok(false);
        }
        if proof
            .current_epoch
            .first_block_header_in_epoch
            .height()
            .saturating_add(EPOCH_SYNC_PROOF_MAX_AGE_NUM_EPOCHS * chain.epoch_length)
            < *source_peer_height
        {
            tracing::error!(
                target: "sync",
                %source_peer,
                "ignoring epoch sync proof from peer that is too old"
            );
            return Ok(false);
        }

        self.verify_proof(proof, epoch_manager)?;

        Ok(true)
    }

    /// Applies a previously validated epoch sync proof to the store and updates
    /// sync status. Must only be called after `validate_proof` returns `Ok(true)`.
    fn apply_validated_proof(
        &self,
        status: &mut SyncStatus,
        chain: &Chain,
        proof: EpochSyncProofV1,
        epoch_manager: &dyn EpochManagerAdapter,
    ) -> Result<(), Error> {
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
            &mut store_update.epoch_store_update(),
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
        let current_protocol_version =
            epoch_manager.get_epoch_protocol_version(last_header.epoch_id())?;
        let mut first_block_info_in_epoch = BlockInfo::from_header(
            &last_header,
            last_header_last_finalized_height,
            current_protocol_version,
        );
        // We need to populate fields below manually, as they are set to defaults by `BlockInfo::from_header`.
        *first_block_info_in_epoch.epoch_first_block_mut() = *last_header.hash();
        *first_block_info_in_epoch.epoch_id_mut() = *last_header.epoch_id();

        store_update.epoch_store_update().set_block_info(&first_block_info_in_epoch);
        // The epoch-sync first block bypasses `record_block_info`, so seed its
        // ChunkProducers rows here in the same update. The consensus reader
        // hard-errors on a missing same-epoch anchor, and this block is the
        // grandparent anchor for chunks at epoch-start + 2.
        epoch_manager.seed_chunk_producers_after_epoch_sync(
            &mut store_update.epoch_store_update(),
            &first_block_info_in_epoch,
        )?;
        // `record_block_info`, which epoch sync bypasses, is otherwise the only
        // block-processing writer of `EpochStart`. The early-kickout grace check no longer
        // reads this column (it walks `BlockInfo.epoch_first_block`), but other readers
        // still key on it and would error on the synced epoch without this row, e.g.
        // `get_validator_info` (RPC), `compare_epoch_id`, the epoch-sync-proof migration,
        // and `find_target_epoch_to_produce_proof_for` (serving epoch sync to other nodes).
        store_update
            .epoch_store_update()
            .set_epoch_start(last_header.epoch_id(), last_header.height());
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

        store_update.commit();

        *status = SyncStatus::EpochSync(EpochSyncStatus::Done);
        tracing::info!(target: "sync", epoch_id=?last_header.epoch_id(), "bootstrapped from epoch sync");

        Ok(())
    }

    pub fn verify_proof(
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
        Self::verify_first_epoch_against_genesis(&self.genesis, &all_epochs[0], epoch_manager)?;
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
            if epoch.last_final_block_header.epoch_id()
                != prev_epoch.last_final_block_header.next_epoch_id()
            {
                return Err(Error::InvalidEpochSyncProof(format!(
                    "epoch_id mismatch at all_epochs[{}]: expected {:?}, got {:?}",
                    epoch_index,
                    prev_epoch.last_final_block_header.next_epoch_id(),
                    epoch.last_final_block_header.epoch_id(),
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
        // Verify that first_block_header_in_epoch is in the same epoch as the
        // last final block. Without this check, an attacker could substitute the
        // first block of a previous epoch (which is also in the Merkle tree),
        // causing the node to initialize with stale epoch data.
        let first_block_header = &current_epoch.first_block_header_in_epoch;
        if first_block_header.epoch_id() != current_epoch_final_block_header.epoch_id() {
            return Err(Error::InvalidEpochSyncProof(
                "first_block_header_in_epoch is not in the expected epoch".to_string(),
            ));
        }

        // Verify first_block_header_in_epoch hash
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
        // Use checked_add so an attacker-controlled size of u64::MAX cannot trigger an arithmetic
        // overflow panic (which would crash a bootstrapping node) before the is_well_formed check.
        if current_epoch.partial_merkle_tree_for_first_block.size().checked_add(1)
            != Some(first_block_header.block_ordinal())
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
        let epoch_sync_data_hash = last_epoch.compute_epoch_sync_data_hash();
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
    /// Verifies that the first epoch of a proof is the epoch following genesis, against the
    /// block producers this node derives from its own genesis. This is the root of the
    /// induction the rest of the proof rests on, so it deliberately uses nothing the sender
    /// supplied.
    fn verify_first_epoch_against_genesis(
        genesis: &BlockHeader,
        first_epoch: &EpochSyncProofEpochData,
        epoch_manager: &dyn EpochManagerAdapter,
    ) -> Result<(), Error> {
        let second_next_epoch_id_after_genesis = EpochId(*genesis.hash());
        let second_next_epoch_info_after_genesis =
            epoch_manager.get_epoch_info(&second_next_epoch_id_after_genesis)?;
        if first_epoch.block_producers
            != get_epoch_info_block_producers(&second_next_epoch_info_after_genesis)
        {
            return Err(Error::InvalidEpochSyncProof(
                "invalid block producers for second epoch after genesis".to_string(),
            ));
        }
        if first_epoch.last_final_block_header.epoch_id() != &second_next_epoch_id_after_genesis {
            return Err(Error::InvalidEpochSyncProof(format!(
                "epoch_id mismatch for all_epochs[0] last final block header: expected {:?}, got {:?}",
                second_next_epoch_id_after_genesis,
                first_epoch.last_final_block_header.epoch_id(),
            )));
        }
        Ok(())
    }

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

        // `block_height` comes from an attacker-controlled header in the proof and is not bounded
        // before this point, so use checked_add to avoid an arithmetic overflow panic (which would
        // crash a bootstrapping node) when the height is u64::MAX.
        let Some(target_height) = block_height.checked_add(1) else {
            return Err(Error::InvalidEpochSyncProof(format!(
                "block height {block_height} too large in epoch sync proof"
            )));
        };
        let message_to_sign =
            Approval::get_data_for_sig(&ApprovalInner::Endorsement(prev_block_hash), target_height);

        let mut total_stake = Balance::ZERO;
        let mut endorsed_stake = Balance::ZERO;

        for (validator, may_be_signature) in block_producers.iter().zip(endorsements.iter()) {
            if let Some(signature) = may_be_signature {
                if !signature.verify(&message_to_sign, validator.public_key()) {
                    return Err(Error::InvalidEpochSyncProof(format!(
                        "Invalid signature for block {} from validator {:?}",
                        block_height,
                        validator.account_id()
                    )));
                }
                endorsed_stake = endorsed_stake.checked_add(validator.stake()).unwrap();
            }
            total_stake = total_stake.checked_add(validator.stake()).unwrap();
        }

        let required_stake = total_stake.checked_mul(2).unwrap().checked_div(3).unwrap();
        if endorsed_stake <= required_stake {
            return Err(near_chain::Error::InvalidEpochSyncProof(format!(
                "Block {} does not have enough endorsements",
                block_height
            )));
        }

        Ok(())
    }
}

impl ClientActor {
    fn spawn_batched_epoch_sync_response(
        &self,
        task_name: &'static str,
        respond: impl FnOnce(&BatchedEpochSyncProof) -> Option<NetworkRequests> + Send + 'static,
        response_permit: OutgoingPermit,
    ) {
        if !ProtocolFeature::BatchedEpochSync.enabled(PROTOCOL_VERSION) {
            tracing::debug!(target: "sync", task_name, "ignoring batched epoch sync request, feature is not enabled");
            return;
        }

        let store = self.client.chain.chain_store.store();
        let transaction_validity_period = self.client.chain.transaction_validity_period();
        let cache = self.client.sync_handler.epoch_sync.last_batched_response_cache.clone();
        let network_adapter = self.client.network_adapter.clone();
        self.client.sync_handler.epoch_sync.async_computation_spawner.spawn(task_name, move || {
            let response = match EpochSync::derive_batched_epoch_sync_proof(
                store,
                transaction_validity_period,
                cache,
            ) {
                Ok(response) => response,
                Err(err) => {
                    tracing::error!(target: "sync", ?err, "failed to derive batched epoch sync proof");
                    return;
                }
            };
            if let Some(request) = respond(&response) {
                network_adapter.send(NetworkRequestWithPermit { request, permit: response_permit });
            }
        })
    }

    fn request_data_reset_if_stale(&mut self) -> bool {
        let tip_height = match self.client.chain.header_head() {
            Ok(head) => head.height,
            Err(err) => {
                tracing::error!(target: "sync", ?err, "failed to read header head while handling epoch sync proof");
                return true;
            }
        };
        if tip_height == self.client.chain.genesis().height() {
            return false;
        }
        tracing::info!(target: "sync", "stale node validated epoch sync proof, requesting data reset");
        if let Some(tx) = self.shutdown_signal.take() {
            let _ = tx.send(ShutdownReason::EpochSyncDataReset);
        }
        true
    }
}

impl Handler<EpochSyncRequestMessage> for ClientActor {
    fn handle(&mut self, msg: EpochSyncRequestMessage) {
        let response_permit = msg.response_permit;
        if ProtocolFeature::ContinuousEpochSync.enabled(PROTOCOL_VERSION) {
            // When ContinuousEpochSync is enabled, we simply return the stored compressed proof.
            // The proof is automatically updated at the beginning of each epoch via the epoch manager.
            let epoch_store = self.client.chain.chain_store.epoch_store();
            let Some(proof) = epoch_store.get_compressed_epoch_sync_proof() else {
                // This would likely only happen when the blockchain is an epoch or two around genesis.
                let chain_store = epoch_store.chain_store();
                let head = chain_store.head();
                let genesis_height = chain_store.get_genesis_height();
                tracing::warn!(target: "sync", ?head, ?genesis_height, "no epoch sync proof is stored");
                return;
            };
            self.client.network_adapter.send(NetworkRequestWithPermit {
                request: NetworkRequests::EpochSyncResponse { peer_id: msg.from_peer, proof },
                permit: response_permit,
            });
        } else {
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
                            tracing::error!(target: "sync", ?err, "failed to derive epoch sync proof");
                            return;
                        }
                    };
                    network_adapter.send(NetworkRequestWithPermit {
                        request: NetworkRequests::EpochSyncResponse {
                            peer_id: requester_peer_id,
                            proof,
                        },
                        permit: response_permit,
                    });
                },
            )
        }
    }
}

impl Handler<EpochSyncResponseMessage> for ClientActor {
    fn handle(&mut self, msg: EpochSyncResponseMessage) {
        // Pre-check: only decode if we are expecting an epoch sync response from this peer.
        // This avoids wasting resources processing unsolicited responses.
        match &self.client.sync_handler.sync_status {
            SyncStatus::EpochSync(EpochSyncStatus::InProgress { source_peer_id, .. })
                if *source_peer_id == msg.from_peer => {}
            _ => {
                tracing::warn!(target: "sync", from_peer = %msg.from_peer, "ignoring unsolicited epoch sync response");
                return;
            }
        }
        let (proof, _) = match msg.proof.decode() {
            Ok(proof) => proof,
            Err(err) => {
                tracing::error!(target: "sync", ?err, "failed to uncompress epoch sync proof");
                return;
            }
        };
        let proof = proof.into_v1();

        // Validate the proof without writing anything to the store.
        match self.client.sync_handler.epoch_sync.validate_proof(
            &self.client.sync_handler.sync_status,
            &self.client.chain,
            &proof,
            &msg.from_peer,
            self.client.epoch_manager.as_ref(),
        ) {
            Ok(true) => {}
            Ok(false) => return, // silently ignored (logged inside validate_proof)
            Err(err) => {
                tracing::error!(target: "sync", ?err, "failed to validate epoch sync proof");
                return;
            }
        }

        // If the proof is valid but the node is stale (data beyond genesis), shut down for data reset immediately
        if self.request_data_reset_if_stale() {
            return;
        }

        // Apply the validated proof to the store.
        if let Err(err) = self.client.sync_handler.epoch_sync.apply_validated_proof(
            &mut self.client.sync_handler.sync_status,
            &mut self.client.chain,
            proof,
            self.client.epoch_manager.as_ref(),
        ) {
            tracing::error!(target: "sync", ?err, "failed to apply epoch sync proof");
        }
    }
}

impl Handler<EpochSyncManifestRequestMessage> for ClientActor {
    fn handle(
        &mut self,
        EpochSyncManifestRequestMessage { from_peer, recv_permit: _, response_permit }: EpochSyncManifestRequestMessage,
    ) {
        self.spawn_batched_epoch_sync_response(
            "respond to epoch sync manifest request",
            move |response| {
                Some(NetworkRequests::EpochSyncManifestResponse {
                    peer_id: from_peer,
                    manifest: response.manifest.clone(),
                })
            },
            response_permit,
        );
    }
}

impl Handler<EpochSyncManifestResponseMessage> for ClientActor {
    fn handle(
        &mut self,
        EpochSyncManifestResponseMessage { from_peer, manifest, recv_permit: _ }: EpochSyncManifestResponseMessage,
    ) {
        let SyncStatus::EpochSync(EpochSyncStatus::FetchingManifest {
            source_peer_id,
            source_peer_height,
            attempt_time,
        }) = &self.client.sync_handler.sync_status
        else {
            tracing::warn!(target: "sync", %from_peer, "ignoring unsolicited epoch sync response");
            return;
        };

        let (manifest, _) = match manifest.decode() {
            Ok(manifest) => manifest,
            Err(err) => {
                tracing::error!(target: "sync", ?err, "failed to uncompress epoch sync proof manifest");
                return;
            }
        };

        if *source_peer_id != from_peer {
            tracing::warn!(target: "sync", %from_peer, "ignoring epoch sync response from a wrong peer");
            return;
        }

        let assembler = match EpochSyncProofAssembler::new(manifest.into_v1()) {
            Ok(assembler) => assembler,
            Err(err) => {
                tracing::warn!(target: "sync", %from_peer, ?err, "ignoring invalid manifest");
                return;
            }
        };
        let total_batches = assembler.total_batches() as u64;
        self.client.sync_handler.epoch_sync.proof_assembler = Some(assembler);
        self.client.sync_handler.sync_status =
            SyncStatus::EpochSync(EpochSyncStatus::FetchingBatches(FetchingEpochSyncBatchesState {
                manifest_source_peer_id: source_peer_id.clone(),
                manifest_source_peer_height: *source_peer_height,
                attempt_time: *attempt_time,
                total_batches,
                verified_batches: 0,
                in_flight: 0,
            }))
    }
}

impl Handler<EpochSyncBatchRequestMessage> for ClientActor {
    fn handle(
        &mut self,
        EpochSyncBatchRequestMessage {
            from_peer,
            batch_index,
            recv_permit: _,
            response_permit,
        }: EpochSyncBatchRequestMessage,
    ) {
        self.spawn_batched_epoch_sync_response(
            "respond to epoch sync batch request",
            move |response| {
                let Some(batch) = response.batches.get(batch_index as usize) else {
                    tracing::debug!(
                        target: "sync",
                        %from_peer,
                        batch_index,
                        num_batches = response.batches.len(),
                        "ignoring epoch sync batch request for an unknown batch",
                    );
                    return None;
                };
                Some(NetworkRequests::EpochSyncBatchResponse {
                    peer_id: from_peer,
                    batch_index,
                    batch: batch.clone(),
                })
            },
            response_permit,
        );
    }
}

impl Handler<EpochSyncBatchResponseMessage> for ClientActor {
    fn handle(
        &mut self,
        EpochSyncBatchResponseMessage {
            from_peer,
            batch_index,
            batch,
            recv_permit: _,
        }: EpochSyncBatchResponseMessage,
    ) {
        let SyncStatus::EpochSync(EpochSyncStatus::FetchingBatches(
            FetchingEpochSyncBatchesState { manifest_source_peer_id, verified_batches, .. },
        )) = &mut self.client.sync_handler.sync_status
        else {
            tracing::warn!(target: "sync", %from_peer, "ignoring unsolicited epoch proof batch response");
            return;
        };

        // As of today, we only request batches from the same peer we requested the manifest from.
        if *manifest_source_peer_id != from_peer {
            tracing::warn!(target: "sync", %from_peer, "ignoring epoch sync response from a wrong peer");
            return;
        }

        let Some(assembler) = self.client.sync_handler.epoch_sync.proof_assembler.as_mut() else {
            tracing::error!(target: "sync", "proof assembler is missing, this should never happen");
            return;
        };

        if !assembler.is_awaiting_batches(batch_index) {
            tracing::debug!(target: "sync", %from_peer, batch_index, "ignoring epoch sync proof batch that was not requested");
            return;
        }

        let (batch, _) = match batch.decode() {
            Ok(batch) => batch,
            Err(err) => {
                tracing::error!(target: "sync", batch_index, ?err, "failed to uncompress epoch sync proof batch");
                return;
            }
        };

        let batch = batch.into_v1();
        if batch_index == 0 {
            if let Err(err) = batch
                .epochs
                .first()
                .ok_or_else(|| Error::Other(String::from("batch 0 was empty")))
                .and_then(|first_epoch| {
                    EpochSync::verify_first_epoch_against_genesis(
                        &self.client.sync_handler.epoch_sync.genesis,
                        first_epoch,
                        self.client.epoch_manager.as_ref(),
                    )
                })
            {
                tracing::warn!(target: "sync", %from_peer, ?err, "rejecting epoch sync proof batch 0 that is not anchored at genesis");
                return;
            }
        }

        match assembler.try_add_batch(batch_index, batch) {
            Ok(_) => *verified_batches += 1,
            Err(err) => {
                tracing::error!(target: "sync", ?err, "Failed to add batch");
                return;
            }
        }

        if !assembler.is_ready() {
            return;
        }

        let proof = match assembler.try_build() {
            Ok(proof) => proof,
            Err(err) => {
                tracing::error!(target: "sync", ?err, "failed to build epoch sync proof");
                return;
            }
        };

        let restart_epoch_sync = |sync_handler: &mut SyncHandler| {
            sync_handler.epoch_sync.proof_assembler = None;
            sync_handler.sync_status.update(SyncStatus::EpochSync(EpochSyncStatus::NotStarted));
        };

        match self.client.sync_handler.epoch_sync.validate_proof(
            &self.client.sync_handler.sync_status,
            &self.client.chain,
            &proof,
            &from_peer,
            self.client.epoch_manager.as_ref(),
        ) {
            Ok(true) => {}
            Ok(false) => {
                restart_epoch_sync(&mut self.client.sync_handler);
                return;
            }
            Err(err) => {
                tracing::error!(target: "sync", %from_peer, ?err, "failed to validate epoch sync proof");
                restart_epoch_sync(&mut self.client.sync_handler);
                return;
            }
        }

        // If the proof is valid but the node is stale (data beyond genesis), shut down for data reset immediately
        if self.request_data_reset_if_stale() {
            return;
        }

        self.client.sync_handler.epoch_sync.proof_assembler = None;

        // Apply the validated proof to the store.
        if let Err(err) = self.client.sync_handler.epoch_sync.apply_validated_proof(
            &mut self.client.sync_handler.sync_status,
            &mut self.client.chain,
            proof,
            self.client.epoch_manager.as_ref(),
        ) {
            tracing::error!(target: "sync", ?err, "failed to apply epoch sync proof");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{EpochSync, EpochSyncProofAssembler};
    use near_chain::Error;
    use near_primitives::block::{Block, compute_bp_hash_from_validator_stakes};
    use near_primitives::epoch_sync::{
        EPOCHS_PER_BATCH_V1, EpochSyncProofCurrentEpochData, EpochSyncProofEpochData,
        EpochSyncProofLastEpochData, EpochSyncProofV1,
    };
    use near_primitives::genesis::genesis_block;
    use near_primitives::hash::CryptoHash;
    use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
    use near_primitives::types::validator_stake::ValidatorStake;
    use near_primitives::types::{Balance, EpochId};
    use near_primitives::validator_signer::ValidatorSigner;
    use near_primitives::version::PROTOCOL_VERSION;
    use near_time::{Clock, Utc};
    use std::sync::Arc;

    fn test_epoch_id(index: usize) -> EpochId {
        EpochId(CryptoHash::hash_bytes(format!("epoch{index}").as_bytes()))
    }

    fn test_block_producers(index: usize) -> Vec<ValidatorStake> {
        vec![ValidatorStake::test(format!("bp{index}").parse().unwrap())]
    }

    fn test_proof(
        num_epochs: usize,
        genesis: &Block,
        signer: &Arc<ValidatorSigner>,
    ) -> EpochSyncProofV1 {
        let all_epochs = (0..num_epochs)
            .map(|index| {
                let block =
                    TestBlockBuilder::from_prev_block(Clock::real(), genesis, signer.clone())
                        .height(index as u64 + 1)
                        .epoch_id(test_epoch_id(index))
                        .next_epoch_id(test_epoch_id(index + 1))
                        .next_bp_hash(compute_bp_hash_from_validator_stakes(
                            &test_block_producers(index + 1),
                            true,
                        ))
                        .build();
                EpochSyncProofEpochData {
                    block_producers: test_block_producers(index),
                    use_versioned_bp_hash_format: true,
                    last_final_block_header: Arc::new(block.header().clone()),
                    this_epoch_endorsements_for_last_final_block: vec![],
                }
            })
            .collect();

        let genesis_header = Arc::new(genesis.header().clone());
        EpochSyncProofV1 {
            all_epochs,
            last_epoch: EpochSyncProofLastEpochData {
                epoch_info: Default::default(),
                next_epoch_info: Default::default(),
                next_next_epoch_info: Default::default(),
                first_block_in_epoch: Default::default(),
                last_block_in_epoch: Default::default(),
                second_last_block_in_epoch: Default::default(),
            },
            current_epoch: EpochSyncProofCurrentEpochData {
                first_block_header_in_epoch: genesis_header.clone(),
                last_block_header_in_prev_epoch: genesis_header.clone(),
                second_last_block_header_in_prev_epoch: genesis_header,
                merkle_proof_for_first_block: vec![],
                partial_merkle_tree_for_first_block: Default::default(),
            },
        }
    }

    /// Splitting a proof and feeding the batches back through the assembler must reproduce the
    /// proof exactly, including when the batches arrive out of order.
    #[test]
    fn split_into_batches_and_reassemble_roundtrip() {
        let signer = Arc::new(create_test_signer("test"));
        let genesis = genesis_block(
            PROTOCOL_VERSION,
            vec![],
            Utc::UNIX_EPOCH,
            0,
            Balance::from_yoctonear(1),
            Balance::from_yoctonear(1),
            &vec![],
        );

        // One full batch plus a remainder, so that both the boundary anchor between batches and
        // the short last batch are exercised.
        let num_epochs = EPOCHS_PER_BATCH_V1 as usize + 1;
        let proof = test_proof(num_epochs, &genesis, &signer);

        let (manifest, batches) = proof.clone().split_into_batches();
        assert_eq!(manifest.total_epochs, num_epochs as u64);
        assert_eq!(manifest.expected_num_batches(), 2);
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].epochs.len(), EPOCHS_PER_BATCH_V1 as usize);
        assert_eq!(batches[1].epochs.len(), 1);

        let mut assembler = EpochSyncProofAssembler::new(manifest).unwrap();
        assert_eq!(assembler.total_batches(), 2);
        assert!(!assembler.is_ready());

        // Out of order on purpose: a batch is placed by its index and checked against the
        // manifest, so it must not depend on its neighbours having arrived.
        for batch_index in [1, 0] {
            assert!(!assembler.is_awaiting_batches(batch_index));
            assembler.mark_requested_for_test(batch_index);
            assert!(assembler.is_awaiting_batches(batch_index));
            assembler
                .try_add_batch(batch_index, batches[batch_index as usize].clone())
                .expect("batch produced by split_into_batches must be accepted");
        }

        assert!(assembler.is_ready());
        assert_eq!(assembler.try_build().unwrap(), proof);
    }

    #[test]
    fn try_add_batch_rejects_mismatched_batch() {
        let signer = Arc::new(create_test_signer("test"));
        let genesis = genesis_block(
            PROTOCOL_VERSION,
            vec![],
            Utc::UNIX_EPOCH,
            0,
            Balance::from_yoctonear(1),
            Balance::from_yoctonear(1),
            &vec![],
        );
        let num_epochs = EPOCHS_PER_BATCH_V1 as usize + 1;
        let (manifest, batches) = test_proof(num_epochs, &genesis, &signer).split_into_batches();

        // Serving batch 0's epochs under index 1 fails the length check.
        let mut assembler = EpochSyncProofAssembler::new(manifest.clone()).unwrap();
        assembler.mark_requested_for_test(1);
        assert!(assembler.try_add_batch(1, batches[0].clone()).is_err());

        // A batch that is the right length but starts at the wrong epoch fails the anchor check.
        let mut short_batch = batches[1].clone();
        short_batch.epochs = vec![batches[0].epochs[0].clone()];
        let mut assembler = EpochSyncProofAssembler::new(manifest).unwrap();
        assembler.mark_requested_for_test(1);
        assert!(assembler.try_add_batch(1, short_batch).is_err());
    }

    /// Regression test: an attacker-supplied epoch sync proof may carry a block header whose height
    /// is u64::MAX. `verify_block_endorsements` computes `block_height + 1`, which would overflow
    /// and crash a bootstrapping node. It must instead be rejected as an invalid proof.
    #[test]
    fn verify_block_endorsements_rejects_max_height() {
        let err = EpochSync::verify_block_endorsements(CryptoHash::default(), u64::MAX, &[], &[])
            .unwrap_err();
        match &err {
            Error::InvalidEpochSyncProof(msg) => {
                assert!(msg.contains("too large"), "unexpected message: {msg}");
            }
            _ => panic!("expected InvalidEpochSyncProof, got: {err}"),
        }
    }
}

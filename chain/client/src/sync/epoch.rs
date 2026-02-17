use crate::client_actor::ClientActor;
use near_async::futures::{AsyncComputationSpawner, AsyncComputationSpawnerExt};
use near_async::messaging::{CanSend, Handler};
use near_async::time::Clock;
use near_chain::types::Tip;
use near_chain::{BlockHeader, Chain, ChainStoreAccess, Error};
use near_chain_configs::EpochSyncConfig;
use near_client_primitives::types::{EpochSyncStatus, SyncStatus};
use near_crypto::Signature;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::epoch_sync::{
    derive_epoch_sync_proof_from_last_block, find_target_epoch_to_produce_proof_for,
    get_epoch_info_block_producers,
};
use near_network::client::{EpochSyncRequestMessage, EpochSyncResponseMessage};
use near_network::types::{
    HighestHeightPeerInfo, NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest,
};
use near_primitives::block::{Approval, ApprovalInner, compute_bp_hash_from_validator_stakes};
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_sync::{
    CompressedEpochSyncProof, EpochSyncProof, EpochSyncProofCurrentEpochData,
    EpochSyncProofEpochData, EpochSyncProofLastEpochData, EpochSyncProofV1,
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
    my_own_epoch_sync_boundary_block_header: Option<Arc<BlockHeader>>,
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
        self.my_own_epoch_sync_boundary_block_header.as_deref()
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

    /// Performs the epoch sync logic if applicable in the current state of the blockchain.
    /// This is periodically called by the client actor.
    pub fn run(
        &self,
        status: &mut SyncStatus,
        chain: &Chain,
        highest_height: BlockHeight,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Result<(), Error> {
        let tip_height = chain.chain_store().header_head()?.height;
        if tip_height != chain.genesis().height() {
            // Epoch Sync only supports bootstrapping at genesis. This is because there is no reason
            // to use Epoch Sync on an already existing node; we would have to carefully delete old
            // data and then the result would be the same as if we just started the node from
            // scratch.
            return Ok(());
        }
        if tip_height + self.config.epoch_sync_horizon_num_epochs * chain.epoch_length
            >= highest_height
        {
            return Ok(());
        }
        match status {
            SyncStatus::EpochSync(status) => {
                if status.attempt_time + self.config.timeout_for_epoch_sync < self.clock.now_utc() {
                    tracing::warn!(source_peer_id = %status.source_peer_id, "epoch sync from peer timed out, retrying");
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

        tracing::info!(peer_id=?peer.peer_info.id, "bootstrapping node via epoch sync");

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
                tracing::warn!(%source_peer, expected_peer = %status.source_peer_id, "ignoring epoch sync proof from unexpected peer");
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
                    %source_peer,
                    "ignoring epoch sync proof from peer that is too recent"
                );
                return Ok(());
            }
            if proof
                .current_epoch
                .first_block_header_in_epoch
                .height()
                .saturating_add(self.config.epoch_sync_horizon_num_epochs * chain.epoch_length)
                < status.source_peer_height
            {
                tracing::error!(
                    %source_peer,
                    "ignoring epoch sync proof from peer that is too old"
                );
                return Ok(());
            }
        } else {
            tracing::warn!(%source_peer, "ignoring unexpected epoch sync proof");
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

        *status = SyncStatus::EpochSyncDone;
        tracing::info!(epoch_id=?last_header.epoch_id(), "bootstrapped from epoch sync");

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
            != get_epoch_info_block_producers(&second_next_epoch_info_after_genesis)
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

        let mut total_stake = Balance::ZERO;
        let mut endorsed_stake = Balance::ZERO;

        for (validator, may_be_signature) in block_producers.iter().zip(endorsements.iter()) {
            if let Some(signature) = may_be_signature {
                if !signature.verify(&message_to_sign, validator.public_key()) {
                    return Err(near_chain::Error::InvalidEpochSyncProof(format!(
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

impl Handler<EpochSyncRequestMessage> for ClientActor {
    fn handle(&mut self, msg: EpochSyncRequestMessage) {
        if ProtocolFeature::ContinuousEpochSync.enabled(PROTOCOL_VERSION) {
            // When ContinuousEpochSync is enabled, we simply return the stored compressed proof.
            // The proof is automatically updated at the beginning of each epoch via the epoch manager.
            let epoch_store = self.client.chain.chain_store.epoch_store();
            let Some(proof) = epoch_store.get_compressed_epoch_sync_proof() else {
                // This would likely only happen when the blockchain is an epoch or two around genesis.
                let chain_store = epoch_store.chain_store();
                let head = chain_store.head();
                let genesis_height = chain_store.get_genesis_height();
                tracing::warn!(?head, ?genesis_height, "no epoch sync proof is stored");
                return;
            };
            self.client.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::EpochSyncResponse { peer_id: msg.from_peer, proof },
            ));
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
                            tracing::error!(?err, "failed to derive epoch sync proof");
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
}

impl Handler<EpochSyncResponseMessage> for ClientActor {
    fn handle(&mut self, msg: EpochSyncResponseMessage) {
        let (proof, _) = match msg.proof.decode() {
            Ok(proof) => proof,
            Err(err) => {
                tracing::error!(?err, "failed to uncompress epoch sync proof");
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
            tracing::error!(?err, "failed to apply epoch sync proof");
        }
    }
}

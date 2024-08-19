use crate::client_actor::ClientActorInner;
use borsh::BorshDeserialize;
use near_async::futures::{AsyncComputationSpawner, AsyncComputationSpawnerExt};
use near_async::messaging::{CanSend, Handler};
use near_async::time::Clock;
use near_chain::types::Tip;
use near_chain::{BlockHeader, Chain, ChainStoreAccess, Error};
use near_chain_configs::EpochSyncConfig;
use near_client_primitives::types::{EpochSyncStatus, SyncStatus};
use near_epoch_manager::EpochManagerAdapter;
use near_network::client::{EpochSyncRequestMessage, EpochSyncResponseMessage};
use near_network::types::{
    HighestHeightPeerInfo, NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest,
};
use near_performance_metrics_macros::perf;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::epoch_manager::AGGREGATOR_KEY;
use near_primitives::epoch_sync::{
    EpochSyncProof, EpochSyncProofCurrentEpochData, EpochSyncProofLastEpochData,
    EpochSyncProofPastEpochData,
};
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::network::PeerId;
use near_primitives::types::{BlockHeight, EpochId};
use near_store::{DBCol, Store, FINAL_HEAD_KEY};
use rand::seq::SliceRandom;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::instrument;

pub struct EpochSync {
    clock: Clock,
    network_adapter: PeerManagerAdapter,
    genesis: BlockHeader,
    async_computation_spawner: Arc<dyn AsyncComputationSpawner>,
    config: EpochSyncConfig,
}

impl EpochSync {
    pub fn new(
        clock: Clock,
        network_adapter: PeerManagerAdapter,
        genesis: BlockHeader,
        async_computation_spawner: Arc<dyn AsyncComputationSpawner>,
        config: EpochSyncConfig,
    ) -> Self {
        Self { clock, network_adapter, genesis, async_computation_spawner, config }
    }

    /// Derives an epoch sync proof for a recent epoch, that can be directly used to bootstrap
    /// a new node or bring a far-behind node to a recent epoch.
    #[instrument(skip(store))]
    fn derive_epoch_sync_proof(store: Store) -> Result<EpochSyncProof, Error> {
        // Epoch sync initializes a new node with the first block of some epoch; we call that
        // epoch the "target epoch". In the context of talking about the proof or the newly
        // bootstrapped node, it is also called the "current epoch".
        //
        // The basic requirement for picking the target epoch is that its first block must be
        // final. That's just so that we don't have to deal with any forks. Therefore, it is
        // sufficient to pick whatever epoch the current final block is in. However, because
        // state sync also requires some previous headers to be available (depending on how
        // many chunks were missing), it is more convenient to just pick the prior epoch, so
        // that by the time the new node does state sync, it would have a whole epoch of headers
        // available via header sync.
        //
        // In other words, we pick the target epoch to be the previous epoch of the final tip.
        let tip = store
            .get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)?
            .ok_or_else(|| Error::Other("Could not find tip".to_string()))?;
        let next_next_epoch_id = tip.next_epoch_id; // for finding last block of target epoch
        let target_epoch_last_block_header = store
            .get_ser::<BlockHeader>(DBCol::BlockHeader, next_next_epoch_id.0.as_bytes())?
            .ok_or_else(|| Error::Other("Could not find last block of target epoch".to_string()))?;
        let target_epoch_second_last_block_header = store
            .get_ser::<BlockHeader>(
                DBCol::BlockHeader,
                target_epoch_last_block_header.prev_hash().as_bytes(),
            )?
            .ok_or_else(|| {
                Error::Other("Could not find second last block of target epoch".to_string())
            })?;

        Self::derive_epoch_sync_proof_from_final_block(store, target_epoch_second_last_block_header)
    }

    /// Derives an epoch sync proof using a target epoch which the given block header is in.
    /// The given block header must be some block in the epoch that is right after a final
    /// block in the same epoch. But it doesn't matter which final block it is.
    fn derive_epoch_sync_proof_from_final_block(
        store: Store,
        next_block_header_after_final_block_in_current_epoch: BlockHeader,
    ) -> Result<EpochSyncProof, Error> {
        let final_block_header_in_current_epoch = store
            .get_ser::<BlockHeader>(
                DBCol::BlockHeader,
                next_block_header_after_final_block_in_current_epoch.prev_hash().as_bytes(),
            )?
            .ok_or_else(|| Error::Other("Could not find final block header".to_string()))?;

        let current_epoch = *final_block_header_in_current_epoch.epoch_id();
        let current_epoch_info = store
            .get_ser::<EpochInfo>(DBCol::EpochInfo, current_epoch.0.as_bytes())?
            .ok_or_else(|| Error::EpochOutOfBounds(current_epoch))?;
        let next_epoch = *final_block_header_in_current_epoch.next_epoch_id();
        let next_epoch_info = store
            .get_ser::<EpochInfo>(DBCol::EpochInfo, next_epoch.0.as_bytes())?
            .ok_or_else(|| Error::EpochOutOfBounds(next_epoch))?;

        // TODO: don't always generate from genesis
        let all_past_epochs = Self::get_all_past_epochs(
            &store,
            EpochId::default(),
            next_epoch,
            &final_block_header_in_current_epoch,
        )?;
        if all_past_epochs.is_empty() {
            return Err(Error::Other("Need at least three epochs to epoch sync".to_string()));
        }
        let prev_epoch = *all_past_epochs.last().unwrap().last_final_block_header.epoch_id();
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
                final_block_header_in_current_epoch.hash().as_bytes(),
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

        let first_block_info_of_current_epoch = store
            .get_ser::<BlockInfo>(
                DBCol::BlockInfo,
                block_info_for_final_block_of_current_epoch.epoch_first_block().as_bytes(),
            )?
            .ok_or_else(|| {
                Error::Other("Could not find first block info of next epoch".to_string())
            })?;

        let merkle_proof_for_first_block_of_current_epoch = store
            .get_ser::<PartialMerkleTree>(
                DBCol::BlockMerkleTree,
                first_block_of_current_epoch.hash().as_bytes(),
            )?
            .ok_or_else(|| {
                Error::Other("Could not find merkle proof for first block".to_string())
            })?;

        let proof = EpochSyncProof {
            past_epochs: all_past_epochs,
            last_epoch: EpochSyncProofLastEpochData {
                epoch_info: prev_epoch_info,
                next_epoch_info: current_epoch_info,
                next_next_epoch_info: next_epoch_info,
                first_block_in_epoch: first_block_info_of_prev_epoch,
                last_block_in_epoch: last_block_info_of_prev_epoch,
                second_last_block_in_epoch: second_last_block_info_of_prev_epoch,
                final_block_header_in_next_epoch: final_block_header_in_current_epoch,
                approvals_for_final_block_in_next_epoch:
                    next_block_header_after_final_block_in_current_epoch.approvals().to_vec(),
            },
            current_epoch: EpochSyncProofCurrentEpochData {
                first_block_header_in_epoch: first_block_of_current_epoch,
                first_block_info_in_epoch: first_block_info_of_current_epoch,
                last_block_header_in_prev_epoch: last_block_of_prev_epoch,
                second_last_block_header_in_prev_epoch: second_last_block_of_prev_epoch,
                merkle_proof_for_first_block: merkle_proof_for_first_block_of_current_epoch,
            },
        };

        Ok(proof)
    }

    /// Get all the past epoch data needed for epoch sync, between `after_epoch` and `next_epoch`
    /// (both exclusive). `current_epoch_any_header` is any block header in the current epoch,
    /// which is the epoch before `next_epoch`.
    #[instrument(skip(store, current_epoch_any_header))]
    fn get_all_past_epochs(
        store: &Store,
        after_epoch: EpochId,
        next_epoch: EpochId,
        current_epoch_any_header: &BlockHeader,
    ) -> Result<Vec<EpochSyncProofPastEpochData>, Error> {
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
            *current_epoch_any_header.next_epoch_id(),
            *current_epoch_any_header.epoch_id(),
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
        let epochs = (0..epoch_ids.len() - 2)
            .into_par_iter()
            .map(|index| -> Result<EpochSyncProofPastEpochData, Error> {
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
                Ok(EpochSyncProofPastEpochData {
                    block_producers: epoch_info
                        .block_producers_settlement()
                        .iter()
                        .map(|validator_id| epoch_info.get_validator(*validator_id))
                        .collect(),
                    last_final_block_header: third_last_block_header,
                    approvals_for_last_final_block: second_last_block_header.approvals().to_vec(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(epochs)
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

        let peer = highest_height_peers
            .choose(&mut rand::thread_rng())
            .ok_or_else(|| Error::Other("No peers to request epoch sync from".to_string()))?;

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
            if proof.current_epoch.first_block_header_in_epoch.height()
                + self.config.epoch_sync_accept_proof_max_horizon
                < status.source_peer_height
            {
                tracing::error!(
                    "Ignoring epoch sync proof from peer {} with too high height",
                    source_peer
                );
                return Ok(());
            }
        } else {
            tracing::warn!("Ignoring unexpected epoch sync proof from peer: {}", source_peer);
            return Ok(());
        }

        // TODO(#11932): Verify the proof.

        let last_header = proof.current_epoch.first_block_header_in_epoch;
        let mut store_update = chain.chain_store.store().store_update();

        let mut update = chain.mut_chain_store().store_update();
        update.save_block_header_no_update_tree(last_header.clone())?;
        update.save_block_header_no_update_tree(
            proof.current_epoch.last_block_header_in_prev_epoch,
        )?;
        update.save_block_header_no_update_tree(
            proof.current_epoch.second_last_block_header_in_prev_epoch,
        )?;
        tracing::info!(
            "last final block of last past epoch: {:?}",
            proof.past_epochs.last().unwrap().last_final_block_header.hash()
        );
        update.save_block_header_no_update_tree(
            proof.past_epochs.last().unwrap().last_final_block_header.clone(),
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
            proof.last_epoch.final_block_header_in_next_epoch.epoch_id(),
            proof.last_epoch.next_epoch_info,
            proof.last_epoch.final_block_header_in_next_epoch.next_epoch_id(),
            proof.last_epoch.next_next_epoch_info,
        )?;

        store_update.insert_ser(
            DBCol::BlockInfo,
            &borsh::to_vec(proof.current_epoch.first_block_info_in_epoch.hash()).unwrap(),
            &proof.current_epoch.first_block_info_in_epoch,
        )?;

        store_update.set_ser(
            DBCol::BlockOrdinal,
            &borsh::to_vec(&proof.current_epoch.merkle_proof_for_first_block.size()).unwrap(),
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
            &proof.current_epoch.merkle_proof_for_first_block,
        )?;

        update.merge(store_update);
        update.commit()?;

        *status = SyncStatus::EpochSyncDone;

        Ok(())
    }
}

impl Handler<EpochSyncRequestMessage> for ClientActorInner {
    #[perf]
    fn handle(&mut self, msg: EpochSyncRequestMessage) {
        let store = self.client.chain.chain_store.store().clone();
        let network_adapter = self.client.network_adapter.clone();
        let route_back = msg.route_back;
        self.client.epoch_sync.async_computation_spawner.spawn(
            "respond to epoch sync request",
            move || {
                let proof = match EpochSync::derive_epoch_sync_proof(store) {
                    Ok(epoch_sync_proof) => epoch_sync_proof,
                    Err(e) => {
                        tracing::error!("Failed to derive epoch sync proof: {:?}", e);
                        return;
                    }
                };
                network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                    NetworkRequests::EpochSyncResponse { route_back, proof },
                ));
            },
        )
    }
}

impl Handler<EpochSyncResponseMessage> for ClientActorInner {
    #[perf]
    fn handle(&mut self, msg: EpochSyncResponseMessage) {
        if let Err(e) = self.client.epoch_sync.apply_proof(
            &mut self.client.sync_status,
            &mut self.client.chain,
            msg.proof,
            msg.from_peer,
            self.client.epoch_manager.as_ref(),
        ) {
            tracing::error!("Failed to apply epoch sync proof: {:?}", e);
        }
    }
}

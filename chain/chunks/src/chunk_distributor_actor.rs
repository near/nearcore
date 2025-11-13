use crate::adapter::ShardsManagerRequestFromClient;
use crate::logic::make_outgoing_receipts_proofs;
use crate::metrics;
use near_async::messaging::{Actor, Handler, Sender};
use near_chain_configs::MutableValidatorSigner;
pub use near_chunks_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::types::{NetworkRequests, PeerManagerMessageRequest};
use near_primitives::merkle::MerklePath;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{EncodedShardChunk, PartialEncodedChunk};
use near_primitives::types::AccountId;
use std::collections::HashMap;
use std::sync::Arc;

pub struct ChunkDistributorActor {
    validator_signer: MutableValidatorSigner,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    peer_manager_adapter: Sender<PeerManagerMessageRequest>,
    shards_manager_adapter: Sender<ShardsManagerRequestFromClient>,
}

impl Actor for ChunkDistributorActor {}

impl Handler<ShardsManagerRequestFromClient> for ChunkDistributorActor {
    fn handle(&mut self, msg: ShardsManagerRequestFromClient) {
        let ShardsManagerRequestFromClient::DistributeEncodedChunk {
            partial_chunk,
            encoded_chunk,
            merkle_paths,
            outgoing_receipts,
        } = msg
        else {
            panic!("Unexpected message type");
        };
        let me = self.validator_signer.get().map(|signer| signer.validator_id().clone());
        let me = me.as_ref();
        if let Err(e) = self.distribute_encoded_chunk(
            &partial_chunk,
            &encoded_chunk,
            &merkle_paths,
            outgoing_receipts,
            me,
        ) {
            tracing::warn!(target: "chunks", "Error distributing encoded chunk: {:?}", e);
        }
        self.shards_manager_adapter.send(ShardsManagerRequestFromClient::DistributeEncodedChunk {
            partial_chunk,
            encoded_chunk,
            merkle_paths,
            outgoing_receipts: vec![], // unused
        });
    }
}

impl ChunkDistributorActor {
    pub fn new(
        validator_signer: MutableValidatorSigner,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        peer_manager_adapter: Sender<PeerManagerMessageRequest>,
        shards_manager_adapter: Sender<ShardsManagerRequestFromClient>,
    ) -> Self {
        Self {
            validator_signer,
            epoch_manager,
            shard_tracker,
            peer_manager_adapter,
            shards_manager_adapter,
        }
    }

    fn distribute_encoded_chunk(
        &mut self,
        partial_chunk: &PartialEncodedChunk,
        encoded_chunk: &EncodedShardChunk,
        merkle_paths: &[MerklePath],
        outgoing_receipts: Vec<Receipt>,
        me: Option<&AccountId>,
    ) -> Result<(), Error> {
        let shard_id = encoded_chunk.shard_id();
        let _timer = metrics::DISTRIBUTE_ENCODED_CHUNK_TIME
            .with_label_values(&[&shard_id.to_string()])
            .start_timer();
        // TODO: if the number of validators exceeds the number of parts, this logic must be changed
        let chunk_header = encoded_chunk.cloned_header();
        #[cfg(not(feature = "test_features"))]
        debug_assert_eq!(chunk_header, partial_chunk.cloned_header());
        let prev_block_hash = chunk_header.prev_block_hash();

        let mut block_producer_mapping = HashMap::new();
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(&prev_block_hash)?;
        for part_ord in 0..self.epoch_manager.num_total_parts() {
            let part_ord = part_ord as u64;
            let to_whom = self.epoch_manager.get_part_owner(&epoch_id, part_ord).unwrap();

            let entry = block_producer_mapping.entry(to_whom).or_insert_with(Vec::new);
            entry.push(part_ord);
        }

        // Receipt proofs need to be distributed to the block producers of the next epoch
        // because they already start tracking the shard in the current epoch.
        let next_epoch_id = self.epoch_manager.get_next_epoch_id(prev_block_hash)?;
        let next_epoch_block_producers =
            self.epoch_manager.get_epoch_block_producers_ordered(&next_epoch_id)?;
        for bp in next_epoch_block_producers {
            if !block_producer_mapping.contains_key(bp.account_id()) {
                block_producer_mapping.insert(bp.account_id().clone(), vec![]);
            }
        }

        let receipt_proofs = make_outgoing_receipts_proofs(
            &chunk_header,
            outgoing_receipts,
            self.epoch_manager.as_ref(),
        )?
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>();

        for (to_whom, part_ords) in block_producer_mapping {
            if Some(&to_whom) == me {
                continue;
            }

            let part_receipt_proofs = receipt_proofs
                .iter()
                .filter(|proof| {
                    let proof_shard_id = proof.1.to_shard_id;
                    self.shard_tracker.cares_about_shard_this_or_next_epoch_for_account_id(
                        &to_whom,
                        &prev_block_hash,
                        proof_shard_id,
                    )
                })
                .cloned()
                .collect();

            let partial_encoded_chunk = encoded_chunk
                .create_partial_encoded_chunk_with_arc_receipts(
                    part_ords,
                    part_receipt_proofs,
                    &merkle_paths,
                );

            self.peer_manager_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::PartialEncodedChunkMessage {
                    account_id: to_whom,
                    partial_encoded_chunk,
                },
            ));
        }

        Ok(())
    }
}

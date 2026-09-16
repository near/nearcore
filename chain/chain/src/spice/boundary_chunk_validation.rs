//! Validation of the first spice block.

use crate::chain::{NewChunkData, ShardContext, StorageContext};
use crate::sharding::{get_receipts_shuffle_salt, shuffle_receipt_proofs};
use crate::spice::boundary::{anchor_and_replay_blocks, boundary_source_blocks_for_target};
use crate::spice::chunk_validation::{SpicePreValidationOutput, validate_receipt_proof};
use crate::store::filter_incoming_receipts_for_shard;
use crate::types::{
    ApplyChunkBlockContext, MaybePinnedMemtrieRoot, RuntimeAdapter, StorageDataSource,
};
use crate::update_shard::{OldChunkData, OldChunkResult, apply_old_chunk};
use crate::{Chain, ChainStore};
use itertools::Itertools;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_primitives::apply::ApplyChunkReason;
use near_primitives::block::Block;
use near_primitives::hash::hash;
use near_primitives::merkle::merklize;
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::sharding::{ChunkHash, ReceiptProof};
use near_primitives::spice::state_witness::SpiceBoundaryChunkStateWitness;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{BlockExecutionResults, ShardId};
use near_store::PartialStorage;
use node_runtime::SignedValidPeriodTransactions;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::Span;

/// An old-chunk replay of a boundary witness, derived from an on-chain block.
pub(super) struct BoundaryReplay {
    pub(super) block_context: ApplyChunkBlockContext,
    pub(super) shard_uid: ShardUId,
}

/// Pre-spice, a chunk missing in `block` was last applied at the anchor: the block
/// of the shard's last included chunk. The main transition is the anchor's, with
/// the anchor's context and receipts, and every later block through `block` is an
/// old-chunk replay.
pub(super) fn pre_validate_boundary_chunk_state_witness(
    state_witness: &SpiceBoundaryChunkStateWitness,
    block: &Block,
    prev_execution_results: &BlockExecutionResults,
    epoch_manager: &dyn EpochManagerAdapter,
    store: &ChainStore,
    prev_validator_proposals: Vec<ValidatorStake>,
) -> Result<SpicePreValidationOutput, Error> {
    if block.is_spice_block() {
        return Err(Error::InvalidChunkStateWitness(
            "boundary witness for a spice block".to_string(),
        ));
    }
    if block.header().is_genesis() {
        return Err(Error::InvalidChunkStateWitness(
            "State witness is for genesis block".to_string(),
        ));
    }
    let epoch_id = epoch_manager.get_epoch_id(block.header().hash())?;
    let shard_id = state_witness.chunk_id.shard_id;
    let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
    if !shard_layout.shard_ids().contains(&shard_id) {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Shard layout for block's ({:?}) epoch ({:?}) doesn't contain witness shard {:?}",
            block.hash(),
            epoch_id,
            shard_id
        )));
    }
    let chunks = block.chunks();
    let shard_index = shard_layout.get_shard_index(shard_id)?;
    let chunk_header = chunks.get(shard_index).ok_or(Error::InvalidShardId(shard_id))?;

    let (anchor_block, replay_blocks) =
        anchor_and_replay_blocks(store, epoch_manager, block, shard_id)?;
    let anchor_prev_block = store.get_block(anchor_block.header().prev_hash())?;
    let anchor_epoch_id = epoch_manager.get_epoch_id(anchor_block.header().hash())?;
    let anchor_shard_layout = epoch_manager.get_shard_layout(&anchor_epoch_id)?;
    let is_new_chunk = chunk_header.is_new_chunk(anchor_block.header().height());
    if is_new_chunk {
        let protocol_version = epoch_manager.get_epoch_info(&anchor_epoch_id)?.protocol_version();
        chunk_header.validate_version(protocol_version)?;
    }

    let mut boundary_replays = Vec::with_capacity(replay_blocks.len());
    for replay_block in &replay_blocks {
        let replay_prev_header = store.get_block_header(replay_block.header().prev_hash())?;
        let block_context =
            Chain::get_apply_chunk_block_context(replay_block, &replay_prev_header, false);
        let replay_epoch_id = epoch_manager.get_epoch_id(replay_block.header().hash())?;
        let shard_uid = shard_id_to_uid(epoch_manager, shard_id, &replay_epoch_id)?;
        boundary_replays.push(BoundaryReplay { block_context, shard_uid });
    }

    let source_blocks =
        boundary_source_blocks_for_target(store, epoch_manager, &anchor_block, shard_id)?;
    let receipts_to_apply = validate_boundary_source_receipts_proofs(
        &state_witness.source_receipt_proofs,
        &source_blocks,
        &anchor_shard_layout,
        shard_id,
    )?;
    let applied_receipts_hash = hash(&borsh::to_vec(receipts_to_apply.as_slice()).unwrap());
    if applied_receipts_hash != state_witness.applied_receipts_hash {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Receipts hash {:?} does not match expected receipts hash {:?}",
            applied_receipts_hash, state_witness.applied_receipts_hash
        )));
    }

    let (tx_root_from_state_witness, _) = merklize(&state_witness.transactions);
    let chunk_tx_root = if is_new_chunk {
        *chunk_header.tx_root()
    } else {
        // Missing chunks are treated as empty chunks.
        let (empty_txs_root, _) = merklize::<SignedTransaction>(&[]);
        empty_txs_root
    };
    if chunk_tx_root != tx_root_from_state_witness {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Transaction root {:?} does not match expected transaction root {:?}",
            tx_root_from_state_witness, chunk_tx_root
        )));
    }
    let transaction_validity_check_results = state_witness
        .transactions
        .iter()
        .map(|tx| {
            store
                .check_transaction_validity_period(
                    anchor_prev_block.header(),
                    tx.transaction.block_hash(),
                )
                .is_ok()
        })
        .collect::<Vec<_>>();

    let (_, prev_shard_id, _) =
        epoch_manager.get_prev_shard_id_from_prev_hash(anchor_prev_block.hash(), shard_id)?;
    let prev_chunk_chunk_extra = &prev_execution_results
        .0
        .get(&prev_shard_id)
        .expect("execution results for all prev_block chunks should be available")
        .chunk_extra;
    let new_chunk_data = NewChunkData {
        gas_limit: prev_chunk_chunk_extra.gas_limit(),
        prev_state_root: *prev_chunk_chunk_extra.state_root(),
        prev_validator_proposals,
        chunk_hash: if is_new_chunk { Some(chunk_header.chunk_hash().clone()) } else { None },
        transactions: SignedValidPeriodTransactions::new(
            state_witness.transactions.clone(),
            transaction_validity_check_results,
        ),
        receipts: receipts_to_apply,
        block: Chain::get_apply_chunk_block_context(
            &anchor_block,
            anchor_prev_block.header(),
            true,
        ),
        storage_context: StorageContext {
            storage_data_source: StorageDataSource::Recorded(PartialStorage {
                nodes: state_witness.pre_state.clone(),
            }),
            state_patch: Default::default(),
        },
    };
    Ok(SpicePreValidationOutput { new_chunk_data, boundary_replays })
}

/// Boundary-witness counterpart of `validate_source_receipts_proofs`, shaped like
/// the pre-spice validation.
fn validate_boundary_source_receipts_proofs(
    source_receipt_proofs: &HashMap<ChunkHash, ReceiptProof>,
    source_blocks: &[Arc<Block>],
    shard_layout: &ShardLayout,
    target_shard_id: ShardId,
) -> Result<Vec<Receipt>, Error> {
    let mut receipts = Vec::new();
    let mut expected_proofs = 0;
    for source_block in source_blocks {
        let mut block_proofs = Vec::new();
        for chunk_header in source_block.chunks().iter_new() {
            let Some(receipt_proof) = source_receipt_proofs.get(&chunk_header.chunk_hash()) else {
                return Err(Error::InvalidChunkStateWitness(format!(
                    "Missing source receipt proof for chunk {:?}",
                    chunk_header.chunk_hash()
                )));
            };
            validate_receipt_proof(
                receipt_proof,
                chunk_header.shard_id(),
                target_shard_id,
                *chunk_header.prev_outgoing_receipts_root(),
            )?;
            expected_proofs += 1;
            block_proofs.push(receipt_proof.clone());
        }

        let mut block_proofs = filter_incoming_receipts_for_shard(
            shard_layout,
            target_shard_id,
            Arc::new(block_proofs),
        )?;
        shuffle_receipt_proofs(&mut block_proofs, get_receipts_shuffle_salt(source_block));
        receipts.extend(block_proofs.into_iter().map(|proof| proof.0).flatten());
    }

    if source_receipt_proofs.len() != expected_proofs {
        return Err(Error::InvalidChunkStateWitness(format!(
            "source_receipt_proofs contains too many proofs. Expected {} proofs, found {}",
            expected_proofs,
            source_receipt_proofs.len(),
        )));
    }
    Ok(receipts)
}

/// Replays the witness's old-chunk transitions on top of the main transition's
/// `chunk_extra`, checking each against the post state root it claims.
pub(super) fn replay_boundary_implicit_transitions(
    state_witness: &SpiceBoundaryChunkStateWitness,
    boundary_replays: Vec<BoundaryReplay>,
    mut chunk_extra: ChunkExtra,
    runtime_adapter: &dyn RuntimeAdapter,
) -> Result<ChunkExtra, Error> {
    let implicit_transitions = &state_witness.implicit_transitions;
    if boundary_replays.len() != implicit_transitions.len() {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Implicit transitions count mismatch. Expected {}, found {}",
            boundary_replays.len(),
            implicit_transitions.len(),
        )));
    }
    for (BoundaryReplay { block_context, shard_uid }, transition) in
        boundary_replays.into_iter().zip(implicit_transitions)
    {
        let old_chunk_data = OldChunkData {
            prev_chunk_extra: chunk_extra.clone(),
            block: block_context,
            storage_context: StorageContext {
                storage_data_source: StorageDataSource::Recorded(PartialStorage {
                    nodes: transition.base_state.clone(),
                }),
                state_patch: Default::default(),
            },
        };
        let OldChunkResult { apply_result, .. } = apply_old_chunk(
            ApplyChunkReason::ValidateChunkStateWitness,
            &Span::current(),
            old_chunk_data,
            ShardContext { shard_uid, should_apply_chunk: false },
            runtime_adapter,
            // Recorded-storage replay; no memtrie path.
            MaybePinnedMemtrieRoot::no_memtries(),
        )?;
        chunk_extra = chunk_extra.next_for_old_chunk(apply_result.new_root);
        if chunk_extra.state_root() != &transition.post_state_root {
            return Err(Error::InvalidChunkStateWitness(format!(
                "Post state root {:?} for implicit transition at block {:?} does not match expected state root {:?}",
                chunk_extra.state_root(),
                transition.block_hash,
                transition.post_state_root,
            )));
        }
    }
    Ok(chunk_extra)
}

#[cfg(test)]
/// Era-semantics tests for a witness of the spice activation parent whose chunk
/// is missing.
mod tests {
    use super::*;
    use crate::spice::boundary::execution_result_from_pre_spice_child;
    use crate::spice::chunk_validation::spice_pre_validate_chunk_state_witness;
    use crate::spice::tests::{
        pre_spice_chunk_endorsements, save_and_record_block, setup_pre_spice_chain,
    };
    use near_async::time::Clock;
    use near_o11y::testonly::init_test_logger;
    use near_primitives::bandwidth_scheduler::BandwidthRequests;
    use near_primitives::congestion_info::CongestionInfo;
    use near_primitives::gas::Gas;
    use near_primitives::hash::CryptoHash;
    use near_primitives::sharding::{ShardChunkHeader, ShardChunkHeaderV3};
    use near_primitives::spice::state_witness::SpiceBoundaryChunkStateWitness;
    use near_primitives::spice::state_witness::SpiceChunkStateWitness;
    use near_primitives::state::PartialState;
    use near_primitives::test_utils::{
        TestBlockBuilder, create_test_signer, pre_spice_protocol_version,
    };
    use near_primitives::types::{AccountId, SpiceChunkId};
    use near_primitives::types::{Balance, BlockHeight};
    use std::collections::BTreeSet;

    struct BoundaryChain {
        chain: Chain,
        /// Block including the other shard's chunk, mid-range for the target
        /// shard's witness: the target's chunk is missing in it.
        mid_range_block: Arc<Block>,
        /// Block of the target shard's last included chunk.
        anchor_block: Arc<Block>,
        /// The pre-spice block the witness is keyed to; every chunk missing.
        boundary_block: Arc<Block>,
        target_shard_id: ShardId,
        other_shard_id: ShardId,
    }

    fn test_receiver() -> &'static str {
        "test1"
    }

    /// A staggered gap straddling the anchor:
    /// full block -> mid-range (other new, target missing) -> anchor (target
    /// new, other new iff `other_included_at_anchor`) -> boundary block
    /// (everything missing).
    fn setup_boundary_chain(other_included_at_anchor: bool) -> BoundaryChain {
        init_test_logger();
        let chain = setup_pre_spice_chain(2);
        let genesis_block = chain.get_block(&chain.genesis().hash().clone()).unwrap();
        let shard_layout =
            chain.epoch_manager.get_shard_layout(genesis_block.header().epoch_id()).unwrap();
        let target_shard_id = test_receiver()
            .parse::<AccountId>()
            .map(|account_id| shard_layout.account_id_to_shard_id(&account_id))
            .unwrap();
        let other_shard_id =
            shard_layout.shard_ids().find(|shard_id| shard_id != &target_shard_id).unwrap();

        let mut boundary_chain = BoundaryChain {
            chain,
            mid_range_block: genesis_block.clone(),
            anchor_block: genesis_block.clone(),
            boundary_block: genesis_block,
            target_shard_id,
            other_shard_id,
        };
        let full_block =
            boundary_chain.add_block(&boundary_chain.genesis(), &[target_shard_id, other_shard_id]);
        boundary_chain.mid_range_block = boundary_chain.add_block(&full_block, &[other_shard_id]);
        let anchor_shards: &[ShardId] = if other_included_at_anchor {
            &[target_shard_id, other_shard_id]
        } else {
            &[target_shard_id]
        };
        boundary_chain.anchor_block =
            boundary_chain.add_block(&boundary_chain.mid_range_block.clone(), anchor_shards);
        boundary_chain.boundary_block =
            boundary_chain.add_block(&boundary_chain.anchor_block.clone(), &[]);
        boundary_chain
    }

    impl BoundaryChain {
        fn genesis(&self) -> Arc<Block> {
            self.chain.get_block(&self.chain.genesis().hash().clone()).unwrap()
        }

        fn shard_layout(&self) -> ShardLayout {
            self.chain
                .epoch_manager
                .get_shard_layout(self.boundary_block.header().epoch_id())
                .unwrap()
        }

        fn receipts_from(&self, from_shard_id: ShardId, height: BlockHeight) -> Vec<Receipt> {
            let shard_id: u64 = from_shard_id.into();
            let deposit = 100 * u128::from(height) + u128::from(shard_id);
            vec![Receipt::new_balance_refund(
                &test_receiver().parse().unwrap(),
                Balance::from_yoctonear(deposit),
            )]
        }

        /// The receipts root a new chunk of `shard_id` included at `height`
        /// commits to as its previous chunk's output.
        fn receipts_root_from(&self, shard_id: ShardId, height: BlockHeight) -> CryptoHash {
            let (root, _) = Chain::create_receipts_proofs_from_outgoing_receipts(
                &self.shard_layout(),
                shard_id,
                self.receipts_from(shard_id, height),
            )
            .unwrap();
            root
        }

        /// Fabricates the next block: shards in `new_chunk_shards` get a new
        /// pre-spice chunk header whose `prev_outgoing_receipts_root` commits
        /// to [`Self::receipts_from`] that shard at this height — the root
        /// witness source proofs are verified against — and every other shard
        /// carries the previous block's header.
        fn add_block(&mut self, prev_block: &Block, new_chunk_shards: &[ShardId]) -> Arc<Block> {
            let signer = Arc::new(create_test_signer("test1"));
            let height = prev_block.header().height() + 1;
            let chunks: Vec<_> = prev_block
                .chunks()
                .iter_raw()
                .map(|carried| {
                    let shard_id = carried.shard_id();
                    if !new_chunk_shards.contains(&shard_id) {
                        return carried.clone();
                    }
                    let mut chunk_header = ShardChunkHeader::V3(ShardChunkHeaderV3::new(
                        *prev_block.hash(),
                        CryptoHash::default(),
                        CryptoHash::default(),
                        CryptoHash::default(),
                        0,
                        height,
                        shard_id,
                        Gas::ZERO,
                        Gas::ZERO,
                        Balance::ZERO,
                        self.receipts_root_from(shard_id, height),
                        CryptoHash::default(),
                        vec![],
                        CongestionInfo::default(),
                        BandwidthRequests::empty(),
                        None,
                        &signer,
                        pre_spice_protocol_version(),
                    ));
                    *chunk_header.height_included_mut() = height;
                    chunk_header
                })
                .collect();
            let chunk_endorsements =
                pre_spice_chunk_endorsements(&self.chain, prev_block, height, &chunks);
            let block = TestBlockBuilder::from_prev_block(Clock::real(), prev_block, signer)
                .chunks(chunks)
                .chunk_endorsements(chunk_endorsements)
                .protocol_version(pre_spice_protocol_version())
                .build();
            save_and_record_block(&mut self.chain, &block, pre_spice_protocol_version());
            block
        }

        /// Hash of the chunk of `shard_id` carried by `block`.
        fn chunk_hash(&self, block: &Block, shard_id: ShardId) -> ChunkHash {
            let shard_index = self.shard_layout().get_shard_index(shard_id).unwrap();
            let chunks = block.chunks();
            chunks.get(shard_index).unwrap().chunk_hash().clone()
        }

        fn source_blocks(&self) -> Vec<Arc<Block>> {
            boundary_source_blocks_for_target(
                &self.chain.chain_store,
                self.chain.epoch_manager.as_ref(),
                &self.anchor_block,
                self.target_shard_id,
            )
            .unwrap()
        }

        fn source_receipt_proofs(&self) -> HashMap<ChunkHash, ReceiptProof> {
            let shard_layout = self.shard_layout();
            let mut source_receipt_proofs = HashMap::new();
            for source_block in self.source_blocks() {
                let height = source_block.header().height();
                for chunk_header in source_block.chunks().iter_new() {
                    let (_, proofs) = Chain::create_receipts_proofs_from_outgoing_receipts(
                        &shard_layout,
                        chunk_header.shard_id(),
                        self.receipts_from(chunk_header.shard_id(), height),
                    )
                    .unwrap();
                    let proof = proofs
                        .into_iter()
                        .find(|proof| proof.1.to_shard_id == self.target_shard_id)
                        .unwrap();
                    source_receipt_proofs.insert(chunk_header.chunk_hash().clone(), proof);
                }
            }
            source_receipt_proofs
        }

        fn prev_execution_results(&self) -> BlockExecutionResults {
            let prev_result = execution_result_from_pre_spice_child(
                self.chain.epoch_manager.as_ref(),
                &self.anchor_block,
                self.target_shard_id,
            )
            .unwrap()
            .unwrap();
            BlockExecutionResults(HashMap::from([(self.target_shard_id, Arc::new(prev_result))]))
        }

        fn boundary_witness_with_proofs(
            &self,
            source_receipt_proofs: HashMap<ChunkHash, ReceiptProof>,
        ) -> SpiceChunkStateWitness {
            let receipts_to_apply = validate_boundary_source_receipts_proofs(
                &self.source_receipt_proofs(),
                &self.source_blocks(),
                &self.shard_layout(),
                self.target_shard_id,
            )
            .unwrap();
            let applied_receipts_hash = hash(&borsh::to_vec(receipts_to_apply.as_slice()).unwrap());
            SpiceChunkStateWitness::Boundary(SpiceBoundaryChunkStateWitness {
                chunk_id: SpiceChunkId {
                    block_hash: *self.boundary_block.hash(),
                    shard_id: self.target_shard_id,
                },
                pre_state: PartialState::TrieValues(vec![]),
                source_receipt_proofs,
                applied_receipts_hash,
                transactions: vec![],
                contract_accesses: BTreeSet::new(),
                implicit_transitions: vec![],
            })
        }

        fn boundary_witness(&self) -> SpiceChunkStateWitness {
            self.boundary_witness_with_proofs(self.source_receipt_proofs())
        }

        fn run_pre_validation(
            &self,
            state_witness: &SpiceChunkStateWitness,
        ) -> Result<SpicePreValidationOutput, Error> {
            spice_pre_validate_chunk_state_witness(
                state_witness,
                &self.boundary_block,
                &self.anchor_block,
                &self.prev_execution_results(),
                self.chain.epoch_manager.as_ref(),
                self.chain.chain_store(),
                vec![],
            )
        }
    }

    fn assert_rejected(result: Result<SpicePreValidationOutput, Error>, what: &str) {
        let err = match result {
            Ok(_) => panic!("{what} must be rejected"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::InvalidChunkStateWitness(_)), "wrong error kind: {err:?}",);
    }

    fn sorted(mut receipts: Vec<Receipt>) -> Vec<Receipt> {
        receipts.sort_by_cached_key(|receipt| borsh::to_vec(receipt).unwrap());
        receipts
    }

    /// The main transition's context must be the anchor's pre-spice context: the
    /// anchor's height and parent, the anchor parent's gas price, and the
    /// anchor's real missed-chunk counts (the other shard's chunk is missing in
    /// the anchor).
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_boundary_witness_uses_pre_spice_anchor_context() {
        let boundary_chain = setup_boundary_chain(false);
        let witness = boundary_chain.boundary_witness();
        let output = boundary_chain.run_pre_validation(&witness).unwrap();

        let anchor_block = &boundary_chain.anchor_block;
        let block_context = &output.new_chunk_data.block;
        assert_eq!(block_context.height, anchor_block.header().height());
        assert_eq!(&block_context.prev_block_hash, anchor_block.header().prev_hash());
        let anchor_prev_header =
            boundary_chain.chain.get_block_header(anchor_block.header().prev_hash()).unwrap();
        assert_eq!(block_context.gas_price, anchor_prev_header.next_gas_price());
        let congestion_info = &block_context.congestion_info;
        assert_eq!(
            congestion_info.get(&boundary_chain.target_shard_id).unwrap().missed_chunks_count,
            0,
        );
        assert_eq!(
            congestion_info.get(&boundary_chain.other_shard_id).unwrap().missed_chunks_count,
            1,
        );

        // Main transition is the anchor's chunk.
        let shard_layout = boundary_chain.shard_layout();
        let target_shard_index =
            shard_layout.get_shard_index(boundary_chain.target_shard_id).unwrap();
        let anchor_chunks = anchor_block.chunks();
        let anchor_chunk_header = anchor_chunks.get(target_shard_index).unwrap();
        assert_eq!(
            output.new_chunk_data.chunk_hash,
            Some(anchor_chunk_header.chunk_hash().clone()),
        );

        // Receipts come from each source shard's own inclusion: the target's at
        // the anchor (newest first), the other shard's at the mid-range block.
        let expected_receipts = [
            boundary_chain
                .receipts_from(boundary_chain.target_shard_id, anchor_block.header().height()),
            boundary_chain.receipts_from(
                boundary_chain.other_shard_id,
                boundary_chain.mid_range_block.header().height(),
            ),
        ]
        .concat();
        assert_eq!(output.new_chunk_data.receipts, expected_receipts);

        // One implicit replay: the boundary block itself, as an old chunk.
        assert_eq!(output.boundary_replays.len(), 1);
        let replay = &output.boundary_replays[0];
        assert_eq!(replay.block_context.height, boundary_chain.boundary_block.header().height());
        assert_eq!(&replay.block_context.prev_block_hash, anchor_block.hash());
        assert_eq!(replay.shard_uid.shard_id(), boundary_chain.target_shard_id);
    }

    /// A proof carrying receipts other than the ones its chunk header commits to
    /// must be rejected.
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_boundary_witness_source_proofs_verify_against_source_header_roots() {
        let boundary_chain = setup_boundary_chain(false);
        let mut source_receipt_proofs = boundary_chain.source_receipt_proofs();
        let target_id =
            boundary_chain.chunk_hash(&boundary_chain.anchor_block, boundary_chain.target_shard_id);
        let other_id = boundary_chain
            .chunk_hash(&boundary_chain.mid_range_block, boundary_chain.other_shard_id);
        let target = source_receipt_proofs[&target_id].clone();
        let mut other = source_receipt_proofs[&other_id].clone();
        // Swap the receipt payloads while keeping the shard routing.
        other.0 = target.0;
        source_receipt_proofs.insert(other_id, other);
        let witness = boundary_chain.boundary_witness_with_proofs(source_receipt_proofs);

        assert_rejected(boundary_chain.run_pre_validation(&witness), "tampered source proof");
    }

    /// The proof set must cover exactly the chunk headers included within the
    /// consumed range: dropping the mid-range contribution is rejected.
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_boundary_witness_source_proofs_come_from_source_shard_inclusions() {
        let boundary_chain = setup_boundary_chain(false);
        let mut source_receipt_proofs = boundary_chain.source_receipt_proofs();
        source_receipt_proofs.remove(
            &boundary_chain
                .chunk_hash(&boundary_chain.mid_range_block, boundary_chain.other_shard_id),
        );
        let witness = boundary_chain.boundary_witness_with_proofs(source_receipt_proofs);

        assert_rejected(boundary_chain.run_pre_validation(&witness), "dropped source proof");
    }

    /// A source shard included at both the mid-range block and the anchor
    /// contributes one proof per inclusion, each verified against its own
    /// header.
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_boundary_witness_source_shard_included_twice_in_range() {
        let boundary_chain = setup_boundary_chain(true);
        let anchor_height = boundary_chain.anchor_block.header().height();
        let mid_range_height = boundary_chain.mid_range_block.header().height();
        let other_at_anchor =
            boundary_chain.chunk_hash(&boundary_chain.anchor_block, boundary_chain.other_shard_id);
        let other_at_mid_range = boundary_chain
            .chunk_hash(&boundary_chain.mid_range_block, boundary_chain.other_shard_id);

        let source_receipt_proofs = boundary_chain.source_receipt_proofs();
        assert_eq!(source_receipt_proofs.len(), 3);
        let witness = boundary_chain.boundary_witness_with_proofs(source_receipt_proofs.clone());
        let output = boundary_chain.run_pre_validation(&witness).unwrap();
        let expected_receipts = [
            boundary_chain.receipts_from(boundary_chain.target_shard_id, anchor_height),
            boundary_chain.receipts_from(boundary_chain.other_shard_id, anchor_height),
            boundary_chain.receipts_from(boundary_chain.other_shard_id, mid_range_height),
        ]
        .concat();
        assert_eq!(sorted(output.new_chunk_data.receipts), sorted(expected_receipts));

        let mut dropped = source_receipt_proofs.clone();
        dropped.remove(&other_at_mid_range);
        let witness = boundary_chain.boundary_witness_with_proofs(dropped);
        assert_rejected(
            boundary_chain.run_pre_validation(&witness),
            "dropped proof of one of two inclusions",
        );

        let mut swapped = source_receipt_proofs;
        let mut at_mid_range = swapped[&other_at_mid_range].clone();
        at_mid_range.0 = swapped[&other_at_anchor].0.clone();
        swapped.insert(other_at_mid_range, at_mid_range);
        let witness = boundary_chain.boundary_witness_with_proofs(swapped);
        assert_rejected(
            boundary_chain.run_pre_validation(&witness),
            "proof carrying the same shard's other inclusion",
        );
    }
}

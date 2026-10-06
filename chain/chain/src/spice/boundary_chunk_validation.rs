//! Validation of a boundary witness: the state witness of the last pre-spice
//! block's chunk.

use crate::chain::{NewChunkData, ShardContext, StorageContext};
use crate::spice::boundary::is_last_pre_spice_block;
use crate::spice::boundary_synthesis::{
    PreSpiceChunkApplyBlocks, execution_result_from_pre_spice_child,
    get_incoming_receipt_blocks_for_shard, get_last_new_chunk_block_and_old_chunk_blocks,
};
use crate::spice::chunk_validation::SpicePreValidationOutput;
use crate::stateless_validation::chunk_validation::validate_source_receipt_proofs;
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
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::merkle::merklize;
use near_primitives::shard_layout::ShardUId;
use near_primitives::spice::state_witness::SpiceBoundaryChunkStateWitness;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_store::PartialStorage;
use node_runtime::SignedValidPeriodTransactions;
use tracing::Span;

/// An old-chunk replay of a boundary witness, derived from an on-chain block.
pub(super) struct BoundaryReplay {
    pub(super) block_hash: CryptoHash,
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
    epoch_manager: &dyn EpochManagerAdapter,
    store: &ChainStore,
) -> Result<SpicePreValidationOutput, Error> {
    if !is_last_pre_spice_block(epoch_manager, block.hash())? {
        return Err(Error::InvalidChunkStateWitness(
            "boundary witness for a block other than the last pre-spice block".to_string(),
        ));
    }
    let epoch_id = epoch_manager.get_epoch_id(block.header().hash())?;
    let shard_id = state_witness.chunk_id.shard_id;
    let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
    if !shard_layout.shard_ids().contains(&shard_id) {
        return Err(Error::InvalidChunkStateWitness(format!(
            "shard layout for block's ({:?}) epoch ({:?}) doesn't contain witness shard {:?}",
            block.hash(),
            epoch_id,
            shard_id
        )));
    }
    let chunks = block.chunks();
    let shard_index = shard_layout.get_shard_index(shard_id)?;
    let chunk_header = chunks.get(shard_index).ok_or(Error::InvalidShardId(shard_id))?;

    let PreSpiceChunkApplyBlocks { last_new_chunk_block, old_chunk_blocks } =
        get_last_new_chunk_block_and_old_chunk_blocks(store, epoch_manager, block, shard_id)?;
    let anchor_prev_block = store.get_block(last_new_chunk_block.header().prev_hash())?;
    let anchor_epoch_id = epoch_manager.get_epoch_id(last_new_chunk_block.header().hash())?;
    let anchor_shard_layout = epoch_manager.get_shard_layout(&anchor_epoch_id)?;
    let protocol_version = epoch_manager.get_epoch_info(&anchor_epoch_id)?.protocol_version();
    chunk_header.validate_version(protocol_version)?;

    let mut boundary_replays = Vec::with_capacity(old_chunk_blocks.len());
    for old_chunk_block in &old_chunk_blocks {
        let replay_prev_header = store.get_block_header(old_chunk_block.header().prev_hash())?;
        let block_context =
            Chain::get_apply_chunk_block_context(old_chunk_block, &replay_prev_header, false);
        let replay_epoch_id = epoch_manager.get_epoch_id(old_chunk_block.header().hash())?;
        let shard_uid = shard_id_to_uid(epoch_manager, shard_id, &replay_epoch_id)?;
        boundary_replays.push(BoundaryReplay {
            block_hash: *old_chunk_block.hash(),
            block_context,
            shard_uid,
        });
    }

    let source_blocks = get_incoming_receipt_blocks_for_shard(
        store,
        epoch_manager,
        &last_new_chunk_block,
        shard_id,
    )?;
    let receipts_to_apply = validate_source_receipt_proofs(
        epoch_manager,
        &state_witness.source_receipt_proofs,
        &source_blocks,
        anchor_shard_layout,
        shard_id,
    )?;
    let applied_receipts_hash = hash(&borsh::to_vec(receipts_to_apply.as_slice()).unwrap());
    if applied_receipts_hash != state_witness.applied_receipts_hash {
        return Err(Error::InvalidChunkStateWitness(format!(
            "receipts hash {:?} does not match expected receipts hash {:?}",
            applied_receipts_hash, state_witness.applied_receipts_hash
        )));
    }

    let (tx_root_from_state_witness, _) = merklize(&state_witness.transactions);
    if chunk_header.tx_root() != &tx_root_from_state_witness {
        return Err(Error::InvalidChunkStateWitness(format!(
            "transaction root {:?} does not match expected transaction root {:?}",
            tx_root_from_state_witness,
            chunk_header.tx_root()
        )));
    }
    let transaction_validity_check_results =
        store.compute_transaction_validity(anchor_prev_block.header(), &state_witness.transactions);

    let prev_chunk_extra =
        execution_result_from_pre_spice_child(epoch_manager, &last_new_chunk_block, shard_id)?
            .ok_or_else(|| {
                Error::Other(format!(
                    "anchor block {} includes no chunk of shard {}",
                    last_new_chunk_block.hash(),
                    shard_id
                ))
            })?
            .chunk_extra;
    let new_chunk_data = NewChunkData {
        gas_limit: prev_chunk_extra.gas_limit(),
        prev_state_root: *prev_chunk_extra.state_root(),
        prev_validator_proposals: prev_chunk_extra.validator_proposals().collect(),
        chunk_hash: Some(chunk_header.chunk_hash().clone()),
        transactions: SignedValidPeriodTransactions::new(
            state_witness.transactions.clone(),
            transaction_validity_check_results,
        ),
        receipts: receipts_to_apply,
        block: Chain::get_apply_chunk_block_context(
            &last_new_chunk_block,
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
            "implicit transitions count mismatch: expected {}, found {}",
            boundary_replays.len(),
            implicit_transitions.len(),
        )));
    }
    for (BoundaryReplay { block_hash, block_context, shard_uid }, transition) in
        boundary_replays.into_iter().zip(implicit_transitions)
    {
        if transition.block_hash != block_hash {
            return Err(Error::InvalidChunkStateWitness(format!(
                "implicit transition block hash {:?} does not match expected block hash {:?}",
                transition.block_hash, block_hash,
            )));
        }
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
                "post state root {:?} for implicit transition at block {:?} does not match expected state root {:?}",
                chunk_extra.state_root(),
                transition.block_hash,
                transition.post_state_root,
            )));
        }
    }
    Ok(chunk_extra)
}

#[cfg(test)]
/// Era-semantics tests for a witness of the last pre-spice block whose chunk
/// is missing.
mod tests {
    use super::*;
    use crate::spice::chunk_validation::tests::assert_invalid_witness;
    use crate::spice::chunk_validation::{
        spice_pre_validate_chunk_state_witness, spice_validate_chunk_state_witness,
    };
    use crate::spice::tests::pre_spice::{
        build_pre_spice_block_with_chunks, grow_to_last_pre_spice_block, save_and_record_block,
        setup_pre_spice_chain,
    };
    use near_o11y::testonly::init_test_logger;
    use near_primitives::bandwidth_scheduler::BandwidthRequests;
    use near_primitives::congestion_info::CongestionInfo;
    use near_primitives::gas::Gas;
    use near_primitives::receipt::Receipt;
    use near_primitives::sharding::{
        ChunkHash, ReceiptProof, ShardChunkHeader, ShardChunkHeaderV3,
    };
    use near_primitives::spice::state_witness::SpiceChunkStateWitness;
    use near_primitives::state::PartialState;
    use near_primitives::stateless_validation::state_witness::ChunkStateTransition;
    use near_primitives::test_utils::{create_test_signer, pre_spice_protocol_version};
    use near_primitives::types::{
        AccountId, Balance, BlockExecutionResults, BlockHeight, ChunkExecutionResult, ShardId,
        SpiceChunkId,
    };
    use std::collections::{BTreeSet, HashMap};
    use std::sync::Arc;

    struct BoundaryChain {
        chain: Chain,
        /// Block including the other shard's chunk, mid-range for the target
        /// shard's witness: the target's chunk is missing in it.
        mid_range_block: Arc<Block>,
        /// Block of the target shard's last included chunk.
        last_new_chunk_block: Arc<Block>,
        /// The pre-spice block the witness is keyed to; every chunk missing.
        boundary_block: Arc<Block>,
        target_shard_id: ShardId,
        other_shard_id: ShardId,
    }

    fn test_receiver() -> AccountId {
        "test1".parse().unwrap()
    }

    /// A staggered gap straddling the anchor, ending at the last pre-spice block:
    /// full block -> mid-range (other new, target missing) -> anchor (target
    /// new, other missing) -> boundary block (everything missing).
    fn setup_boundary_chain() -> BoundaryChain {
        init_test_logger();
        let mut chain = setup_pre_spice_chain(2);
        // Where the boundary lands depends only on its height, so the gap forks off
        // the full chain three blocks before its last pre-spice block.
        let (last_pre_spice_block, _) = grow_to_last_pre_spice_block(&mut chain);
        let mut full_block = last_pre_spice_block;
        for _ in 0..3 {
            full_block = chain.get_block(full_block.header().prev_hash()).unwrap();
        }
        let shard_layout =
            chain.epoch_manager.get_shard_layout(full_block.header().epoch_id()).unwrap();
        let target_shard_id = shard_layout.account_id_to_shard_id(&test_receiver());
        let other_shard_id =
            shard_layout.shard_ids().find(|shard_id| shard_id != &target_shard_id).unwrap();

        let mid_range_block = add_block(&mut chain, &full_block, other_shard_id);
        let last_new_chunk_block = add_block(&mut chain, &mid_range_block, target_shard_id);
        let boundary_block = add_block(&mut chain, &last_new_chunk_block, None);
        assert!(
            is_last_pre_spice_block(chain.epoch_manager.as_ref(), boundary_block.hash()).unwrap(),
            "the staggered gap must end at the last pre-spice block"
        );
        BoundaryChain {
            chain,
            mid_range_block,
            last_new_chunk_block,
            boundary_block,
            target_shard_id,
            other_shard_id,
        }
    }

    /// The receipts a chunk of `from_shard_id` included at `height`, in the block
    /// following `prev_block_hash`, sends to the test receiver, with the receipts
    /// root that chunk's successor commits to and their proof.
    fn outgoing_receipts(
        chain: &Chain,
        from_shard_id: ShardId,
        prev_block_hash: &CryptoHash,
        height: BlockHeight,
    ) -> (Vec<Receipt>, CryptoHash, ReceiptProof) {
        let shard_layout =
            chain.epoch_manager.get_shard_layout_from_prev_block(prev_block_hash).unwrap();
        let from_shard_index: u64 = from_shard_id.into();
        let deposit = 100 * u128::from(height) + u128::from(from_shard_index);
        let receipts =
            vec![Receipt::new_balance_refund(&test_receiver(), Balance::from_yoctonear(deposit))];
        let (root, proofs) = Chain::create_receipts_proofs_from_outgoing_receipts(
            &shard_layout,
            from_shard_id,
            receipts.clone(),
        )
        .unwrap();
        let target_shard_id = shard_layout.account_id_to_shard_id(&test_receiver());
        let proof =
            proofs.into_iter().find(|proof| proof.1.to_shard_id == target_shard_id).unwrap();
        (receipts, root, proof)
    }

    /// Fabricates, saves and records the next block. `new_chunk_shard` gets a new
    /// chunk header committing to [`outgoing_receipts`] at this height; every other
    /// shard carries the previous block's header.
    fn add_block(
        chain: &mut Chain,
        prev_block: &Block,
        new_chunk_shard: impl Into<Option<ShardId>>,
    ) -> Arc<Block> {
        let new_chunk_shard = new_chunk_shard.into();
        let signer = create_test_signer("test1");
        let height = prev_block.header().height() + 1;
        let chunks = prev_block
            .chunks()
            .iter_raw()
            .map(|carried| {
                let shard_id = carried.shard_id();
                if new_chunk_shard != Some(shard_id) {
                    return carried.clone();
                }
                let (_, receipts_root, _) =
                    outgoing_receipts(chain, shard_id, prev_block.hash(), height);
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
                    receipts_root,
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
        let block = build_pre_spice_block_with_chunks(
            chain,
            prev_block,
            chunks,
            pre_spice_protocol_version(),
        );
        save_and_record_block(chain, &block, pre_spice_protocol_version());
        block
    }

    impl BoundaryChain {
        /// Hash of the chunk of `shard_id` carried by `block`.
        fn chunk_hash(&self, block: &Block, shard_id: ShardId) -> ChunkHash {
            let shard_layout =
                self.chain.epoch_manager.get_shard_layout(block.header().epoch_id()).unwrap();
            let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
            block.chunks().get(shard_index).unwrap().chunk_hash().clone()
        }

        /// The source chunks of the anchor's receipts, newest first: the target's
        /// at the anchor, the other shard's at the mid-range block.
        fn sources(&self) -> [(&Arc<Block>, ShardId); 2] {
            [
                (&self.last_new_chunk_block, self.target_shard_id),
                (&self.mid_range_block, self.other_shard_id),
            ]
        }

        fn expected_receipts(&self) -> Vec<Receipt> {
            self.sources()
                .into_iter()
                .flat_map(|(block, shard_id)| {
                    outgoing_receipts(
                        &self.chain,
                        shard_id,
                        block.header().prev_hash(),
                        block.header().height(),
                    )
                    .0
                })
                .collect()
        }

        /// A boundary witness with valid source receipts and no implicit
        /// transitions.
        fn witness(&self) -> SpiceBoundaryChunkStateWitness {
            let source_receipt_proofs = self
                .sources()
                .into_iter()
                .map(|(block, shard_id)| {
                    let (_, _, proof) = outgoing_receipts(
                        &self.chain,
                        shard_id,
                        block.header().prev_hash(),
                        block.header().height(),
                    );
                    (self.chunk_hash(block, shard_id), proof)
                })
                .collect();
            SpiceBoundaryChunkStateWitness {
                chunk_id: SpiceChunkId {
                    block_hash: *self.boundary_block.hash(),
                    shard_id: self.target_shard_id,
                },
                pre_state: PartialState::TrieValues(vec![]),
                source_receipt_proofs,
                applied_receipts_hash: hash(&borsh::to_vec(&self.expected_receipts()).unwrap()),
                transactions: vec![],
                contract_accesses: BTreeSet::new(),
                implicit_transitions: vec![],
            }
        }

        /// [`Self::witness`] claiming one old-chunk transition at `block_hash`
        /// with an empty base state and `post_state_root`.
        fn witness_with_implicit_transition(
            &self,
            block_hash: CryptoHash,
            post_state_root: CryptoHash,
        ) -> SpiceBoundaryChunkStateWitness {
            let mut witness = self.witness();
            witness.implicit_transitions = vec![ChunkStateTransition {
                block_hash,
                base_state: PartialState::TrieValues(vec![]),
                post_state_root,
            }];
            witness
        }

        /// The anchor's previous chunk extra. The fabricated headers commit to
        /// the empty trie, so a replay on top of it needs no base state nodes.
        fn empty_state_chunk_extra(&self) -> ChunkExtra {
            let chunk_extra = execution_result_from_pre_spice_child(
                self.chain.epoch_manager.as_ref(),
                &self.last_new_chunk_block,
                self.target_shard_id,
            )
            .unwrap()
            .unwrap()
            .chunk_extra;
            assert_eq!(chunk_extra.state_root(), &CryptoHash::default());
            chunk_extra
        }

        fn run_pre_validation(
            &self,
            witness: &SpiceBoundaryChunkStateWitness,
        ) -> Result<SpicePreValidationOutput, Error> {
            let block = self.chain.get_block(&witness.chunk_id.block_hash).unwrap();
            let prev_block = self.chain.get_block(block.header().prev_hash()).unwrap();
            spice_pre_validate_chunk_state_witness(
                &SpiceChunkStateWitness::Boundary(witness.clone()),
                &block,
                &prev_block,
                // A boundary witness derives its previous chunk's inputs itself.
                &BlockExecutionResults(HashMap::new()),
                self.chain.epoch_manager.as_ref(),
                self.chain.chain_store(),
                vec![],
            )
        }

        fn run_validation(
            &self,
            witness: SpiceBoundaryChunkStateWitness,
        ) -> Result<ChunkExecutionResult, Error> {
            let output = self.run_pre_validation(&witness)?;
            spice_validate_chunk_state_witness(
                SpiceChunkStateWitness::Boundary(witness),
                output,
                self.chain.epoch_manager.as_ref(),
                self.chain.runtime_adapter.as_ref(),
            )
        }
    }

    /// The main transition's context must be the anchor's pre-spice context: the
    /// anchor's height and prev hash, the anchor's prev block's gas price, and the
    /// anchor's real missed-chunk counts (the other shard's chunk is missing in
    /// the anchor).
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_boundary_witness_uses_pre_spice_anchor_context() {
        let boundary_chain = setup_boundary_chain();
        let output = boundary_chain.run_pre_validation(&boundary_chain.witness()).unwrap();

        let last_new_chunk_block = &boundary_chain.last_new_chunk_block;
        let block_context = &output.new_chunk_data.block;
        assert_eq!(block_context.height, last_new_chunk_block.header().height());
        assert_eq!(&block_context.prev_block_hash, last_new_chunk_block.header().prev_hash());
        let anchor_prev_header = boundary_chain
            .chain
            .get_block_header(last_new_chunk_block.header().prev_hash())
            .unwrap();
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
        let shard_layout = boundary_chain
            .chain
            .epoch_manager
            .get_shard_layout(last_new_chunk_block.header().epoch_id())
            .unwrap();
        let target_shard_index =
            shard_layout.get_shard_index(boundary_chain.target_shard_id).unwrap();
        let anchor_chunks = last_new_chunk_block.chunks();
        let anchor_chunk_header = anchor_chunks.get(target_shard_index).unwrap();
        assert_eq!(
            output.new_chunk_data.chunk_hash,
            Some(anchor_chunk_header.chunk_hash().clone()),
        );
        // It builds on the previous chunk's result the anchor's header commits to.
        assert_eq!(output.new_chunk_data.prev_state_root, anchor_chunk_header.prev_state_root());
        assert_eq!(output.new_chunk_data.gas_limit, anchor_chunk_header.gas_limit());
        assert_eq!(
            output.new_chunk_data.prev_validator_proposals,
            anchor_chunk_header.prev_validator_proposals().collect::<Vec<_>>(),
        );

        // Receipts come from each source shard's own inclusion, newest first.
        assert_eq!(output.new_chunk_data.receipts, boundary_chain.expected_receipts());

        // One implicit replay: the boundary block itself, as an old chunk.
        assert_eq!(output.boundary_replays.len(), 1);
        let replay = &output.boundary_replays[0];
        assert_eq!(replay.block_context.height, boundary_chain.boundary_block.header().height());
        assert_eq!(&replay.block_context.prev_block_hash, last_new_chunk_block.hash());
        assert_eq!(replay.shard_uid.shard_id(), boundary_chain.target_shard_id);
    }

    /// The last pre-spice block is attested by a boundary witness only: a regular
    /// witness keyed to it must be rejected.
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_regular_witness_rejected_for_last_pre_spice_block() {
        let boundary_chain = setup_boundary_chain();
        let block = &boundary_chain.boundary_block;
        let prev_block = boundary_chain.chain.get_block(block.header().prev_hash()).unwrap();
        let witness = SpiceChunkStateWitness::new(
            SpiceChunkId { block_hash: *block.hash(), shard_id: boundary_chain.target_shard_id },
            PartialState::TrieValues(vec![]),
            HashMap::new(),
            hash(&borsh::to_vec(&boundary_chain.expected_receipts()).unwrap()),
            vec![],
            BTreeSet::new(),
            None,
        );

        let result = spice_pre_validate_chunk_state_witness(
            &witness,
            block,
            &prev_block,
            &BlockExecutionResults(HashMap::new()),
            boundary_chain.chain.epoch_manager.as_ref(),
            boundary_chain.chain.chain_store(),
            vec![],
        );

        assert_invalid_witness(result, "regular spice witness for a pre-spice block");
    }

    /// A boundary witness attests the last pre-spice block only: one keyed to an
    /// earlier pre-spice block, whose chunk never certifies, must be rejected.
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_boundary_witness_rejected_for_earlier_pre_spice_block() {
        let boundary_chain = setup_boundary_chain();
        let mut witness = boundary_chain.witness();
        witness.chunk_id.block_hash = *boundary_chain.mid_range_block.hash();

        // The guard comes first, so the rejection reason discriminates it from the
        // proof checks the witness would also fail against the earlier block.
        assert_invalid_witness(boundary_chain.run_pre_validation(&witness), "last pre-spice block");
    }

    /// The proof set must cover every chunk included within the consumed range:
    /// dropping the mid-range contribution is rejected.
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_boundary_witness_rejected_without_mid_range_source_proof() {
        let boundary_chain = setup_boundary_chain();
        let mut witness = boundary_chain.witness();
        witness.source_receipt_proofs.remove(
            &boundary_chain
                .chunk_hash(&boundary_chain.mid_range_block, boundary_chain.other_shard_id),
        );

        assert_invalid_witness(boundary_chain.run_pre_validation(&witness), "source receipt proof");
    }

    /// The boundary block's chunk is missing, so the witness must carry its
    /// old-chunk transition: one without any is rejected after the main apply.
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_boundary_witness_rejected_without_implicit_transitions() {
        let boundary_chain = setup_boundary_chain();

        assert_invalid_witness(
            boundary_chain.run_validation(boundary_chain.witness()),
            "implicit transitions count mismatch",
        );
    }

    /// An implicit transition must be keyed to the block it replays.
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_boundary_witness_rejected_for_implicit_transition_at_wrong_block() {
        let boundary_chain = setup_boundary_chain();
        let witness = boundary_chain.witness_with_implicit_transition(
            *boundary_chain.last_new_chunk_block.hash(),
            CryptoHash::default(),
        );

        assert_invalid_witness(
            boundary_chain.run_validation(witness),
            "implicit transition block hash",
        );
    }

    /// A claimed post state root other than the replay's must be rejected.
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_boundary_witness_rejected_for_wrong_implicit_post_state_root() {
        let boundary_chain = setup_boundary_chain();
        let witness = boundary_chain.witness_with_implicit_transition(
            *boundary_chain.boundary_block.hash(),
            hash(b"wrong post state root"),
        );
        let output = boundary_chain.run_pre_validation(&witness).unwrap();

        assert_invalid_witness(
            replay_boundary_implicit_transitions(
                &witness,
                output.boundary_replays,
                boundary_chain.empty_state_chunk_extra(),
                boundary_chain.chain.runtime_adapter.as_ref(),
            ),
            "post state root",
        );
    }

    /// A replay moves only the state root forward: every other field is carried
    /// over from the main transition's chunk extra.
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_boundary_witness_implicit_transition_carries_main_chunk_extra() {
        let boundary_chain = setup_boundary_chain();
        let boundary_block_hash = *boundary_chain.boundary_block.hash();
        let main_chunk_extra = boundary_chain.empty_state_chunk_extra();
        let runtime_adapter = boundary_chain.chain.runtime_adapter.as_ref();

        // The runtime's own old-chunk apply is the reference post state root.
        let witness = boundary_chain
            .witness_with_implicit_transition(boundary_block_hash, CryptoHash::default());
        let output = boundary_chain.run_pre_validation(&witness).unwrap();
        let Ok(BoundaryReplay { block_context, shard_uid, .. }) =
            output.boundary_replays.into_iter().exactly_one()
        else {
            panic!("the boundary block is the only old-chunk replay");
        };
        let old_chunk_data = OldChunkData {
            prev_chunk_extra: main_chunk_extra.clone(),
            block: block_context,
            storage_context: StorageContext {
                storage_data_source: StorageDataSource::Recorded(PartialStorage {
                    nodes: PartialState::TrieValues(vec![]),
                }),
                state_patch: Default::default(),
            },
        };
        let expected_post_state_root = apply_old_chunk(
            ApplyChunkReason::ValidateChunkStateWitness,
            &Span::current(),
            old_chunk_data,
            ShardContext { shard_uid, should_apply_chunk: false },
            runtime_adapter,
            MaybePinnedMemtrieRoot::no_memtries(),
        )
        .unwrap()
        .apply_result
        .new_root;

        let witness = boundary_chain
            .witness_with_implicit_transition(boundary_block_hash, expected_post_state_root);
        let output = boundary_chain.run_pre_validation(&witness).unwrap();
        let chunk_extra = replay_boundary_implicit_transitions(
            &witness,
            output.boundary_replays,
            main_chunk_extra.clone(),
            runtime_adapter,
        )
        .unwrap();
        assert_eq!(chunk_extra, main_chunk_extra.next_for_old_chunk(expected_post_state_root));
    }
}

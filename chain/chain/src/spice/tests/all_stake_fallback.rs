use crate::spice::all_stake_fallback::{
    SPICE_FALLBACK_CERTIFICATION_DELAY, all_stake_fallback_assignment, fallback_eligible,
    is_fallback_only_chunk, is_fallback_only_height_for_shard_index,
};
use crate::spice::tests::core::{
    block_certification_core_statements, build_block, endorsement_into_core_statement,
    process_block, setup, setup_with_validators, test_chunk_endorsement,
    test_execution_result_for_chunk,
};
use crate::{Block, Chain};
use assert_matches::assert_matches;
use near_primitives::block_body::SpiceCoreStatement;
use near_primitives::errors::InvalidSpiceCoreStatementsError;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::types::{
    AccountId, BlockHeight, BlockHeightDelta, EpochHeight, ShardId, SpiceChunkId,
    SpiceUncertifiedChunkInfo,
};
use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

fn first_shard_chunk_id(block: &Block) -> SpiceChunkId {
    let shard_id = block.chunks().iter_raw().next().unwrap().shard_id();
    SpiceChunkId { block_hash: *block.hash(), shard_id }
}

// Builds a block on `prev`, processes it, and returns it.
fn append_block(
    chain: &mut Chain,
    prev: &Block,
    statements: Vec<SpiceCoreStatement>,
) -> Arc<Block> {
    let block = build_block(chain, prev, statements);
    process_block(chain, block.clone());
    block
}

// Appends empty blocks onto `from` until the tip reaches `target_height`, returning that tip.
fn advance_to_height(
    chain: &mut Chain,
    from: &Arc<Block>,
    target_height: BlockHeight,
) -> Arc<Block> {
    let mut tip = from.clone();
    while tip.header().height() < target_height {
        tip = append_block(chain, &tip, vec![]);
    }
    tip
}

// Endorsement core statements from `accounts` for `chunk` in `block`.
fn endorsement_statements(
    accounts: &[AccountId],
    block: &Block,
    chunk: &ShardChunkHeader,
) -> Vec<SpiceCoreStatement> {
    accounts
        .iter()
        .map(|account| {
            endorsement_into_core_statement(test_chunk_endorsement(account.as_str(), block, chunk))
        })
        .collect()
}

// The execution-result core statement for `chunk` in `block`.
fn execution_result_statement(block: &Block, chunk: &ShardChunkHeader) -> SpiceCoreStatement {
    SpiceCoreStatement::ChunkExecutionResult {
        chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id: chunk.shard_id() },
        execution_result: test_execution_result_for_chunk(chunk),
    }
}

// `accounts`' endorsements for `chunk` plus its execution result (the shape of a certifying block).
fn endorsements_and_execution_result(
    accounts: &[AccountId],
    block: &Block,
    chunk: &ShardChunkHeader,
) -> Vec<SpiceCoreStatement> {
    let mut statements = endorsement_statements(accounts, block, chunk);
    statements.push(execution_result_statement(block, chunk));
    statements
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_fallback_eligible_false_when_parent_not_certified() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block1 = append_block(&mut chain, &genesis, vec![]);
    let block2 = append_block(&mut chain, &block1, vec![]); // prev(block2) = block1 is uncertified

    // Even arbitrarily far in the future, block2 is not eligible while its parent block1 is
    // uncertified.
    let eligible = core_reader
        .fallback_eligible_in_carrying_block(100, block2.hash(), &first_shard_chunk_id(&block2))
        .unwrap();
    assert!(!eligible);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_fallback_eligible_respects_delay_after_parent_certified() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block1 = append_block(&mut chain, &genesis, vec![]);
    let block2 = append_block(&mut chain, &block1, vec![]);
    // block3 includes block1's endorsements + results, certifying it. cert_height(block1) =
    // block3.height().
    let block3 = append_block(&mut chain, &block2, block_certification_core_statements(&block1));

    let chunk = first_shard_chunk_id(&block2);
    let delay = SPICE_FALLBACK_CERTIFICATION_DELAY;
    let cert_height = block3.header().height();
    let eligible_at = |height| {
        core_reader.fallback_eligible_in_carrying_block(height, block3.hash(), &chunk).unwrap()
    };

    assert!(!eligible_at(cert_height + delay - 1));
    assert!(eligible_at(cert_height + delay));
    // Monotone: stays eligible far past the window.
    assert!(eligible_at(cert_height + delay + 50));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_fallback_eligible_false_when_height_below_delay() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block1 = append_block(&mut chain, &genesis, vec![]);
    let block2 = append_block(&mut chain, &block1, vec![]);

    // carrying_height < delay is false (covers genesis / spice activation).
    let below_delay = SPICE_FALLBACK_CERTIFICATION_DELAY - 1;
    assert!(
        !core_reader
            .fallback_eligible_in_carrying_block(
                below_delay,
                block2.hash(),
                &first_shard_chunk_id(&block2)
            )
            .unwrap()
    );
}

// More validators than the default chunk-validator mandate count per shard (68), so each chunk's
// designated assignment is a strict subset and there are non-designated validators to exercise
// the all-stake fallback path.
fn many_validators() -> Vec<String> {
    (0..100).map(|i| format!("test{i}")).collect()
}

// Certifies `block` using only each chunk's designated validators.
fn certify_block_designated(chain: &Chain, block: &Block) -> Vec<SpiceCoreStatement> {
    let epoch_id = block.header().epoch_id();
    let mut statements = Vec::new();
    for chunk in block.chunks().iter_raw() {
        let assignments = chain
            .epoch_manager
            .get_chunk_validator_assignments(epoch_id, chunk.shard_id(), block.header().height())
            .unwrap();
        statements.extend(endorsements_and_execution_result(
            &assignments.ordered_chunk_validators(),
            block,
            chunk,
        ));
    }
    statements
}

// Finds a (chunk header, validator) where the validator is NOT in the chunk's designated
// assignment, so its endorsement is only admissible via the all-stake fallback.
fn find_non_designated(
    chain: &Chain,
    block: &Block,
    validators: &[String],
) -> (ShardChunkHeader, AccountId) {
    let epoch_id = block.header().epoch_id();
    for chunk in block.chunks().iter_raw() {
        let assignments = chain
            .epoch_manager
            .get_chunk_validator_assignments(epoch_id, chunk.shard_id(), block.header().height())
            .unwrap();
        for validator in validators {
            let account: AccountId = validator.parse().unwrap();
            if !assignments.contains(&account) {
                return (chunk.clone(), account);
            }
        }
    }
    panic!("no non-designated validator found; increase validator count");
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_admits_non_designated_endorsement_only_when_eligible() {
    let validators = many_validators();
    let (mut chain, core_reader) = setup_with_validators(&validators);
    let genesis = chain.genesis_block();
    let block1 = append_block(&mut chain, &genesis, vec![]);
    let certify_block1 = certify_block_designated(&chain, &block1);
    let block2 = append_block(&mut chain, &block1, vec![]); // target chunk's block
    let block3 = append_block(&mut chain, &block2, certify_block1); // certifies block1

    // block3 certifies block1, so cert_height(block1) = block3.height(). block2's chunks become
    // fallback-eligible once the block including their endorsement is at height
    // cert_height + SPICE_FALLBACK_CERTIFICATION_DELAY or above. Advance empty blocks so `tip` sits
    // at cert_height + SPICE_FALLBACK_CERTIFICATION_DELAY - 1 and `prev` one block below it: then a
    // block built on `tip` lands exactly at the window (eligible) and one built on `prev` lands
    // just below it (ineligible).
    let delay = SPICE_FALLBACK_CERTIFICATION_DELAY;
    let cert_height = block3.header().height();
    let prev = advance_to_height(&mut chain, &block3, cert_height + delay - 2);
    let tip = append_block(&mut chain, &prev, vec![]);

    let (chunk_header, non_designated) = find_non_designated(&chain, &block2, &validators);
    let endorsement = test_chunk_endorsement(non_designated.as_str(), &block2, &chunk_header);
    let core_endorsement = endorsement_into_core_statement(endorsement);

    // Built on `tip`, this block lands at the window: the non-designated endorsement is admitted.
    let eligible_block = build_block(&chain, &tip, vec![core_endorsement.clone()]);
    assert_eq!(eligible_block.header().height(), cert_height + delay);
    core_reader.validate_core_statements_in_block(&eligible_block).unwrap();

    // Built on `prev`, this block lands just below the window: the same endorsement is rejected as
    // irrelevant.
    let ineligible_block = build_block(&chain, &prev, vec![core_endorsement]);
    assert_eq!(ineligible_block.header().height(), cert_height + delay - 1);
    assert_matches!(
        core_reader.validate_core_statements_in_block(&ineligible_block),
        Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
            reason: "endorsement is irrelevant",
            ..
        })
    );
}

// Splits `validators` into (designated, non_designated) for `chunk` as of `block`.
pub(super) fn split_designated(
    chain: &Chain,
    block: &Block,
    chunk: &ShardChunkHeader,
    validators: &[String],
) -> (Vec<AccountId>, Vec<AccountId>) {
    let assignments = chain
        .epoch_manager
        .get_chunk_validator_assignments(
            block.header().epoch_id(),
            chunk.shard_id(),
            block.header().height(),
        )
        .unwrap();
    let mut designated = Vec::new();
    let mut non_designated = Vec::new();
    for validator in validators {
        let account: AccountId = validator.parse().unwrap();
        if assignments.contains(&account) {
            designated.push(account);
        } else {
            non_designated.push(account);
        }
    }
    (designated, non_designated)
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_all_stake_certification_when_designated_insufficient() {
    let validators = many_validators();
    let (mut chain, core_reader) = setup_with_validators(&validators);
    let genesis = chain.genesis_block();
    let block1 = append_block(&mut chain, &genesis, vec![]);
    let certify_block1 = certify_block_designated(&chain, &block1);
    let block2 = append_block(&mut chain, &block1, vec![]);
    let block3 = append_block(&mut chain, &block2, certify_block1);

    let delay = SPICE_FALLBACK_CERTIFICATION_DELAY;
    let cert_height = block3.header().height();
    let tip = advance_to_height(&mut chain, &block3, cert_height + delay - 1);

    let (chunk_header, _) = find_non_designated(&chain, &block2, &validators);
    let (designated, non_designated) =
        split_designated(&chain, &block2, &chunk_header, &validators);
    // Include all non-designated plus at most 2/3 of the designated set by count (validators have
    // equal stake, so count ratios match stake ratios): below the strict >2/3 designated-stake
    // threshold, but enough total stake to certify on the all-stake path.
    let designated_subset = &designated[..2 * designated.len() / 3];

    // Block at the window (eligible): non-designated + partial designated certifies via all-stake.
    let mut all_stake_endorsers = non_designated;
    all_stake_endorsers.extend_from_slice(designated_subset);
    let certifying_block = build_block(
        &chain,
        &tip,
        endorsements_and_execution_result(&all_stake_endorsers, &block2, &chunk_header),
    );
    core_reader.validate_core_statements_in_block(&certifying_block).unwrap();

    // Same window block but only the partial designated set: neither path reaches the threshold, so
    // the included result is rejected.
    let insufficient_block = build_block(
        &chain,
        &tip,
        endorsements_and_execution_result(designated_subset, &block2, &chunk_header),
    );
    assert_matches!(
        core_reader.validate_core_statements_in_block(&insufficient_block),
        Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
            reason: "execution results included without enough corresponding endorsement",
            ..
        })
    );
}

// Like the designated path, once on-chain endorsements reach the all-stake threshold (2/3 of total
// epoch stake) the block must include the execution result; the certifying endorser set is rejected
// when the result is omitted.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_all_stake_certification_requires_execution_result() {
    let validators = many_validators();
    let (mut chain, core_reader) = setup_with_validators(&validators);
    let genesis = chain.genesis_block();
    let block1 = append_block(&mut chain, &genesis, vec![]);
    let certify_block1 = certify_block_designated(&chain, &block1);
    let block2 = append_block(&mut chain, &block1, vec![]);
    let block3 = append_block(&mut chain, &block2, certify_block1);

    let delay = SPICE_FALLBACK_CERTIFICATION_DELAY;
    let cert_height = block3.header().height();
    let tip = advance_to_height(&mut chain, &block3, cert_height + delay - 1);

    let (chunk_header, _) = find_non_designated(&chain, &block2, &validators);
    let (designated, non_designated) =
        split_designated(&chain, &block2, &chunk_header, &validators);
    let designated_subset = &designated[..2 * designated.len() / 3];

    // Non-designated + sub-quorum designated reach 2/3 of total stake but the block omits the
    // execution result.
    let mut all_stake_endorsers = non_designated;
    all_stake_endorsers.extend_from_slice(designated_subset);
    let endorsements = endorsement_statements(&all_stake_endorsers, &block2, &chunk_header);
    let block_without_result = build_block(&chain, &tip, endorsements);
    assert_matches!(
        core_reader.validate_core_statements_in_block(&block_without_result),
        Err(InvalidSpiceCoreStatementsError::NoExecutionResultForEndorsedChunk { .. })
    );
}

// A designated endorsement already carried in the ancestry cannot be re-included, even once the
// chunk is fallback-eligible (when the wider non-designated admissibility opens up).
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_rejects_reincluded_designated_endorsement_when_eligible() {
    let validators = many_validators();
    let (mut chain, core_reader) = setup_with_validators(&validators);
    let genesis = chain.genesis_block();
    let block1 = append_block(&mut chain, &genesis, vec![]);
    let certify_block1 = certify_block_designated(&chain, &block1);
    let block2 = append_block(&mut chain, &block1, vec![]);
    let block3 = append_block(&mut chain, &block2, certify_block1);

    let (chunk_header, _) = find_non_designated(&chain, &block2, &validators);
    let (designated, _) = split_designated(&chain, &block2, &chunk_header, &validators);
    let endorser = designated[0].clone();
    let endorsement = || {
        endorsement_into_core_statement(test_chunk_endorsement(
            endorser.as_str(),
            &block2,
            &chunk_header,
        ))
    };

    // The designated endorsement lands on chain, moving into the chunk's present endorsements.
    let block4 = append_block(&mut chain, &block3, vec![endorsement()]);

    let delay = SPICE_FALLBACK_CERTIFICATION_DELAY;
    let cert_height = block3.header().height();
    let tip = advance_to_height(&mut chain, &block4, cert_height + delay - 1);

    // Re-including the already-on-chain designated endorsement in an eligible block is rejected.
    let reinclude = build_block(&chain, &tip, vec![endorsement()]);
    assert!(reinclude.header().height() >= cert_height + delay);
    assert_matches!(
        core_reader.validate_core_statements_in_block(&reinclude),
        Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
            reason: "endorsement is irrelevant",
            ..
        })
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_fallback_eligible_when_one_block_certifies_several_blocks() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block1 = append_block(&mut chain, &genesis, vec![]);
    let block2 = append_block(&mut chain, &block1, vec![]);
    let block3 = append_block(&mut chain, &block2, vec![]);

    // block4 certifies block1 and block2 at once, so the oldest uncertified block jumps from
    // block1 straight to block3 without block2 ever having been the oldest.
    let mut statements = block_certification_core_statements(&block1);
    statements.extend(block_certification_core_statements(&block2));
    let block4 = append_block(&mut chain, &block3, statements);

    let uncertified = core_reader.get_uncertified_chunks(block4.hash()).unwrap();
    assert!(uncertified.iter().all(|info| info.chunk_id.block_hash != *block1.hash()));
    assert!(uncertified.iter().all(|info| info.chunk_id.block_hash != *block2.hash()));
    let block3_chunks: Vec<_> =
        uncertified.iter().filter(|info| info.chunk_id.block_hash == *block3.hash()).collect();
    assert!(!block3_chunks.is_empty());
    for chunk_info in block3_chunks {
        assert_eq!(chunk_info.certifiable_since_height, Some(block4.header().height()));
    }
    // block4's own chunks are not certifiable yet: block3 is still uncertified.
    for chunk_info in &uncertified {
        if chunk_info.chunk_id.block_hash == *block4.hash() {
            assert_eq!(chunk_info.certifiable_since_height, None);
        }
    }

    let chunk = first_shard_chunk_id(&block3);
    let stamped = block4.header().height();
    let eligible_at = |height| {
        core_reader.fallback_eligible_in_carrying_block(height, block4.hash(), &chunk).unwrap()
    };
    assert!(!eligible_at(stamped + SPICE_FALLBACK_CERTIFICATION_DELAY - 1));
    assert!(eligible_at(stamped + SPICE_FALLBACK_CERTIFICATION_DELAY));
}

const FALLBACK_ONLY_SCHEDULE_EPOCH_LENGTH: BlockHeightDelta = 400;
const FALLBACK_ONLY_SCHEDULE_NUM_SHARDS: usize = 4;

fn fallback_only_heights(
    epoch_height: EpochHeight,
    shard_index: usize,
    range: Range<BlockHeight>,
) -> Vec<BlockHeight> {
    range
        .filter(|height| {
            is_fallback_only_height_for_shard_index(
                FALLBACK_ONLY_SCHEDULE_EPOCH_LENGTH,
                epoch_height,
                FALLBACK_ONLY_SCHEDULE_NUM_SHARDS,
                shard_index,
                *height,
            )
        })
        .collect()
}

#[test]
fn test_each_shard_is_fallback_only_once_per_epoch_length() {
    let scheduled: Vec<_> = (0..FALLBACK_ONLY_SCHEDULE_NUM_SHARDS)
        .map(|shard_index| fallback_only_heights(0, shard_index, 1000..1400))
        .collect();
    assert_eq!(scheduled, vec![vec![1200], vec![1300], vec![1000], vec![1100]]);
}

#[test]
fn test_fallback_only_slots_rotate_across_epochs() {
    let scheduled: Vec<_> = (0..FALLBACK_ONLY_SCHEDULE_NUM_SHARDS as u64)
        .map(|epoch_height| fallback_only_heights(epoch_height, 0, 1000..1400))
        .collect();
    assert_eq!(scheduled, vec![vec![1200], vec![1100], vec![1000], vec![1300]]);
}

#[test]
fn test_no_fallback_only_chunk_when_epoch_shorter_than_shard_count() {
    assert!(
        (1000..1010).all(|height| !is_fallback_only_height_for_shard_index(5, 0, 6, 0, height))
    );
}

// Walks the chain until a block carries a fallback-only chunk. Each step certifies the block two
// heights back, since a block may not skip an earlier chunk's execution result.
pub(super) fn grow_chain_to_fallback_only_block(
    chain: &mut Chain,
    bound: usize,
) -> (Arc<Block>, ShardId) {
    let mut blocks = vec![chain.genesis_block()];
    for _ in 0..bound {
        let parent = blocks.last().unwrap().clone();
        let statements = if blocks.len() >= 3 {
            certify_block_designated(chain, &blocks[blocks.len() - 2])
        } else {
            vec![]
        };
        let block = append_block(chain, &parent, statements);
        let fallback_only_shard =
            block.chunks().iter_raw().map(|chunk| chunk.shard_id()).find(|shard_id| {
                is_fallback_only_chunk(chain.epoch_manager.as_ref(), block.header(), *shard_id)
                    .unwrap()
            });
        if let Some(shard_id) = fallback_only_shard {
            return (block, shard_id);
        }
        blocks.push(block);
    }
    panic!("no fallback-only block within {bound} heights");
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_recorded_chunk_info_marks_the_scheduled_shard_fallback_only() {
    let (mut chain, core_reader) = setup();
    let (fallback_only_block, fallback_only_shard) =
        grow_chain_to_fallback_only_block(&mut chain, 40);
    let fallback_only_chunks: Vec<_> = core_reader
        .get_uncertified_chunks(fallback_only_block.hash())
        .unwrap()
        .into_iter()
        .filter(|chunk_info| chunk_info.is_fallback_only)
        .map(|chunk_info| chunk_info.chunk_id)
        .collect();
    assert_eq!(
        fallback_only_chunks,
        vec![SpiceChunkId {
            block_hash: *fallback_only_block.hash(),
            shard_id: fallback_only_shard
        }]
    );
}

// Enough validators that a chunk's designated assignment stays under 2/3 of total stake, asserted
// below.
pub(super) fn validators_with_minority_designated_stake() -> Vec<String> {
    (0..150).map(|i| format!("test{i}")).collect()
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_rejects_designated_only_certification_of_fallback_only_chunk() {
    let validators = validators_with_minority_designated_stake();
    let (mut chain, core_reader) = setup_with_validators(&validators);
    let (fallback_only_block, shard_id) = grow_chain_to_fallback_only_block(&mut chain, 40);
    let chunk_header = fallback_only_block
        .chunks()
        .iter_raw()
        .find(|chunk| chunk.shard_id() == shard_id)
        .unwrap()
        .clone();
    // Certifying the parent leaves the fallback-only chunk as the oldest uncertified one.
    let parent = chain.chain_store().get_block(fallback_only_block.header().prev_hash()).unwrap();
    let parent_certification = certify_block_designated(&chain, &parent);
    let tip = append_block(&mut chain, &fallback_only_block, parent_certification);
    let uncertified = core_reader.get_uncertified_chunks(tip.hash()).unwrap();
    let chunk_id = SpiceChunkId { block_hash: *fallback_only_block.hash(), shard_id };
    assert!(uncertified.iter().all(|info| info.chunk_id.block_hash != *parent.hash()));
    assert!(uncertified.iter().any(|info| info.chunk_id == chunk_id && info.is_fallback_only));

    let (designated, non_designated) =
        split_designated(&chain, &fallback_only_block, &chunk_header, &validators);
    let all_stake = all_stake_fallback_assignment(
        chain.epoch_manager.as_ref(),
        fallback_only_block.header().epoch_id(),
    )
    .unwrap();
    // The whole designated set must fall short of 2/3 of total stake, or a designated-only block
    // would certify on the all-stake path and prove nothing about the designated rule.
    let mut endorsers: HashSet<AccountId> = designated.iter().cloned().collect();
    assert!(!all_stake.is_endorsed(&endorsers));

    let designated_only = build_block(
        &chain,
        &tip,
        endorsements_and_execution_result(&designated, &fallback_only_block, &chunk_header),
    );
    assert_matches!(
        core_reader.validate_core_statements_in_block(&designated_only),
        Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
            reason: "execution results included without enough corresponding endorsement",
            ..
        })
    );

    // Topping the designated set up to 2/3 of total stake certifies, so the rejection above is the
    // designated rule being skipped, not an unreachable threshold. Stakes are not uniform, so the
    // set is grown against the assignment rather than by a count.
    for account in non_designated {
        if all_stake.is_endorsed(&endorsers) {
            break;
        }
        endorsers.insert(account);
    }
    assert!(all_stake.is_endorsed(&endorsers), "fallback set must be able to certify");
    let mut all_stake_endorsers: Vec<AccountId> = endorsers.into_iter().collect();
    all_stake_endorsers.sort();

    let all_stake_block = build_block(
        &chain,
        &tip,
        endorsements_and_execution_result(
            &all_stake_endorsers,
            &fallback_only_block,
            &chunk_header,
        ),
    );
    core_reader.validate_core_statements_in_block(&all_stake_block).unwrap();
}

fn chunk_info(
    certifiable_since_height: Option<BlockHeight>,
    is_fallback_only: bool,
) -> SpiceUncertifiedChunkInfo {
    SpiceUncertifiedChunkInfo {
        chunk_id: SpiceChunkId { block_hash: CryptoHash::default(), shard_id: ShardId::new(0) },
        missing_endorsements: vec![],
        present_endorsements: vec![],
        present_fallback_endorsements: vec![],
        certifiable_since_height,
        is_fallback_only,
    }
}

fn fallback_only_chunk_info(
    certifiable_since_height: Option<BlockHeight>,
) -> SpiceUncertifiedChunkInfo {
    chunk_info(certifiable_since_height, true)
}

fn ordinary_chunk_info(certifiable_since_height: Option<BlockHeight>) -> SpiceUncertifiedChunkInfo {
    chunk_info(certifiable_since_height, false)
}

#[test]
fn test_fallback_only_chunk_is_eligible_before_it_is_certifiable() {
    // Waiting until it is certifiable would open the rule a block after the endorsements land,
    // and no block can justify a result with no endorsement.
    assert!(fallback_eligible(1, &fallback_only_chunk_info(None)));
    assert!(fallback_eligible(1, &fallback_only_chunk_info(Some(1))));
}

#[test]
fn test_ordinary_chunk_waits_the_full_delay_after_becoming_certifiable() {
    assert!(!fallback_eligible(BlockHeight::MAX, &ordinary_chunk_info(None)));
    let certifiable_since = 100;
    let info = ordinary_chunk_info(Some(certifiable_since));
    assert!(!fallback_eligible(certifiable_since + SPICE_FALLBACK_CERTIFICATION_DELAY - 1, &info));
    assert!(fallback_eligible(certifiable_since + SPICE_FALLBACK_CERTIFICATION_DELAY, &info));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_rejects_skipping_an_uncertified_fallback_only_chunk() {
    let validators = validators_with_minority_designated_stake();
    let (mut chain, core_reader) = setup_with_validators(&validators);
    let (fallback_only_block, shard_id) = grow_chain_to_fallback_only_block(&mut chain, 40);
    let chunk_of = |block: &Block| {
        block.chunks().iter_raw().find(|chunk| chunk.shard_id() == shard_id).unwrap().clone()
    };
    let chunk_header = chunk_of(&fallback_only_block);
    let parent = chain.chain_store().get_block(fallback_only_block.header().prev_hash()).unwrap();
    let parent_certification = certify_block_designated(&chain, &parent);
    let next_block = append_block(&mut chain, &fallback_only_block, parent_certification);

    // Every designated endorsement lands, which empties missing_endorsements without certifying
    // the chunk: only 2/3 of total stake can do that.
    let (designated, _) =
        split_designated(&chain, &fallback_only_block, &chunk_header, &validators);
    let tip = append_block(
        &mut chain,
        &next_block,
        endorsement_statements(&designated, &fallback_only_block, &chunk_header),
    );
    let chunk_id = SpiceChunkId { block_hash: *fallback_only_block.hash(), shard_id };
    let chunk_info = core_reader
        .get_uncertified_chunks(tip.hash())
        .unwrap()
        .into_iter()
        .find(|info| info.chunk_id == chunk_id)
        .expect("the fallback-only chunk is still uncertified");
    assert!(chunk_info.missing_endorsements.is_empty());

    // Certifying the same shard one height later would endorse a child before its parent.
    let later_chunk = chunk_of(&next_block);
    let (later_designated, _) = split_designated(&chain, &next_block, &later_chunk, &validators);
    let skipping_block = build_block(
        &chain,
        &tip,
        endorsements_and_execution_result(&later_designated, &next_block, &later_chunk),
    );
    assert_matches!(
        core_reader.validate_core_statements_in_block(&skipping_block),
        Err(InvalidSpiceCoreStatementsError::SkippedExecutionResult { .. })
    );
}

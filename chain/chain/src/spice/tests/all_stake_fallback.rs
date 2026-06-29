use crate::spice::core::{SPICE_FALLBACK_CERTIFICATION_DELAY, fallback_eligible};
use crate::spice::tests::core::{
    block_certification_core_statements, build_block, endorsement_into_core_statement,
    process_block, setup, setup_with_validators, test_chunk_endorsement,
    test_execution_result_for_chunk,
};
use crate::{Block, Chain};
use assert_matches::assert_matches;
use near_primitives::block_body::SpiceCoreStatement;
use near_primitives::errors::InvalidSpiceCoreStatementsError;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::types::{AccountId, BlockHeight, SpiceChunkId};
use near_store::adapter::StoreAdapter as _;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::sync::Arc;

fn store_adapter(chain: &Chain) -> ChainStoreAdapter {
    chain.chain_store().chain_store()
}

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
    let (mut chain, _core_reader) = setup();
    let genesis = chain.genesis_block();
    let block1 = append_block(&mut chain, &genesis, vec![]);
    let block2 = append_block(&mut chain, &block1, vec![]); // prev(block2) = block1 is uncertified

    // Even arbitrarily far in the future, block2 is not eligible while its parent block1 is
    // uncertified.
    let eligible = fallback_eligible(
        &store_adapter(&chain),
        100,
        block2.hash(),
        &first_shard_chunk_id(&block2),
    )
    .unwrap();
    assert!(!eligible);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_fallback_eligible_respects_delay_after_parent_certified() {
    let (mut chain, _core_reader) = setup();
    let genesis = chain.genesis_block();
    let block1 = append_block(&mut chain, &genesis, vec![]);
    let block2 = append_block(&mut chain, &block1, vec![]);
    // block3 includes block1's endorsements + results, certifying it. cert_height(block1) =
    // block3.height().
    let block3 = append_block(&mut chain, &block2, block_certification_core_statements(&block1));

    let store = store_adapter(&chain);
    let chunk = first_shard_chunk_id(&block2);
    let delay = SPICE_FALLBACK_CERTIFICATION_DELAY;
    let cert_height = block3.header().height();

    // Below the window: not eligible.
    assert!(!fallback_eligible(&store, cert_height + delay - 1, block3.hash(), &chunk).unwrap());
    // At the window: eligible.
    assert!(fallback_eligible(&store, cert_height + delay, block3.hash(), &chunk).unwrap());
    // Monotone: stays eligible far past the window (also exercises a large height gap in the
    // look-back walk).
    assert!(fallback_eligible(&store, cert_height + delay + 50, block3.hash(), &chunk).unwrap());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_fallback_eligible_false_when_height_below_delay() {
    let (mut chain, _core_reader) = setup();
    let genesis = chain.genesis_block();
    let block1 = append_block(&mut chain, &genesis, vec![]);
    let block2 = append_block(&mut chain, &block1, vec![]);

    // carrying_height < delay short-circuits to false (covers genesis / spice activation).
    let below_delay = SPICE_FALLBACK_CERTIFICATION_DELAY - 1;
    assert!(
        !fallback_eligible(
            &store_adapter(&chain),
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
fn split_designated(
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

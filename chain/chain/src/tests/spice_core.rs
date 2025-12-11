use assert_matches::assert_matches;
use itertools::Itertools as _;
use near_async::messaging::{Handler as _, IntoSender as _, noop};
use near_async::time::Clock;
use near_chain_configs::test_genesis::{TestGenesisBuilder, ValidatorsSpec};
use near_network::client::SpiceChunkEndorsementMessage;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::Block;
use near_primitives::block_body::SpiceCoreStatement;
use near_primitives::errors::InvalidSpiceCoreStatementsError;
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::stateless_validation::spice_chunk_endorsement::testonly_create_endorsement_core_statement;
use near_primitives::stateless_validation::spice_chunk_endorsement::{
    SpiceChunkEndorsement, SpiceEndorsementSignedData, SpiceVerifiedEndorsement,
};
use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{
    BlockExecutionResults, ChunkExecutionResult, ChunkExecutionResultHash, SpiceChunkId,
};
use near_store::adapter::StoreAdapter as _;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use crate::spice_core::{SpiceCoreReader, record_uncertified_chunks_for_block};
use crate::spice_core_writer_actor::{ProcessedBlock, SpiceCoreWriterActor};
use crate::test_utils::{
    get_chain_with_genesis, get_fake_next_block_spice_chunk_headers, process_block_sync,
};
use crate::{BlockProcessingArtifact, Chain, Provenance};

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_get_execution_by_shard_id_with_no_execution_results() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    let execution_results = core_reader.get_execution_results_by_shard_id(block.header()).unwrap();
    assert_eq!(execution_results, HashMap::new());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_get_execution_by_shard_id_with_some_execution_results() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let mut core_writer_actor = core_writer_actor(&chain);
    for validator in test_validators() {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_writer_actor.handle(SpiceChunkEndorsementMessage(endorsement));
    }
    let execution_results = core_reader.get_execution_results_by_shard_id(block.header()).unwrap();
    assert_eq!(execution_results.len(), 1);
    assert!(execution_results.contains_key(&chunk_header.shard_id()));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_get_execution_by_shard_id_with_all_execution_results() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();

    let mut core_writer_actor = core_writer_actor(&chain);
    for chunk_header in chunks.iter_raw() {
        for validator in test_validators() {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            core_writer_actor.handle(SpiceChunkEndorsementMessage(endorsement));
        }
    }
    let execution_results = core_reader.get_execution_results_by_shard_id(block.header()).unwrap();
    for chunk_header in chunks.iter_raw() {
        assert!(execution_results.contains_key(&chunk_header.shard_id()));
    }
    assert_eq!(execution_results.len(), chunks.len());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_get_block_execution_results_for_genesis() {
    let (chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let execution_results = core_reader.get_block_execution_results(genesis.header()).unwrap();
    assert!(execution_results.is_some());
    assert_eq!(execution_results.unwrap().0.len(), genesis.chunks().len());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_get_block_execution_results_with_no_execution_results() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let execution_results = core_reader.get_block_execution_results(block.header()).unwrap();
    assert!(execution_results.is_none())
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_get_block_execution_results_with_some_execution_results_missing() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let mut core_writer_actor = core_writer_actor(&chain);
    for validator in test_validators() {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_writer_actor.handle(SpiceChunkEndorsementMessage(endorsement));
    }
    let execution_results = core_reader.get_block_execution_results(block.header()).unwrap();
    assert!(execution_results.is_none())
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_get_block_execution_results_with_all_execution_results_present() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();

    let mut core_writer_actor = core_writer_actor(&chain);
    for chunk_header in chunks.iter_raw() {
        for validator in test_validators() {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            core_writer_actor.handle(SpiceChunkEndorsementMessage(endorsement));
        }
    }
    let execution_results = core_reader.get_block_execution_results(block.header()).unwrap();
    assert!(execution_results.is_some());
    let execution_results = execution_results.unwrap();
    for chunk_header in chunks.iter_raw() {
        assert!(execution_results.0.contains_key(&chunk_header.shard_id()));
    }
    assert_eq!(execution_results.0.len(), chunks.len());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_all_execution_results_exist_when_all_exist() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();

    let mut core_writer_actor = core_writer_actor(&chain);
    for chunk_header in chunks.iter_raw() {
        for validator in test_validators() {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            core_writer_actor.handle(SpiceChunkEndorsementMessage(endorsement));
        }
    }
    assert!(core_reader.all_execution_results_exist(block.header()).unwrap());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_all_execution_results_exist_when_some_are_missing() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let mut core_writer_actor = core_writer_actor(&chain);
    for validator in test_validators() {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_writer_actor.handle(SpiceChunkEndorsementMessage(endorsement));
    }
    assert!(!core_reader.all_execution_results_exist(block.header()).unwrap());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_for_genesis() {
    let (chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    assert_eq!(core_reader.core_statement_for_next_block(genesis.header()).unwrap(), vec![]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_for_non_spice_block() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_non_spice_block(&mut chain, &genesis);
    process_block(&mut chain, block.clone());
    assert_eq!(core_reader.core_statement_for_next_block(block.header()).unwrap(), vec![]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_when_block_is_not_recorded() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    assert!(core_reader.core_statement_for_next_block(block.header()).is_err());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_contains_no_endorsements() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let core_statements = core_reader.core_statement_for_next_block(block.header()).unwrap();
    assert_eq!(core_statements, vec![]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_contains_new_endorsements() {
    let (mut chain, core_reader) = setup();
    let mut core_writer_actor = core_writer_actor(&chain);
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    core_writer_actor.handle(SpiceChunkEndorsementMessage(endorsement.clone()));

    let core_statements = core_reader.core_statement_for_next_block(block.header()).unwrap();
    assert_eq!(core_statements.len(), 1);
    assert_eq!(core_statements[0], endorsement_into_core_statement(endorsement));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_contains_no_endorsements_for_fork_block() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    // We create fork of length 2 because the way we build test chunks they would be equivalent
    // in fork_block and block when fork is of length 1, so endorsements would be considered
    // valid.
    let fork_block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, fork_block.clone());
    let fork_block = build_block(&mut chain, &fork_block, vec![]);
    process_block(&mut chain, fork_block.clone());

    let fork_chunks = fork_block.chunks();
    let fork_chunk_header = fork_chunks.iter_raw().next().unwrap();
    let mut core_writer_actor = core_writer_actor(&chain);
    let endorsement = test_chunk_endorsement(&test_validators()[0], &fork_block, fork_chunk_header);
    core_writer_actor.handle(SpiceChunkEndorsementMessage(endorsement));

    let core_statements = core_reader.core_statement_for_next_block(block.header()).unwrap();
    assert_eq!(core_statements.len(), 0);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_contains_new_endorsement() {
    let (mut chain, core_reader) = setup();
    let mut core_writer_actor = core_writer_actor(&chain);
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    core_writer_actor.handle(SpiceChunkEndorsementMessage(endorsement.clone()));

    let core_statements = core_reader.core_statement_for_next_block(block.header()).unwrap();
    assert_eq!(core_statements.len(), 1);
    assert_eq!(core_statements[0], endorsement_into_core_statement(endorsement));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_contains_new_execution_results() {
    let (mut chain, core_reader) = setup();
    let mut core_writer_actor = core_writer_actor(&chain);
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    let validators = test_validators();
    for validator in &validators {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_writer_actor.handle(SpiceChunkEndorsementMessage(endorsement.clone()));
    }

    let execution_result = test_execution_result_for_chunk(&chunk_header);
    let core_statements = core_reader.core_statement_for_next_block(block.header()).unwrap();
    assert!(core_statements.contains(&SpiceCoreStatement::ChunkExecutionResult {
        chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id: chunk_header.shard_id() },
        execution_result,
    }));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_with_endorsements_creates_valid_block() {
    let (mut chain, core_reader) = setup();
    let mut core_writer_actor = core_writer_actor(&chain);
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    core_writer_actor.handle(SpiceChunkEndorsementMessage(endorsement));

    let core_statements = core_reader.core_statement_for_next_block(block.header()).unwrap();
    let next_block = build_block(&chain, &block, core_statements);
    assert!(core_reader.validate_core_statements_in_block(&next_block).is_ok());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_with_execution_results_creates_valid_block() {
    let (mut chain, core_reader) = setup();
    let mut core_writer_actor = core_writer_actor(&chain);
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    let validators = test_validators();
    for validator in &validators {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_writer_actor.handle(SpiceChunkEndorsementMessage(endorsement.clone()));
    }

    let core_statements = core_reader.core_statement_for_next_block(block.header()).unwrap();
    let next_block = build_block(&chain, &block, core_statements);
    assert!(core_reader.validate_core_statements_in_block(&next_block).is_ok());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_contains_no_already_included_execution_results() {
    let (mut chain, core_reader) = setup();
    let mut core_writer_actor = core_writer_actor(&chain);
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    let validators = test_validators();
    for validator in &validators {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_writer_actor.handle(SpiceChunkEndorsementMessage(endorsement.clone()));
    }

    let core_statements = core_reader.core_statement_for_next_block(block.header()).unwrap();
    let next_block = build_block(&chain, &block, core_statements);
    process_block(&mut chain, next_block.clone());

    assert_eq!(core_reader.core_statement_for_next_block(next_block.header()).unwrap(), vec![]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_contains_no_already_included_endorsement() {
    let (mut chain, core_reader) = setup();
    let mut core_writer_actor = core_writer_actor(&chain);
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    core_writer_actor.handle(SpiceChunkEndorsementMessage(endorsement));

    let core_statements = core_reader.core_statement_for_next_block(block.header()).unwrap();
    let next_block = build_block(&chain, &block, core_statements);
    process_block(&mut chain, next_block.clone());

    assert_eq!(core_reader.core_statement_for_next_block(next_block.header()).unwrap(), vec![]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_contains_no_endorsements_for_included_execution_result() {
    let (mut chain, core_reader) = setup();
    let mut core_writer_actor = core_writer_actor(&chain);
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    let all_validators = test_validators();
    let (last_validator, validators) = all_validators.split_last().unwrap();
    for validator in validators {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_writer_actor.handle(SpiceChunkEndorsementMessage(endorsement.clone()));
    }

    let core_statements = core_reader.core_statement_for_next_block(block.header()).unwrap();
    let next_block = build_block(&chain, &block, core_statements);
    process_block(&mut chain, next_block.clone());

    let endorsement = test_chunk_endorsement(&last_validator, &block, chunk_header);
    core_writer_actor.handle(SpiceChunkEndorsementMessage(endorsement));

    assert_eq!(core_reader.core_statement_for_next_block(next_block.header()).unwrap(), vec![]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_contains_all_endorsements() {
    let (mut chain, core_reader) = setup();
    let mut core_writer_actor = core_writer_actor(&chain);
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    let all_validators = test_validators();
    let mut all_endorsements = Vec::new();
    for validator in all_validators {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_writer_actor.handle(SpiceChunkEndorsementMessage(endorsement.clone()));
        all_endorsements.push(endorsement_into_core_statement(endorsement));
    }

    let core_statements = core_reader.core_statement_for_next_block(block.header()).unwrap();
    for endorsement in &all_endorsements {
        assert!(core_statements.contains(endorsement));
    }
}

fn run_record_uncertified_chunks_for_block(chain: &mut Chain, block: &Block) {
    let mut chain_store_update = chain.chain_store.store_update();
    assert!(
        record_uncertified_chunks_for_block(
            &mut chain_store_update,
            chain.epoch_manager.as_ref(),
            &block
        )
        .is_ok()
    );
    chain_store_update.commit().unwrap();
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_test_record_block_for_non_spice_block_for_non_spice_block() {
    let (mut chain, _core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_non_spice_block(&mut chain, &genesis);
    run_record_uncertified_chunks_for_block(&mut chain, &block);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_record_block_for_block_without_endorsements() {
    let (mut chain, _core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    run_record_uncertified_chunks_for_block(&mut chain, &block);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_record_block_for_block_with_endorsements() {
    let (mut chain, _core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let all_validators = test_validators();
    let (in_block_validators, in_core_validators) =
        all_validators.split_at(all_validators.len() / 2);
    assert!(in_block_validators.len() >= in_core_validators.len());

    let block_core_statements = in_block_validators
        .iter()
        .map(|validator| {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            endorsement_into_core_statement(endorsement)
        })
        .collect_vec();

    let next_block = build_block(&mut chain, &genesis, block_core_statements);
    run_record_uncertified_chunks_for_block(&mut chain, &next_block);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_record_block_for_block_with_execution_results() {
    let (mut chain, _core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let all_validators = test_validators();
    let mut block_core_statements = all_validators
        .iter()
        .map(|validator| {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            endorsement_into_core_statement(endorsement)
        })
        .collect_vec();
    let execution_result = test_execution_result_for_chunk(&chunk_header);
    block_core_statements.push(SpiceCoreStatement::ChunkExecutionResult {
        chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id: chunk_header.shard_id() },
        execution_result,
    });

    let next_block = build_block(&mut chain, &genesis, block_core_statements);
    run_record_uncertified_chunks_for_block(&mut chain, &next_block);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_endorsements_from_forks_can_be_used_in_other_forks() {
    let (mut chain, core_reader) = setup();
    let mut core_writer_actor = core_writer_actor(&chain);
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    let core_endorsement = endorsement_into_core_statement(endorsement);

    let fork_block = build_block(&mut chain, &block, vec![core_endorsement.clone()]);
    process_block(&mut chain, fork_block.clone());
    core_writer_actor.handle(ProcessedBlock { block_hash: *fork_block.hash() });

    let core_statements = core_reader.core_statement_for_next_block(block.header()).unwrap();
    assert_eq!(core_statements, vec![core_endorsement]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_valid_with_non_spice_block() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_non_spice_block(&mut chain, &genesis);
    assert!(core_reader.validate_core_statements_in_block(&block).is_ok());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_with_parent_not_recorded() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    let next_block = build_block(&mut chain, &block, vec![]);
    assert_matches!(
        core_reader.validate_core_statements_in_block(&next_block),
        Err(InvalidSpiceCoreStatementsError::NoPrevUncertifiedChunks)
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_with_endorsement_with_invalid_account_id() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let invalid_validator = "invalid-validator";
    let endorsement = test_chunk_endorsement(invalid_validator, &block, chunk_header);
    let core_endorsement = endorsement_into_core_statement(endorsement);

    let next_block = build_block(&mut chain, &block, vec![core_endorsement]);
    assert_matches!(
        core_reader.validate_core_statements_in_block(&next_block),
        Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
            reason: "endorsement is irrelevant",
            index: 0
        })
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_with_endorsement_with_invalid_signature() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    let verified = endorsement_into_verified(endorsement);

    let core_endorsement =
        SpiceCoreStatement::Endorsement(testonly_create_endorsement_core_statement(
            verified.account_id().clone(),
            verified.signature().clone(),
            SpiceEndorsementSignedData {
                execution_result_hash: ChunkExecutionResultHash(CryptoHash::default()),
                chunk_id: verified.chunk_id().clone(),
            },
        ));
    let next_block = build_block(&mut chain, &block, vec![core_endorsement]);
    assert_matches!(
        core_reader.validate_core_statements_in_block(&next_block),
        Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
            reason: "invalid signature",
            index: 0
        })
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_with_endorsement_already_included() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    let core_endorsement = endorsement_into_core_statement(endorsement);

    let next_block = build_block(&mut chain, &block, vec![core_endorsement.clone()]);
    process_block(&mut chain, next_block.clone());

    let next_next_block = build_block(&mut chain, &next_block, vec![core_endorsement]);
    assert_matches!(
        core_reader.validate_core_statements_in_block(&next_next_block),
        Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
            reason: "endorsement is irrelevant",
            index: 0
        })
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_with_endorsement_for_unknown_block() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let next_block = build_block(&mut chain, &block, vec![]);
    let next_block_chunks = next_block.chunks();
    let next_block_chunk_header = next_block_chunks.iter_raw().next().unwrap();

    let endorsement =
        test_chunk_endorsement(&test_validators()[0], &next_block, next_block_chunk_header);
    let core_endorsement = endorsement_into_core_statement(endorsement);

    let fork_block = build_block(&mut chain, &block, vec![core_endorsement]);
    assert_matches!(
        core_reader.validate_core_statements_in_block(&fork_block),
        Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
            reason: "endorsement is irrelevant",
            index: 0
        })
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_with_duplicate_endorsement() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    let core_endorsement = endorsement_into_core_statement(endorsement);
    let next_block =
        build_block(&mut chain, &block, vec![core_endorsement.clone(), core_endorsement]);
    assert_matches!(
        core_reader.validate_core_statements_in_block(&next_block),
        Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
            reason: "duplicate endorsement",
            index: 1
        })
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_with_duplicate_execution_result() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let all_validators = test_validators();
    let mut block_core_statements = all_validators
        .iter()
        .map(|validator| {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            endorsement_into_core_statement(endorsement)
        })
        .collect_vec();
    let execution_result = SpiceCoreStatement::ChunkExecutionResult {
        chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id: chunk_header.shard_id() },
        execution_result: test_execution_result_for_chunk(&chunk_header),
    };
    block_core_statements.push(execution_result.clone());
    block_core_statements.push(execution_result);
    let duplicate_index = block_core_statements.len() - 1;

    let next_block = build_block(&mut chain, &block, block_core_statements);
    let result = core_reader.validate_core_statements_in_block(&next_block);
    assert_matches!(
        result,
        Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
            reason: "duplicate execution result",
            index: _,
        })
    );
    let Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement { index, .. }) = result else {
        panic!()
    };
    assert_eq!(index, duplicate_index);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_with_child_execution_result_included_before_parent() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let parent_block = build_block(&mut chain, &genesis, vec![]);
    let parent_chunks = parent_block.chunks();
    let parent_chunk_header = parent_chunks.iter_raw().next().unwrap();
    process_block(&mut chain, parent_block.clone());
    let block = build_block(&mut chain, &parent_block, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let all_validators = test_validators();
    let mut block_core_statements = all_validators
        .iter()
        .map(|validator| {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            endorsement_into_core_statement(endorsement)
        })
        .collect_vec();
    let execution_result = SpiceCoreStatement::ChunkExecutionResult {
        chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id: chunk_header.shard_id() },
        execution_result: test_execution_result_for_chunk(&chunk_header),
    };
    block_core_statements.push(execution_result);

    let next_block = build_block(&mut chain, &block, block_core_statements);
    let result = core_reader.validate_core_statements_in_block(&next_block);
    assert_matches!(result, Err(InvalidSpiceCoreStatementsError::SkippedExecutionResult { .. }));

    let Err(InvalidSpiceCoreStatementsError::SkippedExecutionResult {
        chunk_id: SpiceChunkId { block_hash, shard_id },
    }) = result
    else {
        panic!();
    };
    assert_eq!(parent_block.hash(), &block_hash);
    assert_eq!(parent_chunk_header.shard_id(), shard_id);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_with_execution_result_included_without_enough_endorsements()
 {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    let verified = endorsement_into_verified(endorsement.clone());
    let block_core_statements = vec![
        endorsement_into_core_statement(endorsement),
        SpiceCoreStatement::ChunkExecutionResult {
            chunk_id: verified.chunk_id().clone(),
            execution_result: test_execution_result_for_chunk(&chunk_header),
        },
    ];

    let next_block = build_block(&mut chain, &block, block_core_statements);
    let result = core_reader.validate_core_statements_in_block(&next_block);
    assert_matches!(
        result,
        Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
            index: 1,
            reason: "execution results included without enough corresponding endorsement",
        })
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_with_execution_result_included_without_endorsements() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let block_core_statements = vec![SpiceCoreStatement::ChunkExecutionResult {
        chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id: chunk_header.shard_id() },
        execution_result: test_execution_result_for_chunk(&chunk_header),
    }];

    let next_block = build_block(&mut chain, &block, block_core_statements);
    let result = core_reader.validate_core_statements_in_block(&next_block);
    assert_matches!(
        result,
        Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
            index: 0,
            reason: "execution results included without enough corresponding endorsement",
        })
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_with_enough_endorsements_but_no_execution_result() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let all_validators = test_validators();
    let block_core_statements = all_validators
        .iter()
        .map(|validator| {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            endorsement_into_core_statement(endorsement)
        })
        .collect_vec();

    let next_block = build_block(&mut chain, &block, block_core_statements);
    let result = core_reader.validate_core_statements_in_block(&next_block);
    assert_matches!(
        result,
        Err(InvalidSpiceCoreStatementsError::NoExecutionResultForEndorsedChunk { .. })
    );

    let Err(InvalidSpiceCoreStatementsError::NoExecutionResultForEndorsedChunk {
        chunk_id: SpiceChunkId { block_hash, shard_id },
    }) = result
    else {
        panic!();
    };
    assert_eq!(block.hash(), &block_hash);
    assert_eq!(chunk_header.shard_id(), shard_id);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_with_execution_result_different_from_endorsed_one() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let all_validators = test_validators();
    let mut block_core_statements = all_validators
        .iter()
        .map(|validator| {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            endorsement_into_core_statement(endorsement)
        })
        .collect_vec();
    block_core_statements.push(SpiceCoreStatement::ChunkExecutionResult {
        chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id: chunk_header.shard_id() },
        execution_result: invalid_execution_result_for_chunk(&chunk_header),
    });
    let execution_result_index = block_core_statements.len() - 1;

    let next_block = build_block(&mut chain, &block, block_core_statements);
    let result = core_reader.validate_core_statements_in_block(&next_block);
    assert_matches!(
        result,
        Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
            reason: "endorsed execution result is different from execution result in block",
            index: _,
        })
    );
    let Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement { index, .. }) = result else {
        panic!();
    };
    assert_eq!(index, execution_result_index);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_valid_with_no_core_statements() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let next_block = build_block(&mut chain, &block, vec![]);
    assert_matches!(core_reader.validate_core_statements_in_block(&next_block), Ok(()));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_valid_with_not_enough_on_chain_endorsements_for_execution_result()
 {
    let (mut chain, core_reader) = setup();
    let mut core_writer_actor = core_writer_actor(&chain);
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let all_validators = test_validators();
    let (in_block_validators, in_core_validators) =
        all_validators.split_at(all_validators.len() / 2);
    assert!(in_block_validators.len() >= in_core_validators.len());

    let block_core_statements = in_block_validators
        .iter()
        .map(|validator| {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            endorsement_into_core_statement(endorsement)
        })
        .collect_vec();

    for validator in in_core_validators {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_writer_actor.handle(SpiceChunkEndorsementMessage(endorsement));
    }
    let next_block = build_block(&mut chain, &block, block_core_statements);
    assert_matches!(core_reader.validate_core_statements_in_block(&next_block), Ok(()));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_valid_endorsements_without_execution_result() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let all_validators = test_validators();
    let (left_validators, right_validators) = all_validators.split_at(all_validators.len() / 2);
    assert!(left_validators.len() <= right_validators.len());
    let block_core_statements = left_validators
        .iter()
        .map(|validator| {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            endorsement_into_core_statement(endorsement)
        })
        .collect_vec();

    let next_block = build_block(&mut chain, &block, block_core_statements);
    assert_matches!(core_reader.validate_core_statements_in_block(&next_block), Ok(()));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_valid_endorsements_with_execution_result() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let all_validators = test_validators();
    let mut block_core_statements = all_validators
        .iter()
        .map(|validator| {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            endorsement_into_core_statement(endorsement)
        })
        .collect_vec();
    block_core_statements.push(SpiceCoreStatement::ChunkExecutionResult {
        chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id: chunk_header.shard_id() },
        execution_result: test_execution_result_for_chunk(&chunk_header),
    });

    let next_block = build_block(&mut chain, &block, block_core_statements);
    assert_matches!(core_reader.validate_core_statements_in_block(&next_block), Ok(()));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_valid_execution_result_with_ancestral_endorsements() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let all_validators = test_validators();
    let (left_validators, right_validators) = all_validators.split_at(all_validators.len() / 2);
    assert!(left_validators.len() <= right_validators.len());
    let next_block_core_statements = left_validators
        .iter()
        .map(|validator| {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            endorsement_into_core_statement(endorsement)
        })
        .collect_vec();
    let next_block = build_block(&chain, &block, next_block_core_statements);
    process_block(&mut chain, next_block.clone());

    let mut next_next_block_core_statements = right_validators
        .iter()
        .map(|validator| {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            endorsement_into_core_statement(endorsement)
        })
        .collect_vec();
    next_next_block_core_statements.push(SpiceCoreStatement::ChunkExecutionResult {
        chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id: chunk_header.shard_id() },
        execution_result: test_execution_result_for_chunk(&chunk_header),
    });

    let next_next_block = build_block(&mut chain, &next_block, next_next_block_core_statements);
    assert_matches!(core_reader.validate_core_statements_in_block(&next_next_block), Ok(()));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_get_last_certified_execution_results_for_next_block_with_no_certifications() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let execution_results = core_reader
        .get_last_certified_execution_results_for_next_block(block.header(), &[])
        .unwrap();
    let genesis_execution_results =
        core_reader.get_block_execution_results(genesis.header()).unwrap();
    assert!(!execution_results.0.is_empty());
    assert_eq!(execution_results, genesis_execution_results.unwrap());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_get_last_certified_execution_results_for_next_block_with_execution_result_in_core() {
    let (mut chain, core_reader) = setup();
    let mut core_writer_actor = core_writer_actor(&chain);
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let core_statements = block_certification_core_statements(&block);
    let next_block = build_block(&mut chain, &block, core_statements);
    process_block(&mut chain, next_block.clone());
    core_writer_actor.handle(ProcessedBlock { block_hash: *next_block.hash() });

    let execution_results = core_reader
        .get_last_certified_execution_results_for_next_block(next_block.header(), &[])
        .unwrap();
    let block_execution_results = block_execution_results(&block);
    assert_eq!(block_execution_results, execution_results);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_get_last_certified_execution_results_for_next_block_with_last_block_certified() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let core_statements = block_certification_core_statements(&block);
    let execution_results = core_reader
        .get_last_certified_execution_results_for_next_block(block.header(), &core_statements)
        .unwrap();
    let block_execution_results = block_execution_results(&block);
    assert_eq!(block_execution_results, execution_results);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_get_last_certified_execution_results_for_next_block_with_old_block_certified() {
    let (mut chain, core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let mut core_writer_actor = core_writer_actor(&chain);
    let core_statements = block_certification_core_statements(&block);
    let mut last_block = build_block(&mut chain, &block, core_statements);
    process_block(&mut chain, last_block.clone());
    core_writer_actor.handle(ProcessedBlock { block_hash: *last_block.hash() });

    for _ in 0..3 {
        let new_block = build_block(&chain, &last_block, vec![]);
        process_block(&mut chain, new_block.clone());
        last_block = new_block;
    }

    let execution_results = core_reader
        .get_last_certified_execution_results_for_next_block(last_block.header(), &[])
        .unwrap();
    let block_execution_results = block_execution_results(&block);
    assert_eq!(block_execution_results, execution_results);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_get_last_certified_execution_results_for_next_block_with_certification_split_between_core_and_core_statements()
 {
    let (mut chain, core_reader) = setup();
    let mut core_writer_actor = core_writer_actor(&chain);
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let last_shard_id = block.chunks().iter_raw().next().unwrap().shard_id();

    let (last_shard_core_statements, next_block_core_statements) =
        block_certification_core_statements(&block)
            .into_iter()
            .partition(|statement| statement.chunk_id().shard_id == last_shard_id);

    let next_block = build_block(&mut chain, &block, next_block_core_statements);
    process_block(&mut chain, next_block.clone());
    core_writer_actor.handle(ProcessedBlock { block_hash: *next_block.hash() });

    let execution_results = core_reader
        .get_last_certified_execution_results_for_next_block(
            next_block.header(),
            &last_shard_core_statements,
        )
        .unwrap();
    let block_execution_results = block_execution_results(&block);
    assert_eq!(block_execution_results, execution_results);
}

fn block_execution_results(block: &Block) -> BlockExecutionResults {
    let mut results = HashMap::new();
    for chunk in block.chunks().iter_raw() {
        results.insert(chunk.shard_id(), Arc::new(test_execution_result_for_chunk(&chunk)));
    }
    BlockExecutionResults(results)
}

fn block_certification_core_statements(block: &Block) -> Vec<SpiceCoreStatement> {
    let validators = test_validators();
    let mut core_statements = Vec::new();

    for chunk in block.chunks().iter_raw() {
        core_statements.extend(validators.iter().map(|validator| {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk);
            endorsement_into_core_statement(endorsement)
        }));
        core_statements.push(SpiceCoreStatement::ChunkExecutionResult {
            chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id: chunk.shard_id() },
            execution_result: test_execution_result_for_chunk(&chunk),
        });
    }
    core_statements
}

fn block_builder(chain: &Chain, prev_block: &Block) -> TestBlockBuilder {
    let block_producer = chain
        .epoch_manager
        .get_block_producer_info(prev_block.header().epoch_id(), prev_block.header().height() + 1)
        .unwrap();
    let signer = Arc::new(create_test_signer(block_producer.account_id().as_str()));
    TestBlockBuilder::new(Clock::real(), prev_block, signer)
        .chunks(get_fake_next_block_spice_chunk_headers(&prev_block, chain.epoch_manager.as_ref()))
}

fn build_non_spice_block(chain: &Chain, prev_block: &Block) -> Arc<Block> {
    // FIXME: Either don't use get_fake_next_block_spice_chunk_headers or use equivalent but
    // spice-less.
    block_builder(chain, prev_block).non_spice_block().build()
}

fn build_block(
    chain: &Chain,
    prev_block: &Block,
    spice_core_statements: Vec<SpiceCoreStatement>,
) -> Arc<Block> {
    block_builder(chain, prev_block).spice_core_statements(spice_core_statements).build()
}

fn process_block(chain: &mut Chain, block: Arc<Block>) {
    process_block_sync(
        chain,
        block.into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
}

fn test_validators() -> Vec<String> {
    (0..4).map(|i| format!("test{i}")).collect()
}

fn setup() -> (Chain, SpiceCoreReader) {
    init_test_logger();

    let num_shards = 3;

    let shard_layout = ShardLayout::multi_shard(num_shards, 0);

    let validators = test_validators();
    let validators_spec =
        ValidatorsSpec::desired_roles(&validators.iter().map(|v| v.as_str()).collect_vec(), &[]);

    let genesis = TestGenesisBuilder::new()
        .genesis_time_from_clock(&Clock::real())
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .build();

    let chain = get_chain_with_genesis(Clock::real(), genesis);
    let core_reader = core_reader(&chain);
    (chain, core_reader)
}

fn core_reader(chain: &Chain) -> SpiceCoreReader {
    SpiceCoreReader::new(
        chain.chain_store().chain_store(),
        chain.epoch_manager.clone(),
        Gas::from_teragas(100),
    )
}

fn core_writer_actor(chain: &Chain) -> SpiceCoreWriterActor {
    SpiceCoreWriterActor::new(
        chain.chain_store().chain_store(),
        chain.epoch_manager.clone(),
        core_reader(&chain),
        noop().into_sender(),
        noop().into_sender(),
    )
}

fn test_execution_result_for_chunk(chunk_header: &ShardChunkHeader) -> ChunkExecutionResult {
    ChunkExecutionResult {
        // Using chunk_hash makes sure that each chunk has a different execution result.
        chunk_extra: ChunkExtra::new_with_only_state_root(&chunk_header.chunk_hash().0),
        outgoing_receipts_root: CryptoHash::default(),
    }
}

fn invalid_execution_result_for_chunk(chunk_header: &ShardChunkHeader) -> ChunkExecutionResult {
    let mut execution_result = test_execution_result_for_chunk(chunk_header);
    execution_result.outgoing_receipts_root =
        CryptoHash::from_str("32222222222233333333334444444444445555555777").unwrap();
    execution_result
}

fn test_chunk_endorsement(
    validator: &str,
    block: &Block,
    chunk_header: &ShardChunkHeader,
) -> SpiceChunkEndorsement {
    let execution_result = test_execution_result_for_chunk(chunk_header);
    let signer = create_test_signer(&validator);
    SpiceChunkEndorsement::new(
        SpiceChunkId { block_hash: *block.hash(), shard_id: chunk_header.shard_id() },
        execution_result,
        &signer,
    )
}

fn endorsement_into_verified(endorsement: SpiceChunkEndorsement) -> SpiceVerifiedEndorsement {
    let signer = create_test_signer(endorsement.account_id().as_str());
    endorsement.into_verified(&signer.public_key()).unwrap()
}

fn endorsement_into_core_statement(endorsement: SpiceChunkEndorsement) -> SpiceCoreStatement {
    let verified = endorsement_into_verified(endorsement);
    verified
        .to_stored()
        .into_core_statement(verified.chunk_id().clone(), verified.account_id().clone())
}

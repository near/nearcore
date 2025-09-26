use assert_matches::assert_matches;
use itertools::Itertools as _;
use near_async::messaging::{IntoSender as _, Sender, noop};
use near_async::time::Clock;
use near_chain_configs::test_genesis::{TestGenesisBuilder, ValidatorsSpec};
use near_chain_primitives::Error;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::Block;
use near_primitives::block_body::SpiceCoreStatement;
use near_primitives::errors::InvalidSpiceCoreStatementsError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::stateless_validation::chunk_endorsement::{
    ChunkEndorsement, SpiceEndorsementSignedInner, SpiceEndorsementWithSignature,
};
use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{ChunkExecutionResult, ChunkExecutionResultHash, SpiceChunkId};
use near_store::adapter::StoreAdapter as _;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

use crate::spice_core::CoreStatementsProcessor;
use crate::spice_core::ExecutionResultEndorsed;
use crate::test_utils::{
    get_chain_with_genesis, get_fake_next_block_chunk_headers, process_block_sync,
};
use crate::{BlockProcessingArtifact, Chain, Provenance};

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_record_chunk_endorsement_non_spice_endorsement() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_non_spice_block(&mut chain, &genesis);
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    let epoch_id = block.header().epoch_id();
    let signer = create_test_signer(&test_validators()[0]);
    let endorsement = ChunkEndorsement::new(*epoch_id, chunk_header, &signer);
    assert!(core_processor.record_chunk_endorsement(endorsement).is_ok());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_record_chunk_endorsement_with_execution_result_already_present() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_non_spice_block(&mut chain, &genesis);
    process_block(&mut chain, block.clone());

    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    for validator in test_validators() {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_processor.record_chunk_endorsement(endorsement).unwrap();
    }
    let execution_results = core_processor.get_execution_results_by_shard_id(&block).unwrap();
    assert!(execution_results.contains_key(&chunk_header.shard_id()));

    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    assert!(core_processor.record_chunk_endorsement(endorsement).is_ok());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_record_chunk_endorsement_with_unknown_old_block() {
    let (mut chain, core_processor) = setup();

    let genesis = chain.genesis_block();
    let old_block = build_block(&chain, &genesis, vec![]);

    let mut prev_block = genesis;
    while chain.chain_store().final_head().unwrap().height < old_block.header().height() {
        let block = build_block(&mut chain, &prev_block, vec![]);
        process_block(&mut chain, block.clone());
        prev_block = block;
    }
    assert_eq!(old_block.header().height(), chain.chain_store().final_head().unwrap().height);

    let chunks = old_block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    let endorsement = test_chunk_endorsement(&test_validators()[0], &old_block, chunk_header);
    assert_matches!(
        core_processor.record_chunk_endorsement(endorsement),
        Err(Error::InvalidChunkEndorsement)
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_record_chunk_endorsement_with_unknown_block() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    assert!(core_processor.record_chunk_endorsement(endorsement).is_ok());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_record_chunk_endorsement_does_not_record_result_without_enough_endorsements() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    core_processor.record_chunk_endorsement(endorsement).unwrap();

    let execution_results = core_processor.get_execution_results_by_shard_id(&block).unwrap();
    assert!(!execution_results.contains_key(&chunk_header.shard_id()));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_record_chunk_endorsement_does_not_record_result_with_enough_endorsements() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    for validator in test_validators() {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_processor.record_chunk_endorsement(endorsement).unwrap();
    }

    let execution_results = core_processor.get_execution_results_by_shard_id(&block).unwrap();
    assert!(execution_results.contains_key(&chunk_header.shard_id()));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_record_chunk_endorsement_results_endorsed_notification_is_sent() {
    let (executor_sc, mut executor_rc) = unbounded_channel();
    let (validator_sc, mut validator_rc) = unbounded_channel();
    let (mut chain, core_processor) =
        setup_with_senders(sender_from_channel(executor_sc), sender_from_channel(validator_sc));
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();

    for chunk_header in chunks.iter_raw() {
        assert_matches!(executor_rc.try_recv(), Err(TryRecvError::Empty));
        assert_matches!(validator_rc.try_recv(), Err(TryRecvError::Empty));
        for validator in test_validators() {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            core_processor.record_chunk_endorsement(endorsement).unwrap();
        }
    }

    assert_eq!(
        executor_rc.try_recv().unwrap(),
        ExecutionResultEndorsed { block_hash: *block.hash() }
    );
    assert_matches!(executor_rc.try_recv(), Err(TryRecvError::Empty));

    assert_eq!(
        validator_rc.try_recv().unwrap(),
        ExecutionResultEndorsed { block_hash: *block.hash() }
    );
    assert_matches!(validator_rc.try_recv(), Err(TryRecvError::Empty));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_get_execution_by_shard_id_with_no_execution_results() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    let execution_results = core_processor.get_execution_results_by_shard_id(&block).unwrap();
    assert_eq!(execution_results, HashMap::new());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_get_execution_by_shard_id_with_some_execution_results() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    for validator in test_validators() {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_processor.record_chunk_endorsement(endorsement).unwrap();
    }
    let execution_results = core_processor.get_execution_results_by_shard_id(&block).unwrap();
    assert_eq!(execution_results.len(), 1);
    assert!(execution_results.contains_key(&chunk_header.shard_id()));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_get_execution_by_shard_id_with_all_execution_results() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();

    for chunk_header in chunks.iter_raw() {
        for validator in test_validators() {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            core_processor.record_chunk_endorsement(endorsement).unwrap();
        }
    }
    let execution_results = core_processor.get_execution_results_by_shard_id(&block).unwrap();
    for chunk_header in chunks.iter_raw() {
        assert!(execution_results.contains_key(&chunk_header.shard_id()));
    }
    assert_eq!(execution_results.len(), chunks.len());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_get_block_execution_results_for_genesis() {
    let (chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let execution_results = core_processor.get_block_execution_results(&genesis).unwrap();
    assert!(execution_results.is_some());
    assert_eq!(execution_results.unwrap().0, HashMap::new());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_get_block_execution_results_with_no_execution_results() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let execution_results = core_processor.get_block_execution_results(&block).unwrap();
    assert!(execution_results.is_none())
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_get_block_execution_results_with_some_execution_results_missing() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    for validator in test_validators() {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_processor.record_chunk_endorsement(endorsement).unwrap();
    }
    let execution_results = core_processor.get_block_execution_results(&block).unwrap();
    assert!(execution_results.is_none())
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_get_block_execution_results_with_all_execution_results_present() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();

    for chunk_header in chunks.iter_raw() {
        for validator in test_validators() {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            core_processor.record_chunk_endorsement(endorsement).unwrap();
        }
    }
    let execution_results = core_processor.get_block_execution_results(&block).unwrap();
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
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();

    for chunk_header in chunks.iter_raw() {
        for validator in test_validators() {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            core_processor.record_chunk_endorsement(endorsement).unwrap();
        }
    }
    assert!(core_processor.all_execution_results_exist(&block).unwrap());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_all_execution_results_exist_when_some_are_missing() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    for validator in test_validators() {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_processor.record_chunk_endorsement(endorsement).unwrap();
    }
    assert!(!core_processor.all_execution_results_exist(&block).unwrap());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_for_genesis() {
    let (chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    assert_eq!(core_processor.core_statement_for_next_block(genesis.header()).unwrap(), vec![]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_for_non_spice_block() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_non_spice_block(&mut chain, &genesis);
    process_block(&mut chain, block.clone());
    assert_eq!(core_processor.core_statement_for_next_block(block.header()).unwrap(), vec![]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_when_block_is_not_recorded() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    assert!(core_processor.core_statement_for_next_block(block.header()).is_err());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_contains_no_endorsements() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
    assert_eq!(core_statements, vec![]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_contains_new_endorsements() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    core_processor.record_chunk_endorsement(endorsement.clone()).unwrap();

    let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
    assert_eq!(core_statements.len(), 1);
    let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
        endorsement.spice_destructure().unwrap();
    assert_eq!(
        core_statements[0],
        SpiceCoreStatement::Endorsement {
            chunk_id: SpiceChunkId {
                block_hash: *block.hash(),
                shard_id: chunk_production_key.shard_id,
            },
            account_id,
            endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature }
        }
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_contains_no_endorsements_for_fork_block() {
    let (mut chain, core_processor) = setup();
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
    let endorsement = test_chunk_endorsement(&test_validators()[0], &fork_block, fork_chunk_header);
    core_processor.record_chunk_endorsement(endorsement).unwrap();

    let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
    assert_eq!(core_statements.len(), 0);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_contains_new_endorsement() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    core_processor.record_chunk_endorsement(endorsement.clone()).unwrap();

    let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
    assert_eq!(core_statements.len(), 1);
    let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
        endorsement.spice_destructure().unwrap();
    assert_eq!(
        core_statements[0],
        SpiceCoreStatement::Endorsement {
            chunk_id: SpiceChunkId {
                block_hash: *block.hash(),
                shard_id: chunk_production_key.shard_id,
            },
            account_id,
            endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature }
        }
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_contains_new_execution_results() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    let validators = test_validators();
    for validator in &validators {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_processor.record_chunk_endorsement(endorsement.clone()).unwrap();
    }

    let execution_result = test_execution_result_for_chunk(&chunk_header);
    let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
    assert!(core_statements.contains(&SpiceCoreStatement::ChunkExecutionResult {
        chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id: chunk_header.shard_id() },
        execution_result,
    }));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_with_endorsements_creates_valid_block() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    core_processor.record_chunk_endorsement(endorsement).unwrap();

    let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
    let next_block = build_block(&chain, &block, core_statements);
    assert!(core_processor.validate_core_statements_in_block(&next_block).is_ok());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_with_execution_results_creates_valid_block() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    let validators = test_validators();
    for validator in &validators {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_processor.record_chunk_endorsement(endorsement.clone()).unwrap();
    }

    let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
    let next_block = build_block(&chain, &block, core_statements);
    assert!(core_processor.validate_core_statements_in_block(&next_block).is_ok());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_contains_no_already_included_execution_results() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    let validators = test_validators();
    for validator in &validators {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_processor.record_chunk_endorsement(endorsement.clone()).unwrap();
    }

    let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
    let next_block = build_block(&chain, &block, core_statements);
    process_block(&mut chain, next_block.clone());

    assert_eq!(core_processor.core_statement_for_next_block(next_block.header()).unwrap(), vec![]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_contains_no_already_included_endorsement() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    core_processor.record_chunk_endorsement(endorsement).unwrap();

    let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
    let next_block = build_block(&chain, &block, core_statements);
    process_block(&mut chain, next_block.clone());

    assert_eq!(core_processor.core_statement_for_next_block(next_block.header()).unwrap(), vec![]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_contains_no_endorsements_for_included_execution_result() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    let all_validators = test_validators();
    let (last_validator, validators) = all_validators.split_last().unwrap();
    for validator in validators {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_processor.record_chunk_endorsement(endorsement.clone()).unwrap();
    }

    let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
    let next_block = build_block(&chain, &block, core_statements);
    process_block(&mut chain, next_block.clone());

    let endorsement = test_chunk_endorsement(&last_validator, &block, chunk_header);
    core_processor.record_chunk_endorsement(endorsement).unwrap();

    assert_eq!(core_processor.core_statement_for_next_block(next_block.header()).unwrap(), vec![]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_core_statement_for_next_block_contains_all_endorsements() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    let all_validators = test_validators();
    let mut all_endorsements = Vec::new();
    for validator in all_validators {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_processor.record_chunk_endorsement(endorsement.clone()).unwrap();
        let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
            endorsement.spice_destructure().unwrap();
        all_endorsements.push(SpiceCoreStatement::Endorsement {
            chunk_id: SpiceChunkId {
                block_hash: *block.hash(),
                shard_id: chunk_production_key.shard_id,
            },
            account_id,
            endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
        });
    }

    let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
    for endorsement in &all_endorsements {
        assert!(core_statements.contains(endorsement));
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_record_block_for_non_spice_block() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_non_spice_block(&mut chain, &genesis);
    assert!(core_processor.record_block(&block).is_ok());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_record_block_for_block_without_endorsements() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    assert!(core_processor.record_block(&block).is_ok());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_record_block_for_block_with_endorsements() {
    let (mut chain, core_processor) = setup();
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
            let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                endorsement.spice_destructure().unwrap();
            SpiceCoreStatement::Endorsement {
                chunk_id: SpiceChunkId {
                    block_hash: *block.hash(),
                    shard_id: chunk_production_key.shard_id,
                },
                account_id,
                endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
            }
        })
        .collect_vec();

    let next_block = build_block(&mut chain, &genesis, block_core_statements);
    let store_update = core_processor.record_block(&next_block).unwrap();
    store_update.commit().unwrap();

    assert!(core_processor.get_execution_results_by_shard_id(&block).unwrap().is_empty());
    for validator in in_core_validators {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_processor.record_chunk_endorsement(endorsement).unwrap();
    }
    let execution_results = core_processor.get_execution_results_by_shard_id(&block).unwrap();
    assert!(execution_results.contains_key(&chunk_header.shard_id()));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_record_block_for_block_with_execution_results() {
    let (mut chain, core_processor) = setup();
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
            let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                endorsement.spice_destructure().unwrap();
            SpiceCoreStatement::Endorsement {
                chunk_id: SpiceChunkId {
                    block_hash: *block.hash(),
                    shard_id: chunk_production_key.shard_id,
                },
                account_id,
                endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
            }
        })
        .collect_vec();
    let execution_result = test_execution_result_for_chunk(&chunk_header);
    block_core_statements.push(SpiceCoreStatement::ChunkExecutionResult {
        chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id: chunk_header.shard_id() },
        execution_result: execution_result.clone(),
    });

    let next_block = build_block(&mut chain, &genesis, block_core_statements);
    let store_update = core_processor.record_block(&next_block).unwrap();
    store_update.commit().unwrap();
    let execution_results = core_processor.get_execution_results_by_shard_id(&block).unwrap();
    assert_eq!(execution_results.get(&chunk_header.shard_id()), Some(&Arc::new(execution_result)));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_record_block_processes_pending_endorsements() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    for validator in test_validators() {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_processor.record_chunk_endorsement(endorsement).unwrap();
    }

    assert!(core_processor.get_execution_results_by_shard_id(&block).unwrap().is_empty());
    let store_update = core_processor.record_block(&block).unwrap();
    store_update.commit().unwrap();

    let execution_results = core_processor.get_execution_results_by_shard_id(&block).unwrap();
    assert_eq!(
        execution_results.get(&chunk_header.shard_id()),
        Some(&Arc::new(test_execution_result_for_chunk(&chunk_header)))
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_record_block_processes_pending_endorsements_with_invalid_endorsement() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let all_validators = test_validators();
    let (last_validator, validators) = all_validators.split_last().unwrap();
    for validator in validators {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_processor.record_chunk_endorsement(endorsement).unwrap();
    }
    let invalid_endorsement = invalid_chunk_endorsement(&last_validator, &block, chunk_header);
    core_processor.record_chunk_endorsement(invalid_endorsement).unwrap();

    assert!(core_processor.get_execution_results_by_shard_id(&block).unwrap().is_empty());
    let store_update = core_processor.record_block(&block).unwrap();
    store_update.commit().unwrap();

    let execution_results = core_processor.get_execution_results_by_shard_id(&block).unwrap();
    assert_eq!(
        execution_results.get(&chunk_header.shard_id()),
        Some(&Arc::new(test_execution_result_for_chunk(&chunk_header)))
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_endorsements_from_forks_can_be_used_in_other_forks() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    let (_chunk_production_key, account_id, signed_inner, _execution_result, signature) =
        endorsement.spice_destructure().unwrap();
    let core_endorsement = SpiceCoreStatement::Endorsement {
        chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id: chunk_header.shard_id() },
        account_id,
        endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
    };

    let fork_block = build_block(&mut chain, &block, vec![core_endorsement.clone()]);
    let store_update = core_processor.record_block(&fork_block).unwrap();
    store_update.commit().unwrap();

    let core_statements = core_processor.core_statement_for_next_block(block.header()).unwrap();
    assert_eq!(core_statements, vec![core_endorsement]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_valid_with_non_spice_block() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_non_spice_block(&mut chain, &genesis);
    assert!(core_processor.validate_core_statements_in_block(&block).is_ok());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_with_parent_not_recorded() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    let next_block = build_block(&mut chain, &block, vec![]);
    assert_matches!(
        core_processor.validate_core_statements_in_block(&next_block),
        Err(InvalidSpiceCoreStatementsError::NoPrevUncertifiedChunks)
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_with_endorsement_with_invalid_account_id() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let invalid_validator = "invalid-validator";
    let endorsement = test_chunk_endorsement(invalid_validator, &block, chunk_header);
    let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
        endorsement.spice_destructure().unwrap();
    let core_endorsement = SpiceCoreStatement::Endorsement {
        chunk_id: SpiceChunkId {
            block_hash: *block.hash(),
            shard_id: chunk_production_key.shard_id,
        },
        account_id,
        endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
    };

    let next_block = build_block(&mut chain, &block, vec![core_endorsement]);
    assert_matches!(
        core_processor.validate_core_statements_in_block(&next_block),
        Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
            reason: "endorsement is irrelevant",
            index: 0
        })
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_with_endorsement_with_invalid_signature() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    let (chunk_production_key, account_id, _signed_inner, _execution_result, signature) =
        endorsement.spice_destructure().unwrap();
    let core_endorsement = SpiceCoreStatement::Endorsement {
        chunk_id: SpiceChunkId {
            block_hash: *block.hash(),
            shard_id: chunk_production_key.shard_id,
        },
        account_id,
        endorsement: SpiceEndorsementWithSignature {
            inner: SpiceEndorsementSignedInner {
                block_hash: *block.hash(),
                execution_result_hash: ChunkExecutionResultHash(CryptoHash::default()),
            },
            signature,
        },
    };

    let next_block = build_block(&mut chain, &block, vec![core_endorsement]);
    assert_matches!(
        core_processor.validate_core_statements_in_block(&next_block),
        Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
            reason: "invalid signature",
            index: 0
        })
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_with_endorsement_already_included() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
        endorsement.spice_destructure().unwrap();
    let core_endorsement = SpiceCoreStatement::Endorsement {
        chunk_id: SpiceChunkId {
            block_hash: *block.hash(),
            shard_id: chunk_production_key.shard_id,
        },
        account_id,
        endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
    };

    let next_block = build_block(&mut chain, &block, vec![core_endorsement.clone()]);
    process_block(&mut chain, next_block.clone());

    let next_next_block = build_block(&mut chain, &next_block, vec![core_endorsement]);
    assert_matches!(
        core_processor.validate_core_statements_in_block(&next_next_block),
        Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
            reason: "endorsement is irrelevant",
            index: 0
        })
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_with_endorsement_for_unknown_block() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let next_block = build_block(&mut chain, &block, vec![]);
    let next_block_chunks = next_block.chunks();
    let next_block_chunk_header = next_block_chunks.iter_raw().next().unwrap();

    let endorsement =
        test_chunk_endorsement(&test_validators()[0], &next_block, next_block_chunk_header);
    let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
        endorsement.spice_destructure().unwrap();
    let core_endorsement = SpiceCoreStatement::Endorsement {
        chunk_id: SpiceChunkId {
            block_hash: *next_block.hash(),
            shard_id: chunk_production_key.shard_id,
        },
        account_id,
        endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
    };

    let fork_block = build_block(&mut chain, &block, vec![core_endorsement]);
    assert_matches!(
        core_processor.validate_core_statements_in_block(&fork_block),
        Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
            reason: "endorsement is irrelevant",
            index: 0
        })
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_with_duplicate_endorsement() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
        endorsement.spice_destructure().unwrap();
    let core_endorsement = SpiceCoreStatement::Endorsement {
        chunk_id: SpiceChunkId {
            block_hash: *block.hash(),
            shard_id: chunk_production_key.shard_id,
        },
        account_id,
        endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
    };
    let next_block =
        build_block(&mut chain, &block, vec![core_endorsement.clone(), core_endorsement]);
    assert_matches!(
        core_processor.validate_core_statements_in_block(&next_block),
        Err(InvalidSpiceCoreStatementsError::InvalidCoreStatement {
            reason: "duplicate endorsement",
            index: 1
        })
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_with_duplicate_execution_result() {
    let (mut chain, core_processor) = setup();
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
            let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                endorsement.spice_destructure().unwrap();
            SpiceCoreStatement::Endorsement {
                chunk_id: SpiceChunkId {
                    block_hash: *block.hash(),
                    shard_id: chunk_production_key.shard_id,
                },
                account_id,
                endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
            }
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
    let result = core_processor.validate_core_statements_in_block(&next_block);
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
    let (mut chain, core_processor) = setup();
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
            let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                endorsement.spice_destructure().unwrap();
            SpiceCoreStatement::Endorsement {
                chunk_id: SpiceChunkId {
                    block_hash: *block.hash(),
                    shard_id: chunk_production_key.shard_id,
                },
                account_id,
                endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
            }
        })
        .collect_vec();
    let execution_result = SpiceCoreStatement::ChunkExecutionResult {
        chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id: chunk_header.shard_id() },
        execution_result: test_execution_result_for_chunk(&chunk_header),
    };
    block_core_statements.push(execution_result);

    let next_block = build_block(&mut chain, &block, block_core_statements);
    let result = core_processor.validate_core_statements_in_block(&next_block);
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
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
        endorsement.spice_destructure().unwrap();
    let block_core_statements = vec![
        SpiceCoreStatement::Endorsement {
            chunk_id: SpiceChunkId {
                block_hash: *block.hash(),
                shard_id: chunk_production_key.shard_id,
            },
            account_id,
            endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
        },
        SpiceCoreStatement::ChunkExecutionResult {
            chunk_id: SpiceChunkId {
                block_hash: *block.hash(),
                shard_id: chunk_production_key.shard_id,
            },
            execution_result: test_execution_result_for_chunk(&chunk_header),
        },
    ];

    let next_block = build_block(&mut chain, &block, block_core_statements);
    let result = core_processor.validate_core_statements_in_block(&next_block);
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
    let (mut chain, core_processor) = setup();
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
    let result = core_processor.validate_core_statements_in_block(&next_block);
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
    let (mut chain, core_processor) = setup();
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
            let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                endorsement.spice_destructure().unwrap();
            SpiceCoreStatement::Endorsement {
                chunk_id: SpiceChunkId {
                    block_hash: *block.hash(),
                    shard_id: chunk_production_key.shard_id,
                },
                account_id,
                endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
            }
        })
        .collect_vec();

    let next_block = build_block(&mut chain, &block, block_core_statements);
    let result = core_processor.validate_core_statements_in_block(&next_block);
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
    let (mut chain, core_processor) = setup();
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
            let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                endorsement.spice_destructure().unwrap();
            SpiceCoreStatement::Endorsement {
                chunk_id: SpiceChunkId {
                    block_hash: *block.hash(),
                    shard_id: chunk_production_key.shard_id,
                },
                account_id,
                endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
            }
        })
        .collect_vec();
    block_core_statements.push(SpiceCoreStatement::ChunkExecutionResult {
        chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id: chunk_header.shard_id() },
        execution_result: invalid_execution_result_for_chunk(&chunk_header),
    });
    let execution_result_index = block_core_statements.len() - 1;

    let next_block = build_block(&mut chain, &block, block_core_statements);
    let result = core_processor.validate_core_statements_in_block(&next_block);
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
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let next_block = build_block(&mut chain, &block, vec![]);
    assert_matches!(core_processor.validate_core_statements_in_block(&next_block), Ok(()));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_valid_with_not_enough_on_chain_endorsements_for_execution_result()
 {
    let (mut chain, core_processor) = setup();
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
            let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                endorsement.spice_destructure().unwrap();
            SpiceCoreStatement::Endorsement {
                chunk_id: SpiceChunkId {
                    block_hash: *block.hash(),
                    shard_id: chunk_production_key.shard_id,
                },
                account_id,
                endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
            }
        })
        .collect_vec();

    for validator in in_core_validators {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_processor.record_chunk_endorsement(endorsement).unwrap();
    }
    let next_block = build_block(&mut chain, &block, block_core_statements);
    assert_matches!(core_processor.validate_core_statements_in_block(&next_block), Ok(()));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_valid_endorsements_without_execution_result() {
    let (mut chain, core_processor) = setup();
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
            let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                endorsement.spice_destructure().unwrap();
            SpiceCoreStatement::Endorsement {
                chunk_id: SpiceChunkId {
                    block_hash: *block.hash(),
                    shard_id: chunk_production_key.shard_id,
                },
                account_id,
                endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
            }
        })
        .collect_vec();

    let next_block = build_block(&mut chain, &block, block_core_statements);
    assert_matches!(core_processor.validate_core_statements_in_block(&next_block), Ok(()));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_valid_endorsements_with_execution_result() {
    let (mut chain, core_processor) = setup();
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
            let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                endorsement.spice_destructure().unwrap();
            SpiceCoreStatement::Endorsement {
                chunk_id: SpiceChunkId {
                    block_hash: *block.hash(),
                    shard_id: chunk_production_key.shard_id,
                },
                account_id,
                endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
            }
        })
        .collect_vec();
    block_core_statements.push(SpiceCoreStatement::ChunkExecutionResult {
        chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id: chunk_header.shard_id() },
        execution_result: test_execution_result_for_chunk(&chunk_header),
    });

    let next_block = build_block(&mut chain, &block, block_core_statements);
    assert_matches!(core_processor.validate_core_statements_in_block(&next_block), Ok(()));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_validate_core_statements_in_block_valid_execution_result_with_ancestral_endorsements() {
    let (mut chain, core_processor) = setup();
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
            let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                endorsement.spice_destructure().unwrap();
            SpiceCoreStatement::Endorsement {
                chunk_id: SpiceChunkId {
                    block_hash: *block.hash(),
                    shard_id: chunk_production_key.shard_id,
                },
                account_id,
                endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
            }
        })
        .collect_vec();
    let next_block = build_block(&chain, &block, next_block_core_statements);
    process_block(&mut chain, next_block.clone());

    let mut next_next_block_core_statements = right_validators
        .iter()
        .map(|validator| {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            let (chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                endorsement.spice_destructure().unwrap();
            SpiceCoreStatement::Endorsement {
                chunk_id: SpiceChunkId {
                    block_hash: *block.hash(),
                    shard_id: chunk_production_key.shard_id,
                },
                account_id,
                endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
            }
        })
        .collect_vec();
    next_next_block_core_statements.push(SpiceCoreStatement::ChunkExecutionResult {
        chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id: chunk_header.shard_id() },
        execution_result: test_execution_result_for_chunk(&chunk_header),
    });

    let next_next_block = build_block(&mut chain, &next_block, next_next_block_core_statements);
    assert_matches!(core_processor.validate_core_statements_in_block(&next_next_block), Ok(()));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_send_execution_result_endorsements_with_endorsements_but_without_execution_result() {
    let (executor_sc, mut executor_rc) = unbounded_channel();
    let (validator_sc, mut validator_rc) = unbounded_channel();
    let (mut chain, core_processor) =
        setup_with_senders(sender_from_channel(executor_sc), sender_from_channel(validator_sc));
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    let (_chunk_production_key, account_id, signed_inner, _execution_result, signature) =
        endorsement.spice_destructure().unwrap();
    let block_core_statements = vec![SpiceCoreStatement::Endorsement {
        chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id: chunk_header.shard_id() },
        account_id,
        endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
    }];
    let next_block = build_block(&mut chain, &block, block_core_statements);
    process_block(&mut chain, next_block.clone());
    core_processor.send_execution_result_endorsements(&next_block);
    assert_matches!(executor_rc.try_recv(), Err(TryRecvError::Empty));
    assert_matches!(validator_rc.try_recv(), Err(TryRecvError::Empty));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_send_execution_result_endorsements_with_non_spice_block() {
    let (mut chain, core_processor) = setup();
    let genesis = chain.genesis_block();
    let block = build_non_spice_block(&mut chain, &genesis);
    // We just want to make sure we don't panic.
    core_processor.send_execution_result_endorsements(&block);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_send_execution_result_endorsements_with_endorsements_and_execution_results() {
    let (executor_sc, mut executor_rc) = unbounded_channel();
    let (validator_sc, mut validator_rc) = unbounded_channel();
    let (mut chain, core_processor) =
        setup_with_senders(sender_from_channel(executor_sc), sender_from_channel(validator_sc));
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let mut block_core_statements = Vec::new();

    let all_validators = test_validators();
    for chunk_header in block.chunks().iter_raw() {
        for validator in &all_validators {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            let (_chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                endorsement.spice_destructure().unwrap();
            block_core_statements.push(SpiceCoreStatement::Endorsement {
                chunk_id: SpiceChunkId {
                    block_hash: *block.hash(),
                    shard_id: chunk_header.shard_id(),
                },
                account_id,
                endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
            });
        }
        block_core_statements.push(SpiceCoreStatement::ChunkExecutionResult {
            chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id: chunk_header.shard_id() },
            execution_result: test_execution_result_for_chunk(&chunk_header),
        });
    }
    let next_block = build_block(&mut chain, &block, block_core_statements);
    process_block(&mut chain, next_block.clone());
    core_processor.send_execution_result_endorsements(&next_block);

    let mut executor_notifications = Vec::new();
    while let Ok(event) = executor_rc.try_recv() {
        executor_notifications.push(event);
    }
    assert_eq!(executor_notifications, vec![ExecutionResultEndorsed { block_hash: *block.hash() }]);

    let mut validator_notifications = Vec::new();
    while let Ok(event) = validator_rc.try_recv() {
        validator_notifications.push(event);
    }
    assert_eq!(
        validator_notifications,
        vec![ExecutionResultEndorsed { block_hash: *block.hash() }]
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_send_execution_result_endorsements_with_execution_results_for_several_blocks() {
    let (executor_sc, mut executor_rc) = unbounded_channel();
    let (validator_sc, mut validator_rc) = unbounded_channel();
    let (mut chain, core_processor) =
        setup_with_senders(sender_from_channel(executor_sc), sender_from_channel(validator_sc));
    let genesis = chain.genesis_block();
    let parent_block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, parent_block.clone());
    let current_block = build_block(&mut chain, &parent_block, vec![]);
    process_block(&mut chain, current_block.clone());

    let all_validators = test_validators();

    let mut core_statements = Vec::new();

    for block in [&parent_block, &current_block] {
        for chunk_header in block.chunks().iter_raw() {
            for validator in &all_validators {
                let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
                let (_chunk_production_key, account_id, signed_inner, _execution_result, signature) =
                    endorsement.spice_destructure().unwrap();
                core_statements.push(SpiceCoreStatement::Endorsement {
                    chunk_id: SpiceChunkId {
                        block_hash: *block.hash(),
                        shard_id: chunk_header.shard_id(),
                    },
                    account_id,
                    endorsement: SpiceEndorsementWithSignature { inner: signed_inner, signature },
                });
            }
            core_statements.push(SpiceCoreStatement::ChunkExecutionResult {
                chunk_id: SpiceChunkId {
                    block_hash: *block.hash(),
                    shard_id: chunk_header.shard_id(),
                },
                execution_result: test_execution_result_for_chunk(&chunk_header),
            });
        }
    }

    let next_block = build_block(&mut chain, &current_block, core_statements);
    process_block(&mut chain, next_block.clone());
    core_processor.send_execution_result_endorsements(&next_block);

    let current_block_notification = ExecutionResultEndorsed { block_hash: *current_block.hash() };
    let parent_block_notification = ExecutionResultEndorsed { block_hash: *parent_block.hash() };

    let mut executor_notifications = Vec::new();
    while let Ok(event) = executor_rc.try_recv() {
        executor_notifications.push(event);
    }

    let mut validator_notifications = Vec::new();
    while let Ok(event) = validator_rc.try_recv() {
        validator_notifications.push(event);
    }

    assert_eq!(executor_notifications.len(), 2);
    assert!(executor_notifications.contains(&current_block_notification));
    assert!(executor_notifications.contains(&parent_block_notification));

    assert_eq!(validator_notifications.len(), 2);
    assert!(validator_notifications.contains(&current_block_notification));
    assert!(validator_notifications.contains(&parent_block_notification));
}

fn block_builder(chain: &Chain, prev_block: &Block) -> TestBlockBuilder {
    let block_producer = chain
        .epoch_manager
        .get_block_producer_info(prev_block.header().epoch_id(), prev_block.header().height() + 1)
        .unwrap();
    let signer = Arc::new(create_test_signer(block_producer.account_id().as_str()));
    TestBlockBuilder::new(Clock::real(), prev_block, signer)
        .chunks(get_fake_next_block_chunk_headers(&prev_block, chain.epoch_manager.as_ref()))
}

fn build_non_spice_block(chain: &Chain, prev_block: &Block) -> Arc<Block> {
    block_builder(chain, prev_block).build()
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

fn sender_from_channel<T: Send>(sc: UnboundedSender<T>) -> Sender<T> {
    Sender::from_fn(move |event| {
        sc.send(event).unwrap();
    })
}

fn setup() -> (Chain, CoreStatementsProcessor) {
    setup_with_senders(noop().into_sender(), noop().into_sender())
}

fn setup_with_senders(
    chunk_executor_sender: Sender<ExecutionResultEndorsed>,
    spice_chunk_validator_sender: Sender<ExecutionResultEndorsed>,
) -> (Chain, CoreStatementsProcessor) {
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

    let mut chain = get_chain_with_genesis(Clock::real(), genesis);
    let core_processor = CoreStatementsProcessor::new(
        chain.chain_store().chain_store(),
        chain.epoch_manager.clone(),
        chunk_executor_sender,
        spice_chunk_validator_sender,
    );
    chain.spice_core_processor = core_processor.clone();
    (chain, core_processor)
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

fn invalid_chunk_endorsement(
    validator: &str,
    block: &Block,
    chunk_header: &ShardChunkHeader,
) -> ChunkEndorsement {
    let execution_result = invalid_execution_result_for_chunk(chunk_header);
    let epoch_id = block.header().epoch_id();
    let signer = create_test_signer(&validator);
    ChunkEndorsement::new_with_execution_result(
        *epoch_id,
        execution_result,
        *block.hash(),
        chunk_header.shard_id(),
        chunk_header.height_created(),
        &signer,
    )
}

fn test_chunk_endorsement(
    validator: &str,
    block: &Block,
    chunk_header: &ShardChunkHeader,
) -> ChunkEndorsement {
    let execution_result = test_execution_result_for_chunk(chunk_header);
    let epoch_id = block.header().epoch_id();
    let signer = create_test_signer(&validator);
    ChunkEndorsement::new_with_execution_result(
        *epoch_id,
        execution_result,
        *block.hash(),
        chunk_header.shard_id(),
        chunk_header.height_created(),
        &signer,
    )
}

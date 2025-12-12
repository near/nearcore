use assert_matches::assert_matches;
use itertools::Itertools as _;
use near_async::messaging::{IntoSender as _, Sender, noop};
use near_async::time::Clock;
use near_chain_configs::Genesis;
use near_chain_configs::test_genesis::{TestGenesisBuilder, ValidatorsSpec};
use near_crypto::Signature;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::Block;
use near_primitives::block_body::SpiceCoreStatement;
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::stateless_validation::spice_chunk_endorsement::testonly_create_chunk_endorsement;
use near_primitives::stateless_validation::spice_chunk_endorsement::{
    SpiceChunkEndorsement, SpiceVerifiedEndorsement,
};
use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{AccountId, ChunkExecutionResult, ShardId, SpiceChunkId};
use near_store::adapter::StoreAdapter as _;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

use crate::spice_core::SpiceCoreReader;
use crate::spice_core_writer_actor::{
    ExecutionResultEndorsed, InvalidSpiceEndorsementError, ProcessChunkError, SpiceCoreWriterActor,
};
use crate::test_utils::{
    get_chain_with_genesis, get_fake_next_block_chunk_headers, process_block_sync,
};
use crate::{BlockProcessingArtifact, Chain, Provenance};

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_process_chunk_endorsement_with_execution_result_already_present() {
    let (mut chain, core_writer_actor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    for validator in test_validators() {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_writer_actor.process_chunk_endorsement(endorsement).unwrap();
    }
    let execution_results =
        core_writer_actor.core_reader.get_execution_results_by_shard_id(block.header()).unwrap();
    assert!(execution_results.contains_key(&chunk_header.shard_id()));

    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    assert!(core_writer_actor.process_chunk_endorsement(endorsement).is_ok());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_process_chunk_endorsement_is_ok_with_unknown_block() {
    let (mut chain, core_writer_actor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();
    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    assert!(core_writer_actor.process_chunk_endorsement(endorsement).is_ok());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_process_chunk_endorsement_does_not_record_result_without_enough_endorsements() {
    let (mut chain, core_writer_actor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    core_writer_actor.process_chunk_endorsement(endorsement).unwrap();

    let execution_results =
        core_writer_actor.core_reader.get_execution_results_by_shard_id(block.header()).unwrap();
    assert!(!execution_results.contains_key(&chunk_header.shard_id()));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_process_chunk_endorsement_does_not_record_result_with_enough_endorsements() {
    let (mut chain, core_writer_actor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    for validator in test_validators() {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_writer_actor.process_chunk_endorsement(endorsement).unwrap();
    }

    let execution_results =
        core_writer_actor.core_reader.get_execution_results_by_shard_id(block.header()).unwrap();
    assert!(execution_results.contains_key(&chunk_header.shard_id()));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_process_chunk_endorsement_results_endorsed_notification_is_sent() {
    let (executor_sc, mut executor_rc) = unbounded_channel();
    let (validator_sc, mut validator_rc) = unbounded_channel();
    let (mut chain, core_writer_actor) =
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
            core_writer_actor.process_chunk_endorsement(endorsement).unwrap();
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
fn test_process_chunk_endorsement_fails_with_irrelevant_endorsement() {
    // We use a lot of validators to make sure that not all are required to validate all shards.
    let validators = (0..100).map(|i| format!("test{i}")).collect_vec();
    let num_shards = 3;
    let sharding_version = 3;
    let (mut chain, core_writer_actor) = setup_with_genesis(
        TestGenesisBuilder::new()
            .shard_layout(ShardLayout::multi_shard(num_shards, sharding_version))
            .validators_spec(ValidatorsSpec::desired_roles(
                &["producer"],
                &validators.iter().map(String::as_str).collect_vec(),
            ))
            .build(),
    );

    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let irrelevant_validator =
        find_irrelevant_validator(&validators, chain, &block, chunk_header.shard_id());
    let endorsement = test_chunk_endorsement(&irrelevant_validator, &block, chunk_header);
    assert_matches!(
        core_writer_actor.process_chunk_endorsement(endorsement),
        Err(ProcessChunkError::InvalidEndorsement(
            InvalidSpiceEndorsementError::EndorsementIsNotRelevant
        ))
    );
    assert!(
        !core_writer_actor
            .core_reader
            .endorsement_exists(
                block.hash(),
                chunk_header.shard_id(),
                &AccountId::from_str(&irrelevant_validator).unwrap()
            )
            .unwrap()
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_process_chunk_endorsement_fails_bad_account_id() {
    let (mut chain, core_writer_actor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let endorsement = test_chunk_endorsement("bad-account", &block, chunk_header);
    assert_matches!(
        core_writer_actor.process_chunk_endorsement(endorsement),
        Err(ProcessChunkError::InvalidEndorsement(
            InvalidSpiceEndorsementError::AccountIsNotValidator
        ))
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_process_chunk_endorsement_fails_bad_shard_id() {
    let (mut chain, core_writer_actor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let execution_result = test_execution_result_for_chunk(chunk_header);
    let signer = create_test_signer(&test_validators()[0]);
    let endorsement = SpiceChunkEndorsement::new(
        SpiceChunkId { block_hash: *block.hash(), shard_id: ShardId::new(42) },
        execution_result,
        &signer,
    );
    assert_matches!(
        core_writer_actor.process_chunk_endorsement(endorsement),
        Err(ProcessChunkError::InvalidEndorsement(InvalidSpiceEndorsementError::InvalidShardId))
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_process_chunk_endorsement_fails_bad_signature() {
    let (mut chain, core_writer_actor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let execution_result = test_execution_result_for_chunk(chunk_header);
    let endorsement = testonly_create_chunk_endorsement(
        SpiceChunkId { block_hash: *block.hash(), shard_id: chunk_header.shard_id() },
        AccountId::from_str(&test_validators()[0]).unwrap(),
        Signature::default(),
        execution_result,
    );
    assert_matches!(
        core_writer_actor.process_chunk_endorsement(endorsement),
        Err(ProcessChunkError::InvalidEndorsement(InvalidSpiceEndorsementError::InvalidSignature))
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_process_chunk_endorsement_fails_with_unknown_block_and_bad_signature() {
    let (mut chain, core_writer_actor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let execution_result = test_execution_result_for_chunk(chunk_header);
    let endorsement = testonly_create_chunk_endorsement(
        SpiceChunkId { block_hash: *block.hash(), shard_id: chunk_header.shard_id() },
        AccountId::from_str(&test_validators()[0]).unwrap(),
        Signature::default(),
        execution_result,
    );
    assert_matches!(
        core_writer_actor.process_chunk_endorsement(endorsement),
        Err(ProcessChunkError::InvalidPendingEndorsement(
            InvalidSpiceEndorsementError::InvalidSignature
        ))
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_process_chunk_endorsement_fails_with_unknown_block_and_non_validator_account() {
    let (mut chain, core_writer_actor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let endorsement = test_chunk_endorsement("bad-account", &block, chunk_header);
    assert_matches!(
        core_writer_actor.process_chunk_endorsement(endorsement),
        Err(ProcessChunkError::InvalidPendingEndorsement(
            InvalidSpiceEndorsementError::AccountIsNotValidator
        ))
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_process_chunk_endorsement_fails_with_unknown_block_and_wrong_shard_id() {
    let (mut chain, core_writer_actor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let execution_result = test_execution_result_for_chunk(chunk_header);
    let signer = create_test_signer(&test_validators()[0]);
    let endorsement = SpiceChunkEndorsement::new(
        SpiceChunkId { block_hash: *block.hash(), shard_id: ShardId::new(42) },
        execution_result,
        &signer,
    );
    assert_matches!(
        core_writer_actor.process_chunk_endorsement(endorsement),
        Err(ProcessChunkError::InvalidPendingEndorsement(
            InvalidSpiceEndorsementError::InvalidShardId
        ))
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_handle_processed_block_for_non_spice_block() {
    let (mut chain, core_writer_actor) = setup();
    let genesis = chain.genesis_block();
    let block = build_non_spice_block(&mut chain, &genesis);
    process_block(&mut chain, block.clone());
    assert!(core_writer_actor.handle_processed_block(*block.hash()).is_ok());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_handle_processed_block_for_block_without_endorsements() {
    let (mut chain, core_writer_actor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    assert!(core_writer_actor.handle_processed_block(*block.hash()).is_ok());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_handle_processed_block_for_block_with_endorsements() {
    let (mut chain, core_writer_actor) = setup();
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

    let next_block = build_block(&mut chain, &block, block_core_statements);
    process_block(&mut chain, next_block.clone());
    core_writer_actor.handle_processed_block(*next_block.hash()).unwrap();

    assert!(
        core_writer_actor
            .core_reader
            .get_execution_results_by_shard_id(block.header())
            .unwrap()
            .is_empty()
    );
    for validator in in_core_validators {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_writer_actor.process_chunk_endorsement(endorsement).unwrap();
    }
    let execution_results =
        core_writer_actor.core_reader.get_execution_results_by_shard_id(block.header()).unwrap();
    assert!(execution_results.contains_key(&chunk_header.shard_id()));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_handle_processed_block_for_block_with_final_endorsement_and_no_execution_result() {
    let (mut chain, core_writer_actor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let all_validators = test_validators();
    let (in_block_validators, in_core_validators) =
        all_validators.split_at(all_validators.len() / 2);
    assert!(in_block_validators.len() >= in_core_validators.len());

    for validator in in_core_validators {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_writer_actor.process_chunk_endorsement(endorsement).unwrap();
    }
    assert!(
        core_writer_actor
            .core_reader
            .get_execution_results_by_shard_id(block.header())
            .unwrap()
            .is_empty()
    );

    let block_core_statements = in_block_validators
        .iter()
        .map(|validator| {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            endorsement_into_core_statement(endorsement)
        })
        .collect_vec();
    let next_block = build_block(&mut chain, &block, block_core_statements);

    process_block(&mut chain, next_block.clone());
    core_writer_actor.handle_processed_block(*next_block.hash()).unwrap();

    let execution_results =
        core_writer_actor.core_reader.get_execution_results_by_shard_id(block.header()).unwrap();
    assert!(execution_results.contains_key(&chunk_header.shard_id()));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_handle_processed_block_for_block_with_execution_results() {
    let (mut chain, core_writer_actor) = setup();
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
        execution_result: execution_result.clone(),
    });

    let next_block = build_block(&mut chain, &block, block_core_statements);
    process_block(&mut chain, next_block.clone());
    core_writer_actor.handle_processed_block(*next_block.hash()).unwrap();
    let execution_results =
        core_writer_actor.core_reader.get_execution_results_by_shard_id(block.header()).unwrap();
    assert_eq!(execution_results.get(&chunk_header.shard_id()), Some(&Arc::new(execution_result)));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_handle_processed_block_processes_pending_endorsements() {
    let (mut chain, core_writer_actor) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    for validator in test_validators() {
        let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
        core_writer_actor.process_chunk_endorsement(endorsement).unwrap();
    }

    assert!(
        core_writer_actor
            .core_reader
            .get_execution_results_by_shard_id(block.header())
            .unwrap()
            .is_empty()
    );
    process_block(&mut chain, block.clone());
    core_writer_actor.handle_processed_block(*block.hash()).unwrap();

    let execution_results =
        core_writer_actor.core_reader.get_execution_results_by_shard_id(block.header()).unwrap();
    assert_eq!(
        execution_results.get(&chunk_header.shard_id()),
        Some(&Arc::new(test_execution_result_for_chunk(&chunk_header)))
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_handle_processed_block_processes_pending_endorsements_with_invalid_endorsement() {
    // We use a lot of validators to make sure that not all are required to validate all shards.
    let validators = (0..100).map(|i| format!("test{i}")).collect_vec();
    let num_shards = 3;
    let sharding_version = 3;
    let (mut chain, core_writer_actor) = setup_with_genesis(
        TestGenesisBuilder::new()
            .shard_layout(ShardLayout::multi_shard(num_shards, sharding_version))
            .validators_spec(ValidatorsSpec::desired_roles(
                &["producer"],
                &validators.iter().map(String::as_str).collect_vec(),
            ))
            .build(),
    );
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    for validator in &validators {
        let endorsement = test_chunk_endorsement(validator, &block, chunk_header);
        core_writer_actor.process_chunk_endorsement(endorsement).unwrap();
    }

    assert!(
        core_writer_actor
            .core_reader
            .get_execution_results_by_shard_id(block.header())
            .unwrap()
            .is_empty()
    );
    process_block(&mut chain, block.clone());
    core_writer_actor.handle_processed_block(*block.hash()).unwrap();

    let execution_results =
        core_writer_actor.core_reader.get_execution_results_by_shard_id(block.header()).unwrap();
    assert_eq!(
        execution_results.get(&chunk_header.shard_id()),
        Some(&Arc::new(test_execution_result_for_chunk(&chunk_header)))
    );

    let irrelevant_validator =
        find_irrelevant_validator(&validators, chain, &block, chunk_header.shard_id());
    assert!(
        !core_writer_actor
            .core_reader
            .endorsement_exists(
                block.hash(),
                chunk_header.shard_id(),
                &AccountId::from_str(&irrelevant_validator).unwrap()
            )
            .unwrap()
    )
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_send_execution_result_endorsements_with_endorsements_but_without_execution_result() {
    let (executor_sc, mut executor_rc) = unbounded_channel();
    let (validator_sc, mut validator_rc) = unbounded_channel();
    let (mut chain, core_writer_actor) =
        setup_with_senders(sender_from_channel(executor_sc), sender_from_channel(validator_sc));
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let chunks = block.chunks();
    let chunk_header = chunks.iter_raw().next().unwrap();

    let endorsement = test_chunk_endorsement(&test_validators()[0], &block, chunk_header);
    let block_core_statements = vec![endorsement_into_core_statement(endorsement)];
    let next_block = build_block(&mut chain, &block, block_core_statements);
    process_block(&mut chain, next_block.clone());
    core_writer_actor.handle_processed_block(*next_block.hash()).unwrap();
    assert_matches!(executor_rc.try_recv(), Err(TryRecvError::Empty));
    assert_matches!(validator_rc.try_recv(), Err(TryRecvError::Empty));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_send_execution_result_endorsements_with_endorsements_and_execution_results() {
    let (executor_sc, mut executor_rc) = unbounded_channel();
    let (validator_sc, mut validator_rc) = unbounded_channel();
    let (mut chain, core_writer_actor) =
        setup_with_senders(sender_from_channel(executor_sc), sender_from_channel(validator_sc));
    let genesis = chain.genesis_block();
    let block = build_block(&mut chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());

    let mut block_core_statements = Vec::new();

    let all_validators = test_validators();
    for chunk_header in block.chunks().iter_raw() {
        for validator in &all_validators {
            let endorsement = test_chunk_endorsement(&validator, &block, chunk_header);
            block_core_statements.push(endorsement_into_core_statement(endorsement));
        }
        block_core_statements.push(SpiceCoreStatement::ChunkExecutionResult {
            chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id: chunk_header.shard_id() },
            execution_result: test_execution_result_for_chunk(&chunk_header),
        });
    }
    let next_block = build_block(&mut chain, &block, block_core_statements);
    process_block(&mut chain, next_block.clone());
    core_writer_actor.handle_processed_block(*next_block.hash()).unwrap();

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
    let (mut chain, core_writer_actor) =
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
                core_statements.push(endorsement_into_core_statement(endorsement));
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
    core_writer_actor.handle_processed_block(*next_block.hash()).unwrap();

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
    TestBlockBuilder::from_prev_block(Clock::real(), prev_block, signer)
        .chunks(get_fake_next_block_chunk_headers(&prev_block, chain.epoch_manager.as_ref()))
}

fn build_non_spice_block(chain: &Chain, prev_block: &Block) -> Arc<Block> {
    block_builder(chain, prev_block).non_spice_block().build()
}

fn build_block(
    chain: &Chain,
    prev_block: &Block,
    spice_core_statements: Vec<SpiceCoreStatement>,
) -> Arc<Block> {
    block_builder(chain, prev_block).spice_core_statements(spice_core_statements).build()
}

#[track_caller]
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

fn setup() -> (Chain, SpiceCoreWriterActor) {
    setup_with_senders(noop().into_sender(), noop().into_sender())
}

fn core_reader(chain: &Chain) -> SpiceCoreReader {
    SpiceCoreReader::new(
        chain.chain_store().chain_store(),
        chain.epoch_manager.clone(),
        Gas::from_teragas(100),
    )
}

fn setup_with_senders(
    chunk_executor_sender: Sender<ExecutionResultEndorsed>,
    spice_chunk_validator_sender: Sender<ExecutionResultEndorsed>,
) -> (Chain, SpiceCoreWriterActor) {
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
    let core_writer_actor = SpiceCoreWriterActor::new(
        chain.chain_store().chain_store(),
        chain.epoch_manager.clone(),
        core_reader(&chain),
        chunk_executor_sender,
        spice_chunk_validator_sender,
    );
    (chain, core_writer_actor)
}

fn setup_with_genesis(genesis: Genesis) -> (Chain, SpiceCoreWriterActor) {
    let chain = get_chain_with_genesis(Clock::real(), genesis);
    let core_writer_actor = SpiceCoreWriterActor::new(
        chain.chain_store().chain_store(),
        chain.epoch_manager.clone(),
        core_reader(&chain),
        noop().into_sender(),
        noop().into_sender(),
    );
    (chain, core_writer_actor)
}

fn test_execution_result_for_chunk(chunk_header: &ShardChunkHeader) -> ChunkExecutionResult {
    ChunkExecutionResult {
        // Using chunk_hash makes sure that each chunk has a different execution result.
        chunk_extra: ChunkExtra::new_with_only_state_root(&chunk_header.chunk_hash().0),
        outgoing_receipts_root: CryptoHash::default(),
    }
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

fn find_irrelevant_validator(
    validators: &[String],
    chain: Chain,
    block: &Block,
    shard_id: ShardId,
) -> String {
    let chunk_validator_assignments = chain
        .epoch_manager
        .get_chunk_validator_assignments(
            &block.header().epoch_id(),
            shard_id,
            block.header().height(),
        )
        .unwrap();
    let irrelevant_validator = validators
        .iter()
        .find(|validator| {
            !chunk_validator_assignments.contains(&AccountId::from_str(validator).unwrap())
        })
        .unwrap();
    irrelevant_validator.clone()
}

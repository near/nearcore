use super::*;
use crate::runtime::apply_chunk_test_utils::{
    TestApplyChunkParams, TestEnv, TestEnvConfig, test_apply_new_chunk_impl,
    test_apply_new_chunk_setup,
};
use crate::types::{ChainConfig, RuntimeStorageConfig};
use crate::{Chain, ChainGenesis, ChainStoreAccess, DoomslugThresholdMode};
use assert_matches::assert_matches;
use near_async::messaging::{IntoMultiSender, noop};
use near_async::time::Clock;
use near_chain_configs::test_utils::{TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
use near_chain_configs::{
    Genesis, MutableConfigValue, NEAR_BASE, default_produce_chunk_add_transactions_time_limit,
};
use near_crypto::{InMemorySigner, Signer};
use near_epoch_manager::EpochManager;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_o11y::testonly::init_test_logger;
use near_pool::{InsertTransactionResult, PoolIteratorWrapper, TransactionPool};
use near_primitives::action::FunctionCallAction;
use near_primitives::block::Tip;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_info::RngSeed;
use near_primitives::receipt::{ActionReceipt, ReceiptV1};
use near_primitives::state::PartialState;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::test_utils::create_test_signer;
use near_primitives::transaction::{Action, DeleteAccountAction, StakeAction, TransferAction};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{Nonce, ValidatorInfoIdentifier, ValidatorKickoutReason};
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{
    CurrentEpochValidatorInfo, EpochValidatorInfo, NextEpochValidatorInfo, ValidatorKickoutView,
};
use near_store::genesis::initialize_genesis_state;
use near_store::trie::AccessOptions;
use near_store::{PartialStorage, get_genesis_state_roots};
use near_vm_runner::{
    CompiledContract, CompiledContractInfo, FilesystemContractRuntimeCache, get_contract_cache_key,
};
use rand::{SeedableRng, rngs::StdRng, seq::SliceRandom};
use std::collections::HashSet;

#[test]
fn test_apply_new_chunk() {
    let params =
        TestApplyChunkParams { num_txs_per_chunk: 1000, num_shards: 4, num_accounts: 1000 };
    let setup = test_apply_new_chunk_setup(params);
    let verbose = true;
    test_apply_new_chunk_impl(&setup, verbose);
}

/// Start with 2 validators with default stake X.
/// 1. Validator 0 stakes 2 * X
/// 2. Validator 0 creates new account Validator 2 with 3 * X in balance
/// 3. Validator 2 stakes 2 * X
/// 4. Validator 1 gets unstaked because not enough stake.
/// 5. At the end Validator 0 and 2 with 2 * X are validators. Validator 1 has stake returned to balance.
#[test]
fn test_validator_rotation() {
    init_test_logger();
    let num_nodes = 2;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new(vec![validators.clone()], 2, false);
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();
    let signer = InMemorySigner::test_signer(&validators[0]);
    // test1 doubles stake and the new account stakes the same, so test2 will be kicked out.`
    let staking_transaction = stake(1, &signer, &block_producers[0], TESTING_INIT_STAKE * 2);
    let new_account = AccountId::try_from(format!("test{}", num_nodes + 1)).unwrap();
    let new_validator = create_test_signer(new_account.as_str());
    let new_signer: Signer = InMemorySigner::test_signer(&new_account);
    let create_account_transaction = SignedTransaction::create_account(
        2,
        block_producers[0].validator_id().clone(),
        new_account,
        TESTING_INIT_STAKE * 3,
        new_signer.public_key(),
        &signer,
        CryptoHash::default(),
    );
    let test2_stake_amount = 3600 * NEAR_BASE;
    let transactions = {
        // With the new validator selection algorithm, test2 needs to have less stake to
        // become a fisherman.
        let signer = InMemorySigner::test_signer(&validators[1]);
        vec![
            staking_transaction,
            create_account_transaction,
            stake(1, &signer, &block_producers[1], test2_stake_amount),
        ]
    };
    env.step_default(transactions);
    env.step_default(vec![]);
    let account = env.view_account(block_producers[0].validator_id());
    assert_eq!(account.locked, 2 * TESTING_INIT_STAKE);
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE * 5);

    let stake_transaction =
        stake(env.head.height * 1_000_000, &new_signer, &new_validator, TESTING_INIT_STAKE * 2);
    env.step_default(vec![stake_transaction]);
    env.step_default(vec![]);

    // Roll steps for 3 epochs to pass.
    for _ in 5..=9 {
        env.step_default(vec![]);
    }

    let epoch_id =
        env.epoch_manager.get_epoch_id_from_prev_block(&env.head.last_block_hash).unwrap();
    assert_eq!(
        env.epoch_manager
            .get_epoch_block_producers_ordered(&epoch_id)
            .unwrap()
            .iter()
            .map(|x| x.account_id().clone())
            .collect::<HashSet<_>>(),
        vec!["test3".parse().unwrap(), "test1".parse().unwrap()]
            .into_iter()
            .collect::<HashSet<_>>()
    );

    let test1_acc = env.view_account(&"test1".parse().unwrap());
    // Staked 2 * X, sent 3 * X to test3.
    assert_eq!(
        (test1_acc.amount, test1_acc.locked),
        (TESTING_INIT_BALANCE - 5 * TESTING_INIT_STAKE, 2 * TESTING_INIT_STAKE)
    );
    let test2_acc = env.view_account(&"test2".parse().unwrap());
    // Become fishermen instead
    assert_eq!((test2_acc.amount, test2_acc.locked), (TESTING_INIT_BALANCE, 0));
    let test3_acc = env.view_account(&"test3".parse().unwrap());
    // Got 3 * X, staking 2 * X of them.
    assert_eq!((test3_acc.amount, test3_acc.locked), (TESTING_INIT_STAKE, 2 * TESTING_INIT_STAKE));
}

/// One validator tries to decrease their stake in epoch T. Make sure that the stake return happens in epoch T+3.
#[test]
fn test_validator_stake_change() {
    let num_nodes = 2;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new(vec![validators.clone()], 2, false);
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();
    let signer = InMemorySigner::test_signer(&validators[0]);

    let desired_stake = 2 * TESTING_INIT_STAKE / 3;
    let staking_transaction = stake(1, &signer, &block_producers[0], desired_stake);
    env.step_default(vec![staking_transaction]);
    let account = env.view_account(block_producers[0].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
    assert_eq!(account.locked, TESTING_INIT_STAKE);
    for _ in 2..=4 {
        env.step_default(vec![]);
    }

    let account = env.view_account(block_producers[0].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
    assert_eq!(account.locked, TESTING_INIT_STAKE);

    for _ in 5..=7 {
        env.step_default(vec![]);
    }

    let account = env.view_account(block_producers[0].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE - desired_stake);
    assert_eq!(account.locked, desired_stake);
}

#[test]
fn test_validator_stake_change_multiple_times() {
    init_test_logger();
    let num_nodes = 4;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new(vec![validators.clone()], 4, false);
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();
    let signers: Vec<_> = validators.iter().map(|id| InMemorySigner::test_signer(&id)).collect();

    let staking_transaction = stake(1, &signers[0], &block_producers[0], TESTING_INIT_STAKE - 1);
    let staking_transaction1 = stake(2, &signers[0], &block_producers[0], TESTING_INIT_STAKE - 2);
    let staking_transaction2 = stake(1, &signers[1], &block_producers[1], TESTING_INIT_STAKE + 1);
    env.step_default(vec![staking_transaction, staking_transaction1, staking_transaction2]);
    let account = env.view_account(block_producers[0].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
    assert_eq!(account.locked, TESTING_INIT_STAKE);

    let staking_transaction = stake(3, &signers[0], &block_producers[0], TESTING_INIT_STAKE + 1);
    let staking_transaction1 = stake(2, &signers[1], &block_producers[1], TESTING_INIT_STAKE + 2);
    let staking_transaction2 = stake(3, &signers[1], &block_producers[1], TESTING_INIT_STAKE - 1);
    let staking_transaction3 = stake(1, &signers[3], &block_producers[3], TESTING_INIT_STAKE - 1);
    env.step_default(vec![
        staking_transaction,
        staking_transaction1,
        staking_transaction2,
        staking_transaction3,
    ]);

    for _ in 3..=8 {
        env.step_default(vec![]);
    }

    let account = env.view_account(block_producers[0].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1);
    assert_eq!(account.locked, TESTING_INIT_STAKE + 1);

    let account = env.view_account(block_producers[1].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
    assert_eq!(account.locked, TESTING_INIT_STAKE);

    let account = env.view_account(block_producers[2].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
    assert_eq!(account.locked, TESTING_INIT_STAKE);

    let account = env.view_account(block_producers[3].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
    assert_eq!(account.locked, TESTING_INIT_STAKE);

    for _ in 9..=12 {
        env.step_default(vec![]);
    }

    let account = env.view_account(block_producers[0].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1);
    assert_eq!(account.locked, TESTING_INIT_STAKE + 1);

    let account = env.view_account(block_producers[1].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
    assert_eq!(account.locked, TESTING_INIT_STAKE);

    let account = env.view_account(block_producers[2].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
    assert_eq!(account.locked, TESTING_INIT_STAKE);

    let account = env.view_account(block_producers[3].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
    assert_eq!(account.locked, TESTING_INIT_STAKE);

    for _ in 13..=16 {
        env.step_default(vec![]);
    }

    let account = env.view_account(block_producers[0].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1);
    assert_eq!(account.locked, TESTING_INIT_STAKE + 1);

    let account = env.view_account(block_producers[1].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE + 1);
    assert_eq!(account.locked, TESTING_INIT_STAKE - 1);

    let account = env.view_account(block_producers[2].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
    assert_eq!(account.locked, TESTING_INIT_STAKE);

    let account = env.view_account(block_producers[3].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE + 1);
    assert_eq!(account.locked, TESTING_INIT_STAKE - 1);
}

#[test]
fn test_stake_in_last_block_of_an_epoch() {
    init_test_logger();
    let num_nodes = 4;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new(vec![validators.clone()], 5, false);
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();
    let signers: Vec<_> = validators.iter().map(|id| InMemorySigner::test_signer(&id)).collect();
    let staking_transaction =
        stake(1, &signers[0], &block_producers[0], TESTING_INIT_STAKE + TESTING_INIT_STAKE / 6);
    env.step_default(vec![staking_transaction]);
    for _ in 2..10 {
        env.step_default(vec![]);
    }
    let staking_transaction =
        stake(2, &signers[0], &block_producers[0], TESTING_INIT_STAKE + TESTING_INIT_STAKE / 2);
    env.step_default(vec![staking_transaction]);
    env.step_default(vec![]);
    let staking_transaction = stake(3, &signers[0], &block_producers[0], TESTING_INIT_STAKE);
    env.step_default(vec![staking_transaction]);
    for _ in 13..=16 {
        env.step_default(vec![]);
    }
    let account = env.view_account(block_producers[0].validator_id());
    let return_stake = (TESTING_INIT_STAKE + TESTING_INIT_STAKE / 2)
        - (TESTING_INIT_STAKE + TESTING_INIT_STAKE / 6);
    assert_eq!(
        account.amount,
        TESTING_INIT_BALANCE - (TESTING_INIT_STAKE + TESTING_INIT_STAKE / 2) + return_stake
    );
    assert_eq!(account.locked, TESTING_INIT_STAKE + TESTING_INIT_STAKE / 2 - return_stake);
}

#[test]
fn test_verify_validator_signature() {
    let validators =
        (0..2).map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap()).collect::<Vec<_>>();
    let env = TestEnv::new(vec![validators.clone()], 2, true);
    let data = [0; 32];
    let signer = InMemorySigner::test_signer(&validators[0]);
    let signature = signer.sign(&data);
    assert!(
        env.epoch_manager
            .verify_validator_signature(&env.head.epoch_id, &validators[0], &data, &signature)
            .unwrap()
    );
}

// TODO (#7327): enable test when flat storage will support state sync.
#[ignore]
#[test]
fn test_state_sync() {
    init_test_logger();
    let num_nodes = 2;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new(vec![validators.clone()], 2, false);
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();
    let signer = InMemorySigner::test_signer(&validators[0]);
    let staking_transaction = stake(1, &signer, &block_producers[0], TESTING_INIT_STAKE + 1);
    env.step_default(vec![staking_transaction]);
    env.step_default(vec![]);
    let block_hash = hash(&[env.head.height as u8]);

    let shard_layout = env.epoch_manager.get_shard_layout(&env.head.epoch_id).unwrap();
    let shard_id = shard_layout.shard_ids().next().unwrap();

    let state_part = env
        .runtime
        .obtain_state_part(shard_id, &block_hash, &env.state_roots[0], PartId::new(0, 1))
        .unwrap();
    let root_node =
        env.runtime.get_state_root_node(shard_id, &block_hash, &env.state_roots[0]).unwrap();
    let mut new_env = TestEnv::new(vec![validators], 2, false);
    for i in 1..=2 {
        let prev_hash = hash(&[new_env.head.height as u8]);
        let cur_hash = hash(&[(new_env.head.height + 1) as u8]);
        let proposals = if i == 1 {
            vec![ValidatorStake::new(
                block_producers[0].validator_id().clone(),
                block_producers[0].public_key(),
                TESTING_INIT_STAKE + 1,
            )]
        } else {
            vec![]
        };
        new_env
            .epoch_manager
            .add_validator_proposals(
                BlockInfo::new(
                    cur_hash,
                    i,
                    i.saturating_sub(2),
                    prev_hash,
                    prev_hash,
                    new_env.last_proposals,
                    vec![true],
                    new_env.runtime.genesis_config.total_supply,
                    new_env.runtime.genesis_config.protocol_version,
                    new_env.time,
                    None,
                ),
                [0; 32].as_ref().try_into().unwrap(),
            )
            .unwrap()
            .commit()
            .unwrap();
        new_env.head.height = i;
        new_env.head.last_block_hash = cur_hash;
        new_env.head.prev_block_hash = prev_hash;
        new_env.last_proposals = proposals;
        new_env.time += 10u64.pow(9);
    }
    assert!(new_env.runtime.validate_state_root_node(&root_node, &env.state_roots[0]));
    let mut root_node_wrong = root_node;
    root_node_wrong.memory_usage += 1;
    assert!(!new_env.runtime.validate_state_root_node(&root_node_wrong, &env.state_roots[0]));
    root_node_wrong.data = std::sync::Arc::new([123]);
    assert!(!new_env.runtime.validate_state_root_node(&root_node_wrong, &env.state_roots[0]));
    assert!(!new_env.runtime.validate_state_part(
        &Trie::EMPTY_ROOT,
        PartId::new(0, 1),
        &state_part
    ));
    new_env.runtime.validate_state_part(&env.state_roots[0], PartId::new(0, 1), &state_part);
    let epoch_id = &new_env.head.epoch_id;
    new_env
        .runtime
        .apply_state_part(shard_id, &env.state_roots[0], PartId::new(0, 1), &state_part, epoch_id)
        .unwrap();
    new_env.state_roots[0] = env.state_roots[0];
    for _ in 3..=5 {
        new_env.step_default(vec![]);
    }

    let account = new_env.view_account(block_producers[0].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1);
    assert_eq!(account.locked, TESTING_INIT_STAKE + 1);

    let account = new_env.view_account(block_producers[1].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
    assert_eq!(account.locked, TESTING_INIT_STAKE);
}

#[test]
fn test_get_validator_info() {
    let num_nodes = 2;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new(vec![validators.clone()], 2, false);
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();
    let signer = InMemorySigner::test_signer(&validators[0]);
    let staking_transaction = stake(1, &signer, &block_producers[0], 0);
    let mut expected_blocks = [0, 0];
    let mut expected_chunks = [0, 0];
    let mut expected_endorsements = [0, 0];
    let update_validator_stats =
        |env: &mut TestEnv,
         expected_blocks: &mut [u64; 2],
         expected_chunks: &mut [u64; 2],
         expected_endorsements: &mut [u64; 2]| {
            let epoch_id = env.head.epoch_id;
            let height = env.head.height;

            let shard_layout = env.epoch_manager.get_shard_layout(&epoch_id).unwrap();
            let shard_id = shard_layout.shard_ids().next().unwrap();

            let em = env.runtime.epoch_manager.clone();
            let bp = em.get_block_producer_info(&epoch_id, height).unwrap();
            let cp_key = ChunkProductionKey { epoch_id, height_created: height, shard_id };
            let cp = em.get_chunk_producer_info(&cp_key).unwrap();
            let stateless_validators =
                em.get_chunk_validator_assignments(&epoch_id, shard_id, height).ok();

            if let Some(vs) = stateless_validators {
                if vs.contains(&validators[0]) {
                    expected_endorsements[0] += 1;
                }
                if vs.contains(&validators[1]) {
                    expected_endorsements[1] += 1;
                }
            }

            if bp.account_id() == "test1" {
                expected_blocks[0] += 1;
            } else {
                expected_blocks[1] += 1;
            }

            if cp.account_id() == "test1" {
                expected_chunks[0] += 1;
            } else {
                expected_chunks[1] += 1;
            }
        };
    env.step_default(vec![staking_transaction]);
    update_validator_stats(
        &mut env,
        &mut expected_blocks,
        &mut expected_chunks,
        &mut expected_endorsements,
    );
    assert!(
        env.epoch_manager
            .get_validator_info(ValidatorInfoIdentifier::EpochId(env.head.epoch_id))
            .is_err()
    );
    env.step_default(vec![]);
    update_validator_stats(
        &mut env,
        &mut expected_blocks,
        &mut expected_chunks,
        &mut expected_endorsements,
    );

    let shard_layout = env.epoch_manager.get_shard_layout(&env.head.epoch_id).unwrap();
    let shard_id = shard_layout.shard_ids().next().unwrap();

    let mut current_epoch_validator_info = vec![
        CurrentEpochValidatorInfo {
            account_id: "test1".parse().unwrap(),
            public_key: block_producers[0].public_key(),
            is_slashed: false,
            stake: TESTING_INIT_STAKE,
            shards_produced: vec![shard_id],
            shards_endorsed: vec![shard_id],
            num_produced_blocks: expected_blocks[0],
            num_expected_blocks: expected_blocks[0],
            num_produced_chunks: expected_chunks[0],
            num_expected_chunks: expected_chunks[0],
            num_produced_chunks_per_shard: vec![expected_chunks[0]],
            num_expected_chunks_per_shard: vec![expected_chunks[0]],
            num_produced_endorsements: expected_endorsements[0],
            num_expected_endorsements: expected_endorsements[0],
            num_expected_endorsements_per_shard: vec![expected_endorsements[0]],
            num_produced_endorsements_per_shard: vec![expected_endorsements[0]],
        },
        CurrentEpochValidatorInfo {
            account_id: "test2".parse().unwrap(),
            public_key: block_producers[1].public_key(),
            is_slashed: false,
            stake: TESTING_INIT_STAKE,
            shards_produced: vec![shard_id],
            shards_endorsed: vec![shard_id],
            num_produced_blocks: expected_blocks[1],
            num_expected_blocks: expected_blocks[1],
            num_produced_chunks: expected_chunks[1],
            num_expected_chunks: expected_chunks[1],
            num_produced_chunks_per_shard: vec![expected_chunks[1]],
            num_expected_chunks_per_shard: vec![expected_chunks[1]],
            num_produced_endorsements: expected_endorsements[1],
            num_expected_endorsements: expected_endorsements[1],
            num_expected_endorsements_per_shard: vec![expected_endorsements[1]],
            num_produced_endorsements_per_shard: vec![expected_endorsements[1]],
        },
    ];
    let next_epoch_validator_info = vec![
        NextEpochValidatorInfo {
            account_id: "test1".parse().unwrap(),
            public_key: block_producers[0].public_key(),
            stake: TESTING_INIT_STAKE,
            shards: vec![shard_id],
        },
        NextEpochValidatorInfo {
            account_id: "test2".parse().unwrap(),
            public_key: block_producers[1].public_key(),
            stake: TESTING_INIT_STAKE,
            shards: vec![shard_id],
        },
    ];
    let response = env
        .epoch_manager
        .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
        .unwrap();
    assert_eq!(
        response,
        EpochValidatorInfo {
            current_validators: current_epoch_validator_info.clone(),
            next_validators: next_epoch_validator_info,
            current_fishermen: vec![],
            next_fishermen: vec![],
            current_proposals: vec![
                ValidatorStake::new("test1".parse().unwrap(), block_producers[0].public_key(), 0,)
                    .into()
            ],
            prev_epoch_kickout: Default::default(),
            epoch_start_height: 1,
            epoch_height: 1,
        }
    );
    expected_blocks = [0, 0];
    expected_chunks = [0, 0];
    expected_endorsements = [0, 0];
    env.step_default(vec![]);
    update_validator_stats(
        &mut env,
        &mut expected_blocks,
        &mut expected_chunks,
        &mut expected_endorsements,
    );
    let response = env
        .epoch_manager
        .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
        .unwrap();

    current_epoch_validator_info[0].num_produced_blocks = expected_blocks[0];
    current_epoch_validator_info[0].num_expected_blocks = expected_blocks[0];
    current_epoch_validator_info[0].num_produced_chunks = expected_chunks[0];
    current_epoch_validator_info[0].num_expected_chunks = expected_chunks[0];
    current_epoch_validator_info[0].num_produced_chunks_per_shard = vec![expected_chunks[0]];
    current_epoch_validator_info[0].num_expected_chunks_per_shard = vec![expected_chunks[0]];
    current_epoch_validator_info[0].num_produced_endorsements = expected_endorsements[0];
    current_epoch_validator_info[0].num_expected_endorsements = expected_endorsements[0];
    current_epoch_validator_info[0].num_produced_endorsements_per_shard =
        vec![expected_endorsements[0]];
    current_epoch_validator_info[0].num_expected_endorsements_per_shard =
        vec![expected_endorsements[0]];
    current_epoch_validator_info[1].num_produced_blocks = expected_blocks[1];
    current_epoch_validator_info[1].num_expected_blocks = expected_blocks[1];
    current_epoch_validator_info[1].num_produced_chunks = expected_chunks[1];
    current_epoch_validator_info[1].num_expected_chunks = expected_chunks[1];
    current_epoch_validator_info[1].num_produced_chunks_per_shard = vec![];
    current_epoch_validator_info[1].num_expected_chunks_per_shard = vec![];
    current_epoch_validator_info[1].num_produced_endorsements = expected_endorsements[1];
    current_epoch_validator_info[1].num_expected_endorsements = expected_endorsements[1];
    current_epoch_validator_info[1].num_produced_endorsements_per_shard =
        vec![expected_endorsements[1]];
    current_epoch_validator_info[1].num_expected_endorsements_per_shard =
        vec![expected_endorsements[1]];
    current_epoch_validator_info[1].shards_produced = vec![];
    assert_eq!(response.current_validators, current_epoch_validator_info);
    assert_eq!(
        response.next_validators,
        vec![NextEpochValidatorInfo {
            account_id: "test2".parse().unwrap(),
            public_key: block_producers[1].public_key(),
            stake: TESTING_INIT_STAKE,
            shards: vec![ShardId::new(0)],
        }]
    );
    assert!(response.current_proposals.is_empty());
    assert_eq!(
        response.prev_epoch_kickout,
        vec![ValidatorKickoutView {
            account_id: "test1".parse().unwrap(),
            reason: ValidatorKickoutReason::Unstaked
        }]
    );
    assert_eq!(response.epoch_start_height, 3);
}

/// Run 4 validators. Two of them first change their stake to below validator threshold but above
/// fishermen threshold. Make sure their balance is correct. Then one fisherman increases their
/// stake to become a validator again while the other one decreases to below fishermen threshold.
/// Check that the first one becomes a validator and the second one gets unstaked completely.
#[test]
fn test_fishermen_stake() {
    init_test_logger();
    let num_nodes = 4;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new_with_config(
        vec![validators.clone()],
        TestEnvConfig {
            epoch_length: 4,
            has_reward: false,
            // We need to be able to stake enough to be fisherman, but not enough to be
            // validator
            minimum_stake_divisor: Some(20000),
            zero_fees: true,
            create_flat_storage: true,
        },
    );
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();
    let signers: Vec<_> = validators.iter().map(|id| InMemorySigner::test_signer(&id)).collect();
    let fishermen_stake = 3300 * NEAR_BASE + 1;

    let staking_transaction = stake(1, &signers[0], &block_producers[0], fishermen_stake);
    let staking_transaction1 = stake(1, &signers[1], &block_producers[1], fishermen_stake);
    env.step_default(vec![staking_transaction, staking_transaction1]);
    let account = env.view_account(block_producers[0].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
    assert_eq!(account.locked, TESTING_INIT_STAKE);
    for _ in 2..=13 {
        env.step_default(vec![]);
    }
    let account0 = env.view_account(block_producers[0].validator_id());
    assert_eq!(account0.locked, 0);
    assert_eq!(account0.amount, TESTING_INIT_BALANCE);
    let response = env
        .epoch_manager
        .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
        .unwrap();
    assert!(response.current_fishermen.is_empty());
    let staking_transaction = stake(2, &signers[0], &block_producers[0], TESTING_INIT_STAKE);
    let staking_transaction2 = stake(2, &signers[1], &block_producers[1], 0);
    env.step_default(vec![staking_transaction, staking_transaction2]);

    for _ in 13..=25 {
        env.step_default(vec![]);
    }

    let account0 = env.view_account(block_producers[0].validator_id());
    assert_eq!(account0.locked, TESTING_INIT_STAKE);
    assert_eq!(account0.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);

    let account1 = env.view_account(block_producers[1].validator_id());
    assert_eq!(account1.locked, 0);
    assert_eq!(account1.amount, TESTING_INIT_BALANCE);
    let response = env
        .epoch_manager
        .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
        .unwrap();
    assert!(response.current_fishermen.is_empty());
}

/// Test that when fishermen unstake they get their tokens back.
#[test]
fn test_fishermen_unstake() {
    init_test_logger();
    let num_nodes = 2;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new_with_config(
        vec![validators.clone()],
        TestEnvConfig {
            epoch_length: 2,
            has_reward: false,
            // We need to be able to stake enough to be fisherman, but not enough to be
            // validator
            minimum_stake_divisor: Some(20000),
            zero_fees: true,
            create_flat_storage: true,
        },
    );
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();
    let signers: Vec<_> = validators.iter().map(|id| InMemorySigner::test_signer(&id)).collect();
    let fishermen_stake = 3300 * NEAR_BASE + 1;

    let staking_transaction = stake(1, &signers[0], &block_producers[0], fishermen_stake);
    env.step_default(vec![staking_transaction]);
    for _ in 2..9 {
        env.step_default(vec![]);
    }

    let account0 = env.view_account(block_producers[0].validator_id());
    assert_eq!(account0.locked, 0);
    assert_eq!(account0.amount, TESTING_INIT_BALANCE);
    let response = env
        .epoch_manager
        .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
        .unwrap();
    assert!(response.current_fishermen.is_empty());
    let staking_transaction = stake(2, &signers[0], &block_producers[0], 0);
    env.step_default(vec![staking_transaction]);
    for _ in 10..17 {
        env.step_default(vec![]);
    }

    let account0 = env.view_account(block_producers[0].validator_id());
    assert_eq!(account0.locked, 0);
    assert_eq!(account0.amount, TESTING_INIT_BALANCE);
    let response = env
        .epoch_manager
        .get_validator_info(ValidatorInfoIdentifier::BlockHash(env.head.last_block_hash))
        .unwrap();
    assert!(response.current_fishermen.is_empty());
}

/// Enable reward and make sure that validators get reward proportional to their stake.
#[test]
fn test_validator_reward() {
    init_test_logger();
    let num_nodes = 4;
    let epoch_length = 40;
    let validators =
        (0..num_nodes).map(|i| format!("test{}", i + 1).parse().unwrap()).collect::<Vec<_>>();
    let mut env = TestEnv::new(vec![validators.clone()], epoch_length, true);
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();

    for _ in 0..(epoch_length + 1) {
        env.step_default(vec![]);
    }

    let (validator_reward, protocol_treasury_reward) =
        env.compute_reward(num_nodes, epoch_length * 10u64.pow(9));
    for i in 0..4 {
        let account = env.view_account(block_producers[i].validator_id());
        assert_eq!(account.locked, TESTING_INIT_STAKE + validator_reward);
    }

    let protocol_treasury_account =
        env.view_account(&env.runtime.genesis_config.protocol_treasury_account);
    assert_eq!(protocol_treasury_account.amount, TESTING_INIT_BALANCE + protocol_treasury_reward);
}

#[test]
fn test_delete_account_after_unstake() {
    init_test_logger();
    let num_nodes = 2;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new(vec![validators.clone()], 4, false);
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();
    let signers: Vec<_> = validators.iter().map(|id| InMemorySigner::test_signer(&id)).collect();

    let staking_transaction1 = stake(1, &signers[1], &block_producers[1], 0);
    env.step_default(vec![staking_transaction1]);
    let account = env.view_account(block_producers[1].validator_id());
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
    assert_eq!(account.locked, TESTING_INIT_STAKE);
    for _ in 2..=5 {
        env.step_default(vec![]);
    }
    let staking_transaction2 = stake(2, &signers[1], &block_producers[1], 1);
    env.step_default(vec![staking_transaction2]);
    for _ in 7..=13 {
        env.step_default(vec![]);
    }
    let account = env.view_account(block_producers[1].validator_id());
    assert_eq!(account.locked, 0);

    let delete_account_transaction = SignedTransaction::from_actions(
        4,
        signers[1].get_account_id(),
        signers[1].get_account_id(),
        &signers[1],
        vec![Action::DeleteAccount(DeleteAccountAction {
            beneficiary_id: signers[0].get_account_id(),
        })],
        // runtime does not validate block history
        CryptoHash::default(),
        0,
    );
    env.step_default(vec![delete_account_transaction]);
    for _ in 15..=17 {
        env.step_default(vec![]);
    }
}

#[test]
fn test_proposal_deduped() {
    let num_nodes = 2;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new(vec![validators.clone()], 4, false);
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();
    let signers: Vec<_> = validators.iter().map(|id| InMemorySigner::test_signer(&id)).collect();

    let staking_transaction1 = stake(1, &signers[1], &block_producers[1], TESTING_INIT_STAKE - 100);
    let staking_transaction2 = stake(2, &signers[1], &block_producers[1], TESTING_INIT_STAKE - 10);
    env.step_default(vec![staking_transaction1, staking_transaction2]);
    assert_eq!(env.last_proposals.len(), 1);
    assert_eq!(env.last_proposals[0].stake(), TESTING_INIT_STAKE - 10);
}

#[test]
fn test_insufficient_stake() {
    let num_nodes = 2;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new(vec![validators.clone()], 4, false);
    let block_producers: Vec<_> =
        validators.iter().map(|id| create_test_signer(id.as_str())).collect();
    let signers: Vec<_> = validators.iter().map(|id| InMemorySigner::test_signer(&id)).collect();

    let staking_transaction1 = stake(1, &signers[1], &block_producers[1], 100);
    let staking_transaction2 = stake(2, &signers[1], &block_producers[1], 100 * NEAR_BASE);
    env.step_default(vec![staking_transaction1, staking_transaction2]);
    assert!(env.last_proposals.is_empty());
    let staking_transaction3 = stake(3, &signers[1], &block_producers[1], 0);
    env.step_default(vec![staking_transaction3]);
    assert_eq!(env.last_proposals.len(), 1);
    assert_eq!(env.last_proposals[0].stake(), 0);
}

/// Check that flat state is included into trie and is not included into view trie, because we can't apply flat
/// state optimization to view calls.
#[test]
fn test_flat_state_usage() {
    let env = TestEnv::new(vec![vec!["test1".parse().unwrap()]], 4, false);
    let prev_hash = env.head.prev_block_hash;
    let shard_layout = env.epoch_manager.get_shard_layout_from_prev_block(&prev_hash).unwrap();
    let shard_id = shard_layout.shard_ids().next().unwrap();
    let state_root = Trie::EMPTY_ROOT;

    let trie = env.runtime.get_trie_for_shard(shard_id, &prev_hash, state_root, true).unwrap();
    assert!(trie.has_flat_storage_chunk_view());

    let trie = env.runtime.get_view_trie_for_shard(shard_id, &prev_hash, state_root).unwrap();
    assert!(!trie.has_flat_storage_chunk_view());
}

/// Check that querying trie and flat state gives the same result.
#[test]
fn test_trie_and_flat_state_equality() {
    let num_nodes = 2;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let mut env = TestEnv::new(vec![validators.clone()], 4, false);
    let signers: Vec<_> = validators.iter().map(|id| InMemorySigner::test_signer(&id)).collect();

    let transfer_tx = SignedTransaction::from_actions(
        4,
        signers[0].get_account_id(),
        validators[1].clone(),
        &signers[0],
        vec![Action::Transfer(TransferAction { deposit: 10 })],
        // runtime does not validate block history
        CryptoHash::default(),
        0,
    );
    env.step_default(vec![transfer_tx]);
    for _ in 1..=5 {
        env.step_default(vec![]);
    }

    // Extract account in two ways:
    // - using state trie, which should use flat state after enabling it in the protocol
    // - using view state, which should never use flat state
    let prev_hash = env.head.prev_block_hash;
    let shard_layout = env.epoch_manager.get_shard_layout_from_prev_block(&prev_hash).unwrap();
    let shard_id = shard_layout.shard_ids().next().unwrap();

    let state_root = env.state_roots[0];
    let state = env.runtime.get_trie_for_shard(shard_id, &prev_hash, state_root, true).unwrap();
    let view_state = env.runtime.get_view_trie_for_shard(shard_id, &prev_hash, state_root).unwrap();
    let trie_key = TrieKey::Account { account_id: validators[1].clone() };
    let key = trie_key.to_vec();

    let state_value = state.get(&key, AccessOptions::DEFAULT).unwrap().unwrap();
    let account = Account::try_from_slice(&state_value).unwrap();
    assert_eq!(account.amount(), TESTING_INIT_BALANCE - TESTING_INIT_STAKE + 10);

    let view_state_value = view_state.get(&key, AccessOptions::DEFAULT).unwrap().unwrap();
    assert_eq!(state_value, view_state_value);
}

/// Check that mainnet genesis hash still matches, to make sure that we're still backwards compatible.
#[test]
fn test_genesis_hash() {
    let genesis = near_mainnet_res::mainnet_genesis();
    let chain_genesis = ChainGenesis::new(&genesis.config);
    let store = near_store::test_utils::create_test_store();

    let tempdir = tempfile::tempdir().unwrap();
    initialize_genesis_state(store.clone(), &genesis, Some(tempdir.path()));
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
    let runtime = NightshadeRuntime::test_with_runtime_config_store(
        tempdir.path(),
        store.clone(),
        FilesystemContractRuntimeCache::new(tempdir.path(), None::<&str>)
            .expect("filesystem contract cache")
            .handle(),
        &genesis.config,
        epoch_manager.clone(),
        RuntimeConfigStore::new(None),
    );

    let state_roots =
        get_genesis_state_roots(runtime.store()).unwrap().expect("genesis should be initialized.");
    let (block, _chunks) = Chain::make_genesis_block(
        epoch_manager.as_ref(),
        runtime.as_ref(),
        &chain_genesis,
        state_roots,
    )
    .unwrap();
    assert_eq!(block.header().hash().to_string(), "EPnLgE7iEq9s7yTkos96M3cWymH5avBAPm3qx3NXqR8H");

    let epoch_manager = EpochManager::new_arc_handle(store, &genesis.config, None);
    let epoch_info = epoch_manager.get_epoch_info(&EpochId::default()).unwrap();
    // Verify the order of the block producers.
    assert_eq!(
        [
            1, 0, 1, 0, 0, 3, 3, 2, 2, 3, 0, 2, 0, 0, 1, 1, 1, 1, 3, 2, 3, 2, 0, 3, 3, 3, 0, 3, 1,
            3, 1, 0, 1, 2, 3, 0, 1, 0, 0, 0, 2, 2, 2, 3, 3, 3, 3, 1, 2, 0, 1, 0, 1, 0, 3, 2, 1, 2,
            0, 1, 3, 3, 1, 2, 1, 2, 1, 0, 2, 3, 1, 2, 1, 2, 3, 2, 0, 3, 3, 2, 0, 0, 2, 3, 0, 3, 0,
            2, 3, 1, 1, 2, 1, 0, 1, 2, 2, 1, 2, 0
        ],
        epoch_info.block_producers_settlement()
    );
}

/// Creates a signed transaction between each pair of `signers`,
/// where transaction outcomes from a single signer differ by nonce.
/// The transactions are then shuffled and used to fill a transaction pool.
fn generate_transaction_pool(signers: &Vec<Signer>, block_hash: CryptoHash) -> TransactionPool {
    const TEST_SEED: RngSeed = [3; 32];
    let mut rng = StdRng::from_seed(TEST_SEED);
    let signer_count = signers.len();

    let mut transactions = vec![];
    for round in 1..signer_count {
        for i in 0..signer_count {
            let transaction = SignedTransaction::send_money(
                round.try_into().unwrap(),
                signers[i].get_account_id(),
                signers[(i + round) % signer_count].get_account_id(),
                &signers[i],
                round.try_into().unwrap(),
                block_hash,
            );
            let validated_tx = ValidatedTransaction::new_for_test(transaction);
            transactions.push(validated_tx);
        }
    }
    transactions.shuffle(&mut rng);

    let mut pool = TransactionPool::new(TEST_SEED, None, "");
    for transaction in transactions {
        assert_eq!(pool.insert_transaction(transaction), InsertTransactionResult::Success);
    }
    pool
}

fn get_test_env_with_chain_and_pool() -> (TestEnv, Chain, TransactionPool) {
    let num_nodes = 4;
    let validators = (0..num_nodes)
        .map(|i| AccountId::try_from(format!("test{}", i + 1)).unwrap())
        .collect::<Vec<_>>();
    let chain_genesis = ChainGenesis::new(&GenesisConfig::test(Clock::real()));
    let mut env = TestEnv::new_with_config(
        vec![validators.clone()],
        TestEnvConfig {
            epoch_length: chain_genesis.epoch_length,
            has_reward: false,
            minimum_stake_divisor: None,
            zero_fees: false,
            create_flat_storage: false,
        },
    );

    let chain = Chain::new(
        Clock::real(),
        env.epoch_manager.clone(),
        ShardTracker::new_empty(env.epoch_manager.clone()),
        env.runtime.clone(),
        &chain_genesis,
        DoomslugThresholdMode::NoApprovals,
        ChainConfig::test(),
        None,
        Default::default(),
        MutableConfigValue::new(None, "validator_signer"),
        noop().into_multi_sender(),
    )
    .unwrap();

    // Make sure `chain` and test `env` use the same genesis hash.
    env.head = Tip::clone(&chain.chain_store().head().unwrap());
    // Produce a single block, so that `prev_block_hash` is valid.
    env.step_default(vec![]);

    let signers: Vec<_> = validators.iter().map(|id| InMemorySigner::test_signer(&id)).collect();

    let transaction_pool = generate_transaction_pool(&signers, env.head.prev_block_hash);
    (env, chain, transaction_pool)
}

fn prepare_transactions(
    env: &TestEnv,
    chain: &Chain,
    transaction_groups: &mut dyn TransactionGroupIterator,
    storage_config: RuntimeStorageConfig,
) -> Result<PreparedTransactions, Error> {
    let prev_hash = env.head.prev_block_hash;
    let shard_layout = env.epoch_manager.get_shard_layout_from_prev_block(&prev_hash).unwrap();
    let shard_id = shard_layout.shard_ids().next().unwrap();
    let block = chain.get_block(&prev_hash).unwrap();
    let congestion_info = block.block_congestion_info();

    env.runtime.prepare_transactions(
        storage_config,
        shard_id,
        PrepareTransactionsBlockContext {
            next_gas_price: env.runtime.genesis_config.min_gas_price,
            height: env.head.height,
            block_hash: env.head.last_block_hash,
            congestion_info,
        },
        transaction_groups,
        &mut |tx: &SignedTransaction| -> bool {
            chain
                .chain_store()
                .check_transaction_validity_period(&block.header(), tx.transaction.block_hash())
                .is_ok()
        },
        default_produce_chunk_add_transactions_time_limit(),
    )
}

/// Check that transactions validation fails if provided empty storage proof.
#[test]
fn test_prepare_transactions_empty_storage_proof() {
    // First prepare transactions using proper db and check all transactions are
    // included in the result.
    let db_storage_source = StorageDataSource::Db;
    let (transaction_count, prepared_transactions) =
        test_prepare_transactions_helper(db_storage_source)
            .expect("prepare transactions should succeed with proper db");
    assert_ne!(transaction_count, 0);
    assert_eq!(prepared_transactions.transactions.len(), transaction_count);

    // Second prepare transactions using empty storage proof and check that
    // prepare_transactions fails.
    let empty_storage_source =
        StorageDataSource::Recorded(PartialStorage { nodes: PartialState::default() });
    test_prepare_transactions_helper(empty_storage_source)
        .expect_err("prepare transactions should fail with empty storage proof");
}

// Helper function to test prepare_transactions with different storage sources.
fn test_prepare_transactions_helper(
    storage_source: StorageDataSource,
) -> Result<(usize, PreparedTransactions), Error> {
    let (env, chain, mut transaction_pool) = get_test_env_with_chain_and_pool();
    let transactions_count = transaction_pool.len();

    let storage_config = RuntimeStorageConfig {
        state_root: env.state_roots[0],
        use_flat_storage: true,
        source: storage_source,
        state_patch: Default::default(),
    };

    let mut transaction_groups = PoolIteratorWrapper::new(&mut transaction_pool);
    let prepared_transactions =
        match prepare_transactions(&env, &chain, &mut transaction_groups, storage_config) {
            Ok(prepared_transactions) => prepared_transactions,
            Err(err) => {
                return Err(err);
            }
        };
    Ok((transactions_count, prepared_transactions))
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
fn test_storage_proof_garbage() {
    let shard_id = ShardId::new(0);
    let signer = create_test_signer("test1");
    let env = TestEnv::new(vec![vec![signer.validator_id().clone()]], 100, false);
    let garbage_size_mb = 50usize;
    let receipt = Receipt::V1(ReceiptV1 {
        predecessor_id: signer.validator_id().clone(),
        receiver_id: signer.validator_id().clone(),
        receipt_id: CryptoHash::hash_bytes(&[42]),
        receipt: near_primitives::receipt::ReceiptEnum::Action(ActionReceipt {
            signer_id: signer.validator_id().clone(),
            signer_public_key: signer.public_key(),
            gas_price: env.runtime.genesis_config.min_gas_price,
            output_data_receivers: vec![],
            input_data_ids: vec![],
            actions: vec![Action::FunctionCall(
                FunctionCallAction {
                    method_name: format!("internal_record_storage_garbage_{garbage_size_mb}"),
                    args: vec![],
                    gas: 300000000000000,
                    deposit: 300000000000000,
                }
                .into(),
            )],
        }),
        priority: 0,
    });
    let apply_result = env.apply_new_chunk(shard_id, vec![], &[receipt]);
    let PartialState::TrieValues(storage_proof) = apply_result.proof.unwrap().nodes;
    let total_size: usize = storage_proof.iter().map(|v| v.len()).sum();
    assert_eq!(total_size / 1000_000, garbage_size_mb);
}

/// Tests that precompiling a set of contracts updates the compiled contract cache.
#[test]
fn test_precompile_contracts_updates_cache() {
    struct FakeTestCompiledContractType; // For testing AnyCache.
    let genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    let store = near_store::test_utils::create_test_store();
    let tempdir = tempfile::tempdir().unwrap();
    initialize_genesis_state(store.clone(), &genesis, Some(tempdir.path()));
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);

    let contract_cache = FilesystemContractRuntimeCache::new(tempdir.path(), None::<&str>)
        .expect("filesystem contract cache");
    let runtime = NightshadeRuntime::test_with_runtime_config_store(
        tempdir.path(),
        store,
        contract_cache.handle(),
        &genesis.config,
        epoch_manager,
        RuntimeConfigStore::new(None),
    );

    let contracts = vec![
        ContractCode::new(near_test_contracts::sized_contract(100).to_vec(), None),
        ContractCode::new(near_test_contracts::rs_contract().to_vec(), None),
        ContractCode::new(near_test_contracts::trivial_contract().to_vec(), None),
    ];
    let code_hashes: Vec<CryptoHash> = contracts.iter().map(|c| c.hash()).cloned().collect();

    // First check that the cache does not have the contracts.
    for code_hash in &code_hashes {
        let cache_key = get_contract_cache_key(
            *code_hash,
            &runtime.get_runtime_config(PROTOCOL_VERSION).wasm_config,
        );
        let contract = contract_cache.get(&cache_key).unwrap();
        assert!(contract.is_none());
    }

    runtime.precompile_contracts(&EpochId::default(), contracts).unwrap();

    // Check that the persistent cache contains the compiled contract after precompilation,
    // but it does not populate the in-memory cache (so that the value is generated by try_lookup call).
    for code_hash in code_hashes {
        let cache_key = get_contract_cache_key(
            code_hash,
            &runtime.get_runtime_config(PROTOCOL_VERSION).wasm_config,
        );

        let contract = contract_cache.get(&cache_key).unwrap();
        assert_matches!(
            contract,
            Some(CompiledContractInfo { compiled: CompiledContract::Code(_), .. })
        );

        let result = contract_cache
            .memory_cache()
            .try_lookup(
                cache_key,
                || Ok::<_, ()>(Box::new(FakeTestCompiledContractType)),
                |v| {
                    assert!(v.is::<FakeTestCompiledContractType>());
                    "compiled code"
                },
            )
            .unwrap();
        assert_eq!(result, "compiled code");
    }
}

fn stake(
    nonce: Nonce,
    signer: &Signer,
    sender: &ValidatorSigner,
    stake: Balance,
) -> SignedTransaction {
    SignedTransaction::from_actions(
        nonce,
        sender.validator_id().clone(),
        sender.validator_id().clone(),
        &signer,
        vec![Action::Stake(Box::new(StakeAction { stake, public_key: sender.public_key() }))],
        // runtime does not validate block history
        CryptoHash::default(),
        0,
    )
}

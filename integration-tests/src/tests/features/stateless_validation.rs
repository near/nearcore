use crate::tests::features::wallet_contract::{NearSigner, create_rlp_execute_tx, view_balance};
use assert_matches::assert_matches;
use near_chain::Provenance;
use near_chain_configs::{Genesis, GenesisConfig, GenesisRecords};
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, KeyType, SecretKey};
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_o11y::testonly::init_integration_logger;
use near_primitives::account::id::AccountIdRef;
use near_primitives::account::{AccessKeyPermission, AccountContract, FunctionCallPermission};
use near_primitives::action::{Action, AddKeyAction, TransferAction};
use near_primitives::epoch_manager::AllEpochConfigTestOverrides;
use near_primitives::num_rational::Rational32;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::state_record::StateRecord;
use near_primitives::stateless_validation::state_witness::ChunkStateWitness;
use near_primitives::test_utils::{create_test_signer, create_user_test_signer};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountInfo, EpochId, ShardId};
use near_primitives::utils::derive_eth_implicit_account_id;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolVersion};
use near_primitives::views::FinalExecutionStatus;
use near_primitives_core::account::{AccessKey, Account};
use near_primitives_core::types::{AccountId, NumSeats};
use near_store::test_utils::create_test_store;
use node_runtime::config::total_prepaid_gas;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{HashMap, HashSet};

use crate::env::nightshade_setup::TestEnvNightshadeSetupExt;
use crate::env::test_env::TestEnv;

const ONE_NEAR: u128 = 1_000_000_000_000_000_000_000_000;

fn run_chunk_validation_test(
    seed: u64,
    prob_missing_chunk: f64,
    prob_missing_block: f64,
    genesis_protocol_version: ProtocolVersion,
) {
    init_integration_logger();

    let initial_balance = 100 * ONE_NEAR;
    let validator_stake = 1000000 * ONE_NEAR;
    let blocks_to_produce = if prob_missing_block > 0.0 { 200 } else { 50 };

    let num_accounts = 9;
    let num_validators = 8;
    let (accounts, shard_layout) = get_accounts_and_shard_layout(num_accounts, num_validators);

    let num_shards = shard_layout.shard_ids().count();
    let mut genesis_config = GenesisConfig {
        // Use the latest protocol version. Otherwise, the version may be too
        // old that e.g. blocks don't even store previous heights.
        protocol_version: genesis_protocol_version,
        // Some arbitrary starting height. Doesn't matter.
        genesis_height: 10000,
        shard_layout,
        validators: accounts
            .iter()
            .take(num_validators)
            .map(|account_id| AccountInfo {
                account_id: account_id.clone(),
                public_key: create_test_signer(account_id.as_str()).public_key(),
                amount: validator_stake,
            })
            .collect(),
        // Ensures 4 epoch transitions.
        epoch_length: 10,
        // The genesis requires this, so set it to something arbitrary.
        protocol_treasury_account: accounts[num_validators].clone(),
        // Simply make all validators block producers.
        num_block_producer_seats: num_validators as NumSeats,
        // Each shard has 2 chunk producers, so 4 shards, 8 chunk producers total.
        minimum_validators_per_shard: 2,
        // Even though not used for the most recent protocol version,
        // this must still have the same length as the number of shards,
        // or else the genesis fails validation.
        num_block_producer_seats_per_shard: vec![8; num_shards],
        gas_limit: 10u64.pow(15),
        transaction_validity_period: 120,
        // Needed to completely avoid validator kickouts as we want to test
        // missing chunks functionality.
        block_producer_kickout_threshold: 0,
        chunk_producer_kickout_threshold: 0,
        chunk_validator_only_kickout_threshold: 0,
        // Needed to distribute full non-trivial reward to each validator if at
        // least some block/chunk was produced.
        // This itself is needed to make state transition on epoch boundary
        // non-trivial even if chunk is missing, so that functionality of
        // storing and validating implicit state transitions can be checked.
        online_min_threshold: Rational32::new(0, 1),
        online_max_threshold: Rational32::new(1, 1000),
        protocol_reward_rate: Rational32::new(1, 10),
        max_inflation_rate: Rational32::new(1, 1),
        ..Default::default()
    };
    let epoch_config_test_overrides = AllEpochConfigTestOverrides {
        block_producer_kickout_threshold: Some(0),
        chunk_producer_kickout_threshold: Some(0),
    };

    // Set up the records corresponding to the validator accounts.
    let mut records = Vec::new();
    for (i, account) in accounts.iter().enumerate() {
        // The staked amount must be consistent with validators from genesis.
        let staked = if i < num_validators { validator_stake } else { 0 };
        records.push(StateRecord::Account {
            account_id: account.clone(),
            account: Account::new(initial_balance, staked, AccountContract::None, 0),
        });
        records.push(StateRecord::AccessKey {
            account_id: account.clone(),
            public_key: create_test_signer(account.as_str()).public_key(),
            access_key: AccessKey::full_access(),
        });
        // The total supply must be correct to pass validation.
        genesis_config.total_supply += initial_balance + staked;
    }
    let genesis = Genesis::new(genesis_config, GenesisRecords(records)).unwrap();
    let mut env = TestEnv::builder(&genesis.config)
        .clients(accounts.iter().take(8).cloned().collect())
        .epoch_managers_with_test_overrides(epoch_config_test_overrides)
        // Disable congestion control in order to avoid rejecting transactions
        // in tests with missing chunks.
        .nightshade_runtimes_congestion_control_disabled(&genesis)
        .build();
    let mut tx_hashes = vec![];

    let mut rng: StdRng = SeedableRng::seed_from_u64(seed);
    let mut found_differing_post_state_root_due_to_state_transitions = false;

    let tip = env.clients[0].chain.head().unwrap();
    let mut height = tip.height;

    for round in 0..blocks_to_produce {
        height += 1;
        if rng.gen_bool(prob_missing_block) {
            // Skip producing a block.
            continue;
        }

        let heads = env
            .clients
            .iter()
            .map(|client| client.chain.head().unwrap().last_block_hash)
            .collect::<HashSet<_>>();
        assert_eq!(heads.len(), 1, "All clients should have the same head");
        let tip = env.clients[0].chain.head().unwrap();

        let sender_account = accounts[round % num_accounts].clone();
        let receiver_account = accounts[(round + 1) % num_accounts].clone();
        let signer = InMemorySigner::test_signer(&sender_account);
        if round > 1 {
            let tx = SignedTransaction::send_money(
                round as u64,
                sender_account,
                receiver_account,
                &signer,
                ONE_NEAR,
                tip.last_block_hash,
            );
            tx_hashes.push(tx.get_hash());
            let _ = env.rpc_handlers[0].process_tx(tx, false, false);
        }

        let height_offset = height - tip.height;
        let block_producer = env.get_block_producer_at_offset(&tip, height_offset);
        tracing::debug!(
            target: "client",
            "Producing block at height {} by {}", height, block_producer
        );
        let block = env.client(&block_producer).produce_block(height).unwrap().unwrap();

        // Apply the block.
        for i in 0..env.clients.len() {
            let validator_id = env.get_client_id(i);
            tracing::debug!(
                target: "client",
                "Applying block at height {} at {}", block.header().height(), validator_id
            );
            let blocks_processed = if rng.gen_bool(prob_missing_chunk) {
                env.clients[i]
                    .process_block_test_no_produce_chunk(block.clone().into(), Provenance::NONE)
                    .unwrap()
            } else {
                env.clients[i].process_block_test(block.clone().into(), Provenance::NONE).unwrap()
            };
            assert_eq!(blocks_processed, vec![*block.hash()]);
        }

        env.process_partial_encoded_chunks();
        for j in 0..env.clients.len() {
            env.process_shards_manager_responses_and_finish_processing_blocks(j);
        }

        let output = env.propagate_chunk_state_witnesses(false);
        env.propagate_chunk_endorsements(false);

        found_differing_post_state_root_due_to_state_transitions |=
            output.found_differing_post_state_root_due_to_state_transitions;
    }

    // Check that at least one tx was fully executed, ensuring that executing
    // state witness against non-trivial recorded storage is checked.
    let mut has_executed_txs = false;
    for tx_hash in tx_hashes {
        let outcome = env.clients[0].chain.get_final_transaction_result(&tx_hash);
        if let Ok(outcome) = outcome {
            if let FinalExecutionStatus::SuccessValue(_) = outcome.status {
                has_executed_txs = true;
            }
        }
    }
    assert!(has_executed_txs);

    // We have 4 epoch boundaries on each of 4 shards. If probability of
    // missing chunk is at least 0.8, then some chunk on epoch boundary will
    // miss with probability 1 - pow(0.2, 16), so probability of flake will be
    // around 10**-12. And this event will cause two different post
    // state roots in some state witness.
    if prob_missing_chunk >= 0.8 {
        assert!(found_differing_post_state_root_due_to_state_transitions);
    }

    let client = &env.clients[0];

    let genesis = client.chain.genesis();
    let genesis_epoch_id = client.epoch_manager.get_epoch_id(genesis.hash()).unwrap();
    assert_eq!(
        genesis_protocol_version,
        client.epoch_manager.get_epoch_protocol_version(&genesis_epoch_id).unwrap()
    );

    let head = client.chain.head().unwrap();
    let head_epoch_id = client.epoch_manager.get_epoch_id(&head.last_block_hash).unwrap();
    assert_eq!(
        PROTOCOL_VERSION,
        client.epoch_manager.get_epoch_protocol_version(&head_epoch_id).unwrap()
    );

    env.print_summary();
}

#[test]
fn slow_test_chunk_validation_no_missing_chunks() {
    run_chunk_validation_test(42, 0.0, 0.0, PROTOCOL_VERSION);
}

#[test]
fn test_chunk_validation_low_missing_chunks() {
    run_chunk_validation_test(43, 0.3, 0.0, PROTOCOL_VERSION);
}

#[test]
fn test_chunk_validation_high_missing_chunks() {
    run_chunk_validation_test(44, 0.81, 0.0, PROTOCOL_VERSION);
}

#[test]
fn test_protocol_upgrade_81() {
    init_integration_logger();

    let validator_stake = 1000000 * ONE_NEAR;
    let num_accounts = 9;
    let num_validators = 8;

    let (accounts, shard_layout) = get_accounts_and_shard_layout(num_accounts, num_validators);

    let num_shards = shard_layout.shard_ids().count();
    let genesis_config = GenesisConfig {
        protocol_version: PROTOCOL_VERSION,
        chain_id: "mocknet".to_string(),
        shard_layout,
        validators: accounts
            .iter()
            .take(num_validators)
            .map(|account_id| AccountInfo {
                account_id: account_id.clone(),
                public_key: create_test_signer(account_id.as_str()).public_key(),
                amount: validator_stake,
            })
            .collect(),
        // The genesis requires this, so set it to something arbitrary.
        protocol_treasury_account: accounts[num_validators].clone(),
        num_block_producer_seats: num_validators as NumSeats,
        minimum_validators_per_shard: num_validators as NumSeats,
        num_block_producer_seats_per_shard: vec![8; num_shards],
        block_producer_kickout_threshold: 90,
        chunk_producer_kickout_threshold: 90,
        ..Default::default()
    };
    let epoch_manager = EpochManager::new_arc_handle(create_test_store(), &genesis_config, None);
    let config = epoch_manager.get_epoch_config(&EpochId::default()).unwrap();
    assert_eq!(config.block_producer_kickout_threshold, 90);
    assert_eq!(config.chunk_producer_kickout_threshold, 90);
}

fn get_accounts_and_shard_layout(
    num_accounts: usize,
    num_validators: usize,
) -> (Vec<AccountId>, ShardLayout) {
    // Split accounts into 4 shards, so that each shard will store two validator
    // accounts.
    let accounts = (0..num_accounts)
        .map(|i| format!("account{}", i).parse().unwrap())
        .collect::<Vec<AccountId>>();
    let boundary_accounts = vec!["account2", "account4", "account6"];
    let boundary_accounts = boundary_accounts.into_iter().map(|s| s.parse().unwrap()).collect();
    let shard_layout = ShardLayout::multi_shard_custom(boundary_accounts, 3);

    // The number of accounts in each shard.
    let mut shard_account_count: HashMap<ShardId, u32> = HashMap::new();
    for account in &accounts[..num_validators] {
        let shard_id = shard_layout.account_id_to_shard_id(account);
        *shard_account_count.entry(shard_id).or_default() += 1;
    }
    for shard_id in shard_layout.shard_ids() {
        let account_count = shard_account_count.get(&shard_id).unwrap_or(&0);
        assert_eq!(account_count, &2, "Each shard should have 2 validator accounts");
    }

    (accounts, shard_layout)
}

/// Test that Client rejects ChunkStateWitnesses with invalid shard_id
#[test]
fn test_chunk_state_witness_bad_shard_id() {
    init_integration_logger();

    let accounts = vec!["test0".parse().unwrap()];
    let genesis = Genesis::test(accounts.clone(), 1);
    let mut env = TestEnv::builder(&genesis.config)
        .validators(accounts)
        .nightshade_runtimes(&genesis)
        .build();

    // Run the client for a few blocks
    let upper_height = 6;
    for height in 1..upper_height {
        tracing::info!(target: "test", "Producing block at height: {height}");
        let block = env.clients[0].produce_block(height).unwrap().unwrap();
        env.process_block(0, block, Provenance::PRODUCED);
    }

    // Create a dummy ChunkStateWitness with an invalid shard_id
    let previous_block = env.clients[0].chain.head().unwrap().prev_block_hash;
    let invalid_shard_id = ShardId::new(1000000000);
    let witness = ChunkStateWitness::new_dummy(upper_height, invalid_shard_id, previous_block);
    let witness_size = borsh::object_length(&witness).unwrap();

    // Client should reject this ChunkStateWitness and the error message should mention "shard"
    tracing::info!(target: "test", "Processing invalid ChunkStateWitness");
    let signer = env.clients[0].validator_signer.get();
    let res = env.clients[0].process_chunk_state_witness(witness, witness_size, None, signer);
    let error = res.unwrap_err();
    let error_message = format!("{}", error).to_lowercase();
    tracing::info!(target: "test", "error message: {}", error_message);
    assert!(error_message.contains("invalid shard 1000000000"));
    assert_matches!(error, near_chain::Error::ValidatorError(_));
}

/// Tests that eth-implicit accounts still work with stateless validation.
#[test]
fn test_eth_implicit_accounts() {
    let accounts =
        vec!["test0".parse().unwrap(), "test1".parse().unwrap(), "test2".parse().unwrap()];
    let genesis = Genesis::test(accounts.clone(), 2);
    let mut env = TestEnv::builder(&genesis.config)
        .validators(accounts.clone())
        .clients(accounts)
        .nightshade_runtimes(&genesis)
        .build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let signer = create_user_test_signer(AccountIdRef::new("test2").unwrap());

    // 1. Create two eth-implicit accounts
    let secret_key = SecretKey::from_seed(KeyType::SECP256K1, "test");
    let public_key = secret_key.public_key();
    let alice_eth_account = derive_eth_implicit_account_id(public_key.unwrap_as_secp256k1());
    let bob_eth_account: AccountId = "0x0000000000000000000000000000000000000b0b".parse().unwrap();

    let alice_init_balance = 3 * ONE_NEAR;
    let create_alice_tx = SignedTransaction::send_money(
        1,
        signer.get_account_id(),
        alice_eth_account.clone(),
        &signer.clone().into(),
        alice_init_balance,
        *genesis_block.hash(),
    );

    let bob_init_balance = 0;
    let create_bob_tx = SignedTransaction::send_money(
        2,
        signer.get_account_id(),
        bob_eth_account.clone(),
        &signer.clone().into(),
        bob_init_balance,
        *genesis_block.hash(),
    );

    assert_eq!(
        env.rpc_handlers[0].process_tx(create_alice_tx, false, false),
        ProcessTxResponse::ValidTx
    );
    assert_eq!(
        env.rpc_handlers[0].process_tx(create_bob_tx, false, false),
        ProcessTxResponse::ValidTx
    );

    // Process some blocks to ensure the transactions are complete.
    for _ in 0..10 {
        produce_block(&mut env);
    }

    assert_eq!(view_balance(&env, &alice_eth_account), alice_init_balance);
    assert_eq!(view_balance(&env, &bob_eth_account), bob_init_balance);

    // 2. Add function call access key to one eth-implicit account
    let relayer_account_id = signer.get_account_id();
    let mut relayer_signer = NearSigner { account_id: &relayer_account_id, signer };
    let relayer_pk = relayer_signer.signer.public_key();
    let action = Action::AddKey(Box::new(AddKeyAction {
        public_key: relayer_pk,
        access_key: AccessKey {
            nonce: 0,
            permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                allowance: None,
                receiver_id: alice_eth_account.to_string(),
                method_names: vec!["rlp_execute".into()],
            }),
        },
    }));
    let signed_transaction = create_rlp_execute_tx(
        &alice_eth_account,
        action,
        0,
        &alice_eth_account,
        &secret_key,
        &mut relayer_signer,
        &env,
    );
    let prepaid_gas = total_prepaid_gas(signed_transaction.transaction.actions()).unwrap();

    assert_eq!(
        env.rpc_handlers[0].process_tx(signed_transaction, false, false),
        ProcessTxResponse::ValidTx
    );

    for _ in 0..10 {
        produce_block(&mut env);
    }

    // Now the relayer can sign transactions on behalf of the implicit account
    relayer_signer.account_id = &alice_eth_account;

    // 3. Use one implicit account to make a transfer to the other.
    let transfer_amount = ONE_NEAR;
    let action = Action::Transfer(TransferAction { deposit: transfer_amount });
    let signed_transaction = create_rlp_execute_tx(
        &bob_eth_account,
        action,
        1,
        &alice_eth_account,
        &secret_key,
        &mut relayer_signer,
        &env,
    );

    assert_eq!(
        env.rpc_handlers[0].process_tx(signed_transaction, false, false),
        ProcessTxResponse::ValidTx
    );

    for _ in 0..10 {
        produce_block(&mut env);
    }

    let alice_final_balance = view_balance(&env, &alice_eth_account);
    let bob_final_balance = view_balance(&env, &bob_eth_account);

    let tip = env.clients[0].chain.head().unwrap();
    let runtime_config = env.get_runtime_config(0, tip.epoch_id);

    let gas_price = env.clients[0].chain.block_economics_config.min_gas_price();

    // Bob receives the transfer
    assert_eq!(bob_final_balance, bob_init_balance + transfer_amount);

    // The only tokens lost in the transaction are due to gas and refund penalty
    let max_gas_cost = ONE_NEAR / 500;
    let max_refund_cost =
        runtime_config.fees.gas_penalty_for_gas_refund(prepaid_gas) as u128 * gas_price;
    let tx_cost =
        (alice_init_balance + bob_init_balance) - (alice_final_balance + bob_final_balance);
    assert_eq!(alice_final_balance, alice_init_balance - transfer_amount - tx_cost);
    assert!(
        tx_cost < max_refund_cost + max_gas_cost,
        "{tx_cost} < {max_refund_cost} + {max_gas_cost}"
    );
}

/// Produce a block, apply it and propagate it through the network (including state witnesses).
fn produce_block(env: &mut TestEnv) {
    let heads = env
        .clients
        .iter()
        .map(|client| client.chain.head().unwrap().last_block_hash)
        .collect::<HashSet<_>>();
    assert_eq!(heads.len(), 1, "All clients should have the same head");
    let tip = env.clients[0].chain.head().unwrap();
    let block_producer = env.get_block_producer_at_offset(&tip, 1);
    let block = env.client(&block_producer).produce_block(tip.height + 1).unwrap().unwrap();

    for i in 0..env.clients.len() {
        let validator_id = env.get_client_id(i);
        tracing::debug!(
            target: "client",
            "Applying block at height {} at {}", block.header().height(), validator_id
        );
        let blocks_processed =
            env.clients[i].process_block_test(block.clone().into(), Provenance::NONE).unwrap();
        assert_eq!(blocks_processed, vec![*block.hash()]);
    }

    env.process_partial_encoded_chunks();
    for j in 0..env.clients.len() {
        env.process_shards_manager_responses_and_finish_processing_blocks(j);
    }

    env.propagate_chunk_state_witnesses(false);
    env.propagate_chunk_endorsements(false);
}

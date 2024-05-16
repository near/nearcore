use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_primitives::stateless_validation::{ChunkStateWitness, EncodedChunkStateWitness};
use near_store::test_utils::create_test_store;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashSet;

use near_chain::Provenance;
use near_chain_configs::{Genesis, GenesisConfig, GenesisRecords};
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType};
use near_o11y::testonly::init_integration_logger;
use near_primitives::epoch_manager::AllEpochConfigTestOverrides;
use near_primitives::num_rational::Rational32;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::state_record::StateRecord;
use near_primitives::test_utils::create_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountInfo, EpochId};
use near_primitives::views::FinalExecutionStatus;
use near_primitives_core::account::{AccessKey, Account};
use near_primitives_core::checked_feature;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{AccountId, NumSeats};
use near_primitives_core::version::PROTOCOL_VERSION;
use nearcore::test_utils::TestEnvNightshadeSetupExt;

const ONE_NEAR: u128 = 1_000_000_000_000_000_000_000_000;

fn run_chunk_validation_test(seed: u64, prob_missing_chunk: f64) {
    init_integration_logger();

    if !checked_feature!("stable", StatelessValidationV0, PROTOCOL_VERSION) {
        println!("Test not applicable without StatelessValidation enabled");
        return;
    }

    let initial_balance = 100 * ONE_NEAR;
    let validator_stake = 1000000 * ONE_NEAR;
    let blocks_to_produce = 50;
    let num_accounts = 9;
    let accounts = (0..num_accounts)
        .map(|i| format!("account{}", i).parse().unwrap())
        .collect::<Vec<AccountId>>();
    let num_validators = 8;
    // Split accounts into 4 shards, so that each shard will store two
    // validator accounts.
    let shard_layout = ShardLayout::v1(
        vec!["account2", "account4", "account6"].into_iter().map(|s| s.parse().unwrap()).collect(),
        None,
        1,
    );
    let num_shards = shard_layout.shard_ids().count();
    let mut genesis_config = GenesisConfig {
        // Use the latest protocol version. Otherwise, the version may be too
        // old that e.g. blocks don't even store previous heights.
        protocol_version: PROTOCOL_VERSION,
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
        // Each shard has 2 chunk prducers, so 4 shards, 8 chunk producers total.
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
    let epoch_config_test_overrides = Some(AllEpochConfigTestOverrides {
        block_producer_kickout_threshold: Some(0),
        chunk_producer_kickout_threshold: Some(0),
    });

    // Set up the records corresponding to the validator accounts.
    let mut records = Vec::new();
    for (i, account) in accounts.iter().enumerate() {
        // The staked amount must be consistent with validators from genesis.
        let staked = if i < num_validators { validator_stake } else { 0 };
        records.push(StateRecord::Account {
            account_id: account.clone(),
            account: Account::new(
                initial_balance,
                staked,
                0,
                CryptoHash::default(),
                0,
                PROTOCOL_VERSION,
            ),
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
        .nightshade_runtimes(&genesis)
        .build();
    let mut tx_hashes = vec![];

    let mut rng: StdRng = SeedableRng::seed_from_u64(seed);
    let mut found_differing_post_state_root_due_to_state_transitions = false;
    for round in 0..blocks_to_produce {
        let heads = env
            .clients
            .iter()
            .map(|client| client.chain.head().unwrap().last_block_hash)
            .collect::<HashSet<_>>();
        assert_eq!(heads.len(), 1, "All clients should have the same head");
        let tip = env.clients[0].chain.head().unwrap();

        let sender_account = accounts[round % num_accounts].clone();
        let receiver_account = accounts[(round + 1) % num_accounts].clone();
        let signer = InMemorySigner::from_seed(
            sender_account.clone(),
            KeyType::ED25519,
            sender_account.as_ref(),
        );
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
            let _ = env.clients[0].process_tx(tx, false, false);
        }

        let block_producer = env.get_block_producer_at_offset(&tip, 1);
        tracing::debug!(
            target: "stateless_validation",
            "Producing block at height {} by {}", tip.height + 1, block_producer
        );
        let block = env.client(&block_producer).produce_block(tip.height + 1).unwrap().unwrap();

        // Apply the block.
        for i in 0..env.clients.len() {
            let validator_id = env.get_client_id(i);
            tracing::debug!(
                target: "stateless_validation",
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
}

#[test]
fn test_chunk_validation_no_missing_chunks() {
    run_chunk_validation_test(42, 0.0);
}

#[test]
fn test_chunk_validation_low_missing_chunks() {
    run_chunk_validation_test(43, 0.3);
}

// This test fails because transactions are rejected when there are too many
// missing chunks in a row.
// TODO(congestion_control) - make congestion control configurable,
// disable it here and re-enable this test
#[ignore]
#[test]
fn test_chunk_validation_high_missing_chunks() {
    run_chunk_validation_test(44, 0.81);
}

#[test]
fn test_protocol_upgrade_81() {
    init_integration_logger();

    if !checked_feature!("stable", LowerValidatorKickoutPercentForDebugging, PROTOCOL_VERSION) {
        println!("Test not applicable without LowerValidatorKickoutPercentForDebugging enabled");
        return;
    }
    for is_statelessnet in [true, false] {
        let validator_stake = 1000000 * ONE_NEAR;
        let num_accounts = 9;
        let accounts = (0..num_accounts)
            .map(|i| format!("account{}", i).parse().unwrap())
            .collect::<Vec<AccountId>>();
        let num_validators = 8;
        // Split accounts into 4 shards, so that each shard will store two
        // validator accounts.
        let shard_layout = ShardLayout::v1(
            vec!["account2", "account4", "account6"]
                .into_iter()
                .map(|s| s.parse().unwrap())
                .collect(),
            None,
            1,
        );
        let num_shards = shard_layout.shard_ids().count();
        let genesis_config = GenesisConfig {
            protocol_version: PROTOCOL_VERSION,
            chain_id: if is_statelessnet {
                "statelessnet".to_string()
            } else {
                "mocknet".to_string()
            },
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
        let epoch_manager = EpochManager::new_arc_handle(create_test_store(), &genesis_config);
        let config = epoch_manager.get_epoch_config(&EpochId::default()).unwrap();
        if is_statelessnet {
            assert_eq!(config.block_producer_kickout_threshold, 50);
            assert_eq!(config.chunk_producer_kickout_threshold, 50);
        } else {
            assert_eq!(config.block_producer_kickout_threshold, 90);
            assert_eq!(config.chunk_producer_kickout_threshold, 90);
        }
    }
}

/// Test that Client rejects ChunkStateWitnesses with invalid shard_id
#[test]
fn test_chunk_state_witness_bad_shard_id() {
    init_integration_logger();

    if !checked_feature!("stable", StatelessValidationV0, PROTOCOL_VERSION) {
        println!("Test not applicable without StatelessValidation enabled");
        return;
    }

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
    let invalid_shard_id = 1000000000;
    let witness = ChunkStateWitness::new_dummy(upper_height, invalid_shard_id, previous_block);
    let encoded_witness = EncodedChunkStateWitness::encode(&witness).unwrap().0;

    // Client should reject this ChunkStateWitness and the error message should mention "shard"
    tracing::info!(target: "test", "Processing invalid ChunkStateWitness");
    let res = env.clients[0].process_chunk_state_witness(encoded_witness, None);
    let error = res.unwrap_err();
    let error_message = format!("{}", error).to_lowercase();
    tracing::info!(target: "test", "error message: {}", error_message);
    assert!(error_message.contains("shard"));
}

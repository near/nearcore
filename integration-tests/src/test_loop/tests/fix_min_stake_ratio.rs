use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;
use crate::test_loop::utils::validators::get_epoch_all_validators;
use crate::test_loop::utils::ONE_NEAR;
use itertools::Itertools;
use near_async::messaging::SendAsync;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_network::client::ProcessTxRequest;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::num_rational::Rational32;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use near_primitives_core::version::ProtocolFeature;
use std::string::ToString;
use std::sync::atomic::{AtomicU64, Ordering};

/// Check that small validator is included in the validator set after
/// enabling protocol feature `FixMinStakeRatio`.
#[test]
fn test_fix_min_stake_ratio() {
    init_test_logger();
    let builder = TestLoopBuilder::new();

    let initial_balance = 1_000_000 * ONE_NEAR;
    let epoch_length = 10;
    let accounts =
        (0..8).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.iter().cloned().collect_vec();
    let small_validator = accounts[2].clone();

    // Total stake is 62_500 NEAR - 1 unit, which is just enough for everyone
    // to join the validator set after protocol upgrade.
    let validators = vec![
        near_primitives::types::AccountInfo {
            account_id: accounts[0].clone(),
            public_key: near_primitives::test_utils::create_test_signer(accounts[0].as_str())
                .public_key(),
            amount: 31_249 * ONE_NEAR,
        },
        near_primitives::types::AccountInfo {
            account_id: accounts[1].clone(),
            public_key: near_primitives::test_utils::create_test_signer(accounts[1].as_str())
                .public_key(),
            amount: 31_250 * ONE_NEAR - 1,
        },
        near_primitives::types::AccountInfo {
            account_id: accounts[2].clone(),
            public_key: near_primitives::test_utils::create_test_signer(accounts[2].as_str())
                .public_key(),
            amount: ONE_NEAR,
        },
    ];

    // Create chain with version before FixMinStakeRatio was enabled.
    // Check that the small validator is not included in the validator set.
    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&builder.clock())
        .shard_layout(ShardLayout::get_simple_nightshade_layout_v3())
        .protocol_version(ProtocolFeature::FixMinStakeRatio.protocol_version() - 1)
        .epoch_length(epoch_length)
        .validators_raw(validators, 1, 2)
        // For genesis, set high minimum stake ratio so that small validator
        // will be excluded from the validator set.
        .minimum_stake_ratio(Rational32::new(1, 6_250))
        // Disable validator rewards.
        .max_inflation_rate(Rational32::new(0, 1));
    for account in &accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let genesis = genesis_builder.build();

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } =
        builder.genesis(genesis).clients(clients).build();

    let client_sender = node_datas[0].client_sender.clone();
    let client_handle = node_datas[0].client_sender.actor_handle();
    let initial_validators = get_epoch_all_validators(&test_loop.data.get(&client_handle).client);
    assert_eq!(initial_validators.len(), 2);
    assert!(!initial_validators.contains(&small_validator.to_string()));

    // Generate new validator proposal for small account at each epoch start,
    // so it can get included in the validator set.
    // `AtomicU64` is used because `success_condition` is `Fn` which doesn't
    // allow mutation.
    // TODO: consider using specific handler for spawning transactions
    // based on chain/network events.
    let latest_epoch_height = AtomicU64::new(0);
    let stake_if_new_epoch_started = |prev_block_hash: CryptoHash, epoch_height: u64| {
        if epoch_height > latest_epoch_height.load(Ordering::Relaxed) {
            latest_epoch_height.store(epoch_height, Ordering::Relaxed);
            let sender = &accounts[2];
            let tx = SignedTransaction::stake(
                epoch_height,
                sender.clone(),
                &create_user_test_signer(sender).into(),
                ONE_NEAR,
                near_primitives::test_utils::create_test_signer(accounts[2].as_str()).public_key(),
                prev_block_hash,
            );
            let future = client_sender.send_async(ProcessTxRequest {
                transaction: tx,
                is_forwarded: false,
                check_only: false,
            });
            drop(future);
        }
    };

    // Run chain for couple epochs.
    // Due to how protocol version voting works, chain must automatically
    // upgrade to the latest protocol version which includes FixMinStakeRatio
    // enabled.
    // In the epoch where it happens, small validator must join the validator
    // set.
    let success_condition = |test_loop_data: &mut TestLoopData| -> bool {
        let client = &test_loop_data.get(&client_handle).client;
        let validators = get_epoch_all_validators(client);
        let tip = client.chain.head().unwrap();
        let epoch_height =
            client.epoch_manager.get_epoch_height_from_prev_block(&tip.prev_block_hash).unwrap();
        stake_if_new_epoch_started(tip.prev_block_hash, epoch_height);

        assert!(epoch_height < 4);
        return if validators.len() == 3 {
            assert!(validators.contains(&small_validator.to_string()));
            let epoch_config = client.epoch_manager.get_epoch_config(&tip.epoch_id).unwrap();
            assert_eq!(
                epoch_config.validator_selection_config.minimum_stake_ratio,
                Rational32::new(1, 62_500)
            );
            true
        } else {
            false
        };
    };

    test_loop.run_until(
        success_condition,
        // Timeout at producing 5 epochs, approximately.
        Duration::seconds((5 * epoch_length) as i64),
    );

    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

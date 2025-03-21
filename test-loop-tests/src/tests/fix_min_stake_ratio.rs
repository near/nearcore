use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::ONE_NEAR;
use crate::utils::validators::get_epoch_all_validators;
use itertools::Itertools;
use near_async::messaging::SendAsync;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestGenesisBuilder, ValidatorsSpec};
use near_network::client::ProcessTxRequest;
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::hash::CryptoHash;
use near_primitives::num_rational::Rational32;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::ProtocolFeature;
use std::string::ToString;
use std::sync::atomic::{AtomicU64, Ordering};

/// Check that small validator is included in the validator set after
/// enabling protocol feature `FixMinStakeRatio`.
#[test]
fn slow_test_fix_min_stake_ratio() {
    init_test_logger();

    // Take epoch configuration before the protocol upgrade, where minimum
    // stake ratio was 1/6250.
    let epoch_config_store = EpochConfigStore::for_chain_id("mainnet", None).unwrap();
    let target_protocol_version = ProtocolFeature::FixMinStakeRatio.protocol_version();
    let genesis_protocol_version = target_protocol_version - 1;

    // Immediately start voting for the new protocol version
    let protocol_upgrade_schedule =
        ProtocolUpgradeVotingSchedule::new_immediate(target_protocol_version);

    let builder = TestLoopBuilder::new().protocol_upgrade_schedule(protocol_upgrade_schedule);

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

    let shard_layout =
        epoch_config_store.get_config(genesis_protocol_version).as_ref().shard_layout.clone();
    let validators_spec = ValidatorsSpec::raw(validators, 1, 1, 2);

    // Create chain with version before FixMinStakeRatio was enabled.
    // Check that the small validator is not included in the validator set.
    let genesis = TestGenesisBuilder::new()
        .genesis_time_from_clock(&builder.clock())
        .shard_layout(shard_layout)
        .protocol_version(genesis_protocol_version)
        .epoch_length(epoch_length)
        .validators_spec(validators_spec)
        // Disable validator rewards.
        .max_inflation_rate(Rational32::new(0, 1))
        .add_user_accounts_simple(&accounts, initial_balance)
        .build();

    let TestLoopEnv { mut test_loop, node_datas, shared_state } = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build()
        .warmup();

    let tx_processor_sender = node_datas[0].tx_processor_sender.clone();
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
            let future = tx_processor_sender.send_async(ProcessTxRequest {
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
            assert_eq!(epoch_config.minimum_stake_ratio, Rational32::new(1, 62_500));
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

    TestLoopEnv { test_loop, node_datas, shared_state }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

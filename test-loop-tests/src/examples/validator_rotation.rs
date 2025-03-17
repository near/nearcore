use std::sync::atomic::{AtomicU64, Ordering};

use itertools::Itertools as _;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_crypto::Signer;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, AccountInfo, Balance, Nonce, NumSeats};

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::{ONE_NEAR, transactions};

#[test]
fn test_validator_rotation() {
    init_test_logger();

    let stake = ONE_NEAR;

    let original_validators = (0..3)
        .map(|i| {
            let account: AccountId = format!("original_validator{i}").parse().unwrap();
            Validator { signer: create_user_test_signer(&account), account }
        })
        .collect::<Vec<_>>();
    let alternative_validators = (0..5)
        .map(|i| {
            let account: AccountId = format!("alternative_validator{i}").parse().unwrap();
            Validator { signer: create_user_test_signer(&account), account }
        })
        .collect::<Vec<_>>();

    let seats: NumSeats = original_validators.len().try_into().unwrap();
    let validators_spec = ValidatorsSpec::raw(
        original_validators
            .iter()
            .map(|v| AccountInfo {
                account_id: v.account.clone(),
                public_key: v.signer.public_key(),
                amount: stake,
            })
            .collect(),
        seats,
        seats,
        seats,
    );

    let accounts = original_validators
        .iter()
        .chain(&alternative_validators)
        .map(|v| v.account.clone())
        .collect::<Vec<_>>();

    let epoch_length: u64 = 10;
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, stake)
        .shard_layout(ShardLayout::single_shard())
        .build();

    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(accounts)
        .epoch_config_store(epoch_config_store)
        .build()
        .warmup();

    let client_actor_handle = env.node_datas[0].client_sender.actor_handle();

    let assert_current_validators = |env: &TestLoopEnv, validators: &[Validator]| {
        let client = &env.test_loop.data.get(&client_actor_handle).client;
        let epoch_id = client.chain.head().unwrap().epoch_id;
        let current_validators: Vec<_> = client
            .epoch_manager
            .get_epoch_all_validators(&epoch_id)
            .unwrap()
            .into_iter()
            .map(|v| {
                let (account, _, _) = v.destructure();
                account
            })
            .sorted()
            .collect();
        let validators: Vec<_> = validators.iter().map(|v| v.account.clone()).sorted().collect();
        assert_eq!(current_validators, validators);
    };

    let stake_original = |env: &mut TestLoopEnv| {
        let mut stake_txs: Vec<_> =
            original_validators.iter().map(|v| v.stake_tx(env, stake)).collect();
        stake_txs.extend(alternative_validators.iter().map(|v| v.stake_tx(env, 0)));
        transactions::run_txs_parallel(
            &mut env.test_loop,
            stake_txs,
            &env.node_datas,
            Duration::seconds(5),
        );
    };

    let stake_alternative = |env: &mut TestLoopEnv| {
        let mut stake_txs: Vec<_> =
            alternative_validators.iter().map(|v| v.stake_tx(env, stake)).collect();
        stake_txs.extend(original_validators.iter().map(|v| v.stake_tx(env, 0)));
        transactions::run_txs_parallel(
            &mut env.test_loop,
            stake_txs,
            &env.node_datas,
            Duration::seconds(5),
        );
    };

    let run_until_next_epoch = |env: &mut TestLoopEnv| {
        let next_epoch_id =
            env.test_loop.data.get(&client_actor_handle).client.chain.head().unwrap().next_epoch_id;

        env.test_loop.run_until(
            |test_loop_data| {
                let client = &test_loop_data.get(&client_actor_handle).client;
                let epoch_id = client.chain.head().unwrap().epoch_id;
                epoch_id == next_epoch_id
            },
            Duration::seconds(1 + i64::try_from(epoch_length).unwrap()),
        );
    };

    assert_current_validators(&env, &original_validators);

    // At the end of epoch T we define epoch_info (which defines validator set) for epoch T+2 using
    // current staking. Because of this validator reassignment would have 1 epoch lag compared to
    // staking distribution.

    stake_alternative(&mut env);
    run_until_next_epoch(&mut env);
    assert_current_validators(&env, &original_validators);

    stake_original(&mut env);
    run_until_next_epoch(&mut env);
    assert_current_validators(&env, &alternative_validators);

    stake_alternative(&mut env);
    run_until_next_epoch(&mut env);
    assert_current_validators(&env, &original_validators);

    stake_original(&mut env);
    run_until_next_epoch(&mut env);
    assert_current_validators(&env, &alternative_validators);

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

struct Validator {
    account: AccountId,
    signer: Signer,
}

impl Validator {
    fn stake_tx(&self, env: &mut TestLoopEnv, stake: Balance) -> SignedTransaction {
        let block_hash = transactions::get_shared_block_hash(&env.node_datas, &env.test_loop.data);
        SignedTransaction::stake(
            nonce(),
            self.account.clone(),
            &self.signer,
            stake,
            self.signer.public_key(),
            block_hash,
        )
    }
}

fn nonce() -> Nonce {
    static NONCE: AtomicU64 = AtomicU64::new(1);
    NONCE.fetch_add(1, Ordering::Relaxed)
}

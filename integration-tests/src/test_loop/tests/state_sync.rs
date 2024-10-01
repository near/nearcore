use near_async::messaging::SendAsync;
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_network::client::ProcessTxRequest;
use near_o11y::testonly::init_test_logger;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, AccountInfo, BlockHeightDelta, Nonce, NumSeats};

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::{TestData, TestLoopEnv};
use crate::test_loop::utils::transactions::get_anchor_hash;
use crate::test_loop::utils::ONE_NEAR;

use itertools::Itertools;

const EPOCH_LENGTH: BlockHeightDelta = 40;

struct ShardAccounts {
    boundary_accounts: Vec<String>,
    accounts: Vec<Vec<(AccountId, Nonce)>>,
}

fn generate_accounts(num_shards: usize) -> ShardAccounts {
    let accounts_per_shard = 5;

    if num_shards > 27 {
        todo!("don't know how to include more than 27 shards yet!");
    }
    let mut boundary_accounts = Vec::<String>::new();
    for c in b'a'..=b'z' {
        if boundary_accounts.len() + 1 >= num_shards {
            break;
        }
        let mut boundary_account = format!("{}", c as char);
        while boundary_account.len() < AccountId::MIN_LEN {
            boundary_account.push('0');
        }
        boundary_accounts.push(boundary_account);
    }

    let mut accounts = Vec::new();
    let mut account_base = "0";
    for a in boundary_accounts.iter() {
        accounts.push(
            (0..accounts_per_shard)
                .map(|i| (format!("{}{}", account_base, i).parse().unwrap(), 1))
                .collect::<Vec<_>>(),
        );
        account_base = a.as_str();
    }
    accounts.push(
        (0..accounts_per_shard)
            .map(|i| (format!("{}{}", account_base, i).parse().unwrap(), 1))
            .collect::<Vec<_>>(),
    );

    ShardAccounts { boundary_accounts, accounts }
}

struct TestState {
    env: TestLoopEnv,
    accounts: Vec<Vec<(AccountId, Nonce)>>,
}

fn setup_initial_blockchain(num_shards: usize, chunks_produced: Vec<Vec<bool>>) -> TestState {
    let builder = TestLoopBuilder::new();

    let num_block_producer_seats = 1;
    let num_chunk_producer_seats = num_shards;
    let num_validators = std::cmp::max(num_block_producer_seats, num_chunk_producer_seats);
    let validators = (0..num_validators)
        .map(|i| {
            let account_id = format!("node{}", i);
            AccountInfo {
                account_id: account_id.parse().unwrap(),
                public_key: near_primitives::test_utils::create_test_signer(account_id.as_str())
                    .public_key(),
                amount: 10000 * ONE_NEAR,
            }
        })
        .collect::<Vec<_>>();
    let clients = validators.iter().map(|v| v.account_id.clone()).collect::<Vec<_>>();

    let ShardAccounts { boundary_accounts, accounts } = generate_accounts(num_shards);

    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&builder.clock())
        .protocol_version_latest()
        .genesis_height(10000)
        .epoch_length(EPOCH_LENGTH)
        .gas_prices_free()
        .gas_limit_one_petagas()
        .shard_layout_simple_v1(&boundary_accounts.iter().map(|s| s.as_str()).collect::<Vec<_>>())
        .transaction_validity_period(1000)
        .epoch_length(10)
        .validators_raw(
            validators,
            num_block_producer_seats as NumSeats,
            num_chunk_producer_seats as NumSeats,
            0,
        )
        // shuffle the shard assignment so that nodes will have to state sync to catch up future tracked shards.
        // This part is the only reference to state sync at all in this test, since all we check is that the blockchain
        // progresses for a few epochs, meaning that state sync must have been successful.
        .shuffle_shard_assignment_for_chunk_producers(true);
    for accounts in accounts.iter() {
        for (account, _nonce) in accounts.iter() {
            genesis_builder.add_user_account_simple(account.clone(), 10000 * ONE_NEAR);
        }
    }
    let genesis = genesis_builder.build();

    let env =
        builder.genesis(genesis.clone()).clients(clients).chunks_produced(chunks_produced).build();

    TestState { env, accounts }
}

fn get_wrapped<T>(s: &[T], idx: usize) -> &T {
    &s[idx % s.len()]
}

fn get_wrapped_mut<T>(s: &mut [T], idx: usize) -> &mut T {
    &mut s[idx % s.len()]
}

/// tries to generate transactions between lots of different pairs of shards (accounts for shard i are in accounts[i])
fn send_txs_between_shards(
    test_loop: &mut TestLoopV2,
    node_data: &[TestData],
    accounts: &mut [Vec<(AccountId, Nonce)>],
) {
    let clients = node_data
        .iter()
        .map(|data| &test_loop.data.get(&data.client_sender.actor_handle()).client)
        .collect_vec();
    let block_hash = get_anchor_hash(&clients);

    let num_shards = accounts.len();

    // which client should we send txs to next?
    let mut client_idx = 0;
    let mut from_shard = 0;
    // which account should we choose among all the accounts of a shard?
    let mut account_idx = 0;
    let mut shard_diff = 1;

    let mut txs_sent = 0;
    while txs_sent < 200 {
        let to_shard = (from_shard + shard_diff) % num_shards;
        let (receiver, _nonce) = get_wrapped(&accounts[to_shard], account_idx);
        let receiver = receiver.clone();
        let (sender, nonce) = get_wrapped_mut(&mut accounts[from_shard], account_idx);

        let tx = SignedTransaction::send_money(
            *nonce,
            sender.clone(),
            receiver.clone(),
            &create_user_test_signer(sender).into(),
            1000,
            block_hash,
        );
        *nonce += 1;

        let future = get_wrapped(node_data, client_idx)
            .client_sender
            .clone()
            //.with_delay(Duration::milliseconds(300 * txs_sent as i64))
            .send_async(ProcessTxRequest {
                transaction: tx,
                is_forwarded: false,
                check_only: false,
            });
        drop(future);

        txs_sent += 1;
        from_shard = (from_shard + 1) % num_shards;
        if from_shard == 0 {
            shard_diff += 1;
        }
        account_idx += 1;
        client_idx = 1;
    }
}

/// runs the network and sends transactions at the beginning of each epoch. At the end the condition we're
/// looking for is just that a few epochs have passed, because that should only be possible if state sync was successful
/// (which will be required because we enable chunk producer shard shuffling on this chain)
fn produce_chunks(env: &mut TestLoopEnv, mut accounts: Vec<Vec<(AccountId, Nonce)>>) {
    let handle = env.datas[0].client_sender.actor_handle();
    let client = &env.test_loop.data.get(&handle).client;
    let mut tip = client.chain.head().unwrap();
    // TODO: make this more precise. We don't have to wait 2 whole seconds, but the amount we wait will
    // depend on whether this block is meant to have skipped chunks.
    let timeout = client.config.min_block_production_delay + Duration::seconds(2);

    let mut epoch_id_switches = 0;
    loop {
        env.test_loop.run_until(
            |data| {
                let client = &data.get(&handle).client;
                let new_tip = client.chain.head().unwrap();
                new_tip.height != tip.height
            },
            timeout,
        );

        let handle = env.datas[0].client_sender.actor_handle();
        let client = &env.test_loop.data.get(&handle).client;
        let new_tip = client.chain.head().unwrap();
        let header = client.chain.get_block_header(&tip.last_block_hash).unwrap();
        tracing::debug!("chunk mask for #{} {:?}", header.height(), header.chunk_mask());

        if new_tip.epoch_id != tip.epoch_id {
            epoch_id_switches += 1;
            if epoch_id_switches > 2 {
                break;
            }
            send_txs_between_shards(&mut env.test_loop, &env.datas, &mut accounts);
        }
        tip = new_tip;
    }
}

fn run_test(state: TestState) {
    let TestState { mut env, mut accounts } = state;
    let handle = env.datas[0].client_sender.actor_handle();
    let client = &env.test_loop.data.get(&handle).client;
    let first_epoch_time = client.config.min_block_production_delay
        * u32::try_from(EPOCH_LENGTH).unwrap_or(u32::MAX)
        + Duration::seconds(2);

    send_txs_between_shards(&mut env.test_loop, &env.datas, &mut accounts);

    env.test_loop.run_until(
        |data| {
            let handle = env.datas[0].client_sender.actor_handle();
            let client = &data.get(&handle).client;
            let tip = client.chain.head().unwrap();
            tip.epoch_id != Default::default()
        },
        first_epoch_time,
    );

    produce_chunks(&mut env, accounts);
    env.shutdown_and_drain_remaining_events(Duration::seconds(3));
}

#[derive(Debug)]
struct StateSyncTest {
    num_shards: usize,
    chunks_produced: &'static [&'static [bool]],
}

static TEST_CASES: &[StateSyncTest] = &[
    // The first two make no modifications to chunks_produced, and all chunks should be produced. This is the normal case
    StateSyncTest { num_shards: 2, chunks_produced: &[] },
    StateSyncTest { num_shards: 4, chunks_produced: &[] },
    // Now we miss some chunks at the beginning of the epoch
    StateSyncTest { num_shards: 4, chunks_produced: &[&[false, true, true, true]] },
    StateSyncTest {
        num_shards: 4,
        chunks_produced: &[&[true, true, true, true], &[false, false, true, true]],
    },
    StateSyncTest {
        num_shards: 4,
        chunks_produced: &[
            &[false, false, false, false],
            &[true, true, true, true],
            &[true, false, true, true],
        ],
    },
];

#[test]
fn test_state_sync_current_epoch() {
    init_test_logger();

    for t in TEST_CASES.iter() {
        tracing::info!("run test: {:?}", t);
        let state = setup_initial_blockchain(
            t.num_shards,
            t.chunks_produced.to_vec().into_iter().map(|v| v.to_vec()).collect(),
        );
        run_test(state);
    }
}

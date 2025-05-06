use near_async::messaging::{Handler, SendAsync};
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain_configs::TrackedShardsConfig;
use near_chain_configs::test_genesis::{
    TestEpochConfigBuilder, TestGenesisBuilder, ValidatorsSpec,
};
use near_network::client::{ProcessTxRequest, StateRequestHeader};
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{
    AccountId, AccountInfo, BlockHeight, BlockHeightDelta, Nonce, NumSeats, ShardId,
};
use near_primitives::version::PROTOCOL_VERSION;

use crate::setup::builder::{NodeStateBuilder, TestLoopBuilder};
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use crate::setup::state::NodeExecutionData;
use crate::utils::ONE_NEAR;
use crate::utils::transactions::{get_anchor_hash, get_smallest_height_head};

use itertools::Itertools;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

const GENESIS_HEIGHT: BlockHeight = 10000;

const EPOCH_LENGTH: BlockHeightDelta = 10;

fn get_boundary_accounts(num_shards: usize) -> Vec<String> {
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
    boundary_accounts
}

fn generate_accounts(boundary_accounts: &[String]) -> Vec<Vec<(AccountId, Nonce)>> {
    let accounts_per_shard = 5;
    let mut accounts = Vec::new();
    let mut account_base = "0";
    for a in boundary_accounts {
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

    accounts
}

struct TestState {
    env: TestLoopEnv,
    accounts: Option<Vec<Vec<(AccountId, Nonce)>>>,
    skip_block_height: Option<BlockHeight>,
}

fn sync_height() -> BlockHeight {
    // It would probably be better not to rely on this height calculation, since that makes
    // some assumptions about the state sync protocol that ideally tests wouldn't make. In the future
    // it would be nice to modify `drop_blocks_by_height()` to allow for more complex logic to decide
    // whether to drop the block, and be more robust to state sync protocol changes. But for now this
    // will trigger the behavior we want and it's quite a bit easier.
    GENESIS_HEIGHT + EPOCH_LENGTH + 4
}

fn setup_initial_blockchain(
    num_validators: usize,
    num_block_producer_seats: usize,
    num_chunk_producer_seats: usize,
    num_shards: usize,
    generate_shard_accounts: bool,
    chunks_produced: HashMap<ShardId, Vec<bool>>,
    skip_block_sync_height_delta: Option<isize>,
    extra_node_shard_schedule: &Option<Vec<Vec<ShardId>>>,
) -> TestState {
    let mut builder = TestLoopBuilder::new();

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
    let mut clients = validators.iter().map(|v| v.account_id.clone()).collect::<Vec<_>>();

    if let Some(schedule) = extra_node_shard_schedule.as_ref() {
        let idx = clients.len();
        let schedule = schedule.clone();
        clients.push("extra-node".parse().unwrap());

        builder = builder.config_modifier(move |config, client_index| {
            if client_index != idx {
                return;
            }
            config.tracked_shards_config = TrackedShardsConfig::Schedule(schedule.clone());
        });
    }

    let boundary_accounts = get_boundary_accounts(num_shards);
    let accounts =
        if generate_shard_accounts { Some(generate_accounts(&boundary_accounts)) } else { None };

    let shard_layout =
        ShardLayout::simple_v1(&boundary_accounts.iter().map(|s| s.as_str()).collect::<Vec<_>>());
    let validators_spec = ValidatorsSpec::raw(
        validators,
        num_block_producer_seats as NumSeats,
        num_chunk_producer_seats as NumSeats,
        num_validators as NumSeats,
    );

    let mut genesis_builder = TestGenesisBuilder::new()
        .genesis_time_from_clock(&builder.clock())
        .protocol_version(PROTOCOL_VERSION)
        .genesis_height(GENESIS_HEIGHT)
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(shard_layout.clone())
        .transaction_validity_period(1000)
        .validators_spec(validators_spec.clone());
    if let Some(accounts) = accounts.as_ref() {
        for accounts in accounts {
            for (account, _nonce) in accounts {
                genesis_builder =
                    genesis_builder.add_user_account_simple(account.clone(), 10000 * ONE_NEAR);
            }
        }
    }
    let genesis = genesis_builder.build();

    let epoch_config = TestEpochConfigBuilder::new()
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        // shuffle the shard assignment so that nodes will have to state sync to catch up future tracked shards.
        // This part is the only reference to state sync at all in this test, since all we check is that the blockchain
        // progresses for a few epochs, meaning that state sync must have been successful.
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build();
    let epoch_config_store =
        EpochConfigStore::test(BTreeMap::from([(PROTOCOL_VERSION, Arc::new(epoch_config))]));

    let mut env =
        builder.genesis(genesis).epoch_config_store(epoch_config_store).clients(clients).build();

    let skip_block_height = if let Some(delta) = skip_block_sync_height_delta {
        let sync_height = sync_height();
        let height = if delta >= 0 {
            sync_height.saturating_add(delta as BlockHeight)
        } else {
            sync_height.saturating_sub(-delta as BlockHeight)
        };
        env = env.drop(DropCondition::BlocksByHeight([height].into_iter().collect()));
        Some(height)
    } else {
        None
    };

    let env = env.drop(DropCondition::ChunksProducedByHeight(chunks_produced)).warmup();
    TestState { env, accounts, skip_block_height }
}

fn get_wrapped<T>(s: &[T], idx: usize) -> &T {
    &s[idx % s.len()]
}

fn get_wrapped_mut<T>(s: &mut [T], idx: usize) -> &mut T {
    &mut s[idx % s.len()]
}

/// tries to generate transactions between lots of different pairs of shards (accounts for shard i are in accounts[i])
fn send_txs_between_shards(
    test_loop: &TestLoopV2,
    node_datas: &[NodeExecutionData],
    accounts: &mut [Vec<(AccountId, Nonce)>],
) {
    let clients = node_datas
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

        let future = get_wrapped(node_datas, client_idx)
            .rpc_handler_sender
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

// Check that no block with height `skip_block_height` made it on the canonical chain, so we're testing
// what we think we should be.
fn assert_fork_happened(env: &TestLoopEnv, skip_block_height: BlockHeight) {
    let client_handles =
        env.node_datas.iter().map(|data| data.client_sender.actor_handle()).collect_vec();
    let clients =
        client_handles.iter().map(|handle| &env.test_loop.data.get(handle).client).collect_vec();

    // Here we assume the one before the skipped block will exist, since it's easier that way and it should
    // be true in this test.
    let prev_hash = clients[0].chain.get_block_hash_by_height(skip_block_height - 1).unwrap();
    let next_hash = clients[0].chain.chain_store.get_next_block_hash(&prev_hash).unwrap();
    let header = clients[0].chain.get_block_header(&next_hash).unwrap();
    assert!(header.height() > skip_block_height);

    // The way it's implemented currently, only one client will be aware of the fork
    for client in clients {
        let hashes =
            client.chain.chain_store.get_all_block_hashes_by_height(skip_block_height).unwrap();
        if !hashes.is_empty() {
            return;
        }
    }
    panic!(
        "Intended to have a fork at height {}, but no client knows about any blocks at that height",
        skip_block_height
    );
}

/// runs the network and sends transactions at the beginning of each epoch. At the end the condition we're
/// looking for is just that a few epochs have passed, because that should only be possible if state sync was successful
/// (which will be required because we enable chunk producer shard shuffling on this chain)
fn produce_chunks(
    env: &mut TestLoopEnv,
    mut accounts: Option<Vec<Vec<(AccountId, Nonce)>>>,
    skip_block_height: Option<BlockHeight>,
) {
    let handle = env.node_datas[0].client_sender.actor_handle();
    let client = &env.test_loop.data.get(&handle).client;
    let mut tip = client.chain.head().unwrap();
    // TODO: make this more precise. We don't have to wait 20 whole seconds, but the amount we wait will
    // depend on whether this block is meant to have skipped chunks or whether we're generating skipped blocks.
    let timeout = client.config.min_block_production_delay + Duration::seconds(20);

    let mut epoch_id_switches = 0;
    loop {
        env.test_loop.run_until(
            |data| {
                let clients = env
                    .node_datas
                    .iter()
                    .map(|test_data| &data.get(&test_data.client_sender.actor_handle()).client)
                    .collect_vec();
                let new_tip = get_smallest_height_head(&clients);
                new_tip.height > tip.height
            },
            timeout,
        );

        let clients = env
            .node_datas
            .iter()
            .map(|test_data| {
                &env.test_loop.data.get(&test_data.client_sender.actor_handle()).client
            })
            .collect_vec();
        let new_tip = get_smallest_height_head(&clients);

        let header = clients[0].chain.get_block_header(&tip.last_block_hash).unwrap();
        tracing::debug!(
            "chunk mask for #{} {:?} {} {:?}",
            header.height(),
            header.chunk_mask(),
            tip.last_block_hash,
            tip.epoch_id
        );

        if new_tip.epoch_id != tip.epoch_id {
            epoch_id_switches += 1;
            if epoch_id_switches > 3 {
                break;
            }
            if let Some(accounts) = accounts.as_mut() {
                send_txs_between_shards(&mut env.test_loop, &env.node_datas, accounts);
            }
        }
        tip = new_tip;
    }

    if let Some(skip_block_height) = skip_block_height {
        assert_fork_happened(env, skip_block_height);
    }
}

fn run_test(state: TestState) {
    let TestState { mut env, mut accounts, skip_block_height } = state;
    let handle = env.node_datas[0].client_sender.actor_handle();
    let client = &env.test_loop.data.get(&handle).client;
    let first_epoch_time = client.config.min_block_production_delay
        * u32::try_from(EPOCH_LENGTH).unwrap_or(u32::MAX)
        + Duration::seconds(2);

    if let Some(accounts) = accounts.as_mut() {
        send_txs_between_shards(&mut env.test_loop, &env.node_datas, accounts);
    }

    env.test_loop.run_until(
        |data| {
            let handle = env.node_datas[0].client_sender.actor_handle();
            let client = &data.get(&handle).client;
            let tip = client.chain.head().unwrap();
            let header = client.chain.get_block_header(&tip.last_block_hash).unwrap();
            tracing::debug!(
                "chunk mask for #{} {:?} {} {:?}",
                header.height(),
                header.chunk_mask(),
                tip.last_block_hash,
                tip.epoch_id
            );
            tip.epoch_id != Default::default()
        },
        first_epoch_time,
    );

    produce_chunks(&mut env, accounts, skip_block_height);

    env.shutdown_and_drain_remaining_events(Duration::seconds(3));
}

fn run_test_with_added_node(state: TestState) {
    let TestState { mut env, mut accounts, skip_block_height } = state;

    if let Some(accounts) = accounts.as_mut() {
        send_txs_between_shards(&mut env.test_loop, &env.node_datas, accounts);
    }

    // TODO: due to current limitations of TestLoop we have to wait for the
    // sync hash block before starting the new node.
    let sync_hash = await_sync_hash(&mut env);

    // In TestLoop the network infrastructure doesn't exist. State sync happens by
    // writing to and reading from a local directory.
    //
    // Here we query the clients directly to confirm that those nodes which are expected
    // to generate state responses in peer-to-peer sync are capable of doing so.
    env.test_loop.run_until(
        |data| {
            for test_data in &env.node_datas {
                let client = data.get_mut(&test_data.view_client_sender.actor_handle());

                let account_id = test_data.account_id.clone();
                let epoch_id = client.chain.head().unwrap().epoch_id;
                let shard_ids = client.chain.epoch_manager.shard_ids(&epoch_id).unwrap();

                for shard_id in shard_ids {
                    // Get the header and part regardless of whether the node was tracking the
                    // shard. It shouldn't crash on unexpected requests.
                    let header = client
                        .chain
                        .state_sync_adapter
                        .get_state_response_header(shard_id, sync_hash);
                    let part = client
                        .chain
                        .state_sync_adapter
                        .get_state_response_part(shard_id, 0, sync_hash);

                    let was_tracking = client
                        .chain
                        .epoch_manager
                        .cared_about_shard_prev_epoch_from_prev_block(
                            &sync_hash,
                            &account_id,
                            shard_id,
                        )
                        .unwrap();
                    if !was_tracking {
                        continue;
                    }

                    // The node is expected to serve the state if it was tracking the shard
                    // in the previous epoch. We make the run_until wait until we get back
                    // Ok responses for both the header and part.
                    if header.is_err() {
                        return false;
                    }
                    if part.is_err() {
                        return false;
                    }
                }
            }

            true
        },
        Duration::seconds(1),
    );

    // Add new node which will sync from scratch.
    let genesis = env.shared_state.genesis.clone();
    let tempdir_path = env.shared_state.tempdir.path().to_path_buf();
    let account_id: AccountId = "sync-from-scratch".parse().unwrap();
    let new_node_state = NodeStateBuilder::new(genesis, tempdir_path)
        .account_id(account_id.clone())
        .config_modifier(move |config| {
            // Lower the threshold at which state sync is chosen over block sync
            config.block_fetch_horizon = 5;
            config.tracked_shards_config = TrackedShardsConfig::AllShards;
        })
        .build();
    env.add_node(account_id.as_str(), new_node_state);

    env.test_loop.run_until(
        |data| {
            let handle = env.node_datas.last().unwrap().client_sender.actor_handle();
            let client = &data.get(&handle).client;
            let new_tip = client.chain.head().unwrap();
            if new_tip.height > GENESIS_HEIGHT {
                // Make sure the node catches up through state sync, not block sync.
                // It will skip straight from genesis to the sync prev block.
                let sync_header = client.chain.get_block_header(&sync_hash).unwrap();
                assert!(new_tip.last_block_hash == *sync_header.prev_hash());
                true
            } else {
                false
            }
        },
        Duration::seconds(3),
    );

    produce_chunks(&mut env, accounts.clone(), skip_block_height);

    env.shutdown_and_drain_remaining_events(Duration::seconds(3));
}

#[derive(Debug)]
struct StateSyncTest {
    num_validators: usize,
    num_block_producer_seats: usize,
    num_chunk_producer_seats: usize,
    num_shards: usize,
    // If true, generate several extra accounts per shard. We have a test with this disabled
    // to test state syncing shards without any account data
    generate_shard_accounts: bool,
    chunks_produced: Vec<(ShardId, Vec<bool>)>,
    // If Some(), this delta represents the delta with respect to the expected "sync_hash" block. So
    // a value of 0 will have us generate a skip on the first block that will probably be the sync_hash,
    // and a value of 1 will have us skip the one after that.
    skip_block_sync_height_delta: Option<isize>,
    extra_node_shard_schedule: Option<Vec<Vec<ShardId>>>,
}

fn run_state_sync_test_case(t: StateSyncTest) {
    tracing::info!("run test: {:?}", t);
    let state = setup_initial_blockchain(
        t.num_validators,
        t.num_block_producer_seats,
        t.num_chunk_producer_seats,
        t.num_shards,
        t.generate_shard_accounts,
        t.chunks_produced
            .iter()
            .map(|(shard_id, produced)| (*shard_id, produced.to_vec()))
            .collect(),
        t.skip_block_sync_height_delta,
        &t.extra_node_shard_schedule,
    );
    run_test(state);

    tracing::info!("run test with added node: {:?}", t);
    let state = setup_initial_blockchain(
        t.num_validators,
        t.num_block_producer_seats,
        t.num_chunk_producer_seats,
        t.num_shards,
        t.generate_shard_accounts,
        t.chunks_produced
            .iter()
            .map(|(shard_id, produced)| (*shard_id, produced.to_vec()))
            .collect(),
        t.skip_block_sync_height_delta,
        &t.extra_node_shard_schedule,
    );
    run_test_with_added_node(state);
}

// The normal case with 2 nodes and no missing chunks.
#[test]
fn slow_test_state_sync_simple_two_node() {
    init_test_logger();
    let t = StateSyncTest {
        num_validators: 2,
        num_block_producer_seats: 2,
        num_chunk_producer_seats: 2,
        num_shards: 2,
        generate_shard_accounts: true,
        chunks_produced: vec![],
        skip_block_sync_height_delta: None,
        extra_node_shard_schedule: None,
    };
    run_state_sync_test_case(t);
}

// The normal case with 5 nodes and no missing chunks.
#[test]
fn slow_test_state_sync_simple_five_node() {
    init_test_logger();
    let t = StateSyncTest {
        num_validators: 5,
        num_block_producer_seats: 4,
        num_chunk_producer_seats: 4,
        num_shards: 4,
        generate_shard_accounts: true,
        chunks_produced: vec![],
        skip_block_sync_height_delta: None,
        extra_node_shard_schedule: None,
    };
    run_state_sync_test_case(t);
}

// In this test we have 2 validators and 4 shards, and we don't generate any extra accounts.
// That makes 3 accounts including the "near" account. This means at least one shard will have no
// accounts in it, so we check that corner case here.
#[test]
fn slow_test_state_sync_empty_shard() {
    init_test_logger();
    let t = StateSyncTest {
        num_validators: 2,
        num_block_producer_seats: 2,
        num_chunk_producer_seats: 2,
        num_shards: 4,
        generate_shard_accounts: false,
        chunks_produced: vec![],
        skip_block_sync_height_delta: None,
        extra_node_shard_schedule: None,
    };
    run_state_sync_test_case(t);
}

// Miss a chunk in the first block of the new epoch; it won't affect the sync hash
#[test]
fn slow_test_state_sync_miss_chunks_first_block() {
    init_test_logger();
    let chunks_produced = vec![
        (ShardId::new(0), vec![false]),
        (ShardId::new(1), vec![true]),
        (ShardId::new(2), vec![true]),
        (ShardId::new(3), vec![true]),
    ];
    let t = StateSyncTest {
        num_validators: 5,
        num_block_producer_seats: 4,
        num_chunk_producer_seats: 4,
        num_shards: 4,
        generate_shard_accounts: true,
        chunks_produced,
        skip_block_sync_height_delta: None,
        extra_node_shard_schedule: None,
    };
    run_state_sync_test_case(t);
}

// Miss chunks in the second block of the new epoch;
// the sync hash will be one block later than usual
#[test]
fn slow_test_state_sync_miss_chunks_second_block() {
    init_test_logger();
    let chunks_produced =
        vec![(ShardId::new(0), vec![true, false]), (ShardId::new(1), vec![true, false])];
    let t = StateSyncTest {
        num_validators: 5,
        num_block_producer_seats: 4,
        num_chunk_producer_seats: 4,
        num_shards: 4,
        generate_shard_accounts: true,
        chunks_produced,
        skip_block_sync_height_delta: None,
        extra_node_shard_schedule: None,
    };
    run_state_sync_test_case(t);
}

// Miss chunks in the third block of the new epoch;
// the sync hash will be one block later than usual
#[test]
fn slow_test_state_sync_miss_chunks_third_block() {
    init_test_logger();
    let chunks_produced = vec![
        (ShardId::new(0), vec![true, true, false]),
        (ShardId::new(2), vec![true, true, false]),
    ];
    let t = StateSyncTest {
        num_validators: 5,
        num_block_producer_seats: 4,
        num_chunk_producer_seats: 4,
        num_shards: 4,
        generate_shard_accounts: true,
        chunks_produced,
        skip_block_sync_height_delta: None,
        extra_node_shard_schedule: None,
    };
    run_state_sync_test_case(t);
}

// Make the sync block have missing chunks
#[test]
fn slow_test_state_sync_miss_chunks_sync_block() {
    init_test_logger();
    let chunks_produced = vec![
        (ShardId::new(0), vec![true, true, true, false]),
        (ShardId::new(1), vec![true, true, true, false]),
    ];
    let t = StateSyncTest {
        num_validators: 5,
        num_block_producer_seats: 4,
        num_chunk_producer_seats: 4,
        num_shards: 4,
        generate_shard_accounts: true,
        chunks_produced,
        skip_block_sync_height_delta: None,
        extra_node_shard_schedule: None,
    };
    run_state_sync_test_case(t);
}

// Make the sync prev block have a missing chunk.
// Notice that the sync hash is one block later than usual because of shard 3.
// Shard 1 will be missing a chunk in the prev block.
#[test]
fn slow_test_state_sync_miss_chunks_sync_prev_block() {
    init_test_logger();
    let chunks_produced = vec![
        (ShardId::new(1), vec![true, true, true, false]),
        (ShardId::new(3), vec![true, false, true, true]),
    ];
    let t = StateSyncTest {
        num_validators: 5,
        num_block_producer_seats: 4,
        num_chunk_producer_seats: 4,
        num_shards: 4,
        generate_shard_accounts: true,
        chunks_produced,
        skip_block_sync_height_delta: None,
        extra_node_shard_schedule: None,
    };
    run_state_sync_test_case(t);
}

// Create missing chunks leading up to the last new chunk included
// before the sync hash block
#[test]
fn slow_test_state_sync_miss_chunks_before_last_chunk_included() {
    init_test_logger();
    let chunks_produced = vec![
        (ShardId::new(0), vec![false, true, false, false, true, false]),
        (ShardId::new(1), vec![false, true, false, false, true, true]),
        (ShardId::new(2), vec![false, true, false, true, false, false]),
        (ShardId::new(3), vec![false, true, false, true, false, true]),
    ];
    let t = StateSyncTest {
        num_validators: 5,
        num_block_producer_seats: 4,
        num_chunk_producer_seats: 4,
        num_shards: 4,
        generate_shard_accounts: true,
        chunks_produced,
        skip_block_sync_height_delta: None,
        extra_node_shard_schedule: None,
    };
    run_state_sync_test_case(t);
}

// Combination of different cases:
//  - Shard 0 has multiple chunk misses leading up to the sync hash block
//  - Shard 1 has multiple misses leading up to and including the sync hash block
//  - Shard 2 has missing chunks leading up to the last chunk included before sync hash block
//  - Shard 3 has no missing chunks until the sync hash block
#[test]
fn slow_test_state_sync_miss_chunks_multiple() {
    init_test_logger();
    let chunks_produced = vec![
        (ShardId::new(0), vec![true, true, true, false, false, true]),
        (ShardId::new(1), vec![true, true, true, false, false, false]),
        (ShardId::new(2), vec![false, true, false, false, true, true]),
        (ShardId::new(3), vec![true, true, true, true, true, false]),
    ];
    let t = StateSyncTest {
        num_validators: 5,
        num_block_producer_seats: 4,
        num_chunk_producer_seats: 4,
        num_shards: 4,
        generate_shard_accounts: true,
        chunks_produced,
        skip_block_sync_height_delta: None,
        extra_node_shard_schedule: None,
    };
    run_state_sync_test_case(t);
}

// This adds an extra node with an explicit tracked shards schedule to test more corner cases.
// Specifically, checking what happens when we stop tracking a shard and then track it again,
// while also needing to state sync another shard.
#[test]
fn slow_test_state_sync_untrack_then_track() {
    init_test_logger();

    let params = StateSyncTest {
        num_validators: 5,
        num_block_producer_seats: 4,
        num_chunk_producer_seats: 4,
        num_shards: 5,
        generate_shard_accounts: true,
        chunks_produced: vec![],
        skip_block_sync_height_delta: None,
        extra_node_shard_schedule: Some(vec![
            vec![ShardId::new(0), ShardId::new(1)],
            vec![ShardId::new(0), ShardId::new(1)],
            vec![ShardId::new(1), ShardId::new(2)],
            vec![ShardId::new(0), ShardId::new(3)],
        ]),
    };
    run_state_sync_test_case(params);
}

// Here we drop the block that's supposed to be the sync hash after the first full epoch,
// which causes it to be produced but then skipped on the final chain. If the state sync code
// is unaware of the possibility of forks, this will cause the producer of that block to
// believe that that block should be the sync hash block, while all other nodes will
// believe it should be the next block.
// This particular test fails without fork-aware state sync because the node that produces the
// first sync block that will end up skipped on the canonical chain (node0) provides a
// state sync header that other nodes see as invalid.
#[test]
fn slow_test_state_sync_from_fork() {
    init_test_logger();

    let params = StateSyncTest {
        num_validators: 5,
        num_block_producer_seats: 4,
        num_chunk_producer_seats: 4,
        num_shards: 5,
        generate_shard_accounts: true,
        chunks_produced: vec![],
        skip_block_sync_height_delta: Some(0),
        extra_node_shard_schedule: None,
    };
    run_state_sync_test_case(params);
}

// This is the same as the above test_state_sync_from_fork() except we tweak some parameters so that
// this test fails without fork-aware state sync because the node that produces the first sync block that will
// end up skipped on the canonical chain (node4) tries to state sync from other nodes that all know about the
// other finalized sync hash.
// TODO: Would be great to be able to set the schedules for all upcoming shard assignments in tests. It might
// even be possible to do it without reaching into and modifying the implementation, by writing some function
// that will hack together just the right parameters (account IDs, stakes, etc)
#[test]
fn slow_test_state_sync_to_fork() {
    init_test_logger();

    let params = StateSyncTest {
        num_validators: 6,
        num_block_producer_seats: 6,
        num_chunk_producer_seats: 4,
        num_shards: 5,
        generate_shard_accounts: true,
        chunks_produced: vec![],
        skip_block_sync_height_delta: Some(0),
        extra_node_shard_schedule: None,
    };
    run_state_sync_test_case(params);
}

// This one tests what happens when we skip a block after the sync block. This checks a corner case where
// the "sync_hash" will never appear as the final block for any new head block, since the final block will skip
// from one before it to one after it, so that when setting the sync hash, we cannot just check the final head
// on each new header update.
#[test]
fn slow_test_state_sync_fork_after_sync() {
    init_test_logger();

    let params = StateSyncTest {
        num_validators: 6,
        num_block_producer_seats: 6,
        num_chunk_producer_seats: 4,
        num_shards: 5,
        generate_shard_accounts: true,
        chunks_produced: vec![],
        skip_block_sync_height_delta: Some(1),
        extra_node_shard_schedule: None,
    };
    run_state_sync_test_case(params);
}

// This one tests what happens when we skip a block before the sync block.
#[test]
fn slow_test_state_sync_fork_before_sync() {
    init_test_logger();

    let params = StateSyncTest {
        num_validators: 6,
        num_block_producer_seats: 6,
        num_chunk_producer_seats: 4,
        num_shards: 5,
        generate_shard_accounts: true,
        chunks_produced: vec![],
        skip_block_sync_height_delta: Some(-1),
        extra_node_shard_schedule: None,
    };
    run_state_sync_test_case(params);
}

fn await_sync_hash(env: &mut TestLoopEnv) -> CryptoHash {
    env.test_loop.run_until(
        |data| {
            let handle = env.node_datas[0].client_sender.actor_handle();
            let client = &data.get(&handle).client;
            let tip = client.chain.head().unwrap();
            let header = client.chain.get_block_header(&tip.last_block_hash).unwrap();
            tracing::debug!(
                "chunk mask for #{} {:?} {} {:?}",
                header.height(),
                header.chunk_mask(),
                tip.last_block_hash,
                tip.epoch_id
            );
            if tip.epoch_id == Default::default() {
                return false;
            }
            client.chain.get_sync_hash(&tip.last_block_hash).unwrap().is_some()
        },
        Duration::seconds(20),
    );
    let client_handle = env.node_datas[0].client_sender.actor_handle();
    let client = &env.test_loop.data.get(&client_handle).client;
    let tip = client.chain.head().unwrap();
    let sync_hash = client.chain.get_sync_hash(&tip.last_block_hash).unwrap().unwrap();
    tracing::debug!("await_sync_hash: {sync_hash}");
    sync_hash
}

// cspell:ignore reqs
fn spam_state_sync_header_reqs(env: &mut TestLoopEnv) {
    let sync_hash = await_sync_hash(env);

    let view_client_handle = env.node_datas[0].view_client_sender.actor_handle();
    let view_client = env.test_loop.data.get_mut(&view_client_handle);

    for _ in 0..30 {
        let res = view_client.handle(StateRequestHeader { shard_id: ShardId::new(0), sync_hash });
        assert!(res.is_some());
    }

    // immediately query again, should be rejected
    let shard_id = ShardId::new(0);
    let res = view_client.handle(StateRequestHeader { shard_id, sync_hash });
    assert!(res.is_none());

    env.test_loop.run_for(Duration::seconds(40));

    let sync_hash = await_sync_hash(env);
    let view_client_handle = env.node_datas[0].view_client_sender.actor_handle();
    let view_client = env.test_loop.data.get_mut(&view_client_handle);

    let res = view_client.handle(StateRequestHeader { shard_id, sync_hash });
    assert!(res.is_some());
}

#[test]
fn slow_test_state_request() {
    init_test_logger();

    let TestState { mut env, .. } =
        setup_initial_blockchain(4, 4, 4, 4, false, HashMap::default(), None, &None);

    spam_state_sync_header_reqs(&mut env);
    env.shutdown_and_drain_remaining_events(Duration::seconds(3));
}

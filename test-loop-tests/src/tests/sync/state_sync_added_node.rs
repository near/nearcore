use crate::setup::builder::TestLoopBuilder;
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use crate::setup::state::NodeExecutionData;
use crate::utils::account::create_account_id;
use crate::utils::transactions::{get_anchor_hash, get_smallest_height_head};
use itertools::Itertools;
use near_async::messaging::CanSend;
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain_configs::test_genesis::{
    TestEpochConfigBuilder, TestGenesisBuilder, ValidatorsSpec,
};
use near_chain_configs::{StateSyncConfig, TrackedShardsConfig};
use near_client::SyncStatus;
use near_network::client::ProcessTxRequest;
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{
    AccountId, AccountInfo, Balance, BlockHeight, BlockHeightDelta, Nonce, NumSeats, ShardId,
};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolVersion};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

const GENESIS_HEIGHT: BlockHeight = 10000;

const EPOCH_LENGTH: BlockHeightDelta = 10;

fn get_boundary_accounts(num_shards: usize) -> Vec<AccountId> {
    if num_shards > 27 {
        todo!("don't know how to include more than 27 shards yet!");
    }
    let mut boundary_accounts = Vec::<AccountId>::new();
    for c in b'a'..=b'z' {
        if boundary_accounts.len() + 1 >= num_shards {
            break;
        }
        let mut boundary_account = format!("{}", c as char);
        while boundary_account.len() < AccountId::MIN_LEN {
            boundary_account.push('0');
        }
        boundary_accounts.push(boundary_account.parse().unwrap());
    }
    boundary_accounts
}

fn generate_accounts(boundary_accounts: &[AccountId]) -> Vec<Vec<(AccountId, Nonce)>> {
    let accounts_per_shard = 5;
    let mut accounts = Vec::new();
    let mut account_base = "0";
    for account in boundary_accounts {
        accounts.push(
            (0..accounts_per_shard)
                .map(|i| (format!("{}{}", account_base, i).parse().unwrap(), 1))
                .collect::<Vec<_>>(),
        );
        account_base = account.as_str();
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
    initial_protocol_version: ProtocolVersion,
) -> TestState {
    let builder = TestLoopBuilder::new();

    let validators = (0..num_validators)
        .map(|i| {
            let account_id = format!("node{}", i);
            AccountInfo {
                account_id: account_id.parse().unwrap(),
                public_key: near_primitives::test_utils::create_test_signer(account_id.as_str())
                    .public_key(),
                amount: Balance::from_near(10000),
            }
        })
        .collect::<Vec<_>>();
    let clients = validators.iter().map(|v| v.account_id.clone()).collect::<Vec<_>>();

    let boundary_accounts = get_boundary_accounts(num_shards);
    let accounts =
        if generate_shard_accounts { Some(generate_accounts(&boundary_accounts)) } else { None };
    let shard_layout = ShardLayout::multi_shard_custom(boundary_accounts, 1);
    let validators_spec = ValidatorsSpec::raw(
        validators,
        num_block_producer_seats as NumSeats,
        num_chunk_producer_seats as NumSeats,
        num_validators as NumSeats,
    );

    let mut genesis_builder = TestGenesisBuilder::new()
        .genesis_time_from_clock(&builder.clock())
        .protocol_version(initial_protocol_version)
        .genesis_height(GENESIS_HEIGHT)
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(shard_layout.clone())
        .validators_spec(validators_spec.clone());
    if let Some(accounts) = accounts.as_ref() {
        for accounts in accounts {
            for (account, _nonce) in accounts {
                genesis_builder = genesis_builder
                    .add_user_account_simple(account.clone(), Balance::from_near(10000));
            }
        }
    }
    let genesis = genesis_builder.build();

    let epoch_config = TestEpochConfigBuilder::new()
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build();
    let epoch_config_store = EpochConfigStore::test(BTreeMap::from([(
        initial_protocol_version,
        Arc::new(epoch_config),
    )]));

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

    let mut client_idx = 0;
    let mut from_shard = 0;
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
            Balance::from_yoctonear(1000),
            block_hash,
        );
        *nonce += 1;

        get_wrapped(node_datas, client_idx).rpc_handler_sender.send(ProcessTxRequest {
            transaction: tx,
            is_forwarded: false,
            check_only: false,
        });

        txs_sent += 1;
        from_shard = (from_shard + 1) % num_shards;
        if from_shard == 0 {
            shard_diff += 1;
        }
        account_idx += 1;
        client_idx = 1;
    }
}

fn assert_fork_happened(env: &TestLoopEnv, skip_block_height: BlockHeight) {
    let client_handles =
        env.node_datas.iter().map(|data| data.client_sender.actor_handle()).collect_vec();
    let clients =
        client_handles.iter().map(|handle| &env.test_loop.data.get(handle).client).collect_vec();

    let prev_hash = clients[0].chain.get_block_hash_by_height(skip_block_height - 1).unwrap();
    let next_hash = clients[0].chain.chain_store.get_next_block_hash(&prev_hash).unwrap();
    let header = clients[0].chain.get_block_header(&next_hash).unwrap();
    assert!(header.height() > skip_block_height);

    for client in clients {
        let hashes = client.chain.chain_store.get_all_block_hashes_by_height(skip_block_height);
        if !hashes.is_empty() {
            return;
        }
    }
    panic!(
        "Intended to have a fork at height {}, but no client knows about any blocks at that height",
        skip_block_height
    );
}

fn get_network_protocol_version(env: &TestLoopEnv) -> ProtocolVersion {
    let client_handle = env.node_datas[0].client_sender.actor_handle();
    let client = &env.test_loop.data.get(&client_handle).client;
    let tip = client.chain.head().unwrap();
    client.epoch_manager.get_epoch_protocol_version(&tip.epoch_id).unwrap()
}

fn produce_chunks(
    env: &mut TestLoopEnv,
    mut accounts: Option<Vec<Vec<(AccountId, Nonce)>>>,
    skip_block_height: Option<BlockHeight>,
) {
    let handle = env.node_datas[0].client_sender.actor_handle();
    let client = &env.test_loop.data.get(&handle).client;
    let mut tip = client.chain.head().unwrap();
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
            height = %header.height(),
            chunk_mask = ?header.chunk_mask(),
            ?tip.last_block_hash,
            ?tip.epoch_id,
            "chunk mask for block"
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
    assert_eq!(get_network_protocol_version(env), PROTOCOL_VERSION);
}

fn await_sync_hash(env: &mut TestLoopEnv) -> CryptoHash {
    env.test_loop.run_until(
        |data| {
            let handle = env.node_datas[0].client_sender.actor_handle();
            let client = &data.get(&handle).client;
            let tip = client.chain.head().unwrap();
            let header = client.chain.get_block_header(&tip.last_block_hash).unwrap();
            tracing::debug!(
                height = %header.height(),
                chunk_mask = ?header.chunk_mask(),
                ?tip.last_block_hash,
                ?tip.epoch_id,
                "chunk mask for block"
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
    tracing::debug!(%sync_hash, "await_sync_hash");
    sync_hash
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
    let identifier = "sync-from-scratch";
    let new_node_state = env
        .node_state_builder()
        .account_id(&create_account_id(identifier))
        .config_modifier(move |config| {
            config.block_fetch_horizon = 5;
            config.tracked_shards_config = TrackedShardsConfig::AllShards;
        })
        .build();
    env.add_node(identifier, new_node_state);

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
struct AddedNodeTest {
    num_validators: usize,
    num_block_producer_seats: usize,
    num_chunk_producer_seats: usize,
    num_shards: usize,
    generate_shard_accounts: bool,
    chunks_produced: Vec<(ShardId, Vec<bool>)>,
    skip_block_sync_height_delta: Option<isize>,
    initial_protocol_version: ProtocolVersion,
}

fn run_added_node_test_case(t: AddedNodeTest) {
    tracing::info!(?t, "run test with added node");
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
        t.initial_protocol_version,
    );
    assert_eq!(get_network_protocol_version(&state.env), t.initial_protocol_version);
    run_test_with_added_node(state);
}

// 5 validators, 4 shards, no drops. Basic added-node test.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_added_node_simple() {
    init_test_logger();
    let t = AddedNodeTest {
        num_validators: 5,
        num_block_producer_seats: 4,
        num_chunk_producer_seats: 4,
        num_shards: 4,
        generate_shard_accounts: true,
        chunks_produced: vec![],
        skip_block_sync_height_delta: None,
        initial_protocol_version: PROTOCOL_VERSION,
    };
    run_added_node_test_case(t);
}

// 2 validators, 4 shards, no extra accounts. At least one shard will be empty.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_added_node_empty_shard() {
    init_test_logger();
    let t = AddedNodeTest {
        num_validators: 2,
        num_block_producer_seats: 2,
        num_chunk_producer_seats: 2,
        num_shards: 4,
        generate_shard_accounts: false,
        chunks_produced: vec![],
        skip_block_sync_height_delta: None,
        initial_protocol_version: PROTOCOL_VERSION,
    };
    run_added_node_test_case(t);
}

// 5 validators, 4 shards, missing chunk in first block of epoch.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_added_node_missing_chunks() {
    init_test_logger();
    let chunks_produced = vec![
        (ShardId::new(0), vec![false]),
        (ShardId::new(1), vec![true]),
        (ShardId::new(2), vec![true]),
        (ShardId::new(3), vec![true]),
    ];
    let t = AddedNodeTest {
        num_validators: 5,
        num_block_producer_seats: 4,
        num_chunk_producer_seats: 4,
        num_shards: 4,
        generate_shard_accounts: true,
        chunks_produced,
        skip_block_sync_height_delta: None,
        initial_protocol_version: PROTOCOL_VERSION,
    };
    run_added_node_test_case(t);
}

// 5 validators, 5 shards, skip block at sync hash (fork test).
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_added_node_from_fork() {
    init_test_logger();
    let t = AddedNodeTest {
        num_validators: 5,
        num_block_producer_seats: 4,
        num_chunk_producer_seats: 4,
        num_shards: 5,
        generate_shard_accounts: true,
        chunks_produced: vec![],
        skip_block_sync_height_delta: Some(0),
        initial_protocol_version: PROTOCOL_VERSION,
    };
    run_added_node_test_case(t);
}

// Test that when a node is in state sync, the header height keeps increasing.
// State sync stalls because StateRequestHeader and StateRequestPart messages
// are intercepted by the peer manager mock and never fulfilled. This lets us
// observe header sync advancing in the background while the node remains stuck
// in StateSync status.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_added_node_no_parts_provided() {
    init_test_logger();

    let TestState { mut env, .. } =
        setup_initial_blockchain(4, 4, 4, 4, false, HashMap::default(), None, PROTOCOL_VERSION);

    // Let the network run for 3 epochs to ensure state sync will be triggered
    let healthy_node_handle = env.node_datas[0].client_sender.actor_handle();
    let healthy_client = &env.test_loop.data.get(&healthy_node_handle).client;
    let initial_epoch_id = healthy_client.chain.head().unwrap().epoch_id;
    let min_block_production_delay = healthy_client.config.min_block_production_delay;

    let epochs_to_run = 3;
    let blocks_to_run = epochs_to_run * EPOCH_LENGTH;
    let epoch_timeout = min_block_production_delay * blocks_to_run as u32 + Duration::seconds(20);

    let mut current_epoch_id = initial_epoch_id;
    let mut epochs_passed = 0;

    env.test_loop.run_until(
        |data| {
            let client = &data.get(&healthy_node_handle).client;
            let tip = client.chain.head().unwrap();
            if tip.epoch_id != current_epoch_id {
                epochs_passed += 1;
                current_epoch_id = tip.epoch_id;
            }
            epochs_passed >= epochs_to_run
        },
        epoch_timeout,
    );

    // Start a new node
    let identifier = "sync-no-parts";
    let new_node_state = env
        .node_state_builder()
        .account_id(&create_account_id(identifier))
        .config_modifier(move |config| {
            config.block_fetch_horizon = 5;
            config.tracked_shards_config = TrackedShardsConfig::AllShards;
            config.state_sync_enabled = true;
            config.state_sync = StateSyncConfig::default();
            // Prevent epoch sync from intercepting this test; we want state
            // sync to handle the gap.
            config.epoch_sync.epoch_sync_horizon_num_epochs = 10;
        })
        .build();
    env.add_node(identifier, new_node_state);

    let syncing_node_data = env.node_datas.last().unwrap();

    // Run until the new node starts state sync
    let syncing_handle = syncing_node_data.client_sender.actor_handle();
    env.test_loop.run_until(
        |data| {
            let client = &data.get(&syncing_handle).client;
            let is_state_sync = matches!(client.sync_handler.sync_status, SyncStatus::StateSync(_));
            if is_state_sync {
                tracing::debug!("Node entered state sync");
            }
            is_state_sync
        },
        Duration::seconds(5),
    );

    let (syncing_initial_header_height, syncing_initial_epoch_id) = {
        let client = &env.test_loop.data.get(&syncing_handle).client;
        let header_head = client.chain.header_head().unwrap();
        (header_head.height, header_head.epoch_id)
    };

    env.test_loop.run_until(
        |data| {
            let client = &data.get(&syncing_handle).client;
            let header_head = client.chain.header_head().unwrap();
            tracing::debug!(
                height = %header_head.height,
                "syncing node header head height"
            );
            header_head.height >= syncing_initial_header_height + 10
        },
        Duration::seconds(10),
    );

    let client = &env.test_loop.data.get(&syncing_handle).client;

    tracing::info!(
        syncing_initial_header_height = %syncing_initial_header_height,
        syncing_final_header_height = %client.chain.header_head().unwrap().height,
        syncing_initial_epoch = ?syncing_initial_epoch_id,
        syncing_final_epoch = ?client.chain.head().unwrap().epoch_id,
        "Test completed: Syncing node header height increased by 10 blocks and is in a new epoch during state sync"
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

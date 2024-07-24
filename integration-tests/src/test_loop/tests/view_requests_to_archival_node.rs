use std::collections::HashSet;

use itertools::Itertools;
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_client::{GetBlock, GetChunk};
use near_o11y::testonly::init_test_logger;
use near_primitives::types::{AccountId, BlockId};

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;
use crate::test_loop::utils::transactions::execute_money_transfers;
use crate::test_loop::utils::view_client::ViewRequestSender;
use crate::test_loop::utils::ONE_NEAR;

const NUM_VALIDATORS: usize = 2;
const NUM_ACCOUNTS: usize = 20;
const EPOCH_LENGTH: u64 = 10;
const GENESIS_HEIGHT: u64 = 0;
const GC_NUM_EPOCHS_TO_KEEP: u64 = 3;
const ARCHIVAL_CLIENT: usize = 2;

#[test]
fn test_view_requests_to_archival_node() {
    init_test_logger();
    let builder = TestLoopBuilder::new();

    let initial_balance = 10000 * ONE_NEAR;
    let accounts = (0..NUM_ACCOUNTS)
        .map(|i| format!("account{}", i).parse().unwrap())
        .collect::<Vec<AccountId>>();
    // Validators take all the roles: block+chunk producer and chunk validator.
    let validators = (0..NUM_VALIDATORS).map(|idx| accounts[idx].as_str()).collect_vec();

    // Contains the accounts of the validators and the non-validator archival node.
    let all_clients: Vec<AccountId> =
        accounts.iter().take(NUM_VALIDATORS + 1).cloned().collect_vec();
    // Contains the account of the non-validator archival node.
    let archival_clients: HashSet<AccountId> =
        vec![all_clients[NUM_VALIDATORS].clone()].into_iter().collect();

    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&builder.clock())
        .genesis_height(GENESIS_HEIGHT)
        .gas_limit_one_petagas()
        .shard_layout_simple_v1(&["account3", "account5", "account7"])
        .epoch_length(EPOCH_LENGTH)
        .validators_desired_roles(&validators, &[]);
    for account in &accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let genesis = genesis_builder.build();

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } = builder
        .genesis(genesis)
        .clients(all_clients)
        .archival(archival_clients)
        .gc_num_epochs_to_keep(GC_NUM_EPOCHS_TO_KEEP)
        .build();

    let non_validator_accounts = accounts.iter().skip(NUM_VALIDATORS).cloned().collect_vec();
    execute_money_transfers(&mut test_loop, &node_datas, &non_validator_accounts);

    // Run the chain until it garbage collects blocks from the first epoch.
    let client_handle = node_datas[ARCHIVAL_CLIENT].client_sender.actor_handle();
    let target_height: u64 = EPOCH_LENGTH * (GC_NUM_EPOCHS_TO_KEEP + 2) + 5;
    test_loop.run_until(
        |test_loop_data| {
            let chain = &test_loop_data.get(&client_handle).client.chain;
            chain.head().unwrap().height >= target_height
        },
        Duration::seconds(target_height as i64),
    );

    let view = ViewRequestSender::new(&node_datas);

    // Sanity check: Validators cannot return old blocks (post-GC) but archival node can.
    let genesis_block =
        GetBlock(near_primitives::types::BlockReference::BlockId(BlockId::Height(GENESIS_HEIGHT)));
    assert!(view.get_block(genesis_block.clone(), &mut test_loop, 0).is_err());
    assert!(view.get_block(genesis_block.clone(), &mut test_loop, 1).is_err());
    assert!(view.get_block(genesis_block, &mut test_loop, ARCHIVAL_CLIENT).is_ok());

    check_view_methods(&view, &mut test_loop);

    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

fn check_view_methods(view: &ViewRequestSender, test_loop: &mut TestLoopV2) {
    check_get_block(view, test_loop);
    check_get_chunk(view, test_loop);
}

fn check_get_block(view: &ViewRequestSender, test_loop: &mut TestLoopV2) {
    let block_by_height =
        GetBlock(near_primitives::types::BlockReference::BlockId(BlockId::Height(5)));
    let block = view.get_block(block_by_height, test_loop, ARCHIVAL_CLIENT).unwrap();
    assert_eq!(block.header.chunks_included, 4);

    let block_by_hash = GetBlock(near_primitives::types::BlockReference::BlockId(BlockId::Hash(
        block.header.prev_hash,
    )));
    let block = view.get_block(block_by_hash, test_loop, ARCHIVAL_CLIENT).unwrap();
    assert_eq!(block.header.chunks_included, 4);
}

fn check_get_chunk(view: &ViewRequestSender, test_loop: &mut TestLoopV2) {
    let chunk_by_height = GetChunk::Height(5, 0);
    let chunk = view.get_chunk(chunk_by_height, test_loop, ARCHIVAL_CLIENT).unwrap();
    assert_eq!(chunk.header.gas_limit, 1_000_000_000_000_000);
}

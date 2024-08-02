use std::collections::HashSet;

use itertools::Itertools;
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_client::{
    GetBlock, GetChunk, GetExecutionOutcomesForBlock, GetProtocolConfig, GetShardChunk,
    GetStateChanges, GetStateChangesInBlock, GetValidatorInfo, GetValidatorOrdered,
};
use near_network::client::BlockHeadersRequest;
use near_o11y::testonly::init_test_logger;
use near_primitives::sharding::ChunkHash;
use near_primitives::types::{
    AccountId, BlockId, BlockReference, EpochId, EpochReference, Finality, SyncCheckpoint,
};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{
    StateChangeCauseView, StateChangeKindView, StateChangeValueView, StateChangesRequestView,
};

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
const NUM_SHARDS: usize = 4;

/// Tests view client functionality of an archival node.
/// For this, it runs a network with 2 validators and a non-validator archival node.
/// Then it makes calls to the view client of the validator node to exercise the view client methods.
/// It extensively generates all kinds of requests, but it does not check the return value extensively.
/// The goal is to exercise the codepath that answers the requests, rather than checking
/// it returns a fully correct response.
#[test]
fn test_view_requests_to_archival_node() {
    init_test_logger();
    let builder = TestLoopBuilder::new();

    let initial_balance = 10000 * ONE_NEAR;
    let accounts = (0..NUM_ACCOUNTS)
        .map(|i| format!("account{}", i).parse().unwrap())
        .collect::<Vec<AccountId>>();
    // Validators take all the roles: block+chunk producer and chunk validator.
    let validators =
        accounts.iter().take(NUM_VALIDATORS).map(|account| account.as_str()).collect_vec();

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
        .archival_clients(archival_clients)
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

    // Sanity check: Validators cannot return old blocks after GC (eg. genesis block) but archival node can.
    let genesis_block = GetBlock(BlockReference::BlockId(BlockId::Height(GENESIS_HEIGHT)));
    assert!(view.get_block(genesis_block.clone(), &mut test_loop, 0).is_err());
    assert!(view.get_block(genesis_block.clone(), &mut test_loop, 1).is_err());
    assert!(view.get_block(genesis_block, &mut test_loop, ARCHIVAL_CLIENT).is_ok());

    check_view_methods(&view, &mut test_loop);

    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Generates variations of the messages to retrieve different kinds of information
/// and issues them to the view client of the archival node.
fn check_view_methods(view: &ViewRequestSender, test_loop: &mut TestLoopV2) {
    check_get_block(view, test_loop);
    check_get_block_headers(view, test_loop);
    check_get_chunk(view, test_loop);
    check_get_shard_chunk(view, test_loop);
    check_get_protocol_config(view, test_loop);
    check_get_validator_info(view, test_loop);
    check_get_ordered_validators(view, test_loop);
    check_get_state_changes_in_block(view, test_loop);
    check_get_state_changes(view, test_loop);
    check_get_execution_outcomes(view, test_loop);
}

/// Generates variations of the [`GetBlock`] request and issues them to the view client of the archival node.
fn check_get_block(view: &ViewRequestSender, test_loop: &mut TestLoopV2) {
    let mut get_and_check_block = |request| {
        let block = view.get_block(request, test_loop, ARCHIVAL_CLIENT).unwrap();
        assert_eq!(block.header.chunks_included, 4);
        block
    };

    let block_by_height = GetBlock(BlockReference::BlockId(BlockId::Height(5)));
    let block = get_and_check_block(block_by_height);

    let block_by_hash = GetBlock(BlockReference::BlockId(BlockId::Hash(block.header.prev_hash)));
    get_and_check_block(block_by_hash);

    let block_by_finality_optimistic = GetBlock(BlockReference::Finality(Finality::None));
    get_and_check_block(block_by_finality_optimistic);

    let block_by_finality_doomslug = GetBlock(BlockReference::Finality(Finality::DoomSlug));
    get_and_check_block(block_by_finality_doomslug);

    let block_by_finality_final = GetBlock(BlockReference::Finality(Finality::Final));
    get_and_check_block(block_by_finality_final);

    let block_by_sync_genesis = GetBlock(BlockReference::SyncCheckpoint(SyncCheckpoint::Genesis));
    get_and_check_block(block_by_sync_genesis);

    let block_by_sync_earliest =
        GetBlock(BlockReference::SyncCheckpoint(SyncCheckpoint::EarliestAvailable));
    get_and_check_block(block_by_sync_earliest);
}

/// Generates variations of the [`BlockHeadersRequest`] request and issues them to the view client of the archival node.
fn check_get_block_headers(view: &ViewRequestSender, test_loop: &mut TestLoopV2) {
    let block_by_height = GetBlock(BlockReference::BlockId(BlockId::Height(5)));
    let block = view.get_block(block_by_height, test_loop, ARCHIVAL_CLIENT).unwrap();

    let headers_request = BlockHeadersRequest(vec![block.header.hash]);
    let headers_response =
        view.get_block_headers(headers_request, test_loop, ARCHIVAL_CLIENT).unwrap().unwrap();
    assert_eq!(headers_response.len() as u64, EPOCH_LENGTH * (GC_NUM_EPOCHS_TO_KEEP + 2));
}

/// Generates variations of the [`GetChunk`] request and issues them to the view client of the archival node.
fn check_get_chunk(view: &ViewRequestSender, test_loop: &mut TestLoopV2) {
    let block_by_height = GetBlock(BlockReference::BlockId(BlockId::Height(5)));
    let block = view.get_block(block_by_height, test_loop, ARCHIVAL_CLIENT).unwrap();

    let mut get_and_check_chunk = |request| {
        let chunk = view.get_chunk(request, test_loop, ARCHIVAL_CLIENT).unwrap();
        assert_eq!(chunk.header.gas_limit, 1_000_000_000_000_000);
        chunk
    };

    let chunk_by_height = GetChunk::Height(5, 0);
    get_and_check_chunk(chunk_by_height);

    let chunk_by_block_hash = GetChunk::BlockHash(block.header.hash, 0);
    get_and_check_chunk(chunk_by_block_hash);

    let chunk_by_chunk_hash = GetChunk::ChunkHash(ChunkHash(block.chunks[0].chunk_hash));
    get_and_check_chunk(chunk_by_chunk_hash);
}

/// Generates variations of the [`GetShardChunk`] request and issues them to the view client of the archival node.
fn check_get_shard_chunk(view: &ViewRequestSender, test_loop: &mut TestLoopV2) {
    let block_by_height = GetBlock(BlockReference::BlockId(BlockId::Height(5)));
    let block = view.get_block(block_by_height, test_loop, ARCHIVAL_CLIENT).unwrap();

    let mut get_and_check_shard_chunk = |request| {
        let shard_chunk = view.get_shard_chunk(request, test_loop, ARCHIVAL_CLIENT).unwrap();
        assert_eq!(shard_chunk.take_header().gas_limit(), 1_000_000_000_000_000);
    };

    let chunk_by_height = GetShardChunk::Height(5, 0);
    get_and_check_shard_chunk(chunk_by_height);

    let chunk_by_block_hash = GetShardChunk::BlockHash(block.header.hash, 0);
    get_and_check_shard_chunk(chunk_by_block_hash);

    let chunk_by_chunk_hash = GetShardChunk::ChunkHash(ChunkHash(block.chunks[0].chunk_hash));
    get_and_check_shard_chunk(chunk_by_chunk_hash);
}

/// Generates variations of the [`GetProtocolConfig`] request and issues them to the view client of the archival node.
fn check_get_protocol_config(view: &ViewRequestSender, test_loop: &mut TestLoopV2) {
    let block_by_height = GetBlock(BlockReference::BlockId(BlockId::Height(5)));
    let block = view.get_block(block_by_height, test_loop, ARCHIVAL_CLIENT).unwrap();

    let mut get_and_check_protocol_config = |request| {
        let config = view.get_protocol_config(request, test_loop, ARCHIVAL_CLIENT).unwrap();
        assert_eq!(config.protocol_version, PROTOCOL_VERSION);
        config
    };

    let protocol_config_by_height = GetProtocolConfig(BlockReference::BlockId(BlockId::Height(5)));
    get_and_check_protocol_config(protocol_config_by_height);

    let protocol_config_by_hash =
        GetProtocolConfig(BlockReference::BlockId(BlockId::Hash(block.header.prev_hash)));
    get_and_check_protocol_config(protocol_config_by_hash);

    let protocol_config_finality_optimistic =
        GetProtocolConfig(BlockReference::Finality(Finality::None));
    get_and_check_protocol_config(protocol_config_finality_optimistic);

    let protocol_config_finality_doomslug =
        GetProtocolConfig(BlockReference::Finality(Finality::DoomSlug));
    get_and_check_protocol_config(protocol_config_finality_doomslug);

    let protocol_config_finality_final =
        GetProtocolConfig(BlockReference::Finality(Finality::Final));
    get_and_check_protocol_config(protocol_config_finality_final);

    let protocol_config_by_sync_genesis =
        GetProtocolConfig(BlockReference::SyncCheckpoint(SyncCheckpoint::Genesis));
    get_and_check_protocol_config(protocol_config_by_sync_genesis);

    let protocol_config_by_sync_earliest =
        GetProtocolConfig(BlockReference::SyncCheckpoint(SyncCheckpoint::EarliestAvailable));
    get_and_check_protocol_config(protocol_config_by_sync_earliest);
}

/// Generates variations of the [`GetValidatorInfo`] request and issues them to the view client of the archival node.
fn check_get_validator_info(view: &ViewRequestSender, test_loop: &mut TestLoopV2) {
    // For getting validator info by block height/hash, use the last block of an epoch.
    let block_by_height = GetBlock(BlockReference::BlockId(BlockId::Height(EPOCH_LENGTH * 2)));
    let block = view.get_block(block_by_height, test_loop, ARCHIVAL_CLIENT).unwrap();

    let mut get_and_check_validator_info = |request| {
        let validator_info = view.get_validator_info(request, test_loop, ARCHIVAL_CLIENT).unwrap();
        assert_eq!(validator_info.current_validators.len(), 2);
        validator_info
    };

    let validator_info_by_epoch_id = GetValidatorInfo {
        epoch_reference: EpochReference::EpochId(EpochId(block.header.epoch_id)),
    };
    get_and_check_validator_info(validator_info_by_epoch_id);

    let validator_info_by_block_height = GetValidatorInfo {
        epoch_reference: EpochReference::BlockId(BlockId::Height(EPOCH_LENGTH * 2)),
    };
    get_and_check_validator_info(validator_info_by_block_height);

    let validator_info_by_block_hash = GetValidatorInfo {
        epoch_reference: EpochReference::BlockId(BlockId::Hash(block.header.hash)),
    };
    get_and_check_validator_info(validator_info_by_block_hash);

    let validator_info_latest = GetValidatorInfo { epoch_reference: EpochReference::Latest };
    get_and_check_validator_info(validator_info_latest);
}

/// Generates variations of the [`GetValidatorInfo`] request and issues them to the view client of the archival node.
fn check_get_ordered_validators(view: &ViewRequestSender, test_loop: &mut TestLoopV2) {
    // For getting validator info by block height/hash, use the last block of an epoch.
    let block_by_height = GetBlock(BlockReference::BlockId(BlockId::Height(EPOCH_LENGTH * 2)));
    let block = view.get_block(block_by_height, test_loop, ARCHIVAL_CLIENT).unwrap();

    let mut get_and_check_ordered_validators = |request| {
        let validator_info =
            view.get_validators_ordered(request, test_loop, ARCHIVAL_CLIENT).unwrap();
        assert_eq!(validator_info.len(), 2);
        validator_info
    };

    let ordered_validators_by_height = GetValidatorOrdered { block_id: Some(BlockId::Height(5)) };
    get_and_check_ordered_validators(ordered_validators_by_height);

    let ordered_validators_by_block_hash =
        GetValidatorOrdered { block_id: Some(BlockId::Hash(block.header.hash)) };
    get_and_check_ordered_validators(ordered_validators_by_block_hash);

    let ordered_validators_latest = GetValidatorOrdered { block_id: None };
    get_and_check_ordered_validators(ordered_validators_latest);
}

/// Generates variations of the [`GetBlockStateChangesInBlock`] request and issues them to the view client of the archival node.    
fn check_get_state_changes_in_block(view: &ViewRequestSender, test_loop: &mut TestLoopV2) {
    let block_by_height = GetBlock(BlockReference::BlockId(BlockId::Height(5)));
    let block = view.get_block(block_by_height, test_loop, ARCHIVAL_CLIENT).unwrap();

    let state_changes_in_block = GetStateChangesInBlock { block_hash: block.header.hash };
    let state_changes = view
        .get_state_changes_in_block(state_changes_in_block, test_loop, ARCHIVAL_CLIENT)
        .unwrap();
    assert_eq!(state_changes.len(), 4);
    assert_eq!(
        state_changes[0],
        StateChangeKindView::AccountTouched { account_id: "account2".parse().unwrap() }
    );
    assert_eq!(
        state_changes[1],
        StateChangeKindView::AccountTouched { account_id: "account3".parse().unwrap() }
    );
    assert_eq!(
        state_changes[2],
        StateChangeKindView::AccessKeyTouched { account_id: "account2".parse().unwrap() }
    );
    assert_eq!(
        state_changes[3],
        StateChangeKindView::AccessKeyTouched { account_id: "account3".parse().unwrap() }
    );
}

/// Generates variations of the [`GetReceipt`] request and issues them to the view client of the archival node.
fn check_get_execution_outcomes(view: &ViewRequestSender, test_loop: &mut TestLoopV2) {
    let block_by_height = GetBlock(BlockReference::BlockId(BlockId::Height(5)));
    let block = view.get_block(block_by_height, test_loop, ARCHIVAL_CLIENT).unwrap();

    let request = GetExecutionOutcomesForBlock { block_hash: block.header.hash };
    let outcomes = view.get_execution_outcomes(request, test_loop, ARCHIVAL_CLIENT).unwrap();
    assert_eq!(outcomes.len(), NUM_SHARDS);
    assert_eq!(outcomes[&0].len(), 1);
    assert_eq!(outcomes[&1].len(), 1);
    assert_eq!(outcomes[&2].len(), 0);
    assert_eq!(outcomes[&3].len(), 0);
}

/// Generates variations of the [`GetStateChanges`] request and issues them to the view client of the archival node.
fn check_get_state_changes(view: &ViewRequestSender, test_loop: &mut TestLoopV2) {
    let block_by_height = GetBlock(BlockReference::BlockId(BlockId::Height(5)));
    let block = view.get_block(block_by_height, test_loop, ARCHIVAL_CLIENT).unwrap();

    let accounts = (0..NUM_ACCOUNTS)
        .map(|i| format!("account{}", i).parse().unwrap())
        .collect::<Vec<AccountId>>();
    let request = GetStateChanges {
        block_hash: block.header.hash,
        state_changes_request: StateChangesRequestView::AccountChanges { account_ids: accounts },
    };
    let state_changes = view.get_state_changes(request, test_loop, ARCHIVAL_CLIENT).unwrap();
    assert_eq!(state_changes.len(), 2);
    assert!(matches!(state_changes[0].cause, StateChangeCauseView::TransactionProcessing { .. }));
    assert!(matches!(state_changes[1].cause, StateChangeCauseView::TransactionProcessing { .. }));
    assert!(matches!(state_changes[0].value, StateChangeValueView::AccountUpdate { .. }));
    assert!(matches!(state_changes[1].value, StateChangeValueView::AccountUpdate { .. }));
}

//  left: "{3: [], 0: [ExecutionOutcomeWithIdView { proof: [], block_hash: FXQ6TQfSbqHp4xpK82vrq8h8ThSqsmLpyZhb6N5HJBiL, id: Aok8EzkCeh6bKFPtQNX85XS3VAFpcKEdWdmLyu88tiQS, outcome: ExecutionOutcomeView { logs: [], receipt_ids: [EhXNPRoHo2viKf9aFhpLddQjHc483v47nywRgTbkuPt8], gas_burnt: 223182562500, tokens_burnt: 0, executor_id: AccountId(\"account2\"), status: SuccessReceiptId(EhXNPRoHo2viKf9aFhpLddQjHc483v47nywRgTbkuPt8), metadata: ExecutionMetadataView { version: 1, gas_profile: None } } }],
// 1: [ExecutionOutcomeWithIdView { proof: [], block_hash: FXQ6TQfSbqHp4xpK82vrq8h8ThSqsmLpyZhb6N5HJBiL, id: H9Q53gLvXe69mEiMCht2u2gmad89Dy4FjghxD26QQLHr, outcome: ExecutionOutcomeView { logs: [], receipt_ids: [4UJRqAgRBdBMuCG1yDfFnj5K9trsekc6XarwNt1e4qj2], gas_burnt: 223182562500, tokens_burnt: 0, executor_id: AccountId(\"account3\"), status: SuccessReceiptId(4UJRqAgRBdBMuCG1yDfFnj5K9trsekc6XarwNt1e4qj2), metadata: ExecutionMetadataView { version: 1, gas_profile: None } } }], 2: []}"

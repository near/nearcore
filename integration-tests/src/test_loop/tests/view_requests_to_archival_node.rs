use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use near_async::messaging::Handler;
use near_async::test_loop::data::TestLoopDataHandle;
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{
    build_genesis_and_epoch_config_store, GenesisAndEpochConfigParams, ValidatorsSpec,
};
use near_client::{
    GetBlock, GetChunk, GetExecutionOutcomesForBlock, GetProtocolConfig, GetShardChunk,
    GetStateChanges, GetStateChangesInBlock, GetValidatorInfo, GetValidatorOrdered,
    ViewClientActorInner,
};
use near_network::client::BlockHeadersRequest;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::ChunkHash;
use near_primitives::types::{
    AccountId, BlockHeight, BlockId, BlockReference, EpochId, EpochReference, Finality,
    SyncCheckpoint,
};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{
    BlockView, ExecutionOutcomeView, ExecutionOutcomeWithIdView, ExecutionStatusView,
    StateChangeCauseView, StateChangeKindView, StateChangeValueView, StateChangesRequestView,
};

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::{TestData, TestLoopEnv};
use crate::test_loop::utils::transactions::execute_money_transfers;

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
fn slow_test_view_requests_to_archival_node() {
    init_test_logger();
    let builder = TestLoopBuilder::new();

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

    let shard_layout = ShardLayout::simple_v1(&["account3", "account5", "account7"]);
    let (genesis, epoch_config_store) = build_genesis_and_epoch_config_store(
        GenesisAndEpochConfigParams {
            epoch_length: EPOCH_LENGTH,
            protocol_version: PROTOCOL_VERSION,
            shard_layout: shard_layout.clone(),
            validators_spec: ValidatorsSpec::desired_roles(&validators, &[]),
            accounts: &accounts,
        },
        |genesis_builder| genesis_builder.genesis_height(GENESIS_HEIGHT),
        |epoch_config_builder| epoch_config_builder,
    );

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(all_clients)
        .archival_clients(archival_clients)
        .gc_num_epochs_to_keep(GC_NUM_EPOCHS_TO_KEEP)
        .build();

    let non_validator_accounts = accounts.iter().skip(NUM_VALIDATORS).cloned().collect_vec();
    execute_money_transfers(&mut test_loop, &node_datas, &non_validator_accounts).unwrap();

    // Run the chain until it garbage collects blocks from the first epoch.
    let client_handle = node_datas[ARCHIVAL_CLIENT].client_sender.actor_handle();
    let target_height: u64 = EPOCH_LENGTH * (GC_NUM_EPOCHS_TO_KEEP + 2) + 6;
    test_loop.run_until(
        |test_loop_data| {
            let chain = &test_loop_data.get(&client_handle).client.chain;
            chain.head().unwrap().height >= target_height
        },
        Duration::seconds(target_height as i64),
    );

    let mut view_client_tester = ViewClientTester::new(&mut test_loop, &node_datas);
    view_client_tester.run_tests(&shard_layout);

    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

struct ViewClientTester<'a> {
    test_loop: &'a mut TestLoopV2,
    /// List of data handles to the view client senders for sending the requests.
    /// Used to locate the right view client to send a request (by index).
    handles: Vec<TestLoopDataHandle<ViewClientActorInner>>,
    /// Cache of block height to Blocks (as they are called in multiple checks).
    block_cache: HashMap<BlockHeight, BlockView>,
}

impl<'a> ViewClientTester<'a> {
    fn new(test_loop: &'a mut TestLoopV2, test_data: &Vec<TestData>) -> Self {
        Self {
            test_loop,
            // Save the handles for the view client senders.
            handles: test_data
                .iter()
                .map(|data| data.view_client_sender.actor_handle())
                .collect_vec(),
            block_cache: HashMap::new(),
        }
    }

    /// Sends a message to the `[ViewClientActorInner]` for the client at position `idx`.
    fn send<M: actix::Message>(&mut self, request: M, idx: usize) -> M::Result
    where
        M::Result: Send,
        ViewClientActorInner: Handler<M>,
    {
        let view_client = self.test_loop.data.get_mut(&self.handles[idx]);
        view_client.handle(request)
    }

    /// Generates variations of the messages to retrieve different kinds of information
    /// and issues them to the view client of the archival node.
    fn run_tests(&mut self, shard_layout: &ShardLayout) {
        // Sanity check: Validators cannot return old blocks after GC (eg. genesis block) but archival node can.
        let genesis_block_request =
            GetBlock(BlockReference::BlockId(BlockId::Height(GENESIS_HEIGHT)));
        assert!(self.send(genesis_block_request.clone(), 0).is_err());
        assert!(self.send(genesis_block_request.clone(), 1).is_err());
        assert!(self.send(genesis_block_request, ARCHIVAL_CLIENT).is_ok());

        // Run the tests for various kinds of requests.
        self.check_get_block();
        self.check_get_block_headers();
        self.check_get_chunk(shard_layout);
        self.check_get_shard_chunk(shard_layout);
        self.check_get_protocol_config();
        self.check_get_validator_info();
        self.check_get_ordered_validators();
        self.check_get_state_changes_in_block();
        self.check_get_state_changes();
        self.check_get_execution_outcomes(shard_layout);
    }

    fn get_block_at_height(&mut self, height: BlockHeight) -> BlockView {
        if let Some(block) = self.block_cache.get(&height) {
            block.clone()
        } else {
            let block = self
                .send(GetBlock(BlockReference::BlockId(BlockId::Height(height))), ARCHIVAL_CLIENT)
                .unwrap();
            self.block_cache.insert(height, block.clone());
            block
        }
    }

    /// Generates variations of the [`GetBlock`] request and issues them to the view client of the archival node.
    fn check_get_block(&mut self) {
        let mut get_and_check_block = |request: GetBlock| {
            let block = self.send(request, ARCHIVAL_CLIENT).unwrap();
            assert_eq!(block.header.chunks_included, 4);
            block
        };

        let block_by_height = GetBlock(BlockReference::BlockId(BlockId::Height(6)));
        let block = get_and_check_block(block_by_height);

        let block_by_hash =
            GetBlock(BlockReference::BlockId(BlockId::Hash(block.header.prev_hash)));
        get_and_check_block(block_by_hash);

        let block_by_finality_optimistic = GetBlock(BlockReference::Finality(Finality::None));
        get_and_check_block(block_by_finality_optimistic);

        let block_by_finality_doomslug = GetBlock(BlockReference::Finality(Finality::DoomSlug));
        get_and_check_block(block_by_finality_doomslug);

        let block_by_finality_final = GetBlock(BlockReference::Finality(Finality::Final));
        get_and_check_block(block_by_finality_final);

        let block_by_sync_genesis =
            GetBlock(BlockReference::SyncCheckpoint(SyncCheckpoint::Genesis));
        get_and_check_block(block_by_sync_genesis);

        let block_by_sync_earliest =
            GetBlock(BlockReference::SyncCheckpoint(SyncCheckpoint::EarliestAvailable));
        get_and_check_block(block_by_sync_earliest);
    }

    /// Generates variations of the [`BlockHeadersRequest`] request and issues them to the view client of the archival node.
    fn check_get_block_headers(&mut self) {
        let block = self.get_block_at_height(6);

        let headers_request = BlockHeadersRequest(vec![block.header.hash]);
        let headers_response = self.send(headers_request, ARCHIVAL_CLIENT).unwrap();
        assert_eq!(headers_response.len() as u64, EPOCH_LENGTH * (GC_NUM_EPOCHS_TO_KEEP + 2));
    }

    /// Generates variations of the [`GetChunk`] request and issues them to the view client of the archival node.
    fn check_get_chunk(&mut self, shard_layout: &ShardLayout) {
        let shard_info = shard_layout.shard_infos().next().unwrap();
        let shard_id = shard_info.shard_id();
        let shard_index = shard_info.shard_index();

        let block = self.get_block_at_height(6);

        let mut get_and_check_chunk = |request: GetChunk| {
            let chunk = self.send(request, ARCHIVAL_CLIENT).unwrap();
            assert_eq!(chunk.header.gas_limit, 1_000_000_000_000_000);
            chunk
        };

        let chunk_by_height = GetChunk::Height(6, shard_id);
        get_and_check_chunk(chunk_by_height);

        let chunk_by_block_hash = GetChunk::BlockHash(block.header.hash, shard_id);
        get_and_check_chunk(chunk_by_block_hash);

        let chunk_hash = ChunkHash(block.chunks[shard_index].chunk_hash);
        let chunk_by_chunk_hash = GetChunk::ChunkHash(chunk_hash);
        get_and_check_chunk(chunk_by_chunk_hash);
    }

    /// Generates variations of the [`GetShardChunk`] request and issues them to the view client of the archival node.
    fn check_get_shard_chunk(&mut self, shard_layout: &ShardLayout) {
        let shard_info = shard_layout.shard_infos().next().unwrap();
        let shard_id = shard_info.shard_id();
        let shard_index = shard_info.shard_index();

        let block = self.get_block_at_height(6);

        let mut get_and_check_shard_chunk = |request: GetShardChunk| {
            let shard_chunk = self.send(request, ARCHIVAL_CLIENT).unwrap();
            assert_eq!(shard_chunk.take_header().gas_limit(), 1_000_000_000_000_000);
        };

        let chunk_by_height = GetShardChunk::Height(6, shard_id);
        get_and_check_shard_chunk(chunk_by_height);

        let chunk_by_block_hash = GetShardChunk::BlockHash(block.header.hash, shard_id);
        get_and_check_shard_chunk(chunk_by_block_hash);

        let chunk_hash = ChunkHash(block.chunks[shard_index].chunk_hash);
        let chunk_by_chunk_hash = GetShardChunk::ChunkHash(chunk_hash);
        get_and_check_shard_chunk(chunk_by_chunk_hash);
    }

    /// Generates variations of the [`GetProtocolConfig`] request and issues them to the view client of the archival node.
    fn check_get_protocol_config(&mut self) {
        let block = self.get_block_at_height(6);

        let mut get_and_check_protocol_config = |request: GetProtocolConfig| {
            let config = self.send(request, ARCHIVAL_CLIENT).unwrap();
            assert_eq!(config.protocol_version, PROTOCOL_VERSION);
            config
        };

        let protocol_config_by_height =
            GetProtocolConfig(BlockReference::BlockId(BlockId::Height(6)));
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
    fn check_get_validator_info(&mut self) {
        // For getting validator info by block height/hash, use the last block of an epoch.
        let block = self.get_block_at_height(EPOCH_LENGTH * 2);

        let mut get_and_check_validator_info = |request: GetValidatorInfo| {
            let validator_info = self.send(request, ARCHIVAL_CLIENT).unwrap();
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
    fn check_get_ordered_validators(&mut self) {
        // For getting validator info by block height/hash, use the last block of an epoch.
        let block = self.get_block_at_height(EPOCH_LENGTH * 2);

        let mut get_and_check_ordered_validators = |request: GetValidatorOrdered| {
            let validator_info = self.send(request, ARCHIVAL_CLIENT).unwrap();
            assert_eq!(validator_info.len(), 2);
            validator_info
        };

        let ordered_validators_by_height =
            GetValidatorOrdered { block_id: Some(BlockId::Height(6)) };
        get_and_check_ordered_validators(ordered_validators_by_height);

        let ordered_validators_by_block_hash =
            GetValidatorOrdered { block_id: Some(BlockId::Hash(block.header.hash)) };
        get_and_check_ordered_validators(ordered_validators_by_block_hash);

        let ordered_validators_latest = GetValidatorOrdered { block_id: None };
        get_and_check_ordered_validators(ordered_validators_latest);
    }

    /// Generates variations of the [`GetBlockStateChangesInBlock`] request and issues them to the view client of the archival node.
    fn check_get_state_changes_in_block(&mut self) {
        let block = self.get_block_at_height(6);

        let state_changes_in_block = GetStateChangesInBlock { block_hash: block.header.hash };
        let state_changes = self.send(state_changes_in_block, ARCHIVAL_CLIENT).unwrap();
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
    fn check_get_execution_outcomes(&mut self, shard_layout: &ShardLayout) {
        let block = self.get_block_at_height(6);

        let request = GetExecutionOutcomesForBlock { block_hash: block.header.hash };
        let outcomes = self.send(request, ARCHIVAL_CLIENT).unwrap();
        assert_eq!(outcomes.len(), NUM_SHARDS);

        let [s0, s1, s2, s3] = shard_layout.shard_ids().collect_vec()[..] else {
            panic!("Expected 4 shards in the shard layout");
        };

        assert_eq!(outcomes[&s0].len(), 1);
        assert!(matches!(
            outcomes[&s0][0],
            ExecutionOutcomeWithIdView {
                outcome: ExecutionOutcomeView {
                    status: ExecutionStatusView::SuccessReceiptId(_),
                    ..
                },
                ..
            }
        ));
        assert_eq!(outcomes[&s1].len(), 1);
        assert!(matches!(
            outcomes[&s1][0],
            ExecutionOutcomeWithIdView {
                outcome: ExecutionOutcomeView {
                    status: ExecutionStatusView::SuccessReceiptId(_),
                    ..
                },
                ..
            }
        ));
        assert_eq!(outcomes[&s2].len(), 0);
        assert_eq!(outcomes[&s3].len(), 0);
    }

    /// Generates variations of the [`GetStateChanges`] request and issues them to the view client of the archival node.
    fn check_get_state_changes(&mut self) {
        let block = self.get_block_at_height(6);

        let accounts = (0..NUM_ACCOUNTS)
            .map(|i| format!("account{}", i).parse().unwrap())
            .collect::<Vec<AccountId>>();
        let request = GetStateChanges {
            block_hash: block.header.hash,
            state_changes_request: StateChangesRequestView::AccountChanges {
                account_ids: accounts,
            },
        };
        let state_changes = self.send(request, ARCHIVAL_CLIENT).unwrap();
        assert_eq!(state_changes.len(), 2);
        assert!(matches!(
            state_changes[0].cause,
            StateChangeCauseView::TransactionProcessing { .. }
        ));
        assert!(matches!(
            state_changes[1].cause,
            StateChangeCauseView::TransactionProcessing { .. }
        ));
        assert!(matches!(state_changes[0].value, StateChangeValueView::AccountUpdate { .. }));
        assert!(matches!(state_changes[1].value, StateChangeValueView::AccountUpdate { .. }));
    }
}

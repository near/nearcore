//! Test bandwidth scheduler on a "real" chain.
//! Start a single node with some shards and run a workload which sends a lot of cross-shard
//! receipts. Measure bandwidth utilization and fairness and make sure that they're reasonable. It's
//! a bit tricky to achieve high utilization. You need to have a lot of concurrent transactions, and
//! even then most of the time some outgoing buffers will be empty, it's probably slowed down by
//! some limits :/ Fairness often isn't great because the number of blocks that the workload is run
//! for isn't high, and because of that there's high variance between how much each link sent. The
//! tests already take ~50 seconds to execute, so I don't want to make them any longer. Still, these
//! tests are useful to show that the scheduler works on a "real" chain. For more throughout testing
//! of the scheduler algorithm itself, check out `ChainSimulator` which tests the scheduler on
//! various scenarios and is able to run for much longer while using less CPU time.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Poll;

use bytesize::ByteSize;
use near_async::test_loop::TestLoopV2;
use near_async::test_loop::data::TestLoopData;
use near_async::test_loop::futures::TestLoopFutureSpawner;
use near_async::test_loop::sender::TestLoopSender;
use near_async::time::Duration;
use near_chain::{ChainStoreAccess, ReceiptFilter, get_incoming_receipts_for_shard};
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::{Client, RpcHandler};
use near_crypto::Signer;
use near_primitives::account::{AccessKey, AccessKeyPermission};
use near_primitives::action::{Action, FunctionCallAction};
use near_primitives::bandwidth_scheduler::{
    BandwidthRequest, BandwidthRequests, BandwidthSchedulerParams,
};
use near_primitives::block::MaybeNew;
use near_primitives::congestion_info::CongestionControl;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    ActionReceipt, Receipt, ReceiptEnum, ReceiptOrStateStoredReceipt, ReceiptV0,
};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockHeight, Nonce, ShardId, ShardIndex};
use near_store::adapter::StoreAdapter;
use near_store::trie::outgoing_metadata::{ReceiptGroupsConfig, ReceiptGroupsQueue};
use near_store::trie::receipts_column_helper::{ShardsOutgoingReceiptBuffer, TrieQueue};
use near_store::{ShardUId, Trie, TrieDBStorage};
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use testlib::bandwidth_scheduler::{
    ChunkBandwidthStats, LinkGenerators, RandomReceiptSizeGenerator, TestBandwidthStats,
    TestScenario, TestScenarioBuilder, TestSummary,
};

use crate::setup::builder::TestLoopBuilder;
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use crate::setup::state::NodeExecutionData;
use crate::utils::transactions::{TransactionRunner, run_txs_parallel};
use crate::utils::{ONE_NEAR, TGAS};

/// 3 shards, random receipt sizes
#[test]
fn ultra_slow_test_bandwidth_scheduler_three_shards_random_receipts() {
    let scenario = TestScenarioBuilder::new()
        .num_shards(3)
        .default_link_generator(|| Box::new(RandomReceiptSizeGenerator))
        .build();
    let summary = run_bandwidth_scheduler_test(scenario, 2000);
    assert!(summary.bandwidth_utilization > 0.55); // 55% utilization
    assert!(summary.link_imbalance_ratio < 1.8); // < 80% difference on links
    assert!(summary.worst_link_estimation_ratio > 0.4); // 40% of estimated link throughput
    assert!(summary.max_incoming <= summary.max_shard_bandwidth); // Incoming max_shard_bandwidth is respected
    assert!(summary.max_outgoing <= summary.max_shard_bandwidth); // Outgoing max_shard_bandwidth is respected
}

/// 4 shards, random receipt sizes, 10% probability of missing chunks
#[test]
#[ignore] // TODO: #12836
fn ultra_slow_test_bandwidth_scheduler_four_shards_random_receipts_missing_chunks() {
    let scenario = TestScenarioBuilder::new()
        .num_shards(5)
        .default_link_generator(|| Box::new(RandomReceiptSizeGenerator))
        .missing_chunk_probability(0.1)
        .build();
    let summary = run_bandwidth_scheduler_test(scenario, 2000);
    assert!(summary.bandwidth_utilization > 0.35); // 35% utilization
    assert!(summary.link_imbalance_ratio < 6.0); // < 500% difference on links
    assert!(summary.worst_link_estimation_ratio > 0.1); // 10% of estimated link throughput

    // Incoming max_shard_bandwidth is not respected! When a chunk is missing, the receipts that
    // were sent previously will arrive later and they can mix with other incoming receipts, and the
    // receiver can receive more than max_shard_bandwidth of receipts :/
    // TODO(bandwidth_scheduler) - prevent shard from having too many incoming receipts
    assert!(summary.max_incoming > summary.max_shard_bandwidth);

    // Outgoing max_shard_bandwidth is respected
    assert!(summary.max_outgoing <= summary.max_shard_bandwidth);
}

fn run_bandwidth_scheduler_test(scenario: TestScenario, tx_concurrency: usize) -> TestSummary {
    let active_links = scenario.get_active_links();
    let mut rng: ChaCha20Rng = ChaCha20Rng::seed_from_u64(0);

    // Number of blocks to run the workload for
    let workload_blocks = 50;

    // Boundary accounts between shards
    let boundary_accounts: Vec<AccountId> =
        (1..scenario.num_shards).map(|i| format!("shard{}", i).parse().unwrap()).collect();
    let shard_layout = ShardLayout::multi_shard_custom(boundary_accounts.clone(), 0);

    // Accounts that will be sending receipts to each other. One per shard.
    let workload_accounts: Vec<AccountId> = (0..scenario.num_shards)
        .map(|i| format!("shard{}_workload_sender", i).parse().unwrap())
        .collect();
    let shard_accounts: BTreeMap<ShardIndex, AccountId> = workload_accounts
        .iter()
        .map(|account| {
            let shard_id = shard_layout.account_id_to_shard_id(account);
            let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
            (shard_index, account.clone())
        })
        .collect();
    assert_eq!(shard_accounts.len() as u64, shard_layout.num_shards());

    // Account of the only producer/validator in the chain
    let node_account: AccountId = "node_account".parse().unwrap();

    let mut all_accounts = boundary_accounts;
    all_accounts.extend(workload_accounts);
    all_accounts.push(node_account.clone());

    // Choose which chunks should be missing based on `scenario.missing_chunk_probability`
    let epoch_length = 10_000;
    let mut missing_chunks_map: HashMap<ShardId, Vec<bool>> = HashMap::new();
    for shard_id in shard_layout.shard_ids() {
        let mut should_chunk_be_produced_at_height = Vec::new();
        for _ in 0..epoch_length {
            if rng.gen_bool(scenario.missing_chunk_probability) {
                should_chunk_be_produced_at_height.push(false);
            } else {
                should_chunk_be_produced_at_height.push(true);
            }
        }
        missing_chunks_map.insert(shard_id, should_chunk_be_produced_at_height);
    }
    // TODO(bandwidth_scheduler) - add support for missing block probability?

    // Build TestLoop
    let validators_spec = ValidatorsSpec::desired_roles(&[node_account.as_str()], &[]);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&all_accounts, 1_000_000 * ONE_NEAR)
        .genesis_height(10000)
        .transaction_validity_period(1000)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);

    let TestLoopEnv { mut test_loop, node_datas, shared_state } = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(vec![node_account])
        .build()
        .drop(DropCondition::ChunksProducedByHeight(missing_chunks_map))
        .warmup();

    // Initialize the workload generator.
    let mut workload_generator = WorkloadGenerator::init(
        shard_accounts,
        tx_concurrency,
        &mut test_loop,
        &node_datas,
        0,
        scenario.link_generators,
    );

    let client_handle = node_datas[0].client_sender.actor_handle();
    let tx_processor_sender = node_datas[0].rpc_handler_sender.clone();
    let future_spawner = test_loop.future_spawner("WorkloadGenerator");

    // Run the workload for a number of blocks and verify that the bandwidth requests are generated correctly.
    let mut last_height: Option<BlockHeight> = None;
    let mut first_height: Option<BlockHeight> = None;
    let testloop_func = |loop_data: &mut TestLoopData| -> bool {
        let client = &loop_data.get(&client_handle).client;

        // Keep track of the first seen/last seen heights.
        let tip = client.chain.head().unwrap();
        if first_height.is_none() {
            first_height = Some(tip.height);
        }
        if Some(tip.height) == last_height {
            // No new block, wait for the next one.
            return false;
        }
        last_height = Some(tip.height);

        tracing::info!(target: "scheduler_test", "Height: {}", tip.height);

        if tip.height - first_height.unwrap() > workload_blocks {
            return true;
        }

        // Run the transactions which generate cross-shard receipts.
        workload_generator.run(&tx_processor_sender, client, &future_spawner);

        false
    };
    test_loop.run_until(testloop_func, Duration::seconds(300));

    tracing::info!(target: "scheduler_test", "Total transactions completed: {}", workload_generator.txs_done());

    let client = &test_loop.data.get(&client_handle).client;
    let bandwidth_stats =
        analyze_workload_blocks(first_height.unwrap(), last_height.unwrap(), client);

    TestLoopEnv { test_loop, node_datas, shared_state }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));

    let summary = bandwidth_stats.summarize(&active_links);
    println!("{}", summary);
    summary
}

/// Analyze blocks and chunks produced while the workload was running
/// Gather bandwidth stats, validate bandwidth requests
fn analyze_workload_blocks(
    first_height: BlockHeight,
    last_height: BlockHeight,
    client: &Client,
) -> TestBandwidthStats {
    assert!(first_height <= last_height);

    let chain = &client.chain;
    let epoch_manager = &*client.epoch_manager;

    let tip = chain.head().unwrap();
    if tip.height < first_height {
        panic!(
            "Can't gather stats between {}..={} - tip height is {}",
            first_height, last_height, tip.height
        );
    }

    let mut block = chain.get_block(&tip.last_block_hash).unwrap();
    let mut prev_block = chain.get_block(&block.header().prev_hash()).unwrap();

    let epoch_id = epoch_manager.get_epoch_id(block.hash()).unwrap();
    let num_shards = epoch_manager.get_shard_layout(&epoch_id).unwrap().num_shards();
    let protocol_version = epoch_manager.get_epoch_protocol_version(&epoch_id).unwrap();
    let runtime_config = client.runtime_adapter.get_runtime_config(protocol_version);
    let scheduler_params =
        BandwidthSchedulerParams::new(num_shards.try_into().unwrap(), &runtime_config);
    let mut bandwidth_stats =
        TestBandwidthStats { chunk_stats: BTreeMap::new(), num_shards, scheduler_params };

    // Go over all blocks in the range
    loop {
        if block.header().height() < first_height {
            break;
        }
        let cur_shard_layout = client
            .epoch_manager
            .get_shard_layout(&epoch_manager.get_epoch_id(&block.hash()).unwrap())
            .unwrap();

        // Go over all new chunks in a block
        for chunk_header in block.chunks().iter() {
            let MaybeNew::New(new_chunk) = chunk_header else {
                continue;
            };
            let shard_id = new_chunk.shard_id();
            let shard_index = cur_shard_layout.get_shard_index(shard_id).unwrap();
            let shard_uid = ShardUId::new(cur_shard_layout.version(), shard_id);
            let prev_shard_index = epoch_manager
                .get_prev_shard_id_from_prev_hash(block.header().prev_hash(), shard_id)
                .unwrap()
                .2;
            let prev_height_included =
                prev_block.chunks().get(prev_shard_index).unwrap().height_included();

            // pre-state trie
            let store = client.chain.chain_store().store();
            let trie_storage = Arc::new(TrieDBStorage::new(store.trie_store(), shard_uid));
            let trie = Trie::new(trie_storage, new_chunk.prev_state_root(), None);

            let mut cur_chunk_stats = ChunkBandwidthStats::new();

            let congestion_info = new_chunk.congestion_info();
            let congestion_control = CongestionControl::new(
                runtime_config.congestion_control_config,
                congestion_info,
                0,
            );
            cur_chunk_stats.congestion_level = congestion_control.congestion_level();

            // Fetch incoming receipts for this chunk
            let incoming_receipts_proofs = get_incoming_receipts_for_shard(
                &chain.chain_store,
                epoch_manager,
                shard_id,
                &cur_shard_layout,
                *block.hash(),
                prev_height_included,
                ReceiptFilter::TargetShard,
            )
            .unwrap();

            // Fetch outgoing receipts generated by this chunk
            let outgoing_receipts =
                chain.chain_store.get_outgoing_receipts(&block.hash(), shard_id).unwrap();

            // Record size of incoming receipts in chunk stats
            for receipt_proof_response in incoming_receipts_proofs {
                for receipt_proof in receipt_proof_response.1.iter() {
                    for receipt in &receipt_proof.0 {
                        cur_chunk_stats.total_incoming_receipts_size +=
                            ByteSize::b(borsh::object_length(receipt).unwrap().try_into().unwrap());
                    }
                }
            }

            // Record size of outgoing receipts in chunk stats
            for receipt in outgoing_receipts.iter() {
                let receipt_size =
                    ByteSize::b(borsh::object_length(receipt).unwrap().try_into().unwrap());

                cur_chunk_stats.total_outgoing_receipts_size += receipt_size;

                let receiver_shard_index = cur_shard_layout
                    .get_shard_index(cur_shard_layout.account_id_to_shard_id(receipt.receiver_id()))
                    .unwrap();

                *cur_chunk_stats
                    .size_of_outgoing_receipts_to_shard
                    .entry(receiver_shard_index)
                    .or_insert(ByteSize::b(0)) += receipt_size;
            }

            // Extract the current bandwidth requests
            let BandwidthRequests::V1(bandwidth_requests) = new_chunk
                .bandwidth_requests()
                .expect("Bandwidth scheduler is enabled, the requests should be there");

            // Look into the outgoing buffers
            let mut outgoing_buffers = ShardsOutgoingReceiptBuffer::load(&trie).unwrap();
            for target_shard_id in outgoing_buffers.shards() {
                let target_shard_index = cur_shard_layout.get_shard_index(target_shard_id).unwrap();

                // Read sizes of receipts stored in the outgoing buffer to the target shard
                let buffered_receipt_sizes: Vec<ByteSize> = outgoing_buffers
                    .to_shard(target_shard_id)
                    .iter(&trie, false)
                    .map(|res| res.unwrap())
                    .map(|receipt| match receipt {
                        ReceiptOrStateStoredReceipt::Receipt(_) => {
                            panic!("Old receipts shouldn't occur")
                        }
                        ReceiptOrStateStoredReceipt::StateStoredReceipt(state_stored_receipt) => {
                            ByteSize::b(state_stored_receipt.metadata().congestion_size)
                        }
                    })
                    .collect();
                let mut total_size: ByteSize = ByteSize::b(0);
                for &receipt_size in &buffered_receipt_sizes {
                    total_size += receipt_size;
                }
                cur_chunk_stats
                    .size_of_buffered_receipts_to_shard
                    .insert(target_shard_index, total_size);

                let first_five_sizes: Vec<ByteSize> =
                    buffered_receipt_sizes.iter().copied().take(5).collect();
                if !first_five_sizes.is_empty() {
                    cur_chunk_stats
                        .first_five_buffered_sizes
                        .insert(target_shard_index, first_five_sizes);
                }
                let first_five_big_sizes: Vec<ByteSize> = buffered_receipt_sizes
                    .iter()
                    .copied()
                    .filter(|size| *size > ByteSize::kb(100))
                    .take(5)
                    .collect();
                if !first_five_big_sizes.is_empty() {
                    cur_chunk_stats
                        .first_five_big_buffered_sizes
                        .insert(target_shard_index, first_five_big_sizes);
                }

                // Fetch the bandwidth request generated based on this outgoing buffer
                let observed_bandwidth_request = bandwidth_requests
                    .requests
                    .iter()
                    .find(|req| ShardId::new(req.to_shard.into()) == target_shard_id)
                    .cloned();

                // Read the sizes of receipt groups corresponding to the buffered receipts
                let groups_config = ReceiptGroupsConfig::default_config();
                let receipt_group_sizes: Vec<u64> =
                    ReceiptGroupsQueue::load(&trie, target_shard_id)
                        .unwrap()
                        .unwrap()
                        .iter_receipt_group_sizes(&trie, false)
                        .map(|res| res.unwrap())
                        .collect();

                // Verify that the groups match the receipts
                assert_groups_match_receipts(
                    &buffered_receipt_sizes,
                    &receipt_group_sizes,
                    &groups_config,
                );

                // Verify that the bandwidth request is generated correctly
                let expected_bandwidth_request = BandwidthRequest::make_from_receipt_sizes(
                    target_shard_id.try_into().unwrap(),
                    receipt_group_sizes.iter().map(|size| Ok::<u64, Infallible>(*size)),
                    &scheduler_params,
                )
                .unwrap();
                assert_eq!(observed_bandwidth_request, expected_bandwidth_request);
            }

            // Save stats about this chunk
            bandwidth_stats
                .chunk_stats
                .insert((block.header().height(), shard_index), cur_chunk_stats);
        }

        block = prev_block;
        prev_block = chain.get_block(block.header().prev_hash()).unwrap();
    }

    bandwidth_stats
}

/// Verify that the group sizes are correct for the given receipt sizes.
fn assert_groups_match_receipts(
    receipt_sizes: &[ByteSize],
    group_sizes: &[u64],
    group_config: &ReceiptGroupsConfig,
) {
    let mut cur_group_remaining_size = 0;
    let mut group_sizes_iter = group_sizes.iter();
    for receipt_size in receipt_sizes.iter().map(|s| s.as_u64()) {
        if cur_group_remaining_size == 0 {
            cur_group_remaining_size = group_sizes_iter.next().copied().unwrap();
        }
        if cur_group_remaining_size > group_config.size_upper_bound.as_u64() {
            assert_eq!(cur_group_remaining_size, receipt_size);
        }

        cur_group_remaining_size -= receipt_size;
    }
    assert_eq!(cur_group_remaining_size, 0);
    assert!(group_sizes_iter.next().is_none());
}

/// Generates a workload of transactions which causes a lot of cross-shard receipts to be sent.
struct WorkloadGenerator {
    /// Accounts that will be sending receipts to each other. One per shard.
    shard_accounts: Arc<BTreeMap<ShardIndex, AccountId>>,
    /// Each sender sends one transaction at a time. There are `concurrency` many senders.
    workload_senders: Vec<WorkloadSender>,
    rng: ChaCha20Rng,
}

impl WorkloadGenerator {
    /// Init the workload generator.
    /// Deploy a contract on all workload accounts and add `concurrency` access keys the accounts.
    pub fn init(
        shard_accounts: BTreeMap<ShardIndex, AccountId>,
        concurrency: usize,
        test_loop: &mut TestLoopV2,
        node_datas: &[NodeExecutionData],
        random_seed: u64,
        link_generators: LinkGenerators,
    ) -> Self {
        let mut generator = WorkloadGenerator {
            shard_accounts: Arc::new(shard_accounts),
            workload_senders: Vec::new(),
            rng: ChaCha20Rng::seed_from_u64(random_seed),
        };

        // Deploy the test contract on all accounts
        generator.deploy_contracts(test_loop, node_datas);

        // Generate and add access keys to the workload accounts
        let account_signers = generator.generate_access_keys(test_loop, node_datas, concurrency);

        let link_generators = Rc::new(RefCell::new(link_generators));

        // Create a WorkloadSender for each pair of (account, access_key)
        for (account, signers) in account_signers {
            for signer in signers {
                generator.workload_senders.push(WorkloadSender::new(
                    account.clone(),
                    signer.into(),
                    generator.shard_accounts.clone(),
                    link_generators.clone(),
                ));
            }
        }
        tracing::info!(target: "scheduler_test", "Workload senders: {}", generator.workload_senders.len());

        generator
    }

    /// Deploy the test contract on all workload accounts
    fn deploy_contracts(&self, test_loop: &mut TestLoopV2, node_datas: &[NodeExecutionData]) {
        tracing::info!(target: "scheduler_test", "Deploying contracts...");
        let (last_block_hash, nonce) = get_last_block_and_nonce(test_loop, node_datas);
        let deploy_contracts_txs: Vec<SignedTransaction> = self
            .shard_accounts
            .values()
            .map(|account| {
                SignedTransaction::deploy_contract(
                    nonce,
                    account,
                    near_test_contracts::rs_contract().into(),
                    &create_user_test_signer(account).into(),
                    last_block_hash,
                )
            })
            .collect();
        run_txs_parallel(test_loop, deploy_contracts_txs, &node_datas, Duration::seconds(30));
        tracing::info!(target: "scheduler_test", "Contracts deployed");
    }

    /// Add `concurrency` many access keys to the workload accounts.
    /// One access key allows to run only one transaction at a time. We need to have many access keys to achieve
    /// high concurrency.
    fn generate_access_keys(
        &self,
        test_loop: &mut TestLoopV2,
        node_datas: &[NodeExecutionData],
        concurrency: usize,
    ) -> BTreeMap<AccountId, Vec<Signer>> {
        tracing::info!(target: "scheduler_test", "Adding access keys...");

        // Signers with access keys that were already added to the accounts
        let mut available_signers: BTreeMap<AccountId, Vec<Signer>> = self
            .shard_accounts
            .values()
            .map(|a| (a.clone(), vec![create_user_test_signer(&a)]))
            .collect();

        // Signers with access keys that should be added to the accounts
        let mut signers_to_add: BTreeMap<AccountId, Vec<Signer>> = BTreeMap::new();

        // The goal is to have `concurrency` many access keys, distributed evenly among the workload accounts.
        // There is already one access key per account, so we need to add `concurrency - shard_accounts.len()` more.
        for (i, account) in self
            .shard_accounts
            .values()
            .cycle()
            .take(concurrency - self.shard_accounts.len())
            .enumerate()
        {
            let signer_seed: AccountId = format!("{}_key{}", account, i).parse().unwrap();
            let new_signer = create_user_test_signer(&signer_seed);
            signers_to_add.entry(account.clone()).or_insert_with(Vec::new).push(new_signer);
        }

        // Use the available access keys to add new access keys
        // Repeat until all access keys are added
        while !signers_to_add.iter().all(|(_, to_add)| to_add.is_empty()) {
            let mut add_key_txs: Vec<SignedTransaction> = Vec::new();
            let mut new_signers = Vec::new();

            let (last_block_hash, nonce) = get_last_block_and_nonce(test_loop, node_datas);
            tracing::info!(target: "scheduler_test", "Adding access keys with nonce {}", nonce);

            for (account, usable_signers) in &available_signers {
                let Some(to_add) = signers_to_add.get_mut(account) else {
                    continue;
                };

                for usable_signer in usable_signers {
                    let Some(new_signer) = to_add.pop() else {
                        break;
                    };

                    add_key_txs.push(SignedTransaction::add_key(
                        nonce,
                        account.clone(),
                        &usable_signer.clone().into(),
                        new_signer.public_key(),
                        AccessKey { nonce: 0, permission: AccessKeyPermission::FullAccess },
                        last_block_hash,
                    ));

                    new_signers.push((account.clone(), new_signer));
                }
            }

            run_txs_parallel(test_loop, add_key_txs, node_datas, Duration::seconds(20));

            tracing::info!(target: "scheduler_test", "Added {} access keys", new_signers.len());
            for (account, new_signer) in new_signers {
                available_signers.entry(account).or_insert_with(Vec::new).push(new_signer);
            }
        }

        available_signers
    }

    pub fn run(
        &mut self,
        client_sender: &TestLoopSender<RpcHandler>,
        client: &Client,
        future_spawner: &TestLoopFutureSpawner,
    ) {
        for sender in &mut self.workload_senders {
            sender.run(client_sender, client, future_spawner, &mut self.rng);
        }
    }

    pub fn txs_done(&self) -> u64 {
        self.workload_senders.iter().map(|s| s.txs_done).sum()
    }
}

/// Runs one transaction at a time using unique pair of (account, access_key)
/// Generates infinitely many transactions and runs them one by one.
struct WorkloadSender {
    account: AccountId,
    signer: Signer,
    shard_accounts: Arc<BTreeMap<ShardIndex, AccountId>>,
    link_generators: Rc<RefCell<LinkGenerators>>,
    tx_runner: Option<TransactionRunner>,
    txs_done: u64,
    my_sender_shard_index: ShardIndex,
}

impl WorkloadSender {
    pub fn new(
        account: AccountId,
        signer: Signer,
        shard_accounts: Arc<BTreeMap<ShardIndex, AccountId>>,
        link_generators: Rc<RefCell<LinkGenerators>>,
    ) -> Self {
        let my_sender_shard_index = *shard_accounts
            .iter()
            .find(|(_shard_index, shard_account)| **shard_account == account)
            .unwrap()
            .0;
        WorkloadSender {
            account,
            signer,
            shard_accounts,
            tx_runner: None,
            txs_done: 0,
            link_generators,
            my_sender_shard_index,
        }
    }

    pub fn run(
        &mut self,
        client_sender: &TestLoopSender<RpcHandler>,
        client: &Client,
        future_spawner: &TestLoopFutureSpawner,
        rng: &mut ChaCha20Rng,
    ) {
        match self.tx_runner {
            Some(ref mut tx_runner) => {
                match tx_runner.poll_assert_success(client_sender, client, future_spawner) {
                    Poll::Pending => {}
                    Poll::Ready(_) => {
                        self.txs_done += 1;
                        self.start_new_transaction(client_sender, client, future_spawner, rng)
                    }
                }
            }
            None => self.start_new_transaction(client_sender, client, future_spawner, rng),
        }
    }

    pub fn start_new_transaction(
        &mut self,
        client_sender: &TestLoopSender<RpcHandler>,
        client: &Client,
        future_spawner: &TestLoopFutureSpawner,
        rng: &mut ChaCha20Rng,
    ) {
        self.tx_runner = None;

        // Generate a new transaction
        // The transaction will do a function call to a contract deployed on this sender's account.
        // The contract will send a receipt with the desired size to a random receiver account.
        let mut link_generators_mut = self.link_generators.borrow_mut();
        let Some(my_link_senders) = link_generators_mut.get_mut(&self.my_sender_shard_index) else {
            // my_sender_shard_index isn't supposed to send any receipts to other shards in this scenario.
            return;
        };
        if my_link_senders.is_empty() {
            // my_sender_shard_index isn't supposed to send any receipts to other shards in this scenario.
            return;
        }
        let (receiver_index, link_generator) = my_link_senders.choose_mut(rng).unwrap();
        let receiver_account = self.shard_accounts.get(receiver_index).unwrap();

        let target_receipt_size = link_generator.generate_receipt_size(rng);
        let (last_block_hash, nonce) = get_last_block_and_nonce_from_client(client);
        let tx = make_send_receipt_transaction(
            self.account.clone(),
            &self.signer,
            receiver_account.clone(),
            target_receipt_size,
            nonce,
            last_block_hash,
        );

        // Start the transaction
        let mut tx_runner = TransactionRunner::new(tx, true);
        let _poll_res = tx_runner.poll_assert_success(client_sender, client, future_spawner);
        self.tx_runner = Some(tx_runner);
    }
}

// Create a transaction which is sent from the `sender_account` and calls the
// `do_function_call_with_args_of_size` method on contract deployed on `sender_account`.
// `do_function_call_with_args_of_size` method sends a receipt with the desired size to `receiver_account`.
// The effect is that `sender_account` sends a receipt with the desired size to `receiver_account`.
// When sender and receiver are on different shards, this generates a cross-shard receipt.
fn make_send_receipt_transaction(
    sender_account: AccountId,
    sender_signer: &Signer,
    receiver_account: AccountId,
    target_receipt_size: ByteSize,
    nonce: Nonce,
    last_block_hash: CryptoHash,
) -> SignedTransaction {
    // The receipt sent to `receiver_account` will call the `noop` method.
    let method_name = "noop".to_string();

    // Find out how large a basic receipt with empty arguments will be.
    let base_receipt_size = borsh::object_length(&Receipt::V0(ReceiptV0 {
        predecessor_id: sender_account.clone(),
        receiver_id: receiver_account.clone(),
        receipt_id: CryptoHash::default(),
        receipt: ReceiptEnum::Action(ActionReceipt {
            signer_id: sender_account.clone(),
            signer_public_key: sender_signer.public_key(),
            gas_price: 0,
            output_data_receivers: vec![],
            input_data_ids: vec![],
            actions: vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: method_name.clone(),
                args: Vec::new(),
                gas: 0,
                deposit: 0,
            }))],
        }),
    }))
    .unwrap();

    // Choose the size of the arguments so that the total receipt size is `target_receipt_size`.
    let args_size = target_receipt_size.as_u64().saturating_sub(base_receipt_size as u64);

    SignedTransaction::call(
        nonce,
        sender_account.clone(),
        sender_account,
        &sender_signer,
        0,
        "do_function_call_with_args_of_size".to_string(),
        serde_json::json!({
            "account_id": receiver_account,
            "method_name": method_name,
            "args_size": args_size
        })
        .to_string()
        .into_bytes(),
        300 * TGAS,
        last_block_hash,
    )
}

/// When a new access key is added, its nonce is set to `height * ACCESS_KEY_NONCE_RANGE_MULTIPLIER`.
/// This makes it hard to predict what nonce should be used for transactions that use this key,
/// as it depends on the height at which the key was added. We could query the nonce, but it's
/// easier to use a value which is guaranteed to be higher than the nonce of the key.
/// That value is tip_height * ACCESS_KEY_NONCE_RANGE_MULTIPLIER + 1.
/// The access key was added before tip_height, so this nonce will be large enough and also not too big
/// (nonces that are too large are rejected)
fn calculate_tx_nonce_for_height(tip_height: BlockHeight) -> Nonce {
    tip_height * near_primitives::account::AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER + 1
}

fn get_last_block_and_nonce_from_client(client: &Client) -> (CryptoHash, Nonce) {
    let tip = client.chain.head().unwrap();
    let last_block_hash = tip.last_block_hash;
    let nonce = calculate_tx_nonce_for_height(tip.height);
    (last_block_hash, nonce)
}

fn get_last_block_and_nonce(
    test_loop: &TestLoopV2,
    node_datas: &[NodeExecutionData],
) -> (CryptoHash, Nonce) {
    let client = &test_loop.data.get(&node_datas[0].client_sender.actor_handle()).client;
    get_last_block_and_nonce_from_client(client)
}

use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::task::Poll;

use bytesize::ByteSize;
use near_async::test_loop::data::TestLoopData;
use near_async::test_loop::futures::TestLoopFututeSpawner;
use near_async::test_loop::sender::TestLoopSender;
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_client::client_actor::ClientActorInner;
use near_client::Client;
use near_crypto::{InMemorySigner, Signer};
use near_o11y::testonly::init_test_logger;
use near_primitives::account::{AccessKey, AccessKeyPermission};
use near_primitives::action::{Action, FunctionCallAction};
use near_primitives::bandwidth_scheduler::{
    BandwidthRequest, BandwidthRequests, BandwidthSchedulerParams,
};
use near_primitives::block::MaybeNew;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    ActionReceipt, Receipt, ReceiptEnum, ReceiptOrStateStoredReceipt, ReceiptV0,
};
use near_primitives::shard_layout::{account_id_to_shard_id, ShardLayout};
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockHeight, Nonce, ShardId};
use near_primitives::version::{ProtocolFeature, PROTOCOL_VERSION};
use near_store::adapter::StoreAdapter;
use near_store::trie::outgoing_metadata::{ReceiptGroupsConfig, ReceiptGroupsQueue};
use near_store::trie::receipts_column_helper::{ShardsOutgoingReceiptBuffer, TrieQueue};
use near_store::{ShardUId, Trie, TrieDBStorage};
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use testlib::bandwidth_scheduler::get_random_receipt_size_for_test;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::{TestData, TestLoopEnv};
use crate::test_loop::utils::transactions::{run_txs_parallel, TransactionRunner};
use crate::test_loop::utils::{ONE_NEAR, TGAS};

/// 1 node, 3 shards
/// Lots of transactions which generate congestion and buffered cross-shard receipts.
/// Verify that the generated bandwidth requests are correct.
#[test]
fn slow_test_bandwidth_scheduler_request_generation() {
    if !ProtocolFeature::BandwidthScheduler.enabled(PROTOCOL_VERSION) {
        return;
    }

    init_test_logger();

    let num_shards = 3;

    // Number of blocks to run the workload for
    let workload_blocks = 10;

    // Number of transactions to run in parallel
    // Each workload_account will run concurrency/num_shards transactions at once.
    let concurrency = 5000;

    // Boundary accounts between shards
    let boundary_accounts: Vec<AccountId> =
        (1..num_shards).map(|i| format!("shard{}", i).parse().unwrap()).collect();
    let shard_layout = ShardLayout::multi_shard_custom(boundary_accounts.clone(), 0);

    // Accounts that will be sending receipts to each other. One per shard.
    let workload_accounts: Vec<AccountId> =
        (0..num_shards).map(|i| format!("shard{}_workload_sender", i).parse().unwrap()).collect();
    let workload_accounts_shards: BTreeSet<ShardId> =
        workload_accounts.iter().map(|a| account_id_to_shard_id(a, &shard_layout)).collect();
    assert_eq!(workload_accounts_shards, shard_layout.shard_ids().collect::<BTreeSet<_>>());

    // Account of the only producer/validator in the chain
    let node_account: AccountId = "node_account".parse().unwrap();

    let mut all_accounts = boundary_accounts;
    all_accounts.extend(workload_accounts.clone());
    all_accounts.push(node_account.clone());

    let builder = TestLoopBuilder::new();
    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&builder.clock())
        .protocol_version_latest()
        .genesis_height(10000)
        .gas_limit_one_petagas()
        .shard_layout(shard_layout)
        .transaction_validity_period(1000)
        .epoch_length(10000)
        .validators_desired_roles(&[node_account.as_str()], &[]);
    for account in all_accounts {
        genesis_builder.add_user_account_simple(account.clone(), 100_000_0000 * ONE_NEAR);
    }

    let (genesis, epoch_config_store) = genesis_builder.build();

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(vec![node_account])
        .build();

    // Initialize the workload generator.
    // Adds a lot of access keys to workload accounts to allow submitting lots of transactions in parallel.
    let mut workload_generator =
        WorkloadGenerator::init(&workload_accounts, concurrency, &mut test_loop, &node_datas, 0);

    let client_handle = node_datas[0].client_sender.actor_handle();
    let client_sender = node_datas[0].client_sender.clone();
    let future_spawner = test_loop.future_spawner();
    let mut stats = TestStats::default();

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
        workload_generator.run(&client_sender, client, &future_spawner);

        // Verify the generated bandwidth requests at this height.
        verify_bandwidth_requests(client, &mut stats);

        false
    };
    test_loop.run_until(testloop_func, Duration::seconds(30));

    stats.total_transactions_completed = workload_generator.txs_done();
    tracing::info!(target: "scheduler_test", "Total transactions completed: {}", stats.total_transactions_completed);

    // Make sure that the test covers the interesting cases
    assert!(stats.saw_receipts_below_base_bandwidth);
    assert!(stats.saw_total_receipts_size_above_max_shard_bandwidth);
    assert!(stats.saw_max_size_receipt);

    // The congestion is so high that it takes tens of blocks for the first transactions
    // to be completed. `total_transactions_completed` will likely be 0 at the end of the test.
    // assert!(stats.total_transactions_completed > 100);

    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

#[derive(Default)]
struct TestStats {
    saw_receipts_below_base_bandwidth: bool,
    saw_total_receipts_size_above_max_shard_bandwidth: bool,
    saw_max_size_receipt: bool,
    total_transactions_completed: u64,
}

fn verify_bandwidth_requests(client: &Client, stats: &mut TestStats) {
    // Gather chain information needed to verify the requests
    let tip = client.chain.head().unwrap();
    let last_block = client.chain.get_block(&tip.last_block_hash).unwrap();
    let epoch_manager = &client.epoch_manager;
    let epoch_id = epoch_manager.get_epoch_id(&tip.last_block_hash).unwrap();
    let protocol_version = epoch_manager.get_epoch_protocol_version(&epoch_id).unwrap();
    let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
    let layout_version = shard_layout.version();
    let runtime_config = client.chain.runtime_adapter.get_runtime_config(protocol_version).unwrap();
    let num_shards: u64 = shard_layout.shard_ids().count().try_into().unwrap();
    let scheduler_params =
        BandwidthSchedulerParams::new(NonZeroU64::new(num_shards).unwrap(), &runtime_config);

    tracing::info!(target: "scheduler_test", "Current scheduler params: {:?}", scheduler_params);

    // Verify the requests in each chunk of the last block
    for chunk_header in last_block.chunks().iter() {
        match chunk_header {
            MaybeNew::Old(_) => panic!("Chunks should not be missing"),
            MaybeNew::New(new_chunk_header) => {
                let shard_uid = ShardUId::new(layout_version, new_chunk_header.shard_id());
                verify_bandwidth_requests_in_chunk(
                    new_chunk_header,
                    shard_uid,
                    client,
                    &scheduler_params,
                    stats,
                )
            }
        };
    }
}

fn verify_bandwidth_requests_in_chunk(
    header: &ShardChunkHeader,
    shard_uid: ShardUId,
    client: &Client,
    scheduler_params: &BandwidthSchedulerParams,
    stats: &mut TestStats,
) {
    let BandwidthRequests::V1(bandwidth_requests) = header
        .bandwidth_requests()
        .expect("Bandwidth scheduler is enabled, the requests should be there");
    let state_root = header.prev_state_root();

    let store = client.chain.chain_store().store();
    let trie_storage = Arc::new(TrieDBStorage::new(store.trie_store(), shard_uid));
    let trie = Trie::new(trie_storage, state_root, None);

    let mut outgoing_buffers = ShardsOutgoingReceiptBuffer::load(&trie).unwrap();

    for target_shard_id in outgoing_buffers.shards() {
        tracing::info!(target: "scheduler_test", "Verifying bandwidth requests {} -> {} ", shard_uid, target_shard_id);

        // Read sizes of receipts stored in the outgoing buffer to the target shard
        let buffered_receipt_sizes: Vec<u64> = outgoing_buffers
            .to_shard(target_shard_id)
            .iter(&trie, false)
            .map(|res| res.unwrap())
            .map(|receipt| match receipt {
                ReceiptOrStateStoredReceipt::Receipt(_) => panic!("Old receipts shouldn't occur"),
                ReceiptOrStateStoredReceipt::StateStoredReceipt(state_stored_receipt) => {
                    //tracing::info!(target: "scheduler_test", "Receipt: {:#?}", state_stored_receipt);
                    state_stored_receipt.metadata().congestion_size
                }
            })
            .collect();
        let total_size = buffered_receipt_sizes.iter().sum::<u64>();

        let observed_bandwidth_request = bandwidth_requests
            .requests
            .iter()
            .find(|req| ShardId::new(req.to_shard.into()) == target_shard_id)
            .cloned();

        if total_size < scheduler_params.base_bandwidth {
            assert_eq!(observed_bandwidth_request, None);
            stats.saw_receipts_below_base_bandwidth = true;
        }

        if total_size > scheduler_params.max_shard_bandwidth {
            stats.saw_total_receipts_size_above_max_shard_bandwidth = true;
        }

        if buffered_receipt_sizes.contains(&scheduler_params.max_receipt_size) {
            stats.saw_max_size_receipt = true;
        }

        tracing::info!(target: "scheduler_test", "Buffered receipts: len: {}, total size: {}, first_ten_sizes: {:?}", buffered_receipt_sizes.len(), ByteSize::b(total_size), &buffered_receipt_sizes[..std::cmp::min(10, buffered_receipt_sizes.len())]);

        // Read the sizes of receipt groups corresponding to the buffered receipts
        let groups_config = ReceiptGroupsConfig::default_config();
        let receipt_group_sizes: Vec<u64> = ReceiptGroupsQueue::load(&trie, target_shard_id)
            .unwrap()
            .unwrap()
            .iter_receipt_group_sizes(&trie, false)
            .map(|res| res.unwrap())
            .collect();

        // Verify that the groups match the receipts
        assert_groups_match_receipts(&buffered_receipt_sizes, &receipt_group_sizes, &groups_config);

        // Verify that the bandwidth request is generated correctly
        let expected_bandwidth_request = BandwidthRequest::make_from_receipt_sizes(
            target_shard_id.try_into().unwrap(),
            receipt_group_sizes.iter().map(|size| Ok::<u64, Infallible>(*size)),
            scheduler_params,
        )
        .unwrap();
        assert_eq!(observed_bandwidth_request, expected_bandwidth_request);
    }
}

/// Verify that the group sizes are correct for the given receipt sizes.
fn assert_groups_match_receipts(
    receipt_sizes: &[u64],
    group_sizes: &[u64],
    group_config: &ReceiptGroupsConfig,
) {
    let mut cur_group_remaining_size = 0;
    let mut group_sizes_iter = group_sizes.iter();
    for receipt_size in receipt_sizes {
        if cur_group_remaining_size == 0 {
            cur_group_remaining_size = group_sizes_iter.next().copied().unwrap();
        }
        if cur_group_remaining_size > group_config.size_upper_bound.as_u64() {
            assert_eq!(cur_group_remaining_size, *receipt_size);
        }

        cur_group_remaining_size -= receipt_size;
    }
    assert_eq!(cur_group_remaining_size, 0);
    assert!(group_sizes_iter.next().is_none());
}

/// Generates a workload of transactions which causes a lot of cross-shard receipts to be sent.
struct WorkloadGenerator {
    /// Accounts that will be sending receipts to each other.
    workload_accounts: Arc<[AccountId]>,
    /// Each sender sends one transaction at a time. There are `concurrency` many senders.
    workload_senders: Vec<WorkloadSender>,
    rng: ChaCha20Rng,
}

impl WorkloadGenerator {
    /// Init the workload generator.
    /// Deploy a contract on all workload accounts and add `concurrency` access keys the accounts.
    pub fn init(
        workload_accounts: &[AccountId],
        concurrency: usize,
        test_loop: &mut TestLoopV2,
        node_datas: &[TestData],
        random_seed: u64,
    ) -> Self {
        let mut generator = WorkloadGenerator {
            workload_accounts: workload_accounts.into(),
            workload_senders: Vec::new(),
            rng: ChaCha20Rng::seed_from_u64(random_seed),
        };

        // Deploy the test contract on all accounts
        generator.deploy_contracts(test_loop, node_datas);

        // Generate and add access keys to the workload accounts
        let account_signers = generator.generate_access_keys(test_loop, node_datas, concurrency);

        // Create a WorkloadSender for each pair of (account, access_key)
        for (account, signers) in account_signers {
            for signer in signers {
                generator.workload_senders.push(WorkloadSender::new(
                    account.clone(),
                    signer.into(),
                    generator.workload_accounts.clone(),
                ));
            }
        }
        tracing::info!(target: "scheduler_test", "Workload senders: {}", generator.workload_senders.len());

        generator
    }

    /// Deploy the test contract on all workload accounts
    fn deploy_contracts(&mut self, test_loop: &mut TestLoopV2, node_datas: &[TestData]) {
        tracing::info!(target: "scheduler_test", "Deploying contracts...");
        let (last_block_hash, nonce) = get_last_block_and_nonce(test_loop, node_datas);
        let deploy_contracts_txs: Vec<SignedTransaction> = self
            .workload_accounts
            .iter()
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
        &mut self,
        test_loop: &mut TestLoopV2,
        node_datas: &[TestData],
        concurrency: usize,
    ) -> BTreeMap<AccountId, Vec<InMemorySigner>> {
        tracing::info!(target: "scheduler_test", "Adding access keys...");

        // Signers with access keys that were already added to the accounts
        let mut available_signers: BTreeMap<AccountId, Vec<InMemorySigner>> = self
            .workload_accounts
            .iter()
            .map(|a| (a.clone(), vec![create_user_test_signer(&a)]))
            .collect();

        // Signers with access keys that should be added to the accounts
        let mut signers_to_add: BTreeMap<AccountId, Vec<InMemorySigner>> = BTreeMap::new();

        // The goal is to have `concurrency` many access keys, distributed evenly among the workload accounts.
        // There is already one access key per account, so we need to add `concurrency - workload_accounts.len()` more.
        for i in self.workload_accounts.len()..concurrency {
            let account = self.workload_accounts[i % self.workload_accounts.len()].clone();
            let signer_seed: AccountId = format!("{}_key{}", account, i).parse().unwrap();
            let new_signer = create_user_test_signer(&signer_seed);
            signers_to_add.entry(account).or_insert_with(Vec::new).push(new_signer);
        }

        // Use the available access keys to add new access keys
        // Repeat until all access keys are added
        while !signers_to_add.iter().all(|(_, to_add)| to_add.is_empty()) {
            let mut add_key_txs: Vec<SignedTransaction> = Vec::new();
            let mut new_signers = Vec::new();

            let (last_block_hash, nonce) = get_last_block_and_nonce(test_loop, node_datas);
            tracing::info!(target: "scheduler_test", "Adding access keys with nonce {}", nonce);

            for (account, usable_signers) in available_signers.iter() {
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
        client_sender: &TestLoopSender<ClientActorInner>,
        client: &Client,
        future_spawner: &TestLoopFututeSpawner,
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
    workload_accounts: Arc<[AccountId]>,
    tx_runner: Option<TransactionRunner>,
    txs_done: u64,
}

impl WorkloadSender {
    pub fn new(account: AccountId, signer: Signer, workload_accounts: Arc<[AccountId]>) -> Self {
        WorkloadSender { account, signer, workload_accounts, tx_runner: None, txs_done: 0 }
    }

    pub fn run(
        &mut self,
        client_sender: &TestLoopSender<ClientActorInner>,
        client: &Client,
        future_spawner: &TestLoopFututeSpawner,
        rng: &mut impl Rng,
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
        client_sender: &TestLoopSender<ClientActorInner>,
        client: &Client,
        future_spawner: &TestLoopFututeSpawner,
        rng: &mut impl Rng,
    ) {
        // Generate a new transaction
        // The transaction will do a function call to a contract deployed on this sender's account.
        // The contract will send a receipt with the desired size to a random receiver account.
        let receiver_account = self.workload_accounts.choose(rng).unwrap();
        let target_receipt_size = get_random_receipt_size_for_test(rng);
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
    target_receipt_size: u64,
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
    let args_size = target_receipt_size.saturating_sub(base_receipt_size as u64);

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
    node_datas: &[TestData],
) -> (CryptoHash, Nonce) {
    let client = &test_loop.data.get(&node_datas[0].client_sender.actor_handle()).client;
    get_last_block_and_nonce_from_client(client)
}

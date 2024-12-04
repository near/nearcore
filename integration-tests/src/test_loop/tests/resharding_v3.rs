use borsh::BorshDeserialize;
use itertools::Itertools;
use near_async::test_loop::data::{TestLoopData, TestLoopDataHandle};
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_chain_configs::DEFAULT_GC_NUM_EPOCHS_TO_KEEP;
use near_client::Client;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::Tip;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{account_id_to_shard_uid, ShardLayout};
use near_primitives::state_record::StateRecord;
use near_primitives::types::{AccountId, BlockHeightDelta, EpochId, Gas, ShardId};
use near_primitives::version::{ProtocolFeature, PROTOCOL_VERSION};
use near_store::adapter::StoreAdapter;
use near_store::db::refcount::decode_value_with_rc;
use near_store::{get, DBCol, ShardUId, Trie};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::{TestData, TestLoopEnv};
use crate::test_loop::utils::transactions::{
    get_shared_block_hash, get_smallest_height_head, run_tx, submit_tx,
};
use crate::test_loop::utils::{ONE_NEAR, TGAS};
use assert_matches::assert_matches;
use near_client::client_actor::ClientActorInner;
use near_crypto::Signer;
use near_epoch_manager::EpochManagerAdapter;
use near_parameters::{RuntimeConfig, RuntimeConfigStore};
use near_primitives::receipt::{BufferedReceiptIndices, DelayedReceiptIndices};
use near_primitives::state::FlatStateValue;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::trie_key::TrieKey;
use near_primitives::views::FinalExecutionStatus;
use std::cell::Cell;
use std::u64;

fn client_tracking_shard(client: &Client, shard_id: ShardId, parent_hash: &CryptoHash) -> bool {
    let signer = client.validator_signer.get();
    let account_id = signer.as_ref().map(|s| s.validator_id());
    client.shard_tracker.care_about_shard(account_id, parent_hash, shard_id, true)
}

fn get_client_tracking_shard<'a>(
    clients: &'a [&Client],
    tip: &Tip,
    shard_id: ShardId,
) -> &'a Client {
    for client in clients {
        if client_tracking_shard(client, shard_id, &tip.prev_block_hash) {
            return client;
        }
    }
    panic!(
        "get_client_tracking_shard() could not find client tracking shard {} at {} #{}",
        shard_id, &tip.last_block_hash, tip.height
    );
}

fn print_and_assert_shard_accounts(clients: &[&Client], tip: &Tip) {
    let epoch_config = clients[0].epoch_manager.get_epoch_config(&tip.epoch_id).unwrap();
    for shard_uid in epoch_config.shard_layout.shard_uids() {
        let client = get_client_tracking_shard(clients, tip, shard_uid.shard_id());
        let chunk_extra = client.chain.get_chunk_extra(&tip.prev_block_hash, &shard_uid).unwrap();
        let trie = client
            .runtime_adapter
            .get_trie_for_shard(
                shard_uid.shard_id(),
                &tip.prev_block_hash,
                *chunk_extra.state_root(),
                false,
            )
            .unwrap();
        let mut shard_accounts = vec![];
        for item in trie.lock_for_iter().iter().unwrap() {
            let (key, value) = item.unwrap();
            let state_record = StateRecord::from_raw_key_value(key, value);
            if let Some(StateRecord::Account { account_id, .. }) = state_record {
                shard_accounts.push(account_id.to_string());
            }
        }
        println!("accounts for shard {}: {:?}", shard_uid, shard_accounts);
        assert!(!shard_accounts.is_empty());
    }
}

/// Asserts that all parent shard State is accessible via parent and children shards.
fn check_state_shard_uid_mapping_after_resharding(client: &Client, parent_shard_uid: ShardUId) {
    let tip = client.chain.head().unwrap();
    let epoch_id = tip.epoch_id;
    let epoch_config = client.epoch_manager.get_epoch_config(&epoch_id).unwrap();
    let children_shard_uids =
        epoch_config.shard_layout.get_children_shards_uids(parent_shard_uid.shard_id()).unwrap();
    assert_eq!(children_shard_uids.len(), 2);

    let store = client.chain.chain_store.store().trie_store();
    for kv in store.store().iter_raw_bytes(DBCol::State) {
        let (key, value) = kv.unwrap();
        let shard_uid = ShardUId::try_from_slice(&key[0..8]).unwrap();
        // Just after resharding, no State data must be keyed using children ShardUIds.
        assert!(!children_shard_uids.contains(&shard_uid));
        if shard_uid != parent_shard_uid {
            continue;
        }
        let node_hash = CryptoHash::try_from_slice(&key[8..]).unwrap();
        let (value, _) = decode_value_with_rc(&value);
        let parent_value = store.get(parent_shard_uid, &node_hash);
        // Parent shard data must still be accessible using parent ShardUId.
        assert_eq!(&parent_value.unwrap()[..], value.unwrap());
        // All parent shard data is available via both children shards.
        for child_shard_uid in &children_shard_uids {
            let child_value = store.get(*child_shard_uid, &node_hash);
            assert_eq!(&child_value.unwrap()[..], value.unwrap());
        }
    }
}

/// Signature of functions callable from inside the inner loop of the resharding suite of tests.
type LoopActionFn =
    Box<dyn Fn(&[TestData], &mut TestLoopData, TestLoopDataHandle<ClientActorInner>)>;

#[derive(Default)]
struct TestReshardingParameters {
    chunk_ranges_to_drop: HashMap<ShardUId, std::ops::Range<i64>>,
    accounts: Vec<AccountId>,
    clients: Vec<AccountId>,
    block_and_chunk_producers: Vec<AccountId>,
    initial_balance: u128,
    epoch_length: BlockHeightDelta,
    shuffle_shard_assignment_for_chunk_producers: bool,
    track_all_shards: bool,
    load_mem_tries_for_tracked_shards: bool,
    /// Custom behavior executed at every iteration of test loop.
    loop_actions: Vec<LoopActionFn>,
    // When enabling shard shuffling with a short epoch length, sometimes a node might not finish
    // catching up by the end of the epoch, and then misses a chunk. This can be fixed by using a longer
    // epoch length, but it's good to also check what happens with shorter ones.
    all_chunks_expected: bool,
    /// Optionally deploy the test contract
    /// (see nearcore/runtime/near-test-contracts/test-contract-rs/src/lib.rs) on the provided accounts.
    deploy_test_contract: Vec<AccountId>,
    /// Enable a stricter limit on outgoing gas to easily trigger congestion control.
    limit_outgoing_gas: bool,
    /// If non zero, split parent shard for flat state resharding will be delayed by an additional
    /// `BlockHeightDelta` number of blocks. Useful to simulate slower task completion.
    delay_flat_state_resharding: BlockHeightDelta,
}

impl TestReshardingParameters {
    fn new() -> Self {
        Self::with_clients(3)
    }

    fn with_clients(num_clients: u64) -> Self {
        let num_accounts = 8;
        let initial_balance = 1_000_000 * ONE_NEAR;
        let epoch_length = 6;
        let track_all_shards = true;
        let all_chunks_expected = true;

        // #12195 prevents number of BPs bigger than `epoch_length`.
        assert!(num_clients > 0 && num_clients <= epoch_length);

        let accounts = (0..num_accounts)
            .map(|i| format!("account{}", i).parse().unwrap())
            .collect::<Vec<AccountId>>();

        // This piece of code creates `num_clients` from `accounts`. First client is at index 0 and
        // other clients are spaced in the accounts' space as evenly as possible.
        let clients_per_account = num_clients as f64 / accounts.len() as f64;
        let mut client_parts = 1.0 - clients_per_account;
        let clients: Vec<_> = accounts
            .iter()
            .filter(|_| {
                client_parts += clients_per_account;
                if client_parts >= 1.0 {
                    client_parts -= 1.0;
                    true
                } else {
                    false
                }
            })
            .cloned()
            .collect();

        let block_and_chunk_producers = clients.clone();
        let load_mem_tries_for_tracked_shards = true;

        Self {
            accounts,
            clients,
            block_and_chunk_producers,
            initial_balance,
            epoch_length,
            track_all_shards,
            all_chunks_expected,
            load_mem_tries_for_tracked_shards,
            ..Default::default()
        }
    }

    fn chunk_ranges_to_drop(
        mut self,
        chunk_ranges_to_drop: HashMap<ShardUId, std::ops::Range<i64>>,
    ) -> Self {
        self.chunk_ranges_to_drop = chunk_ranges_to_drop;
        self
    }

    #[allow(unused)]
    fn clients(mut self, clients: Vec<AccountId>) -> Self {
        self.clients = clients;
        self
    }

    #[allow(unused)]
    fn block_and_chunk_producers(mut self, block_and_chunk_producers: Vec<AccountId>) -> Self {
        self.block_and_chunk_producers = block_and_chunk_producers;
        self
    }

    fn add_loop_action(mut self, loop_action: LoopActionFn) -> Self {
        self.loop_actions.push(loop_action);
        self
    }

    fn shuffle_shard_assignment(mut self) -> Self {
        self.shuffle_shard_assignment_for_chunk_producers = true;
        self
    }

    fn single_shard_tracking(mut self) -> Self {
        self.track_all_shards = false;
        self
    }

    fn chunk_miss_possible(mut self) -> Self {
        self.all_chunks_expected = false;
        self
    }

    fn deploy_test_contract(mut self, account_id: AccountId) -> Self {
        self.deploy_test_contract.push(account_id);
        self
    }

    fn limit_outgoing_gas(mut self) -> Self {
        self.limit_outgoing_gas = true;
        self
    }

    fn load_mem_tries_for_tracked_shards(
        mut self,
        load_mem_tries_for_tracked_shards: bool,
    ) -> Self {
        self.load_mem_tries_for_tracked_shards = load_mem_tries_for_tracked_shards;
        self
    }

    #[allow(unused)]
    fn delay_flat_state_resharding(mut self, num_blocks: BlockHeightDelta) -> Self {
        self.delay_flat_state_resharding = num_blocks;
        self
    }
}

// Returns a callable function that, when invoked inside a test loop iteration, can force the creation of a chain fork.
#[cfg(feature = "test_features")]
fn fork_before_resharding_block(double_signing: bool) -> LoopActionFn {
    use near_client::client_actor::AdvProduceBlockHeightSelection;

    let done = Cell::new(false);
    Box::new(
        move |_: &[TestData],
              test_loop_data: &mut TestLoopData,
              client_handle: TestLoopDataHandle<ClientActorInner>| {
            // It must happen only for the first resharding block encountered.
            if done.get() {
                return;
            }

            let client_actor = &mut test_loop_data.get_mut(&client_handle);
            let tip = client_actor.client.chain.head().unwrap();

            // If there's a new shard layout force a chain fork.
            if next_block_has_new_shard_layout(client_actor.client.epoch_manager.as_ref(), &tip) {
                println!("creating chain fork at height {}", tip.height);
                let height_selection = if double_signing {
                    // In the double signing scenario we want a new block on top of prev block, with consecutive height.
                    AdvProduceBlockHeightSelection::NextHeightOnSelectedBlock {
                        base_block_height: tip.height - 1,
                    }
                } else {
                    // To avoid double signing skip already produced height.
                    AdvProduceBlockHeightSelection::SelectedHeightOnSelectedBlock {
                        produced_block_height: tip.height + 1,
                        base_block_height: tip.height - 1,
                    }
                };
                client_actor.adv_produce_blocks_on(3, true, height_selection);
                done.set(true);
            }
        },
    )
}

enum ReceiptKind {
    Delayed,
    Buffered,
}

/// Checks that the shard containing `account` has a non empty set of receipts
/// of type `kind` at the resharding block.
fn check_receipts_presence_at_resharding_block(
    account: AccountId,
    kind: ReceiptKind,
) -> LoopActionFn {
    Box::new(
        move |_: &[TestData],
              test_loop_data: &mut TestLoopData,
              client_handle: TestLoopDataHandle<ClientActorInner>| {
            let client_actor = test_loop_data.get_mut(&client_handle);
            let tip = client_actor.client.chain.head().unwrap();

            if !next_block_has_new_shard_layout(client_actor.client.epoch_manager.as_ref(), &tip) {
                return;
            }

            check_receipts_at_block(client_actor, &account, &kind, tip);
        },
    )
}

/// Checks that the shard containing `account` has a non empty set of receipts
/// of type `kind` at the block after the resharding block.
fn check_receipts_presence_after_resharding_block(
    account: AccountId,
    kind: ReceiptKind,
) -> LoopActionFn {
    Box::new(
        move |_: &[TestData],
              test_loop_data: &mut TestLoopData,
              client_handle: TestLoopDataHandle<ClientActorInner>| {
            let client_actor = test_loop_data.get_mut(&client_handle);
            let tip = client_actor.client.chain.head().unwrap();

            if !this_block_has_new_shard_layout(client_actor.client.epoch_manager.as_ref(), &tip) {
                return;
            }

            check_receipts_at_block(client_actor, &account, &kind, tip);
        },
    )
}

fn check_receipts_at_block(
    client_actor: &mut ClientActorInner,
    account: &AccountId,
    kind: &ReceiptKind,
    tip: Tip,
) {
    let epoch_manager = &client_actor.client.epoch_manager;
    let shard_layout = epoch_manager.get_shard_layout(&tip.epoch_id).unwrap();
    let shard_id = epoch_manager.account_id_to_shard_id(&account, &tip.epoch_id).unwrap();
    let shard_uid = &ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
    let congestion_info = &client_actor
        .client
        .chain
        .chain_store()
        .get_chunk_extra(&tip.last_block_hash, shard_uid)
        .unwrap()
        .congestion_info()
        .unwrap();

    let num_shards = shard_layout.shard_ids().count();
    let has_delayed = congestion_info.delayed_receipts_gas() != 0;
    let has_buffered = congestion_info.buffered_receipts_gas() != 0;
    tracing::info!(target: "test", height=tip.height, num_shards, ?shard_id, has_delayed, has_buffered, "checking receipts");

    match kind {
        ReceiptKind::Delayed => {
            assert!(has_delayed);
            check_delayed_receipts_exist_in_memtrie(
                &client_actor.client,
                &shard_uid,
                &tip.prev_block_hash,
            );
        }
        ReceiptKind::Buffered => {
            assert!(has_buffered);
            check_buffered_receipts_exist_in_memtrie(
                &client_actor.client,
                &shard_uid,
                &tip.prev_block_hash,
            );
        }
    }
}

/// Asserts that a non zero amount of delayed receipts exist in MemTrie for the given shard.
fn check_delayed_receipts_exist_in_memtrie(
    client: &Client,
    shard_uid: &ShardUId,
    prev_block_hash: &CryptoHash,
) {
    let memtrie = get_memtrie_for_shard(client, shard_uid, prev_block_hash);
    let indices: DelayedReceiptIndices =
        get(&memtrie, &TrieKey::DelayedReceiptIndices).unwrap().unwrap();
    assert_ne!(indices.len(), 0);
}

/// Asserts that a non zero amount of buffered receipts exist in MemTrie for the given shard.
fn check_buffered_receipts_exist_in_memtrie(
    client: &Client,
    shard_uid: &ShardUId,
    prev_block_hash: &CryptoHash,
) {
    let memtrie = get_memtrie_for_shard(client, shard_uid, prev_block_hash);
    let indices: BufferedReceiptIndices =
        get(&memtrie, &TrieKey::BufferedReceiptIndices).unwrap().unwrap();
    // There should be at least one buffered receipt going to some other shard. It's not very precise but good enough.
    assert_ne!(indices.shard_buffers.values().fold(0, |acc, buffer| acc + buffer.len()), 0);
}

/// Returns a loop action that invokes a costly method from a contract
/// `CALLS_PER_BLOCK_HEIGHT` times per block height.
///
/// The account invoking the contract is taken in sequential order from `signed_ids`.
///
/// The account receiving the contract call is taken in sequential order from `receiver_ids`.
fn call_burn_gas_contract(
    signer_ids: Vec<AccountId>,
    receiver_ids: Vec<AccountId>,
    gas_burnt_per_call: Gas,
) -> LoopActionFn {
    const TX_CHECK_BLOCKS_AFTER_RESHARDING: u64 = 5;
    const CALLS_PER_BLOCK_HEIGHT: usize = 5;

    let resharding_height = Cell::new(None);
    let nonce = Cell::new(102);
    let txs = Cell::new(vec![]);
    let latest_height = Cell::new(0);
    // TODO: to be fixed when all shard tracking gets disabled.
    let rpc_id: AccountId = "account0".parse().unwrap();

    Box::new(
        move |node_datas: &[TestData],
              test_loop_data: &mut TestLoopData,
              client_handle: TestLoopDataHandle<ClientActorInner>| {
            let client_actor = &mut test_loop_data.get_mut(&client_handle);
            let tip = client_actor.client.chain.head().unwrap();

            // Run this action only once at every block height.
            if latest_height.get() == tip.height {
                return;
            }
            latest_height.set(tip.height);

            // After resharding: wait some blocks and check that all txs have been executed correctly.
            if let Some(height) = resharding_height.get() {
                if tip.height > height + TX_CHECK_BLOCKS_AFTER_RESHARDING {
                    for (tx, tx_height) in txs.take() {
                        let tx_outcome =
                            client_actor.client.chain.get_partial_transaction_result(&tx);
                        let status = tx_outcome.as_ref().map(|o| o.status.clone());
                        let status = status.unwrap();
                        tracing::debug!(target: "test", ?tx_height, ?tx, ?status, "transaction status");
                        assert_matches!(status, FinalExecutionStatus::SuccessValue(_));
                    }
                }
            } else {
                if next_block_has_new_shard_layout(client_actor.client.epoch_manager.as_ref(), &tip)
                {
                    tracing::debug!(target: "test", height=tip.height, "resharding height set");
                    resharding_height.set(Some(tip.height));
                }
            }
            // Before resharding and one block after: call the test contract a few times per block.
            // The objective is to pile up receipts (e.g. delayed).
            if tip.height <= resharding_height.get().unwrap_or(1000) + 1 {
                for i in 0..CALLS_PER_BLOCK_HEIGHT {
                    // Note that if the number of signers and receivers is the
                    // same then the traffic will always flow the same way. It
                    // would be nice to randomize it a bit.
                    let signer_id = &signer_ids[i % signer_ids.len()];
                    let receiver_id = &receiver_ids[i % receiver_ids.len()];
                    let signer: Signer = create_user_test_signer(signer_id).into();
                    nonce.set(nonce.get() + 1);
                    let method_name = "burn_gas_raw".to_owned();
                    let burn_gas: u64 = gas_burnt_per_call;
                    let args = burn_gas.to_le_bytes().to_vec();
                    let tx = SignedTransaction::call(
                        nonce.get(),
                        signer_id.clone(),
                        receiver_id.clone(),
                        &signer,
                        0,
                        method_name,
                        args,
                        gas_burnt_per_call + 10 * TGAS,
                        tip.last_block_hash,
                    );
                    let mut txs_vec = txs.take();
                    tracing::debug!(target: "test", height=tip.height, tx_hash=?tx.get_hash(), ?signer_id, ?receiver_id, "submitting transaction");
                    txs_vec.push((tx.get_hash(), tip.height));
                    txs.set(txs_vec);
                    submit_tx(&node_datas, &rpc_id, tx);
                }
            }
        },
    )
}

// We want to understand if the most recent block is a resharding block. To do
// this check if the latest block is an epoch start and compare the two epochs'
// shard layouts.
fn next_block_has_new_shard_layout(epoch_manager: &dyn EpochManagerAdapter, tip: &Tip) -> bool {
    if !epoch_manager.is_next_block_epoch_start(&tip.last_block_hash).unwrap() {
        return false;
    }

    let this_epoch_id = tip.epoch_id;
    let next_epoch_id = epoch_manager.get_next_epoch_id(&tip.last_block_hash).unwrap();

    let this_shard_layout = epoch_manager.get_shard_layout(&this_epoch_id).unwrap();
    let next_shard_layout = epoch_manager.get_shard_layout(&next_epoch_id).unwrap();

    this_shard_layout != next_shard_layout
}

// We want to understand if the most recent block is the first block with the
// new shard layout. This is also the block immediately after the resharding
// block. To do this check if the latest block is an epoch start and compare the
// two epochs' shard layouts.
fn this_block_has_new_shard_layout(epoch_manager: &dyn EpochManagerAdapter, tip: &Tip) -> bool {
    if !epoch_manager.is_next_block_epoch_start(&tip.prev_block_hash).unwrap() {
        return false;
    }

    let prev_epoch_id = epoch_manager.get_epoch_id(&tip.prev_block_hash).unwrap();
    let this_epoch_id = epoch_manager.get_epoch_id(&tip.last_block_hash).unwrap();

    let prev_shard_layout = epoch_manager.get_shard_layout(&prev_epoch_id).unwrap();
    let this_shard_layout = epoch_manager.get_shard_layout(&this_epoch_id).unwrap();

    this_shard_layout != prev_shard_layout
}

fn get_memtrie_for_shard(
    client: &Client,
    shard_uid: &ShardUId,
    prev_block_hash: &CryptoHash,
) -> Trie {
    let state_root =
        *client.chain.get_chunk_extra(prev_block_hash, shard_uid).unwrap().state_root();

    // Here memtries will be used as long as client has memtries enabled.
    let memtrie = client
        .runtime_adapter
        .get_trie_for_shard(shard_uid.shard_id(), prev_block_hash, state_root, false)
        .unwrap();
    assert!(memtrie.has_memtries());
    memtrie
}

fn assert_state_equal(
    values1: &HashSet<(Vec<u8>, Vec<u8>)>,
    values2: &HashSet<(Vec<u8>, Vec<u8>)>,
    shard_uid: ShardUId,
    cmp_msg: &str,
) {
    let diff = values1.symmetric_difference(values2);
    let mut has_diff = false;
    for (key, value) in diff {
        has_diff = true;
        tracing::error!(target: "test", ?shard_uid, key=?key, ?value, "Difference in state between {}!", cmp_msg);
    }
    assert!(!has_diff, "{} state mismatch!", cmp_msg);
}

fn shard_was_split(shard_layout: &ShardLayout, shard_id: ShardId) -> bool {
    let Ok(parent) = shard_layout.get_parent_shard_id(shard_id) else {
        return false;
    };
    parent != shard_id
}

/// Asserts that for each child shard:
/// MemTrie, FlatState and DiskTrie all contain the same key-value pairs.
/// If `load_mem_tries_for_tracked_shards` is false, we only enforce memtries for split shards
/// Returns the ShardUIds that this client tracks and has sane memtries and flat storage for
fn assert_state_sanity(
    client: &Client,
    final_head: &Tip,
    load_mem_tries_for_tracked_shards: bool,
) -> Vec<ShardUId> {
    let shard_layout = client.epoch_manager.get_shard_layout(&final_head.epoch_id).unwrap();
    let mut checked_shards = Vec::new();

    for shard_uid in shard_layout.shard_uids() {
        if !load_mem_tries_for_tracked_shards
            && !shard_was_split(&shard_layout, shard_uid.shard_id())
        {
            continue;
        }
        if !client_tracking_shard(client, shard_uid.shard_id(), &final_head.prev_block_hash) {
            continue;
        }

        let memtrie = get_memtrie_for_shard(client, &shard_uid, &final_head.prev_block_hash);
        let memtrie_state =
            memtrie.lock_for_iter().iter().unwrap().collect::<Result<HashSet<_>, _>>().unwrap();

        let state_root = *client
            .chain
            .get_chunk_extra(&final_head.prev_block_hash, &shard_uid)
            .unwrap()
            .state_root();

        // To get a view on disk tries we can leverage the fact that get_view_trie_for_shard() never
        // uses memtries.
        let trie = client
            .runtime_adapter
            .get_view_trie_for_shard(shard_uid.shard_id(), &final_head.prev_block_hash, state_root)
            .unwrap();
        assert!(!trie.has_memtries());
        let trie_state =
            trie.lock_for_iter().iter().unwrap().collect::<Result<HashSet<_>, _>>().unwrap();
        assert_state_equal(&memtrie_state, &trie_state, shard_uid, "memtrie and trie");

        let Some(flat_store_chunk_view) = client
            .chain
            .runtime_adapter
            .get_flat_storage_manager()
            .chunk_view(shard_uid, final_head.last_block_hash)
        else {
            continue;
        };
        let flat_store_state = flat_store_chunk_view
            .iter_range(None, None)
            .map_ok(|(key, value)| {
                let value = match value {
                    FlatStateValue::Ref(value) => client
                        .chain
                        .chain_store()
                        .store()
                        .trie_store()
                        .get(shard_uid, &value.hash)
                        .unwrap()
                        .to_vec(),
                    FlatStateValue::Inlined(data) => data,
                };
                (key, value)
            })
            .collect::<Result<HashSet<_>, _>>()
            .unwrap();

        assert_state_equal(&memtrie_state, &flat_store_state, shard_uid, "memtrie and flat store");
        checked_shards.push(shard_uid);
    }
    checked_shards
}

// For each epoch, keep a map from AccountId to a map with keys equal to
// the set of shards that account tracks in that epoch, and bool values indicating
// whether the equality of flat storage and memtries has been checked for that shard
type EpochTrieCheck = HashMap<AccountId, HashMap<ShardUId, bool>>;

/// Keeps track of the needed trie comparisons for each epoch. After we successfully call
/// assert_state_sanity() for an account ID, we mark those shards as checked for that epoch,
/// and then at the end of the test we check whether all expected shards for each account
/// were checked at least once in that epoch. We do this because assert_state_sanity() isn't
/// always able to perform the check if child shard flat storages are still being created, but
/// we want to make sure that it's always eventually checked by the end of the epoch
struct TrieSanityCheck {
    accounts: Vec<AccountId>,
    load_mem_tries_for_tracked_shards: bool,
    checks: HashMap<EpochId, EpochTrieCheck>,
}

impl TrieSanityCheck {
    fn new(clients: &[&Client], load_mem_tries_for_tracked_shards: bool) -> Self {
        let accounts = clients
            .iter()
            .filter_map(|c| {
                let signer = c.validator_signer.get();
                signer.map(|s| s.validator_id().clone())
            })
            .collect();
        Self { accounts, load_mem_tries_for_tracked_shards, checks: HashMap::new() }
    }

    // If it's not already stored, initialize it with the expected ShardUIds for each account
    fn get_epoch_check(&mut self, client: &Client, tip: &Tip) -> &mut EpochTrieCheck {
        match self.checks.entry(tip.epoch_id) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => {
                let shard_layout = client.epoch_manager.get_shard_layout(&tip.epoch_id).unwrap();
                let shard_uids = shard_layout.shard_uids().collect_vec();
                let mut check = HashMap::new();
                for account_id in self.accounts.iter() {
                    let tracked = shard_uids
                        .iter()
                        .filter_map(|uid| {
                            if !self.load_mem_tries_for_tracked_shards
                                && !shard_was_split(&shard_layout, uid.shard_id())
                            {
                                return None;
                            }
                            let cares = client.shard_tracker.care_about_shard(
                                Some(account_id),
                                &tip.prev_block_hash,
                                uid.shard_id(),
                                false,
                            );
                            if cares {
                                Some((*uid, false))
                            } else {
                                None
                            }
                        })
                        .collect();
                    check.insert(account_id.clone(), tracked);
                }
                e.insert(check)
            }
        }
    }

    // Check trie sanity and keep track of which shards were succesfully fully checked
    fn assert_state_sanity(&mut self, clients: &[&Client]) {
        for client in clients {
            let signer = client.validator_signer.get();
            let Some(account_id) = signer.as_ref().map(|s| s.validator_id()) else {
                // For now this is never relevant, since all of them have account IDs, but
                // if this changes in the future, here we'll just skip those.
                continue;
            };
            let head = client.chain.head().unwrap();
            if head.epoch_id == EpochId::default() {
                continue;
            }
            let final_head = client.chain.final_head().unwrap();
            // At the end of an epoch, we unload memtries for shards we'll no longer track. Also,
            // the key/value equality comparison in assert_state_equal() is only guaranteed for
            // final blocks. So these two together mean that we should only check this when the head
            // and final head are in the same epoch.
            if head.epoch_id != final_head.epoch_id {
                continue;
            }
            let checked_shards =
                assert_state_sanity(client, &final_head, self.load_mem_tries_for_tracked_shards);
            let check = self.get_epoch_check(client, &head);
            let check = check.get_mut(account_id).unwrap();
            for shard_uid in checked_shards {
                check.insert(shard_uid, true);
            }
        }
    }

    /// Look through all the epochs before the current one (because the current one will be early into the epoch,
    /// and we won't have checked it yet) and make sure that for all accounts, all expected shards were checked at least once
    fn check_epochs(&self, client: &Client) {
        let tip = client.chain.head().unwrap();
        let mut block_info = client.epoch_manager.get_block_info(&tip.last_block_hash).unwrap();

        loop {
            let epoch_id = client
                .epoch_manager
                .get_prev_epoch_id_from_prev_block(block_info.prev_hash())
                .unwrap();
            if epoch_id == EpochId::default() {
                break;
            }
            let check = self.checks.get(&epoch_id).unwrap_or_else(|| {
                panic!("No trie comparison checks made for epoch {}", &epoch_id.0)
            });
            for (account_id, checked_shards) in check.iter() {
                for (shard_uid, checked) in checked_shards.iter() {
                    assert!(
                        checked,
                        "No trie comparison checks made for account {} epoch {} shard {}",
                        account_id, &epoch_id.0, shard_uid
                    );
                }
            }

            block_info =
                client.epoch_manager.get_block_info(block_info.epoch_first_block()).unwrap();
            block_info = client.epoch_manager.get_block_info(block_info.prev_hash()).unwrap();
        }
    }
}

/// Base setup to check sanity of Resharding V3.
/// TODO(#11881): add the following scenarios:
/// - Nodes must not track all shards. State sync must succeed.
/// - Set up chunk validator-only nodes. State witness must pass validation.
/// - Consistent tx load. All txs must succeed.
/// - Delayed receipts, congestion control computation.
/// - Cross-shard receipts of all kinds, crossing resharding boundary.
/// - Shard layout v2 -> v2 transition.
/// - Shard layout can be taken from mainnet.
fn test_resharding_v3_base(params: TestReshardingParameters) {
    if !ProtocolFeature::SimpleNightshadeV4.enabled(PROTOCOL_VERSION) {
        return;
    }

    init_test_logger();
    let mut builder = TestLoopBuilder::new();

    // Adjust the resharding configuration to make the tests faster.
    builder = builder.config_modifier(|config, _| {
        let mut resharding_config = config.resharding_config.get();
        resharding_config.batch_delay = Duration::milliseconds(1);
        config.resharding_config.update(resharding_config);
    });

    // Prepare shard split configuration.
    let base_epoch_config_store = EpochConfigStore::for_chain_id("mainnet", None).unwrap();
    let base_protocol_version = ProtocolFeature::SimpleNightshadeV4.protocol_version() - 1;
    let mut base_epoch_config =
        base_epoch_config_store.get_config(base_protocol_version).as_ref().clone();
    base_epoch_config.validator_selection_config.shuffle_shard_assignment_for_chunk_producers =
        params.shuffle_shard_assignment_for_chunk_producers;
    if !params.chunk_ranges_to_drop.is_empty() {
        base_epoch_config.block_producer_kickout_threshold = 0;
        base_epoch_config.chunk_producer_kickout_threshold = 0;
        base_epoch_config.chunk_validator_only_kickout_threshold = 0;
    }

    let boundary_accounts = vec!["account1".parse().unwrap(), "account3".parse().unwrap()];
    let base_shard_layout = ShardLayout::multi_shard_custom(boundary_accounts, 3);

    base_epoch_config.shard_layout = base_shard_layout.clone();
    let new_boundary_account = "account6".parse().unwrap();
    let mut epoch_config = base_epoch_config.clone();
    let parent_shard_uid = account_id_to_shard_uid(&new_boundary_account, &base_shard_layout);

    epoch_config.shard_layout =
        ShardLayout::derive_shard_layout(&base_shard_layout, new_boundary_account);
    tracing::info!(target: "test", ?base_shard_layout, new_shard_layout=?epoch_config.shard_layout, "shard layout");

    let expected_num_shards = epoch_config.shard_layout.shard_ids().count();
    let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter(vec![
        (base_protocol_version, Arc::new(base_epoch_config)),
        (base_protocol_version + 1, Arc::new(epoch_config)),
    ]));

    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&builder.clock())
        .shard_layout(base_shard_layout)
        .protocol_version(base_protocol_version)
        .epoch_length(params.epoch_length)
        .validators_desired_roles(
            &params
                .block_and_chunk_producers
                .iter()
                .map(|account_id| account_id.as_str())
                .collect_vec(),
            &[],
        );
    for account in &params.accounts {
        genesis_builder.add_user_account_simple(account.clone(), params.initial_balance);
    }
    let (genesis, _) = genesis_builder.build();

    if params.track_all_shards {
        builder = builder.track_all_shards();
    }

    if params.limit_outgoing_gas {
        let mut runtime_config = RuntimeConfig::test();
        runtime_config.congestion_control_config.max_outgoing_gas = 100 * TGAS;
        runtime_config.congestion_control_config.min_outgoing_gas = 100 * TGAS;
        let runtime_config_store = RuntimeConfigStore::with_one_config(runtime_config);
        builder = builder.runtime_config_store(runtime_config_store);
    }

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(params.clients)
        .load_mem_tries_for_tracked_shards(params.load_mem_tries_for_tracked_shards)
        .drop_protocol_upgrade_chunks(
            base_protocol_version + 1,
            params.chunk_ranges_to_drop.clone(),
        )
        .build();

    for contract_id in &params.deploy_test_contract {
        let signer = &create_user_test_signer(&contract_id).into();
        let deploy_contract_tx = SignedTransaction::deploy_contract(
            101,
            &contract_id,
            near_test_contracts::rs_contract().into(),
            &signer,
            get_shared_block_hash(&node_datas, &test_loop),
        );
        run_tx(&mut test_loop, deploy_contract_tx, &node_datas, Duration::seconds(5));
    }

    let client_handles =
        node_datas.iter().map(|data| data.client_sender.actor_handle()).collect_vec();

    #[cfg(feature = "test_features")]
    {
        if params.delay_flat_state_resharding > 0 {
            client_handles.iter().for_each(|handle| {
                let client = &mut test_loop.data.get_mut(handle).client;
                client.chain.resharding_manager.flat_storage_resharder.adv_task_delay_by_blocks =
                    params.delay_flat_state_resharding;
            });
        }
    }

    let clients =
        client_handles.iter().map(|handle| &test_loop.data.get(handle).client).collect_vec();
    let mut trie_sanity_check =
        TrieSanityCheck::new(&clients, params.load_mem_tries_for_tracked_shards);

    let latest_block_height = std::cell::Cell::new(0u64);
    let success_condition = |test_loop_data: &mut TestLoopData| -> bool {
        params
            .loop_actions
            .iter()
            .for_each(|action| action(&node_datas, test_loop_data, client_handles[0].clone()));

        let clients =
            client_handles.iter().map(|handle| &test_loop_data.get(handle).client).collect_vec();
        let client = &clients[0];

        let tip = get_smallest_height_head(&clients);

        // Check that all chunks are included.
        let block_header = client.chain.get_block_header(&tip.last_block_hash).unwrap();
        if latest_block_height.get() < tip.height {
            if latest_block_height.get() == 0 {
                println!("State before resharding:");
                print_and_assert_shard_accounts(&clients, &tip);
            }
            trie_sanity_check.assert_state_sanity(&clients);
            latest_block_height.set(tip.height);
            println!("block: {} chunks: {:?}", tip.height, block_header.chunk_mask());
            if params.all_chunks_expected && params.chunk_ranges_to_drop.is_empty() {
                assert!(block_header.chunk_mask().iter().all(|chunk_bit| *chunk_bit));
            }
        }

        // Return true if we passed an epoch with increased number of shards.
        let epoch_height =
            client.epoch_manager.get_epoch_height_from_prev_block(&tip.prev_block_hash).unwrap();
        assert!(epoch_height < 6);
        let prev_epoch_id =
            client.epoch_manager.get_prev_epoch_id_from_prev_block(&tip.prev_block_hash).unwrap();
        let epoch_config = client.epoch_manager.get_epoch_config(&prev_epoch_id).unwrap();
        if epoch_config.shard_layout.shard_ids().count() != expected_num_shards {
            return false;
        }

        println!("State after resharding:");
        print_and_assert_shard_accounts(&clients, &tip);
        check_state_shard_uid_mapping_after_resharding(&client, parent_shard_uid);
        return true;
    };

    test_loop.run_until(
        success_condition,
        // Give enough time to produce ~7 epochs.
        Duration::seconds((7 * params.epoch_length) as i64),
    );
    let client = &test_loop.data.get(&client_handles[0]).client;
    trie_sanity_check.check_epochs(client);
    // Wait for garbage collection to kick in, so that it is tested as well.
    test_loop
        .run_for(Duration::seconds((DEFAULT_GC_NUM_EPOCHS_TO_KEEP * params.epoch_length) as i64));

    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

#[test]
fn test_resharding_v3() {
    test_resharding_v3_base(TestReshardingParameters::new());
}

#[test]
fn test_resharding_v3_drop_chunks_before() {
    let chunk_ranges_to_drop = HashMap::from([(ShardUId { shard_id: 1, version: 3 }, -2..0)]);
    test_resharding_v3_base(
        TestReshardingParameters::new().chunk_ranges_to_drop(chunk_ranges_to_drop),
    );
}

#[test]
fn test_resharding_v3_drop_chunks_after() {
    let chunk_ranges_to_drop = HashMap::from([(ShardUId { shard_id: 2, version: 3 }, 0..2)]);
    test_resharding_v3_base(
        TestReshardingParameters::new().chunk_ranges_to_drop(chunk_ranges_to_drop),
    );
}

#[test]
fn test_resharding_v3_drop_chunks_before_and_after() {
    let chunk_ranges_to_drop = HashMap::from([(ShardUId { shard_id: 0, version: 3 }, -2..2)]);
    test_resharding_v3_base(
        TestReshardingParameters::new().chunk_ranges_to_drop(chunk_ranges_to_drop),
    );
}

#[test]
fn test_resharding_v3_drop_chunks_all() {
    let chunk_ranges_to_drop = HashMap::from([
        (ShardUId { shard_id: 0, version: 3 }, -1..2),
        (ShardUId { shard_id: 1, version: 3 }, -3..0),
        (ShardUId { shard_id: 2, version: 3 }, 0..3),
        (ShardUId { shard_id: 3, version: 3 }, 0..1),
    ]);
    test_resharding_v3_base(
        TestReshardingParameters::new().chunk_ranges_to_drop(chunk_ranges_to_drop),
    );
}

#[test]
// TODO(resharding): fix nearcore and un-ignore this test
#[ignore]
#[cfg(feature = "test_features")]
fn test_resharding_v3_resharding_block_in_fork() {
    test_resharding_v3_base(
        TestReshardingParameters::with_clients(1)
            .add_loop_action(fork_before_resharding_block(false)),
    );
}

#[test]
// TODO(resharding): fix nearcore and un-ignore this test
// TODO(resharding): duplicate this test so that in one case resharding is performed on block
//                   B(height=13) and in another case resharding is performed on block B'(height=13)
#[ignore]
#[cfg(feature = "test_features")]
fn test_resharding_v3_double_sign_resharding_block() {
    test_resharding_v3_base(
        TestReshardingParameters::with_clients(1)
            .add_loop_action(fork_before_resharding_block(true)),
    );
}

#[test]
fn test_resharding_v3_shard_shuffling() {
    let params = TestReshardingParameters::new()
        .shuffle_shard_assignment()
        .single_shard_tracking()
        .chunk_miss_possible();
    test_resharding_v3_base(params);
}

#[test]
// TODO(resharding): fix nearcore and replace the line below with #[cfg_attr(not(feature = "test_features"), ignore)]
#[ignore]
fn test_resharding_v3_delayed_receipts_left_child() {
    let account: AccountId = "account4".parse().unwrap();
    let params = TestReshardingParameters::new()
        .deploy_test_contract(account.clone())
        .add_loop_action(call_burn_gas_contract(
            vec![account.clone()],
            vec![account.clone()],
            275 * TGAS,
        ))
        .add_loop_action(check_receipts_presence_at_resharding_block(
            account,
            ReceiptKind::Delayed,
        ));
    test_resharding_v3_base(params);
}

#[test]
// TODO(resharding): fix nearcore and replace the line below with #[cfg_attr(not(feature = "test_features"), ignore)]
#[ignore]
fn test_resharding_v3_delayed_receipts_right_child() {
    let account: AccountId = "account6".parse().unwrap();
    let params = TestReshardingParameters::new()
        .deploy_test_contract(account.clone())
        .add_loop_action(call_burn_gas_contract(
            vec![account.clone()],
            vec![account.clone()],
            275 * TGAS,
        ))
        .add_loop_action(check_receipts_presence_at_resharding_block(
            account,
            ReceiptKind::Delayed,
        ));
    test_resharding_v3_base(params);
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
fn test_resharding_v3_split_parent_buffered_receipts() {
    let receiver_account: AccountId = "account0".parse().unwrap();
    let account_in_parent: AccountId = "account4".parse().unwrap();
    let account_in_left_child: AccountId = "account4".parse().unwrap();
    let account_in_right_child: AccountId = "account6".parse().unwrap();
    let params = TestReshardingParameters::new()
        .deploy_test_contract(receiver_account.clone())
        .limit_outgoing_gas()
        .add_loop_action(call_burn_gas_contract(
            vec![account_in_left_child.clone(), account_in_right_child],
            vec![receiver_account],
            10 * TGAS,
        ))
        .add_loop_action(check_receipts_presence_at_resharding_block(
            account_in_parent,
            ReceiptKind::Buffered,
        ))
        .add_loop_action(check_receipts_presence_after_resharding_block(
            account_in_left_child,
            ReceiptKind::Buffered,
        ));
    test_resharding_v3_base(params);
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
fn test_resharding_v3_buffered_receipts_towards_splitted_shard() {
    let account_in_left_child: AccountId = "account4".parse().unwrap();
    let account_in_right_child: AccountId = "account6".parse().unwrap();
    let account_in_stable_shard: AccountId = "account1".parse().unwrap();

    let params = TestReshardingParameters::new()
        .deploy_test_contract(account_in_left_child.clone())
        .deploy_test_contract(account_in_right_child.clone())
        .limit_outgoing_gas()
        .add_loop_action(call_burn_gas_contract(
            vec![account_in_stable_shard.clone()],
            vec![account_in_left_child, account_in_right_child],
            10 * TGAS,
        ))
        .add_loop_action(check_receipts_presence_at_resharding_block(
            account_in_stable_shard.clone(),
            ReceiptKind::Buffered,
        ))
        .add_loop_action(check_receipts_presence_after_resharding_block(
            account_in_stable_shard,
            ReceiptKind::Buffered,
        ));
    test_resharding_v3_base(params);
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
fn test_resharding_v3_outgoing_receipts_towards_splitted_shard() {
    let receiver_account: AccountId = "account4".parse().unwrap();
    let account_1_in_stable_shard: AccountId = "account1".parse().unwrap();
    let account_2_in_stable_shard: AccountId = "account2".parse().unwrap();
    let params = TestReshardingParameters::new()
        .deploy_test_contract(receiver_account.clone())
        .add_loop_action(call_burn_gas_contract(
            vec![account_1_in_stable_shard, account_2_in_stable_shard],
            vec![receiver_account],
            5 * TGAS,
        ));
    test_resharding_v3_base(params);
}

#[test]
#[cfg_attr(not(feature = "test_features"), ignore)]
fn test_resharding_v3_outgoing_receipts_from_splitted_shard() {
    let receiver_account: AccountId = "account0".parse().unwrap();
    let account_in_left_child: AccountId = "account4".parse().unwrap();
    let account_in_right_child: AccountId = "account6".parse().unwrap();
    let params = TestReshardingParameters::new()
        .deploy_test_contract(receiver_account.clone())
        .add_loop_action(call_burn_gas_contract(
            vec![account_in_left_child, account_in_right_child],
            vec![receiver_account],
            5 * TGAS,
        ));
    test_resharding_v3_base(params);
}

#[test]
fn test_resharding_v3_load_mem_trie() {
    let params = TestReshardingParameters::new().load_mem_tries_for_tracked_shards(false);
    test_resharding_v3_base(params);
}

#[test]
// TODO(resharding): fix nearcore and un-ignore this test
// #[cfg_attr(not(feature = "test_features"), ignore)]
#[ignore]
fn test_resharding_v3_slower_post_processing_tasks() {
    test_resharding_v3_base(TestReshardingParameters::new().delay_flat_state_resharding(3));
}

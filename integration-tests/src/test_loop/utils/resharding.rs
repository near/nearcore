use std::cell::Cell;
use std::collections::HashSet;
use std::num::NonZero;

use assert_matches::assert_matches;
use borsh::BorshDeserialize;
use itertools::Itertools;
use near_async::test_loop::data::TestLoopData;
use near_chain::ChainStoreAccess;
use near_client::Client;
use near_client::{Query, QueryError::GarbageCollectedBlock};
use near_crypto::Signer;
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockId, BlockReference, Gas, ShardId};
use near_primitives::views::{
    FinalExecutionStatus, QueryRequest, QueryResponse, QueryResponseKind,
};
use near_store::adapter::StoreAdapter;
use near_store::db::refcount::decode_value_with_rc;
use near_store::{DBCol, ShardUId};
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;

use super::sharding::this_block_has_new_shard_layout;
use crate::test_loop::env::TestData;
use crate::test_loop::utils::loop_action::LoopAction;
use crate::test_loop::utils::sharding::next_block_has_new_shard_layout;
use crate::test_loop::utils::transactions::{
    check_txs, delete_account, get_anchor_hash, get_next_nonce, store_and_submit_tx, submit_tx,
};
use crate::test_loop::utils::{get_node_data, retrieve_client_actor, ONE_NEAR, TGAS};

/// A config to tell what shards will be tracked by the client at the given index.
/// For more details, see `TrackedConfig::Schedule`.
#[derive(Clone, Debug)]
pub(crate) struct TrackedShardSchedule {
    pub client_index: usize,
    pub schedule: Vec<Vec<ShardId>>,
}

// Returns a callable function that, when invoked inside a test loop iteration, can force the creation of a chain fork.
#[cfg(feature = "test_features")]
pub(crate) fn fork_before_resharding_block(double_signing: bool) -> LoopAction {
    use near_client::client_actor::AdvProduceBlockHeightSelection;

    let (done, succeeded) = LoopAction::shared_success_flag();
    let action_fn = Box::new(
        move |node_datas: &[TestData],
              test_loop_data: &mut TestLoopData,
              client_account_id: AccountId| {
            // It must happen only for the first resharding block encountered.
            if done.get() {
                return;
            }
            let client_actor =
                retrieve_client_actor(node_datas, test_loop_data, &client_account_id);
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
    );
    LoopAction::new(action_fn, succeeded)
}

pub(crate) fn execute_money_transfers(account_ids: Vec<AccountId>) -> LoopAction {
    const NUM_TRANSFERS_PER_BLOCK: usize = 20;

    let latest_height = Cell::new(0);
    let seed = rand::thread_rng().gen::<u64>();
    println!("Random seed: {}", seed);

    let (ran_transfers, succeeded) = LoopAction::shared_success_flag();
    let action_fn = Box::new(
        move |node_datas: &[TestData],
              test_loop_data: &mut TestLoopData,
              client_account_id: AccountId| {
            let client_actor =
                retrieve_client_actor(node_datas, test_loop_data, &client_account_id);
            let tip = client_actor.client.chain.head().unwrap();

            // Run this action only once at every block height.
            if latest_height.get() == tip.height {
                return;
            }
            latest_height.set(tip.height);

            let mut slice = [0u8; 32];
            slice[0..8].copy_from_slice(&seed.to_le_bytes());
            slice[8..16].copy_from_slice(&tip.height.to_le_bytes());
            let mut rng: ChaCha20Rng = SeedableRng::from_seed(slice);

            for _ in 0..NUM_TRANSFERS_PER_BLOCK {
                let sender = account_ids.choose(&mut rng).unwrap().clone();
                let receiver = account_ids.choose(&mut rng).unwrap().clone();

                let clients = node_datas
                    .iter()
                    .map(|test_data| {
                        &test_loop_data.get(&test_data.client_sender.actor_handle()).client
                    })
                    .collect_vec();

                let anchor_hash = get_anchor_hash(&clients);
                let nonce = get_next_nonce(&test_loop_data, &node_datas, &sender);
                let amount = ONE_NEAR * rng.gen_range(1..=10);
                let tx = SignedTransaction::send_money(
                    nonce,
                    sender.clone(),
                    receiver.clone(),
                    &create_user_test_signer(&sender).into(),
                    amount,
                    anchor_hash,
                );
                submit_tx(&node_datas, &client_account_id, tx);
            }
            ran_transfers.set(true);
        },
    );
    LoopAction::new(action_fn, succeeded)
}

/// Returns a loop action that invokes a costly method from a contract
/// `CALLS_PER_BLOCK_HEIGHT` times per block height.
///
/// The account invoking the contract is taken in sequential order from `signed_ids`.
///
/// The account receiving the contract call is taken in sequential order from `receiver_ids`.
pub(crate) fn call_burn_gas_contract(
    signer_ids: Vec<AccountId>,
    receiver_ids: Vec<AccountId>,
    gas_burnt_per_call: Gas,
    epoch_length: u64,
) -> LoopAction {
    const CALLS_PER_BLOCK_HEIGHT: usize = 5;
    // Set to a value large enough, so that transactions from the past epoch are settled.
    // Must be less than epoch length, otherwise won't be triggered before the test is finished.
    let tx_check_blocks_after_resharding = epoch_length - 2;

    let resharding_height = Cell::new(None);
    let nonce = Cell::new(102);
    let txs = Cell::new(vec![]);
    let latest_height = Cell::new(0);
    let (checked_transactions, succeeded) = LoopAction::shared_success_flag();

    let action_fn = Box::new(
        move |node_datas: &[TestData],
              test_loop_data: &mut TestLoopData,
              client_account_id: AccountId| {
            let client_actor =
                retrieve_client_actor(node_datas, test_loop_data, &client_account_id);
            let tip = client_actor.client.chain.head().unwrap();

            // Run this action only once at every block height.
            if latest_height.get() == tip.height {
                return;
            }
            latest_height.set(tip.height);

            // After resharding: wait some blocks and check that all txs have been executed correctly.
            if let Some(height) = resharding_height.get() {
                if tip.height > height + tx_check_blocks_after_resharding {
                    for (tx, tx_height) in txs.take() {
                        let tx_outcome =
                            client_actor.client.chain.get_partial_transaction_result(&tx);
                        let status = tx_outcome.as_ref().map(|o| o.status.clone());
                        let status = status.unwrap();
                        tracing::debug!(target: "test", ?tx_height, ?tx, ?status, "transaction status");
                        assert_matches!(status, FinalExecutionStatus::SuccessValue(_));
                    }
                    checked_transactions.set(true);
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
                        1,
                        method_name,
                        args,
                        gas_burnt_per_call + 10 * TGAS,
                        tip.last_block_hash,
                    );
                    store_and_submit_tx(
                        &node_datas,
                        &client_account_id,
                        &txs,
                        &signer_id,
                        &receiver_id,
                        tip.height,
                        tx,
                    );
                }
            }
        },
    );
    LoopAction::new(action_fn, succeeded)
}

/// Sends a promise-yield transaction before resharding. Then, if `call_resume` is `true` also sends
/// a yield-resume transaction after resharding, otherwise it lets the promise-yield go into timeout.
///
/// Each `signer_id` sends transaction to the corresponding `receiver_id`.
///
/// A few blocks after resharding all transactions outcomes are checked for successful execution.
pub(crate) fn call_promise_yield(
    call_resume: bool,
    signer_ids: Vec<AccountId>,
    receiver_ids: Vec<AccountId>,
) -> LoopAction {
    let resharding_height: Cell<Option<u64>> = Cell::new(None);
    let txs = Cell::new(vec![]);
    let latest_height = Cell::new(0);
    let promise_txs_sent = Cell::new(false);
    let nonce = Cell::new(102);
    let yield_payload = vec![];
    let (checked_transactions, succeeded) = LoopAction::shared_success_flag();

    let action_fn = Box::new(
        move |node_datas: &[TestData],
              test_loop_data: &mut TestLoopData,
              client_account_id: AccountId| {
            let client_actor =
                retrieve_client_actor(node_datas, test_loop_data, &client_account_id);
            let tip = client_actor.client.chain.head().unwrap();

            // Run this action only once at every block height.
            if latest_height.get() == tip.height {
                return;
            }
            latest_height.set(tip.height);

            // The operation to be done depends on the current block height in relation to the
            // resharding height.
            match (resharding_height.get(), latest_height.get()) {
                // Resharding happened in the previous block.
                // Maybe send the resume transaction.
                (Some(resharding), latest) if latest == resharding + 1 && call_resume => {
                    for (signer_id, receiver_id) in
                        signer_ids.clone().into_iter().zip(receiver_ids.clone().into_iter())
                    {
                        let signer: Signer = create_user_test_signer(&signer_id).into();
                        nonce.set(nonce.get() + 1);
                        let tx = SignedTransaction::call(
                            nonce.get(),
                            signer_id.clone(),
                            receiver_id.clone(),
                            &signer,
                            1,
                            "call_yield_resume_read_data_id_from_storage".to_string(),
                            yield_payload.clone(),
                            300 * TGAS,
                            tip.last_block_hash,
                        );
                        store_and_submit_tx(
                            &node_datas,
                            &client_account_id,
                            &txs,
                            &signer_id,
                            &receiver_id,
                            tip.height,
                            tx,
                        );
                    }
                }
                // Resharding happened a few blocks in the past.
                // Check transactions' outcomes.
                (Some(resharding), latest) if latest == resharding + 4 => {
                    let txs = txs.take();
                    assert_ne!(txs.len(), 0);
                    for (tx, tx_height) in txs {
                        let tx_outcome =
                            client_actor.client.chain.get_partial_transaction_result(&tx);
                        let status = tx_outcome.as_ref().map(|o| o.status.clone());
                        let status = status.unwrap();
                        tracing::debug!(target: "test", ?tx_height, ?tx, ?status, "transaction status");
                        assert_matches!(status, FinalExecutionStatus::SuccessValue(_));
                    }
                    checked_transactions.set(true);
                }
                (Some(_resharding), _latest) => {}
                // Resharding didn't happen in the past.
                (None, _) => {
                    // Check if resharding will happen in this block.
                    if next_block_has_new_shard_layout(
                        client_actor.client.epoch_manager.as_ref(),
                        &tip,
                    ) {
                        tracing::debug!(target: "test", height=tip.height, "resharding height set");
                        resharding_height.set(Some(tip.height));
                        return;
                    }
                    // Before resharding, send a set of promise transactions, just once.
                    if promise_txs_sent.get() {
                        return;
                    }
                    for (signer_id, receiver_id) in
                        signer_ids.clone().into_iter().zip(receiver_ids.clone().into_iter())
                    {
                        let signer: Signer = create_user_test_signer(&signer_id).into();
                        nonce.set(nonce.get() + 1);
                        let tx = SignedTransaction::call(
                            nonce.get(),
                            signer_id.clone(),
                            receiver_id.clone(),
                            &signer,
                            0,
                            "call_yield_create_return_promise".to_string(),
                            yield_payload.clone(),
                            300 * TGAS,
                            tip.last_block_hash,
                        );
                        store_and_submit_tx(
                            &node_datas,
                            &client_account_id,
                            &txs,
                            &signer_id,
                            &receiver_id,
                            tip.height,
                            tx,
                        );
                    }
                    promise_txs_sent.set(true);
                }
            }
        },
    );
    LoopAction::new(action_fn, succeeded)
}

/// After resharding and gc-period, assert the deleted `account_id`
/// is still accessible through archival node view client (if available),
/// and it is not accessible through a regular, RPC node.
fn check_deleted_account_availability(
    node_datas: &[TestData],
    test_loop_data: &mut TestLoopData,
    archival_id: &Option<AccountId>,
    rpc_id: &AccountId,
    account_id: &AccountId,
    height: u64,
) {
    let rpc_node_data = get_node_data(node_datas, &rpc_id);
    let rpc_view_client_handle = rpc_node_data.view_client_sender.actor_handle();

    let block_reference = BlockReference::BlockId(BlockId::Height(height));
    let request = QueryRequest::ViewAccount { account_id: account_id.clone() };
    let msg = Query::new(block_reference, request);

    let rpc_node_result = {
        let view_client = test_loop_data.get_mut(&rpc_view_client_handle);
        near_async::messaging::Handler::handle(view_client, msg.clone())
    };
    assert_matches!(rpc_node_result, Err(GarbageCollectedBlock { .. }));

    if let Some(archival_id) = archival_id {
        let archival_node_data = get_node_data(node_datas, &archival_id);
        let archival_view_client_handle = archival_node_data.view_client_sender.actor_handle();
        let archival_node_result = {
            let view_client = test_loop_data.get_mut(&archival_view_client_handle);
            near_async::messaging::Handler::handle(view_client, msg)
        };
        assert_matches!(
            archival_node_result,
            Ok(QueryResponse { kind: QueryResponseKind::ViewAccount(_), .. })
        );
    }
}

/// Loop action testing a scenario where a temporary account is deleted after resharding.
/// After `gc_num_epochs_to_keep epochs` we assert that the account
/// is not accesible through RPC node but it is still accesible through archival node.
///
/// The `temporary_account_id` must be a subaccount of the `originator_id`.
pub(crate) fn temporary_account_during_resharding(
    archival_id: Option<AccountId>,
    rpc_id: AccountId,
    originator_id: AccountId,
    temporary_account_id: AccountId,
) -> LoopAction {
    let latest_height = Cell::new(0);
    let resharding_height = Cell::new(None);
    let target_height = Cell::new(None);

    let delete_account_tx_hash = Cell::new(None);
    let checked_deleted_account = Cell::new(false);

    let (done, succeeded) = LoopAction::shared_success_flag();
    let action_fn = Box::new(
        move |node_datas: &[TestData],
              test_loop_data: &mut TestLoopData,
              client_account_id: AccountId| {
            if done.get() {
                return;
            }

            let client_actor =
                retrieve_client_actor(node_datas, test_loop_data, &client_account_id);
            let tip = client_actor.client.chain.head().unwrap();

            // Run this action only once at every block height.
            if latest_height.get() == tip.height {
                return;
            }
            latest_height.set(tip.height);
            let epoch_length = client_actor.client.config.epoch_length;
            let gc_num_epochs_to_keep = client_actor.client.config.gc.gc_num_epochs_to_keep;

            if resharding_height.get().is_none() {
                if !this_block_has_new_shard_layout(
                    client_actor.client.epoch_manager.as_ref(),
                    &tip,
                ) {
                    return;
                }
                // Just resharded. Delete the temporary account and set the target height
                // high enough so that the delete account transaction will be garbage collected.
                let tx_hash = delete_account(
                    test_loop_data,
                    node_datas,
                    &client_account_id,
                    &temporary_account_id,
                    &originator_id,
                );
                delete_account_tx_hash.set(Some(tx_hash));
                target_height
                    .set(Some(latest_height.get() + (gc_num_epochs_to_keep + 1) * epoch_length));
                resharding_height.set(Some(latest_height.get()));
            }

            // If an epoch passed since resharding, make sure the delete account transaction finished.
            if latest_height.get() == resharding_height.get().unwrap() + epoch_length {
                check_txs(
                    test_loop_data,
                    node_datas,
                    &client_account_id,
                    &[delete_account_tx_hash.get().unwrap()],
                );
                checked_deleted_account.set(true);
            }

            if latest_height.get() < target_height.get().unwrap() {
                return;
            }
            assert!(checked_deleted_account.get());
            // Since gc window passed after the account was deleted,
            // check that it is not accessible through regular node,
            // but it is accessible through archival node.
            check_deleted_account_availability(
                node_datas,
                test_loop_data,
                &archival_id,
                &rpc_id,
                &temporary_account_id,
                resharding_height.get().unwrap(),
            );
            done.set(true);
        },
    );
    LoopAction::new(action_fn, succeeded)
}

/// Removes from State column all entries where key does not start with `the_only_shard_uid` ShardUId prefix.
fn retain_the_only_shard_state(client: &Client, the_only_shard_uid: ShardUId) {
    let store = client.chain.chain_store.store().trie_store();
    let mut store_update = store.store_update();
    for kv in store.store().iter_raw_bytes(DBCol::State) {
        let (key, value) = kv.unwrap();
        let shard_uid = ShardUId::try_from_slice(&key[0..8]).unwrap();
        if shard_uid == the_only_shard_uid {
            continue;
        }
        let (_, rc) = decode_value_with_rc(&value);
        assert!(rc > 0);
        let node_hash = CryptoHash::try_from_slice(&key[8..]).unwrap();
        store_update.decrement_refcount_by(shard_uid, &node_hash, NonZero::new(rc as u32).unwrap());
    }
    store_update.commit().unwrap();
}

/// Asserts that all other shards State except `the_only_shard_uid` have been cleaned-up.
fn check_has_the_only_shard_state(client: &Client, the_only_shard_uid: ShardUId) {
    let store = client.chain.chain_store.store().trie_store();
    let mut shard_uid_prefixes = HashSet::new();
    for kv in store.store().iter_raw_bytes(DBCol::State) {
        let (key, _) = kv.unwrap();
        let shard_uid = ShardUId::try_from_slice(&key[0..8]).unwrap();
        shard_uid_prefixes.insert(shard_uid);
    }
    let shard_uid_prefixes = shard_uid_prefixes.into_iter().collect_vec();
    assert_eq!(shard_uid_prefixes, [the_only_shard_uid]);
}

// Loop action testing state cleanup after resharding.
// It assumes single shard tracking and it waits for gc after resharding.
// Then it checks whether the last shard tracked by the client
// is the only ShardUId prefix for nodes in the State column.
pub(crate) fn check_state_cleanup_after_resharding(
    tracked_shard_schedule: TrackedShardSchedule,
) -> LoopAction {
    let client_index = tracked_shard_schedule.client_index;
    let latest_height = Cell::new(0);
    let target_height = Cell::new(None);

    let (done, succeeded) = LoopAction::shared_success_flag();
    let action_fn = Box::new(
        move |node_datas: &[TestData], test_loop_data: &mut TestLoopData, _: AccountId| {
            if done.get() {
                return;
            }

            let client_handle = node_datas[client_index].client_sender.actor_handle();
            let client = &test_loop_data.get_mut(&client_handle).client;
            let tip = client.chain.head().unwrap();

            // Run this action only once at every block height.
            if latest_height.get() == tip.height {
                return;
            }

            let epoch_height = client
                .epoch_manager
                .get_epoch_height_from_prev_block(&tip.prev_block_hash)
                .unwrap();
            let [tracked_shard_id] =
                tracked_shard_schedule.schedule[epoch_height as usize].clone().try_into().unwrap();
            let tracked_shard_uid =
                client.epoch_manager.shard_id_to_uid(tracked_shard_id, &tip.epoch_id).unwrap();

            if latest_height.get() == 0 {
                // This is beginning of the test, and the first epoch after genesis has height 1.
                assert_eq!(epoch_height, 1);
                // Get rid of the part of the Genesis State other than the shard we initially track.
                retain_the_only_shard_state(client, tracked_shard_uid);
            }
            latest_height.set(tip.height);

            if target_height.get().is_none() {
                if !this_block_has_new_shard_layout(client.epoch_manager.as_ref(), &tip) {
                    return;
                }
                // Just resharded. Set the target height high enough so that gc will kick in.
                let epoch_length = client.config.epoch_length;
                let gc_num_epochs_to_keep = client.config.gc.gc_num_epochs_to_keep;
                target_height
                    .set(Some(latest_height.get() + (gc_num_epochs_to_keep + 1) * epoch_length));
            }

            if latest_height.get() < target_height.get().unwrap() {
                return;
            }
            // At this point, we should only have State from the last tracked shard.
            check_has_the_only_shard_state(&client, tracked_shard_uid);
            done.set(true);
        },
    );
    LoopAction::new(action_fn, succeeded)
}

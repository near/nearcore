use std::cell::Cell;
use std::collections::{BTreeMap, HashSet};
use std::num::NonZero;

use assert_matches::assert_matches;
use borsh::BorshDeserialize;
use bytesize::ByteSize;
use itertools::Itertools;
use near_async::test_loop::data::TestLoopData;
use near_chain::ChainStoreAccess;
use near_client::Client;
use near_client::{Query, QueryError::GarbageCollectedBlock};
use near_crypto::Signer;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_primitives::action::{Action, FunctionCallAction};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    DelayedReceiptIndices, PromiseYieldIndices, ReceiptOrStateStoredReceipt,
};
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockId, BlockReference, Gas, ShardId};
use near_primitives::views::{
    FinalExecutionStatus, QueryRequest, QueryResponse, QueryResponseKind,
};
use near_store::adapter::StoreAdapter;
use near_store::adapter::trie_store::{TrieStoreAdapter, get_shard_uid_mapping};
use near_store::db::refcount::decode_value_with_rc;
use near_store::trie::receipts_column_helper::{ShardsOutgoingReceiptBuffer, TrieQueue};
use near_store::{DBCol, ShardUId, StorageError, Trie, TrieDBStorage, get};
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;

use super::sharding::{next_epoch_has_new_shard_layout, this_block_has_new_shard_layout};
use crate::setup::state::NodeExecutionData;
use crate::utils::loop_action::LoopAction;
use crate::utils::sharding::{get_memtrie_for_shard, next_block_has_new_shard_layout};
use crate::utils::transactions::{
    check_txs, check_txs_remove_successful, delete_account, get_anchor_hash, get_next_nonce,
    store_and_submit_tx, submit_tx,
};
use crate::utils::{ONE_NEAR, TGAS, get_node_data, retrieve_client_actor};
use near_chain::types::Tip;
use near_client::client_actor::ClientActorInner;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::trie_key::TrieKey;
use std::sync::Arc;

/// A config to tell what shards will be tracked by the client at the given index.
/// For more details, see `TrackedShardsConfig::Schedule`.
#[derive(Clone, Debug)]
pub(crate) struct TrackedShardSchedule {
    pub client_index: usize,
    pub schedule: Vec<Vec<ShardId>>,
}

// Returns a callable function that, when invoked inside a test loop iteration, can force the creation of a chain fork.
#[cfg(feature = "test_features")]
pub(crate) fn fork_before_resharding_block(
    double_signing: bool,
    blocks_produced: near_primitives::types::BlockHeight,
) -> LoopAction {
    use near_client::client_actor::AdvProduceBlockHeightSelection;

    let (done, succeeded) = LoopAction::shared_success_flag();
    let action_fn = Box::new(
        move |node_datas: &[NodeExecutionData],
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
                client_actor.adv_produce_blocks_on(blocks_produced, true, height_selection);
                done.set(true);
            }
        },
    );
    LoopAction::new(action_fn, succeeded)
}

pub(crate) fn execute_money_transfers(account_ids: Vec<AccountId>) -> LoopAction {
    const NUM_TRANSFERS_PER_BLOCK: usize = 20;

    let latest_height = Cell::new(0);
    let seed = rand::thread_rng().r#gen::<u64>();
    println!("Random seed: {}", seed);

    let (ran_transfers, succeeded) = LoopAction::shared_success_flag();
    let action_fn = Box::new(
        move |node_datas: &[NodeExecutionData],
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

/// Returns a loop action that makes storage read and write at every block
/// height.
pub(crate) fn execute_storage_operations(
    sender_id: AccountId,
    receiver_id: AccountId,
) -> LoopAction {
    const TX_CHECK_DEADLINE: u64 = 5;
    let latest_height = Cell::new(0);
    let txs = Cell::new(vec![]);
    let nonce = Cell::new(102);

    let (ran_transfers, succeeded) = LoopAction::shared_success_flag();

    let action_fn = Box::new(
        move |node_datas: &[NodeExecutionData],
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

            let mut remaining_txs = vec![];
            for (tx, tx_height) in txs.take() {
                if tx_height + TX_CHECK_DEADLINE >= tip.height {
                    remaining_txs.push((tx, tx_height));
                    continue;
                }

                let tx_outcome = client_actor.client.chain.get_partial_transaction_result(&tx);
                let status = tx_outcome.as_ref().map(|o| o.status.clone());
                assert_matches!(status, Ok(FinalExecutionStatus::SuccessValue(_)));
            }
            txs.set(remaining_txs);

            let clients = node_datas
                .iter()
                .map(|test_data| {
                    &test_loop_data.get(&test_data.client_sender.actor_handle()).client
                })
                .collect_vec();

            // Send transaction which reads a key and writes a key-value pair
            // to the contract storage.
            let anchor_hash = get_anchor_hash(&clients);
            let gas = 20 * TGAS;
            let salt = 2 * tip.height;
            nonce.set(nonce.get() + 1);
            let read_action = Action::FunctionCall(Box::new(FunctionCallAction {
                args: near_primitives::test_utils::encode(&[salt]),
                method_name: "read_value".to_string(),
                gas,
                deposit: 0,
            }));
            let write_action = Action::FunctionCall(Box::new(FunctionCallAction {
                args: near_primitives::test_utils::encode(&[salt + 1, salt * 10]),
                method_name: "write_key_value".to_string(),
                gas,
                deposit: 0,
            }));
            let tx = SignedTransaction::from_actions(
                nonce.get(),
                sender_id.clone(),
                receiver_id.clone(),
                &create_user_test_signer(&sender_id).into(),
                vec![read_action, write_action],
                anchor_hash,
                0,
            );

            store_and_submit_tx(
                &node_datas,
                &client_account_id,
                &txs,
                &sender_id,
                &receiver_id,
                tip.height,
                tx,
            );
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
        move |node_datas: &[NodeExecutionData],
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

/// Send 3MB receipts from `signer_ids` shards to `receiver_ids` shards.
/// Receipts are sent just before the resharding boundary.
pub(crate) fn send_large_cross_shard_receipts(
    signer_ids: Vec<AccountId>,
    receiver_ids: Vec<AccountId>,
) -> LoopAction {
    // Height of the last block with the old shard layout
    let resharding_height = Cell::new(None);
    let nonce = Cell::new(102);
    let txs = Cell::new(vec![]); // FIXME: Wouldn't RefCell be better?
    let latest_height = Cell::new(0);
    let (action_success_setter, succeeded) = LoopAction::shared_success_flag();

    let action_fn = Box::new(
        move |node_datas: &[NodeExecutionData],
              test_loop_data: &mut TestLoopData,
              client_account_id: AccountId| {
            let client_actor =
                retrieve_client_actor(node_datas, test_loop_data, &client_account_id);
            let tip = client_actor.client.chain.head().unwrap();
            let epoch_manager = &client_actor.client.epoch_manager;

            // Run this action only once at every block height.
            if latest_height.get() == tip.height {
                return;
            }
            latest_height.set(tip.height);

            // Set resharding height once known
            if resharding_height.get().is_none()
                && next_block_has_new_shard_layout(epoch_manager.as_ref(), &tip)
            {
                tracing::debug!(target: "test", height=tip.height, "resharding height set");
                resharding_height.set(Some(tip.height));
            }

            for shard_uid in epoch_manager.get_shard_layout(&tip.epoch_id).unwrap().shard_uids() {
                let mut outgoing_receipt_sizes: BTreeMap<ShardId, Vec<ByteSize>> = BTreeMap::new();

                let memtrie =
                    get_memtrie_for_shard(&client_actor.client, &shard_uid, &tip.prev_block_hash);
                let mut outgoing_buffers = ShardsOutgoingReceiptBuffer::load(&memtrie).unwrap();
                for target_shard in outgoing_buffers.shards() {
                    let mut receipt_sizes = Vec::new();
                    for receipt in outgoing_buffers.to_shard(target_shard).iter(&memtrie, false) {
                        let receipt_size = match receipt {
                            Ok(ReceiptOrStateStoredReceipt::StateStoredReceipt(
                                state_stored_receipt,
                            )) => state_stored_receipt.metadata().congestion_size,
                            _ => panic!("receipt is {:?}", receipt),
                        };
                        receipt_sizes.push(ByteSize::b(receipt_size));
                    }
                    if !receipt_sizes.is_empty() {
                        outgoing_receipt_sizes.insert(target_shard, receipt_sizes);
                    }
                }
                tracing::info!(target: "test", "outgoing buffers from shard {}: {:?}", shard_uid.shard_id(), outgoing_receipt_sizes);
            }

            let is_epoch_before_resharding =
                next_epoch_has_new_shard_layout(epoch_manager.as_ref(), &tip);

            // Estimate the resharding boundary to know when to start sending transactions.
            let estimated_resharding_height = match resharding_height.get() {
                Some(h) => h, // Resharding boundary known, use it.
                None if is_epoch_before_resharding => {
                    // Resharding boundary unknown, estimate it.
                    let cur_epoch_start =
                        epoch_manager.get_epoch_start_height(&tip.last_block_hash).unwrap();
                    let cur_epoch_length =
                        epoch_manager.get_epoch_config(&tip.epoch_id).unwrap().epoch_length;
                    let cur_epoch_estimated_end = cur_epoch_start + cur_epoch_length - 1;
                    cur_epoch_estimated_end
                }
                _ => tip.height + 99999999999999, // Not in the next epoch, set to infinity into the future
            };

            // Send large cross-shard receipts a moment before the resharding happens.
            if tip.height + 4 >= estimated_resharding_height
                && tip.height <= estimated_resharding_height - 2
            {
                for signer_id in &signer_ids {
                    for receiver_id in &receiver_ids {
                        // Send a 3MB cross-shard receipt from signer_id's shard to receiver_id's shard.
                        let signer: Signer = create_user_test_signer(signer_id).into();
                        nonce.set(nonce.get() + 1);
                        let tx = SignedTransaction::call(
                            nonce.get(),
                            signer_id.clone(),
                            signer_id.clone(),
                            &signer,
                            1,
                            "generate_large_receipt".into(),
                            format!(
                                "{{\"account_id\": \"{}\", \"method_name\": \"noop\", \"total_args_size\": 3000000}}",
                                receiver_id
                            ).into(),
                            300 * TGAS,
                            tip.last_block_hash,
                        );
                        tracing::info!(
                            target: "test",
                            "Sending 3MB receipt from {} to {}. tx_hash: {:?}",
                            signer_id,
                            receiver_id,
                            tx.get_hash()
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
            }

            // Check status of transactions, remove successful ones from the list.
            check_txs_remove_successful(&txs, &client_actor.client);

            // If the chain is past the resharding boundary and all transactions finished
            // successfully, declare the action as successful.
            if let Some(height) = resharding_height.get() {
                let taken_txs = txs.take();
                if tip.height > height + 2 && taken_txs.is_empty() {
                    action_success_setter.set(true);
                }
                txs.set(taken_txs);
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
        move |node_datas: &[NodeExecutionData],
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
                    let epoch_manager = client_actor.client.epoch_manager.as_ref();
                    // Check if resharding will happen in this block.
                    if next_block_has_new_shard_layout(epoch_manager, &tip) {
                        tracing::debug!(target: "test", height=tip.height, "resharding height set");
                        resharding_height.set(Some(tip.height));
                        return;
                    }
                    // Before resharding, send a set of promise transactions close to the resharding boundary, just once.
                    if promise_txs_sent.get() {
                        return;
                    }

                    let will_reshard =
                        epoch_manager.will_shard_layout_change(&tip.prev_block_hash).unwrap();
                    if !will_reshard {
                        return;
                    }
                    let epoch_length = client_actor.client.config.epoch_length;
                    let epoch_start =
                        epoch_manager.get_epoch_start_height(&tip.last_block_hash).unwrap();
                    if tip.height + 5 < epoch_start + epoch_length {
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
    node_datas: &[NodeExecutionData],
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
/// is not accessible through RPC node but it is still accessible through archival node.
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
        move |node_datas: &[NodeExecutionData],
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
///
/// `expect_shard_uid_is_mapped` means that `the_only_shard_uid` should use an ancestor
/// ShardUId as the db key prefix.
fn check_has_the_only_shard_state(
    client: &Client,
    the_only_shard_uid: ShardUId,
    expect_shard_uid_is_mapped: bool,
) {
    let store = client.chain.chain_store.store().trie_store();
    let mut shard_uid_prefixes = HashSet::new();
    for kv in store.store().iter_raw_bytes(DBCol::State) {
        let (key, _) = kv.unwrap();
        let shard_uid = ShardUId::try_from_slice(&key[0..8]).unwrap();
        shard_uid_prefixes.insert(shard_uid);
    }
    let mapped_shard_uid = get_shard_uid_mapping(&store.store(), the_only_shard_uid);
    if expect_shard_uid_is_mapped {
        assert_ne!(mapped_shard_uid, the_only_shard_uid);
    } else {
        assert_eq!(mapped_shard_uid, the_only_shard_uid);
    };
    let shard_uid_prefixes = shard_uid_prefixes.into_iter().collect_vec();
    assert_eq!(shard_uid_prefixes, [mapped_shard_uid]);
}

/// Loop action testing state cleanup.
/// It assumes single shard tracking and it waits for `num_epochs_to_wait`.
/// Then it checks whether the last shard tracked by the client
/// is the only ShardUId prefix for nodes in the State column.
///
/// Pass `expect_shard_uid_is_mapped` as true if it is expected at the end of the test
/// that the last tracked shard will use an ancestor ShardUId as a db key prefix.
pub(crate) fn check_state_cleanup(
    tracked_shard_schedule: TrackedShardSchedule,
    num_epochs_to_wait: u64,
    expect_shard_uid_is_mapped: bool,
) -> LoopAction {
    let client_index = tracked_shard_schedule.client_index;
    let latest_height = Cell::new(0);

    let (done, succeeded) = LoopAction::shared_success_flag();
    let action_fn = Box::new(
        move |node_datas: &[NodeExecutionData], test_loop_data: &mut TestLoopData, _: AccountId| {
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
                shard_id_to_uid(client.epoch_manager.as_ref(), tracked_shard_id, &tip.epoch_id)
                    .unwrap();

            if latest_height.get() == 0 {
                // This is beginning of the test, and the first epoch after genesis has height 1.
                assert_eq!(epoch_height, 1);
                // Get rid of the part of the Genesis State other than the shard we initially track.
                retain_the_only_shard_state(client, tracked_shard_uid);
            }
            latest_height.set(tip.height);

            if epoch_height < num_epochs_to_wait {
                return;
            }
            // At this point, we should only have State from the last tracked shard.
            check_has_the_only_shard_state(&client, tracked_shard_uid, expect_shard_uid_is_mapped);
            done.set(true);
        },
    );
    LoopAction::new(action_fn, succeeded)
}

/// Repro case for the issue of 'Missing TrieValue' after GC period for refcounted trie nodes
/// that are duplicated to both children during resharding. This particular scenario tests
/// promise yield indices.
pub(crate) fn promise_yield_repro_missing_trie_value(
    left_child_account: AccountId,
    right_child_account: AccountId,
    shard_layout_after_resharding: ShardLayout,
    gc_num_epochs: u64,
    epoch_length: u64,
) -> LoopAction {
    let resharding_height: Cell<Option<u64>> = Cell::new(None);
    let txs = Cell::new(vec![]);
    let latest_height = Cell::new(0);
    let nonce = Cell::new(102);
    let yield_payload = vec![];
    let pre_resharding_tx_sent = Cell::new(false);
    let (checked_transactions, succeeded) = LoopAction::shared_success_flag();
    let (parent_shard_uid, left_child_shard_uid, right_child_shard_uid) = get_resharded_shard_uids(
        &left_child_account,
        &right_child_account,
        &shard_layout_after_resharding,
    );

    let action_fn = Box::new(
        move |node_datas: &[NodeExecutionData],
              test_loop_data: &mut TestLoopData,
              client_account_id: AccountId| {
            let client_actor =
                retrieve_client_actor(node_datas, test_loop_data, &client_account_id);
            let tip = client_actor.client.chain.head().unwrap();

            // Function to send a promise yield receipt.
            let send_promise_yield =
                |signer_account: &AccountId,
                 receiver_account: &AccountId,
                 tip: &Tip,
                 sent_flag: Option<&Cell<bool>>| {
                    if sent_flag.map_or(false, |flag| flag.get()) {
                        return;
                    }
                    let signer: Signer = create_user_test_signer(signer_account).into();
                    nonce.set(nonce.get() + 1);
                    let tx = SignedTransaction::call(
                        nonce.get(),
                        signer_account.clone(),
                        receiver_account.clone(),
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
                        &signer_account,
                        &receiver_account,
                        tip.height,
                        tx,
                    );
                    sent_flag.map(|flag| flag.set(true));
                    tracing::debug!(target: "test", height=tip.height, ?signer_account, ?receiver_account, "sent promise yield tx");
                };

            // Run this action only once at every block height.
            if latest_height.get() == tip.height {
                return;
            }
            latest_height.set(tip.height);

            let get_promise_yield_indices = |shard_uid| {
                get_trie_node_value::<PromiseYieldIndices>(
                    &client_actor,
                    shard_uid,
                    &tip.prev_block_hash,
                    TrieKey::PromiseYieldIndices,
                )
            };

            let indices_parent_shard = get_promise_yield_indices(parent_shard_uid);
            let indices_left_child_shard = get_promise_yield_indices(left_child_shard_uid);
            let indices_right_child_shard = get_promise_yield_indices(right_child_shard_uid);

            tracing::debug!(target: "test", height=tip.height, epoch=?tip.epoch_id, 
                    ?indices_parent_shard, ?indices_left_child_shard, ?indices_right_child_shard, "promise yield indices");

            // At any height, if the shard exists and it is tracked, the promise yield indices trie
            // node must exist.
            assert_matches!(indices_parent_shard, Some(Ok(_)) | None);
            assert_matches!(indices_left_child_shard, Some(Ok(_)) | None);
            assert_matches!(indices_right_child_shard, Some(Ok(_)) | None);

            // The operation to be done depends on the current block height in relation to the
            // resharding height and the GC height.
            match (resharding_height.get(), latest_height.get()) {
                // Resharding happened in the previous blocks.
                // Send a promise yield transaction in the left child shard.
                (Some(resharding), latest) if latest == resharding + 2 => {
                    send_promise_yield(&left_child_account, &right_child_account, &tip, None);
                }
                // Resharding happened and GC kicked in for the epoch with the old shard layout.
                // Send a promise yield transaction in the right child shard.
                (Some(resharding), latest)
                    if latest == resharding + gc_num_epochs * epoch_length + 5 =>
                {
                    send_promise_yield(&right_child_account, &left_child_account, &tip, None);
                }
                // Send a promise yield resume to complete the promise yield started two blocks before.
                //
                // The two block delay is necessary as the promise yield receipt handling
                // potentially is going to be delayed by a block if it needs to be sent to another
                // shard.
                (Some(resharding), latest)
                    if latest == resharding + gc_num_epochs * epoch_length + 5 + 2 =>
                {
                    let signer: Signer = create_user_test_signer(&right_child_account).into();
                    nonce.set(nonce.get() + 1);
                    let tx = SignedTransaction::call(
                        nonce.get(),
                        right_child_account.clone(),
                        left_child_account.clone(),
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
                        &right_child_account,
                        &left_child_account,
                        tip.height,
                        tx,
                    );
                }
                // GC happened a few blocks in the past.
                // Check transactions' outcomes.
                (Some(resharding), latest)
                    if latest == resharding + gc_num_epochs * epoch_length + 5 + 7 =>
                {
                    let txs = txs.take();
                    for (index, (tx, tx_height)) in txs.iter().enumerate() {
                        let tx_outcome =
                            client_actor.client.chain.get_partial_transaction_result(&tx);
                        let status = tx_outcome.as_ref().map(|o| o.status.clone());
                        tracing::debug!(target: "test", ?tx_height, ?tx, ?status, "transaction status");
                        // First two txs should have been GC'd, so these cannot be found.
                        if index <= 1 {
                            assert_matches!(status, Err(_));
                        } else {
                            assert_matches!(status, Ok(FinalExecutionStatus::SuccessValue(_)));
                        }
                    }
                    checked_transactions.set(true);
                }
                // Catch-all case, do nothing.
                (Some(_resharding), _latest) => {}
                // Resharding didn't happen yet.
                (None, _) => {
                    let epoch_manager = client_actor.client.epoch_manager.as_ref();
                    // Check if resharding will happen in this block.
                    if next_block_has_new_shard_layout(epoch_manager, &tip) {
                        tracing::debug!(target: "test", height=tip.height, "resharding height set");
                        resharding_height.set(Some(tip.height));
                        return;
                    }
                    // Before resharding, send a set of promise transactions close to the resharding
                    // boundary, just once.
                    let will_reshard =
                        epoch_manager.will_shard_layout_change(&tip.prev_block_hash).unwrap();
                    if !will_reshard {
                        return;
                    }
                    let epoch_length = client_actor.client.config.epoch_length;
                    let epoch_start =
                        epoch_manager.get_epoch_start_height(&tip.last_block_hash).unwrap();
                    if tip.height + 5 < epoch_start + epoch_length {
                        return;
                    }
                    send_promise_yield(
                        &left_child_account,
                        &right_child_account,
                        &tip,
                        Some(&pre_resharding_tx_sent),
                    );
                }
            }
        },
    );
    LoopAction::new(action_fn, succeeded)
}

/// Repro case for the issue of 'Missing TrieValue' after GC period for refcounted trie nodes
/// that are duplicated to both children during resharding.
/// This scenario tests a particular combination of contract calls, in order to create
/// delayed receipts only in one child, and verifies that the other child shard is not left
/// with missing trie values.
pub(crate) fn delayed_receipts_repro_missing_trie_value(
    left_child_account: AccountId,
    right_child_account: AccountId,
    shard_layout_after_resharding: ShardLayout,
    gc_num_epochs: u64,
    epoch_length: u64,
) -> LoopAction {
    const CALLS_PER_BLOCK_HEIGHT: usize = 5;
    const GAS_BURNT_PER_CALL: u64 = 275 * TGAS;
    let resharding_height: Cell<Option<u64>> = Cell::new(None);
    let txs = Cell::new(vec![]);
    let latest_height = Cell::new(0);
    let nonce = Cell::new(102);
    let pre_resharding_tx_sent = Cell::new(false);
    let (done, succeeded) = LoopAction::shared_success_flag();
    let (parent_shard_uid, left_child_shard_uid, right_child_shard_uid) = get_resharded_shard_uids(
        &left_child_account,
        &right_child_account,
        &shard_layout_after_resharding,
    );

    let action_fn = Box::new(
        move |node_datas: &[NodeExecutionData],
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

            // Function to burn gas and create delayed receipts.
            let burn_gas = |signer_account: &AccountId,
                            receiver_account: &AccountId,
                            tip: &Tip,
                            sent_flag: Option<&Cell<bool>>| {
                if sent_flag.map_or(false, |flag| flag.get()) {
                    return;
                }
                for _ in 0..CALLS_PER_BLOCK_HEIGHT {
                    let signer: Signer = create_user_test_signer(signer_account).into();
                    nonce.set(nonce.get() + 1);
                    let method_name = "burn_gas_raw".to_owned();
                    let args = GAS_BURNT_PER_CALL.to_le_bytes().to_vec();
                    let tx = SignedTransaction::call(
                        nonce.get(),
                        signer_account.clone(),
                        receiver_account.clone(),
                        &signer,
                        1,
                        method_name,
                        args,
                        GAS_BURNT_PER_CALL + 10 * TGAS,
                        tip.last_block_hash,
                    );
                    store_and_submit_tx(
                        &node_datas,
                        &client_account_id,
                        &txs,
                        &signer_account,
                        &receiver_account,
                        tip.height,
                        tx,
                    );
                }
                sent_flag.map(|flag| flag.set(true));
                tracing::debug!(target: "test", height=tip.height, ?signer_account, ?receiver_account, "sent burn gas txs");
            };

            let get_delayed_receipts_indices = |shard_uid| {
                get_trie_node_value::<DelayedReceiptIndices>(
                    &client_actor,
                    shard_uid,
                    &tip.prev_block_hash,
                    TrieKey::DelayedReceiptIndices,
                )
            };

            let indices_parent_shard = get_delayed_receipts_indices(parent_shard_uid);
            let indices_left_child_shard = get_delayed_receipts_indices(left_child_shard_uid);
            let indices_right_child_shard = get_delayed_receipts_indices(right_child_shard_uid);

            tracing::debug!(target: "test", height=tip.height, epoch=?tip.epoch_id, 
                    ?indices_parent_shard, ?indices_left_child_shard, ?indices_right_child_shard, "delayed receipts indices");

            // At any height, if the shard exists and it is tracked, the delayed receipts indices
            // trie node must exist.
            assert_matches!(indices_parent_shard, Some(Ok(_)) | None);
            assert_matches!(indices_left_child_shard, Some(Ok(_)) | None);
            assert_matches!(indices_right_child_shard, Some(Ok(_)) | None);

            // The operation to be done depends on the current block height in relation to the
            // resharding height and the GC height.
            match (resharding_height.get(), latest_height.get()) {
                // Resharding happened in the previous blocks. Send a batch of transactions to pile
                // up delayed receipts. This will decrease the refcount of the original, shared
                // delayed receipts indices trie node.
                (Some(resharding), latest) if latest == resharding + 2 => {
                    burn_gas(&left_child_account, &right_child_account, &tip, None);
                }
                // A few blocks after sending the transactions, we want to verify that they have
                // been executed correctly.
                (Some(resharding), latest) if latest == resharding + 5 + 5 => {
                    let txs = txs.take();
                    for (tx, tx_height) in &txs {
                        let tx_outcome =
                            client_actor.client.chain.get_partial_transaction_result(&tx);
                        let status = tx_outcome.as_ref().map(|o| o.status.clone());
                        tracing::debug!(target: "test", ?tx_height, ?tx, ?status, "transaction status");
                        assert_matches!(status, Ok(FinalExecutionStatus::SuccessValue(_)));
                    }
                }
                // GC happened a few blocks in the past.
                (Some(resharding), latest)
                    if latest == resharding + gc_num_epochs * epoch_length + 10 =>
                {
                    done.set(true)
                }
                // Catch-all case, do nothing.
                (Some(_resharding), _latest) => {}
                // Resharding didn't happen yet.
                (None, _) => {
                    let epoch_manager = client_actor.client.epoch_manager.as_ref();
                    // Check if resharding will happen in this block.
                    if next_block_has_new_shard_layout(epoch_manager, &tip) {
                        tracing::debug!(target: "test", height=tip.height, "resharding height set");
                        resharding_height.set(Some(tip.height));
                        return;
                    }
                    // Before resharding, send transactions to trigger delayed receipts buffering,
                    // just once.
                    let will_reshard =
                        epoch_manager.will_shard_layout_change(&tip.prev_block_hash).unwrap();
                    if !will_reshard {
                        return;
                    }
                    let epoch_length = client_actor.client.config.epoch_length;
                    let epoch_start =
                        epoch_manager.get_epoch_start_height(&tip.last_block_hash).unwrap();
                    if tip.height + 5 < epoch_start + epoch_length {
                        return;
                    }
                    burn_gas(
                        &left_child_account,
                        &right_child_account,
                        &tip,
                        Some(&pre_resharding_tx_sent),
                    );
                }
            }
        },
    );
    LoopAction::new(action_fn, succeeded)
}

/// Returns a tuple with the shard uids of: parent, left child, right child.
fn get_resharded_shard_uids(
    left_child_account: &AccountId,
    right_child_account: &AccountId,
    shard_layout_after_resharding: &ShardLayout,
) -> (ShardUId, ShardUId, ShardUId) {
    let left_child_shard_uid =
        shard_layout_after_resharding.account_id_to_shard_uid(&left_child_account);
    let right_child_shard_uid =
        shard_layout_after_resharding.account_id_to_shard_uid(&right_child_account);
    let parent_shard_uid = ShardUId::new(
        3,
        shard_layout_after_resharding.get_parent_shard_id(left_child_shard_uid.shard_id()).unwrap(),
    );
    (parent_shard_uid, left_child_shard_uid, right_child_shard_uid)
}

// Helper function to retrieve any key from the trie. This bypasses all intermediate layers
// (caching, memtrie, flat-storage).
fn get_trie_node_value<I: borsh::BorshDeserialize + Default>(
    client_actor: &ClientActorInner,
    shard_uid: ShardUId,
    prev_block_hash: &CryptoHash,
    key: TrieKey,
) -> Option<Result<I, StorageError>> {
    client_actor.client.chain.get_chunk_extra(prev_block_hash, &shard_uid).ok().map(|chunk_extra| {
        let trie = Trie::new(
            Arc::new(TrieDBStorage::new(
                TrieStoreAdapter::new(client_actor.client.runtime_adapter.store().clone()),
                shard_uid,
            )),
            *chunk_extra.state_root(),
            None,
        );
        Ok(get(&trie, &key)?.unwrap_or_default())
    })
}

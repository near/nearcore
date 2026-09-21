use super::sharding::{next_epoch_has_new_shard_layout, this_block_has_new_shard_layout};
use crate::setup::block_observer::BlockSource;
use crate::setup::env::TestLoopEnv;
use crate::setup::state::NodeExecutionData;
use crate::utils::loop_action::LoopAction;
use crate::utils::node::TestLoopNode;
use crate::utils::resharding_check_trace;
use crate::utils::sharding::{get_memtrie_for_shard, next_block_has_new_shard_layout};
use crate::utils::transactions::get_anchor_hash;
use crate::utils::{get_node_data, retrieve_client_actor};
use assert_matches::assert_matches;
use borsh::BorshDeserialize;
use bytesize::ByteSize;
use itertools::Itertools;
use near_async::messaging::CanSend;
use near_async::test_loop::data::TestLoopData;
use near_chain::types::Tip;
use near_chain::{ChainStoreAccess, Error};
use near_client::Client;
use near_client::client_actor::ClientActor;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_network::client::ProcessTxRequest;
use near_primitives::action::{Action, FunctionCallAction};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    DelayedReceiptIndices, PromiseYieldIndices, ReceiptOrStateStoredReceipt,
};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{
    AccountId, Balance, BlockHeight, EpochHeight, Gas, NumShards, ShardId,
};
use near_primitives::views::{FinalExecutionStatus, QueryRequest};
use near_store::adapter::StoreAdapter;
use near_store::adapter::trie_store::TrieStoreAdapter;
use near_store::db::refcount::decode_value_with_rc;
use near_store::flat::FlatStorageStatus;
use near_store::trie::receipts_column_helper::{ShardsOutgoingReceiptBuffer, TrieQueue};
use near_store::{DBCol, ShardUId, StorageError, Trie, TrieDBStorage, get};
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashSet};
use std::env::var;
use std::num::NonZero;
use std::ops::ControlFlow;
use std::rc::Rc;
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
    // The seed can be fixed, so that two runs of a test submit the same transfers.
    let seed = match var("NEAR_TEST_RESHARDING_MONEY_TRANSFERS_SEED") {
        Ok(seed) => seed.parse().unwrap(),
        Err(_) => rand::thread_rng().r#gen::<u64>(),
    };
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
                let node = TestLoopNode {
                    data: test_loop_data,
                    node_data: get_node_data(node_datas, &client_account_id),
                };
                let nonce = node.get_next_nonce(&sender);
                let amount = Balance::from_near(1).checked_mul(rng.gen_range(1..=10)).unwrap();
                let tx = SignedTransaction::send_money(
                    nonce,
                    sender.clone(),
                    receiver.clone(),
                    &create_user_test_signer(&sender).into(),
                    amount,
                    anchor_hash,
                );
                node.submit_tx(tx);
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
            let gas = Gas::from_teragas(20);
            let salt = 2 * tip.height;
            nonce.set(nonce.get() + 1);
            let read_action = Action::FunctionCall(Box::new(FunctionCallAction {
                args: near_primitives::test_utils::encode(&[salt]),
                method_name: "read_value".to_string(),
                gas: gas,
                deposit: Balance::ZERO,
            }));
            let write_action = Action::FunctionCall(Box::new(FunctionCallAction {
                args: near_primitives::test_utils::encode(&[salt + 1, salt * 10]),
                method_name: "write_key_value".to_string(),
                gas: gas,
                deposit: Balance::ZERO,
            }));
            let tx = SignedTransaction::from_actions(
                nonce.get(),
                sender_id.clone(),
                receiver_id.clone(),
                &create_user_test_signer(&sender_id).into(),
                vec![read_action, write_action],
                anchor_hash,
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

/// Checks the outcomes of transactions stored in `txs`. Successful transactions
/// are removed. Transactions that haven't completed yet (`Started`/`NotStarted`)
/// are put back in `txs` for retry on the next iteration. Any other status is
/// treated as a failure and panics. When all transactions have succeeded,
/// `checked_transactions` is set to `true`.
fn check_txs_with_retry(
    client: &Client,
    txs: &Cell<Vec<(CryptoHash, u64)>>,
    checked_transactions: &Cell<bool>,
) {
    let mut remaining = vec![];
    for (tx, tx_height) in txs.take() {
        let tx_outcome = client.chain.get_partial_transaction_result(&tx);
        let status = match tx_outcome {
            Err(e) => panic!("transaction {tx} not found: {e}"),
            Ok(outcome) => outcome.status,
        };
        tracing::debug!(target: "test", ?tx_height, ?tx, ?status, "transaction status");
        match status {
            FinalExecutionStatus::SuccessValue(_) => {}
            FinalExecutionStatus::Started | FinalExecutionStatus::NotStarted => {
                remaining.push((tx, tx_height));
            }
            FinalExecutionStatus::Failure(error) => {
                panic!("transaction {tx} failed with error: {error:?}");
            }
        }
    }
    if remaining.is_empty() {
        checked_transactions.set(true);
    } else {
        txs.set(remaining);
    }
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
    let tx_check_blocks_after_resharding = epoch_length - 1;

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
                    check_txs_with_retry(&client_actor.client, &txs, &checked_transactions);
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
                    let burn_gas: u64 = gas_burnt_per_call.as_gas();
                    let args = burn_gas.to_le_bytes().to_vec();
                    let tx = SignedTransaction::call(
                        nonce.get(),
                        signer_id.clone(),
                        receiver_id.clone(),
                        &signer,
                        Balance::from_yoctonear(1),
                        method_name,
                        args,
                        gas_burnt_per_call.checked_add(Gas::from_teragas(10)).unwrap(),
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
                tracing::info!(target: "test", shard_id = %shard_uid.shard_id(), ?outgoing_receipt_sizes, "outgoing buffers from shard");
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
                            Balance::from_yoctonear(1),
                            "generate_large_receipt".into(),
                            format!(
                                "{{\"account_id\": \"{}\", \"method_name\": \"noop\", \"total_args_size\": 3000000}}",
                                receiver_id
                            ).into(),
                            Gas::from_teragas(300),
                            tip.last_block_hash,
                        );
                        tracing::info!(
                            target: "test",
                            %signer_id,
                            %receiver_id,
                            tx_hash = ?tx.get_hash(),
                            "sending 3MB receipt"
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
                // Resharding happened two blocks ago.
                // Maybe send the resume transaction.
                // Don't send at resharding + 1 (first block of new epoch) because the
                // RPC client may observe it before chunk producers do, causing forwarded
                // txs to be rejected as Expired.
                (Some(resharding), latest) if latest == resharding + 2 && call_resume => {
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
                            Balance::from_yoctonear(1),
                            "call_yield_resume_read_data_id_from_storage".to_string(),
                            yield_payload.clone(),
                            Gas::from_teragas(300),
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
                (Some(resharding), latest) if latest >= resharding + 4 => {
                    check_txs_with_retry(&client_actor.client, &txs, &checked_transactions);
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
                            Balance::ZERO,
                            "call_yield_create_return_promise".to_string(),
                            yield_payload.clone(),
                            Gas::from_teragas(300),
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

/// Like [`call_promise_yield`] but exercises the YieldWithId host functions:
/// `promise_yield_create_with_id` before resharding, and
/// `promise_yield_resume_with_yield_id` two blocks after resharding.
///
/// This indirectly verifies that the `YieldIdToDataId` / `DataIdToYieldId`
/// trie rows survive a shard split — without that, the post-resharding resume
/// would either fail to look up the data_id (returning 0) or the runtime would
/// panic in `shard_split_handle_key_value` on the new columns.
///
/// Each signer is paired with a deterministic 32-byte yield_id so both the
/// pre- and post-resharding transactions agree without inter-tx storage.
pub(crate) fn call_promise_yield_with_id(
    signer_ids: Vec<AccountId>,
    receiver_ids: Vec<AccountId>,
) -> LoopAction {
    use base64::Engine;
    let resharding_height: Cell<Option<u64>> = Cell::new(None);
    let txs = Cell::new(vec![]);
    let latest_height = Cell::new(0);
    let create_txs_sent = Cell::new(false);
    let nonce = Cell::new(102);
    let yield_payload: Vec<u8> = vec![6, 6, 6];
    let (checked_transactions, succeeded) = LoopAction::shared_success_flag();

    // Deterministic yield_id per signer position so create and resume agree.
    let yield_ids: Vec<[u8; 32]> =
        (0..signer_ids.len()).map(|i| [(i as u8).wrapping_add(1); 32]).collect();

    let b64 = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);
    let make_create_args = {
        let yield_payload = yield_payload.clone();
        move |yield_id: &[u8; 32]| -> Vec<u8> {
            let args = serde_json::json!([{
                "yield_create_with_id": {
                    "method_name": "check_promise_result_return_value",
                    "arguments": b64(&yield_payload),
                    "gas": 0,
                    "gas_weight": 1,
                    "yield_id": b64(yield_id),
                },
                "id": 0,
            }]);
            serde_json::to_vec(&args).unwrap()
        }
    };
    let make_resume_args = {
        move |yield_id: &[u8; 32]| -> Vec<u8> {
            // `promise_yield_resume_with_yield_id` returns 1 on success and the
            // test contract's `call_promise` dispatcher asserts it matches `"id"`.
            let args = serde_json::json!([{
                "yield_resume_with_yield_id": {
                    "yield_id": b64(yield_id),
                    "payload": b64(&yield_payload),
                },
                "id": 1,
            }]);
            serde_json::to_vec(&args).unwrap()
        }
    };

    let action_fn = Box::new(
        move |node_datas: &[NodeExecutionData],
              test_loop_data: &mut TestLoopData,
              client_account_id: AccountId| {
            let client_actor =
                retrieve_client_actor(node_datas, test_loop_data, &client_account_id);
            let tip = client_actor.client.chain.head().unwrap();

            if latest_height.get() == tip.height {
                return;
            }
            latest_height.set(tip.height);

            match (resharding_height.get(), latest_height.get()) {
                // Two blocks after resharding: send resume_with_yield_id. We skip
                // resharding+1 (first block of new epoch) to avoid Expired tx
                // rejections seen by `call_promise_yield`.
                (Some(resharding), latest) if latest == resharding + 2 => {
                    for ((signer_id, receiver_id), yield_id) in signer_ids
                        .clone()
                        .into_iter()
                        .zip(receiver_ids.clone().into_iter())
                        .zip(yield_ids.iter())
                    {
                        let signer: Signer = create_user_test_signer(&signer_id).into();
                        nonce.set(nonce.get() + 1);
                        let tx = SignedTransaction::call(
                            nonce.get(),
                            signer_id.clone(),
                            receiver_id.clone(),
                            &signer,
                            Balance::from_yoctonear(1),
                            "call_promise".to_string(),
                            make_resume_args(yield_id),
                            Gas::from_teragas(300),
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
                (Some(resharding), latest) if latest >= resharding + 4 => {
                    check_txs_with_retry(&client_actor.client, &txs, &checked_transactions);
                }
                (Some(_), _) => {}
                (None, _) => {
                    let epoch_manager = client_actor.client.epoch_manager.as_ref();
                    if next_block_has_new_shard_layout(epoch_manager, &tip) {
                        tracing::debug!(
                            target: "test",
                            height = tip.height,
                            "resharding height set",
                        );
                        resharding_height.set(Some(tip.height));
                        return;
                    }
                    if create_txs_sent.get() {
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

                    for ((signer_id, receiver_id), yield_id) in signer_ids
                        .clone()
                        .into_iter()
                        .zip(receiver_ids.clone().into_iter())
                        .zip(yield_ids.iter())
                    {
                        let signer: Signer = create_user_test_signer(&signer_id).into();
                        nonce.set(nonce.get() + 1);
                        let tx = SignedTransaction::call(
                            nonce.get(),
                            signer_id.clone(),
                            receiver_id.clone(),
                            &signer,
                            Balance::ZERO,
                            "call_promise".to_string(),
                            make_create_args(yield_id),
                            Gas::from_teragas(300),
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
                    create_txs_sent.set(true);
                }
            }
        },
    );
    LoopAction::new(action_fn, succeeded)
}

/// The test's nodes at the observed block.
pub(crate) struct BlockNodes<'a> {
    /// Node whose new block this is.
    pub(crate) observed: &'a TestLoopNode<'a>,
    pub(crate) all: &'a [TestLoopNode<'a>],
    /// Node that sends transactions and answers queries.
    pub(crate) request: &'a TestLoopNode<'a>,
    pub(crate) archival: Option<&'a TestLoopNode<'a>>,
}

impl<'a> BlockNodes<'a> {
    /// Head of the node whose new block this is.
    pub(crate) fn tip(&self) -> Arc<Tip> {
        self.observed.head()
    }

    pub(crate) fn clients(&self) -> Vec<&'a Client> {
        self.all.iter().map(|node| node.client()).collect_vec()
    }

    /// Number of shards in the shard layout of the observed block's epoch.
    pub(crate) fn num_shards_at_tip(&self) -> NumShards {
        let tip = self.tip();
        self.request.client().epoch_manager.get_shard_layout(&tip.epoch_id).unwrap().num_shards()
    }

    pub(crate) fn epoch_height_at_tip(&self) -> EpochHeight {
        let tip = self.tip();
        self.request
            .client()
            .epoch_manager
            .get_epoch_height_from_prev_block(&tip.prev_block_hash)
            .unwrap()
    }

    pub(crate) fn account_id_of(&self, node: &TestLoopNode<'_>) -> AccountId {
        node.client().validator_signer.get().map(|signer| signer.validator_id().clone()).unwrap()
    }
}

/// Code a resharding test runs on each new block of one node: a check, traffic, or output.
pub(crate) trait BlockAction: 'static {
    fn on_new_block(&mut self, nodes: &BlockNodes<'_>) -> ControlFlow<()>;
}

/// Indexes of the nodes with a role in the test, used to build `BlockNodes` for every call.
#[derive(Clone, Copy)]
pub(crate) struct NodeRoles {
    pub(crate) request_node_index: usize,
    pub(crate) archival_node_index: Option<usize>,
}

/// Registers `action`, called on each new block of `source`.
pub(crate) fn install_block_action(
    env: &mut TestLoopEnv,
    source: BlockSource,
    roles: NodeRoles,
    mut action: Box<dyn BlockAction>,
) {
    env.on_each_block(source, move |block| {
        let nodes = BlockNodes {
            observed: block.observed_node,
            all: block.nodes,
            request: &block.nodes[roles.request_node_index],
            archival: roles.archival_node_index.map(|index| &block.nodes[index]),
        };
        action.on_new_block(&nodes)
    });
}

/// Submits a transaction that creates `new_account_id` from `originator`.
pub(crate) fn create_account(
    env: &TestLoopEnv,
    rpc_id: &AccountId,
    originator: &AccountId,
    new_account_id: &AccountId,
    amount: Balance,
    nonce: u64,
) -> CryptoHash {
    let node = env.node_for_account(rpc_id);
    let signer = create_user_test_signer(originator);
    let new_signer: Signer = create_user_test_signer(new_account_id);

    let tx = SignedTransaction::create_account(
        nonce,
        originator.clone(),
        new_account_id.clone(),
        amount,
        new_signer.public_key(),
        &signer,
        node.head().last_block_hash,
    );

    node.submit_tx(tx)
}

#[derive(Clone)]
pub(crate) struct AccountDeletedAfterSplit {
    account_id: AccountId,
    beneficiary_id: AccountId,
    state: Rc<RefCell<AccountDeletionState>>,
}

#[derive(Debug)]
enum AccountDeletionState {
    WaitingForNewShardLayout,
    WaitingForDeleteOutcome { delete_tx_hash: CryptoHash, deleted_at_height: BlockHeight },
    WaitingForGarbageCollection { deleted_at_height: BlockHeight },
    Completed,
}

impl AccountDeletedAfterSplit {
    /// `account_id` is a sub-account; its parent account created it and gets its balance back.
    pub(crate) fn new(account_id: AccountId) -> Self {
        let beneficiary_id = account_id
            .get_parent_account_id()
            .unwrap_or_else(|| panic!("{account_id} must be a sub-account"))
            .to_owned();
        Self {
            account_id,
            beneficiary_id,
            state: Rc::new(RefCell::new(AccountDeletionState::WaitingForNewShardLayout)),
        }
    }

    /// The action to register on the request node's blocks: it deletes the account at the first
    /// block of the new shard layout, then follows it to the garbage collection query.
    pub(crate) fn block_action(&self) -> Self {
        self.clone()
    }

    /// Submits the transaction that creates the account, and returns its hash. The parent account
    /// creates it with 10 NEAR, using its first nonce.
    pub(crate) fn submit_create_transaction(
        &self,
        env: &TestLoopEnv,
        request_node_account_id: &AccountId,
    ) -> CryptoHash {
        create_account(
            env,
            request_node_account_id,
            &self.beneficiary_id,
            &self.account_id,
            Balance::from_near(10),
            2,
        )
    }

    /// Asserts that the account was deleted at the first block of the new shard layout, that its
    /// delete transaction succeeded one epoch later, and that after the garbage collection window
    /// the request node no longer served its state at the deletion height.
    pub(crate) fn assert_deleted_and_state_garbage_collected(&self) {
        assert_matches!(
            *self.state.borrow(),
            AccountDeletionState::Completed,
            "{} did not reach the last step of its deletion check",
            self.account_id
        );
    }
}

impl BlockAction for AccountDeletedAfterSplit {
    fn on_new_block(&mut self, nodes: &BlockNodes<'_>) -> ControlFlow<()> {
        let account_id = &self.account_id;
        let beneficiary_id = &self.beneficiary_id;
        let state = &self.state;
        {
            let request_node = nodes.request;
            let tip = nodes.tip();
            let client_config = &request_node.client().config;
            let epoch_length = client_config.epoch_length;
            let gc_num_epochs_to_keep = client_config.gc.gc_num_epochs_to_keep;
            let next_state = match *state.borrow() {
                AccountDeletionState::WaitingForNewShardLayout => {
                    if !this_block_has_new_shard_layout(
                        request_node.client().epoch_manager.as_ref(),
                        &tip,
                    ) {
                        return ControlFlow::Continue(());
                    }
                    // We construct the tx manually instead of using node.tx_delete_account()
                    // because that method uses node.head().last_block_hash, which is the
                    // head of a single node. With shard shuffling enabled, nodes can be at
                    // different heights, and the chunk producer that processes the tx might
                    // not know about that block hash yet. The block at the minimum head
                    // height across all nodes is known by every node.
                    let block_hash = nodes
                        .all
                        .iter()
                        .map(|node| node.head())
                        .min_by_key(|head| head.height)
                        .unwrap()
                        .last_block_hash;
                    let signer = create_user_test_signer(&account_id);
                    let nonce = request_node.get_next_nonce(&account_id);
                    let tx = SignedTransaction::delete_account(
                        nonce,
                        account_id.clone(),
                        account_id.clone(),
                        beneficiary_id.clone(),
                        &signer,
                        block_hash,
                    );
                    let delete_tx_hash = request_node.submit_tx(tx);
                    resharding_check_trace::deleted_account_step(
                        "submitted delete",
                        tip.height,
                        &account_id,
                    );
                    AccountDeletionState::WaitingForDeleteOutcome {
                        delete_tx_hash,
                        deleted_at_height: tip.height,
                    }
                }
                AccountDeletionState::WaitingForDeleteOutcome {
                    delete_tx_hash,
                    deleted_at_height,
                } => {
                    if tip.height != deleted_at_height + epoch_length {
                        return ControlFlow::Continue(());
                    }
                    let status = request_node
                        .client()
                        .chain
                        .get_partial_transaction_result(&delete_tx_hash)
                        .unwrap()
                        .status;
                    assert_matches!(status, FinalExecutionStatus::SuccessValue(_));
                    resharding_check_trace::deleted_account_step(
                        "checked delete outcome",
                        tip.height,
                        &account_id,
                    );
                    AccountDeletionState::WaitingForGarbageCollection { deleted_at_height }
                }
                AccountDeletionState::WaitingForGarbageCollection { deleted_at_height } => {
                    let garbage_collected_height =
                        deleted_at_height + (gc_num_epochs_to_keep + 1) * epoch_length;
                    if tip.height < garbage_collected_height {
                        return ControlFlow::Continue(());
                    }
                    let query = QueryRequest::ViewAccount { account_id: account_id.clone() };
                    let request_node_result =
                        request_node.runtime_query_at_height(deleted_at_height, query.clone());
                    assert_matches!(request_node_result, Err(..));
                    if let Some(archival_node) = nodes.archival {
                        let _archival_node_result =
                            archival_node.runtime_query_at_height(deleted_at_height, query);
                        // TODO(cloud_archival) Assert that the archival node still has the account.
                    }
                    resharding_check_trace::deleted_account_step(
                        "checked state garbage collected",
                        tip.height,
                        &account_id,
                    );
                    AccountDeletionState::Completed
                }
                AccountDeletionState::Completed => return ControlFlow::Break(()),
            };
            let completed = matches!(next_state, AccountDeletionState::Completed);
            *state.borrow_mut() = next_state;
            if completed { ControlFlow::Break(()) } else { ControlFlow::Continue(()) }
        }
    }
}

/// Removes from State column all entries where key does not start with `the_only_shard_uid` ShardUId prefix.
fn retain_the_only_shard_state(client: &Client, the_only_shard_uid: ShardUId) {
    let store = client.chain.chain_store.store().trie_store();
    let mut store_update = store.store_update();
    for (key, value) in store.store().iter_raw_bytes(DBCol::State) {
        let shard_uid = ShardUId::try_from_slice(&key[0..8]).unwrap();
        if shard_uid == the_only_shard_uid {
            continue;
        }
        let (_, rc) = decode_value_with_rc(&value);
        assert!(rc > 0);
        let node_hash = CryptoHash::try_from_slice(&key[8..]).unwrap();
        store_update.decrement_refcount_by(shard_uid, &node_hash, NonZero::new(rc as u32).unwrap());
    }
    store_update.commit();
}

/// Asserts that all other shards State except `the_only_shard_uid` have been cleaned-up.
fn check_has_the_only_shard_state(client: &Client, the_only_shard_uid: ShardUId) {
    let store = client.chain.chain_store.store();
    let mut shard_uid_prefixes = HashSet::new();
    for (key, _) in store.iter_raw_bytes(DBCol::State) {
        let shard_uid = ShardUId::try_from_slice(&key[0..8]).unwrap();
        shard_uid_prefixes.insert(shard_uid);
    }
    assert_eq!(shard_uid_prefixes.into_iter().collect_vec(), [the_only_shard_uid]);
}

/// Loop action testing that resharding is skipped when no children are tracked.
/// Verifies that parent shard flat storage remains Ready after resharding.
pub(crate) fn check_resharding_skipped_when_no_children_tracked(
    parent_shard_uid: ShardUId,
    tracked_shard_schedule: TrackedShardSchedule,
) -> LoopAction {
    let client_index = tracked_shard_schedule.client_index;
    let latest_height = Cell::new(0);
    let resharding_height = Cell::new(None);
    let checked = Cell::new(false);

    let (done, succeeded) = LoopAction::shared_success_flag();
    let action_fn = Box::new(
        move |node_datas: &[NodeExecutionData], test_loop_data: &mut TestLoopData, _: AccountId| {
            if done.get() || checked.get() {
                return;
            }

            let client_handle = node_datas[client_index].client_sender.actor_handle();
            let client = &test_loop_data.get_mut(&client_handle).client;
            let tip = client.chain.head().unwrap();

            // Run this action only once at every block height.
            if latest_height.get() == tip.height {
                return;
            }
            latest_height.set(tip.height);

            if resharding_height.get().is_none() {
                if next_block_has_new_shard_layout(client.epoch_manager.as_ref(), &tip) {
                    resharding_height.set(Some(tip.height));
                    tracing::debug!(target: "test", height=tip.height, "resharding height set");
                }
            }

            // Check flat storage status after resharding.
            if let Some(resharding_h) = resharding_height.get() {
                if tip.height > resharding_h + 3 && !checked.get() {
                    let flat_store = client.runtime_adapter.store().flat_store();
                    let status = flat_store.get_flat_storage_status(parent_shard_uid);

                    match status {
                        FlatStorageStatus::Ready(_) => {
                            // Flat storage should be Ready.
                        }
                        status => {
                            panic!(
                                "Unexpected parent shard status {:?} for shard {:?}",
                                status, parent_shard_uid
                            );
                        }
                    }
                    checked.set(true);
                    done.set(true);
                }
            }
        },
    );
    LoopAction::new(action_fn, succeeded)
}

/// Loop action testing state cleanup.
/// It assumes single shard tracking and it waits for `num_epochs_to_wait`.
/// Then it checks whether the last shard tracked by the client
/// is the only ShardUId prefix for nodes in the State column.
pub(crate) fn check_state_cleanup(
    tracked_shard_schedule: TrackedShardSchedule,
    num_epochs_to_wait: u64,
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
            check_has_the_only_shard_state(&client, tracked_shard_uid);
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
                        Balance::ZERO,
                        "call_yield_create_return_promise".to_string(),
                        yield_payload.clone(),
                        Gas::from_teragas(300),
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
                        Balance::from_yoctonear(1),
                        "call_yield_resume_read_data_id_from_storage".to_string(),
                        yield_payload.clone(),
                        Gas::from_teragas(300),
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
    const GAS_BURNT_PER_CALL: Gas = Gas::from_teragas(275);
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
                    let args = GAS_BURNT_PER_CALL.as_gas().to_le_bytes().to_vec();
                    let tx = SignedTransaction::call(
                        nonce.get(),
                        signer_account.clone(),
                        receiver_account.clone(),
                        &signer,
                        Balance::from_yoctonear(1),
                        method_name,
                        args,
                        GAS_BURNT_PER_CALL.checked_add(Gas::from_teragas(10)).unwrap(),
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
    client_actor: &ClientActor,
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

/// Submit a transaction to the node with the given account id.
fn submit_tx(node_datas: &[NodeExecutionData], rpc_id: &AccountId, tx: SignedTransaction) {
    let process_tx_request =
        ProcessTxRequest { transaction: tx, is_forwarded: false, check_only: false };
    let rpc_node_data = get_node_data(node_datas, rpc_id);
    rpc_node_data.rpc_handler_sender.send(process_tx_request);
}

/// Stores a transaction hash into a vector of `(transaction, block_height)` and then submits the transaction.
fn store_and_submit_tx(
    node_datas: &[NodeExecutionData],
    rpc_id: &AccountId,
    txs: &Cell<Vec<(CryptoHash, BlockHeight)>>,
    signer_id: &AccountId,
    receiver_id: &AccountId,
    height: BlockHeight,
    tx: SignedTransaction,
) {
    resharding_check_trace::submitted_tx(height, signer_id, receiver_id, &tx.get_hash());
    let mut txs_vec = txs.take();
    tracing::debug!(target: "test", height, tx_hash=?tx.get_hash(), ?signer_id, ?receiver_id, "submitting transaction");
    txs_vec.push((tx.get_hash(), height));
    txs.set(txs_vec);
    submit_tx(node_datas, rpc_id, tx);
}

/// Deterministic test gas-key signer for an account. Setup and verification
/// share this helper so both sides agree on the public key.
pub(crate) fn gas_key_signer_for_account(account_id: &AccountId) -> Signer {
    const GAS_KEY_SIGNER_SEED: &str = "gas_key_resharding";
    InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, GAS_KEY_SIGNER_SEED).into()
}

/// Loop action that fires its `assertion` once, `num_blocks_after_new_layout`
/// blocks after the first block of the new shard layout (the block right after
/// the resharding block). Pass `0` to run on the first block of the new layout.
pub(crate) fn assert_after_resharding<F>(
    num_blocks_after_new_layout: u64,
    assertion: F,
) -> LoopAction
where
    F: Fn(&TestLoopNode<'_>) + 'static,
{
    let new_layout_height: Cell<Option<u64>> = Cell::new(None);
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
            let new_layout = match new_layout_height.get() {
                Some(h) => h,
                None => {
                    if !this_block_has_new_shard_layout(
                        client_actor.client.epoch_manager.as_ref(),
                        &tip,
                    ) {
                        return;
                    }
                    new_layout_height.set(Some(tip.height));
                    tip.height
                }
            };
            if tip.height < new_layout + num_blocks_after_new_layout {
                return;
            }
            let node = TestLoopNode {
                data: test_loop_data,
                node_data: get_node_data(node_datas, &client_account_id),
            };
            assertion(&node);
            done.set(true);
        },
    );
    LoopAction::new(action_fn, succeeded)
}

/// Checks status of the provided transactions. Panics if transaction result is an error.
/// Removes transactions that finished successfully from the list.
fn check_txs_remove_successful(txs: &Cell<Vec<(CryptoHash, BlockHeight)>>, client: &Client) {
    let mut unfinished_txs = Vec::new();
    for (tx_hash, tx_height) in txs.take() {
        let tx_outcome = client.chain.get_final_transaction_result(&tx_hash);
        let status = tx_outcome.as_ref().map(|o| o.status.clone());
        tracing::debug!(target: "test", ?tx_height, ?tx_hash, ?status, "transaction status");
        match status {
            Ok(FinalExecutionStatus::SuccessValue(_)) => continue,
            Ok(FinalExecutionStatus::NotStarted)
            | Ok(FinalExecutionStatus::Started)
            | Err(Error::DBNotFoundErr(_)) => unfinished_txs.push((tx_hash, tx_height)),
            _ => panic!(
                "remove_successful_txs: Transaction failed! tx_hash = {:?}, tx_height = {}, status = {:?}",
                tx_hash, tx_height, status
            ),
        };
    }
    txs.set(unfinished_txs);
}

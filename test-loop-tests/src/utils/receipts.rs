use super::loop_action::LoopAction;
use super::retrieve_client_actor;
use super::sharding::{next_block_has_new_shard_layout, this_block_has_new_shard_layout};
use crate::setup::state::NodeExecutionData;
use crate::utils::sharding::get_memtrie_for_shard;
use near_async::test_loop::data::TestLoopData;
use near_chain::ChainStoreAccess;
use near_chain::types::Tip;
use near_client::Client;
use near_client::client_actor::ClientActorInner;
use near_epoch_manager::shard_assignment::account_id_to_shard_id;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    ActionReceipt, ActionReceiptV2, BufferedReceiptIndices, DelayedReceiptIndices,
    PromiseYieldIndices, Receipt, ReceiptEnum, ReceiptV0, VersionedActionReceipt,
};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::AccountId;
use near_primitives::version::ProtocolFeature;
use near_store::{ShardUId, get};
use near_vm_runner::logic::ProtocolVersion;

pub enum ReceiptKind {
    Delayed,
    Buffered,
    PromiseYield,
}

/// Checks that the shards containing `accounts` have a non empty set of receipts
/// of type `kind` at the resharding block.
pub fn check_receipts_presence_at_resharding_block(
    accounts: Vec<AccountId>,
    kind: ReceiptKind,
) -> LoopAction {
    let (checked_receipts, succeeded) = LoopAction::shared_success_flag();
    let action_fn = Box::new(
        move |node_datas: &[NodeExecutionData],
              test_loop_data: &mut TestLoopData,
              client_account_id: AccountId| {
            let client_actor =
                retrieve_client_actor(node_datas, test_loop_data, &client_account_id);
            let tip = client_actor.client.chain.head().unwrap();

            if !next_block_has_new_shard_layout(client_actor.client.epoch_manager.as_ref(), &tip) {
                return;
            }

            accounts
                .iter()
                .for_each(|account| check_receipts_at_block(client_actor, &account, &kind, &tip));
            checked_receipts.set(true);
        },
    );
    LoopAction::new(action_fn, succeeded)
}

/// Checks that the shards containing `accounts` have a non empty set of receipts
/// of type `kind` at the block after the resharding block.
pub fn check_receipts_presence_after_resharding_block(
    accounts: Vec<AccountId>,
    kind: ReceiptKind,
) -> LoopAction {
    let (checked_receipts, succeeded) = LoopAction::shared_success_flag();
    let action_fn = Box::new(
        move |node_datas: &[NodeExecutionData],
              test_loop_data: &mut TestLoopData,
              client_account_id: AccountId| {
            let client_actor =
                retrieve_client_actor(node_datas, test_loop_data, &client_account_id);
            let tip = client_actor.client.chain.head().unwrap();

            if !this_block_has_new_shard_layout(client_actor.client.epoch_manager.as_ref(), &tip) {
                return;
            }

            accounts
                .iter()
                .for_each(|account| check_receipts_at_block(client_actor, &account, &kind, &tip));
            checked_receipts.set(true);
        },
    );
    LoopAction::new(action_fn, succeeded)
}

/// Asserts the presence of any receipt of type `kind` at the provided chain `tip`.
pub fn check_receipts_at_block(
    client_actor: &ClientActorInner,
    account: &AccountId,
    kind: &ReceiptKind,
    tip: &Tip,
) {
    let epoch_manager = &client_actor.client.epoch_manager;
    let shard_layout = epoch_manager.get_shard_layout(&tip.epoch_id).unwrap();
    let shard_id = account_id_to_shard_id(epoch_manager.as_ref(), &account, &tip.epoch_id).unwrap();
    let shard_uid = &ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
    let congestion_info = &client_actor
        .client
        .chain
        .chain_store()
        .get_chunk_extra(&tip.last_block_hash, shard_uid)
        .unwrap()
        .congestion_info();

    let num_shards = shard_layout.shard_ids().count();
    let has_delayed = congestion_info.delayed_receipts_gas() != 0;
    let has_buffered = congestion_info.buffered_receipts_gas() != 0;
    tracing::info!(target: "test", height=tip.height, num_shards, %shard_id, has_delayed, has_buffered, "checking receipts");

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
        ReceiptKind::PromiseYield => check_promise_yield_receipts_exist_in_memtrie(
            &client_actor.client,
            &shard_uid,
            &tip.prev_block_hash,
        ),
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

/// Asserts that a non zero amount of promise yield receipts exist in MemTrie for the given shard.
fn check_promise_yield_receipts_exist_in_memtrie(
    client: &Client,
    shard_uid: &ShardUId,
    prev_block_hash: &CryptoHash,
) {
    let memtrie = get_memtrie_for_shard(client, shard_uid, prev_block_hash);
    let indices: PromiseYieldIndices =
        get(&memtrie, &TrieKey::PromiseYieldIndices).unwrap().unwrap();
    assert_ne!(indices.len(), 0);
}

/// Takes an action receipt of any version and converts it the version that
/// matches `protocol_version`.
pub fn action_receipt_v1_to_latest(input: &Receipt, protocol_version: ProtocolVersion) -> Receipt {
    let versioned = match input.receipt() {
        near_primitives::receipt::ReceiptEnum::Action(action_receipt) => {
            VersionedActionReceipt::from(action_receipt)
        }
        near_primitives::receipt::ReceiptEnum::ActionV2(action_receipt_v2) => {
            VersionedActionReceipt::from(action_receipt_v2)
        }
        _ => panic!("must be action receipt"),
    };

    let action_receipt = if ProtocolFeature::DeterministicAccountIds.enabled(protocol_version) {
        ReceiptEnum::ActionV2(ActionReceiptV2 {
            signer_id: versioned.signer_id().clone(),
            refund_to: versioned.refund_to().clone(),
            signer_public_key: versioned.signer_public_key().clone(),
            gas_price: versioned.gas_price(),
            output_data_receivers: versioned.output_data_receivers().to_vec(),
            input_data_ids: versioned.input_data_ids().to_vec(),
            actions: versioned.actions().to_vec(),
        })
    } else {
        ReceiptEnum::Action(ActionReceipt {
            signer_id: versioned.signer_id().clone(),
            signer_public_key: versioned.signer_public_key().clone(),
            gas_price: versioned.gas_price(),
            output_data_receivers: versioned.output_data_receivers().to_vec(),
            input_data_ids: versioned.input_data_ids().to_vec(),
            actions: versioned.actions().to_vec(),
        })
    };

    Receipt::V0(ReceiptV0 {
        predecessor_id: input.predecessor_id().clone(),
        receiver_id: input.receiver_id().clone(),
        receipt_id: *input.receipt_id(),
        receipt: action_receipt,
    })
}

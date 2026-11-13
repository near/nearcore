use crate::congestion_control::DelayedReceiptQueueWrapper;
use crate::pipelining::ReceiptPreparationPipeline;
use crate::{MAX_RECEIPTS_SCANNED_FOR_PREPARATION, schedule_contract_preparation};
use near_crypto::{KeyType, PublicKey};
use near_parameters::RuntimeConfig;
use near_primitives::action::{Action, DeployContractAction};
use near_primitives::fees::{compute_receipt_congestion_gas, compute_receipt_size};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    ActionReceipt, Receipt, ReceiptEnum, ReceiptV0, StateStoredReceipt, StateStoredReceiptMetadata,
};
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::test_utils::MockEpochInfoProvider;
use near_primitives::types::{
    AccountId, Balance, EpochId, EpochInfoProvider, MerkleHash, StateChangeCause,
};
use near_primitives::version::PROTOCOL_VERSION;
use near_store::test_utils::TestTriesBuilder;
use near_store::trie::receipts_column_helper::{DelayedReceiptQueue, TrieQueue};
use near_vm_runner::{CompilePriority, FilesystemContractRuntimeCache};
use std::sync::Arc;

/// An action receipt with no actions. `ReceiptPreparationPipeline::submit` finds
/// no action to prepare, so it never selects this receipt.
fn unselectable_receipt(account_id: &AccountId, public_key: &PublicKey, index: u64) -> Receipt {
    action_receipt(account_id, public_key, index, vec![])
}

/// `DeployContract` can change the receiver's code identity, so
/// `ReceiptPreparationPipeline::submit` blocks the account and reports it acted.
fn selectable_receipt(account_id: &AccountId, public_key: &PublicKey, index: u64) -> Receipt {
    let deploy = Action::DeployContract(DeployContractAction { code: vec![0u8; 8] });
    action_receipt(account_id, public_key, index, vec![deploy])
}

fn action_receipt(
    account_id: &AccountId,
    public_key: &PublicKey,
    index: u64,
    actions: Vec<Action>,
) -> Receipt {
    Receipt::V0(ReceiptV0 {
        predecessor_id: account_id.clone(),
        receiver_id: account_id.clone(),
        receipt_id: CryptoHash::hash_bytes(&index.to_le_bytes()),
        receipt: ReceiptEnum::Action(ActionReceipt {
            signer_id: account_id.clone(),
            signer_public_key: public_key.clone(),
            gas_price: Balance::from_yoctonear(100_000_000),
            output_data_receivers: vec![],
            input_data_ids: vec![],
            actions,
        }),
    })
}

/// What one case puts in the delayed receipt queue.
struct DelayedReceiptQueueContents {
    receipt_count: u64,
    /// Index of the one receipt `ReceiptPreparationPipeline::submit` selects.
    selectable_at: Option<u64>,
    /// Puts every receipt on the shard the reading shard does not own, which is
    /// what a resharding leaves behind.
    receivers_on_other_shard: bool,
}

/// Builds the queue and reports what one `schedule_contract_preparation` call
/// returns over it.
fn schedule_contract_preparation_once(contents: DelayedReceiptQueueContents) -> Option<usize> {
    let DelayedReceiptQueueContents { receipt_count, selectable_at, receivers_on_other_shard } =
        contents;
    let boundary_account: AccountId = "receiver.near".parse().unwrap();
    let shard_layout = ShardLayout::multi_shard_custom(vec![boundary_account.clone()], 3);
    let account_id = boundary_account;
    let receiver_shard_id = shard_layout.account_id_to_shard_id(&account_id);
    let shard_id = match receivers_on_other_shard {
        true => shard_layout.shard_ids().find(|id| *id != receiver_shard_id).unwrap(),
        false => receiver_shard_id,
    };
    let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
    let epoch_info_provider = MockEpochInfoProvider::new(shard_layout.clone());
    let runtime_config = Arc::new(RuntimeConfig::test());
    let public_key = PublicKey::empty(KeyType::ED25519);
    let tries = TestTriesBuilder::new()
        .with_shard_layout(shard_layout)
        .with_flat_storage(true)
        .with_in_memory_tries(true)
        .build();

    let mut state_update = tries.new_trie_update(shard_uid, MerkleHash::default());
    let mut queue = DelayedReceiptQueue::load(&state_update).unwrap();
    for index in 0..receipt_count {
        let receipt = if selectable_at == Some(index) {
            selectable_receipt(&account_id, &public_key, index)
        } else {
            unselectable_receipt(&account_id, &public_key, index)
        };
        let congestion_gas = compute_receipt_congestion_gas(&receipt, &runtime_config).unwrap();
        let congestion_size = compute_receipt_size(&receipt).unwrap();
        let queue_entry = StateStoredReceipt::new_owned(
            receipt,
            StateStoredReceiptMetadata { congestion_gas, congestion_size },
        );
        queue.push_back(&mut state_update, &queue_entry).unwrap();
    }
    state_update.commit(StateChangeCause::InitialState);
    let trie_changes = state_update.finalize().unwrap().trie_changes;
    let mut store_update = tries.store_update();
    let state_root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
    store_update.commit();
    tries.apply_memtrie_changes(&trie_changes, shard_uid, 1);

    let state_update = tries.new_trie_update(shard_uid, state_root);
    let queue = DelayedReceiptQueue::load(&state_update).unwrap();
    let delayed_receipts =
        DelayedReceiptQueueWrapper::new(queue, &epoch_info_provider, shard_id, EpochId::default());
    let contract_cache = FilesystemContractRuntimeCache::test().unwrap();
    let mut pipeline_manager = ReceiptPreparationPipeline::new(
        Arc::clone(&runtime_config),
        None,
        Some(Box::new(contract_cache)),
        state_update.contract_storage().clone(),
        epoch_info_provider.chain_id(),
        shard_id,
        CompilePriority::Critical,
        PROTOCOL_VERSION,
    );
    schedule_contract_preparation(
        &mut pipeline_manager,
        &state_update,
        &mut delayed_receipts.peek_iter(&state_update),
    )
}

#[test]
fn contract_preparation_stopping_at_bound_asks_to_be_called_again() {
    let bound = MAX_RECEIPTS_SCANNED_FOR_PREPARATION as u64;
    assert_eq!(
        schedule_contract_preparation_once(DelayedReceiptQueueContents {
            receipt_count: bound * 2,
            selectable_at: None,
            receivers_on_other_shard: false,
        }),
        Some(bound as usize)
    );
}

#[test]
fn contract_preparation_reaching_end_reports_nothing_to_prepare() {
    let bound = MAX_RECEIPTS_SCANNED_FOR_PREPARATION as u64;
    assert_eq!(
        schedule_contract_preparation_once(DelayedReceiptQueueContents {
            receipt_count: bound / 2,
            selectable_at: None,
            receivers_on_other_shard: false,
        }),
        None
    );
}

#[test]
fn contract_preparation_finds_selectable_receipt_within_bound() {
    let bound = MAX_RECEIPTS_SCANNED_FOR_PREPARATION as u64;
    assert_eq!(
        schedule_contract_preparation_once(DelayedReceiptQueueContents {
            receipt_count: bound * 2,
            selectable_at: Some(10),
            receivers_on_other_shard: false,
        }),
        Some(11)
    );
}

#[test]
fn contract_preparation_past_bound_defers_selectable_receipt() {
    let bound = MAX_RECEIPTS_SCANNED_FOR_PREPARATION as u64;
    // Reporting `None` here would stop the caller scheduling preparation for the
    // rest of the chunk, leaving this receipt unprepared.
    assert_eq!(
        schedule_contract_preparation_once(DelayedReceiptQueueContents {
            receipt_count: bound * 2,
            selectable_at: Some(bound + 500),
            receivers_on_other_shard: false,
        }),
        Some(bound as usize)
    );
}

#[test]
fn contract_preparation_bound_counts_receipts_of_other_shard() {
    let bound = MAX_RECEIPTS_SCANNED_FOR_PREPARATION as u64;
    // Every entry read belongs to the sibling shard, so the bound stops the read with
    // nothing found. `None` would report that the queue holds nothing left to prepare,
    // although the read stopped at the bound and not at the end of the queue.
    assert_eq!(
        schedule_contract_preparation_once(DelayedReceiptQueueContents {
            receipt_count: bound * 2,
            selectable_at: Some(10),
            receivers_on_other_shard: true,
        }),
        Some(1)
    );
}

pub(crate) mod sync_utils;
pub mod test_utils;

use crate::db::{GENESIS_CONGESTION_INFO_KEY, GENESIS_HEIGHT_KEY};
use crate::trie::AccessOptions;
use crate::{DBCol, GENESIS_STATE_ROOTS_KEY, Store, StoreUpdate, TrieAccess, TrieUpdate};
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::PublicKey;
use near_primitives::account::{AccessKey, Account};
use near_primitives::bandwidth_scheduler::BandwidthSchedulerState;
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    BufferedReceiptIndices, DelayedReceiptIndices, PromiseYieldIndices, PromiseYieldTimeout,
    Receipt, ReceiptEnum, ReceivedData,
};
use near_primitives::trie_key::{TrieKey, trie_key_parsers};
use near_primitives::types::{AccountId, BlockHeight, StateRoot};
use std::io;

/// Reads an object from Trie.
/// # Errors
/// see StorageError
pub fn get<T: BorshDeserialize>(
    trie: &dyn TrieAccess,
    key: &TrieKey,
) -> Result<Option<T>, StorageError> {
    match trie.get(key, AccessOptions::DEFAULT)? {
        None => Ok(None),
        Some(data) => match T::try_from_slice(&data) {
            Err(err) => Err(StorageError::StorageInconsistentState(format!(
                "Failed to deserialize. err={err:?}"
            ))),
            Ok(value) => Ok(Some(value)),
        },
    }
}

/// [`get`] without incurring side effects.
pub fn get_pure<T: BorshDeserialize>(
    trie: &dyn TrieAccess,
    key: &TrieKey,
) -> Result<Option<T>, StorageError> {
    match trie.get(key, AccessOptions::NO_SIDE_EFFECTS)? {
        None => Ok(None),
        Some(data) => match T::try_from_slice(&data) {
            Err(_err) => {
                Err(StorageError::StorageInconsistentState("Failed to deserialize".to_string()))
            }
            Ok(value) => Ok(Some(value)),
        },
    }
}

/// Writes an object into Trie.
pub fn set<T: BorshSerialize>(state_update: &mut TrieUpdate, key: TrieKey, value: &T) {
    let data = borsh::to_vec(&value).expect("Borsh serializer is not expected to ever fail");
    state_update.set(key, data);
}

pub fn set_account(state_update: &mut TrieUpdate, account_id: AccountId, account: &Account) {
    set(state_update, TrieKey::Account { account_id }, account)
}

pub fn get_account(
    trie: &dyn TrieAccess,
    account_id: &AccountId,
) -> Result<Option<Account>, StorageError> {
    get(trie, &TrieKey::Account { account_id: account_id.clone() })
}

pub fn set_received_data(
    state_update: &mut TrieUpdate,
    receiver_id: AccountId,
    data_id: CryptoHash,
    data: &ReceivedData,
) {
    set(state_update, TrieKey::ReceivedData { receiver_id, data_id }, data);
}

pub fn get_received_data(
    trie: &dyn TrieAccess,
    receiver_id: &AccountId,
    data_id: CryptoHash,
) -> Result<Option<ReceivedData>, StorageError> {
    get(trie, &TrieKey::ReceivedData { receiver_id: receiver_id.clone(), data_id })
}

pub fn has_received_data(
    trie: &dyn TrieAccess,
    receiver_id: &AccountId,
    data_id: CryptoHash,
) -> Result<bool, StorageError> {
    trie.contains_key(
        &TrieKey::ReceivedData { receiver_id: receiver_id.clone(), data_id },
        AccessOptions::DEFAULT,
    )
}

pub fn set_postponed_receipt(state_update: &mut TrieUpdate, receipt: &Receipt) {
    assert!(matches!(receipt.receipt(), ReceiptEnum::Action(_)));
    let key = TrieKey::PostponedReceipt {
        receiver_id: receipt.receiver_id().clone(),
        receipt_id: *receipt.receipt_id(),
    };
    set(state_update, key, receipt);
}

pub fn remove_postponed_receipt(
    state_update: &mut TrieUpdate,
    receiver_id: &AccountId,
    receipt_id: CryptoHash,
) {
    state_update.remove(TrieKey::PostponedReceipt { receiver_id: receiver_id.clone(), receipt_id });
}

pub fn get_postponed_receipt(
    trie: &dyn TrieAccess,
    receiver_id: &AccountId,
    receipt_id: CryptoHash,
) -> Result<Option<Receipt>, StorageError> {
    get(trie, &TrieKey::PostponedReceipt { receiver_id: receiver_id.clone(), receipt_id })
}

pub fn get_delayed_receipt_indices(
    trie: &dyn TrieAccess,
) -> Result<DelayedReceiptIndices, StorageError> {
    Ok(get(trie, &TrieKey::DelayedReceiptIndices)?.unwrap_or_default())
}

// Adds the given receipt into the end of the delayed receipt queue in the state.
pub fn set_delayed_receipt(
    state_update: &mut TrieUpdate,
    delayed_receipts_indices: &mut DelayedReceiptIndices,
    receipt: &Receipt,
) {
    set(
        state_update,
        TrieKey::DelayedReceipt { index: delayed_receipts_indices.next_available_index },
        receipt,
    );
    delayed_receipts_indices.next_available_index = delayed_receipts_indices
        .next_available_index
        .checked_add(1)
        .expect("Next available index for delayed receipt exceeded the integer limit");
}

pub fn get_promise_yield_indices(
    trie: &dyn TrieAccess,
) -> Result<PromiseYieldIndices, StorageError> {
    Ok(get(trie, &TrieKey::PromiseYieldIndices)?.unwrap_or_default())
}

pub fn set_promise_yield_indices(
    state_update: &mut TrieUpdate,
    promise_yield_indices: &PromiseYieldIndices,
) {
    set(state_update, TrieKey::PromiseYieldIndices, promise_yield_indices);
}

// Enqueues given timeout to the PromiseYield timeout queue
pub fn enqueue_promise_yield_timeout(
    state_update: &mut TrieUpdate,
    promise_yield_indices: &mut PromiseYieldIndices,
    account_id: AccountId,
    data_id: CryptoHash,
    expires_at: BlockHeight,
) {
    set(
        state_update,
        TrieKey::PromiseYieldTimeout { index: promise_yield_indices.next_available_index },
        &PromiseYieldTimeout { account_id, data_id, expires_at },
    );
    promise_yield_indices.next_available_index = promise_yield_indices
        .next_available_index
        .checked_add(1)
        .expect("Next available index for PromiseYield timeout queue exceeded the integer limit");
}

pub fn set_promise_yield_receipt(state_update: &mut TrieUpdate, receipt: &Receipt) {
    match receipt.receipt() {
        ReceiptEnum::PromiseYield(action_receipt) => {
            assert!(action_receipt.input_data_ids.len() == 1);
            let key = TrieKey::PromiseYieldReceipt {
                receiver_id: receipt.receiver_id().clone(),
                data_id: action_receipt.input_data_ids[0],
            };
            set(state_update, key, receipt);
        }
        _ => unreachable!("Expected PromiseYield receipt"),
    }
}

pub fn remove_promise_yield_receipt(
    state_update: &mut TrieUpdate,
    receiver_id: &AccountId,
    data_id: CryptoHash,
) {
    state_update.remove(TrieKey::PromiseYieldReceipt { receiver_id: receiver_id.clone(), data_id });
}

pub fn get_promise_yield_receipt(
    trie: &dyn TrieAccess,
    receiver_id: &AccountId,
    data_id: CryptoHash,
) -> Result<Option<Receipt>, StorageError> {
    get(trie, &TrieKey::PromiseYieldReceipt { receiver_id: receiver_id.clone(), data_id })
}

pub fn has_promise_yield_receipt(
    trie: &dyn TrieAccess,
    receiver_id: AccountId,
    data_id: CryptoHash,
) -> Result<bool, StorageError> {
    trie.contains_key(
        &TrieKey::PromiseYieldReceipt { receiver_id, data_id },
        AccessOptions::DEFAULT,
    )
}

pub fn get_buffered_receipt_indices(
    trie: &dyn TrieAccess,
) -> Result<BufferedReceiptIndices, StorageError> {
    Ok(get(trie, &TrieKey::BufferedReceiptIndices)?.unwrap_or_default())
}

pub fn get_bandwidth_scheduler_state(
    trie: &dyn TrieAccess,
) -> Result<Option<BandwidthSchedulerState>, StorageError> {
    get(trie, &TrieKey::BandwidthSchedulerState)
}

pub fn set_bandwidth_scheduler_state(
    state_update: &mut TrieUpdate,
    scheduler_state: &BandwidthSchedulerState,
) {
    set(state_update, TrieKey::BandwidthSchedulerState, scheduler_state);
}

pub fn set_access_key(
    state_update: &mut TrieUpdate,
    account_id: AccountId,
    public_key: PublicKey,
    access_key: &AccessKey,
) {
    set(state_update, TrieKey::AccessKey { account_id, public_key }, access_key);
}

pub fn remove_access_key(
    state_update: &mut TrieUpdate,
    account_id: AccountId,
    public_key: PublicKey,
) {
    state_update.remove(TrieKey::AccessKey { account_id, public_key });
}

pub fn get_access_key(
    trie: &dyn TrieAccess,
    account_id: &AccountId,
    public_key: &PublicKey,
) -> Result<Option<AccessKey>, StorageError> {
    get(
        trie,
        &TrieKey::AccessKey { account_id: account_id.clone(), public_key: public_key.clone() },
    )
}

pub fn get_access_key_raw(
    trie: &dyn TrieAccess,
    raw_key: &[u8],
) -> Result<Option<AccessKey>, StorageError> {
    get(
        trie,
        &trie_key_parsers::parse_trie_key_access_key_from_raw_key(raw_key)
            .expect("access key in the state should be correct"),
    )
}

/// Removes account, code and all access keys associated to it.
pub fn remove_account(
    state_update: &mut TrieUpdate,
    account_id: &AccountId,
) -> Result<(), StorageError> {
    state_update.remove(TrieKey::Account { account_id: account_id.clone() });
    state_update.remove(TrieKey::ContractCode { account_id: account_id.clone() });

    // Removing access keys
    let lock = state_update.trie().lock_for_iter();
    let public_keys = state_update
        .locked_iter(&trie_key_parsers::get_raw_prefix_for_access_keys(account_id), &lock)?
        .map(|raw_key| {
            trie_key_parsers::parse_public_key_from_access_key_key(&raw_key?, account_id).map_err(
                |_e| {
                    StorageError::StorageInconsistentState(
                        "Can't parse public key from raw key for AccessKey".to_string(),
                    )
                },
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    drop(lock);

    for public_key in public_keys {
        state_update.remove(TrieKey::AccessKey { account_id: account_id.clone(), public_key });
    }

    // Removing contract data
    let lock = state_update.trie().lock_for_iter();
    let data_keys = state_update
        .locked_iter(&trie_key_parsers::get_raw_prefix_for_contract_data(account_id, &[]), &lock)?
        .map(|raw_key| {
            trie_key_parsers::parse_data_key_from_contract_data_key(&raw_key?, account_id)
                .map_err(|_e| {
                    StorageError::StorageInconsistentState(
                        "Can't parse data key from raw key for ContractData".to_string(),
                    )
                })
                .map(Vec::from)
        })
        .collect::<Result<Vec<_>, _>>()?;
    drop(lock);

    for key in data_keys {
        state_update.remove(TrieKey::ContractData { account_id: account_id.clone(), key });
    }
    Ok(())
}

pub fn get_genesis_state_roots(store: &Store) -> io::Result<Option<Vec<StateRoot>>> {
    store.get_ser::<Vec<StateRoot>>(DBCol::BlockMisc, GENESIS_STATE_ROOTS_KEY)
}

pub fn get_genesis_congestion_infos(store: &Store) -> io::Result<Option<Vec<CongestionInfo>>> {
    store.get_ser::<Vec<CongestionInfo>>(DBCol::BlockMisc, GENESIS_CONGESTION_INFO_KEY)
}

pub fn set_genesis_state_roots(store_update: &mut StoreUpdate, genesis_roots: &[StateRoot]) {
    store_update
        .set_ser(DBCol::BlockMisc, GENESIS_STATE_ROOTS_KEY, genesis_roots)
        .expect("Borsh cannot fail");
}

pub fn set_genesis_congestion_infos(
    store_update: &mut StoreUpdate,
    congestion_infos: &[CongestionInfo],
) {
    store_update
        .set_ser(DBCol::BlockMisc, GENESIS_CONGESTION_INFO_KEY, &congestion_infos)
        .expect("Borsh cannot fail");
}

pub fn get_genesis_height(store: &Store) -> io::Result<Option<BlockHeight>> {
    store.get_ser::<BlockHeight>(DBCol::BlockMisc, GENESIS_HEIGHT_KEY)
}

pub fn set_genesis_height(store_update: &mut StoreUpdate, genesis_height: &BlockHeight) {
    store_update
        .set_ser::<BlockHeight>(DBCol::BlockMisc, GENESIS_HEIGHT_KEY, genesis_height)
        .expect("Borsh cannot fail");
}

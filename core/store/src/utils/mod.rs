pub(crate) mod sync_utils;
pub mod test_utils;

use crate::db::{GENESIS_CONGESTION_INFO_KEY, GENESIS_HEIGHT_KEY};
use crate::trie::AccessOptions;
use crate::{DBCol, GENESIS_STATE_ROOTS_KEY, Store, StoreUpdate, TrieAccess, TrieUpdate};
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::{PublicKey, PublicKeyHandle};
use near_primitives::account::{AccessKey, Account};
use near_primitives::bandwidth_scheduler::BandwidthSchedulerState;
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::errors::StorageError;
use near_primitives::hash::{CryptoHash, YieldId};
use near_primitives::receipt::{
    BufferedReceiptIndices, DelayedReceiptIndices, PromiseYieldIndices, PromiseYieldTimeout,
    Receipt, ReceivedData, VersionedReceiptEnum,
};
use near_primitives::trie_key::{TrieKey, trie_key_parsers};
use near_primitives::types::{
    AccountId, Balance, BlockHeight, Nonce, NonceIndex, PromiseYieldStatus, StateRoot,
};

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
    assert!(matches!(receipt.versioned_receipt(), VersionedReceiptEnum::Action(_)));
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
    match receipt.versioned_receipt() {
        VersionedReceiptEnum::PromiseYield(action_receipt) => {
            assert!(action_receipt.input_data_ids().len() == 1);
            let key = TrieKey::PromiseYieldReceipt {
                receiver_id: receipt.receiver_id().clone(),
                data_id: action_receipt.input_data_ids()[0],
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

pub fn get_promise_yield_status(
    trie: &dyn TrieAccess,
    receiver_id: &AccountId,
    data_id: CryptoHash,
) -> Result<Option<PromiseYieldStatus>, StorageError> {
    get(trie, &TrieKey::PromiseYieldStatus { receiver_id: receiver_id.clone(), data_id })
}

pub fn has_promise_yield_status(
    trie: &dyn TrieAccess,
    receiver_id: &AccountId,
    data_id: CryptoHash,
) -> Result<bool, StorageError> {
    trie.contains_key(
        &TrieKey::PromiseYieldStatus { receiver_id: receiver_id.clone(), data_id },
        AccessOptions::DEFAULT,
    )
}

pub fn set_promise_yield_status(
    state_update: &mut TrieUpdate,
    receiver_id: &AccountId,
    data_id: CryptoHash,
    status: PromiseYieldStatus,
) {
    set(
        state_update,
        TrieKey::PromiseYieldStatus { receiver_id: receiver_id.clone(), data_id },
        &status,
    );
}

pub fn remove_promise_yield_status(
    state_update: &mut TrieUpdate,
    receiver_id: &AccountId,
    data_id: CryptoHash,
) {
    state_update.remove(TrieKey::PromiseYieldStatus { receiver_id: receiver_id.clone(), data_id });
}

pub fn set_yield_id_mapping(
    state_update: &mut TrieUpdate,
    receiver_id: &AccountId,
    yield_id: YieldId,
    data_id: CryptoHash,
) {
    set(
        state_update,
        TrieKey::YieldIdToDataId { receiver_id: receiver_id.clone(), yield_id },
        &data_id,
    );
    set(
        state_update,
        TrieKey::DataIdToYieldId { receiver_id: receiver_id.clone(), data_id },
        &yield_id,
    );
}

pub fn get_data_id_for_yield_id(
    trie: &dyn TrieAccess,
    receiver_id: &AccountId,
    yield_id: YieldId,
) -> Result<Option<CryptoHash>, StorageError> {
    get(trie, &TrieKey::YieldIdToDataId { receiver_id: receiver_id.clone(), yield_id })
}

pub fn get_yield_id_for_data_id(
    trie: &dyn TrieAccess,
    receiver_id: &AccountId,
    data_id: CryptoHash,
) -> Result<Option<YieldId>, StorageError> {
    get(trie, &TrieKey::DataIdToYieldId { receiver_id: receiver_id.clone(), data_id })
}

pub fn has_yield_id_mapping(
    trie: &dyn TrieAccess,
    receiver_id: &AccountId,
    yield_id: YieldId,
) -> Result<bool, StorageError> {
    trie.contains_key(
        &TrieKey::YieldIdToDataId { receiver_id: receiver_id.clone(), yield_id },
        AccessOptions::DEFAULT,
    )
}

pub fn remove_yield_id_mappings(
    state_update: &mut TrieUpdate,
    receiver_id: &AccountId,
    yield_id: YieldId,
    data_id: CryptoHash,
) {
    state_update.remove(TrieKey::YieldIdToDataId { receiver_id: receiver_id.clone(), yield_id });
    state_update.remove(TrieKey::DataIdToYieldId { receiver_id: receiver_id.clone(), data_id });
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
    set_access_key_by_handle(
        state_update,
        account_id,
        PublicKeyHandle::from(public_key),
        access_key,
    );
}

/// Variant of [`set_access_key`] used by genesis state application and
/// iteration paths that already hold a `PublicKeyHandle` (e.g. when reading
/// from a `StateRecord` or replaying state changes).
pub fn set_access_key_by_handle(
    state_update: &mut TrieUpdate,
    account_id: AccountId,
    key_handle: PublicKeyHandle,
    access_key: &AccessKey,
) {
    debug_assert!(
        access_key.gas_key_info().is_none() || access_key.nonce == 0,
        "gas key access key must have nonce 0, got {}",
        access_key.nonce
    );
    set(state_update, TrieKey::access_key(account_id, key_handle), access_key);
}

pub fn set_gas_key_nonce(
    state_update: &mut TrieUpdate,
    account_id: AccountId,
    public_key: PublicKey,
    index: NonceIndex,
    nonce: Nonce,
) {
    set_gas_key_nonce_by_handle(
        state_update,
        account_id,
        PublicKeyHandle::from(public_key),
        index,
        nonce,
    );
}

pub fn set_gas_key_nonce_by_handle(
    state_update: &mut TrieUpdate,
    account_id: AccountId,
    key_handle: PublicKeyHandle,
    index: NonceIndex,
    nonce: Nonce,
) {
    set(state_update, TrieKey::gas_key_nonce(account_id, key_handle, index), &nonce);
}

pub fn remove_access_key(
    state_update: &mut TrieUpdate,
    account_id: AccountId,
    public_key: PublicKey,
) {
    state_update.remove(TrieKey::access_key(account_id, public_key));
}

pub fn remove_gas_key_nonce(
    state_update: &mut TrieUpdate,
    account_id: AccountId,
    public_key: PublicKey,
    nonce_index: NonceIndex,
) {
    state_update.remove(TrieKey::gas_key_nonce(account_id, public_key, nonce_index));
}

pub fn get_access_key(
    trie: &dyn TrieAccess,
    account_id: &AccountId,
    public_key: &PublicKey,
) -> Result<Option<AccessKey>, StorageError> {
    get_access_key_by_handle(trie, account_id, &public_key.into())
}

/// Variant of [`get_access_key`] used by the trie-iteration paths
/// (`compute_gas_key_balance_sum`, `remove_account`, view RPC) that
/// already hold a `PublicKeyHandle` produced by the parse function.
pub fn get_access_key_by_handle(
    trie: &dyn TrieAccess,
    account_id: &AccountId,
    key_handle: &PublicKeyHandle,
) -> Result<Option<AccessKey>, StorageError> {
    get(trie, &TrieKey::access_key(account_id.clone(), key_handle.clone()))
}

pub fn get_gas_key_nonce(
    trie: &dyn TrieAccess,
    account_id: &AccountId,
    public_key: &PublicKey,
    index: NonceIndex,
) -> Result<Option<Nonce>, StorageError> {
    get(trie, &TrieKey::gas_key_nonce(account_id.clone(), public_key, index))
}

/// Computes the total balance across all gas keys for a given account.
pub fn compute_gas_key_balance_sum(
    state_update: &TrieUpdate,
    account_id: &AccountId,
) -> Result<Balance, StorageError> {
    let mut total = Balance::ZERO;
    let lock = state_update.trie().lock_for_iter();
    for raw_key in state_update
        .locked_iter(&trie_key_parsers::get_raw_prefix_for_access_keys(account_id), &lock)?
    {
        let raw_key = raw_key?;
        let key_handle = trie_key_parsers::parse_key_handle_from_access_key_key(
            &raw_key, account_id,
        )
        .map_err(|_e| {
            StorageError::StorageInconsistentState(
                "Can't parse key handle from raw key for AccessKey".to_string(),
            )
        })?;
        let nonce_index =
            trie_key_parsers::parse_nonce_index_from_gas_key_key(&raw_key, account_id, &key_handle)
                .map_err(|_e| {
                    StorageError::StorageInconsistentState(
                        "Can't parse nonce index from raw key for AccessKey".to_string(),
                    )
                })?;
        if nonce_index.is_some() {
            continue;
        }
        if let Some(balance) = get_access_key_by_handle(state_update, account_id, &key_handle)?
            .as_ref()
            .and_then(|access_key| access_key.gas_key_info())
            .map(|gas_key_info| gas_key_info.balance)
        {
            total = total.checked_add(balance).ok_or_else(|| {
                StorageError::StorageInconsistentState("gas key balance overflow".to_string())
            })?;
        }
    }
    Ok(total)
}

pub struct RemoveAccountResult {
    pub gas_key_nonce_count: usize,
    pub gas_key_nonce_total_key_bytes: usize, // used to calculate compute cost
}

/// Removes account, code and all access keys and gas keys associated to it.
pub fn remove_account(
    state_update: &mut TrieUpdate,
    account_id: &AccountId,
) -> Result<RemoveAccountResult, StorageError> {
    state_update.remove(TrieKey::Account { account_id: account_id.clone() });
    state_update.remove(TrieKey::ContractCode { account_id: account_id.clone() });

    let mut gas_key_nonce_count: usize = 0;
    let mut gas_key_nonce_total_key_bytes: usize = 0;

    // Removing access keys and gas key nonces
    let lock = state_update.trie().lock_for_iter();
    let mut keys_to_remove: Vec<TrieKey> = Vec::new();
    for raw_key in state_update
        .locked_iter(&trie_key_parsers::get_raw_prefix_for_access_keys(account_id), &lock)?
    {
        let raw_key = raw_key?;
        let key_handle = trie_key_parsers::parse_key_handle_from_access_key_key(
            &raw_key, account_id,
        )
        .map_err(|_e| {
            StorageError::StorageInconsistentState(
                "Can't parse key handle from raw key for AccessKey".to_string(),
            )
        })?;
        let nonce_index =
            trie_key_parsers::parse_nonce_index_from_gas_key_key(&raw_key, account_id, &key_handle)
                .map_err(|_e| {
                    StorageError::StorageInconsistentState(
                        "Can't parse nonce index from raw key for AccessKey".to_string(),
                    )
                })?;
        if let Some(index) = nonce_index {
            gas_key_nonce_count += 1;
            gas_key_nonce_total_key_bytes += raw_key.len();
            keys_to_remove.push(TrieKey::gas_key_nonce(
                account_id.clone(),
                key_handle.clone(),
                index,
            ));
        } else {
            keys_to_remove.push(TrieKey::access_key(account_id.clone(), key_handle.clone()));
        }
    }
    drop(lock);

    for trie_key in keys_to_remove {
        state_update.remove(trie_key);
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
    Ok(RemoveAccountResult { gas_key_nonce_count, gas_key_nonce_total_key_bytes })
}

pub fn get_genesis_state_roots(store: &Store) -> Option<Vec<StateRoot>> {
    store.get_ser::<Vec<StateRoot>>(DBCol::BlockMisc, GENESIS_STATE_ROOTS_KEY)
}

pub fn get_genesis_congestion_infos(store: &Store) -> Option<Vec<CongestionInfo>> {
    store.get_ser::<Vec<CongestionInfo>>(DBCol::BlockMisc, GENESIS_CONGESTION_INFO_KEY)
}

pub fn set_genesis_state_roots(store_update: &mut StoreUpdate, genesis_roots: &[StateRoot]) {
    store_update.set_ser(DBCol::BlockMisc, GENESIS_STATE_ROOTS_KEY, genesis_roots);
}

pub fn set_genesis_congestion_infos(
    store_update: &mut StoreUpdate,
    congestion_infos: &[CongestionInfo],
) {
    store_update.set_ser(DBCol::BlockMisc, GENESIS_CONGESTION_INFO_KEY, &congestion_infos);
}

pub fn get_genesis_height(store: &Store) -> Option<BlockHeight> {
    store.get_ser::<BlockHeight>(DBCol::BlockMisc, GENESIS_HEIGHT_KEY)
}

pub fn set_genesis_height(store_update: &mut StoreUpdate, genesis_height: &BlockHeight) {
    store_update.set_ser::<BlockHeight>(DBCol::BlockMisc, GENESIS_HEIGHT_KEY, genesis_height);
}

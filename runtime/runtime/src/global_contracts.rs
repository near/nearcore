use std::collections::BTreeSet;
use std::sync::Arc;

use near_primitives::account::{Account, AccountContract};
use near_primitives::action::{
    DeployGlobalContractAction, GlobalContractDeployMode, GlobalContractIdentifier,
    UseGlobalContractAction,
};
use near_primitives::errors::{ActionErrorKind, IntegerOverflowError, RuntimeError};
use near_primitives::global_contract::ContractIsLocalError;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::receipt::{
    GlobalContractDistributionReceipt, Receipt, ReceiptEnum, ReceiptOrigin, ReceiptOriginReceipt,
    ReceiptToTxInfo, ReceiptToTxInfoV1,
};
use near_primitives::trie_key::{GlobalContractCodeIdentifier, TrieKey};
use near_primitives::types::{AccountId, EpochInfoProvider, ShardId, StateChangeCause};
use near_primitives::version::ProtocolFeature;
use near_store::trie::AccessOptions;
use near_store::{KeyLookupMode, StorageError, TrieAccess as _, TrieUpdate};
use near_vm_runner::logic::ProtocolVersion;
use near_vm_runner::{ContractCode, precompile_contract};

use crate::congestion_control::ReceiptSink;
use crate::{ActionResult, ApplyState, clear_account_contract_storage_usage};

pub(crate) fn action_deploy_global_contract(
    state_update: &mut TrieUpdate,
    account: &mut Account,
    account_id: &AccountId,
    apply_state: &ApplyState,
    deploy_contract: &DeployGlobalContractAction,
    result: &mut ActionResult,
) -> Result<(), RuntimeError> {
    let _span = tracing::debug_span!(target: "runtime", "action_deploy_global_contract").entered();

    let storage_cost = apply_state
        .config
        .fees
        .storage_usage_config
        .global_contract_storage_amount_per_byte
        .saturating_mul(deploy_contract.code.len() as u128);
    let Some(updated_balance) = account.amount().checked_sub(storage_cost) else {
        result.result = Err(ActionErrorKind::LackBalanceForState {
            account_id: account_id.clone(),
            amount: storage_cost,
        }
        .into());
        return Ok(());
    };
    if ProtocolFeature::IncludeDeployGlobalContractOutcomeBurntStorage
        .enabled(apply_state.current_protocol_version)
    {
        result.tokens_burnt =
            result.tokens_burnt.checked_add(storage_cost).ok_or(IntegerOverflowError)?;
    }
    account.set_amount(updated_balance);

    initiate_distribution(
        state_update,
        account_id.clone(),
        deploy_contract.code.clone(),
        &deploy_contract.deploy_mode,
        apply_state.shard_id,
        apply_state.current_protocol_version,
        result,
    )?;

    Ok(())
}

pub(crate) fn action_use_global_contract(
    state_update: &mut TrieUpdate,
    account_id: &AccountId,
    account: &mut Account,
    action: &UseGlobalContractAction,
    current_protocol_version: ProtocolVersion,
    result: &mut ActionResult,
) -> Result<(), RuntimeError> {
    let _span = tracing::debug_span!(target: "runtime", "action_use_global_contract").entered();
    use_global_contract(
        state_update,
        account_id,
        account,
        &action.contract_identifier,
        current_protocol_version,
        result,
    )
}

pub(crate) fn use_global_contract(
    state_update: &mut TrieUpdate,
    account_id: &AccountId,
    account: &mut Account,
    contract_identifier: &GlobalContractIdentifier,
    current_protocol_version: ProtocolVersion,
    result: &mut ActionResult,
) -> Result<(), RuntimeError> {
    let key = TrieKey::GlobalContractCode { identifier: contract_identifier.clone().into() };
    if !state_update.contains_key(&key, AccessOptions::DEFAULT)? {
        result.result = Err(ActionErrorKind::GlobalContractDoesNotExist {
            identifier: contract_identifier.clone(),
        }
        .into());
        return Ok(());
    }
    clear_account_contract_storage_usage(
        state_update,
        account_id,
        account,
        current_protocol_version,
    )?;
    if account.contract().is_local() {
        state_update.remove(TrieKey::ContractCode { account_id: account_id.clone() });
    }
    let contract = match contract_identifier {
        GlobalContractIdentifier::CodeHash(code_hash) => AccountContract::Global(*code_hash),
        GlobalContractIdentifier::AccountId(id) => AccountContract::GlobalByAccount(id.clone()),
    };
    account.set_storage_usage(
        account.storage_usage().checked_add(contract_identifier.len() as u64).ok_or_else(|| {
            StorageError::StorageInconsistentState(format!(
                "Storage usage integer overflow for account {}",
                account_id
            ))
        })?,
    );
    account.set_contract(contract);
    Ok(())
}

pub(crate) fn apply_global_contract_distribution_receipt(
    receipt: &Receipt,
    apply_state: &ApplyState,
    epoch_info_provider: &dyn EpochInfoProvider,
    state_update: &mut TrieUpdate,
    receipt_sink: &mut ReceiptSink,
    receipt_to_tx: &mut Vec<(CryptoHash, ReceiptToTxInfo)>,
) -> Result<(), RuntimeError> {
    let _span = tracing::debug_span!(
        target: "runtime",
        "apply_global_contract_distribution_receipt",
    )
    .entered();

    let ReceiptEnum::GlobalContractDistribution(global_contract_data) = receipt.receipt() else {
        unreachable!("given receipt should be an global contract distribution receipt")
    };
    apply_distribution_current_shard(receipt, global_contract_data, apply_state, state_update)?;
    forward_distribution_next_shard(
        receipt,
        global_contract_data,
        apply_state,
        epoch_info_provider,
        state_update,
        receipt_sink,
        receipt_to_tx,
    )?;

    Ok(())
}

fn initiate_distribution(
    state_update: &mut TrieUpdate,
    account_id: AccountId,
    contract_code: Arc<[u8]>,
    deploy_mode: &GlobalContractDeployMode,
    current_shard_id: ShardId,
    protocol_version: ProtocolVersion,
    result: &mut ActionResult,
) -> Result<(), RuntimeError> {
    let id = match deploy_mode {
        GlobalContractDeployMode::CodeHash => {
            GlobalContractIdentifier::CodeHash(hash(&contract_code))
        }
        GlobalContractDeployMode::AccountId => {
            GlobalContractIdentifier::AccountId(account_id.clone())
        }
    };
    // Increment the nonce and write it to state immediately to prevent multiple
    // distributions with the same nonce from being initiated. This requires
    // allowing the same nonce in the freshness check when applying the
    // distribution receipt.
    let nonce = increment_nonce(protocol_version, state_update, &id)?;
    let distribution_receipt = GlobalContractDistributionReceipt::new(
        id,
        current_shard_id,
        vec![],
        contract_code,
        nonce,
        protocol_version,
    );
    let distribution_receipts =
        Receipt::new_global_contract_distribution(account_id, distribution_receipt);
    // No need to set receipt_id here, it will be generated as part of apply_action_receipt
    result.new_receipts.push(distribution_receipts);
    Ok(())
}

/// Increments the nonce for the given global contract identifier and writes
/// it to state immediately.
fn increment_nonce(
    protocol_version: u32,
    state_update: &mut TrieUpdate,
    id: &GlobalContractIdentifier,
) -> Result<u64, RuntimeError> {
    if !ProtocolFeature::GlobalContractDistributionNonce.enabled(protocol_version) {
        // If the feature is not enabled yet the nonce will be ignored anyway.
        return Ok(0);
    }

    let identifier: GlobalContractCodeIdentifier = id.clone().into();

    let nonce_key = TrieKey::GlobalContractNonce { identifier };
    let stored_nonce = get_nonce(state_update, &nonce_key)?;

    let new_nonce = stored_nonce.checked_add(1).ok_or_else(|| {
        RuntimeError::UnexpectedIntegerOverflow("GlobalContractDistributionNonce".into())
    })?;
    set_nonce(state_update, nonce_key, new_nonce);
    Ok(new_nonce)
}

fn apply_distribution_current_shard(
    receipt: &Receipt,
    global_contract_data: &GlobalContractDistributionReceipt,
    apply_state: &ApplyState,
    state_update: &mut TrieUpdate,
) -> Result<(), RuntimeError> {
    let identifier = match &global_contract_data.id() {
        GlobalContractIdentifier::CodeHash(hash) => GlobalContractCodeIdentifier::CodeHash(*hash),
        GlobalContractIdentifier::AccountId(account_id) => {
            GlobalContractCodeIdentifier::AccountId(account_id.clone())
        }
    };

    let is_nonce_fresh =
        check_and_update_nonce(global_contract_data, &identifier, apply_state, state_update)?;
    if !is_nonce_fresh {
        return Ok(());
    }

    let config = apply_state.config.wasm_config.clone();
    let trie_key = TrieKey::GlobalContractCode { identifier };
    state_update.set(trie_key, global_contract_data.code().to_vec());
    state_update.commit(StateChangeCause::ReceiptProcessing { receipt_hash: receipt.get_hash() });
    let code_hash = match global_contract_data.id() {
        GlobalContractIdentifier::CodeHash(hash) => Some(*hash),
        GlobalContractIdentifier::AccountId(_) => None,
    };
    let _ = precompile_contract(
        &ContractCode::new(global_contract_data.code().to_vec(), code_hash),
        config,
        apply_state.cache.as_deref(),
    );
    Ok(())
}

// Checks if the incoming nonce is fresh and updates the stored nonce. Returns
// true if the nonce is fresh, false if it's stale. The nonce is set
// immediately and the freshness check allows the same nonce (>=).
fn check_and_update_nonce(
    global_contract_data: &GlobalContractDistributionReceipt,
    identifier: &GlobalContractCodeIdentifier,
    apply_state: &ApplyState,
    state_update: &mut TrieUpdate,
) -> Result<bool, RuntimeError> {
    if !ProtocolFeature::GlobalContractDistributionNonce
        .enabled(apply_state.current_protocol_version)
    {
        return Ok(true);
    }

    let nonce_key = TrieKey::GlobalContractNonce { identifier: identifier.clone() };
    let stored_nonce = get_nonce(state_update, &nonce_key)?;
    let incoming_nonce = global_contract_data.nonce();

    // Allow the same nonce since the nonce is updated immediately when
    // initiating distribution to prevent multiple distributions with the same
    // nonce from being initiated.
    if incoming_nonce < stored_nonce {
        return Ok(false);
    }

    set_nonce(state_update, nonce_key, incoming_nonce);
    Ok(true)
}

fn set_nonce(state_update: &mut TrieUpdate, nonce_key: TrieKey, nonce: u64) {
    state_update.set(nonce_key, nonce.to_le_bytes().to_vec());
}

// Retrieves the stored nonce for the given global contract identifier. If no
// nonce is stored, returns 0.
fn get_nonce(state_update: &TrieUpdate, nonce_key: &TrieKey) -> Result<u64, RuntimeError> {
    let stored_nonce = state_update.get(nonce_key, AccessOptions::DEFAULT)?;
    let Some(stored_nonce) = stored_nonce else {
        return Ok(0);
    };
    let stored_nonce: [u8; 8] =
        stored_nonce.try_into().expect("GlobalContractNonce should be 8 bytes");
    let stored_nonce = u64::from_le_bytes(stored_nonce);
    Ok(stored_nonce)
}

fn forward_distribution_next_shard(
    receipt: &Receipt,
    global_contract_data: &GlobalContractDistributionReceipt,
    apply_state: &ApplyState,
    epoch_info_provider: &dyn EpochInfoProvider,
    state_update: &mut TrieUpdate,
    receipt_sink: &mut ReceiptSink,
    receipt_to_tx: &mut Vec<(CryptoHash, ReceiptToTxInfo)>,
) -> Result<(), RuntimeError> {
    let shard_layout = epoch_info_provider.shard_layout(&apply_state.epoch_id)?;
    let already_delivered_shards = BTreeSet::from_iter(
        global_contract_data
            .already_delivered_shards()
            .iter()
            .cloned()
            .chain(std::iter::once(apply_state.shard_id)),
    );
    let Some(next_shard) = shard_layout
        .shard_ids()
        .filter(|shard_id| !already_delivered_shards.contains(&shard_id))
        .next()
    else {
        return Ok(());
    };
    let already_delivered_shards = Vec::from_iter(already_delivered_shards);
    let predecessor_id = receipt.predecessor_id().clone();
    let next_receipt = global_contract_data.forward(next_shard, already_delivered_shards);
    let mut next_receipt = Receipt::new_global_contract_distribution(predecessor_id, next_receipt);
    let receipt_id = apply_state.create_receipt_id(receipt.receipt_id(), 0);
    next_receipt.set_receipt_id(receipt_id);
    receipt_to_tx.push((
        receipt_id,
        ReceiptToTxInfo::V1(ReceiptToTxInfoV1 {
            origin: ReceiptOrigin::FromReceipt(ReceiptOriginReceipt {
                parent_receipt_id: *receipt.receipt_id(),
                parent_creator_account_id: receipt.predecessor_id().clone(),
            }),
            receiver_account_id: next_receipt.receiver_id().clone(),
        }),
    ));
    receipt_sink.forward_or_buffer_receipt(next_receipt, apply_state, state_update)?;
    Ok(())
}

pub(crate) trait AccountContractAccessExt {
    fn hash(self, store: &TrieUpdate) -> Result<CryptoHash, StorageError>;
    fn code(
        self,
        local_account_id: &AccountId,
        store: &TrieUpdate,
    ) -> Result<Option<ContractCode>, StorageError>;
}

impl AccountContractAccessExt for AccountContract {
    fn code(
        self,
        local_account_id: &AccountId,
        store: &TrieUpdate,
    ) -> Result<Option<ContractCode>, StorageError> {
        let local_hash = match GlobalContractIdentifier::try_from(self) {
            Ok(identifier) => return identifier.code(store),
            Err(ContractIsLocalError::NotDeployed) => return Ok(None),
            Err(ContractIsLocalError::Deployed(local_hash)) => local_hash,
        };
        let key = TrieKey::ContractCode { account_id: local_account_id.clone() };
        let code = store.get(&key, AccessOptions::DEFAULT)?;
        Ok(code.map(|code| ContractCode::new(code, Some(local_hash))))
    }

    fn hash(self, store: &TrieUpdate) -> Result<CryptoHash, StorageError> {
        match GlobalContractIdentifier::try_from(self) {
            Ok(gci) => return gci.hash(store),
            Err(ContractIsLocalError::NotDeployed) => return Ok(CryptoHash::default()),
            Err(ContractIsLocalError::Deployed(local_hash)) => Ok(local_hash),
        }
    }
}

pub(crate) trait GlobalContractAccessExt {
    fn hash(self, store: &TrieUpdate) -> Result<CryptoHash, StorageError>;
    fn code(self, store: &TrieUpdate) -> Result<Option<ContractCode>, StorageError>;
}

impl GlobalContractAccessExt for GlobalContractIdentifier {
    fn hash(self, store: &TrieUpdate) -> Result<CryptoHash, StorageError> {
        if let GlobalContractIdentifier::CodeHash(crypto_hash) = self {
            return Ok(crypto_hash);
        }
        let key = TrieKey::GlobalContractCode { identifier: self.into() };
        let value_ref = store
            .get_ref(&key, KeyLookupMode::MemOrFlatOrTrie, AccessOptions::DEFAULT)?
            .ok_or_else(|| {
                let TrieKey::GlobalContractCode { identifier } = key else { unreachable!() };
                StorageError::StorageInconsistentState(format!(
                    "Global contract identifier not found {:?}",
                    identifier
                ))
            })?;
        Ok(value_ref.value_hash())
    }

    fn code(self, store: &TrieUpdate) -> Result<Option<ContractCode>, StorageError> {
        let key = TrieKey::GlobalContractCode { identifier: self.clone().into() };
        let code_hash = match self {
            GlobalContractIdentifier::AccountId(_) => None,
            GlobalContractIdentifier::CodeHash(hash) => Some(hash),
        };
        let code = store.get(&key, AccessOptions::DEFAULT)?;
        Ok(code.map(|code| ContractCode::new(code, code_hash)))
    }
}

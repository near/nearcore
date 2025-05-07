use std::collections::BTreeSet;
use std::sync::Arc;

use near_primitives::account::{Account, AccountContract};
use near_primitives::action::{
    DeployGlobalContractAction, GlobalContractDeployMode, GlobalContractIdentifier,
    UseGlobalContractAction,
};
use near_primitives::chunk_apply_stats::ChunkApplyStatsV0;
use near_primitives::errors::{ActionErrorKind, RuntimeError};
use near_primitives::hash::hash;
use near_primitives::receipt::{GlobalContractDistributionReceipt, Receipt, ReceiptEnum};
use near_primitives::trie_key::{GlobalContractCodeIdentifier, TrieKey};
use near_primitives::types::{AccountId, EpochInfoProvider, ShardId, StateChangeCause};
use near_store::trie::AccessOptions;
use near_store::{StorageError, TrieUpdate};
use near_vm_runner::logic::ProtocolVersion;
use near_vm_runner::{ContractCode, precompile_contract};

use crate::congestion_control::ReceiptSink;
use crate::{ActionResult, ApplyState, clear_account_contract_storage_usage};

pub(crate) fn action_deploy_global_contract(
    account: &mut Account,
    account_id: &AccountId,
    apply_state: &ApplyState,
    deploy_contract: &DeployGlobalContractAction,
    result: &mut ActionResult,
    stats: &mut ChunkApplyStatsV0,
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
    stats.balance.global_actions_burnt_amount =
        stats.balance.global_actions_burnt_amount.saturating_add(storage_cost);
    account.set_amount(updated_balance);

    initiate_distribution(
        account_id.clone(),
        deploy_contract.code.clone(),
        &deploy_contract.deploy_mode,
        apply_state.shard_id,
        result,
    );

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
    let key = TrieKey::GlobalContractCode { identifier: action.contract_identifier.clone().into() };
    if !state_update.contains_key(&key, AccessOptions::DEFAULT)? {
        result.result = Err(ActionErrorKind::GlobalContractDoesNotExist {
            identifier: action.contract_identifier.clone(),
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
    let contract = match &action.contract_identifier {
        GlobalContractIdentifier::CodeHash(code_hash) => AccountContract::Global(*code_hash),
        GlobalContractIdentifier::AccountId(id) => AccountContract::GlobalByAccount(id.clone()),
    };
    account.set_storage_usage(
        account.storage_usage().checked_add(action.contract_identifier.len() as u64).ok_or_else(
            || {
                StorageError::StorageInconsistentState(format!(
                    "Storage usage integer overflow for account {}",
                    account_id
                ))
            },
        )?,
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
) -> Result<(), RuntimeError> {
    let _span = tracing::debug_span!(
        target: "runtime",
        "apply_global_contract_distribution_receipt",
    )
    .entered();

    let ReceiptEnum::GlobalContractDistribution(global_contract_data) = receipt.receipt() else {
        unreachable!("given receipt should be an global contract distribution receipt")
    };
    apply_distribution_current_shard(receipt, global_contract_data, apply_state, state_update);
    forward_distribution_next_shard(
        receipt,
        global_contract_data,
        apply_state,
        epoch_info_provider,
        state_update,
        receipt_sink,
    )?;

    Ok(())
}

fn initiate_distribution(
    account_id: AccountId,
    contract_code: Arc<[u8]>,
    deploy_mode: &GlobalContractDeployMode,
    current_shard_id: ShardId,
    result: &mut ActionResult,
) {
    let id = match deploy_mode {
        GlobalContractDeployMode::CodeHash => {
            GlobalContractIdentifier::CodeHash(hash(&contract_code))
        }
        GlobalContractDeployMode::AccountId => {
            GlobalContractIdentifier::AccountId(account_id.clone())
        }
    };
    let distribution_receipts = Receipt::new_global_contract_distribution(
        account_id,
        GlobalContractDistributionReceipt::new(
            id,
            // We can start with any shard, using the current one just for convenience
            current_shard_id,
            vec![],
            contract_code,
        ),
    );
    // No need to set receipt_id here, it will be generated as part of apply_action_receipt
    result.new_receipts.push(distribution_receipts);
}

fn apply_distribution_current_shard(
    receipt: &Receipt,
    global_contract_data: &GlobalContractDistributionReceipt,
    apply_state: &ApplyState,
    state_update: &mut TrieUpdate,
) {
    let config = apply_state.config.wasm_config.clone();
    let trie_key = TrieKey::GlobalContractCode {
        identifier: match &global_contract_data.id() {
            GlobalContractIdentifier::CodeHash(hash) => {
                GlobalContractCodeIdentifier::CodeHash(*hash)
            }
            GlobalContractIdentifier::AccountId(account_id) => {
                GlobalContractCodeIdentifier::AccountId(account_id.clone())
            }
        },
    };
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
}

fn forward_distribution_next_shard(
    receipt: &Receipt,
    global_contract_data: &GlobalContractDistributionReceipt,
    apply_state: &ApplyState,
    epoch_info_provider: &dyn EpochInfoProvider,
    state_update: &mut TrieUpdate,
    receipt_sink: &mut ReceiptSink,
) -> Result<(), RuntimeError> {
    let shard_layout = epoch_info_provider.shard_layout(&apply_state.epoch_id)?;
    let already_delivered_shards = BTreeSet::from_iter(
        global_contract_data
            .already_delivered_shards()
            .iter()
            .cloned()
            .chain(std::iter::once(apply_state.shard_id)),
    );
    if let Some(next_shard) = shard_layout
        .shard_ids()
        .filter(|shard_id| !already_delivered_shards.contains(&shard_id))
        .next()
    {
        let mut next_receipt = Receipt::new_global_contract_distribution(
            receipt.predecessor_id().clone(),
            GlobalContractDistributionReceipt::new(
                global_contract_data.id().clone(),
                next_shard,
                Vec::from_iter(already_delivered_shards),
                global_contract_data.code().clone(),
            ),
        );
        let receipt_id = apply_state.create_receipt_id(receipt.receipt_id(), 0);
        next_receipt.set_receipt_id(receipt_id);
        receipt_sink.forward_or_buffer_receipt(
            next_receipt,
            apply_state,
            state_update,
            epoch_info_provider,
        )?;
    }
    Ok(())
}

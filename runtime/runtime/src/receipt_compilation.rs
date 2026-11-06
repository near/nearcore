//! Conservative compilation preflight for action receipts.
//!
//! Before starting to execute actions in an action receipt, we ensure that all
//! touched contracts are admitted for execution. This is to avoid an excessive
//! compilation workload in a chunk.
//!
//! Specifically, we check that all WASM code that could _potentially_ run in a receipt needs to be either:
//!  - already compiled according to the consensus compilation status cache
//!  - or the WASM code bytes fit in the cold cache compilation budget of the chunk
//!
//! Note that this does not simulate action execution, we just assume all
//! actions in the receipt succeed. If an action fails during execution,
//! following actions are not executed and hence their contracts would not need
//! to be prepared. Those contracts still need to go through admission.

use crate::ApplyState;
use crate::cache_warming::precompile_contract_with_warming;
use crate::contract_code::RuntimeContractIdentifier;
use crate::contract_compilation_status::{
    contract_compilation_is_admitted, try_admit_contract_compilation,
};
use crate::function_call::record_contract_call;
use crate::wallet_contract::eth_wallet_global_contract_hash;
use near_primitives::account::AccountContract;
use near_primitives::account::id::AccountType;
use near_primitives::action::{Action, GlobalContractIdentifier};
use near_primitives::errors::RuntimeError;
use near_primitives::hash::hash;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::AccountId;
use near_primitives::universal_state_init::{DecodeMode, UniversalStateInit};
use near_primitives::version::ProtocolFeature;
use near_store::trie::AccessOptions;
use near_store::{ContractCompilationAdmission, TrieUpdate, get_account};
use near_vm_runner::ContractCode;
use std::sync::Arc;

/// Preflight an action receipt by admitting and preparing contracts in action
/// order, stopping at the first exhausted compilation budget.
///
/// Previously admitted hashes remain warm on the next attempt, so the complete
/// dependency set need not fit in a single chunk's compilation budget.
///
/// Execution later asserts that each contract was admitted. It relies on this
/// preflight check and does not perform a second execution-time admission.
///
/// No account, dependency, or action state is mutated in this function.
pub(crate) fn preflight_receipt_contracts(
    state_update: &mut TrieUpdate,
    apply_state: &ApplyState,
    account_id: &AccountId,
    actions: &[Action],
    chain_id: &str,
) -> Result<ContractCompilationAdmission, RuntimeError> {
    let account = get_account(state_update, account_id)?;
    let mut exists = account.is_some();
    let mut initialized = account.as_ref().is_some_and(|account| account.is_initialized());
    let mut contract =
        account.map_or(AccountContract::None, |account| account.contract().into_owned());
    // Code deployed by this receipt, updated after every deploy action.
    let mut deployed_hash = None;

    for action in actions {
        match action {
            Action::DeployContract(deploy) => {
                let code_hash = hash(&deploy.code);
                if !contract_compilation_is_admitted(state_update, apply_state, code_hash)? {
                    if try_admit_contract_compilation(
                        state_update,
                        apply_state,
                        code_hash,
                        deploy.code.len() as u64,
                    )? == ContractCompilationAdmission::Deferred
                    {
                        return Ok(ContractCompilationAdmission::Deferred);
                    }
                    // Compile now, not when the deployment executes: a later
                    // dependency may defer the entire receipt.
                    let code = ContractCode::new(deploy.code.clone(), Some(code_hash));
                    precompile(&code, apply_state);
                }
                contract = AccountContract::Local(code_hash);
                deployed_hash = Some(code_hash);
            }
            Action::FunctionCall(_) => {
                let identifier = RuntimeContractIdentifier::resolve(
                    account_id,
                    contract.clone(),
                    state_update,
                    chain_id,
                    AccessOptions::DEFAULT,
                    apply_state.current_protocol_version,
                )?;
                // check if code is cold
                match identifier {
                    // fn call without code will fail, no contract code to compile or admit
                    RuntimeContractIdentifier::None => continue,
                    RuntimeContractIdentifier::AccountLocal { code_hash, .. } => {
                        if Some(code_hash) == deployed_hash {
                            // deployed in this receipt, code was already admitted
                            continue;
                        }
                    }
                    RuntimeContractIdentifier::Global { .. } => {}
                }
                if contract_compilation_is_admitted(state_update, apply_state, identifier.hash())? {
                    continue;
                }

                // contract code is cold: check if it fits the budget
                let Some(code_len) = identifier.resolve_code_len(
                    state_update,
                    AccessOptions::DEFAULT,
                    chain_id,
                    apply_state.current_protocol_version,
                )?
                else {
                    // The protocol's wallet contract may not exist on this chain.
                    continue;
                };
                if try_admit_contract_compilation(
                    state_update,
                    apply_state,
                    identifier.hash(),
                    code_len,
                )? == ContractCompilationAdmission::Deferred
                {
                    return Ok(ContractCompilationAdmission::Deferred);
                }

                // Record the function call even if the receipt may still be deferred later.
                // Contracts needed by a deferred preflight must also be sent to
                // validators, even though no function call executes this chunk.
                record_contract_call(state_update, &identifier, &apply_state.apply_reason)?;

                // Like demand preparation, source retrieval is independent of
                // witness reads. A validator may already have the artifact but
                // not its source. Local availability never affects admission.
                if let Some(code) = state_update.contract_storage().get(identifier.hash()) {
                    precompile(&code, apply_state);
                }
            }
            Action::UseGlobalContract(action) => {
                contract = global_contract(state_update, &action.contract_identifier)?;
                deployed_hash = None;
            }
            Action::DeterministicStateInit(action) => {
                if contract.is_none() {
                    contract = global_contract(state_update, action.state_init.code())?;
                    deployed_hash = None;
                }
                exists = true;
                initialized = true;
            }
            Action::UniversalStateInit(action) => {
                if !initialized {
                    let mode = DecodeMode::strict_if(
                        ProtocolFeature::CanonicalUniversalStateInit
                            .enabled(apply_state.current_protocol_version),
                    );
                    if let Ok(state_init) = UniversalStateInit::from_raw(&action.state_init, mode) {
                        contract = match state_init.code() {
                            Some(identifier) => global_contract(state_update, identifier)?,
                            None => AccountContract::None,
                        };
                        deployed_hash = None;
                        initialized = true;
                        exists = true;
                    }
                }
            }
            Action::CreateAccount(_) | Action::DeleteAccount(_) => {
                exists = matches!(action, Action::CreateAccount(_));
                initialized = exists;
                contract = AccountContract::None;
                deployed_hash = None;
            }
            Action::Transfer(_) => {
                if !exists {
                    if account_id.get_account_type() == AccountType::EthImplicitAccount {
                        contract = AccountContract::Global(eth_wallet_global_contract_hash(
                            chain_id,
                            apply_state.current_protocol_version,
                        ));
                    }
                    exists = true;
                    // Universal accounts created by transfers remain uninitialized.
                    initialized = account_id.get_account_type() != AccountType::UniversalAccount;
                }
            }
            // These actions do not change or use this receipt's executable contract.
            Action::Stake(_)
            | Action::AddKey(_)
            | Action::DeleteKey(_)
            | Action::Delegate(_)
            | Action::DelegateV2(_)
            | Action::DeployGlobalContract(_)
            | Action::TransferToGasKey(_)
            | Action::WithdrawFromGasKey(_) => {}
        }
    }
    Ok(ContractCompilationAdmission::Admitted)
}

/// Missing globals cause ordinary action failure, not a preflight storage error.
/// No subsequent call can execute in that case, but later explicit deployments
/// can still be conservatively admitted by the scan.
fn global_contract(
    state_update: &TrieUpdate,
    identifier: &GlobalContractIdentifier,
) -> Result<AccountContract, RuntimeError> {
    let key = TrieKey::GlobalContractCode { identifier: identifier.clone().into() };
    if !state_update.contains_key(&key, AccessOptions::DEFAULT)? {
        return Ok(AccountContract::None);
    }
    Ok(match identifier {
        GlobalContractIdentifier::CodeHash(hash) => AccountContract::Global(*hash),
        GlobalContractIdentifier::AccountId(account_id) => {
            AccountContract::GlobalByAccount(account_id.clone())
        }
    })
}

fn precompile(code: &ContractCode, apply_state: &ApplyState) {
    precompile_contract_with_warming(
        code,
        Arc::clone(&apply_state.config.wasm_config),
        apply_state.next_wasm_config.clone(),
        apply_state.cache.as_deref(),
    );
}

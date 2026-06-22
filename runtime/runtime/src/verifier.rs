use crate::action_validation::{validate_actions, validate_actions_with_mode};
use crate::config::TransactionCost;
use crate::near_primitives::account::Account;
use crate::{AccessKeyUpdate, PendingConstraints, TxVerdict, VerificationResult};
use near_crypto::PublicKey;
use near_parameters::RuntimeConfig;
use near_primitives::account::{AccessKey, FunctionCallPermission};
use near_primitives::errors::{
    DepositCostFailureReason, InvalidAccessKeyError, InvalidTxError, ReceiptValidationError,
};
use near_primitives::receipt::{
    DataReceipt, Receipt, VersionedActionReceipt, VersionedReceiptEnum,
};
use near_primitives::transaction::{
    Action, NonceMode, SignedTransaction, Transaction, ValidatedTransaction,
};
use near_primitives::types::{AccountId, Balance, BlockHeight, Nonce, StorageUsage};
use near_primitives::version::ProtocolVersion;
use near_store::{
    StorageError, TrieUpdate, get_access_key, get_account, set_access_key, set_account,
};
use near_vm_runner::logic::LimitConfig;

pub const ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT: StorageUsage = 770;

/// Possible errors when checking whether an account has enough tokens for storage staking
/// Read details of state staking
/// <https://nomicon.io/Economics/Economics.html#state-stake>.
pub enum StorageStakingError {
    /// An account does not have enough and the additional amount needed for storage staking
    LackBalanceForStorageStaking(Balance),
    /// Storage consistency error: an account has invalid storage usage or amount or locked amount
    StorageError(String),
}

/// Checks if given account has enough balance for storage stake.
///
/// Note that the current account balance has to be provided separately. This is to accommodate
/// callers which want to check for specific balance and not necessarily the balance specified
/// inside the account.
///
/// Returns:
///
///  - Ok(()) if account has enough balance or is a zero-balance account
///  - Err(StorageStakingError::LackBalanceForStorageStaking(amount)) if account doesn't have enough and how much need to be added,
///  - Err(StorageStakingError::StorageError(err)) if account has invalid storage usage or amount/locked.
pub fn check_storage_stake(
    account: &Account,
    account_balance: Balance,
    runtime_config: &RuntimeConfig,
) -> Result<(), StorageStakingError> {
    let billable_storage_bytes = account.storage_usage();
    let required_amount = runtime_config
        .storage_amount_per_byte()
        .checked_mul(u128::from(billable_storage_bytes))
        .ok_or_else(|| {
            format!(
                "Account's billable storage usage {} overflows multiplication",
                billable_storage_bytes
            )
        })
        .map_err(StorageStakingError::StorageError)?;
    let available_amount = account_balance
        .checked_add(account.locked())
        .ok_or_else(|| {
            format!(
                "Account's amount {} and locked {} overflow addition",
                account.amount(),
                account.locked(),
            )
        })
        .map_err(StorageStakingError::StorageError)?;
    if available_amount >= required_amount {
        Ok(())
    } else {
        if is_zero_balance_account(account) {
            return Ok(());
        }
        Err(StorageStakingError::LackBalanceForStorageStaking(
            required_amount.checked_sub(available_amount).unwrap(),
        ))
    }
}

/// Zero Balance Account introduced in NEP 448 https://github.com/near/NEPs/pull/448
/// An account is a zero balance account if and only if the account uses no more than `ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT` bytes
fn is_zero_balance_account(account: &Account) -> bool {
    account.storage_usage() <= ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT
}

fn validate_transaction_actions(
    config: &RuntimeConfig,
    signed_tx: &SignedTransaction,
    current_protocol_version: ProtocolVersion,
) -> Result<(), InvalidTxError> {
    validate_actions(
        &config.wasm_config.limit_config,
        signed_tx.transaction.actions(),
        signed_tx.transaction.receiver_id(),
        current_protocol_version,
    )
    .map_err(InvalidTxError::ActionsValidation)
}

/// Validates the transaction without using the state. It allows any node to validate a
/// transaction before forwarding it to the node that tracks the `signer_id` account.
#[allow(clippy::result_large_err)]
pub fn validate_transaction(
    config: &RuntimeConfig,
    signed_tx: SignedTransaction,
    current_protocol_version: ProtocolVersion,
) -> Result<ValidatedTransaction, (InvalidTxError, SignedTransaction)> {
    if let Err(err) = validate_transaction_actions(&config, &signed_tx, current_protocol_version) {
        return Err((err, signed_tx));
    }
    ValidatedTransaction::new(config, signed_tx, current_protocol_version)
}

/// Set new `signer` and `access_key` in `state_update`.
///
/// Note that this does not commit state changes to the `TrieUpdate`.
pub fn set_tx_state_changes(
    state_update: &mut TrieUpdate,
    validated_tx: &ValidatedTransaction,
    signer: &Account,
    access_key: &AccessKey,
) {
    let tx = validated_tx.to_tx();
    set_access_key(state_update, tx.signer_id().clone(), tx.public_key().clone(), &access_key);
    set_account(state_update, tx.signer_id().clone(), &signer);
}

pub fn get_signer_and_access_key(
    state_update: &dyn near_store::TrieAccess,
    validated_tx: &ValidatedTransaction,
) -> Result<(Account, AccessKey), InvalidTxError> {
    let signer_id = validated_tx.signer_id();

    let signer = match get_account(state_update, signer_id)? {
        Some(signer) => signer,
        None => {
            return Err(InvalidTxError::SignerDoesNotExist { signer_id: signer_id.clone() });
        }
    };

    let access_key = match get_access_key(state_update, signer_id, validated_tx.public_key())? {
        Some(access_key) => access_key,
        None => {
            return Err(InvalidTxError::InvalidAccessKeyError(
                InvalidAccessKeyError::AccessKeyNotFound {
                    account_id: signer_id.clone(),
                    public_key: validated_tx.public_key().clone().into(),
                },
            )
            .into());
        }
    };
    Ok((signer, access_key))
}

/// Validates FunctionCall permission constraints:
/// - Transaction must have exactly one action
/// - Action must be FunctionCall with zero deposit
/// - Receiver must match permission's receiver
/// - Method name must be in allowed list (if list is non-empty)
fn verify_function_call_permission(
    function_call_permission: &FunctionCallPermission,
    tx: &Transaction,
) -> Result<(), InvalidTxError> {
    if tx.actions().len() != 1 {
        return Err(InvalidTxError::InvalidAccessKeyError(
            InvalidAccessKeyError::RequiresFullAccess,
        ));
    }
    let Some(Action::FunctionCall(function_call)) = tx.actions().get(0) else {
        return Err(InvalidTxError::InvalidAccessKeyError(
            InvalidAccessKeyError::RequiresFullAccess,
        ));
    };
    if function_call.deposit > Balance::ZERO {
        return Err(InvalidTxError::InvalidAccessKeyError(
            InvalidAccessKeyError::DepositWithFunctionCall,
        ));
    }
    let tx_receiver = tx.receiver_id();
    let ak_receiver = &function_call_permission.receiver_id;
    if tx_receiver != ak_receiver {
        return Err(InvalidTxError::InvalidAccessKeyError(
            InvalidAccessKeyError::ReceiverMismatch {
                tx_receiver: tx_receiver.clone(),
                ak_receiver: ak_receiver.clone(),
            },
        ));
    }
    if !function_call_permission.method_names.is_empty()
        && function_call_permission
            .method_names
            .iter()
            .all(|method_name| &function_call.method_name != method_name)
    {
        return Err(InvalidTxError::InvalidAccessKeyError(
            InvalidAccessKeyError::MethodNameMismatch {
                method_name: function_call.method_name.clone(),
            },
        ));
    }
    Ok(())
}

/// Verify that the transaction nonce is valid.
fn verify_nonce(
    tx_nonce: Nonce,
    current_nonce: Nonce,
    block_height: Option<BlockHeight>,
    nonce_mode: NonceMode,
) -> Result<(), InvalidTxError> {
    match nonce_mode {
        NonceMode::Monotonic => {
            if tx_nonce <= current_nonce {
                return Err(InvalidTxError::InvalidNonce { tx_nonce, ak_nonce: current_nonce });
            }
        }
        NonceMode::Strict => {
            if !current_nonce.checked_add(1).is_some_and(|expected| tx_nonce == expected) {
                return Err(InvalidTxError::InvalidNonce { tx_nonce, ak_nonce: current_nonce });
            }
        }
    }
    if let Some(height) = block_height {
        let upper_bound = height
            .saturating_mul(near_primitives::account::AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER);
        if tx_nonce >= upper_bound {
            return Err(InvalidTxError::NonceTooLarge { tx_nonce, upper_bound });
        }
    }
    Ok(())
}

fn check_and_compute_new_allowance(
    access_key: &AccessKey,
    account_id: &AccountId,
    public_key: &PublicKey,
    total_cost: Balance,
) -> Result<Option<Balance>, InvalidTxError> {
    let Some(fc) = access_key.permission.function_call_permission() else {
        return Ok(None);
    };
    let Some(allowance) = fc.allowance else {
        return Ok(None);
    };
    let new_allowance = allowance.checked_sub(total_cost).ok_or_else(|| {
        InvalidTxError::InvalidAccessKeyError(InvalidAccessKeyError::NotEnoughAllowance {
            account_id: account_id.clone(),
            public_key: public_key.clone().into(),
            allowance,
            cost: total_cost,
        })
    })?;
    Ok(Some(new_allowance))
}

/// Verify a regular (non-gas-key) transaction and compute the charge outcome.
///
/// Returns `TxVerdict::Success` or `TxVerdict::Failed` (never `DepositFailed`).
/// Callers should apply state changes via `VerificationResult::apply` on success.
///
/// This function performs no mutation; all state changes are returned in the
/// `VerificationResult`.
pub fn verify_and_charge_tx_ephemeral(
    config: &RuntimeConfig,
    account: &Account,
    access_key: &AccessKey,
    tx: &Transaction,
    transaction_cost: &TransactionCost,
    block_height: Option<BlockHeight>,
    pending: &PendingConstraints,
) -> TxVerdict {
    // It's the caller's responsibility to NOT call this function for transactions with
    // nonce_index (i.e. gas key transactions).
    assert!(
        tx.nonce().nonce_index().is_none(),
        "verify_and_charge_tx_ephemeral called for gas key transaction"
    );
    // Gas keys must be used via gas key transaction path (with nonce_index)
    if let Some(gas_key_info) = access_key.gas_key_info() {
        return TxVerdict::Failed(InvalidTxError::InvalidNonceIndex {
            tx_nonce_index: None,
            num_nonces: gas_key_info.num_nonces,
        });
    }
    let TransactionCost {
        gas_burnt,
        compute_burnt,
        gas_remaining,
        receipt_gas_price,
        total_cost,
        burnt_amount,
        ..
    } = *transaction_cost;
    let account_id = tx.signer_id();
    let tx_nonce = tx.nonce().nonce();
    let effective_nonce = std::cmp::max(access_key.nonce, pending.max_nonce);
    if let Err(e) = verify_nonce(tx_nonce, effective_nonce, block_height, tx.nonce_mode()) {
        return TxVerdict::Failed(e);
    }

    // saturating_sub is fine here: on the consensus path pending constraints
    // are always default (zero), so the subtraction is exact. On the RPC /
    // chunk-production path it is best-effort and does not affect consensus.
    let available_balance = account.amount().saturating_sub(pending.paid_from_balance);
    if available_balance < total_cost {
        return TxVerdict::Failed(InvalidTxError::NotEnoughBalance {
            signer_id: account_id.clone(),
            balance: available_balance,
            cost: total_cost,
        });
    }
    // Debit only this tx's cost, not the pending amount (which was already
    // charged in prior chunks and will be applied at execution time).
    let new_amount = account.amount().checked_sub(total_cost).unwrap();

    let new_allowance = match check_and_compute_new_allowance(
        access_key,
        account_id,
        tx.public_key(),
        total_cost,
    ) {
        Ok(a) => a,
        Err(e) => return TxVerdict::Failed(e),
    };

    match check_storage_stake(account, new_amount, config) {
        Ok(()) => {}
        Err(StorageStakingError::LackBalanceForStorageStaking(amount)) => {
            return TxVerdict::Failed(InvalidTxError::LackBalanceForState {
                signer_id: account_id.clone(),
                amount,
            });
        }
        Err(StorageStakingError::StorageError(err)) => {
            return TxVerdict::Failed(StorageError::StorageInconsistentState(err).into());
        }
    };

    // Validate FunctionCall permission constraints if applicable
    if let Some(function_call_permission) = access_key.permission.function_call_permission()
        && let Err(e) = verify_function_call_permission(function_call_permission, tx)
    {
        return TxVerdict::Failed(e);
    }

    TxVerdict::Success(VerificationResult {
        gas_burnt,
        compute_burnt,
        gas_remaining,
        receipt_gas_price,
        burnt_amount,
        new_account_amount: new_amount,
        access_key_update: AccessKeyUpdate::Regular { nonce: tx_nonce, new_allowance },
    })
}

/// Verify a gas key transaction and compute the charge outcome.
///
/// This function performs validation only and does NOT mutate `account` or `access_key`.
/// Callers are responsible for applying state changes based on the returned variant:
/// - `Success(result)`: apply all state changes via `result.apply()`.
/// - `DepositFailed { result, .. }`: apply gas-only changes via `result.apply()`.
/// - `Failed(_)`: no state changes.
pub fn verify_and_charge_gas_key_tx_ephemeral(
    config: &RuntimeConfig,
    account: &Account,
    access_key: &AccessKey,
    current_nonce: Nonce,
    tx: &Transaction,
    transaction_cost: &TransactionCost,
    block_height: Option<BlockHeight>,
    pending: &PendingConstraints,
) -> TxVerdict {
    // It's the caller's responsibility to ONLY call this function for transactions with
    // nonce_index (i.e. gas key transactions).
    let Some(nonce_index) = tx.nonce().nonce_index() else {
        panic!("verify_and_charge_gas_key_tx_ephemeral called for non-gas key transaction")
    };
    let TransactionCost {
        gas_burnt,
        compute_burnt,
        gas_remaining,
        receipt_gas_price,
        burnt_amount,
        gas_cost,
        deposit_cost,
        ..
    } = *transaction_cost;
    let account_id = tx.signer_id();

    // Validate that access key is a gas key
    let Some(gas_key_info) = access_key.gas_key_info() else {
        return TxVerdict::Failed(InvalidTxError::InvalidAccessKeyError(
            InvalidAccessKeyError::AccessKeyNotFound {
                account_id: account_id.clone(),
                public_key: Box::new(tx.public_key().clone()),
            },
        ));
    };

    // Validate nonce_index is in valid range
    if nonce_index >= gas_key_info.num_nonces {
        return TxVerdict::Failed(InvalidTxError::InvalidNonceIndex {
            tx_nonce_index: Some(nonce_index),
            num_nonces: gas_key_info.num_nonces,
        });
    }

    let tx_nonce = tx.nonce().nonce();
    let effective_nonce = std::cmp::max(current_nonce, pending.max_nonce);
    if let Err(e) = verify_nonce(tx_nonce, effective_nonce, block_height, tx.nonce_mode()) {
        return TxVerdict::Failed(e);
    }

    // Check gas key has enough balance for gas costs, accounting for
    // pending gas key costs (prior gas key txs + pending WithdrawFromGasKey).
    // Unlike account balance, gas key balance only changes through transactions
    // that PTQ explicitly tracks, so pending should never exceed the balance.
    let Some(available_gas_key_balance) =
        gas_key_info.balance.checked_sub(pending.paid_from_gas_key)
    else {
        tracing::error!(
            target: "runtime",
            balance = %gas_key_info.balance,
            paid_from_gas_key = %pending.paid_from_gas_key,
            "pending gas key costs exceed gas key balance"
        );
        return TxVerdict::Failed(InvalidTxError::NotEnoughGasKeyBalance {
            signer_id: account_id.clone(),
            balance: Balance::ZERO,
            cost: gas_cost,
        });
    };
    if available_gas_key_balance < gas_cost {
        return TxVerdict::Failed(InvalidTxError::NotEnoughGasKeyBalance {
            signer_id: account_id.clone(),
            balance: available_gas_key_balance,
            cost: gas_cost,
        });
    }
    let new_gas_key_balance = gas_key_info.balance.checked_sub(gas_cost).unwrap();

    // Calculate new key balance in case of deposit failure. Charges only for the gas burned on
    // converting the transaction to a receipt.
    let Some(new_key_balance_on_deposit_failure) = gas_key_info.balance.checked_sub(burnt_amount)
    else {
        return TxVerdict::Failed(InvalidTxError::NotEnoughGasKeyBalance {
            signer_id: account_id.clone(),
            balance: gas_key_info.balance,
            cost: burnt_amount,
        });
    };

    // Validate FunctionCall permission constraints if applicable
    if let Some(function_call_permission) = access_key.permission.function_call_permission()
        && let Err(e) = verify_function_call_permission(function_call_permission, tx)
    {
        return TxVerdict::Failed(e);
    }
    let make_result = move |new_account_amount, new_key_amount| VerificationResult {
        gas_burnt,
        compute_burnt,
        gas_remaining,
        receipt_gas_price,
        burnt_amount,
        new_account_amount,
        access_key_update: AccessKeyUpdate::GasKey {
            new_balance: new_key_amount,
            nonce_index,
            nonce: tx_nonce,
        },
    };
    let make_success_result =
        move |new_account_amount| make_result(new_account_amount, new_gas_key_balance);
    let make_deposit_failed_result = move |new_account_amount| {
        make_result(new_account_amount, new_key_balance_on_deposit_failure)
    };

    // Check account has enough balance for deposits, accounting for
    // pending balance costs from prior txs. saturating_sub is fine: on the
    // consensus path pending constraints are always default (zero), so the
    // subtraction is exact. On the RPC / chunk-production path it is
    // best-effort.
    let available_balance = account.amount().saturating_sub(pending.paid_from_balance);
    if available_balance < deposit_cost {
        return TxVerdict::DepositFailed {
            result: make_deposit_failed_result(account.amount()),
            error: InvalidTxError::NotEnoughBalanceForDeposit {
                signer_id: account_id.clone(),
                balance: available_balance,
                cost: deposit_cost,
                reason: DepositCostFailureReason::NotEnoughBalance,
            },
        };
    }
    // Debit only this tx's deposit cost, not the pending amount.
    let new_account_amount = account.amount().checked_sub(deposit_cost).unwrap();

    match check_storage_stake(account, new_account_amount, config) {
        Ok(()) => {}
        Err(StorageStakingError::LackBalanceForStorageStaking(amount)) => {
            return TxVerdict::DepositFailed {
                result: make_deposit_failed_result(account.amount()),
                error: InvalidTxError::NotEnoughBalanceForDeposit {
                    signer_id: account_id.clone(),
                    balance: new_account_amount,
                    cost: amount,
                    reason: DepositCostFailureReason::LackBalanceForState,
                },
            };
        }
        Err(StorageStakingError::StorageError(err)) => {
            return TxVerdict::Failed(StorageError::StorageInconsistentState(err).into());
        }
    };

    TxVerdict::Success(make_success_result(new_account_amount))
}

/// Validates a given receipt. Checks validity of the Action or Data receipt.
pub(crate) fn validate_receipt(
    limit_config: &LimitConfig,
    receipt: &Receipt,
    current_protocol_version: ProtocolVersion,
    mode: ValidateReceiptMode,
) -> Result<(), ReceiptValidationError> {
    if mode == ValidateReceiptMode::NewReceipt {
        let receipt_size: u64 =
            borsh::object_length(receipt).unwrap().try_into().expect("Can't convert usize to u64");
        if receipt_size > limit_config.max_receipt_size {
            return Err(ReceiptValidationError::ReceiptSizeExceeded {
                size: receipt_size,
                limit: limit_config.max_receipt_size,
            });
        }
    }

    // We retain these checks here as to maintain backwards compatibility
    // with AccountId validation since we illegally parse an AccountId
    // in near-vm-logic/logic.rs#fn(VMLogic::read_and_parse_account_id)
    AccountId::validate(receipt.predecessor_id().as_ref()).map_err(|_| {
        ReceiptValidationError::InvalidPredecessorId {
            account_id: receipt.predecessor_id().to_string(),
        }
    })?;
    AccountId::validate(receipt.receiver_id().as_ref()).map_err(|_| {
        ReceiptValidationError::InvalidReceiverId { account_id: receipt.receiver_id().to_string() }
    })?;

    match receipt.versioned_receipt() {
        VersionedReceiptEnum::Action(action_receipt)
        | VersionedReceiptEnum::PromiseYield(action_receipt) => validate_action_receipt(
            limit_config,
            action_receipt,
            receipt.receiver_id(),
            current_protocol_version,
            mode,
        ),
        VersionedReceiptEnum::Data(data_receipt)
        | VersionedReceiptEnum::PromiseResume(data_receipt) => {
            validate_data_receipt(limit_config, &data_receipt)
        }
        VersionedReceiptEnum::GlobalContractDistribution(_) => Ok(()), // Distribution receipt can't be issued without a valid contract
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidateReceiptMode {
    /// Used for validating new receipts that were just created.
    /// More strict than `OldReceipt` mode, which has to handle older receipts.
    NewReceipt,
    /// Used for validating older receipts that were saved in the state/received. Less strict than
    /// NewReceipt validation. Tolerates some receipts that wouldn't pass new validation. It has to
    /// be less strict because:
    /// 1) Older receipts might have been created before new validation rules.
    /// 2) There is a bug which allows to create receipts that are above the size limit. Runtime has
    ///    to handle them gracefully until the receipt size limit bug is fixed.
    ///    See https://github.com/near/nearcore/issues/12606 for details.
    ExistingReceipt,
}

fn validate_action_receipt(
    limit_config: &LimitConfig,
    receipt: VersionedActionReceipt,
    receiver: &AccountId,
    current_protocol_version: ProtocolVersion,
    mode: ValidateReceiptMode,
) -> Result<(), ReceiptValidationError> {
    if receipt.input_data_ids().len() as u64 > limit_config.max_number_input_data_dependencies {
        return Err(ReceiptValidationError::NumberInputDataDependenciesExceeded {
            number_of_input_data_dependencies: receipt.input_data_ids().len() as u64,
            limit: limit_config.max_number_input_data_dependencies,
        });
    }

    if let Some(account_id) = receipt.refund_to() {
        AccountId::validate(account_id.as_ref()).map_err(|_| {
            ReceiptValidationError::InvalidRefundTo { account_id: account_id.to_string() }
        })?;
    }

    validate_actions_with_mode(
        limit_config,
        receipt.actions(),
        receiver,
        current_protocol_version,
        mode,
    )
    .map_err(ReceiptValidationError::ActionsValidation)
}

/// Validates given data receipt. Checks validity of the length of the returned data.
fn validate_data_receipt(
    limit_config: &LimitConfig,
    receipt: &DataReceipt,
) -> Result<(), ReceiptValidationError> {
    let data_len = receipt.data.as_ref().map(|data| data.len()).unwrap_or(0);
    if data_len as u64 > limit_config.max_length_returned_data {
        return Err(ReceiptValidationError::ReturnedValueLengthExceeded {
            length: data_len as u64,
            limit: limit_config.max_length_returned_data,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access_keys::{action_add_key, initial_nonce_value};
    use crate::config::tx_cost;
    use crate::near_primitives::shard_layout::ShardUId;
    use crate::near_primitives::trie_key::TrieKey;
    use crate::{ActionResult, ApplyState};
    use near_crypto::{InMemorySigner, KeyType, PublicKey, Signer};
    use near_primitives::account::{
        AccessKey, AccessKeyPermission, AccountContract, FunctionCallPermission,
    };
    use near_primitives::action::TransferToGasKeyAction;
    use near_primitives::apply::ApplyChunkReason;
    use near_primitives::bandwidth_scheduler::BlockBandwidthRequests;
    use near_primitives::congestion_info::BlockCongestionInfo;
    use near_primitives::errors::ActionsValidationError;
    use near_primitives::hash::{CryptoHash, hash};
    use near_primitives::receipt::ActionReceipt;
    use near_primitives::test_utils::account_new;
    use near_primitives::transaction::{
        AddKeyAction, CreateAccountAction, DeployContractAction, FunctionCallAction,
        TransactionNonce, TransferAction,
    };
    use near_primitives::types::{
        AccountId, Balance, BlockHeight, EpochId, Gas, MerkleHash, NonceIndex, StateChangeCause,
    };
    use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
    use near_store::test_utils::TestTriesBuilder;
    use near_store::{get_gas_key_nonce, set, set_access_key, set_account};
    use near_vm_runner::ContractCode;
    use std::sync::Arc;
    use testlib::runtime_utils::{alice_account, bob_account, eve_dot_alice_account};

    const TESTING_INIT_BALANCE: Balance = Balance::from_near(1_000_000_000);
    // 10 millinear (was 1). Under AccountCostIncrease the receipt's gas is purchased at
    // min_gas_purchase_price (1e9 yoctoNEAR/gas), so a 100 Tgas function-call tx costs ~2.4
    // millinear up-front — 1 millinear is no longer enough to fund the test transaction.
    const TESTING_GAS_KEY_BALANCE: Balance = Balance::from_millinear(10);

    fn test_limit_config() -> LimitConfig {
        let store = near_parameters::RuntimeConfigStore::test();
        store.get_config(PROTOCOL_VERSION).wasm_config.limit_config.clone()
    }

    fn setup_common(
        initial_balance: Balance,
        initial_locked: Balance,
        access_key: Option<AccessKey>,
    ) -> (Arc<Signer>, TrieUpdate, Balance) {
        let access_keys = if let Some(key) = access_key { vec![key] } else { vec![] };
        setup_accounts(vec![(
            alice_account(),
            initial_balance,
            initial_locked,
            access_keys,
            false,
            false,
        )])
    }

    fn setup_accounts(
        // two bools: first one is whether the account has a contract, second one is whether the
        // account has data
        accounts: Vec<(AccountId, Balance, Balance, Vec<AccessKey>, bool, bool)>,
    ) -> (Arc<Signer>, TrieUpdate, Balance) {
        let tries = TestTriesBuilder::new().build();
        let root = MerkleHash::default();

        let account_id = alice_account();
        let signer: Arc<Signer> = Arc::new(InMemorySigner::test_signer(&account_id));

        let mut initial_state = tries.new_trie_update(ShardUId::single_shard(), root);
        for (account_id, initial_balance, initial_locked, access_keys, has_contract, has_data) in
            accounts
        {
            let mut initial_account = account_new(initial_balance, CryptoHash::default());
            initial_account.set_locked(initial_locked);
            let mut key_count = 0;
            for access_key in access_keys {
                let public_key = if key_count == 0 {
                    signer.public_key()
                } else {
                    PublicKey::from_seed(KeyType::ED25519, format!("{}", key_count).as_str())
                };
                set_access_key(
                    &mut initial_state,
                    account_id.clone(),
                    public_key.clone(),
                    &access_key,
                );
                initial_account.set_storage_usage(
                    initial_account
                        .storage_usage()
                        .checked_add(
                            borsh::object_length(&public_key).unwrap() as u64
                                + borsh::object_length(&access_key).unwrap() as u64
                                + 40, // storage_config.num_extra_bytes_record,
                        )
                        .unwrap(),
                );
                key_count += 1;
            }
            if has_contract {
                let code = vec![0; 100];
                let code_hash = hash(&code);
                initial_state.set_code(
                    account_id.clone(),
                    &ContractCode::new(code.clone(), Some(code_hash)),
                );
                initial_account.set_contract(AccountContract::Local(code_hash));
                initial_account.set_storage_usage(
                    initial_account.storage_usage().checked_add(code.len() as u64).unwrap(),
                );
            }
            if has_data {
                let key = b"test".to_vec();
                let value = vec![0u8; 100];
                set(
                    &mut initial_state,
                    TrieKey::ContractData { account_id: account_id.clone(), key: key.clone() },
                    &value,
                );
                initial_account.set_storage_usage(
                    initial_account
                        .storage_usage()
                        .checked_add(key.len() as u64 + value.len() as u64 + 40)
                        .unwrap(),
                );
            }
            set_account(&mut initial_state, account_id.clone(), &initial_account);
        }
        initial_state.commit(StateChangeCause::InitialState);
        let trie_changes = initial_state.finalize().unwrap().trie_changes;
        let mut store_update = tries.store_update();
        let root = tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
        store_update.commit();

        (
            signer,
            tries.new_trie_update(ShardUId::single_shard(), root),
            Balance::from_yoctonear(100),
        )
    }

    const TEST_GAS_KEY_BLOCK_HEIGHT: BlockHeight = 10;

    fn create_apply_state(block_height: BlockHeight) -> ApplyState {
        ApplyState {
            apply_reason: ApplyChunkReason::UpdateTrackedShard,
            block_height,
            prev_block_hash: CryptoHash::default(),
            shard_id: ShardUId::single_shard().shard_id(),
            epoch_id: EpochId::default(),
            epoch_height: 3,
            gas_price: Balance::from_yoctonear(100),
            block_timestamp: 1,
            gas_limit: None,
            random_seed: CryptoHash::default(),
            current_protocol_version: ProtocolFeature::GasKeys.protocol_version(),
            config: Arc::new(RuntimeConfig::test()),
            next_wasm_config: None,
            cache: None,
            is_new_chunk: false,
            save_receipt_to_tx: false,
            congestion_info: BlockCongestionInfo::default(),
            bandwidth_requests: BlockBandwidthRequests::empty(),
            trie_access_tracker_state: Default::default(),
            on_post_state_ready: None,
        }
    }

    /// Sets up an account with a gas key using action_add_key.
    /// Returns (signer, state_update, gas_price, initial_nonce).
    fn setup_gas_key_account(
        initial_balance: Balance,
        gas_key_balance: Balance,
        num_nonces: NonceIndex,
        function_call_permission: Option<FunctionCallPermission>,
    ) -> (Arc<Signer>, TrieUpdate, Balance, Nonce) {
        let tries = TestTriesBuilder::new().build();
        let root = MerkleHash::default();
        let account_id = alice_account();
        let signer: Arc<Signer> = Arc::new(InMemorySigner::test_signer(&account_id));

        let mut state_update = tries.new_trie_update(ShardUId::single_shard(), root);
        let mut account = account_new(initial_balance, CryptoHash::default());
        set_account(&mut state_update, account_id.clone(), &account);

        // Use action_add_key to add the gas key (initializes nonces)
        let gas_key = match function_call_permission {
            Some(perm) => AccessKey::gas_key_function_call(num_nonces, perm),
            None => AccessKey::gas_key_full_access(num_nonces),
        };
        let apply_state = create_apply_state(TEST_GAS_KEY_BLOCK_HEIGHT);
        let action = AddKeyAction { public_key: signer.public_key(), access_key: gas_key };
        let mut result = ActionResult::default();
        action_add_key(
            &apply_state,
            &mut state_update,
            &mut account,
            &mut result,
            &account_id,
            &action,
        )
        .unwrap();
        assert!(result.result.is_ok(), "action_add_key failed: {:?}", result.result);

        // Fund the gas key balance
        if gas_key_balance > Balance::ZERO {
            let mut access_key =
                get_access_key(&state_update, &account_id, &signer.public_key()).unwrap().unwrap();
            access_key.gas_key_info_mut().unwrap().balance = gas_key_balance;
            set_access_key(&mut state_update, account_id.clone(), signer.public_key(), &access_key);
        }

        set_account(&mut state_update, account_id.clone(), &account);

        // Commit initial state
        state_update.commit(StateChangeCause::InitialState);
        let trie_changes = state_update.finalize().unwrap().trie_changes;
        let mut store_update = tries.store_update();
        let root = tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
        store_update.commit();

        (
            signer,
            tries.new_trie_update(ShardUId::single_shard(), root),
            Balance::from_yoctonear(100),
            initial_nonce_value(TEST_GAS_KEY_BLOCK_HEIGHT),
        )
    }

    fn assert_err_both_validations(
        config: &RuntimeConfig,
        state_update: &TrieUpdate,
        gas_price: Balance,
        signed_transaction: SignedTransaction,
        expected_err: InvalidTxError,
        current_protocol_version: ProtocolVersion,
    ) {
        let validated_tx =
            match validate_transaction(config, signed_transaction, current_protocol_version) {
                Ok(v) => v,
                Err((err, _tx)) => {
                    assert_eq!(err, expected_err);
                    return;
                }
            };
        let cost = match tx_cost(config, &validated_tx.to_tx(), gas_price) {
            Ok(c) => c,
            Err(err) => {
                assert_eq!(InvalidTxError::from(err), expected_err);
                return;
            }
        };

        let (signer, access_key) = match get_signer_and_access_key(state_update, &validated_tx) {
            Ok((signer, access_key)) => (signer, access_key),
            Err(err) => {
                assert_eq!(err, expected_err);
                return;
            }
        };

        let TxVerdict::Failed(err) = verify_and_charge_tx_ephemeral(
            config,
            &signer,
            &access_key,
            validated_tx.to_tx(),
            &cost,
            None,
            &PendingConstraints::default(),
        ) else {
            panic!("expected Failed verdict");
        };
        assert_eq!(err, expected_err);
    }

    pub fn validate_verify_and_charge_transaction(
        config: &RuntimeConfig,
        state_update: &mut TrieUpdate,
        signed_tx: SignedTransaction,
        gas_price: Balance,
        block_height: Option<BlockHeight>,
        current_protocol_version: ProtocolVersion,
    ) -> Result<VerificationResult, InvalidTxError> {
        let validated_tx = match validate_transaction(config, signed_tx, current_protocol_version) {
            Ok(validated_tx) => validated_tx,
            Err((err, _tx)) => return Err(err),
        };
        let (mut signer, mut access_key) = get_signer_and_access_key(state_update, &validated_tx)?;
        let transaction_cost = tx_cost(config, &validated_tx.to_tx(), gas_price)?;
        let tx = validated_tx.to_tx();

        // Check if this is a gas key transaction
        let verdict = if let Some(nonce_index) = tx.nonce().nonce_index() {
            let current_nonce =
                get_gas_key_nonce(state_update, tx.signer_id(), tx.public_key(), nonce_index)?
                    .unwrap_or(0);
            verify_and_charge_gas_key_tx_ephemeral(
                config,
                &signer,
                &access_key,
                current_nonce,
                tx,
                &transaction_cost,
                block_height,
                &PendingConstraints::default(),
            )
        } else {
            verify_and_charge_tx_ephemeral(
                config,
                &signer,
                &access_key,
                tx,
                &transaction_cost,
                block_height,
                &PendingConstraints::default(),
            )
        };
        let result = match verdict {
            TxVerdict::Success(result) => result,
            TxVerdict::Failed(e) | TxVerdict::DepositFailed { error: e, .. } => return Err(e),
        };
        result.apply(&mut signer, &mut access_key);
        set_tx_state_changes(state_update, &validated_tx, &signer, &access_key);
        Ok(result)
    }

    mod zero_balance_account_tests {
        use crate::near_primitives::account::id::AccountId;
        use crate::near_primitives::account::{
            AccessKey, AccessKeyPermission, Account, FunctionCallPermission,
        };
        use crate::near_primitives::types::Balance;
        use crate::verifier::tests::{TESTING_INIT_BALANCE, setup_accounts};
        use crate::verifier::{ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT, is_zero_balance_account};
        use near_store::{TrieUpdate, get_account};
        use testlib::runtime_utils::{alice_account, bob_account};

        fn set_up_test_account(
            account_id: &AccountId,
            num_full_access_keys: u64,
            num_function_call_access_keys: u64,
        ) -> (Account, TrieUpdate) {
            let mut access_keys = vec![];
            for _ in 0..num_full_access_keys {
                access_keys.push(AccessKey::full_access());
            }
            for _ in 0..num_function_call_access_keys {
                let access_key = AccessKey {
                    nonce: 0,
                    permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                        allowance: Some(Balance::from_yoctonear(100)),
                        receiver_id: "a".repeat(64),
                        method_names: vec![],
                    }),
                };
                access_keys.push(access_key);
            }
            let (_, state_update, _) = setup_accounts(vec![(
                account_id.clone(),
                TESTING_INIT_BALANCE,
                Balance::ZERO,
                access_keys,
                false,
                false,
            )]);
            let account = get_account(&state_update, account_id).unwrap().unwrap();
            (account, state_update)
        }

        /// Testing all combination of access keys in this test to make sure that an account
        /// is zero balance only if it uses <= `ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT` bytes in storage
        #[test]
        fn test_zero_balance_account_with_keys() {
            for num_full_access_key in 0..10 {
                for num_function_call_access_key in 0..10 {
                    let account_id: AccountId = format!(
                        "alice{}.near",
                        num_full_access_key * 1000 + num_function_call_access_key
                    )
                    .parse()
                    .unwrap();
                    let (account, _) = set_up_test_account(
                        &account_id,
                        num_full_access_key,
                        num_function_call_access_key,
                    );
                    let res = is_zero_balance_account(&account);
                    assert_eq!(
                        res,
                        num_full_access_key * 82
                            + num_function_call_access_key * 171
                            + std::mem::size_of::<Account>() as u64
                            <= ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT
                    );
                }
            }
        }

        /// A single function call access key that is too large (due to too many method names)
        #[test]
        fn test_zero_balance_account_with_invalid_access_key() {
            let account_id = alice_account();
            let method_names =
                (0..30).map(|i| format!("long_method_name_{}", i)).collect::<Vec<_>>();
            let (_, state_update, _) = setup_accounts(vec![(
                account_id.clone(),
                Balance::ZERO,
                Balance::ZERO,
                vec![AccessKey {
                    nonce: 0,
                    permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                        allowance: Some(Balance::from_yoctonear(100)),
                        receiver_id: bob_account().into(),
                        method_names,
                    }),
                }],
                false,
                false,
            )]);
            let account = get_account(&state_update, &account_id).unwrap().unwrap();
            assert!(!is_zero_balance_account(&account));
        }
    }

    // Transactions

    #[test]
    fn test_validate_transaction_valid() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, Balance::ZERO, Some(AccessKey::full_access()));

        let deposit = Balance::from_yoctonear(100);
        let signed_tx = SignedTransaction::send_money(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            deposit,
            CryptoHash::default(),
        );

        let verification_result = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect("valid transaction");
        // Should not be free. Burning for sending
        assert!(verification_result.gas_burnt > Gas::ZERO);
        // All burned gas goes to the validators at current gas price
        assert_eq!(
            verification_result.burnt_amount,
            gas_price.checked_mul(u128::from(verification_result.gas_burnt.as_gas())).unwrap()
        );

        let account = get_account(&state_update, &alice_account()).unwrap().unwrap();
        // Balance is decreased by (TX fees + transfer balance).
        assert_eq!(
            account.amount(),
            TESTING_INIT_BALANCE
                .checked_sub(
                    verification_result
                        .receipt_gas_price
                        .checked_mul(u128::from(verification_result.gas_remaining.as_gas()))
                        .unwrap()
                )
                .unwrap()
                .checked_sub(verification_result.burnt_amount)
                .unwrap()
                .checked_sub(deposit)
                .unwrap()
        );

        let access_key =
            get_access_key(&state_update, &alice_account(), &signer.public_key()).unwrap().unwrap();
        assert_eq!(access_key.nonce, 1);
    }

    #[test]
    fn test_validate_transaction_invalid_signature() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, Balance::ZERO, Some(AccessKey::full_access()));

        let mut tx = SignedTransaction::send_money(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            Balance::from_yoctonear(100),
            CryptoHash::default(),
        );
        tx.signature = signer.sign(CryptoHash::default().as_ref());

        assert_err_both_validations(
            &config,
            &mut state_update,
            gas_price,
            tx,
            InvalidTxError::InvalidSignature,
            PROTOCOL_VERSION,
        );
    }

    #[test]
    fn test_validate_transaction_invalid_access_key_not_found() {
        let config = RuntimeConfig::test();
        let (bad_signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, Balance::ZERO, None);

        let transaction = SignedTransaction::send_money(
            1,
            alice_account(),
            bob_account(),
            &*bad_signer,
            Balance::from_yoctonear(100),
            CryptoHash::default(),
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            transaction,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
        assert_eq!(
            err,
            InvalidTxError::InvalidAccessKeyError(InvalidAccessKeyError::AccessKeyNotFound {
                account_id: alice_account(),
                public_key: bad_signer.public_key().into(),
            })
        );
    }

    #[test]
    fn test_validate_transaction_invalid_bad_action() {
        let mut config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, Balance::ZERO, Some(AccessKey::full_access()));

        let wasm_config = Arc::make_mut(&mut config.wasm_config);
        wasm_config.limit_config.max_total_prepaid_gas = Gas::from_gas(100);

        assert_err_both_validations(
            &config,
            &mut state_update,
            gas_price,
            SignedTransaction::from_actions(
                1,
                alice_account(),
                bob_account(),
                &*signer,
                vec![Action::FunctionCall(Box::new(FunctionCallAction {
                    method_name: "hello".to_string(),
                    args: b"abc".to_vec(),
                    gas: Gas::from_gas(200),
                    deposit: Balance::ZERO,
                }))],
                CryptoHash::default(),
            ),
            InvalidTxError::ActionsValidation(ActionsValidationError::TotalPrepaidGasExceeded {
                total_prepaid_gas: Gas::from_gas(200),
                limit: Gas::from_gas(100),
            }),
            PROTOCOL_VERSION,
        );
    }

    #[test]
    fn test_validate_transaction_invalid_bad_signer() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, Balance::ZERO, Some(AccessKey::full_access()));

        let signed_tx = SignedTransaction::send_money(
            1,
            bob_account(),
            alice_account(),
            &*signer,
            Balance::from_yoctonear(100),
            CryptoHash::default(),
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
        assert_eq!(err, InvalidTxError::SignerDoesNotExist { signer_id: bob_account() });
    }

    #[test]
    fn test_validate_transaction_invalid_bad_nonce() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) = setup_common(
            TESTING_INIT_BALANCE,
            Balance::ZERO,
            Some(AccessKey { nonce: 2, permission: AccessKeyPermission::FullAccess }),
        );

        let transaction = SignedTransaction::send_money(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            Balance::from_yoctonear(100),
            CryptoHash::default(),
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            transaction,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
        assert_eq!(err, InvalidTxError::InvalidNonce { tx_nonce: 1, ak_nonce: 2 });
    }

    #[test]
    fn test_validate_transaction_invalid_balance_overflow() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, Balance::ZERO, Some(AccessKey::full_access()));

        assert_err_both_validations(
            &config,
            &mut state_update,
            gas_price,
            SignedTransaction::send_money(
                1,
                alice_account(),
                bob_account(),
                &*signer,
                Balance::MAX,
                CryptoHash::default(),
            ),
            InvalidTxError::CostOverflow,
            PROTOCOL_VERSION,
        );
    }

    #[test]
    fn test_validate_transaction_invalid_transaction_version() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, Balance::ZERO, Some(AccessKey::full_access()));

        assert_err_both_validations(
            &config,
            &mut state_update,
            gas_price,
            SignedTransaction::from_actions_v1(
                TransactionNonce::from_nonce(1),
                alice_account(),
                bob_account(),
                &*signer,
                vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(100) })],
                CryptoHash::default(),
            ),
            InvalidTxError::InvalidTransactionVersion,
            ProtocolFeature::GasKeys.protocol_version() - 1,
        );
    }

    #[test]
    fn test_validate_transaction_invalid_not_enough_balance() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, Balance::ZERO, Some(AccessKey::full_access()));

        let signed_tx = SignedTransaction::send_money(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            TESTING_INIT_BALANCE,
            CryptoHash::default(),
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
        if let InvalidTxError::NotEnoughBalance { signer_id, balance, cost } = err {
            assert_eq!(signer_id, alice_account());
            assert_eq!(balance, TESTING_INIT_BALANCE);
            assert!(cost > balance);
        } else {
            panic!("Incorrect error");
        }
    }

    #[test]
    fn test_validate_transaction_transfer_to_gas_key_not_enough_balance() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, Balance::ZERO, Some(AccessKey::full_access()));

        // TransferToGasKey with deposit exceeding account balance
        let excessive_deposit = TESTING_INIT_BALANCE.saturating_mul(2);
        let signed_tx = SignedTransaction::from_actions(
            1,
            alice_account(),
            alice_account(),
            &*signer,
            vec![Action::TransferToGasKey(Box::new(TransferToGasKeyAction {
                public_key: PublicKey::empty(KeyType::ED25519),
                deposit: excessive_deposit,
            }))],
            CryptoHash::default(),
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            ProtocolFeature::GasKeys.protocol_version(),
        )
        .expect_err("expected an error");
        if let InvalidTxError::NotEnoughBalance { signer_id, balance, cost } = err {
            assert_eq!(signer_id, alice_account());
            assert_eq!(balance, TESTING_INIT_BALANCE);
            assert!(cost > balance);
        } else {
            panic!("Incorrect error: {:?}", err);
        }
    }

    #[test]
    fn test_validate_transaction_invalid_not_enough_allowance() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) = setup_common(
            TESTING_INIT_BALANCE,
            Balance::ZERO,
            Some(AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: Some(Balance::from_yoctonear(100)),
                    receiver_id: bob_account().into(),
                    method_names: vec![],
                }),
            }),
        );

        let transaction = SignedTransaction::from_actions(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "hello".to_string(),
                args: b"abc".to_vec(),
                gas: Gas::from_gas(300),
                deposit: Balance::ZERO,
            }))],
            CryptoHash::default(),
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            transaction,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
        if let InvalidTxError::InvalidAccessKeyError(InvalidAccessKeyError::NotEnoughAllowance {
            account_id,
            public_key,
            allowance,
            cost,
        }) = err
        {
            assert_eq!(account_id, alice_account());
            assert_eq!(*public_key, signer.public_key());
            assert_eq!(allowance, Balance::from_yoctonear(100));
            assert!(cost > allowance);
        } else {
            panic!("Incorrect error");
        }
    }

    #[test]
    fn test_validate_transaction_invalid_low_balance() {
        let mut config = RuntimeConfig::free();
        let fees = Arc::make_mut(&mut config.fees);
        fees.storage_usage_config.storage_amount_per_byte = Balance::from_yoctonear(10_000_000);
        let initial_balance = Balance::from_yoctonear(1_000_000_000);
        let transfer_amount = Balance::from_yoctonear(950_000_000);
        let (signer, mut state_update, gas_price) =
            setup_common(initial_balance, Balance::ZERO, Some(AccessKey::full_access()));

        let signed_tx = SignedTransaction::send_money(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            transfer_amount,
            CryptoHash::default(),
        );

        let verification_result = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .unwrap();
        assert_eq!(verification_result.gas_burnt, Gas::ZERO);
        assert_eq!(verification_result.gas_remaining, Gas::ZERO);
        assert!(verification_result.burnt_amount.is_zero());
    }

    #[test]
    fn test_validate_transaction_invalid_low_balance_many_keys() {
        let mut config = RuntimeConfig::free();
        let fees = Arc::make_mut(&mut config.fees);
        fees.storage_usage_config.storage_amount_per_byte = Balance::from_yoctonear(10_000_000);
        let initial_balance = Balance::from_yoctonear(1_000_000_000);
        let transfer_amount = Balance::from_yoctonear(950_000_000);
        let account_id = alice_account();
        let access_keys = vec![AccessKey::full_access(); 10];
        let (signer, mut state_update, gas_price) = setup_accounts(vec![(
            account_id.clone(),
            initial_balance,
            Balance::ZERO,
            access_keys,
            false,
            false,
        )]);

        let signed_tx = SignedTransaction::send_money(
            1,
            account_id.clone(),
            bob_account(),
            &*signer,
            transfer_amount,
            CryptoHash::default(),
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
        let account = get_account(&state_update, &account_id).unwrap().unwrap();

        assert_eq!(
            err,
            InvalidTxError::LackBalanceForState {
                signer_id: account_id,
                amount: config
                    .storage_amount_per_byte()
                    .checked_mul(u128::from(account.storage_usage()))
                    .unwrap()
                    .checked_sub(initial_balance.checked_sub(transfer_amount).unwrap())
                    .unwrap()
            }
        );
    }

    #[test]
    fn test_validate_transaction_invalid_actions_for_function_call() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) = setup_common(
            TESTING_INIT_BALANCE,
            Balance::ZERO,
            Some(AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: None,
                    receiver_id: bob_account().into(),
                    method_names: vec![],
                }),
            }),
        );

        // Case 1
        let signed_tx = SignedTransaction::from_actions(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            vec![
                Action::FunctionCall(Box::new(FunctionCallAction {
                    method_name: "hello".to_string(),
                    args: b"abc".to_vec(),
                    gas: Gas::from_gas(100),
                    deposit: Balance::ZERO,
                })),
                Action::CreateAccount(CreateAccountAction {}),
            ],
            CryptoHash::default(),
        );
        validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");

        // Case 2
        let signed_tx = SignedTransaction::from_actions(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            vec![],
            CryptoHash::default(),
        );
        validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");

        // Case 3
        let signed_tx = SignedTransaction::from_actions(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::CreateAccount(CreateAccountAction {})],
            CryptoHash::default(),
        );
        validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
    }

    #[test]
    fn test_validate_transaction_invalid_receiver_for_function_call() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) = setup_common(
            TESTING_INIT_BALANCE,
            Balance::ZERO,
            Some(AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: None,
                    receiver_id: bob_account().into(),
                    method_names: vec![],
                }),
            }),
        );

        let signed_tx = SignedTransaction::from_actions(
            1,
            alice_account(),
            eve_dot_alice_account(),
            &*signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "hello".to_string(),
                args: b"abc".to_vec(),
                gas: Gas::from_gas(100),
                deposit: Balance::ZERO,
            }))],
            CryptoHash::default(),
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
        assert_eq!(
            err,
            InvalidTxError::InvalidAccessKeyError(InvalidAccessKeyError::ReceiverMismatch {
                tx_receiver: eve_dot_alice_account(),
                ak_receiver: bob_account().into()
            }),
        );
    }

    #[test]
    fn test_validate_transaction_invalid_method_name_for_function_call() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) = setup_common(
            TESTING_INIT_BALANCE,
            Balance::ZERO,
            Some(AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: None,
                    receiver_id: bob_account().into(),
                    method_names: vec!["not_hello".to_string(), "world".to_string()],
                }),
            }),
        );

        let signed_tx = SignedTransaction::from_actions(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "hello".to_string(),
                args: b"abc".to_vec(),
                gas: Gas::from_gas(100),
                deposit: Balance::ZERO,
            }))],
            CryptoHash::default(),
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
        assert_eq!(
            err,
            InvalidTxError::InvalidAccessKeyError(InvalidAccessKeyError::MethodNameMismatch {
                method_name: "hello".to_string()
            }),
        );
    }

    #[test]
    fn test_validate_transaction_deposit_with_function_call() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) = setup_common(
            TESTING_INIT_BALANCE,
            Balance::ZERO,
            Some(AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: None,
                    receiver_id: bob_account().into(),
                    method_names: vec![],
                }),
            }),
        );

        let signed_tx = SignedTransaction::from_actions(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "hello".to_string(),
                args: b"abc".to_vec(),
                gas: Gas::from_gas(100),
                deposit: Balance::from_yoctonear(100),
            }))],
            CryptoHash::default(),
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
        assert_eq!(
            err,
            InvalidTxError::InvalidAccessKeyError(InvalidAccessKeyError::DepositWithFunctionCall,)
        );
    }

    /// Verify that `OneYoctoOnPromise` does NOT relax the rule that
    /// function-call access keys cannot attach any deposit to a transaction.
    /// The feature only applies to promise-level function calls inside a
    /// contract, not to user-signed transactions.
    #[test]
    fn test_validate_transaction_deposit_with_function_call_one_yocto() {
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) = setup_common(
            TESTING_INIT_BALANCE,
            Balance::ZERO,
            Some(AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: None,
                    receiver_id: bob_account().into(),
                    method_names: vec![],
                }),
            }),
        );

        let signed_tx = SignedTransaction::from_actions(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "hello".to_string(),
                args: b"abc".to_vec(),
                gas: Gas::from_gas(100),
                deposit: Balance::from_yoctonear(1),
            }))],
            CryptoHash::default(),
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect_err("expected an error");
        assert_eq!(
            err,
            InvalidTxError::InvalidAccessKeyError(InvalidAccessKeyError::DepositWithFunctionCall,)
        );

        // The same transaction without any deposit should succeed.
        let signed_tx = SignedTransaction::from_actions(
            2,
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "hello".to_string(),
                args: b"abc".to_vec(),
                gas: Gas::from_gas(100),
                deposit: Balance::ZERO,
            }))],
            CryptoHash::default(),
        );

        validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect("transaction with zero deposit should succeed");
    }

    #[test]
    fn test_validate_transaction_exceeding_tx_size_limit() {
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, Balance::ZERO, Some(AccessKey::full_access()));

        let signed_tx = SignedTransaction::from_actions(
            1,
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::DeployContract(DeployContractAction { code: vec![1; 5] })],
            CryptoHash::default(),
        );
        // The size gate uses the full wire size (signature included) once
        // `PostQuantumSignatures` is enabled, and the body-only size before
        // that. Mirror that here so the test holds on both stable and nightly.
        let transaction_size = signed_tx.size_for_limits(PROTOCOL_VERSION);

        let mut config = RuntimeConfig::test();
        let max_transaction_size = transaction_size - 1;
        {
            let wasm_config = Arc::make_mut(&mut config.wasm_config);
            wasm_config.limit_config.max_transaction_size = transaction_size - 1;
        }

        let (err, _tx) = validate_transaction(&config, signed_tx.clone(), PROTOCOL_VERSION)
            .expect_err("expected validation error - size exceeded");
        assert_eq!(
            err,
            InvalidTxError::TransactionSizeExceeded {
                size: transaction_size,
                limit: max_transaction_size
            }
        );

        {
            let wasm_config = Arc::make_mut(&mut config.wasm_config);
            wasm_config.limit_config.max_transaction_size = transaction_size + 1;
        }

        validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            PROTOCOL_VERSION,
        )
        .expect("valid transaction");
    }

    // Receipts

    #[test]
    fn test_validate_receipt_valid() {
        let limit_config = test_limit_config();
        validate_receipt(
            &limit_config,
            &Receipt::new_balance_refund(&alice_account(), Balance::from_yoctonear(10)),
            PROTOCOL_VERSION,
            ValidateReceiptMode::NewReceipt,
        )
        .expect("valid receipt");
    }

    #[test]
    fn test_validate_action_receipt_too_many_input_deps() {
        let mut limit_config = test_limit_config();
        limit_config.max_number_input_data_dependencies = 1;
        let receiver = "alice.near".parse().unwrap();
        assert_eq!(
            validate_action_receipt(
                &limit_config,
                ActionReceipt {
                    signer_id: alice_account(),
                    signer_public_key: PublicKey::empty(KeyType::ED25519),
                    gas_price: Balance::from_yoctonear(100),
                    output_data_receivers: vec![],
                    input_data_ids: vec![CryptoHash::default(), CryptoHash::default()],
                    actions: vec![]
                }
                .into(),
                &receiver,
                PROTOCOL_VERSION,
                ValidateReceiptMode::NewReceipt,
            )
            .expect_err("expected an error"),
            ReceiptValidationError::NumberInputDataDependenciesExceeded {
                number_of_input_data_dependencies: 2,
                limit: 1
            }
        );
    }

    // DataReceipt

    #[test]
    fn test_validate_data_receipt_valid() {
        let limit_config = test_limit_config();
        validate_data_receipt(
            &limit_config,
            &DataReceipt { data_id: CryptoHash::default(), data: None },
        )
        .expect("valid data receipt");
        let data = b"hello".to_vec();
        validate_data_receipt(
            &limit_config,
            &DataReceipt { data_id: CryptoHash::default(), data: Some(data) },
        )
        .expect("valid data receipt");
    }

    #[test]
    fn test_validate_data_receipt_too_much_data() {
        let mut limit_config = test_limit_config();
        let data = b"hello".to_vec();
        limit_config.max_length_returned_data = data.len() as u64 - 1;
        assert_eq!(
            validate_data_receipt(
                &limit_config,
                &DataReceipt { data_id: CryptoHash::default(), data: Some(data.clone()) }
            )
            .expect_err("expected an error"),
            ReceiptValidationError::ReturnedValueLengthExceeded {
                length: data.len() as u64,
                limit: limit_config.max_length_returned_data
            }
        );
    }

    #[test]
    fn test_gas_key_tx_valid_full_access() {
        let config = RuntimeConfig::test();
        let num_nonces = 5;
        let gas_key_balance = TESTING_GAS_KEY_BALANCE;
        let (signer, mut state_update, gas_price, initial_nonce) =
            setup_gas_key_account(TESTING_INIT_BALANCE, gas_key_balance, num_nonces, None);

        let deposit = Balance::from_yoctonear(100);
        let nonce_index = 2;
        let signed_tx = SignedTransaction::from_actions_v1(
            TransactionNonce::from_nonce_and_index(initial_nonce + 1, nonce_index),
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::Transfer(TransferAction { deposit })],
            CryptoHash::default(),
        );

        let result = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            ProtocolFeature::GasKeys.protocol_version(),
        )
        .expect("valid gas key transaction");

        assert_eq!(result.gas_key_nonce_update(), Some((nonce_index, initial_nonce + 1)));
        assert!(result.gas_burnt > Gas::ZERO);
    }

    #[test]
    fn test_gas_key_tx_valid_function_call() {
        let config = RuntimeConfig::test();
        let num_nonces = 3;
        let permission = FunctionCallPermission {
            allowance: None,
            receiver_id: bob_account().to_string(),
            method_names: vec!["do_something".to_string()],
        };
        let (signer, mut state_update, gas_price, initial_nonce) = setup_gas_key_account(
            TESTING_INIT_BALANCE,
            TESTING_GAS_KEY_BALANCE,
            num_nonces,
            Some(permission),
        );

        let nonce_index = 0;
        let signed_tx = SignedTransaction::from_actions_v1(
            TransactionNonce::from_nonce_and_index(initial_nonce + 1, nonce_index),
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "do_something".to_string(),
                args: vec![],
                gas: Gas::from_gigagas(10),
                deposit: Balance::ZERO,
            }))],
            CryptoHash::default(),
        );

        let result = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            ProtocolFeature::GasKeys.protocol_version(),
        )
        .expect("valid gas key function call transaction");

        assert_eq!(result.gas_key_nonce_update(), Some((nonce_index, initial_nonce + 1)));
    }

    #[test]
    fn test_gas_key_tx_nonce_updated() {
        let config = RuntimeConfig::test();
        let num_nonces = 2;
        let (signer, mut state_update, gas_price, initial_nonce) =
            setup_gas_key_account(TESTING_INIT_BALANCE, TESTING_GAS_KEY_BALANCE, num_nonces, None);

        // Send first transaction with nonce_index 0
        let nonce_index = 0;
        let signed_tx1 = SignedTransaction::from_actions_v1(
            TransactionNonce::from_nonce_and_index(initial_nonce + 1, nonce_index),
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) })],
            CryptoHash::default(),
        );

        let result1 = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx1,
            gas_price,
            None,
            ProtocolFeature::GasKeys.protocol_version(),
        )
        .unwrap();
        assert_eq!(result1.gas_key_nonce_update(), Some((nonce_index, initial_nonce + 1)));

        // Send second transaction with nonce_index 1
        let nonce_index = 1;
        let signed_tx2 = SignedTransaction::from_actions_v1(
            TransactionNonce::from_nonce_and_index(initial_nonce + 1, nonce_index),
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) })],
            CryptoHash::default(),
        );

        let result2 = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx2,
            gas_price,
            None,
            ProtocolFeature::GasKeys.protocol_version(),
        )
        .unwrap();
        assert_eq!(result2.gas_key_nonce_update(), Some((nonce_index, initial_nonce + 1)));
    }

    #[test]
    fn test_gas_key_tx_not_a_gas_key() {
        // Set up a regular access key (not a gas key), then try to use it as a gas key
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, Balance::ZERO, Some(AccessKey::full_access()));

        // Try to use it as a gas key transaction (with nonce_index)
        let signed_tx = SignedTransaction::from_actions_v1(
            TransactionNonce::from_nonce_and_index(1, 0),
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(100) })],
            CryptoHash::default(),
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            ProtocolFeature::GasKeys.protocol_version(),
        )
        .expect_err("should fail for non-gas key");

        assert_eq!(
            err,
            InvalidTxError::InvalidAccessKeyError(InvalidAccessKeyError::AccessKeyNotFound {
                account_id: alice_account(),
                public_key: Box::new(signer.public_key()),
            })
        );
    }

    #[test]
    fn test_gas_key_tx_missing_nonce_index() {
        let config = RuntimeConfig::test();
        let num_nonces = 3;
        let (signer, mut state_update, gas_price, initial_nonce) =
            setup_gas_key_account(TESTING_INIT_BALANCE, Balance::ZERO, num_nonces, None);

        // Use TransactionNonce::Nonce (no nonce_index) on a gas key
        let signed_tx = SignedTransaction::from_actions_v1(
            TransactionNonce::from_nonce(initial_nonce + 1),
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(100) })],
            CryptoHash::default(),
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            ProtocolFeature::GasKeys.protocol_version(),
        )
        .expect_err("should fail without nonce_index for gas key");

        // verify_and_charge_tx_ephemeral rejects gas keys used without nonce_index
        assert_eq!(err, InvalidTxError::InvalidNonceIndex { tx_nonce_index: None, num_nonces });
    }

    #[test]
    fn test_gas_key_tx_nonce_index_out_of_range() {
        let config = RuntimeConfig::test();
        let num_nonces = 3;
        let (signer, mut state_update, gas_price, initial_nonce) =
            setup_gas_key_account(TESTING_INIT_BALANCE, Balance::ZERO, num_nonces, None);

        // Use nonce_index = 5 when only 3 nonces exist
        let signed_tx = SignedTransaction::from_actions_v1(
            TransactionNonce::from_nonce_and_index(initial_nonce + 1, 5),
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(100) })],
            CryptoHash::default(),
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            ProtocolFeature::GasKeys.protocol_version(),
        )
        .expect_err("should fail with out-of-range nonce_index");

        assert_eq!(err, InvalidTxError::InvalidNonceIndex { tx_nonce_index: Some(5), num_nonces });
    }

    #[test]
    fn test_gas_key_tx_invalid_nonce() {
        let config = RuntimeConfig::test();
        let num_nonces = 2;
        let (signer, mut state_update, gas_price, initial_nonce) =
            setup_gas_key_account(TESTING_INIT_BALANCE, TESTING_GAS_KEY_BALANCE, num_nonces, None);

        // Use nonce <= current_nonce
        let signed_tx = SignedTransaction::from_actions_v1(
            TransactionNonce::from_nonce_and_index(initial_nonce, 0), // Same as current, not greater
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(100) })],
            CryptoHash::default(),
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            ProtocolFeature::GasKeys.protocol_version(),
        )
        .expect_err("should fail with invalid nonce");

        assert_eq!(
            err,
            InvalidTxError::InvalidNonce { tx_nonce: initial_nonce, ak_nonce: initial_nonce }
        );
    }

    #[test]
    fn test_gas_key_tx_not_enough_gas_key_balance() {
        let config = RuntimeConfig::test();
        let num_nonces = 2;
        // Gas key has zero balance, so any transaction should fail the gas key balance check.
        let (signer, mut state_update, gas_price, initial_nonce) =
            setup_gas_key_account(TESTING_INIT_BALANCE, Balance::ZERO, num_nonces, None);

        let signed_tx = SignedTransaction::from_actions_v1(
            TransactionNonce::from_nonce_and_index(initial_nonce + 1, 0),
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) })],
            CryptoHash::default(),
        );

        let tx_cost = tx_cost(&config, &signed_tx.transaction, gas_price).unwrap();
        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            ProtocolFeature::GasKeys.protocol_version(),
        )
        .expect_err("should fail with not enough gas key balance");

        match err {
            InvalidTxError::NotEnoughGasKeyBalance { signer_id, balance, cost } => {
                assert_eq!(signer_id, alice_account());
                assert_eq!(balance, Balance::ZERO);
                assert_eq!(cost, tx_cost.gas_cost);
            }
            _ => panic!("unexpected error: {:?}", err),
        }
    }

    #[test]
    fn test_gas_key_tx_not_enough_account_balance_for_deposit() {
        let config = RuntimeConfig::test();
        let num_nonces = 2;
        let small_account_balance = Balance::from_yoctonear(1);
        // Gas key has plenty for gas, but account has almost nothing for deposit.
        let (signer, mut state_update, gas_price, initial_nonce) =
            setup_gas_key_account(small_account_balance, TESTING_GAS_KEY_BALANCE, num_nonces, None);

        let signed_tx = SignedTransaction::from_actions_v1(
            TransactionNonce::from_nonce_and_index(initial_nonce + 1, 0),
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::Transfer(TransferAction { deposit: Balance::from_near(1000) })],
            CryptoHash::default(),
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            ProtocolFeature::GasKeys.protocol_version(),
        )
        .unwrap_err();
        assert!(
            matches!(err, InvalidTxError::NotEnoughBalanceForDeposit { .. }),
            "expected NotEnoughBalanceForDeposit, got {:?}",
            err,
        );
    }

    #[test]
    fn test_gas_key_tx_balance_split() {
        let config = RuntimeConfig::test();
        let num_nonces = 2;
        let (signer, mut state_update, gas_price, initial_nonce) =
            setup_gas_key_account(TESTING_INIT_BALANCE, TESTING_GAS_KEY_BALANCE, num_nonces, None);

        let deposit = Balance::from_near(5);
        let signed_tx = SignedTransaction::from_actions_v1(
            TransactionNonce::from_nonce_and_index(initial_nonce + 1, 0),
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::Transfer(TransferAction { deposit })],
            CryptoHash::default(),
        );

        let tx_cost = tx_cost(&config, &signed_tx.transaction, gas_price).unwrap();
        validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            ProtocolFeature::GasKeys.protocol_version(),
        )
        .unwrap();

        // Verify account balance pays for deposit, gas key balance pays for gas
        let account = get_account(&state_update, &alice_account()).unwrap().unwrap();
        assert_eq!(account.amount(), TESTING_INIT_BALANCE.checked_sub(deposit).unwrap());
        let access_key =
            get_access_key(&state_update, &alice_account(), &signer.public_key()).unwrap().unwrap();
        let remaining_gas_key_balance = access_key.gas_key_info().unwrap().balance;
        assert_eq!(
            remaining_gas_key_balance,
            TESTING_GAS_KEY_BALANCE.checked_sub(tx_cost.gas_cost).unwrap()
        );
    }

    #[test]
    fn test_access_key_tx_rejects_nonce_index() {
        // Set up a regular access key, then try to use nonce_index with it
        let config = RuntimeConfig::test();
        let (signer, mut state_update, gas_price) =
            setup_common(TESTING_INIT_BALANCE, Balance::ZERO, Some(AccessKey::full_access()));

        let signed_tx = SignedTransaction::from_actions_v1(
            TransactionNonce::from_nonce_and_index(1, 0), // Has nonce_index
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(100) })],
            CryptoHash::default(),
        );

        let err = validate_verify_and_charge_transaction(
            &config,
            &mut state_update,
            signed_tx,
            gas_price,
            None,
            ProtocolFeature::GasKeys.protocol_version(),
        )
        .expect_err("should fail when using nonce_index with regular access key");

        // The error comes from verify_and_charge_gas_key_tx_ephemeral because nonce_index is present,
        // but the access key is not a gas key
        assert_eq!(
            err,
            InvalidTxError::InvalidAccessKeyError(InvalidAccessKeyError::AccessKeyNotFound {
                account_id: alice_account(),
                public_key: Box::new(signer.public_key()),
            })
        );
    }

    #[test]
    fn test_gas_key_tx_deposit_failed_for_account_balance() {
        let config = RuntimeConfig::test();
        let num_nonces = 2;
        let small_account_balance = Balance::from_yoctonear(1);
        let (signer, state_update, gas_price, initial_nonce) =
            setup_gas_key_account(small_account_balance, TESTING_GAS_KEY_BALANCE, num_nonces, None);

        let signed_tx = SignedTransaction::from_actions_v1(
            TransactionNonce::from_nonce_and_index(initial_nonce + 1, 0),
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::Transfer(TransferAction { deposit: Balance::from_near(1000) })],
            CryptoHash::default(),
        );
        let validated_tx =
            validate_transaction(&config, signed_tx, ProtocolFeature::GasKeys.protocol_version())
                .unwrap();
        let tx = validated_tx.to_tx();
        let cost = tx_cost(&config, &tx, gas_price).unwrap();
        let (signer_account, access_key) =
            get_signer_and_access_key(&state_update, &validated_tx).unwrap();
        let current_nonce =
            get_gas_key_nonce(&state_update, tx.signer_id(), tx.public_key(), 0).unwrap().unwrap();

        let TxVerdict::DepositFailed { result, error } = verify_and_charge_gas_key_tx_ephemeral(
            &config,
            &signer_account,
            &access_key,
            current_nonce,
            tx,
            &cost,
            None,
            &PendingConstraints::default(),
        ) else {
            panic!("expected DepositFailed");
        };
        match error {
            InvalidTxError::NotEnoughBalanceForDeposit { signer_id, reason, .. } => {
                assert_eq!(signer_id, alice_account());
                assert_eq!(reason, DepositCostFailureReason::NotEnoughBalance);
            }
            other => panic!("expected NotEnoughBalanceForDeposit, got {:?}", other),
        }
        // Verify the result carries correct gas-charge data
        assert_eq!(result.gas_burnt, cost.gas_burnt);
        assert_eq!(result.burnt_amount, cost.burnt_amount);
        assert_eq!(result.new_account_amount, small_account_balance);
        assert_eq!(
            result.access_key_update,
            AccessKeyUpdate::GasKey {
                new_balance: TESTING_GAS_KEY_BALANCE.checked_sub(cost.burnt_amount).unwrap(),
                nonce_index: 0,
                nonce: initial_nonce + 1,
            }
        );
    }

    #[test]
    fn test_gas_key_tx_deposit_failed_for_storage_stake() {
        let config = RuntimeConfig::test();
        // Use many nonces to push storage_usage above ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT,
        // so that the account is not exempt from storage staking requirements.
        let num_nonces = 60;
        // Balance high enough to cover the deposit, but after subtracting deposit the
        // remainder is too low to meet storage staking requirements.
        let initial_balance = Balance::from_near(1);
        let deposit = Balance::from_millinear(999);
        let (signer, state_update, gas_price, initial_nonce) =
            setup_gas_key_account(initial_balance, TESTING_GAS_KEY_BALANCE, num_nonces, None);

        let signed_tx = SignedTransaction::from_actions_v1(
            TransactionNonce::from_nonce_and_index(initial_nonce + 1, 0),
            alice_account(),
            bob_account(),
            &*signer,
            vec![Action::Transfer(TransferAction { deposit })],
            CryptoHash::default(),
        );
        let validated_tx =
            validate_transaction(&config, signed_tx, ProtocolFeature::GasKeys.protocol_version())
                .unwrap();
        let tx = validated_tx.to_tx();
        let cost = tx_cost(&config, &tx, gas_price).unwrap();
        let (signer_account, access_key) =
            get_signer_and_access_key(&state_update, &validated_tx).unwrap();
        let current_nonce =
            get_gas_key_nonce(&state_update, tx.signer_id(), tx.public_key(), 0).unwrap().unwrap();

        let TxVerdict::DepositFailed { result, error } = verify_and_charge_gas_key_tx_ephemeral(
            &config,
            &signer_account,
            &access_key,
            current_nonce,
            tx,
            &cost,
            None,
            &PendingConstraints::default(),
        ) else {
            panic!("expected DepositFailed");
        };
        let new_account_amount = initial_balance.checked_sub(cost.deposit_cost).unwrap();
        match error {
            InvalidTxError::NotEnoughBalanceForDeposit { signer_id, balance, reason, .. } => {
                assert_eq!(signer_id, alice_account());
                assert_eq!(reason, DepositCostFailureReason::LackBalanceForState);
                assert_eq!(balance, new_account_amount);
            }
            other => panic!("expected NotEnoughBalanceForDeposit, got {:?}", other),
        }
        assert_eq!(result.gas_burnt, cost.gas_burnt);
        assert_eq!(result.burnt_amount, cost.burnt_amount);
        assert_eq!(result.new_account_amount, initial_balance);
    }

    mod strict_nonce_tests {
        use super::*;

        /// The protocol version where StrictNonce is enabled.
        const STRICT_NONCE_PROTOCOL_VERSION: ProtocolVersion =
            ProtocolFeature::StrictNonce.protocol_version();

        #[test]
        fn test_strict_nonce_exact_next_nonce_succeeds() {
            let config = RuntimeConfig::test();
            let initial_nonce = 5;
            let (signer, mut state_update, gas_price) = setup_common(
                TESTING_INIT_BALANCE,
                Balance::ZERO,
                Some(AccessKey {
                    nonce: initial_nonce,
                    permission: AccessKeyPermission::FullAccess,
                }),
            );

            let signed_tx = SignedTransaction::from_actions_v1_strict(
                TransactionNonce::from_nonce(initial_nonce + 1),
                alice_account(),
                bob_account(),
                &*signer,
                vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(100) })],
                CryptoHash::default(),
            );

            validate_verify_and_charge_transaction(
                &config,
                &mut state_update,
                signed_tx,
                gas_price,
                None,
                STRICT_NONCE_PROTOCOL_VERSION,
            )
            .expect("strict nonce with exact next nonce should succeed");
        }

        #[test]
        fn test_strict_nonce_gap_rejected() {
            let config = RuntimeConfig::test();
            let initial_nonce = 5;
            let (signer, mut state_update, gas_price) = setup_common(
                TESTING_INIT_BALANCE,
                Balance::ZERO,
                Some(AccessKey {
                    nonce: initial_nonce,
                    permission: AccessKeyPermission::FullAccess,
                }),
            );

            // Nonce 7 when ak_nonce is 5: gap (expected 6)
            let signed_tx = SignedTransaction::from_actions_v1_strict(
                TransactionNonce::from_nonce(initial_nonce + 2),
                alice_account(),
                bob_account(),
                &*signer,
                vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(100) })],
                CryptoHash::default(),
            );

            let err = validate_verify_and_charge_transaction(
                &config,
                &mut state_update,
                signed_tx,
                gas_price,
                None,
                STRICT_NONCE_PROTOCOL_VERSION,
            )
            .expect_err("strict nonce with gap should fail");
            assert_eq!(
                err,
                InvalidTxError::InvalidNonce {
                    tx_nonce: initial_nonce + 2,
                    ak_nonce: initial_nonce
                }
            );
        }

        #[test]
        fn test_strict_nonce_stale_rejected() {
            let config = RuntimeConfig::test();
            let initial_nonce = 5;
            let (signer, mut state_update, gas_price) = setup_common(
                TESTING_INIT_BALANCE,
                Balance::ZERO,
                Some(AccessKey {
                    nonce: initial_nonce,
                    permission: AccessKeyPermission::FullAccess,
                }),
            );

            // Nonce 3 when ak_nonce is 5: stale
            let signed_tx = SignedTransaction::from_actions_v1_strict(
                TransactionNonce::from_nonce(3),
                alice_account(),
                bob_account(),
                &*signer,
                vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(100) })],
                CryptoHash::default(),
            );

            let err = validate_verify_and_charge_transaction(
                &config,
                &mut state_update,
                signed_tx,
                gas_price,
                None,
                STRICT_NONCE_PROTOCOL_VERSION,
            )
            .expect_err("strict nonce with stale nonce should fail");
            assert_eq!(err, InvalidTxError::InvalidNonce { tx_nonce: 3, ak_nonce: initial_nonce });
        }

        #[test]
        fn test_monotonic_nonce_gap_succeeds() {
            let config = RuntimeConfig::test();
            let initial_nonce = 5;
            let (signer, mut state_update, gas_price) = setup_common(
                TESTING_INIT_BALANCE,
                Balance::ZERO,
                Some(AccessKey {
                    nonce: initial_nonce,
                    permission: AccessKeyPermission::FullAccess,
                }),
            );

            // Nonce 100 with monotonic mode: should succeed (gap is ok)
            let signed_tx = SignedTransaction::from_actions_v1(
                TransactionNonce::from_nonce(100),
                alice_account(),
                bob_account(),
                &*signer,
                vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(100) })],
                CryptoHash::default(),
            );

            validate_verify_and_charge_transaction(
                &config,
                &mut state_update,
                signed_tx,
                gas_price,
                None,
                STRICT_NONCE_PROTOCOL_VERSION,
            )
            .expect("monotonic nonce with gap should succeed");
        }

        #[test]
        fn test_strict_nonce_v1_rejected_before_feature() {
            let config = RuntimeConfig::test();
            let (signer, _state_update, _gas_price) =
                setup_common(TESTING_INIT_BALANCE, Balance::ZERO, Some(AccessKey::full_access()));

            let signed_tx = SignedTransaction::from_actions_v1_strict(
                TransactionNonce::from_nonce(1),
                alice_account(),
                bob_account(),
                &*signer,
                vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(100) })],
                CryptoHash::default(),
            );

            let protocol_version = STRICT_NONCE_PROTOCOL_VERSION - 1;
            let err = validate_transaction(&config, signed_tx, protocol_version)
                .expect_err("strict nonce V1 tx should be rejected before the feature");
            assert_eq!(err.0, InvalidTxError::InvalidTransactionVersion);
        }

        #[test]
        fn test_strict_nonce_equal_to_current_rejected() {
            let config = RuntimeConfig::test();
            let initial_nonce = 5;
            let (signer, mut state_update, gas_price) = setup_common(
                TESTING_INIT_BALANCE,
                Balance::ZERO,
                Some(AccessKey {
                    nonce: initial_nonce,
                    permission: AccessKeyPermission::FullAccess,
                }),
            );

            // Nonce exactly equal to ak_nonce: should fail (need ak_nonce + 1)
            let signed_tx = SignedTransaction::from_actions_v1_strict(
                TransactionNonce::from_nonce(initial_nonce),
                alice_account(),
                bob_account(),
                &*signer,
                vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(100) })],
                CryptoHash::default(),
            );

            let err = validate_verify_and_charge_transaction(
                &config,
                &mut state_update,
                signed_tx,
                gas_price,
                None,
                STRICT_NONCE_PROTOCOL_VERSION,
            )
            .expect_err("strict nonce equal to current should fail");
            assert_eq!(
                err,
                InvalidTxError::InvalidNonce { tx_nonce: initial_nonce, ak_nonce: initial_nonce }
            );
        }

        #[test]
        fn test_strict_nonce_u64_max_rejected() {
            let config = RuntimeConfig::test();
            let (signer, mut state_update, gas_price) = setup_common(
                TESTING_INIT_BALANCE,
                Balance::ZERO,
                Some(AccessKey { nonce: u64::MAX, permission: AccessKeyPermission::FullAccess }),
            );

            // With ak_nonce at u64::MAX, no valid strict nonce exists.
            let signed_tx = SignedTransaction::from_actions_v1_strict(
                TransactionNonce::from_nonce(u64::MAX),
                alice_account(),
                bob_account(),
                &*signer,
                vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(100) })],
                CryptoHash::default(),
            );

            let err = validate_verify_and_charge_transaction(
                &config,
                &mut state_update,
                signed_tx,
                gas_price,
                None,
                STRICT_NONCE_PROTOCOL_VERSION,
            )
            .expect_err("strict nonce at u64::MAX should fail");
            assert_eq!(
                err,
                InvalidTxError::InvalidNonce { tx_nonce: u64::MAX, ak_nonce: u64::MAX }
            );
        }
    }
}

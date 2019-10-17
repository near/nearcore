#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

use std::collections::{HashMap, HashSet};
use std::convert::TryInto;

use borsh::BorshSerialize;
use kvdb::DBValue;

use near_crypto::PublicKey;
use near_primitives::account::{AccessKey, AccessKeyPermission, Account};
use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{ActionReceipt, DataReceipt, Receipt, ReceiptEnum, ReceivedData};
use near_primitives::serialize::from_base64;
use near_primitives::transaction::{
    Action, ExecutionOutcome, ExecutionOutcomeWithId, ExecutionStatus, LogEntry, SignedTransaction,
};
use near_primitives::types::{
    AccountId, Balance, BlockIndex, Gas, Nonce, StateRoot, ValidatorStake,
};
use near_primitives::utils::{
    create_nonce_with_nonce, is_valid_account_id, key_for_pending_data_count,
    key_for_postponed_receipt, key_for_postponed_receipt_id, key_for_received_data, system_account,
    ACCOUNT_DATA_SEPARATOR,
};
use near_runtime_fees::RuntimeFeesConfig;
use near_store::{
    get, get_access_key, get_account, get_receipt, get_received_data, set, set_access_key,
    set_account, set_code, set_receipt, set_received_data, StorageError, StoreUpdate, TrieChanges,
    TrieUpdate,
};
use near_vm_logic::types::PromiseResult;
use near_vm_logic::ReturnData;

use crate::actions::*;
use crate::config::{
    exec_fee, safe_add_balance, safe_add_gas, safe_gas_to_balance, total_deposit, total_exec_fees,
    total_prepaid_gas, total_send_fees, RuntimeConfig,
};
pub use crate::store::StateRecord;
use near_primitives::errors::{
    ActionError, ExecutionError, InvalidAccessKeyError, InvalidTxError,
    InvalidTxErrorOrStorageError,
};

mod actions;
pub mod adapter;
pub mod cache;
pub mod config;
pub mod ext;
pub mod state_viewer;
mod store;

const OVERFLOW_CHECKED_ERR: &str = "Overflow has already been checked.";

#[derive(Debug)]
pub struct ApplyState {
    /// Currently building block index.
    pub block_index: BlockIndex,
    /// Current epoch length.
    pub epoch_length: BlockIndex,
    /// Price for the gas.
    pub gas_price: Balance,
    /// A block timestamp
    pub block_timestamp: u64,
}

#[derive(Debug)]
pub struct VerificationResult {
    pub gas_burnt: Gas,
    pub gas_used: Gas,
    pub rent_paid: Balance,
    pub validator_reward: Balance,
}

#[derive(Debug, Default)]
pub struct ApplyStats {
    pub total_rent_paid: Balance,
    pub total_validator_reward: Balance,
    pub total_balance_burnt: Balance,
}

pub struct ApplyResult {
    pub state_root: StateRoot,
    pub trie_changes: TrieChanges,
    pub validator_proposals: Vec<ValidatorStake>,
    pub new_receipts: Vec<Receipt>,
    pub tx_result: Vec<ExecutionOutcomeWithId>,
    pub stats: ApplyStats,
}

#[derive(Debug)]
pub struct ActionResult {
    pub gas_burnt: Gas,
    pub gas_used: Gas,
    pub result: Result<ReturnData, ActionError>,
    pub logs: Vec<LogEntry>,
    pub new_receipts: Vec<Receipt>,
    pub validator_proposals: Vec<ValidatorStake>,
}

impl ActionResult {
    pub fn merge(&mut self, mut next_result: ActionResult) {
        self.gas_burnt += next_result.gas_burnt;
        self.gas_used += next_result.gas_used;
        self.result = next_result.result;
        self.logs.append(&mut next_result.logs);
        if let Ok(ReturnData::ReceiptIndex(ref mut receipt_index)) = self.result {
            // Shifting local receipt index to be global receipt index.
            *receipt_index += self.new_receipts.len() as u64;
        }
        if self.result.is_ok() {
            self.new_receipts.append(&mut next_result.new_receipts);
            self.validator_proposals.append(&mut next_result.validator_proposals);
        } else {
            self.new_receipts.clear();
            self.validator_proposals.clear();
        }
    }
}

impl Default for ActionResult {
    fn default() -> Self {
        Self {
            gas_burnt: 0,
            gas_used: 0,
            result: Ok(ReturnData::None),
            logs: vec![],
            new_receipts: vec![],
            validator_proposals: vec![],
        }
    }
}

pub struct Runtime {
    config: RuntimeConfig,
}

impl Runtime {
    pub fn new(config: RuntimeConfig) -> Self {
        Runtime { config }
    }

    fn print_log(log: &[LogEntry]) {
        if log.is_empty() {
            return;
        }
        let log_str = log.iter().fold(String::new(), |acc, s| {
            if acc.is_empty() {
                s.to_string()
            } else {
                acc + "\n" + s
            }
        });
        debug!(target: "runtime", "{}", log_str);
    }

    /// Verifies the signed transaction on top of given state, charges the rent and transaction fees
    /// and balances, and updates the state for the used account and access keys.
    pub fn verify_and_charge_transaction(
        &self,
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
        signed_transaction: &SignedTransaction,
    ) -> Result<VerificationResult, InvalidTxErrorOrStorageError> {
        let transaction = &signed_transaction.transaction;
        let signer_id = &transaction.signer_id;
        if !is_valid_account_id(&signer_id) {
            return Err(InvalidTxError::InvalidSigner(signer_id.clone()).into());
        }
        if !is_valid_account_id(&transaction.receiver_id) {
            return Err(InvalidTxError::InvalidReceiver(transaction.receiver_id.clone()).into());
        }

        if !signed_transaction
            .signature
            .verify(signed_transaction.get_hash().as_ref(), &transaction.public_key)
        {
            return Err(InvalidTxError::InvalidSignature.into());
        }
        let mut signer = match get_account(state_update, signer_id)? {
            Some(signer) => signer,
            None => {
                return Err(InvalidTxError::SignerDoesNotExist(signer_id.clone()).into());
            }
        };
        let mut access_key =
            match get_access_key(state_update, &signer_id, &transaction.public_key)? {
                Some(access_key) => access_key,
                None => {
                    return Err(InvalidAccessKeyError::AccessKeyNotFound(
                        signer_id.clone(),
                        transaction.public_key.clone(),
                    )
                    .into());
                }
            };

        if transaction.nonce <= access_key.nonce {
            return Err(InvalidTxError::InvalidNonce(transaction.nonce, access_key.nonce).into());
        }

        let sender_is_receiver = &transaction.receiver_id == signer_id;

        let rent_paid = apply_rent(&signer_id, &mut signer, apply_state.block_index, &self.config);
        access_key.nonce = transaction.nonce;
        let mut gas_burnt: Gas = self
            .config
            .transaction_costs
            .action_receipt_creation_config
            .send_fee(sender_is_receiver);
        gas_burnt = safe_add_gas(
            gas_burnt,
            total_send_fees(
                &self.config.transaction_costs,
                sender_is_receiver,
                &transaction.actions,
            )?,
        )?;
        let mut gas_used = safe_add_gas(
            gas_burnt,
            self.config.transaction_costs.action_receipt_creation_config.exec_fee(),
        )?;
        gas_used = safe_add_gas(
            gas_used,
            total_exec_fees(&self.config.transaction_costs, &transaction.actions)?,
        )?;
        gas_used = safe_add_gas(gas_used, total_prepaid_gas(&transaction.actions)?)?;
        let mut total_cost = safe_gas_to_balance(apply_state.gas_price, gas_used)?;
        total_cost = safe_add_balance(total_cost, total_deposit(&transaction.actions)?)?;
        signer.amount = signer.amount.checked_sub(total_cost).ok_or_else(|| {
            InvalidTxError::NotEnoughBalance(signer_id.clone(), signer.amount, total_cost)
        })?;

        if let AccessKeyPermission::FunctionCall(ref mut function_call_permission) =
            access_key.permission
        {
            if let Some(ref mut allowance) = function_call_permission.allowance {
                *allowance = allowance.checked_sub(total_cost).ok_or_else(|| {
                    InvalidAccessKeyError::NotEnoughAllowance(
                        signer_id.clone(),
                        transaction.public_key.clone(),
                        *allowance,
                        total_cost,
                    )
                })?;
            }
        }

        if let Err(amount) = check_rent(&signer_id, &signer, &self.config, apply_state.epoch_length)
        {
            return Err(InvalidTxError::RentUnpaid(signer_id.clone(), amount).into());
        }

        if let AccessKeyPermission::FunctionCall(ref function_call_permission) =
            access_key.permission
        {
            if transaction.actions.len() != 1 {
                return Err(InvalidAccessKeyError::ActionError.into());
            }
            if let Some(Action::FunctionCall(ref function_call)) = transaction.actions.get(0) {
                if transaction.receiver_id != function_call_permission.receiver_id {
                    return Err(InvalidAccessKeyError::ReceiverMismatch(
                        transaction.receiver_id.clone(),
                        function_call_permission.receiver_id.clone(),
                    )
                    .into());
                }
                if !function_call_permission.method_names.is_empty()
                    && function_call_permission
                        .method_names
                        .iter()
                        .all(|method_name| &function_call.method_name != method_name)
                {
                    return Err(InvalidAccessKeyError::MethodNameMismatch(
                        function_call.method_name.clone(),
                    )
                    .into());
                }
            } else {
                return Err(InvalidAccessKeyError::ActionError.into());
            }
        };

        set_access_key(state_update, &signer_id, &transaction.public_key, &access_key);

        // Account reward for gas burnt.
        let burnt_gas_reward = Balance::from(
            gas_burnt * self.config.transaction_costs.burnt_gas_reward.numerator
                / self.config.transaction_costs.burnt_gas_reward.denominator,
        ) * apply_state.gas_price;
        signer.amount += burnt_gas_reward;

        set_account(state_update, &signer_id, &signer);

        let validator_reward = Balance::from(gas_burnt) * apply_state.gas_price - burnt_gas_reward;

        Ok(VerificationResult { gas_burnt, gas_used, rent_paid, validator_reward })
    }

    /// Takes one signed transaction, verifies it and converts it to a receipt. Add this receipt
    /// either to the new local receipts if the signer is the same as receiver or to the new
    /// outgoing receipts.
    /// When transaction is converted to a receipt, the account is charged for the full value of
    /// the generated receipt. Also accounts for the account rent.
    /// In case of successful verification (expected for valid chunks), returns
    /// `ExecutionOutcomeWithId` for the transaction.
    /// In case of an error, returns either `InvalidTxError` if the transaction verification failed
    /// or a `StorageError`.
    fn process_transaction(
        &self,
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
        signed_transaction: &SignedTransaction,
        new_local_receipts: &mut Vec<Receipt>,
        new_receipts: &mut Vec<Receipt>,
        stats: &mut ApplyStats,
    ) -> Result<ExecutionOutcomeWithId, InvalidTxErrorOrStorageError> {
        let outcome =
            match self.verify_and_charge_transaction(state_update, apply_state, signed_transaction)
            {
                Ok(verification_result) => {
                    state_update.commit();
                    let transaction = &signed_transaction.transaction;
                    let receipt = Receipt {
                        predecessor_id: transaction.signer_id.clone(),
                        receiver_id: transaction.receiver_id.clone(),
                        receipt_id: create_nonce_with_nonce(&signed_transaction.get_hash(), 0),

                        receipt: ReceiptEnum::Action(ActionReceipt {
                            signer_id: transaction.signer_id.clone(),
                            signer_public_key: transaction.public_key.clone(),
                            gas_price: apply_state.gas_price,
                            output_data_receivers: vec![],
                            input_data_ids: vec![],
                            actions: transaction.actions.clone(),
                        }),
                    };
                    let receipt_id = receipt.receipt_id;
                    if receipt.receiver_id == signed_transaction.transaction.signer_id {
                        new_local_receipts.push(receipt);
                    } else {
                        new_receipts.push(receipt);
                    }
                    stats.total_rent_paid += verification_result.rent_paid;
                    stats.total_validator_reward += verification_result.validator_reward;
                    ExecutionOutcome {
                        status: ExecutionStatus::SuccessReceiptId(receipt_id),
                        logs: vec![],
                        receipt_ids: vec![receipt_id],
                        gas_burnt: verification_result.gas_burnt,
                    }
                }
                Err(e) => {
                    state_update.rollback();
                    return Err(e);
                }
            };
        Self::print_log(&outcome.logs);
        Ok(ExecutionOutcomeWithId { id: signed_transaction.get_hash(), outcome })
    }

    fn apply_action(
        &self,
        action: &Action,
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
        account: &mut Option<Account>,
        actor_id: &mut AccountId,
        receipt: &Receipt,
        action_receipt: &ActionReceipt,
        promise_results: &[PromiseResult],
        action_hash: CryptoHash,
        is_last_action: bool,
    ) -> Result<ActionResult, StorageError> {
        let mut result = ActionResult::default();
        let exec_fees = exec_fee(&self.config.transaction_costs, action);
        result.gas_burnt += exec_fees;
        result.gas_used += exec_fees;
        let account_id = &receipt.receiver_id;
        // Account validation
        if let Err(e) = check_account_existence(action, account, account_id) {
            result.result = Err(e);
            return Ok(result);
        }
        // Permission validation
        if let Err(e) = check_actor_permissions(
            action,
            apply_state,
            account,
            &actor_id,
            account_id,
            &self.config,
        ) {
            result.result = Err(e);
            return Ok(result);
        }
        match action {
            Action::CreateAccount(_) => {
                action_create_account(apply_state, account, actor_id, receipt, &mut result);
            }
            Action::DeployContract(deploy_contract) => {
                action_deploy_contract(state_update, account, &account_id, deploy_contract)?;
            }
            Action::FunctionCall(function_call) => {
                action_function_call(
                    state_update,
                    apply_state,
                    account,
                    receipt,
                    action_receipt,
                    promise_results,
                    &mut result,
                    account_id,
                    function_call,
                    &action_hash,
                    &self.config,
                    is_last_action,
                )?;
            }
            Action::Transfer(transfer) => {
                action_transfer(account, transfer);
            }
            Action::Stake(stake) => {
                action_stake(account, &mut result, account_id, stake);
            }
            Action::AddKey(add_key) => {
                action_add_key(state_update, account, &mut result, account_id, add_key)?;
            }
            Action::DeleteKey(delete_key) => {
                action_delete_key(state_update, account, &mut result, account_id, delete_key)?;
            }
            Action::DeleteAccount(delete_account) => {
                action_delete_account(
                    state_update,
                    account,
                    actor_id,
                    receipt,
                    &mut result,
                    account_id,
                    delete_account,
                )?;
            }
        };
        Ok(result)
    }

    fn apply_action_receipt(
        &self,
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
        receipt: &Receipt,
        new_receipts: &mut Vec<Receipt>,
        validator_proposals: &mut Vec<ValidatorStake>,
        stats: &mut ApplyStats,
    ) -> Result<ExecutionOutcomeWithId, StorageError> {
        let action_receipt = match receipt.receipt {
            ReceiptEnum::Action(ref action_receipt) => action_receipt,
            _ => unreachable!("given receipt should be an action receipt"),
        };
        let account_id = &receipt.receiver_id;
        // Collecting input data and removing it from the state
        let promise_results = action_receipt
            .input_data_ids
            .iter()
            .map(|data_id| {
                let ReceivedData { data } = get_received_data(state_update, account_id, data_id)?
                    .ok_or_else(|| {
                    StorageError::StorageInconsistentState(
                        "received data should be in the state".to_string(),
                    )
                })?;
                state_update.remove(&key_for_received_data(account_id, data_id));
                match data {
                    Some(value) => Ok(PromiseResult::Successful(value)),
                    None => Ok(PromiseResult::Failed),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        // state_update might already have some updates so we need to make sure we commit it before
        // executing the actual receipt
        state_update.commit();

        let mut account = get_account(state_update, account_id)?;
        let mut rent_paid = 0;
        if let Some(ref mut account) = account {
            rent_paid = apply_rent(account_id, account, apply_state.block_index, &self.config);
        }
        let mut actor_id = receipt.predecessor_id.clone();
        let mut result = ActionResult::default();
        let exec_fee = self.config.transaction_costs.action_receipt_creation_config.exec_fee();
        result.gas_used = exec_fee;
        result.gas_burnt = exec_fee;
        // Executing actions one by one
        for (action_index, action) in action_receipt.actions.iter().enumerate() {
            let is_last_action = action_index + 1 == action_receipt.actions.len();
            result.merge(self.apply_action(
                action,
                state_update,
                apply_state,
                &mut account,
                &mut actor_id,
                receipt,
                action_receipt,
                &promise_results,
                create_nonce_with_nonce(
                    &receipt.receipt_id,
                    u64::max_value() - action_index as u64,
                ),
                is_last_action,
            )?);
            // TODO storage error
            if result.result.is_err() {
                break;
            }
        }

        // Going to check rent
        if result.result.is_ok() {
            if let Some(ref mut account) = account {
                if let Err(amount) =
                    check_rent(account_id, account, &self.config, apply_state.epoch_length)
                {
                    result.merge(ActionResult {
                        result: Err(ActionError::RentUnpaid(account_id.clone(), amount)),
                        ..Default::default()
                    });
                } else {
                    set_account(state_update, account_id, account);
                }
            }
        }

        // If the receipt is a refund, then we consider it free without burnt gas.
        if receipt.predecessor_id == system_account() {
            result.gas_burnt = 0;
            result.gas_used = 0;
            // If the refund fails, instead of just burning tokens, we report the total number of
            // tokens burnt in the ApplyResult. It can be used by validators to distribute it.
            if result.result.is_err() {
                stats.total_balance_burnt +=
                    total_deposit(&action_receipt.actions).expect(OVERFLOW_CHECKED_ERR);
            }
        } else {
            // Calculating and generating refunds
            self.generate_refund_receipts(receipt, action_receipt, &mut result);
        }

        // Moving validator proposals
        validator_proposals.append(&mut result.validator_proposals);

        // Committing or rolling back state.
        match &result.result {
            Ok(_) => {
                stats.total_rent_paid += rent_paid;
                state_update.commit();
            }
            Err(_) => {
                state_update.rollback();
            }
        };

        // Adding burnt gas reward if the account exists.
        let gas_reward = result.gas_burnt
            * self.config.transaction_costs.burnt_gas_reward.numerator
            / self.config.transaction_costs.burnt_gas_reward.denominator;
        let mut validator_reward = Balance::from(result.gas_burnt) * action_receipt.gas_price;
        if gas_reward > 0 {
            let mut account = get_account(state_update, account_id)?;
            if let Some(ref mut account) = account {
                let reward = Balance::from(gas_reward) * action_receipt.gas_price;
                // Validators receive the remaining execution reward that was not given to the
                // account holder. If the account doesn't exist by the end of the execution, the
                // validators receive the full reward.
                validator_reward -= reward;
                account.amount += reward;
                set_account(state_update, account_id, account);
                state_update.commit();
            }
        }
        stats.total_validator_reward += validator_reward;

        // Generating outgoing data
        if !action_receipt.output_data_receivers.is_empty() {
            if let Ok(ReturnData::ReceiptIndex(receipt_index)) = result.result {
                // Modifying a new receipt instead of sending data
                match result
                    .new_receipts
                    .get_mut(receipt_index as usize)
                    .expect("the receipt for the given receipt index should exist")
                    .receipt
                {
                    ReceiptEnum::Action(ref mut new_action_receipt) => new_action_receipt
                        .output_data_receivers
                        .extend_from_slice(&action_receipt.output_data_receivers),
                    _ => unreachable!("the receipt should be an action receipt"),
                }
            } else {
                let data = match result.result {
                    Ok(ReturnData::Value(ref data)) => Some(data.clone()),
                    Ok(_) => Some(vec![]),
                    Err(_) => None,
                };
                result.new_receipts.extend(action_receipt.output_data_receivers.iter().map(
                    |data_receiver| Receipt {
                        predecessor_id: account_id.clone(),
                        receiver_id: data_receiver.receiver_id.clone(),
                        receipt_id: CryptoHash::default(),
                        receipt: ReceiptEnum::Data(DataReceipt {
                            data_id: data_receiver.data_id,
                            data: data.clone(),
                        }),
                    },
                ));
            };
        }

        // Generating receipt IDs
        let receipt_ids = result
            .new_receipts
            .into_iter()
            .enumerate()
            .filter_map(|(receipt_index, mut new_receipt)| {
                let receipt_id =
                    create_nonce_with_nonce(&receipt.receipt_id, receipt_index as Nonce);
                new_receipt.receipt_id = receipt_id;
                let is_action = match &new_receipt.receipt {
                    ReceiptEnum::Action(_) => true,
                    _ => false,
                };
                new_receipts.push(new_receipt);
                if is_action {
                    Some(receipt_id)
                } else {
                    None
                }
            })
            .collect();

        let status = match result.result {
            Ok(ReturnData::ReceiptIndex(receipt_index)) => ExecutionStatus::SuccessReceiptId(
                create_nonce_with_nonce(&receipt.receipt_id, receipt_index as Nonce),
            ),
            Ok(ReturnData::Value(data)) => ExecutionStatus::SuccessValue(data),
            Ok(ReturnData::None) => ExecutionStatus::SuccessValue(vec![]),
            Err(e) => ExecutionStatus::Failure(ExecutionError::Action(e)),
        };

        Self::print_log(&result.logs);

        Ok(ExecutionOutcomeWithId {
            id: receipt.receipt_id,
            outcome: ExecutionOutcome {
                status,
                logs: result.logs,
                receipt_ids,
                gas_burnt: result.gas_burnt,
            },
        })
    }

    fn generate_refund_receipts(
        &self,
        receipt: &Receipt,
        action_receipt: &ActionReceipt,
        result: &mut ActionResult,
    ) {
        let total_deposit = total_deposit(&action_receipt.actions).expect(OVERFLOW_CHECKED_ERR);
        let prepaid_gas = total_prepaid_gas(&action_receipt.actions).expect(OVERFLOW_CHECKED_ERR);
        let exec_gas = total_exec_fees(&self.config.transaction_costs, &action_receipt.actions)
            .expect(OVERFLOW_CHECKED_ERR)
            + self.config.transaction_costs.action_receipt_creation_config.exec_fee();
        let mut deposit_refund = if result.result.is_err() { total_deposit } else { 0 };
        let gas_refund = if result.result.is_err() {
            prepaid_gas + exec_gas - result.gas_burnt
        } else {
            prepaid_gas + exec_gas - result.gas_used
        };
        let mut gas_balance_refund = Balance::from(gas_refund) * action_receipt.gas_price;
        if action_receipt.signer_id == receipt.predecessor_id {
            // Merging 2 refunds
            deposit_refund += gas_balance_refund;
            gas_balance_refund = 0;
        }
        if deposit_refund > 0 {
            result.new_receipts.push(Receipt::new_refund(&receipt.predecessor_id, deposit_refund));
        }
        if gas_balance_refund > 0 {
            result
                .new_receipts
                .push(Receipt::new_refund(&action_receipt.signer_id, gas_balance_refund));
        }
    }

    fn process_receipt(
        &self,
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
        receipt: &Receipt,
        new_receipts: &mut Vec<Receipt>,
        validator_proposals: &mut Vec<ValidatorStake>,
        stats: &mut ApplyStats,
    ) -> Result<Option<ExecutionOutcomeWithId>, StorageError> {
        let account_id = &receipt.receiver_id;
        match receipt.receipt {
            ReceiptEnum::Data(ref data_receipt) => {
                // Received a new data receipt.
                // Saving the data into the state keyed by the data_id.
                set_received_data(
                    state_update,
                    account_id,
                    &data_receipt.data_id,
                    &ReceivedData { data: data_receipt.data.clone() },
                );
                // Check if there is already a receipt that was postponed and was awaiting for the
                // given data_id.
                // If we don't have a postponed receipt yet, we don't need to do anything for now.
                if let Some(receipt_id) = get(
                    state_update,
                    &key_for_postponed_receipt_id(account_id, &data_receipt.data_id),
                )? {
                    // There is already a receipt that is awaiting for the just received data.
                    // Removing this pending data_id for the receipt from the state.
                    state_update
                        .remove(&key_for_postponed_receipt_id(account_id, &data_receipt.data_id));
                    // Checking how many input data items is pending for the receipt.
                    let pending_data_count: u32 =
                        get(state_update, &key_for_pending_data_count(account_id, &receipt_id))?
                            .ok_or_else(|| {
                                StorageError::StorageInconsistentState(
                                    "pending data count should be in the state".to_string(),
                                )
                            })?;
                    if pending_data_count == 1 {
                        // It was the last input data pending for this receipt. We'll cleanup
                        // some receipt related fields from the state and execute the receipt.

                        // Removing pending data count form the state.
                        state_update.remove(&key_for_pending_data_count(account_id, &receipt_id));
                        // Fetching the receipt itself.
                        let ready_receipt = get_receipt(state_update, account_id, &receipt_id)?
                            .ok_or_else(|| {
                                StorageError::StorageInconsistentState(
                                    "pending receipt should be in the state".to_string(),
                                )
                            })?;
                        // Removing the receipt from the state.
                        state_update.remove(&key_for_postponed_receipt(account_id, &receipt_id));
                        // Executing the receipt. It will read all the input data and clean it up
                        // from the state.
                        return self
                            .apply_action_receipt(
                                state_update,
                                apply_state,
                                &ready_receipt,
                                new_receipts,
                                validator_proposals,
                                stats,
                            )
                            .map(Some);
                    } else {
                        // There is still some pending data for the receipt, so we update the
                        // pending data count in the state.
                        set(
                            state_update,
                            key_for_pending_data_count(account_id, &receipt_id),
                            &(pending_data_count - 1),
                        );
                    }
                }
            }
            ReceiptEnum::Action(ref action_receipt) => {
                // Received a new action receipt. We'll first check how many input data items
                // were already received before and saved in the state.
                // And if we have all input data, then we can immediately execute the receipt.
                // If not, then we will postpone this receipt for later.
                let mut pending_data_count = 0;
                for data_id in &action_receipt.input_data_ids {
                    if get_received_data(state_update, account_id, data_id)?.is_none() {
                        pending_data_count += 1;
                        // The data for a given data_id is not available, so we save a link to this
                        // receipt_id for the pending data_id into the state.
                        set(
                            state_update,
                            key_for_postponed_receipt_id(account_id, data_id),
                            &receipt.receipt_id,
                        )
                    }
                }
                if pending_data_count == 0 {
                    // All input data is available. Executing the receipt. It will cleanup
                    // input data from the state.
                    return self
                        .apply_action_receipt(
                            state_update,
                            apply_state,
                            receipt,
                            new_receipts,
                            validator_proposals,
                            stats,
                        )
                        .map(Some);
                } else {
                    // Not all input data is available now.
                    // Save the counter for the number of pending input data items into the state.
                    set(
                        state_update,
                        key_for_pending_data_count(account_id, &receipt.receipt_id),
                        &pending_data_count,
                    );
                    // Save the receipt itself into the state.
                    set_receipt(state_update, &receipt);
                }
            }
        };
        // We didn't trigger execution, so we need to commit the state.
        state_update.commit();
        Ok(None)
    }

    /// Applies new singed transactions and incoming receipts for some chunk/shard on top of
    /// given root of the state.
    /// All new signed transactions should be valid and already verified by the chunk producer.
    /// If any transaction is invalid, it would return an `InvalidTxError`.
    /// Returns an `ApplyResult` that contains the new state root, trie changes,
    /// new outgoing receipts, total rent paid by all the affected accounts, execution outcomes for
    /// all transactions, local action receipts (generated from transactions with signer ==
    /// receivers) and incoming action receipts.
    pub fn apply(
        &self,
        mut state_update: TrieUpdate,
        apply_state: &ApplyState,
        prev_receipts: &[Receipt],
        transactions: &[SignedTransaction],
    ) -> Result<ApplyResult, InvalidTxErrorOrStorageError> {
        // TODO(#1481): Remove clone after Runtime bug is fixed.
        let initial_state = state_update.clone();

        let mut new_receipts = Vec::new();
        let mut validator_proposals = vec![];
        let mut local_receipts = vec![];
        let mut tx_result = vec![];
        let mut stats = ApplyStats::default();

        for signed_transaction in transactions {
            tx_result.push(self.process_transaction(
                &mut state_update,
                apply_state,
                signed_transaction,
                &mut local_receipts,
                &mut new_receipts,
                &mut stats,
            )?);
        }

        for receipt in local_receipts.iter().chain(prev_receipts.iter()) {
            self.process_receipt(
                &mut state_update,
                apply_state,
                receipt,
                &mut new_receipts,
                &mut validator_proposals,
                &mut stats,
            )?
            .into_iter()
            .for_each(|res| tx_result.push(res));
        }

        self.check_balance(
            &initial_state,
            &state_update,
            prev_receipts,
            transactions,
            &new_receipts,
            &stats,
        )?;

        let trie_changes = state_update.finalize()?;
        Ok(ApplyResult {
            state_root: StateRoot { hash: trie_changes.new_root, num_parts: 9 }, /* TODO MOO */
            trie_changes,
            validator_proposals,
            new_receipts,
            tx_result,
            stats,
        })
    }

    // TODO: Check for balance overflows
    // TODO: Fix StorageError for partial states when looking up something that doesn't exist.
    fn check_balance(
        &self,
        initial_state: &TrieUpdate,
        final_state: &TrieUpdate,
        prev_receipts: &[Receipt],
        transactions: &[SignedTransaction],
        new_receipts: &[Receipt],
        stats: &ApplyStats,
    ) -> Result<(), InvalidTxErrorOrStorageError> {
        // Accounts
        let all_accounts_ids: HashSet<AccountId> = transactions
            .iter()
            .map(|tx| tx.transaction.signer_id.clone())
            .chain(prev_receipts.iter().map(|r| r.receiver_id.clone()))
            .collect();
        let total_accounts_balance = |state| -> Result<u128, StorageError> {
            Ok(all_accounts_ids
                .iter()
                .map(|account_id| {
                    Ok(get_account(state, account_id)?.map_or(0, |a| a.amount + a.locked))
                })
                .collect::<Result<Vec<u128>, StorageError>>()?
                .into_iter()
                .sum::<u128>())
        };
        let initial_account_balance = total_accounts_balance(&initial_state)?;
        let final_account_balance = total_accounts_balance(&final_state)?;
        // Receipts
        let receipt_cost = |receipt: &Receipt| -> Result<u128, InvalidTxError> {
            Ok(match &receipt.receipt {
                ReceiptEnum::Action(action_receipt) => {
                    let mut total_cost = total_deposit(&action_receipt.actions)?;
                    if receipt.predecessor_id != system_account() {
                        let mut total_gas = safe_add_gas(
                            self.config.transaction_costs.action_receipt_creation_config.exec_fee(),
                            total_exec_fees(
                                &self.config.transaction_costs,
                                &action_receipt.actions,
                            )?,
                        )?;
                        total_gas =
                            safe_add_gas(total_gas, total_prepaid_gas(&action_receipt.actions)?)?;
                        let total_gas_cost =
                            safe_gas_to_balance(action_receipt.gas_price, total_gas)?;
                        total_cost = safe_add_balance(total_cost, total_gas_cost)?;
                    }
                    total_cost
                }
                ReceiptEnum::Data(_) => 0,
            })
        };
        let receipts_cost = |receipts: &[Receipt]| {
            receipts
                .iter()
                .map(receipt_cost)
                .collect::<Result<Vec<u128>, InvalidTxError>>()
                .expect(OVERFLOW_CHECKED_ERR)
                .into_iter()
                .sum::<u128>()
        };
        let incoming_receipts_balance = receipts_cost(prev_receipts);
        let outgoing_receipts_balance = receipts_cost(new_receipts);
        // Postponed actions receipts. The receipts can be postponed and stored with the receiver's
        // account ID when the input data is not received yet.
        // We calculate all potential receipts IDs that might be postponed initially or after the
        // execution.
        let all_potential_postponed_receipt_ids: HashSet<(AccountId, CryptoHash)> = prev_receipts
            .iter()
            .map(|receipt| {
                let account_id = &receipt.receiver_id;
                match &receipt.receipt {
                    ReceiptEnum::Action(_) => {
                        Ok(Some((account_id.clone(), receipt.receipt_id.clone())))
                    }
                    ReceiptEnum::Data(data_receipt) => {
                        if let Some(receipt_id) = get(
                            initial_state,
                            &key_for_postponed_receipt_id(account_id, &data_receipt.data_id),
                        )? {
                            Ok(Some((account_id.clone(), receipt_id)))
                        } else {
                            Ok(None)
                        }
                    }
                }
            })
            .collect::<Result<Vec<Option<_>>, StorageError>>()?
            .into_iter()
            .filter_map(|x| x)
            .collect();

        let total_postponed_receipts_cost = |state| -> Result<u128, InvalidTxErrorOrStorageError> {
            Ok(all_potential_postponed_receipt_ids
                .iter()
                .map(|(account_id, receipt_id)| {
                    Ok(get_receipt(state, account_id, &receipt_id)?
                        .map_or(Ok(0), |r| receipt_cost(&r))?)
                })
                .collect::<Result<Vec<u128>, InvalidTxErrorOrStorageError>>()?
                .into_iter()
                .sum::<u128>())
        };
        let initial_postponed_receipts_balance = total_postponed_receipts_cost(initial_state)?;
        let final_postponed_receipts_balance = total_postponed_receipts_cost(final_state)?;
        // Sum it up
        let initial_balance = initial_account_balance
            + incoming_receipts_balance
            + initial_postponed_receipts_balance;
        let final_balance = final_account_balance
            + outgoing_receipts_balance
            + final_postponed_receipts_balance
            + stats.total_rent_paid
            + stats.total_validator_reward
            + stats.total_balance_burnt;
        if initial_balance != final_balance {
            panic!(
                "Runtime Apply initial balance sum {} doesn't match final balance sum {}\n\
                 \tInitial account balance sum: {}\n\
                 \tFinal account balance sum: {}\n\
                 \tIncoming receipts balance sum: {}\n\
                 \tOutgoing receipts balance sum: {}\n\
                 \tInitial postponed receipts balance sum: {}\n\
                 \tFinal postponed receipts balance sum: {}\n\
                 \t{:?}\n\
                 \tIncoming Receipts {:#?}\n\
                 \tOutgoing Receipts {:#?}",
                initial_balance,
                final_balance,
                initial_account_balance,
                final_account_balance,
                incoming_receipts_balance,
                outgoing_receipts_balance,
                initial_postponed_receipts_balance,
                final_postponed_receipts_balance,
                stats,
                prev_receipts,
                new_receipts,
            );
        }
        Ok(())
    }

    pub fn compute_storage_usage(&self, records: &[StateRecord]) -> HashMap<AccountId, u64> {
        let mut result = HashMap::new();
        let config = RuntimeFeesConfig::default().storage_usage_config;
        for record in records {
            let account_and_storage = match record {
                StateRecord::Account { account_id, .. } => {
                    Some((account_id.clone(), config.account_cost))
                }
                StateRecord::Data { key, value } => {
                    let key = from_base64(key).expect("Failed to decode key");
                    let value = from_base64(value).expect("Failed to decode value");
                    let separator =
                        (1..key.len()).find(|&x| key[x] == ACCOUNT_DATA_SEPARATOR[0]).unwrap();
                    let account_id = &key[1..separator];
                    let account_id =
                        String::from_utf8(account_id.to_vec()).expect("Invalid account id");
                    let data_key = &key[(separator + 1)..];
                    let storage_usage = config.data_record_cost
                        + config.key_cost_per_byte * (data_key.len() as u64)
                        + config.value_cost_per_byte * (value.len() as u64);
                    Some((account_id, storage_usage))
                }
                StateRecord::Contract { account_id, code } => {
                    let code = from_base64(&code).expect("Failed to decode wasm from base64");
                    Some((account_id.clone(), config.code_cost_per_byte * (code.len() as u64)))
                }
                StateRecord::AccessKey { account_id, public_key, access_key } => {
                    let public_key: PublicKey = public_key.clone();
                    let access_key: AccessKey = access_key.clone().into();
                    let storage_usage = config.data_record_cost
                        + config.key_cost_per_byte
                            * (public_key.try_to_vec().ok().unwrap_or_default().len() as u64)
                        + config.value_cost_per_byte
                            * (access_key.try_to_vec().ok().unwrap_or_default().len() as u64);
                    Some((account_id.clone(), storage_usage))
                }
                StateRecord::PostponedReceipt(_) => None,
                StateRecord::ReceivedData { .. } => None,
            };
            if let Some((account, storage_usage)) = account_and_storage {
                *result.entry(account).or_default() += storage_usage;
            }
        }
        result
    }

    /// Balances are account, publickey, initial_balance, initial_tx_stake
    pub fn apply_genesis_state(
        &self,
        mut state_update: TrieUpdate,
        validators: &[(AccountId, PublicKey, Balance)],
        records: &[StateRecord],
    ) -> (StoreUpdate, StateRoot) {
        let mut postponed_receipts: Vec<Receipt> = vec![];
        for record in records {
            match record.clone() {
                StateRecord::Account { account_id, account } => {
                    set_account(&mut state_update, &account_id, &account.into());
                }
                StateRecord::Data { key, value } => {
                    state_update.set(
                        from_base64(&key).expect("Failed to decode key"),
                        DBValue::from_vec(from_base64(&value).expect("Failed to decode value")),
                    );
                }
                StateRecord::Contract { account_id, code } => {
                    let code = ContractCode::new(
                        from_base64(&code).expect("Failed to decode wasm from base64"),
                    );
                    set_code(&mut state_update, &account_id, &code);
                }
                StateRecord::AccessKey { account_id, public_key, access_key } => {
                    set_access_key(&mut state_update, &account_id, &public_key, &access_key.into());
                }
                StateRecord::PostponedReceipt(receipt) => {
                    // Delaying processing postponed receipts, until we process all data first
                    postponed_receipts
                        .push((*receipt).try_into().expect("Failed to convert receipt from view"));
                }
                StateRecord::ReceivedData { account_id, data_id, data } => {
                    set_received_data(
                        &mut state_update,
                        &account_id,
                        &data_id.into(),
                        &ReceivedData { data },
                    );
                }
            }
        }
        for (account_id, storage_usage) in self.compute_storage_usage(records) {
            let mut account = get_account(&state_update, &account_id)
                .expect("Genesis storage error")
                .expect("Account must exist");
            account.storage_usage = storage_usage;
            set_account(&mut state_update, &account_id, &account);
        }
        // Processing postponed receipts after we stored all received data
        for receipt in postponed_receipts {
            let account_id = &receipt.receiver_id;
            let action_receipt = match &receipt.receipt {
                ReceiptEnum::Action(a) => a,
                _ => panic!("Expected action receipt"),
            };
            // Logic similar to `apply_receipt`
            let mut pending_data_count = 0;
            for data_id in &action_receipt.input_data_ids {
                if get_received_data(&state_update, account_id, data_id)
                    .expect("Genesis storage error")
                    .is_none()
                {
                    pending_data_count += 1;
                    set(
                        &mut state_update,
                        key_for_postponed_receipt_id(account_id, data_id),
                        &receipt.receipt_id,
                    )
                }
            }
            if pending_data_count == 0 {
                panic!("Postponed receipt should have pending data")
            } else {
                set(
                    &mut state_update,
                    key_for_pending_data_count(account_id, &receipt.receipt_id),
                    &pending_data_count,
                );
                set_receipt(&mut state_update, &receipt);
            }
        }

        for (account_id, _, amount) in validators {
            let mut account: Account = get_account(&state_update, account_id)
                .expect("Genesis storage error")
                .expect("account must exist");
            account.locked = *amount;
            set_account(&mut state_update, account_id, &account);
        }
        let trie = state_update.trie.clone();
        let state_update_state = state_update
            .finalize()
            .expect("Genesis state update failed")
            .into(trie)
            .expect("Genesis state update failed");
        (
            state_update_state.0,
            StateRoot { hash: state_update_state.1, num_parts: 9 /* TODO MOO */ },
        )
    }
}

#[cfg(test)]
mod tests {
    use near_primitives::hash::hash;
    use near_primitives::types::MerkleHash;
    use near_store::test_utils::create_trie;
    use testlib::runtime_utils::{alice_account, bob_account};

    use super::*;
    use near_crypto::{InMemorySigner, KeyType};
    use near_primitives::transaction::TransferAction;

    #[test]
    fn test_get_and_set_accounts() {
        let trie = create_trie();
        let mut state_update = TrieUpdate::new(trie, MerkleHash::default());
        let test_account = Account::new(10, hash(&[]), 0);
        let account_id = bob_account();
        set_account(&mut state_update, &account_id, &test_account);
        let get_res = get_account(&state_update, &account_id).unwrap().unwrap();
        assert_eq!(test_account, get_res);
    }

    #[test]
    fn test_get_account_from_trie() {
        let trie = create_trie();
        let root = MerkleHash::default();
        let mut state_update = TrieUpdate::new(trie.clone(), root);
        let test_account = Account::new(10, hash(&[]), 0);
        let account_id = bob_account();
        set_account(&mut state_update, &account_id, &test_account);
        let (store_update, new_root) = state_update.finalize().unwrap().into(trie.clone()).unwrap();
        store_update.commit().unwrap();
        let new_state_update = TrieUpdate::new(trie.clone(), new_root);
        let get_res = get_account(&new_state_update, &account_id).unwrap().unwrap();
        assert_eq!(test_account, get_res);
    }

    /***********************/
    /* Balance check tests */
    /***********************/

    #[test]
    fn test_check_balance_no_op() {
        let trie = create_trie();
        let root = MerkleHash::default();
        let initial_state = TrieUpdate::new(trie.clone(), root);
        let final_state = TrieUpdate::new(trie.clone(), root);
        let runtime = Runtime::new(RuntimeConfig::default());
        runtime
            .check_balance(&initial_state, &final_state, &[], &[], &[], &ApplyStats::default())
            .unwrap();
    }

    #[test]
    #[should_panic]
    fn test_check_balance_unaccounted_refund() {
        let trie = create_trie();
        let root = MerkleHash::default();
        let initial_state = TrieUpdate::new(trie.clone(), root);
        let final_state = TrieUpdate::new(trie.clone(), root);
        let runtime = Runtime::new(RuntimeConfig::default());
        runtime
            .check_balance(
                &initial_state,
                &final_state,
                &[Receipt::new_refund(&alice_account(), 1000)],
                &[],
                &[],
                &ApplyStats::default(),
            )
            .unwrap_or_else(|_| ());
    }

    #[test]
    fn test_check_balance_refund() {
        let trie = create_trie();
        let root = MerkleHash::default();
        let account_id = alice_account();

        let initial_balance = 1_000_000;
        let refund_balance = 1000;

        let mut initial_state = TrieUpdate::new(trie.clone(), root);
        let initial_account = Account::new(initial_balance, hash(&[]), 0);
        set_account(&mut initial_state, &account_id, &initial_account);
        initial_state.commit();

        let mut final_state = TrieUpdate::new(trie.clone(), root);
        let final_account = Account::new(initial_balance + refund_balance, hash(&[]), 0);
        set_account(&mut final_state, &account_id, &final_account);
        final_state.commit();

        let runtime = Runtime::new(RuntimeConfig::default());
        runtime
            .check_balance(
                &initial_state,
                &final_state,
                &[Receipt::new_refund(&account_id, refund_balance)],
                &[],
                &[],
                &ApplyStats::default(),
            )
            .unwrap();
    }

    #[test]
    fn test_check_balance_tx_to_receipt() {
        let trie = create_trie();
        let root = MerkleHash::default();
        let account_id = alice_account();

        let initial_balance = 1_000_000_000;
        let deposit = 500_000_000;
        let gas_price = 100;
        let runtime_config = RuntimeConfig::default();
        let cfg = &runtime_config.transaction_costs;
        let exec_gas = cfg.action_receipt_creation_config.exec_fee()
            + cfg.action_creation_config.transfer_cost.exec_fee();
        let send_gas = cfg.action_receipt_creation_config.send_fee(false)
            + cfg.action_creation_config.transfer_cost.send_fee(false);
        let contract_reward = (send_gas * cfg.burnt_gas_reward.numerator
            / cfg.burnt_gas_reward.denominator) as Balance
            * gas_price;
        let total_validator_reward = send_gas as Balance * gas_price - contract_reward;
        let mut initial_state = TrieUpdate::new(trie.clone(), root);
        let initial_account = Account::new(initial_balance, hash(&[]), 0);
        set_account(&mut initial_state, &account_id, &initial_account);
        initial_state.commit();

        let mut final_state = TrieUpdate::new(trie.clone(), root);
        let final_account = Account::new(
            initial_balance - (exec_gas + send_gas) as Balance * gas_price - deposit
                + contract_reward,
            hash(&[]),
            0,
        );
        set_account(&mut final_state, &account_id, &final_account);
        final_state.commit();

        let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);
        let tx = SignedTransaction::send_money(
            1,
            account_id.clone(),
            bob_account(),
            &signer,
            deposit,
            CryptoHash::default(),
        );
        let receipt = Receipt {
            predecessor_id: tx.transaction.signer_id.clone(),
            receiver_id: tx.transaction.receiver_id.clone(),
            receipt_id: Default::default(),
            receipt: ReceiptEnum::Action(ActionReceipt {
                signer_id: tx.transaction.signer_id.clone(),
                signer_public_key: tx.transaction.public_key.clone(),
                gas_price,
                output_data_receivers: vec![],
                input_data_ids: vec![],
                actions: vec![Action::Transfer(TransferAction { deposit })],
            }),
        };

        let runtime = Runtime::new(runtime_config);
        runtime
            .check_balance(
                &initial_state,
                &final_state,
                &[],
                &[tx],
                &[receipt],
                &ApplyStats { total_rent_paid: 0, total_validator_reward, total_balance_burnt: 0 },
            )
            .unwrap();
    }
}

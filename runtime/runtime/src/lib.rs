#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::{Arc, Mutex};

use borsh::BorshSerialize;
use kvdb::DBValue;

use near_crypto::{PublicKey, ReadablePublicKey};
use near_primitives::account::{AccessKey, AccessKeyPermission, Account};
use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{ActionReceipt, DataReceipt, Receipt, ReceiptEnum, ReceivedData};
use near_primitives::serialize::from_base64;
use near_primitives::transaction::{
    Action, LogEntry, SignedTransaction, TransactionLog, TransactionResult, TransactionStatus,
};
use near_primitives::types::{
    AccountId, Balance, BlockIndex, Gas, MerkleHash, Nonce, ValidatorStake,
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
use crate::ethereum::EthashProvider;
pub use crate::store::StateRecord;

mod actions;
pub mod adapter;
pub mod cache;
pub mod config;
pub mod ethereum;
pub mod ext;
pub mod state_viewer;
mod store;

pub const ETHASH_CACHE_PATH: &str = "ethash_cache";

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
}

pub struct ApplyResult {
    pub root: MerkleHash,
    pub trie_changes: TrieChanges,
    pub validator_proposals: Vec<ValidatorStake>,
    pub new_receipts: Vec<Receipt>,
    pub tx_result: Vec<TransactionLog>,
    pub total_rent_paid: Balance,
}

#[derive(Debug)]
pub struct ActionResult {
    pub gas_burnt: Gas,
    pub gas_used: Gas,
    pub result: Result<ReturnData, Box<dyn std::error::Error>>,
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

#[allow(dead_code)]
pub struct Runtime {
    config: RuntimeConfig,
    ethash_provider: Arc<Mutex<EthashProvider>>,
}

impl Runtime {
    pub fn new(config: RuntimeConfig, ethash_provider: Arc<Mutex<EthashProvider>>) -> Self {
        Runtime { config, ethash_provider }
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
    ) -> Result<VerificationResult, Box<dyn std::error::Error>> {
        let transaction = &signed_transaction.transaction;
        let signer_id = &transaction.signer_id;
        if !is_valid_account_id(&signer_id) {
            return Err(format!(
                "Invalid signer account ID {:?} according to requirements",
                signer_id
            )
            .into());
        }
        if !is_valid_account_id(&transaction.receiver_id) {
            return Err(format!(
                "Invalid receiver account ID {:?} according to requirements",
                transaction.receiver_id
            )
            .into());
        }

        if !signed_transaction
            .signature
            .verify(signed_transaction.get_hash().as_ref(), &transaction.public_key)
        {
            return Err("Transaction is not signed with a given public key".into());
        }
        let mut signer = match get_account(state_update, signer_id)? {
            Some(signer) => signer,
            None => {
                return Err(format!("Signer {:?} does not exist", signer_id).into());
            }
        };
        let mut access_key =
            match get_access_key(state_update, &signer_id, &transaction.public_key)? {
                Some(access_key) => access_key,
                None => {
                    return Err(format!(
                        "Signer {:?} doesn't have access key with the given public_key {}",
                        signer_id, &transaction.public_key,
                    )
                    .into());
                }
            };

        if transaction.nonce <= access_key.nonce {
            return Err(format!(
                "Transaction nonce {} must be larger than nonce of the used access key {}",
                transaction.nonce, access_key.nonce,
            )
            .into());
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
            format!(
                "Sender {} does not have enough balance {} for operation costing {}",
                signer_id, signer.amount, total_cost
            )
        })?;

        if let AccessKeyPermission::FunctionCall(ref mut function_call_permission) =
            access_key.permission
        {
            if let Some(ref mut allowance) = function_call_permission.allowance {
                *allowance = allowance.checked_sub(total_cost).ok_or_else(|| {
                    format!(
                        "Access Key {}:{} does not have enough balance {} for transaction costing {}",
                        signer_id, transaction.public_key, allowance, total_cost
                    )
                })?;
            }
        }

        if !check_rent(&signer_id, &signer, &self.config, apply_state.epoch_length) {
            return Err(format!("Failed to execute, because the account {} wouldn't have enough to pay required rent", signer_id).into());
        }

        if let AccessKeyPermission::FunctionCall(ref function_call_permission) =
            access_key.permission
        {
            if transaction.actions.len() != 1 {
                return Err(
                    "Transaction has more than 1 actions and is using function call access key"
                        .into(),
                );
            }
            if let Some(Action::FunctionCall(ref function_call)) = transaction.actions.get(0) {
                if transaction.receiver_id != function_call_permission.receiver_id {
                    return Err(format!(
                        "Transaction receiver_id {:?} doesn't match the access key receiver_id {:?}",
                        &transaction.receiver_id,
                        &function_call_permission.receiver_id,
                    ).into());
                }
                if !function_call_permission.method_names.is_empty()
                    && function_call_permission
                        .method_names
                        .iter()
                        .all(|method_name| &function_call.method_name != method_name)
                {
                    return Err(format!(
                        "Transaction method name {:?} isn't allowed by the access key",
                        &function_call.method_name
                    )
                    .into());
                }
            } else {
                return Err("The used access key requires exactly one FunctionCall action".into());
            }
        };

        set_access_key(state_update, &signer_id, &transaction.public_key, &access_key);

        // Account reward for gas burnt.
        signer.amount += Balance::from(
            gas_burnt * self.config.transaction_costs.burnt_gas_reward.numerator
                / self.config.transaction_costs.burnt_gas_reward.denominator,
        ) * apply_state.gas_price;

        set_account(state_update, &signer_id, &signer);

        Ok(VerificationResult { gas_burnt, gas_used, rent_paid })
    }

    /// Processes signed transaction, charges fees and generates the receipt
    fn process_transaction(
        &self,
        state_update: &mut TrieUpdate,
        apply_state: &ApplyState,
        signed_transaction: &SignedTransaction,
        new_local_receipts: &mut Vec<Receipt>,
        new_receipts: &mut Vec<Receipt>,
        total_rent_paid: &mut Balance,
    ) -> Result<TransactionLog, StorageError> {
        let result =
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
                    *total_rent_paid += verification_result.rent_paid;
                    TransactionResult {
                        status: TransactionStatus::Completed,
                        logs: vec![],
                        receipts: vec![receipt_id],
                        result: None,
                        gas_burnt: verification_result.gas_burnt,
                    }
                }
                Err(s) => {
                    state_update.rollback();
                    if let Some(e) = s.downcast_ref::<StorageError>() {
                        // TODO fix error type in apply_signed_transaction
                        return Err(e.clone());
                    }
                    TransactionResult {
                        status: TransactionStatus::Failed,
                        logs: vec![format!("Runtime error: {}", s)],
                        receipts: vec![],
                        result: None,
                        gas_burnt: 0,
                    }
                }
            };
        Self::print_log(&result.logs);
        Ok(TransactionLog { hash: signed_transaction.get_hash(), result })
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
                );
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
        total_rent_paid: &mut Balance,
    ) -> Result<TransactionLog, StorageError> {
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
                if check_rent(account_id, account, &self.config, apply_state.epoch_length) {
                    set_account(state_update, account_id, account);
                } else {
                    result.merge(ActionResult {
                        result: Err(format!(
                            "`the account {} wouldn't have enough to pay required rent",
                            account_id
                        )
                        .into()),
                        ..Default::default()
                    });
                }
            }
        }

        // If the receipt is a refund, then we consider it free without burnt gas.
        if receipt.predecessor_id == system_account() {
            result.gas_burnt = 0;
            result.gas_used = 0;
        } else {
            // Calculating and generating refunds
            self.generate_refund_receipts(receipt, action_receipt, &mut result);
        }

        // Moving validator proposals
        validator_proposals.append(&mut result.validator_proposals);

        // Generating transaction result and committing or rolling back state.
        let transaction_status = match &result.result {
            Ok(_) => {
                *total_rent_paid += rent_paid;
                state_update.commit();
                TransactionStatus::Completed
            }
            Err(e) => {
                state_update.rollback();
                result.logs.push(format!("Runtime error: {}", e));
                TransactionStatus::Failed
            }
        };

        // Adding burnt gas reward if the account exists.
        let gas_reward = result.gas_burnt
            * self.config.transaction_costs.burnt_gas_reward.numerator
            / self.config.transaction_costs.burnt_gas_reward.denominator;
        if gas_reward > 0 {
            let mut account = get_account(state_update, account_id)?;
            if let Some(ref mut account) = account {
                account.amount += Balance::from(gas_reward) * action_receipt.gas_price;
                set_account(state_update, account_id, account);
                state_update.commit();
            }
        }

        // Generating outgoing data and receipts
        let transaction_result = if let Ok(ReturnData::ReceiptIndex(receipt_index)) = result.result
        {
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
            None
        } else {
            let data = match result.result {
                Ok(ReturnData::Value(data)) => Some(data),
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
            data
        };

        // Generating receipt IDs
        let transaction_new_receipt_ids = result
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

        Self::print_log(&result.logs);

        Ok(TransactionLog {
            hash: receipt.receipt_id,
            result: TransactionResult {
                status: transaction_status,
                logs: result.logs,
                receipts: transaction_new_receipt_ids,
                result: transaction_result,
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
        const OVERFLOW_CHECKED_ERR: &str = "Overflow has already been checked.";
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
        total_rent_paid: &mut Balance,
    ) -> Result<Option<TransactionLog>, StorageError> {
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
                                total_rent_paid,
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
                            total_rent_paid,
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

    /// apply transactions from this block and receipts from previous block
    pub fn apply(
        &self,
        mut state_update: TrieUpdate,
        apply_state: &ApplyState,
        prev_receipts: &[Receipt],
        transactions: &[SignedTransaction],
    ) -> Result<ApplyResult, StorageError> {
        let mut new_receipts = Vec::new();
        let mut validator_proposals = vec![];
        let mut local_receipts = vec![];
        let mut tx_result = vec![];
        let mut total_rent_paid = 0;

        for signed_transaction in transactions {
            tx_result.push(self.process_transaction(
                &mut state_update,
                apply_state,
                signed_transaction,
                &mut local_receipts,
                &mut new_receipts,
                &mut total_rent_paid,
            )?);
        }

        for receipt in local_receipts.iter().chain(prev_receipts.iter()) {
            self.process_receipt(
                &mut state_update,
                apply_state,
                receipt,
                &mut new_receipts,
                &mut validator_proposals,
                &mut total_rent_paid,
            )?
            .into_iter()
            .for_each(|res| tx_result.push(res));
        }
        let trie_changes = state_update.finalize()?;
        Ok(ApplyResult {
            root: trie_changes.new_root,
            trie_changes,
            validator_proposals,
            new_receipts,
            tx_result,
            total_rent_paid,
        })
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
        validators: &[(AccountId, ReadablePublicKey, Balance)],
        records: &[StateRecord],
    ) -> (StoreUpdate, MerkleHash) {
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
            account.staked = *amount;
            set_account(&mut state_update, account_id, &account);
        }
        let trie = state_update.trie.clone();
        state_update
            .finalize()
            .expect("Genesis state update failed")
            .into(trie)
            .expect("Genesis state update failed")
    }
}

#[cfg(test)]
mod tests {
    use near_primitives::hash::hash;
    use near_primitives::types::MerkleHash;
    use near_store::test_utils::create_trie;
    use testlib::runtime_utils::bob_account;

    use super::*;

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
}

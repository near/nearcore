use crate::safe_add_balance_apply;

use crate::config::{
    safe_add_balance, safe_add_gas, safe_gas_to_balance, total_deposit, total_prepaid_exec_fees,
    total_prepaid_gas,
};
use crate::{ApplyStats, DelayedReceiptIndices, ValidatorAccountsUpdate};
use near_primitives::errors::{
    BalanceMismatchError, IntegerOverflowError, RuntimeError, StorageError,
};
use near_primitives::receipt::{Receipt, ReceiptEnum};
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::transaction::SignedTransaction;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{AccountId, Balance};
use near_primitives::utils::system_account;
use near_primitives::version::ProtocolVersion;
use near_store::{get, get_account, get_postponed_receipt, TrieUpdate};
use std::collections::HashSet;

pub fn check_balance(
    transaction_costs: &RuntimeFeesConfig,
    initial_state: &TrieUpdate,
    final_state: &TrieUpdate,
    validator_accounts_update: &Option<ValidatorAccountsUpdate>,
    incoming_receipts: &[Receipt],
    transactions: &[SignedTransaction],
    outgoing_receipts: &[Receipt],
    stats: &ApplyStats,
    current_protocol_version: ProtocolVersion,
) -> Result<(), RuntimeError> {
    // Delayed receipts
    let initial_delayed_receipt_indices: DelayedReceiptIndices =
        get(&initial_state, &TrieKey::DelayedReceiptIndices)?.unwrap_or_default();
    let final_delayed_receipt_indices: DelayedReceiptIndices =
        get(&final_state, &TrieKey::DelayedReceiptIndices)?.unwrap_or_default();
    let get_delayed_receipts = |from_index, to_index, state| {
        (from_index..to_index)
            .map(|index| {
                get(state, &TrieKey::DelayedReceipt { index })?.ok_or_else(|| {
                    StorageError::StorageInconsistentState(format!(
                        "Delayed receipt #{} should be in the state",
                        index
                    ))
                })
            })
            .collect::<Result<Vec<Receipt>, StorageError>>()
    };
    // Previously delayed receipts that were processed this time.
    let processed_delayed_receipts = get_delayed_receipts(
        initial_delayed_receipt_indices.first_index,
        final_delayed_receipt_indices.first_index,
        &initial_state,
    )?;
    // Receipts that were not processed this time and are delayed now.
    let new_delayed_receipts = get_delayed_receipts(
        initial_delayed_receipt_indices.next_available_index,
        final_delayed_receipt_indices.next_available_index,
        &final_state,
    )?;

    // Accounts
    let mut all_accounts_ids: HashSet<AccountId> = transactions
        .iter()
        .map(|tx| tx.transaction.signer_id.clone())
        .chain(incoming_receipts.iter().map(|r| r.receiver_id.clone()))
        .chain(processed_delayed_receipts.iter().map(|r| r.receiver_id.clone()))
        .collect();
    let incoming_validator_rewards =
        if let Some(validator_accounts_update) = validator_accounts_update {
            all_accounts_ids.extend(validator_accounts_update.stake_info.keys().cloned());
            all_accounts_ids.extend(validator_accounts_update.validator_rewards.keys().cloned());
            all_accounts_ids.extend(validator_accounts_update.last_proposals.keys().cloned());
            all_accounts_ids.extend(validator_accounts_update.slashing_info.keys().cloned());
            if let Some(account_id) = &validator_accounts_update.protocol_treasury_account_id {
                all_accounts_ids.insert(account_id.clone());
            }
            validator_accounts_update
                .validator_rewards
                .values()
                .try_fold(0u128, |res, balance| safe_add_balance(res, *balance))?
        } else {
            0
        };
    let total_accounts_balance = |state| -> Result<Balance, RuntimeError> {
        Ok(all_accounts_ids
            .iter()
            .map(|account_id| {
                get_account(state, account_id)?.map_or(Ok(0), |a| {
                    safe_add_balance(a.amount(), a.locked())
                        .map_err(|_| RuntimeError::UnexpectedIntegerOverflow)
                })
            })
            .collect::<Result<Vec<Balance>, RuntimeError>>()?
            .into_iter()
            .try_fold(0u128, |res, balance| safe_add_balance(res, balance))?)
    };
    let initial_accounts_balance = total_accounts_balance(&initial_state)?;
    let final_accounts_balance = total_accounts_balance(&final_state)?;
    // Receipts
    let receipt_cost = |receipt: &Receipt| -> Result<Balance, IntegerOverflowError> {
        Ok(match &receipt.receipt {
            ReceiptEnum::Action(action_receipt) => {
                let mut total_cost = total_deposit(&action_receipt.actions)?;
                if receipt.predecessor_id != system_account() {
                    let mut total_gas = safe_add_gas(
                        transaction_costs.action_receipt_creation_config.exec_fee(),
                        total_prepaid_exec_fees(
                            transaction_costs,
                            &action_receipt.actions,
                            &receipt.receiver_id,
                            current_protocol_version,
                        )?,
                    )?;
                    total_gas =
                        safe_add_gas(total_gas, total_prepaid_gas(&action_receipt.actions)?)?;
                    let total_gas_cost = safe_gas_to_balance(action_receipt.gas_price, total_gas)?;
                    total_cost = safe_add_balance(total_cost, total_gas_cost)?;
                }
                total_cost
            }
            ReceiptEnum::Data(_) => 0,
        })
    };
    let receipts_cost = |receipts: &[Receipt]| -> Result<Balance, IntegerOverflowError> {
        receipts
            .iter()
            .map(receipt_cost)
            .collect::<Result<Vec<Balance>, IntegerOverflowError>>()?
            .into_iter()
            .try_fold(0u128, |res, balance| safe_add_balance(res, balance))
    };
    let incoming_receipts_balance = receipts_cost(incoming_receipts)?;
    let outgoing_receipts_balance = receipts_cost(outgoing_receipts)?;
    let processed_delayed_receipts_balance = receipts_cost(&processed_delayed_receipts)?;
    let new_delayed_receipts_balance = receipts_cost(&new_delayed_receipts)?;
    // Postponed actions receipts. The receipts can be postponed and stored with the receiver's
    // account ID when the input data is not received yet.
    // We calculate all potential receipts IDs that might be postponed initially or after the
    // execution.
    let all_potential_postponed_receipt_ids = incoming_receipts
        .iter()
        .chain(processed_delayed_receipts.iter())
        .map(|receipt| {
            let account_id = &receipt.receiver_id;
            match &receipt.receipt {
                ReceiptEnum::Action(_) => Ok(Some((account_id.clone(), receipt.receipt_id))),
                ReceiptEnum::Data(data_receipt) => {
                    if let Some(receipt_id) = get(
                        initial_state,
                        &TrieKey::PostponedReceiptId {
                            receiver_id: account_id.clone(),
                            data_id: data_receipt.data_id,
                        },
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
        .collect::<HashSet<_>>();

    let total_postponed_receipts_cost = |state| -> Result<Balance, RuntimeError> {
        Ok(all_potential_postponed_receipt_ids
            .iter()
            .map(|(account_id, receipt_id)| {
                Ok(get_postponed_receipt(state, account_id, *receipt_id)?
                    .map_or(Ok(0), |r| receipt_cost(&r))?)
            })
            .collect::<Result<Vec<Balance>, RuntimeError>>()?
            .into_iter()
            .try_fold(0u128, |res, balance| safe_add_balance(res, balance))?)
    };
    let initial_postponed_receipts_balance = total_postponed_receipts_cost(initial_state)?;
    let final_postponed_receipts_balance = total_postponed_receipts_cost(final_state)?;
    // Sum it up

    let initial_balance = safe_add_balance_apply!(
        incoming_validator_rewards,
        initial_accounts_balance,
        incoming_receipts_balance,
        processed_delayed_receipts_balance,
        initial_postponed_receipts_balance
    );
    let final_balance = safe_add_balance_apply!(
        final_accounts_balance,
        outgoing_receipts_balance,
        new_delayed_receipts_balance,
        final_postponed_receipts_balance,
        stats.tx_burnt_amount,
        stats.slashed_burnt_amount,
        stats.other_burnt_amount
    );
    if initial_balance != final_balance {
        Err(BalanceMismatchError {
            // Inputs
            incoming_validator_rewards,
            initial_accounts_balance,
            incoming_receipts_balance,
            processed_delayed_receipts_balance,
            initial_postponed_receipts_balance,
            // Outputs
            final_accounts_balance,
            outgoing_receipts_balance,
            new_delayed_receipts_balance,
            final_postponed_receipts_balance,
            tx_burnt_amount: stats.tx_burnt_amount,
            slashed_burnt_amount: stats.slashed_burnt_amount,
            other_burnt_amount: stats.other_burnt_amount,
        }
        .into())
    } else {
        Ok(())
    }
}

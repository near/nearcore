use crate::config::{
    safe_add_balance, safe_add_gas, safe_gas_to_balance, total_deposit, total_prepaid_exec_fees,
    total_prepaid_gas, total_prepaid_send_fees,
};
use crate::{DelayedReceiptIndices, ValidatorAccountsUpdate};
use crate::{SignedValidPeriodTransactions, safe_add_balance_apply};
use near_parameters::{ActionCosts, RuntimeConfig};
use near_primitives::chunk_apply_stats::BalanceStats;
use near_primitives::errors::{IntegerOverflowError, RuntimeError, StorageError};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{Receipt, ReceiptEnum, ReceiptOrStateStoredReceipt};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{AccountId, Balance, ShardId};
use near_store::trie::receipts_column_helper::{ShardsOutgoingReceiptBuffer, TrieQueue};
use near_store::{
    Trie, TrieAccess, TrieUpdate, get, get_account, get_postponed_receipt,
    get_promise_yield_receipt,
};
use std::collections::{BTreeSet, HashSet};

/// Returns delayed receipts with given range of indices.
fn get_delayed_receipts(
    state: &dyn TrieAccess,
    indexes: std::ops::Range<u64>,
) -> Result<Vec<Receipt>, StorageError> {
    indexes
        .map(|index| {
            let receipt: Result<ReceiptOrStateStoredReceipt, StorageError> =
                get(state, &TrieKey::DelayedReceipt { index })?.ok_or_else(|| {
                    StorageError::StorageInconsistentState(format!(
                        "Delayed receipt #{} should be in the state",
                        index
                    ))
                });
            receipt.map(|receipt| receipt.into_receipt())
        })
        .collect()
}

/// Calculates and returns cost of a receipt.
fn receipt_cost(
    config: &RuntimeConfig,
    receipt: &Receipt,
) -> Result<Balance, IntegerOverflowError> {
    Ok(match receipt.receipt() {
        ReceiptEnum::Action(action_receipt) | ReceiptEnum::PromiseYield(action_receipt) => {
            let mut total_cost = total_deposit(&action_receipt.actions)?;
            if !receipt.predecessor_id().is_system() {
                let mut total_gas = safe_add_gas(
                    config.fees.fee(ActionCosts::new_action_receipt).exec_fee(),
                    total_prepaid_exec_fees(
                        config,
                        &action_receipt.actions,
                        receipt.receiver_id(),
                    )?,
                )?;
                total_gas = safe_add_gas(total_gas, total_prepaid_gas(&action_receipt.actions)?)?;
                total_gas = safe_add_gas(
                    total_gas,
                    total_prepaid_send_fees(config, &action_receipt.actions)?,
                )?;
                let total_gas_cost = safe_gas_to_balance(action_receipt.gas_price, total_gas)?;
                total_cost = safe_add_balance(total_cost, total_gas_cost)?;
            }
            total_cost
        }
        ReceiptEnum::GlobalContractDistribution(_)
        | ReceiptEnum::Data(_)
        | ReceiptEnum::PromiseResume(_) => 0,
    })
}

/// Calculates and returns total cost of all the receipts.
fn total_receipts_cost(
    config: &RuntimeConfig,
    receipts: &[Receipt],
) -> Result<Balance, IntegerOverflowError> {
    receipts.iter().try_fold(0, |accumulator, receipt| {
        let cost = receipt_cost(config, receipt)?;
        safe_add_balance(accumulator, cost)
    })
}

/// Returns total account balance of all accounts with given ids.
fn total_accounts_balance(
    state: &dyn TrieAccess,
    accounts_ids: &HashSet<AccountId>,
) -> Result<Balance, RuntimeError> {
    accounts_ids.iter().try_fold(0u128, |accumulator, account_id| {
        let (amount, locked) = match get_account(state, account_id)? {
            None => return Ok(accumulator),
            Some(account) => (account.amount(), account.locked()),
        };
        Ok(safe_add_balance_apply!(accumulator, amount, locked))
    })
}

#[derive(Eq, Hash, PartialEq)]
enum PostponedReceiptType {
    Action,
    PromiseYield,
}

/// Calculates and returns total costs of all the postponed receipts.
fn total_postponed_receipts_cost(
    state: &dyn TrieAccess,
    config: &RuntimeConfig,
    receipt_ids: &HashSet<(PostponedReceiptType, AccountId, crate::CryptoHash)>,
) -> Result<Balance, RuntimeError> {
    receipt_ids.iter().try_fold(0, |total, item| {
        let (receipt_type, account_id, lookup_id) = item;

        let cost = match receipt_type {
            PostponedReceiptType::Action => {
                match get_postponed_receipt(state, account_id, *lookup_id)? {
                    None => return Ok(total),
                    Some(receipt) => receipt_cost(config, &receipt)?,
                }
            }
            PostponedReceiptType::PromiseYield => {
                match get_promise_yield_receipt(state, account_id, *lookup_id)? {
                    None => return Ok(total),
                    Some(receipt) => receipt_cost(config, &receipt)?,
                }
            }
        };

        safe_add_balance(total, cost).map_err(|_| {
            RuntimeError::UnexpectedIntegerOverflow("total_postponed_receipts_cost".into())
        })
    })
}

/// Compute balance going in and out of the outgoing receipt buffer.
fn buffered_receipts(
    initial_state: &Trie,
    final_state: &TrieUpdate,
) -> Result<(Vec<Receipt>, Vec<Receipt>), RuntimeError> {
    let mut initial_buffers = ShardsOutgoingReceiptBuffer::load(initial_state)?;
    let mut final_buffers = ShardsOutgoingReceiptBuffer::load(final_state)?;
    let mut forwarded_receipts: Vec<Receipt> = vec![];
    let mut new_buffered_receipts: Vec<Receipt> = vec![];

    let mut shards: BTreeSet<ShardId> = BTreeSet::new();
    shards.extend(initial_buffers.shards().iter());
    shards.extend(final_buffers.shards().iter());
    for shard_id in shards {
        let initial_buffer = initial_buffers.to_shard(shard_id);
        let final_buffer = final_buffers.to_shard(shard_id);
        let before = initial_buffer.indices();
        let after = final_buffer.indices();
        // Conservative math check to avoid future problems with merged shards,
        // in which case the final index can be 0 and the initial index larger.
        if let Some(num_forwarded) = after.first_index.checked_sub(before.first_index) {
            // The first n receipts were forwarded.
            for receipt in initial_buffer.iter(initial_state, true).take(num_forwarded as usize) {
                let receipt = receipt?;
                let receipt = receipt.into_receipt();
                forwarded_receipts.push(receipt)
            }
        }
        if let Some(num_buffered) =
            after.next_available_index.checked_sub(before.next_available_index)
        {
            // The last n receipts are new. ("rev" to take from the back)
            for receipt in final_buffer.iter(final_state, true).rev().take(num_buffered as usize) {
                let receipt = receipt?;
                let receipt = receipt.into_receipt();
                new_buffered_receipts.push(receipt)
            }
        }
    }

    Ok((forwarded_receipts, new_buffered_receipts))
}

/// Find account ids of all accounts touched in receipts, transactions, and
/// validator updates.
fn all_touched_accounts(
    incoming_receipts: &[Receipt],
    yield_timeout_receipts: &[Receipt],
    processed_delayed_receipts: &[Receipt],
    transactions: SignedValidPeriodTransactions,
    validator_accounts_update: &Option<ValidatorAccountsUpdate>,
) -> Result<HashSet<AccountId>, RuntimeError> {
    let mut all_accounts_ids: HashSet<AccountId> = transactions
        .iter_nonexpired_transactions()
        .map(|tx| tx.transaction.signer_id().clone())
        .chain(incoming_receipts.iter().map(|r| r.receiver_id().clone()))
        .chain(yield_timeout_receipts.iter().map(|r| r.receiver_id().clone()))
        .chain(processed_delayed_receipts.iter().map(|r| r.receiver_id().clone()))
        .collect();
    if let Some(validator_accounts_update) = validator_accounts_update {
        all_accounts_ids.extend(validator_accounts_update.stake_info.keys().cloned());
        all_accounts_ids.extend(validator_accounts_update.validator_rewards.keys().cloned());
        all_accounts_ids.extend(validator_accounts_update.last_proposals.keys().cloned());
        if let Some(account_id) = &validator_accounts_update.protocol_treasury_account_id {
            all_accounts_ids.insert(account_id.clone());
        }
    };

    Ok(all_accounts_ids)
}

fn validator_rewards(
    validator_accounts_update: &ValidatorAccountsUpdate,
) -> Result<Balance, IntegerOverflowError> {
    validator_accounts_update
        .validator_rewards
        .values()
        .try_fold(0u128, |res, balance| safe_add_balance(res, *balance))
}

/// The receipts can be postponed and stored with the receiver's account ID when
/// the input data is not received yet. We calculate all potential receipts IDs
/// that might be postponed initially or after the execution.
fn potential_postponed_receipt_ids(
    incoming_receipts: &[Receipt],
    yield_timeout_receipts: &[Receipt],
    processed_delayed_receipts: &[Receipt],
    initial_state: &Trie,
) -> Result<HashSet<(PostponedReceiptType, AccountId, CryptoHash)>, StorageError> {
    incoming_receipts
        .iter()
        .chain(processed_delayed_receipts.iter())
        .chain(yield_timeout_receipts.iter())
        .filter_map(|receipt| {
            let account_id = receipt.receiver_id();
            match receipt.receipt() {
                ReceiptEnum::Action(_) => Some(Ok((
                    PostponedReceiptType::Action,
                    account_id.clone(),
                    *receipt.receipt_id(),
                ))),
                ReceiptEnum::Data(data_receipt) => {
                    let result = get(
                        initial_state,
                        &TrieKey::PostponedReceiptId {
                            receiver_id: account_id.clone(),
                            data_id: data_receipt.data_id,
                        },
                    );
                    match result {
                        Err(err) => Some(Err(err)),
                        Ok(None) => None,
                        Ok(Some(receipt_id)) => {
                            Some(Ok((PostponedReceiptType::Action, account_id.clone(), receipt_id)))
                        }
                    }
                }
                ReceiptEnum::PromiseYield(action_receipt) => Some(Ok((
                    PostponedReceiptType::PromiseYield,
                    account_id.clone(),
                    action_receipt.input_data_ids[0],
                ))),
                ReceiptEnum::PromiseResume(data_receipt) => Some(Ok((
                    PostponedReceiptType::PromiseYield,
                    account_id.clone(),
                    data_receipt.data_id,
                ))),
                ReceiptEnum::GlobalContractDistribution(_) => None,
            }
        })
        .collect::<Result<HashSet<_>, StorageError>>()
}

#[tracing::instrument(target = "runtime", level = "debug", "check_balance", skip_all, fields(
    transactions.len = transactions.len(),
    incoming_receipts.len = incoming_receipts.len(),
    yield_timeout_receipts.len = yield_timeout_receipts.len(),
    outgoing_receipts.len = outgoing_receipts.len()
))]
pub(crate) fn check_balance(
    config: &RuntimeConfig,
    final_state: &TrieUpdate,
    validator_accounts_update: &Option<ValidatorAccountsUpdate>,
    incoming_receipts: &[Receipt],
    processed_delayed_receipts: &[Receipt],
    yield_timeout_receipts: &[Receipt],
    transactions: SignedValidPeriodTransactions,
    outgoing_receipts: &[Receipt],
    stats: &BalanceStats,
) -> Result<(), RuntimeError> {
    let initial_state = final_state.trie();

    // Delayed receipts
    let initial_delayed_receipt_indices: DelayedReceiptIndices =
        get(initial_state, &TrieKey::DelayedReceiptIndices)?.unwrap_or_default();
    let final_delayed_receipt_indices: DelayedReceiptIndices =
        get(final_state, &TrieKey::DelayedReceiptIndices)?.unwrap_or_default();

    // Receipts that were not processed this time and are delayed now.
    let new_delayed_receipts = get_delayed_receipts(
        final_state,
        initial_delayed_receipt_indices.next_available_index
            ..final_delayed_receipt_indices.next_available_index,
    )?;

    // Buffered receipts
    let (forwarded_receipts, new_buffered_receipts) =
        buffered_receipts(initial_state, final_state)?;

    // Accounts
    let all_accounts_ids = all_touched_accounts(
        incoming_receipts,
        yield_timeout_receipts,
        &processed_delayed_receipts,
        transactions,
        validator_accounts_update,
    )?;

    let initial_accounts_balance = total_accounts_balance(initial_state, &all_accounts_ids)?;
    let final_accounts_balance = total_accounts_balance(final_state, &all_accounts_ids)?;

    // Validator rewards
    let incoming_validator_rewards =
        validator_accounts_update.as_ref().map(validator_rewards).transpose()?.unwrap_or(0);

    // Receipts
    let receipts_cost = |receipts: &[Receipt]| -> Result<Balance, IntegerOverflowError> {
        total_receipts_cost(config, receipts)
    };
    let incoming_receipts_balance =
        receipts_cost(incoming_receipts)? + receipts_cost(yield_timeout_receipts)?;
    let outgoing_receipts_balance = receipts_cost(outgoing_receipts)?;
    let processed_delayed_receipts_balance = receipts_cost(&processed_delayed_receipts)?;
    let new_delayed_receipts_balance = receipts_cost(&new_delayed_receipts)?;
    let forwarded_buffered_receipts_balance = receipts_cost(&forwarded_receipts)?;
    let new_buffered_receipts_balance = receipts_cost(&new_buffered_receipts)?;

    // Postponed actions receipts.
    let all_potential_postponed_receipt_ids = potential_postponed_receipt_ids(
        incoming_receipts,
        yield_timeout_receipts,
        &processed_delayed_receipts,
        initial_state,
    )?;

    let initial_postponed_receipts_balance =
        total_postponed_receipts_cost(initial_state, config, &all_potential_postponed_receipt_ids)?;
    let final_postponed_receipts_balance =
        total_postponed_receipts_cost(final_state, config, &all_potential_postponed_receipt_ids)?;

    // Sum it up
    let initial_balance = safe_add_balance_apply!(
        incoming_validator_rewards,
        initial_accounts_balance,
        incoming_receipts_balance,
        processed_delayed_receipts_balance,
        initial_postponed_receipts_balance,
        forwarded_buffered_receipts_balance
    );
    let final_balance = safe_add_balance_apply!(
        final_accounts_balance,
        outgoing_receipts_balance,
        new_delayed_receipts_balance,
        final_postponed_receipts_balance,
        stats.tx_burnt_amount,
        stats.slashed_burnt_amount,
        new_buffered_receipts_balance,
        stats.other_burnt_amount,
        stats.global_actions_burnt_amount
    );
    (initial_balance == final_balance)
        .then_some(())
        .ok_or_else(|| StorageError::StorageInternalError.into())
}

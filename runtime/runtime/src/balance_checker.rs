use crate::safe_add_balance_apply;

use crate::config::{
    safe_add_balance, safe_add_gas, safe_gas_to_balance, total_deposit, total_prepaid_exec_fees,
    total_prepaid_gas, total_prepaid_send_fees,
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
use near_primitives::version::ProtocolVersion;
use near_store::{get, get_account, get_postponed_receipt, TrieAccess, TrieUpdate};
use near_vm_runner::logic::ActionCosts;
use std::collections::HashSet;

/// Returns delayed receipts with given range of indices.
fn get_delayed_receipts(
    state: &dyn TrieAccess,
    indexes: std::ops::Range<u64>,
) -> Result<Vec<Receipt>, StorageError> {
    indexes
        .map(|index| {
            get(state, &TrieKey::DelayedReceipt { index })?.ok_or_else(|| {
                StorageError::StorageInconsistentState(format!(
                    "Delayed receipt #{} should be in the state",
                    index
                ))
            })
        })
        .collect()
}

/// Calculates and returns cost of a receipt.
fn receipt_cost(
    transaction_costs: &RuntimeFeesConfig,
    current_protocol_version: ProtocolVersion,
    receipt: &Receipt,
) -> Result<Balance, IntegerOverflowError> {
    Ok(match &receipt.receipt {
        ReceiptEnum::Action(action_receipt) => {
            let mut total_cost = total_deposit(&action_receipt.actions)?;
            if !AccountId::is_system(&receipt.predecessor_id) {
                let mut total_gas = safe_add_gas(
                    transaction_costs.fee(ActionCosts::new_action_receipt).exec_fee(),
                    total_prepaid_exec_fees(
                        transaction_costs,
                        &action_receipt.actions,
                        &receipt.receiver_id,
                        current_protocol_version,
                    )?,
                )?;
                total_gas = safe_add_gas(total_gas, total_prepaid_gas(&action_receipt.actions)?)?;
                total_gas = safe_add_gas(
                    total_gas,
                    total_prepaid_send_fees(
                        transaction_costs,
                        &action_receipt.actions,
                        current_protocol_version,
                    )?,
                )?;
                let total_gas_cost = safe_gas_to_balance(action_receipt.gas_price, total_gas)?;
                total_cost = safe_add_balance(total_cost, total_gas_cost)?;
            }
            total_cost
        }
        ReceiptEnum::Data(_) => 0,
    })
}

/// Calculates and returns total cost of all the receipts.
fn total_receipts_cost(
    transaction_costs: &RuntimeFeesConfig,
    current_protocol_version: ProtocolVersion,
    receipts: &[Receipt],
) -> Result<Balance, IntegerOverflowError> {
    receipts.iter().try_fold(0, |accumulator, receipt| {
        let cost = receipt_cost(transaction_costs, current_protocol_version, receipt)?;
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

/// Calculates and returns total costs of all the postponed receipts.
fn total_postponed_receipts_cost(
    state: &dyn TrieAccess,
    transaction_costs: &RuntimeFeesConfig,
    current_protocol_version: ProtocolVersion,
    receipt_ids: &HashSet<(AccountId, crate::CryptoHash)>,
) -> Result<Balance, RuntimeError> {
    receipt_ids.iter().try_fold(0, |total, item| {
        let (account_id, receipt_id) = item;
        let cost = match get_postponed_receipt(state, account_id, *receipt_id)? {
            None => return Ok(total),
            Some(receipt) => receipt_cost(transaction_costs, current_protocol_version, &receipt)?,
        };
        safe_add_balance(total, cost).map_err(|_| RuntimeError::UnexpectedIntegerOverflow)
    })
}

pub(crate) fn check_balance(
    transaction_costs: &RuntimeFeesConfig,
    final_state: &TrieUpdate,
    validator_accounts_update: &Option<ValidatorAccountsUpdate>,
    incoming_receipts: &[Receipt],
    transactions: &[SignedTransaction],
    outgoing_receipts: &[Receipt],
    stats: &ApplyStats,
    current_protocol_version: ProtocolVersion,
) -> Result<(), RuntimeError> {
    let initial_state = final_state.trie();

    // Delayed receipts
    let initial_delayed_receipt_indices: DelayedReceiptIndices =
        get(initial_state, &TrieKey::DelayedReceiptIndices)?.unwrap_or_default();
    let final_delayed_receipt_indices: DelayedReceiptIndices =
        get(final_state, &TrieKey::DelayedReceiptIndices)?.unwrap_or_default();

    // Previously delayed receipts that were processed this time.
    let processed_delayed_receipts = get_delayed_receipts(
        initial_state,
        initial_delayed_receipt_indices.first_index..final_delayed_receipt_indices.first_index,
    )?;
    // Receipts that were not processed this time and are delayed now.
    let new_delayed_receipts = get_delayed_receipts(
        final_state,
        initial_delayed_receipt_indices.next_available_index
            ..final_delayed_receipt_indices.next_available_index,
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

    let initial_accounts_balance = total_accounts_balance(initial_state, &all_accounts_ids)?;
    let final_accounts_balance = total_accounts_balance(final_state, &all_accounts_ids)?;
    // Receipts
    let receipts_cost = |receipts: &[Receipt]| -> Result<Balance, IntegerOverflowError> {
        total_receipts_cost(transaction_costs, current_protocol_version, receipts)
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
        .filter_map(|receipt| {
            let account_id = &receipt.receiver_id;
            match &receipt.receipt {
                ReceiptEnum::Action(_) => Some(Ok((account_id.clone(), receipt.receipt_id))),
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
                        Ok(Some(receipt_id)) => Some(Ok((account_id.clone(), receipt_id))),
                    }
                }
            }
        })
        .collect::<Result<HashSet<_>, StorageError>>()?;

    let initial_postponed_receipts_balance = total_postponed_receipts_cost(
        initial_state,
        transaction_costs,
        current_protocol_version,
        &all_potential_postponed_receipt_ids,
    )?;
    let final_postponed_receipts_balance = total_postponed_receipts_cost(
        final_state,
        transaction_costs,
        current_protocol_version,
        &all_potential_postponed_receipt_ids,
    )?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ApplyStats;
    use near_crypto::{InMemorySigner, KeyType};
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::receipt::ActionReceipt;
    use near_primitives::runtime::fees::RuntimeFeesConfig;
    use near_primitives::test_utils::account_new;
    use near_primitives::transaction::{Action, TransferAction};
    use near_primitives::types::{MerkleHash, StateChangeCause};
    use near_store::test_utils::create_tries;
    use near_store::{set_account, Trie};
    use testlib::runtime_utils::{alice_account, bob_account};

    use crate::near_primitives::shard_layout::ShardUId;
    use assert_matches::assert_matches;
    use near_primitives::version::PROTOCOL_VERSION;

    /// Initial balance used in tests.
    pub const TESTING_INIT_BALANCE: Balance = 1_000_000_000 * NEAR_BASE;

    /// One NEAR, divisible by 10^24.
    pub const NEAR_BASE: Balance = 1_000_000_000_000_000_000_000_000;

    #[test]
    fn test_check_balance_no_op() {
        let tries = create_tries();
        let root = MerkleHash::default();
        let final_state = tries.new_trie_update(ShardUId::single_shard(), root);
        let transaction_costs = RuntimeFeesConfig::test();
        check_balance(
            &transaction_costs,
            &final_state,
            &None,
            &[],
            &[],
            &[],
            &ApplyStats::default(),
            PROTOCOL_VERSION,
        )
        .unwrap();
    }

    #[test]
    fn test_check_balance_unaccounted_refund() {
        let tries = create_tries();
        let root = MerkleHash::default();
        let final_state = tries.new_trie_update(ShardUId::single_shard(), root);
        let transaction_costs = RuntimeFeesConfig::test();
        let err = check_balance(
            &transaction_costs,
            &final_state,
            &None,
            &[Receipt::new_balance_refund(&alice_account(), 1000)],
            &[],
            &[],
            &ApplyStats::default(),
            PROTOCOL_VERSION,
        )
        .unwrap_err();
        assert_matches!(err, RuntimeError::BalanceMismatchError(_));
    }

    fn prepare_state_change(
        set_initial_state: impl FnOnce(&mut TrieUpdate),
        set_final_state: impl FnOnce(&mut TrieUpdate),
    ) -> TrieUpdate {
        let tries = create_tries();
        let shard_uid = ShardUId::single_shard();

        // Commit initial state
        let root = {
            let mut trie_update = tries.new_trie_update(shard_uid, Trie::EMPTY_ROOT);
            set_initial_state(&mut trie_update);
            trie_update.commit(StateChangeCause::NotWritableToDisk);
            let trie_changes = trie_update.finalize().unwrap().1;
            let mut store_update = tries.store_update();
            let root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
            store_update.commit().unwrap();
            root
        };

        // Prepare final state
        {
            let mut trie_update = tries.new_trie_update(ShardUId::single_shard(), root);
            set_final_state(&mut trie_update);
            trie_update.commit(StateChangeCause::NotWritableToDisk);
            trie_update
        }
    }

    #[test]
    fn test_check_balance_refund() {
        let account_id = alice_account();

        let initial_balance = TESTING_INIT_BALANCE;
        let refund_balance = 1000;

        let final_state = prepare_state_change(
            |trie_update| {
                let initial_account = account_new(initial_balance, hash(&[]));
                set_account(trie_update, account_id.clone(), &initial_account);
            },
            |trie_update| {
                let final_account = account_new(initial_balance + refund_balance, hash(&[]));
                set_account(trie_update, account_id.clone(), &final_account);
            },
        );

        let transaction_costs = RuntimeFeesConfig::test();
        check_balance(
            &transaction_costs,
            &final_state,
            &None,
            &[Receipt::new_balance_refund(&account_id, refund_balance)],
            &[],
            &[],
            &ApplyStats::default(),
            PROTOCOL_VERSION,
        )
        .unwrap();
    }

    #[test]
    fn test_check_balance_tx_to_receipt() {
        let account_id = alice_account();

        let initial_balance = TESTING_INIT_BALANCE / 2;
        let deposit = 500_000_000;
        let gas_price = 100;
        let cfg = RuntimeFeesConfig::test();
        let exec_gas = cfg.fee(ActionCosts::new_action_receipt).exec_fee()
            + cfg.fee(ActionCosts::transfer).exec_fee();
        let send_gas = cfg.fee(ActionCosts::new_action_receipt).send_fee(false)
            + cfg.fee(ActionCosts::transfer).send_fee(false);
        let contract_reward = send_gas as u128 * *cfg.burnt_gas_reward.numer() as u128 * gas_price
            / (*cfg.burnt_gas_reward.denom() as u128);
        let total_validator_reward = send_gas as Balance * gas_price - contract_reward;

        let final_state = prepare_state_change(
            |trie_update| {
                let initial_account = account_new(initial_balance, hash(&[]));
                set_account(trie_update, account_id.clone(), &initial_account);
            },
            |trie_update| {
                let final_account = account_new(
                    initial_balance - (exec_gas + send_gas) as Balance * gas_price - deposit
                        + contract_reward,
                    hash(&[]),
                );
                set_account(trie_update, account_id.clone(), &final_account);
            },
        );

        let signer =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, account_id.as_ref());
        let tx = SignedTransaction::send_money(
            1,
            account_id,
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

        check_balance(
            &cfg,
            &final_state,
            &None,
            &[],
            &[tx],
            &[receipt],
            &ApplyStats {
                tx_burnt_amount: total_validator_reward,
                gas_deficit_amount: 0,
                other_burnt_amount: 0,
                slashed_burnt_amount: 0,
            },
            PROTOCOL_VERSION,
        )
        .unwrap();
    }

    #[test]
    fn test_total_balance_overflow_returns_unexpected_overflow() {
        let tries = create_tries();
        let root = MerkleHash::default();
        let alice_id = alice_account();
        let bob_id = bob_account();
        let gas_price = 100;
        let deposit = 1000;

        let mut initial_state = tries.new_trie_update(ShardUId::single_shard(), root);
        let alice = account_new(u128::MAX, hash(&[]));
        let bob = account_new(1u128, hash(&[]));

        set_account(&mut initial_state, alice_id.clone(), &alice);
        set_account(&mut initial_state, bob_id.clone(), &bob);
        initial_state.commit(StateChangeCause::NotWritableToDisk);

        let signer =
            InMemorySigner::from_seed(alice_id.clone(), KeyType::ED25519, alice_id.as_ref());

        let tx =
            SignedTransaction::send_money(0, alice_id, bob_id, &signer, 1, CryptoHash::default());

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

        let transaction_costs = RuntimeFeesConfig::test();
        assert_eq!(
            check_balance(
                &transaction_costs,
                &initial_state,
                &None,
                &[receipt],
                &[tx],
                &[],
                &ApplyStats::default(),
                PROTOCOL_VERSION,
            ),
            Err(RuntimeError::UnexpectedIntegerOverflow)
        );
    }
}

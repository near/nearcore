use crate::safe_add_balance_apply;

use crate::config::{
    safe_add_balance, safe_add_gas, safe_gas_to_balance, total_deposit, total_exec_fees,
    total_prepaid_gas,
};
use crate::{ApplyStats, DelayedReceiptIndices, ValidatorAccountsUpdate};
use near_primitives::errors::{
    BalanceMismatchError, IntegerOverflowError, RuntimeError, StorageError,
};
use near_primitives::receipt::{Receipt, ReceiptEnum};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance};
use near_primitives::utils::col::DELAYED_RECEIPT_INDICES;
use near_primitives::utils::{
    key_for_delayed_receipt, key_for_postponed_receipt_id, system_account,
};
use near_runtime_fees::RuntimeFeesConfig;
use near_store::{get, get_account, get_receipt, TrieUpdate};
use std::collections::HashSet;

// TODO: Check for balance overflows
// TODO: Fix StorageError for partial states when looking up something that doesn't exist.
pub(crate) fn check_balance(
    transaction_costs: &RuntimeFeesConfig,
    initial_state: &TrieUpdate,
    final_state: &TrieUpdate,
    validator_accounts_update: &Option<ValidatorAccountsUpdate>,
    prev_receipts: &[Receipt],
    transactions: &[SignedTransaction],
    new_receipts: &[Receipt],
    stats: &ApplyStats,
) -> Result<(), RuntimeError> {
    // Delayed receipts
    let initial_delayed_receipt_indices: DelayedReceiptIndices =
        get(&initial_state, DELAYED_RECEIPT_INDICES)?.unwrap_or_default();
    let final_delayed_receipt_indices: DelayedReceiptIndices =
        get(&final_state, DELAYED_RECEIPT_INDICES)?.unwrap_or_default();
    let get_delayed_receipts = |from_index, to_index, state| {
        (from_index..to_index)
            .map(|index| {
                get(state, &key_for_delayed_receipt(index))?.ok_or_else(|| {
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
        .chain(prev_receipts.iter().map(|r| r.receiver_id.clone()))
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
                    safe_add_balance(a.amount, a.locked)
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
                        total_exec_fees(transaction_costs, &action_receipt.actions)?,
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
    let incoming_receipts_balance = receipts_cost(prev_receipts)?;
    let outgoing_receipts_balance = receipts_cost(new_receipts)?;
    let processed_delayed_receipts_balance = receipts_cost(&processed_delayed_receipts)?;
    let new_delayed_receipts_balance = receipts_cost(&new_delayed_receipts)?;
    // Postponed actions receipts. The receipts can be postponed and stored with the receiver's
    // account ID when the input data is not received yet.
    // We calculate all potential receipts IDs that might be postponed initially or after the
    // execution.
    let all_potential_postponed_receipt_ids = prev_receipts
        .iter()
        .chain(processed_delayed_receipts.iter())
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
        .collect::<HashSet<_>>();

    let total_postponed_receipts_cost = |state| -> Result<Balance, RuntimeError> {
        Ok(all_potential_postponed_receipt_ids
            .iter()
            .map(|(account_id, receipt_id)| {
                Ok(get_receipt(state, account_id, &receipt_id)?
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
        stats.total_rent_paid,
        stats.total_validator_reward,
        stats.total_balance_burnt,
        stats.total_balance_slashed
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
            total_rent_paid: stats.total_rent_paid,
            total_validator_reward: stats.total_validator_reward,
            total_balance_burnt: stats.total_balance_burnt,
            total_balance_slashed: stats.total_balance_slashed,
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
    use near_primitives::account::Account;
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::receipt::ActionReceipt;
    use near_primitives::transaction::{Action, TransferAction};
    use near_primitives::types::MerkleHash;
    use near_runtime_fees::RuntimeFeesConfig;
    use near_store::test_utils::create_trie;
    use near_store::{set_account, TrieUpdate};
    use testlib::runtime_utils::{alice_account, bob_account};

    use assert_matches::assert_matches;

    /// Initial balance used in tests.
    pub const TESTING_INIT_BALANCE: Balance = 1_000_000_000 * NEAR_BASE;

    /// Validator's stake used in tests.
    pub const TESTING_INIT_STAKE: Balance = 50_000_000 * NEAR_BASE;

    /// One NEAR, divisible by 10^24.
    pub const NEAR_BASE: Balance = 1_000_000_000_000_000_000_000_000;

    #[test]
    fn test_check_balance_no_op() {
        let trie = create_trie();
        let root = MerkleHash::default();
        let initial_state = TrieUpdate::new(trie.clone(), root);
        let final_state = TrieUpdate::new(trie.clone(), root);
        let transaction_costs = RuntimeFeesConfig::default();
        check_balance(
            &transaction_costs,
            &initial_state,
            &final_state,
            &None,
            &[],
            &[],
            &[],
            &ApplyStats::default(),
        )
        .unwrap();
    }

    #[test]
    fn test_check_balance_unaccounted_refund() {
        let trie = create_trie();
        let root = MerkleHash::default();
        let initial_state = TrieUpdate::new(trie.clone(), root);
        let final_state = TrieUpdate::new(trie.clone(), root);
        let transaction_costs = RuntimeFeesConfig::default();
        let err = check_balance(
            &transaction_costs,
            &initial_state,
            &final_state,
            &None,
            &[Receipt::new_refund(&alice_account(), 1000)],
            &[],
            &[],
            &ApplyStats::default(),
        )
        .unwrap_err();
        assert_matches!(err, RuntimeError::BalanceMismatch(_));
    }

    #[test]
    fn test_check_balance_refund() {
        let trie = create_trie();
        let root = MerkleHash::default();
        let account_id = alice_account();

        let initial_balance = TESTING_INIT_BALANCE;
        let refund_balance = 1000;

        let mut initial_state = TrieUpdate::new(trie.clone(), root);
        let initial_account = Account::new(initial_balance, hash(&[]), 0);
        set_account(&mut initial_state, &account_id, &initial_account);
        initial_state.commit();

        let mut final_state = TrieUpdate::new(trie.clone(), root);
        let final_account = Account::new(initial_balance + refund_balance, hash(&[]), 0);
        set_account(&mut final_state, &account_id, &final_account);
        final_state.commit();

        let transaction_costs = RuntimeFeesConfig::default();
        check_balance(
            &transaction_costs,
            &initial_state,
            &final_state,
            &None,
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

        let initial_balance = TESTING_INIT_BALANCE / 2;
        let deposit = 500_000_000;
        let gas_price = 100;
        let cfg = RuntimeFeesConfig::default();
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

        check_balance(
            &cfg,
            &initial_state,
            &final_state,
            &None,
            &[],
            &[tx],
            &[receipt],
            &ApplyStats {
                total_rent_paid: 0,
                total_validator_reward,
                total_balance_burnt: 0,
                total_balance_slashed: 0,
            },
        )
        .unwrap();
    }

    #[test]
    fn test_total_balance_overflow_returns_unexpected_overflow() {
        let trie = create_trie();
        let root = MerkleHash::default();
        let alice_id = alice_account();
        let bob_id = bob_account();
        let gas_price = 100;
        let deposit = 1000;

        let mut initial_state = TrieUpdate::new(trie.clone(), root);
        let alice = Account::new(std::u128::MAX, hash(&[]), 0);
        let bob = Account::new(1u128, hash(&[]), 0);

        set_account(&mut initial_state, &alice_id, &alice);
        set_account(&mut initial_state, &bob_id, &bob);
        initial_state.commit();

        let signer = InMemorySigner::from_seed(&alice_id, KeyType::ED25519, &alice_id);

        let tx = SignedTransaction::send_money(
            0,
            alice_id.clone(),
            bob_id.clone(),
            &signer,
            1,
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

        let transaction_costs = RuntimeFeesConfig::default();
        assert_eq!(
            check_balance(
                &transaction_costs,
                &initial_state,
                &initial_state,
                &None,
                &[receipt],
                &[tx],
                &[],
                &ApplyStats::default(),
            ),
            Err(RuntimeError::UnexpectedIntegerOverflow)
        );
    }
}

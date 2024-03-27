use crate::config::{
    safe_add_balance, safe_add_gas, safe_gas_to_balance, total_deposit, total_prepaid_exec_fees,
    total_prepaid_gas, total_prepaid_send_fees,
};
use crate::safe_add_balance_apply;
use crate::{ApplyStats, DelayedReceiptIndices, ValidatorAccountsUpdate};
use near_parameters::{ActionCosts, RuntimeConfig};
use near_primitives::errors::{
    BalanceMismatchError, IntegerOverflowError, RuntimeError, StorageError,
};
use near_primitives::receipt::{Receipt, ReceiptEnum};
use near_primitives::transaction::SignedTransaction;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{AccountId, Balance};
use near_store::{
    get, get_account, get_postponed_receipt, get_promise_yield_receipt, TrieAccess, TrieUpdate,
};
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
    config: &RuntimeConfig,
    receipt: &Receipt,
) -> Result<Balance, IntegerOverflowError> {
    Ok(match &receipt.receipt {
        ReceiptEnum::Action(action_receipt) | ReceiptEnum::PromiseYield(action_receipt) => {
            let mut total_cost = total_deposit(&action_receipt.actions)?;
            if !receipt.predecessor_id.is_system() {
                let mut total_gas = safe_add_gas(
                    config.fees.fee(ActionCosts::new_action_receipt).exec_fee(),
                    total_prepaid_exec_fees(config, &action_receipt.actions, &receipt.receiver_id)?,
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
        ReceiptEnum::Data(_) | ReceiptEnum::PromiseResume(_) => 0,
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
        let (amount, locked, nonrefundable) = match get_account(state, account_id)? {
            None => return Ok(accumulator),
            Some(account) => (account.amount(), account.locked(), account.nonrefundable()),
        };
        Ok(safe_add_balance_apply!(accumulator, amount, locked, nonrefundable))
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

        safe_add_balance(total, cost).map_err(|_| RuntimeError::UnexpectedIntegerOverflow)
    })
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
    yield_timeout_receipts: &[Receipt],
    transactions: &[SignedTransaction],
    outgoing_receipts: &[Receipt],
    stats: &ApplyStats,
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
        .chain(yield_timeout_receipts.iter().map(|r| r.receiver_id.clone()))
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
        total_receipts_cost(config, receipts)
    };
    let incoming_receipts_balance =
        receipts_cost(incoming_receipts)? + receipts_cost(yield_timeout_receipts)?;
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
        .chain(yield_timeout_receipts.iter())
        .filter_map(|receipt| {
            let account_id = &receipt.receiver_id;
            match &receipt.receipt {
                ReceiptEnum::Action(_) => {
                    Some(Ok((PostponedReceiptType::Action, account_id.clone(), receipt.receipt_id)))
                }
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
            }
        })
        .collect::<Result<HashSet<_>, StorageError>>()?;

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
    use near_primitives::test_utils::account_new;
    use near_primitives::transaction::{Action, TransferAction};
    use near_primitives::types::{MerkleHash, StateChangeCause};
    use near_store::test_utils::TestTriesBuilder;
    use near_store::{set_account, Trie};
    use testlib::runtime_utils::{alice_account, bob_account};

    use crate::near_primitives::shard_layout::ShardUId;
    use assert_matches::assert_matches;

    /// Initial balance used in tests.
    pub const TESTING_INIT_BALANCE: Balance = 1_000_000_000 * NEAR_BASE;

    /// One NEAR, divisible by 10^24.
    pub const NEAR_BASE: Balance = 1_000_000_000_000_000_000_000_000;

    #[test]
    fn test_check_balance_no_op() {
        let tries = TestTriesBuilder::new().build();
        let root = MerkleHash::default();
        let final_state = tries.new_trie_update(ShardUId::single_shard(), root);
        check_balance(
            &RuntimeConfig::test(),
            &final_state,
            &None,
            &[],
            &[],
            &[],
            &[],
            &ApplyStats::default(),
        )
        .unwrap();
    }

    #[test]
    fn test_check_balance_unaccounted_refund() {
        let tries = TestTriesBuilder::new().build();
        let root = MerkleHash::default();
        let final_state = tries.new_trie_update(ShardUId::single_shard(), root);
        let err = check_balance(
            &RuntimeConfig::test(),
            &final_state,
            &None,
            &[Receipt::new_balance_refund(&alice_account(), 1000)],
            &[],
            &[],
            &[],
            &ApplyStats::default(),
        )
        .unwrap_err();
        assert_matches!(err, RuntimeError::BalanceMismatchError(_));
    }

    fn prepare_state_change(
        set_initial_state: impl FnOnce(&mut TrieUpdate),
        set_final_state: impl FnOnce(&mut TrieUpdate),
    ) -> TrieUpdate {
        let tries = TestTriesBuilder::new().build();
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

        check_balance(
            &RuntimeConfig::test(),
            &final_state,
            &None,
            &[Receipt::new_balance_refund(&account_id, refund_balance)],
            &[],
            &[],
            &[],
            &ApplyStats::default(),
        )
        .unwrap();
    }

    #[test]
    fn test_check_balance_tx_to_receipt() {
        let account_id = alice_account();

        let initial_balance = TESTING_INIT_BALANCE / 2;
        let deposit = 500_000_000;
        let gas_price = 100;
        let cfg = RuntimeConfig::test();
        let fees = &cfg.fees;
        let exec_gas = fees.fee(ActionCosts::new_action_receipt).exec_fee()
            + fees.fee(ActionCosts::transfer).exec_fee();
        let send_gas = fees.fee(ActionCosts::new_action_receipt).send_fee(false)
            + fees.fee(ActionCosts::transfer).send_fee(false);
        let contract_reward = send_gas as u128 * *fees.burnt_gas_reward.numer() as u128 * gas_price
            / (*fees.burnt_gas_reward.denom() as u128);
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
            &[],
            &[tx],
            &[receipt],
            &ApplyStats {
                tx_burnt_amount: total_validator_reward,
                gas_deficit_amount: 0,
                other_burnt_amount: 0,
                slashed_burnt_amount: 0,
            },
        )
        .unwrap();
    }

    /// This tests shows how overflow (which we do not expect) would be handled on a transfer.
    #[test]
    fn test_total_balance_overflow_returns_unexpected_overflow() {
        let tries = TestTriesBuilder::new().build();
        let root = MerkleHash::default();
        let alice_id = alice_account();
        let bob_id = bob_account();
        let gas_price = 100;
        let deposit = 1000;

        let mut initial_state = tries.new_trie_update(ShardUId::single_shard(), root);
        // We use `u128::MAX - 1`, because `u128::MAX` is used as a sentinel value for accounts version 2 or higher.
        // See NEP-491 for more details: https://github.com/near/NEPs/pull/491.
        let alice = account_new(u128::MAX - 1, hash(&[]));
        let bob = account_new(2u128, hash(&[]));

        set_account(&mut initial_state, alice_id.clone(), &alice);
        set_account(&mut initial_state, bob_id.clone(), &bob);
        initial_state.commit(StateChangeCause::NotWritableToDisk);

        let signer =
            InMemorySigner::from_seed(alice_id.clone(), KeyType::ED25519, alice_id.as_ref());

        // Sending 2 yoctoNEAR, so that we have an overflow when adding to alice's balance.
        let tx =
            SignedTransaction::send_money(0, alice_id, bob_id, &signer, 2, CryptoHash::default());

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

        assert_eq!(
            check_balance(
                &RuntimeConfig::test(),
                &initial_state,
                &None,
                &[receipt],
                &[],
                &[tx],
                &[],
                &ApplyStats::default(),
            ),
            Err(RuntimeError::UnexpectedIntegerOverflow)
        );
    }

    /// This tests shows what would happen if the total balance becomes u128::MAX
    /// which is also the sentinel value use to distinguish between accounts version 1 and 2 or higher
    /// See NEP-491 for more details: https://github.com/near/NEPs/pull/491.
    #[test]
    fn test_total_balance_u128_max() {
        let tries = TestTriesBuilder::new().build();
        let root = MerkleHash::default();
        let alice_id = alice_account();
        let bob_id = bob_account();
        let gas_price = 100;
        let deposit = 1000;

        let mut initial_state = tries.new_trie_update(ShardUId::single_shard(), root);
        let alice = account_new(u128::MAX - 1, hash(&[]));
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

        // Alice's balance becomes u128::MAX, which causes it is interpreted as
        // the Alice's account version to be 2 or higher, instead of being interpreted
        // as Alice's balance. Another field is then interpreted as the balance which causes
        // `BalanceMismatchError`.
        assert_matches!(
            check_balance(
                &RuntimeConfig::test(),
                &initial_state,
                &None,
                &[receipt],
                &[],
                &[tx],
                &[],
                &ApplyStats::default(),
            ),
            Err(RuntimeError::BalanceMismatchError { .. })
        );
    }
}

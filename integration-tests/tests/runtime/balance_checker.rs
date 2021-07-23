mod tests {
    use near_crypto::{InMemorySigner, KeyType};
    use near_primitives::errors::RuntimeError;
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::receipt::{ActionReceipt, Receipt, ReceiptEnum};
    use near_primitives::runtime::fees::RuntimeFeesConfig;
    use near_primitives::test_utils::account_new;
    use near_primitives::transaction::{Action, SignedTransaction, TransferAction};
    use near_primitives::types::{Balance, MerkleHash, StateChangeCause};
    use near_primitives::version::PROTOCOL_VERSION;
    use near_store::set_account;
    use near_store::test_utils::create_tries;
    use node_runtime::balance_checker::check_balance;
    use node_runtime::ApplyStats;
    use testlib::runtime_utils::{alice_account, bob_account};

    use assert_matches::assert_matches;

    /// Initial balance used in tests.
    pub const TESTING_INIT_BALANCE: Balance = 1_000_000_000 * NEAR_BASE;

    /// One NEAR, divisible by 10^24.
    pub const NEAR_BASE: Balance = 1_000_000_000_000_000_000_000_000;

    #[test]
    fn test_check_balance_no_op() {
        let tries = create_tries();
        let root = MerkleHash::default();
        let initial_state = tries.new_trie_update(0, root);
        let final_state = tries.new_trie_update(0, root);
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
            PROTOCOL_VERSION,
        )
        .unwrap();
    }

    #[test]
    fn test_check_balance_unaccounted_refund() {
        let tries = create_tries();
        let root = MerkleHash::default();
        let initial_state = tries.new_trie_update(0, root);
        let final_state = tries.new_trie_update(0, root);
        let transaction_costs = RuntimeFeesConfig::default();
        let err = check_balance(
            &transaction_costs,
            &initial_state,
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

    #[test]
    fn test_check_balance_refund() {
        let tries = create_tries();
        let root = MerkleHash::default();
        let account_id = alice_account();

        let initial_balance = TESTING_INIT_BALANCE;
        let refund_balance = 1000;

        let mut initial_state = tries.new_trie_update(0, root);
        let initial_account = account_new(initial_balance, hash(&[]));
        set_account(&mut initial_state, account_id.clone(), &initial_account);
        initial_state.commit(StateChangeCause::NotWritableToDisk);

        let mut final_state = tries.new_trie_update(0, root);
        let final_account = account_new(initial_balance + refund_balance, hash(&[]));
        set_account(&mut final_state, account_id.clone(), &final_account);
        final_state.commit(StateChangeCause::NotWritableToDisk);

        let transaction_costs = RuntimeFeesConfig::default();
        check_balance(
            &transaction_costs,
            &initial_state,
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
        let tries = create_tries();
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
        let contract_reward = send_gas as u128 * *cfg.burnt_gas_reward.numer() as u128 * gas_price
            / (*cfg.burnt_gas_reward.denom() as u128);
        let total_validator_reward = send_gas as Balance * gas_price - contract_reward;
        let mut initial_state = tries.new_trie_update(0, root);
        let initial_account = account_new(initial_balance, hash(&[]));
        set_account(&mut initial_state, account_id.clone(), &initial_account);
        initial_state.commit(StateChangeCause::NotWritableToDisk);

        let mut final_state = tries.new_trie_update(0, root);
        let final_account = account_new(
            initial_balance - (exec_gas + send_gas) as Balance * gas_price - deposit
                + contract_reward,
            hash(&[]),
        );
        set_account(&mut final_state, account_id.clone(), &final_account);
        final_state.commit(StateChangeCause::NotWritableToDisk);

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

        let mut initial_state = tries.new_trie_update(0, root);
        let alice = account_new(std::u128::MAX, hash(&[]));
        let bob = account_new(1u128, hash(&[]));

        set_account(&mut initial_state, alice_id.clone(), &alice);
        set_account(&mut initial_state, bob_id.clone(), &bob);
        initial_state.commit(StateChangeCause::NotWritableToDisk);

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
                PROTOCOL_VERSION,
            ),
            Err(RuntimeError::UnexpectedIntegerOverflow)
        );
    }
}

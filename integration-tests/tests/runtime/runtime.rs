mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use near_crypto::{InMemorySigner, KeyType, PublicKey, Signer};
    use near_primitives::account::AccessKey;
    use near_primitives::contract::ContractCode;
    use near_primitives::errors::{ReceiptValidationError, RuntimeError, StorageError};
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::profile::ProfileData;
    use near_primitives::receipt::{ActionReceipt, DelayedReceiptIndices, Receipt, ReceiptEnum};
    use near_primitives::runtime::apply_state::ApplyState;
    use near_primitives::runtime::config::RuntimeConfig;
    use near_primitives::runtime::migration_data::{MigrationData, MigrationFlags};
    use near_primitives::test_utils::{account_new, MockEpochInfoProvider};
    use near_primitives::transaction::SignedTransaction;
    use near_primitives::transaction::{
        Action, AddKeyAction, DeleteKeyAction, DeployContractAction, FunctionCallAction,
        TransferAction,
    };
    use near_primitives::trie_key::TrieKey;
    use near_primitives::types::{
        AccountId, Balance, EpochInfoProvider, Gas, MerkleHash, StateChangeCause,
    };
    use near_primitives::utils::create_receipt_id_from_transaction;
    use near_primitives::version::PROTOCOL_VERSION;
    use near_store::test_utils::create_tries;
    use near_store::{
        get_account, set, set_access_key, set_account, ShardTries, StoreCompiledContractCache,
    };
    use near_vm_runner::{get_contract_cache_key, VMKind};
    use node_runtime::config::{safe_add_gas, total_prepaid_exec_fees};
    use node_runtime::{Runtime, ValidatorAccountsUpdate};
    use testlib::runtime_utils::{alice_account, bob_account};

    const GAS_PRICE: Balance = 5000;

    fn to_yocto(near: Balance) -> Balance {
        near * 10u128.pow(24)
    }

    fn create_receipts_with_actions(
        account_id: AccountId,
        signer: Arc<InMemorySigner>,
        actions: Vec<Action>,
    ) -> Vec<Receipt> {
        vec![Receipt {
            predecessor_id: account_id.clone(),
            receiver_id: account_id.clone(),
            receipt_id: CryptoHash::default(),
            receipt: ReceiptEnum::Action(ActionReceipt {
                signer_id: account_id,
                signer_public_key: signer.public_key(),
                gas_price: GAS_PRICE,
                output_data_receivers: vec![],
                input_data_ids: vec![],
                actions,
            }),
        }]
    }

    #[test]
    fn test_get_and_set_accounts() {
        let tries = create_tries();
        let mut state_update = tries.new_trie_update(0, MerkleHash::default());
        let test_account = account_new(to_yocto(10), hash(&[]));
        let account_id = bob_account();
        set_account(&mut state_update, account_id.clone(), &test_account);
        let get_res = get_account(&state_update, &account_id).unwrap().unwrap();
        assert_eq!(test_account, get_res);
    }

    #[test]
    fn test_get_account_from_trie() {
        let tries = create_tries();
        let root = MerkleHash::default();
        let mut state_update = tries.new_trie_update(0, root);
        let test_account = account_new(to_yocto(10), hash(&[]));
        let account_id = bob_account();
        set_account(&mut state_update, account_id.clone(), &test_account);
        state_update.commit(StateChangeCause::InitialState);
        let trie_changes = state_update.finalize().unwrap().0;
        let (store_update, new_root) = tries.apply_all(&trie_changes, 0).unwrap();
        store_update.commit().unwrap();
        let new_state_update = tries.new_trie_update(0, new_root);
        let get_res = get_account(&new_state_update, &account_id).unwrap().unwrap();
        assert_eq!(test_account, get_res);
    }

    /***************/
    /* Apply tests */
    /***************/

    fn setup_runtime(
        initial_balance: Balance,
        initial_locked: Balance,
        gas_limit: Gas,
    ) -> (Runtime, ShardTries, CryptoHash, ApplyState, Arc<InMemorySigner>, impl EpochInfoProvider)
    {
        let tries = create_tries();
        let root = MerkleHash::default();
        let runtime = Runtime::new();
        let account_id = alice_account();
        let signer =
            Arc::new(InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id));

        let mut initial_state = tries.new_trie_update(0, root);
        let mut initial_account = account_new(initial_balance, hash(&[]));
        // For the account and a full access key
        initial_account.set_storage_usage(182);
        initial_account.set_locked(initial_locked);
        set_account(&mut initial_state, account_id.clone(), &initial_account);
        set_access_key(
            &mut initial_state,
            account_id.clone(),
            signer.public_key(),
            &AccessKey::full_access(),
        );
        initial_state.commit(StateChangeCause::InitialState);
        let trie_changes = initial_state.finalize().unwrap().0;
        let (store_update, root) = tries.apply_all(&trie_changes, 0).unwrap();
        store_update.commit().unwrap();

        let apply_state = ApplyState {
            block_index: 1,
            prev_block_hash: Default::default(),
            block_hash: Default::default(),
            epoch_id: Default::default(),
            epoch_height: 0,
            gas_price: GAS_PRICE,
            block_timestamp: 100,
            gas_limit: Some(gas_limit),
            random_seed: Default::default(),
            current_protocol_version: PROTOCOL_VERSION,
            config: Arc::new(RuntimeConfig::default()),
            cache: Some(Arc::new(StoreCompiledContractCache { store: tries.get_store() })),
            is_new_chunk: true,
            #[cfg(feature = "protocol_feature_evm")]
            evm_chain_id: near_chain_configs::TESTNET_EVM_CHAIN_ID,
            profile: ProfileData::new(),
            migration_data: Arc::new(MigrationData::default()),
            migration_flags: MigrationFlags::default(),
        };

        (runtime, tries, root, apply_state, signer, MockEpochInfoProvider::default())
    }

    #[test]
    fn test_apply_no_op() {
        let (runtime, tries, root, apply_state, _, epoch_info_provider) =
            setup_runtime(to_yocto(1_000_000), 0, 10u64.pow(15));
        runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &[],
                &[],
                &epoch_info_provider,
                None,
            )
            .unwrap();
    }

    #[test]
    fn test_apply_check_balance_validation_rewards() {
        let initial_locked = to_yocto(500_000);
        let reward = to_yocto(10_000_000);
        let small_refund = to_yocto(500);
        let (runtime, tries, root, apply_state, _, epoch_info_provider) =
            setup_runtime(to_yocto(1_000_000), initial_locked, 10u64.pow(15));

        let validator_accounts_update = ValidatorAccountsUpdate {
            stake_info: vec![(alice_account(), initial_locked)].into_iter().collect(),
            validator_rewards: vec![(alice_account(), reward)].into_iter().collect(),
            last_proposals: Default::default(),
            protocol_treasury_account_id: None,
            slashing_info: HashMap::default(),
        };

        runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &Some(validator_accounts_update),
                &apply_state,
                &[Receipt::new_balance_refund(&alice_account(), small_refund)],
                &[],
                &epoch_info_provider,
                None,
            )
            .unwrap();
    }

    #[test]
    fn test_apply_refund_receipts() {
        let initial_balance = to_yocto(1_000_000);
        let initial_locked = to_yocto(500_000);
        let small_transfer = to_yocto(10_000);
        let gas_limit = 1;
        let (runtime, tries, mut root, apply_state, _, epoch_info_provider) =
            setup_runtime(initial_balance, initial_locked, gas_limit);

        let n = 10;
        let receipts = generate_refund_receipts(small_transfer, n);

        // Checking n receipts delayed
        for i in 1..=n + 3 {
            let prev_receipts: &[Receipt] = if i == 1 { &receipts } else { &[] };
            let apply_result = runtime
                .apply(
                    tries.get_trie_for_shard(0),
                    root,
                    &None,
                    &apply_state,
                    prev_receipts,
                    &[],
                    &epoch_info_provider,
                    None,
                )
                .unwrap();
            let (store_update, new_root) = tries.apply_all(&apply_result.trie_changes, 0).unwrap();
            root = new_root;
            store_update.commit().unwrap();
            let state = tries.new_trie_update(0, root);
            let account = get_account(&state, &alice_account()).unwrap().unwrap();
            let capped_i = std::cmp::min(i, n);
            assert_eq!(
                account.amount(),
                initial_balance
                    + small_transfer * Balance::from(capped_i)
                    + Balance::from(capped_i * (capped_i - 1) / 2)
            );
        }
    }

    #[test]
    fn test_apply_delayed_receipts_feed_all_at_once() {
        let initial_balance = to_yocto(1_000_000);
        let initial_locked = to_yocto(500_000);
        let small_transfer = to_yocto(10_000);
        let gas_limit = 1;
        let (runtime, tries, mut root, apply_state, _, epoch_info_provider) =
            setup_runtime(initial_balance, initial_locked, gas_limit);

        let n = 10;
        let receipts = generate_receipts(small_transfer, n);

        // Checking n receipts delayed by 1 + 3 extra
        for i in 1..=n + 3 {
            let prev_receipts: &[Receipt] = if i == 1 { &receipts } else { &[] };
            let apply_result = runtime
                .apply(
                    tries.get_trie_for_shard(0),
                    root,
                    &None,
                    &apply_state,
                    prev_receipts,
                    &[],
                    &epoch_info_provider,
                    None,
                )
                .unwrap();
            let (store_update, new_root) = tries.apply_all(&apply_result.trie_changes, 0).unwrap();
            root = new_root;
            store_update.commit().unwrap();
            let state = tries.new_trie_update(0, root);
            let account = get_account(&state, &alice_account()).unwrap().unwrap();
            let capped_i = std::cmp::min(i, n);
            assert_eq!(
                account.amount(),
                initial_balance
                    + small_transfer * Balance::from(capped_i)
                    + Balance::from(capped_i * (capped_i - 1) / 2)
            );
        }
    }

    #[test]
    fn test_apply_delayed_receipts_add_more_using_chunks() {
        let initial_balance = to_yocto(1_000_000);
        let initial_locked = to_yocto(500_000);
        let small_transfer = to_yocto(10_000);
        let (runtime, tries, mut root, mut apply_state, _, epoch_info_provider) =
            setup_runtime(initial_balance, initial_locked, 1);

        let receipt_gas_cost = apply_state
            .config
            .transaction_costs
            .action_receipt_creation_config
            .exec_fee()
            + apply_state.config.transaction_costs.action_creation_config.transfer_cost.exec_fee();
        apply_state.gas_limit = Some(receipt_gas_cost * 3);

        let n = 40;
        let receipts = generate_receipts(small_transfer, n);
        let mut receipt_chunks = receipts.chunks_exact(4);

        // Every time we'll process 3 receipts, so we need n / 3 rounded up. Then we do 3 extra.
        for i in 1..=n / 3 + 3 {
            let prev_receipts: &[Receipt] = receipt_chunks.next().unwrap_or_default();
            let apply_result = runtime
                .apply(
                    tries.get_trie_for_shard(0),
                    root,
                    &None,
                    &apply_state,
                    prev_receipts,
                    &[],
                    &epoch_info_provider,
                    None,
                )
                .unwrap();
            let (store_update, new_root) = tries.apply_all(&apply_result.trie_changes, 0).unwrap();
            root = new_root;
            store_update.commit().unwrap();
            let state = tries.new_trie_update(0, root);
            let account = get_account(&state, &alice_account()).unwrap().unwrap();
            let capped_i = std::cmp::min(i * 3, n);
            assert_eq!(
                account.amount(),
                initial_balance
                    + small_transfer * Balance::from(capped_i)
                    + Balance::from(capped_i * (capped_i - 1) / 2)
            );
        }
    }

    #[test]
    fn test_apply_delayed_receipts_adjustable_gas_limit() {
        let initial_balance = to_yocto(1_000_000);
        let initial_locked = to_yocto(500_000);
        let small_transfer = to_yocto(10_000);
        let (runtime, tries, mut root, mut apply_state, _, epoch_info_provider) =
            setup_runtime(initial_balance, initial_locked, 1);

        let receipt_gas_cost = apply_state
            .config
            .transaction_costs
            .action_receipt_creation_config
            .exec_fee()
            + apply_state.config.transaction_costs.action_creation_config.transfer_cost.exec_fee();

        let n = 120;
        let receipts = generate_receipts(small_transfer, n);
        let mut receipt_chunks = receipts.chunks_exact(4);

        let mut num_receipts_given = 0;
        let mut num_receipts_processed = 0;
        let mut num_receipts_per_block = 1;
        // Test adjusts gas limit based on the number of receipt given and number of receipts processed.
        while num_receipts_processed < n {
            if num_receipts_given > num_receipts_processed {
                num_receipts_per_block += 1;
            } else if num_receipts_per_block > 1 {
                num_receipts_per_block -= 1;
            }
            apply_state.gas_limit = Some(num_receipts_per_block * receipt_gas_cost);
            let prev_receipts: &[Receipt] = receipt_chunks.next().unwrap_or_default();
            num_receipts_given += prev_receipts.len() as u64;
            let apply_result = runtime
                .apply(
                    tries.get_trie_for_shard(0),
                    root,
                    &None,
                    &apply_state,
                    prev_receipts,
                    &[],
                    &epoch_info_provider,
                    None,
                )
                .unwrap();
            let (store_update, new_root) = tries.apply_all(&apply_result.trie_changes, 0).unwrap();
            root = new_root;
            store_update.commit().unwrap();
            let state = tries.new_trie_update(0, root);
            num_receipts_processed += apply_result.outcomes.len() as u64;
            let account = get_account(&state, &alice_account()).unwrap().unwrap();
            assert_eq!(
                account.amount(),
                initial_balance
                    + small_transfer * Balance::from(num_receipts_processed)
                    + Balance::from(num_receipts_processed * (num_receipts_processed - 1) / 2)
            );
            println!(
                "{} processed out of {} given. With limit {} receipts per block",
                num_receipts_processed, num_receipts_given, num_receipts_per_block
            );
        }
    }

    fn generate_receipts(small_transfer: u128, n: u64) -> Vec<Receipt> {
        let mut receipt_id = CryptoHash::default();
        (0..n)
            .map(|i| {
                receipt_id = hash(receipt_id.as_ref());
                Receipt {
                    predecessor_id: bob_account(),
                    receiver_id: alice_account(),
                    receipt_id,
                    receipt: ReceiptEnum::Action(ActionReceipt {
                        signer_id: bob_account(),
                        signer_public_key: PublicKey::empty(KeyType::ED25519),
                        gas_price: GAS_PRICE,
                        output_data_receivers: vec![],
                        input_data_ids: vec![],
                        actions: vec![Action::Transfer(TransferAction {
                            deposit: small_transfer + Balance::from(i),
                        })],
                    }),
                }
            })
            .collect()
    }

    fn generate_refund_receipts(small_transfer: u128, n: u64) -> Vec<Receipt> {
        let mut receipt_id = CryptoHash::default();
        (0..n)
            .map(|i| {
                receipt_id = hash(receipt_id.as_ref());
                Receipt::new_balance_refund(&alice_account(), small_transfer + Balance::from(i))
            })
            .collect()
    }

    #[test]
    fn test_apply_delayed_receipts_local_tx() {
        let initial_balance = to_yocto(1_000_000);
        let initial_locked = to_yocto(500_000);
        let small_transfer = to_yocto(10_000);
        let (runtime, tries, root, mut apply_state, signer, epoch_info_provider) =
            setup_runtime(initial_balance, initial_locked, 1);

        let receipt_exec_gas_fee = 1000;
        let mut free_config = RuntimeConfig::free();
        free_config.transaction_costs.action_receipt_creation_config.execution =
            receipt_exec_gas_fee;
        apply_state.config = Arc::new(free_config);
        // This allows us to execute 3 receipts per apply.
        apply_state.gas_limit = Some(receipt_exec_gas_fee * 3);

        let num_receipts = 6;
        let receipts = generate_receipts(small_transfer, num_receipts);

        let num_transactions = 9;
        let local_transactions = (0..num_transactions)
            .map(|i| {
                SignedTransaction::send_money(
                    i + 1,
                    alice_account(),
                    alice_account(),
                    &*signer,
                    small_transfer,
                    CryptoHash::default(),
                )
            })
            .collect::<Vec<_>>();

        // STEP #1. Pass 4 new local transactions + 2 receipts.
        // We can process only 3 local TX receipts TX#0, TX#1, TX#2.
        // TX#3 receipt and R#0, R#1 are delayed.
        // The new delayed queue is TX#3, R#0, R#1.
        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &receipts[0..2],
                &local_transactions[0..4],
                &epoch_info_provider,
                None,
            )
            .unwrap();
        let (store_update, root) = tries.apply_all(&apply_result.trie_changes, 0).unwrap();
        store_update.commit().unwrap();

        assert_eq!(
            apply_result.outcomes.iter().map(|o| o.id).collect::<Vec<_>>(),
            vec![
                local_transactions[0].get_hash(), // tx 0
                local_transactions[1].get_hash(), // tx 1
                local_transactions[2].get_hash(), // tx 2
                local_transactions[3].get_hash(), // tx 3 - the TX is processed, but the receipt is delayed
                create_receipt_id_from_transaction(
                    PROTOCOL_VERSION,
                    &local_transactions[0],
                    &apply_state.prev_block_hash,
                    &apply_state.block_hash
                ), // receipt for tx 0
                create_receipt_id_from_transaction(
                    PROTOCOL_VERSION,
                    &local_transactions[1],
                    &apply_state.prev_block_hash,
                    &apply_state.block_hash
                ), // receipt for tx 1
                create_receipt_id_from_transaction(
                    PROTOCOL_VERSION,
                    &local_transactions[2],
                    &apply_state.prev_block_hash,
                    &apply_state.block_hash
                ), // receipt for tx 2
            ],
            "STEP #1 failed",
        );

        // STEP #2. Pass 1 new local transaction (TX#4) + 1 receipts R#2.
        // We process 1 local receipts for TX#4, then delayed TX#3 receipt and then receipt R#0.
        // R#2 is added to delayed queue.
        // The new delayed queue is R#1, R#2
        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &receipts[2..3],
                &local_transactions[4..5],
                &epoch_info_provider,
                None,
            )
            .unwrap();
        let (store_update, root) = tries.apply_all(&apply_result.trie_changes, 0).unwrap();
        store_update.commit().unwrap();

        assert_eq!(
            apply_result.outcomes.iter().map(|o| o.id).collect::<Vec<_>>(),
            vec![
                local_transactions[4].get_hash(), // tx 4
                create_receipt_id_from_transaction(
                    PROTOCOL_VERSION,
                    &local_transactions[4],
                    &apply_state.prev_block_hash,
                    &apply_state.block_hash,
                ), // receipt for tx 4
                create_receipt_id_from_transaction(
                    PROTOCOL_VERSION,
                    &local_transactions[3],
                    &apply_state.prev_block_hash,
                    &apply_state.block_hash,
                ), // receipt for tx 3
                receipts[0].receipt_id,           // receipt #0
            ],
            "STEP #2 failed",
        );

        // STEP #3. Pass 4 new local transaction (TX#5, TX#6, TX#7, TX#8) and 1 new receipt R#3.
        // We process 3 local receipts for TX#5, TX#6, TX#7.
        // TX#8 and R#3 are added to delayed queue.
        // The new delayed queue is R#1, R#2, TX#8, R#3
        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &receipts[3..4],
                &local_transactions[5..9],
                &epoch_info_provider,
                None,
            )
            .unwrap();
        let (store_update, root) = tries.apply_all(&apply_result.trie_changes, 0).unwrap();
        store_update.commit().unwrap();

        assert_eq!(
            apply_result.outcomes.iter().map(|o| o.id).collect::<Vec<_>>(),
            vec![
                local_transactions[5].get_hash(), // tx 5
                local_transactions[6].get_hash(), // tx 6
                local_transactions[7].get_hash(), // tx 7
                local_transactions[8].get_hash(), // tx 8
                create_receipt_id_from_transaction(
                    PROTOCOL_VERSION,
                    &local_transactions[5],
                    &apply_state.prev_block_hash,
                    &apply_state.block_hash,
                ), // receipt for tx 5
                create_receipt_id_from_transaction(
                    PROTOCOL_VERSION,
                    &local_transactions[6],
                    &apply_state.prev_block_hash,
                    &apply_state.block_hash,
                ), // receipt for tx 6
                create_receipt_id_from_transaction(
                    PROTOCOL_VERSION,
                    &local_transactions[7],
                    &apply_state.prev_block_hash,
                    &apply_state.block_hash,
                ), // receipt for tx 7
            ],
            "STEP #3 failed",
        );

        // STEP #4. Pass no new TXs and 1 receipt R#4.
        // We process R#1, R#2, TX#8.
        // R#4 is added to delayed queue.
        // The new delayed queue is R#3, R#4
        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &receipts[4..5],
                &[],
                &epoch_info_provider,
                None,
            )
            .unwrap();
        let (store_update, root) = tries.apply_all(&apply_result.trie_changes, 0).unwrap();
        store_update.commit().unwrap();

        assert_eq!(
            apply_result.outcomes.iter().map(|o| o.id).collect::<Vec<_>>(),
            vec![
                receipts[1].receipt_id, // receipt #1
                receipts[2].receipt_id, // receipt #2
                create_receipt_id_from_transaction(
                    PROTOCOL_VERSION,
                    &local_transactions[8],
                    &apply_state.prev_block_hash,
                    &apply_state.block_hash,
                ), // receipt for tx 8
            ],
            "STEP #4 failed",
        );

        // STEP #5. Pass no new TXs and 1 receipt R#5.
        // We process R#3, R#4, R#5.
        // The new delayed queue is empty.
        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &receipts[5..6],
                &[],
                &epoch_info_provider,
                None,
            )
            .unwrap();

        assert_eq!(
            apply_result.outcomes.iter().map(|o| o.id).collect::<Vec<_>>(),
            vec![
                receipts[3].receipt_id, // receipt #3
                receipts[4].receipt_id, // receipt #4
                receipts[5].receipt_id, // receipt #5
            ],
            "STEP #5 failed",
        );
    }

    #[test]
    fn test_apply_invalid_incoming_receipts() {
        let initial_balance = to_yocto(1_000_000);
        let initial_locked = to_yocto(500_000);
        let small_transfer = to_yocto(10_000);
        let gas_limit = 1;
        let (runtime, tries, root, apply_state, _, epoch_info_provider) =
            setup_runtime(initial_balance, initial_locked, gas_limit);

        let n = 1;
        let mut receipts = generate_receipts(small_transfer, n);
        let invalid_account_id = "Invalid".to_string();
        receipts.get_mut(0).unwrap().predecessor_id = invalid_account_id.clone();

        let err = runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &receipts,
                &[],
                &epoch_info_provider,
                None,
            )
            .err()
            .unwrap();
        assert_eq!(
            err,
            RuntimeError::ReceiptValidationError(ReceiptValidationError::InvalidPredecessorId {
                account_id: invalid_account_id
            })
        )
    }

    #[test]
    fn test_apply_invalid_delayed_receipts() {
        let initial_balance = to_yocto(1_000_000);
        let initial_locked = to_yocto(500_000);
        let small_transfer = to_yocto(10_000);
        let gas_limit = 1;
        let (runtime, tries, root, apply_state, _, epoch_info_provider) =
            setup_runtime(initial_balance, initial_locked, gas_limit);

        let n = 1;
        let mut invalid_receipt = generate_receipts(small_transfer, n).pop().unwrap();
        let invalid_account_id = "Invalid".to_string();
        invalid_receipt.predecessor_id = invalid_account_id.clone();

        // Saving invalid receipt to the delayed receipts.
        let mut state_update = tries.new_trie_update(0, root);
        let mut delayed_receipts_indices = DelayedReceiptIndices::default();
        Runtime::delay_receipt(&mut state_update, &mut delayed_receipts_indices, &invalid_receipt)
            .unwrap();
        set(&mut state_update, TrieKey::DelayedReceiptIndices, &delayed_receipts_indices);
        state_update.commit(StateChangeCause::UpdatedDelayedReceipts);
        let trie_changes = state_update.finalize().unwrap().0;
        let (store_update, root) = tries.apply_all(&trie_changes, 0).unwrap();
        store_update.commit().unwrap();

        let err = runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &[],
                &[],
                &epoch_info_provider,
                None,
            )
            .err()
            .unwrap();
        assert_eq!(
            err,
            RuntimeError::StorageError(StorageError::StorageInconsistentState(format!(
                "Delayed receipt #0 in the state is invalid: {}",
                ReceiptValidationError::InvalidPredecessorId { account_id: invalid_account_id }
            )))
        )
    }

    #[test]
    fn test_apply_deficit_gas_for_transfer() {
        let initial_balance = to_yocto(1_000_000);
        let initial_locked = to_yocto(500_000);
        let small_transfer = to_yocto(10_000);
        let gas_limit = 10u64.pow(15);
        let (runtime, tries, root, apply_state, _, epoch_info_provider) =
            setup_runtime(initial_balance, initial_locked, gas_limit);

        let n = 1;
        let mut receipts = generate_receipts(small_transfer, n);
        if let ReceiptEnum::Action(action_receipt) = &mut receipts.get_mut(0).unwrap().receipt {
            action_receipt.gas_price = GAS_PRICE / 10;
        }

        let result = runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &receipts,
                &[],
                &epoch_info_provider,
                None,
            )
            .unwrap();
        assert_eq!(result.stats.gas_deficit_amount, result.stats.tx_burnt_amount * 9)
    }

    #[test]
    fn test_apply_deficit_gas_for_function_call_covered() {
        let initial_balance = to_yocto(1_000_000);
        let initial_locked = to_yocto(500_000);
        let gas_limit = 10u64.pow(15);
        let (runtime, tries, root, apply_state, _, epoch_info_provider) =
            setup_runtime(initial_balance, initial_locked, gas_limit);

        let gas = 2 * 10u64.pow(14);
        let gas_price = GAS_PRICE / 10;
        let actions = vec![Action::FunctionCall(FunctionCallAction {
            method_name: "hello".to_string(),
            args: b"world".to_vec(),
            gas,
            deposit: 0,
        })];

        let expected_gas_burnt = safe_add_gas(
            apply_state.config.transaction_costs.action_receipt_creation_config.exec_fee(),
            total_prepaid_exec_fees(
                &apply_state.config.transaction_costs,
                &actions,
                &alice_account(),
                PROTOCOL_VERSION,
            )
            .unwrap(),
        )
        .unwrap();
        let receipts = vec![Receipt {
            predecessor_id: bob_account(),
            receiver_id: alice_account(),
            receipt_id: CryptoHash::default(),
            receipt: ReceiptEnum::Action(ActionReceipt {
                signer_id: bob_account(),
                signer_public_key: PublicKey::empty(KeyType::ED25519),
                gas_price,
                output_data_receivers: vec![],
                input_data_ids: vec![],
                actions,
            }),
        }];
        let total_receipt_cost = Balance::from(gas + expected_gas_burnt) * gas_price;
        let expected_gas_burnt_amount = Balance::from(expected_gas_burnt) * GAS_PRICE;
        let expected_refund = total_receipt_cost - expected_gas_burnt_amount;

        let result = runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &receipts,
                &[],
                &epoch_info_provider,
                None,
            )
            .unwrap();
        // We used part of the prepaid gas to paying extra fees.
        assert_eq!(result.stats.gas_deficit_amount, 0);
        // The refund is less than the received amount.
        match &result.outgoing_receipts[0].receipt {
            ReceiptEnum::Action(ActionReceipt { actions, .. }) => {
                assert!(
                    matches!(actions[0], Action::Transfer(TransferAction { deposit }) if deposit == expected_refund)
                );
            }
            _ => unreachable!(),
        };
    }

    #[test]
    fn test_apply_deficit_gas_for_function_call_partial() {
        let initial_balance = to_yocto(1_000_000);
        let initial_locked = to_yocto(500_000);
        let gas_limit = 10u64.pow(15);
        let (runtime, tries, root, apply_state, _, epoch_info_provider) =
            setup_runtime(initial_balance, initial_locked, gas_limit);

        let gas = 1_000_000;
        let gas_price = GAS_PRICE / 10;
        let actions = vec![Action::FunctionCall(FunctionCallAction {
            method_name: "hello".to_string(),
            args: b"world".to_vec(),
            gas,
            deposit: 0,
        })];

        let expected_gas_burnt = safe_add_gas(
            apply_state.config.transaction_costs.action_receipt_creation_config.exec_fee(),
            total_prepaid_exec_fees(
                &apply_state.config.transaction_costs,
                &actions,
                &alice_account(),
                PROTOCOL_VERSION,
            )
            .unwrap(),
        )
        .unwrap();
        let receipts = vec![Receipt {
            predecessor_id: bob_account(),
            receiver_id: alice_account(),
            receipt_id: CryptoHash::default(),
            receipt: ReceiptEnum::Action(ActionReceipt {
                signer_id: bob_account(),
                signer_public_key: PublicKey::empty(KeyType::ED25519),
                gas_price,
                output_data_receivers: vec![],
                input_data_ids: vec![],
                actions,
            }),
        }];
        let total_receipt_cost = Balance::from(gas + expected_gas_burnt) * gas_price;
        let expected_gas_burnt_amount = Balance::from(expected_gas_burnt) * GAS_PRICE;
        let expected_deficit = expected_gas_burnt_amount - total_receipt_cost;

        let result = runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &receipts,
                &[],
                &epoch_info_provider,
                None,
            )
            .unwrap();
        // Used full prepaid gas, but it still not enough to cover deficit.
        assert_eq!(result.stats.gas_deficit_amount, expected_deficit);
        // Burnt all the fees + all prepaid gas.
        assert_eq!(result.stats.tx_burnt_amount, total_receipt_cost);
    }

    #[test]
    fn test_delete_key_add_key() {
        let initial_locked = to_yocto(500_000);
        let (runtime, tries, root, apply_state, signer, epoch_info_provider) =
            setup_runtime(to_yocto(1_000_000), initial_locked, 10u64.pow(15));

        let state_update = tries.new_trie_update(0, root);
        let initial_account_state = get_account(&state_update, &alice_account()).unwrap().unwrap();

        let actions = vec![
            Action::DeleteKey(DeleteKeyAction { public_key: signer.public_key() }),
            Action::AddKey(AddKeyAction {
                public_key: signer.public_key(),
                access_key: AccessKey::full_access(),
            }),
        ];

        let receipts = create_receipts_with_actions(alice_account(), signer, actions);

        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &receipts,
                &[],
                &epoch_info_provider,
                None,
            )
            .unwrap();
        let (store_update, root) = tries.apply_all(&apply_result.trie_changes, 0).unwrap();
        store_update.commit().unwrap();

        let state_update = tries.new_trie_update(0, root);
        let final_account_state = get_account(&state_update, &alice_account()).unwrap().unwrap();

        assert_eq!(initial_account_state.storage_usage(), final_account_state.storage_usage());
    }

    #[test]
    fn test_delete_key_underflow() {
        let initial_locked = to_yocto(500_000);
        let (runtime, tries, root, apply_state, signer, epoch_info_provider) =
            setup_runtime(to_yocto(1_000_000), initial_locked, 10u64.pow(15));

        let mut state_update = tries.new_trie_update(0, root);
        let mut initial_account_state =
            get_account(&state_update, &alice_account()).unwrap().unwrap();
        initial_account_state.set_storage_usage(10);
        set_account(&mut state_update, alice_account(), &initial_account_state);
        state_update.commit(StateChangeCause::InitialState);
        let trie_changes = state_update.finalize().unwrap().0;
        let (store_update, root) = tries.apply_all(&trie_changes, 0).unwrap();
        store_update.commit().unwrap();

        let actions = vec![Action::DeleteKey(DeleteKeyAction { public_key: signer.public_key() })];

        let receipts = create_receipts_with_actions(alice_account(), signer, actions);

        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &receipts,
                &[],
                &epoch_info_provider,
                None,
            )
            .unwrap();
        let (store_update, root) = tries.apply_all(&apply_result.trie_changes, 0).unwrap();
        store_update.commit().unwrap();

        let state_update = tries.new_trie_update(0, root);
        let final_account_state = get_account(&state_update, &alice_account()).unwrap().unwrap();

        assert_eq!(final_account_state.storage_usage(), 0);
    }

    #[test]
    fn test_contract_precompilation() {
        let initial_balance = to_yocto(1_000_000);
        let initial_locked = to_yocto(500_000);
        let gas_limit = 10u64.pow(15);
        let (runtime, tries, root, apply_state, signer, epoch_info_provider) =
            setup_runtime(initial_balance, initial_locked, gas_limit);

        let wasm_code = near_test_contracts::rs_contract().to_vec();
        let actions =
            vec![Action::DeployContract(DeployContractAction { code: wasm_code.clone() })];

        let receipts = create_receipts_with_actions(alice_account(), signer, actions);

        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(0),
                root,
                &None,
                &apply_state,
                &receipts,
                &[],
                &epoch_info_provider,
                None,
            )
            .unwrap();
        let (store_update, _) = tries.apply_all(&apply_result.trie_changes, 0).unwrap();
        store_update.commit().unwrap();

        let contract_code = ContractCode::new(wasm_code, None);
        let key = get_contract_cache_key(
            &contract_code,
            VMKind::default(),
            &apply_state.config.wasm_config,
        );
        apply_state
            .cache
            .unwrap()
            .get(&key.0)
            .expect("Compiled contract should be cached")
            .expect("Compilation result should be non-empty");
    }
}

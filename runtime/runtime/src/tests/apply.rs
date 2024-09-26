use super::{to_yocto, GAS_PRICE};
use crate::config::safe_add_gas;
use crate::congestion_control::{receipt_congestion_gas, receipt_size};
use crate::tests::{create_receipt_with_actions, MAX_ATTACHED_GAS};
use crate::total_prepaid_exec_fees;
use crate::{ApplyResult, ApplyState, Runtime, ValidatorAccountsUpdate};
use assert_matches::assert_matches;
use near_crypto::{InMemorySigner, KeyType, PublicKey, Signer};
use near_parameters::{ActionCosts, ExtCosts, ParameterCost, RuntimeConfig};
use near_primitives::account::AccessKey;
use near_primitives::action::delegate::{DelegateAction, NonDelegateAction, SignedDelegateAction};
use near_primitives::action::Action;
use near_primitives::checked_feature;
use near_primitives::congestion_info::{
    BlockCongestionInfo, CongestionControl, CongestionInfo, ExtendedCongestionInfo,
};
use near_primitives::errors::{ActionErrorKind, FunctionCallError, TxExecutionError};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::receipt::{ActionReceipt, Receipt, ReceiptEnum, ReceiptPriority, ReceiptV0};
use near_primitives::runtime::migration_data::{MigrationData, MigrationFlags};
use near_primitives::shard_layout::ShardUId;
use near_primitives::test_utils::{account_new, MockEpochInfoProvider};
use near_primitives::transaction::{
    AddKeyAction, DeleteKeyAction, DeployContractAction, ExecutionOutcomeWithId, ExecutionStatus,
    FunctionCallAction, SignedTransaction, TransferAction,
};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{
    Balance, Compute, EpochInfoProvider, Gas, MerkleHash, ShardId, StateChangeCause,
};
use near_primitives::utils::create_receipt_id_from_transaction;
use near_primitives::version::{ProtocolFeature, PROTOCOL_VERSION};
use near_store::test_utils::TestTriesBuilder;
use near_store::trie::global::GlobalShard;
use near_store::trie::receipts_column_helper::ShardsOutgoingReceiptBuffer;
use near_store::{get_account, set_access_key, set_account, ShardTries, Trie};
use near_vm_runner::FilesystemContractRuntimeCache;
use std::collections::HashMap;
use std::sync::Arc;
use testlib::runtime_utils::{alice_account, bob_account};

/***************/
/* Apply tests */
/***************/

fn setup_runtime(
    initial_balance: Balance,
    initial_locked: Balance,
    gas_limit: Gas,
) -> (Runtime, ShardTries, CryptoHash, ApplyState, Arc<Signer>, impl EpochInfoProvider) {
    setup_runtime_for_shard(initial_balance, initial_locked, gas_limit, ShardUId::single_shard())
}

fn setup_runtime_for_shard(
    initial_balance: Balance,
    initial_locked: Balance,
    gas_limit: Gas,
    shard_uid: ShardUId,
) -> (Runtime, ShardTries, CryptoHash, ApplyState, Arc<Signer>, impl EpochInfoProvider) {
    let tries = TestTriesBuilder::new().build();
    let root = MerkleHash::default();
    let runtime = Runtime::new();
    let account_id = alice_account();
    let signer: Arc<Signer> = Arc::new(
        InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, account_id.as_ref()).into(),
    );

    let mut initial_state = tries.new_trie_update(shard_uid, root);
    let mut initial_account = account_new(initial_balance, hash(&[]));
    // For the account and a full access key
    initial_account.set_storage_usage(182);
    initial_account.set_locked(initial_locked);
    set_account(&mut initial_state, account_id.clone(), &initial_account);
    set_access_key(&mut initial_state, account_id, signer.public_key(), &AccessKey::full_access());
    initial_state.commit(StateChangeCause::InitialState);
    let trie_changes = initial_state.finalize().unwrap().1;
    let mut store_update = tries.store_update();
    let root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
    store_update.commit().unwrap();
    let contract_cache = FilesystemContractRuntimeCache::test().unwrap();
    let shards_congestion_info = if ProtocolFeature::CongestionControl.enabled(PROTOCOL_VERSION) {
        [(0, ExtendedCongestionInfo::default())].into()
    } else {
        [].into()
    };
    let congestion_info = BlockCongestionInfo::new(shards_congestion_info);
    let apply_state = ApplyState {
        apply_reason: None,
        block_height: 1,
        prev_block_hash: Default::default(),
        block_hash: Default::default(),
        shard_id: shard_uid.shard_id(),
        epoch_id: Default::default(),
        epoch_height: 0,
        gas_price: GAS_PRICE,
        block_timestamp: 100,
        gas_limit: Some(gas_limit),
        random_seed: Default::default(),
        current_protocol_version: PROTOCOL_VERSION,
        config: Arc::new(RuntimeConfig::test()),
        cache: Some(Box::new(contract_cache)),
        is_new_chunk: true,
        migration_data: Arc::new(MigrationData::default()),
        migration_flags: MigrationFlags::default(),
        congestion_info,
    };

    (runtime, tries, root, apply_state, signer, MockEpochInfoProvider::default())
}

#[test]
fn test_apply_no_op() {
    let (runtime, tries, root, apply_state, _, epoch_info_provider) =
        setup_runtime(to_yocto(1_000_000), 0, 10u64.pow(15));
    let global_shard_state = tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default());
    runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            GlobalShard::new(global_shard_state),
            &None,
            &apply_state,
            &[],
            &[],
            &epoch_info_provider,
            Default::default(),
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
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            GlobalShard::new(tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default())),
            &Some(validator_accounts_update),
            &apply_state,
            &[Receipt::new_balance_refund(
                &alice_account(),
                small_refund,
                ReceiptPriority::NoPriority,
            )],
            &[],
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
}

#[test]
fn test_apply_refund_receipts() {
    let initial_balance = to_yocto(1_000_000);
    let initial_locked = to_yocto(500_000);
    let small_transfer = to_yocto(10_000);
    let gas_limit = 1;
    let (runtime, tries, mut root, mut apply_state, _, epoch_info_provider) =
        setup_runtime(initial_balance, initial_locked, gas_limit);

    let n = 10;
    let receipts = generate_refund_receipts(small_transfer, n);

    // Checking n receipts delayed
    for i in 1..=n + 3 {
        let prev_receipts: &[Receipt] = if i == 1 { &receipts } else { &[] };
        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(ShardUId::single_shard(), root),
                GlobalShard::new(
                    tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default()),
                ),
                &None,
                &apply_state,
                prev_receipts,
                &[],
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        root = commit_apply_result(&apply_result, &mut apply_state, &tries);
        let state = tries.new_trie_update(ShardUId::single_shard(), root);
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
    let (runtime, tries, mut root, mut apply_state, _, epoch_info_provider) =
        setup_runtime(initial_balance, initial_locked, gas_limit);

    let n = 10;
    let receipts = generate_receipts(small_transfer, n);

    // Checking n receipts delayed by 1 + 3 extra
    for i in 1..=n + 3 {
        let prev_receipts: &[Receipt] = if i == 1 { &receipts } else { &[] };
        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(ShardUId::single_shard(), root),
                GlobalShard::new(
                    tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default()),
                ),
                &None,
                &apply_state,
                prev_receipts,
                &[],
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        root = commit_apply_result(&apply_result, &mut apply_state, &tries);

        let state = tries.new_trie_update(ShardUId::single_shard(), root);
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

    let receipt_gas_cost = apply_state.config.fees.fee(ActionCosts::new_action_receipt).exec_fee()
        + apply_state.config.fees.fee(ActionCosts::transfer).exec_fee();
    apply_state.gas_limit = Some(receipt_gas_cost * 3);

    let n = 40;
    let receipts = generate_receipts(small_transfer, n);
    let mut receipt_chunks = receipts.chunks_exact(4);

    // Every time we'll process 3 receipts, so we need n / 3 rounded up. Then we do 3 extra.
    for i in 1..=n / 3 + 3 {
        let prev_receipts: &[Receipt] = receipt_chunks.next().unwrap_or_default();
        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(ShardUId::single_shard(), root),
                GlobalShard::new(
                    tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default()),
                ),
                &None,
                &apply_state,
                prev_receipts,
                &[],
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        root = commit_apply_result(&apply_result, &mut apply_state, &tries);
        let state = tries.new_trie_update(ShardUId::single_shard(), root);
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

    let receipt_gas_cost = apply_state.config.fees.fee(ActionCosts::new_action_receipt).exec_fee()
        + apply_state.config.fees.fee(ActionCosts::transfer).exec_fee();

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
                tries.get_trie_for_shard(ShardUId::single_shard(), root),
                GlobalShard::new(
                    tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default()),
                ),
                &None,
                &apply_state,
                prev_receipts,
                &[],
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        root = commit_apply_result(&apply_result, &mut apply_state, &tries);
        let state = tries.new_trie_update(ShardUId::single_shard(), root);
        num_receipts_processed += apply_result.outcomes.len() as u64;
        let account = get_account(&state, &alice_account()).unwrap().unwrap();
        assert_eq!(
            account.amount(),
            initial_balance
                + small_transfer * Balance::from(num_receipts_processed)
                + Balance::from(num_receipts_processed * (num_receipts_processed - 1) / 2)
        );
        let expected_queue_length = num_receipts_given - num_receipts_processed;
        println!(
                "{} processed out of {} given. With limit {} receipts per block. The expected delayed_receipts_count is {}. The delayed_receipts_count is {}.",
                num_receipts_processed,
                num_receipts_given,
                num_receipts_per_block,
                expected_queue_length,
                apply_result.delayed_receipts_count,
            );
        assert_eq!(apply_result.delayed_receipts_count, expected_queue_length);
    }
}

fn generate_receipts(small_transfer: u128, n: u64) -> Vec<Receipt> {
    let mut receipt_id = CryptoHash::default();
    (0..n)
        .map(|i| {
            receipt_id = hash(receipt_id.as_ref());
            Receipt::V0(ReceiptV0 {
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
            })
        })
        .collect()
}

fn generate_refund_receipts(small_transfer: u128, n: u64) -> Vec<Receipt> {
    let mut receipt_id = CryptoHash::default();
    (0..n)
        .map(|i| {
            receipt_id = hash(receipt_id.as_ref());
            Receipt::new_balance_refund(
                &alice_account(),
                small_transfer + Balance::from(i),
                ReceiptPriority::NoPriority,
            )
        })
        .collect()
}

fn generate_delegate_actions(deposit: u128, n: u64) -> Vec<Receipt> {
    // Setup_runtime only creates alice_account() in state, hence we use the
    // id as relayer and sender. This allows the delegate action to execute
    // successfully. But the inner function call will fail, since the
    // contract account does not exists.
    let relayer_id = alice_account();
    let sender_id = alice_account();
    let receiver_id = bob_account();
    let signer = Arc::new(InMemorySigner::from_seed(
        sender_id.clone(),
        KeyType::ED25519,
        sender_id.as_ref(),
    ));
    (0..n)
        .map(|i| {
            let inner_actions = [Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "foo".to_string(),
                args: b"arg".to_vec(),
                gas: MAX_ATTACHED_GAS,
                deposit,
            }))];

            let delegate_action = DelegateAction {
                sender_id: sender_id.clone(),
                receiver_id: receiver_id.clone(),
                actions: inner_actions
                    .iter()
                    .map(|a| NonDelegateAction::try_from(a.clone()).unwrap())
                    .collect(),
                nonce: 2 + i as u64,
                max_block_height: 10000,
                public_key: signer.public_key(),
            };
            let signed_delegate_action = Action::Delegate(Box::new(SignedDelegateAction {
                signature: signer.sign(delegate_action.get_nep461_hash().as_bytes()),
                delegate_action,
            }));
            let receipt_id = hash(&i.to_le_bytes());
            Receipt::V0(ReceiptV0 {
                predecessor_id: relayer_id.clone(),
                receiver_id: alice_account(),
                receipt_id,
                receipt: ReceiptEnum::Action(ActionReceipt {
                    signer_id: relayer_id.clone(),
                    signer_public_key: PublicKey::empty(KeyType::ED25519),
                    gas_price: GAS_PRICE,
                    output_data_receivers: vec![],
                    input_data_ids: vec![],
                    actions: vec![signed_delegate_action],
                }),
            })
        })
        .collect()
}

#[test]
fn test_apply_delayed_receipts_local_tx() {
    let initial_balance = to_yocto(1_000_000);
    let initial_locked = to_yocto(500_000);
    let small_transfer = to_yocto(10_000);
    let (runtime, tries, mut root, mut apply_state, signer, epoch_info_provider) =
        setup_runtime(initial_balance, initial_locked, 1);

    let receipt_exec_gas_fee = 1000;
    let mut free_config = RuntimeConfig::free();
    let fees = Arc::make_mut(&mut free_config.fees);
    fees.action_fees[ActionCosts::new_action_receipt].execution = receipt_exec_gas_fee;
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
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            GlobalShard::new(tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default())),
            &None,
            &apply_state,
            &receipts[0..2],
            &local_transactions[0..4],
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    root = commit_apply_result(&apply_result, &mut apply_state, &tries);

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
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            GlobalShard::new(tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default())),
            &None,
            &apply_state,
            &receipts[2..3],
            &local_transactions[4..5],
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    let mut store_update = tries.store_update();
    let root =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
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
            *receipts[0].receipt_id(),        // receipt #0
        ],
        "STEP #2 failed",
    );

    // STEP #3. Pass 4 new local transaction (TX#5, TX#6, TX#7, TX#8) and 1 new receipt R#3.
    // We process 3 local receipts for TX#5, TX#6, TX#7.
    // TX#8 and R#3 are added to delayed queue.
    // The new delayed queue is R#1, R#2, TX#8, R#3
    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            GlobalShard::new(tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default())),
            &None,
            &apply_state,
            &receipts[3..4],
            &local_transactions[5..9],
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    let mut store_update = tries.store_update();
    let root =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
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
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            GlobalShard::new(tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default())),
            &None,
            &apply_state,
            &receipts[4..5],
            &[],
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    let mut store_update = tries.store_update();
    let root =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit().unwrap();

    assert_eq!(
        apply_result.outcomes.iter().map(|o| o.id).collect::<Vec<_>>(),
        vec![
            *receipts[1].receipt_id(), // receipt #1
            *receipts[2].receipt_id(), // receipt #2
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
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            GlobalShard::new(tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default())),
            &None,
            &apply_state,
            &receipts[5..6],
            &[],
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(
        apply_result.outcomes.iter().map(|o| o.id).collect::<Vec<_>>(),
        vec![
            *receipts[3].receipt_id(), // receipt #3
            *receipts[4].receipt_id(), // receipt #4
            *receipts[5].receipt_id(), // receipt #5
        ],
        "STEP #5 failed",
    );
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
    if let ReceiptEnum::Action(action_receipt) = receipts.get_mut(0).unwrap().receipt_mut() {
        action_receipt.gas_price = GAS_PRICE / 10;
    }

    let result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            GlobalShard::new(tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default())),
            &None,
            &apply_state,
            &receipts,
            &[],
            &epoch_info_provider,
            Default::default(),
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
    let actions = vec![Action::FunctionCall(Box::new(FunctionCallAction {
        method_name: "hello".to_string(),
        args: b"world".to_vec(),
        gas,
        deposit: 0,
    }))];

    let expected_gas_burnt = safe_add_gas(
        apply_state.config.fees.fee(ActionCosts::new_action_receipt).exec_fee(),
        total_prepaid_exec_fees(&apply_state.config, &actions, &alice_account()).unwrap(),
    )
    .unwrap();
    let receipts = vec![Receipt::V0(ReceiptV0 {
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
    })];
    let total_receipt_cost = Balance::from(gas + expected_gas_burnt) * gas_price;
    let expected_gas_burnt_amount = Balance::from(expected_gas_burnt) * GAS_PRICE;
    let expected_refund = total_receipt_cost - expected_gas_burnt_amount;

    let result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            GlobalShard::new(tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default())),
            &None,
            &apply_state,
            &receipts,
            &[],
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    // We used part of the prepaid gas to paying extra fees.
    assert_eq!(result.stats.gas_deficit_amount, 0);
    // The refund is less than the received amount.
    match result.outgoing_receipts[0].receipt() {
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
    let actions = vec![Action::FunctionCall(Box::new(FunctionCallAction {
        method_name: "hello".to_string(),
        args: b"world".to_vec(),
        gas,
        deposit: 0,
    }))];

    let expected_gas_burnt = safe_add_gas(
        apply_state.config.fees.fee(ActionCosts::new_action_receipt).exec_fee(),
        total_prepaid_exec_fees(&apply_state.config, &actions, &alice_account()).unwrap(),
    )
    .unwrap();
    let receipts = vec![Receipt::V0(ReceiptV0 {
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
    })];
    let total_receipt_cost = Balance::from(gas + expected_gas_burnt) * gas_price;
    let expected_gas_burnt_amount = Balance::from(expected_gas_burnt) * GAS_PRICE;
    let expected_deficit = expected_gas_burnt_amount - total_receipt_cost;

    let result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            GlobalShard::new(tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default())),
            &None,
            &apply_state,
            &receipts,
            &[],
            &epoch_info_provider,
            Default::default(),
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

    let state_update = tries.new_trie_update(ShardUId::single_shard(), root);
    let initial_account_state = get_account(&state_update, &alice_account()).unwrap().unwrap();

    let actions = vec![
        Action::DeleteKey(Box::new(DeleteKeyAction { public_key: signer.public_key() })),
        Action::AddKey(Box::new(AddKeyAction {
            public_key: signer.public_key(),
            access_key: AccessKey::full_access(),
        })),
    ];

    let receipts = vec![create_receipt_with_actions(alice_account(), signer, actions)];

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            GlobalShard::new(tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default())),
            &None,
            &apply_state,
            &receipts,
            &[],
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    let mut store_update = tries.store_update();
    let root =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit().unwrap();

    let state_update = tries.new_trie_update(ShardUId::single_shard(), root);
    let final_account_state = get_account(&state_update, &alice_account()).unwrap().unwrap();

    assert_eq!(initial_account_state.storage_usage(), final_account_state.storage_usage());
}

#[test]
fn test_delete_key_underflow() {
    let initial_locked = to_yocto(500_000);
    let (runtime, tries, root, apply_state, signer, epoch_info_provider) =
        setup_runtime(to_yocto(1_000_000), initial_locked, 10u64.pow(15));

    let mut state_update = tries.new_trie_update(ShardUId::single_shard(), root);
    let mut initial_account_state = get_account(&state_update, &alice_account()).unwrap().unwrap();
    initial_account_state.set_storage_usage(10);
    set_account(&mut state_update, alice_account(), &initial_account_state);
    state_update.commit(StateChangeCause::InitialState);
    let trie_changes = state_update.finalize().unwrap().1;
    let mut store_update = tries.store_update();
    let root = tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit().unwrap();

    let actions =
        vec![Action::DeleteKey(Box::new(DeleteKeyAction { public_key: signer.public_key() }))];

    let receipts = vec![create_receipt_with_actions(alice_account(), signer, actions)];

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            GlobalShard::new(tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default())),
            &None,
            &apply_state,
            &receipts,
            &[],
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    let mut store_update = tries.store_update();
    let root =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit().unwrap();

    let state_update = tries.new_trie_update(ShardUId::single_shard(), root);
    let final_account_state = get_account(&state_update, &alice_account()).unwrap().unwrap();

    assert_eq!(final_account_state.storage_usage(), 0);
}

// This test only works on platforms that support wasmer2.
#[test]
#[cfg(target_arch = "x86_64")]
fn test_contract_precompilation() {
    use super::create_receipt_with_actions;

    let initial_balance = to_yocto(1_000_000);
    let initial_locked = to_yocto(500_000);
    let gas_limit = 10u64.pow(15);
    let (runtime, tries, root, apply_state, signer, epoch_info_provider) =
        setup_runtime(initial_balance, initial_locked, gas_limit);

    let wasm_code = near_test_contracts::rs_contract().to_vec();
    let actions = vec![Action::DeployContract(DeployContractAction { code: wasm_code.clone() })];

    let receipts = vec![create_receipt_with_actions(alice_account(), signer, actions)];

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            &apply_state,
            &receipts,
            &[],
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    let mut store_update = tries.store_update();
    tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit().unwrap();

    let contract_code = near_vm_runner::ContractCode::new(wasm_code, None);
    let key = near_vm_runner::get_contract_cache_key(
        *contract_code.hash(),
        &apply_state.config.wasm_config,
    );
    apply_state
        .cache
        .unwrap()
        .get(&key)
        .expect("Compiled contract should be cached")
        .expect("Compilation result should be non-empty");
}

#[test]
fn test_compute_usage_limit() {
    let (runtime, tries, mut root, mut apply_state, signer, epoch_info_provider) =
        setup_runtime(to_yocto(1_000_000), to_yocto(500_000), 1);

    let mut free_config = RuntimeConfig::free();
    let sha256_cost = ParameterCost {
        gas: Gas::from(1_000_000u64),
        compute: Compute::from(10_000_000_000_000u64),
    };
    let wasm_config = Arc::make_mut(&mut free_config.wasm_config);
    wasm_config.ext_costs.costs[ExtCosts::sha256_base] = sha256_cost.clone();
    apply_state.config = Arc::new(free_config);
    // This allows us to execute 1 receipt with a function call per apply.
    apply_state.gas_limit = Some(sha256_cost.compute);

    let deploy_contract_receipt = create_receipt_with_actions(
        alice_account(),
        signer.clone(),
        vec![Action::DeployContract(DeployContractAction {
            code: near_test_contracts::rs_contract().to_vec(),
        })],
    );

    let first_call_receipt = create_receipt_with_actions(
        alice_account(),
        signer.clone(),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "ext_sha256".to_string(),
            args: b"first".to_vec(),
            gas: sha256_cost.gas,
            deposit: 0,
        }))],
    );

    let second_call_receipt = create_receipt_with_actions(
        alice_account(),
        signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "ext_sha256".to_string(),
            args: b"second".to_vec(),
            gas: sha256_cost.gas,
            deposit: 0,
        }))],
    );

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            GlobalShard::new(tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default())),
            &None,
            &apply_state,
            &[
                deploy_contract_receipt.clone(),
                first_call_receipt.clone(),
                second_call_receipt.clone(),
            ],
            &[],
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    root = commit_apply_result(&apply_result, &mut apply_state, &tries);

    // Only first two receipts should fit into the chunk due to the compute usage limit.
    assert_matches!(&apply_result.outcomes[..], [first, second] => {
        assert_eq!(first.id, *deploy_contract_receipt.receipt_id());
        assert_matches!(first.outcome.status, ExecutionStatus::SuccessValue(_));

        assert_eq!(second.id, *first_call_receipt.receipt_id());
        assert_eq!(second.outcome.compute_usage.unwrap(), sha256_cost.compute);
        assert_matches!(second.outcome.status, ExecutionStatus::SuccessValue(_));
    });

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            GlobalShard::new(tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default())),
            &None,
            &apply_state,
            &[],
            &[],
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_matches!(&apply_result.outcomes[..], [ExecutionOutcomeWithId { id, outcome }] => {
        assert_eq!(id, second_call_receipt.receipt_id());
        assert_eq!(outcome.compute_usage.unwrap(), sha256_cost.compute);
        assert_matches!(outcome.status, ExecutionStatus::SuccessValue(_));
    });
}

#[test]
fn test_compute_usage_limit_with_failed_receipt() {
    let (runtime, tries, root, apply_state, signer, epoch_info_provider) =
        setup_runtime(to_yocto(1_000_000), to_yocto(500_000), 10u64.pow(15));

    let deploy_contract_receipt = create_receipt_with_actions(
        alice_account(),
        signer.clone(),
        vec![Action::DeployContract(DeployContractAction {
            code: near_test_contracts::rs_contract().to_vec(),
        })],
    );

    let first_call_receipt = create_receipt_with_actions(
        alice_account(),
        signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "ext_sha256".to_string(),
            args: b"first".to_vec(),
            gas: 1,
            deposit: 0,
        }))],
    );

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            GlobalShard::new(tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default())),
            &None,
            &apply_state,
            &[deploy_contract_receipt.clone(), first_call_receipt.clone()],
            &[],
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_matches!(&apply_result.outcomes[..], [first, second] => {
        assert_eq!(first.id, *deploy_contract_receipt.receipt_id());
        assert_matches!(first.outcome.status, ExecutionStatus::SuccessValue(_));

        assert_eq!(second.id, *first_call_receipt.receipt_id());
        assert_matches!(second.outcome.status, ExecutionStatus::Failure(_));
    });
}

#[test]
fn test_main_storage_proof_size_soft_limit() {
    if !checked_feature!("stable", StatelessValidation, PROTOCOL_VERSION) {
        return;
    }
    let (runtime, tries, root, mut apply_state, signer, epoch_info_provider) =
        setup_runtime(to_yocto(1_000_000), to_yocto(500_000), 10u64.pow(15));

    // Change main_storage_proof_size_soft_limit to a smaller value
    // The value of 500 is small enough to let the first receipt go through but not the second
    let mut runtime_config = RuntimeConfig::test();
    runtime_config.witness_config.main_storage_proof_size_soft_limit = 5000;
    apply_state.config = Arc::new(runtime_config);

    let create_acc_fn = |account_id| {
        create_receipt_with_actions(
            account_id,
            signer.clone(),
            vec![Action::DeployContract(DeployContractAction {
                code: near_test_contracts::sized_contract(5000).to_vec(),
            })],
        )
    };

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads(),
            GlobalShard::new(tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default())),
            &None,
            &apply_state,
            &[create_acc_fn(alice_account()), create_acc_fn(bob_account())],
            &[],
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    let mut store_update = tries.store_update();
    let root =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit().unwrap();

    let function_call_fn = |account_id| {
        create_receipt_with_actions(
            account_id,
            signer.clone(),
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "main".to_string(),
                args: Vec::new(),
                gas: 1,
                deposit: 0,
            }))],
        )
    };

    // The function call to bob_account should hit the main_storage_proof_size_soft_limit
    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads(),
            GlobalShard::new(tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default())),
            &None,
            &apply_state,
            &[function_call_fn(alice_account()), function_call_fn(bob_account())],
            &[],
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    // We expect function_call_fn(bob_account()) to be in delayed receipts
    assert_eq!(apply_result.delayed_receipts_count, 1);

    // Check that alice contract is present in storage proof and bob
    // contract is not.
    let partial_storage = apply_result.proof.unwrap();
    let storage = Trie::from_recorded_storage(partial_storage, root, false);
    let code_key = TrieKey::ContractCode { account_id: alice_account() };
    assert_matches!(storage.get(&code_key.to_vec()), Ok(Some(_)));
    let code_key = TrieKey::ContractCode { account_id: bob_account() };
    assert_matches!(storage.get(&code_key.to_vec()), Err(_) | Ok(None));
}

/// Check that applying nothing does not change the state trie.
///
/// This test is useful to check that trie columns are not accidentally
/// initialized. Many integration tests will fail as well if this fails, but
/// those are harder to root cause.
#[test]
fn test_empty_apply() {
    let initial_balance = to_yocto(1_000_000);
    let initial_locked = to_yocto(500_000);
    let gas_limit = 10u64.pow(15);
    let (runtime, tries, root_before, apply_state, _signer, epoch_info_provider) =
        setup_runtime(initial_balance, initial_locked, gas_limit);

    let receipts = [];
    let transactions = [];

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root_before),
            GlobalShard::new(tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default())),
            &None,
            &apply_state,
            &receipts,
            &transactions,
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    let mut store_update = tries.store_update();
    let root_after =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
    assert_eq!(root_before, root_after, "state root changed for applying empty receipts");
}

/// Test that delayed receipts are accounted for in the congestion info of
/// the ApplyResult.
#[test]
fn test_congestion_delayed_receipts_accounting() {
    let initial_balance = to_yocto(10);
    let initial_locked = to_yocto(0);
    let deposit = to_yocto(1);
    let gas_limit = 1;
    let (runtime, tries, root, apply_state, _, epoch_info_provider) =
        setup_runtime(initial_balance, initial_locked, gas_limit);

    let n = 10;
    let receipts = generate_receipts(deposit, n);

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            GlobalShard::new(tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default())),
            &None,
            &apply_state,
            &receipts,
            &[],
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(n - 1, apply_result.delayed_receipts_count);
    if ProtocolFeature::CongestionControl.enabled(PROTOCOL_VERSION) {
        let congestion = apply_result.congestion_info.unwrap();
        let expected_delayed_gas =
            (n - 1) * receipt_congestion_gas(&receipts[0], &apply_state.config).unwrap();
        let expected_receipts_bytes = (n - 1) * receipt_size(&receipts[0]).unwrap() as u64;

        assert_eq!(expected_delayed_gas as u128, congestion.delayed_receipts_gas());
        assert_eq!(expected_receipts_bytes, congestion.receipt_bytes());
    }
}

/// Test that the outgoing receipts buffer works as intended.
///
/// Specifically, we want to check that
///   (a) receipts to congested shards are held back in outgoing buffers
///   (b) receipts in the outgoing buffer are drained when possible
///   (c) drained receipts are forwarded
///
/// The test uses receipts with balances attached, which also tests
/// necessary changes to the balance checker.
#[test]
fn test_congestion_buffering() {
    if !ProtocolFeature::CongestionControl.enabled(PROTOCOL_VERSION) {
        return;
    }
    // In the test setup with he MockEpochInfoProvider, all accounts are on
    // shard 0. Hence all receipts will be forwarded to shard 0. We don't
    // want local forwarding in the test, hence we need to use a different
    // shard id.
    let local_shard = 1 as ShardId;
    let local_shard_uid = ShardUId { version: 0, shard_id: local_shard as u32 };
    let receiver_shard = 0 as ShardId;

    let initial_balance = to_yocto(1_000_000);
    let initial_locked = to_yocto(500_000);
    let deposit = to_yocto(10_000);
    // execute a single receipt per chunk
    let gas_limit = 1;
    let (runtime, tries, mut root, mut apply_state, _, epoch_info_provider) =
        setup_runtime_for_shard(initial_balance, initial_locked, gas_limit, local_shard_uid);

    apply_state.shard_id = local_shard;

    // Mark shard 0 as congested. Which method we use doesn't matter, this
    // test only checks that receipt buffering works. Unit tests
    // congestion_info.rs test that the congestion level is picked up for
    // all possible congestion conditions.
    let max_congestion_incoming_gas: Gas =
        apply_state.config.congestion_control_config.max_congestion_incoming_gas;
    apply_state
        .congestion_info
        .get_mut(&0)
        .unwrap()
        .congestion_info
        .add_delayed_receipt_gas(max_congestion_incoming_gas)
        .unwrap();
    // set allowed shard of shard 0 to 0 to prevent shard 1 from forwarding
    apply_state.congestion_info.get_mut(&0).unwrap().congestion_info.set_allowed_shard(0);
    apply_state.congestion_info.insert(1, Default::default());

    // We need receipts that produce an outgoing receipt. Function calls and
    // delegate actions are currently the two only choices. We use delegate
    // actions because this doesn't require a contract setup.
    let n = 10;
    let receipts = generate_delegate_actions(deposit, n);

    // Checking n receipts delayed by 1 + 3 extra
    for i in 1..=n + 3 {
        let prev_receipts: &[Receipt] = if i == 1 { &receipts } else { &[] };
        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(local_shard_uid, root),
                GlobalShard::new(
                    tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default()),
                ),
                &None,
                &apply_state,
                prev_receipts,
                &[],
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        if let Some(congestion_info) = apply_result.congestion_info {
            apply_state
                .congestion_info
                .insert(local_shard, ExtendedCongestionInfo::new(congestion_info, 0));
        }
        let mut store_update = tries.store_update();
        root = tries.apply_all(&apply_result.trie_changes, local_shard_uid, &mut store_update);
        store_update.commit().unwrap();

        // (a) check receipts are held back in buffer
        let state = tries.get_trie_for_shard(local_shard_uid, root);
        let buffers = ShardsOutgoingReceiptBuffer::load(&state).unwrap();
        let capped_i = std::cmp::min(i, n);
        assert_eq!(0, apply_result.outgoing_receipts.len());
        assert_eq!(capped_i, buffers.buffer_len(receiver_shard).unwrap());
        let congestion = apply_result.congestion_info.unwrap();
        assert!(congestion.buffered_receipts_gas() > 0);
        assert!(congestion.receipt_bytes() > 0);
    }

    // Check congestion is 1.0
    let congestion = apply_state.congestion_control(receiver_shard, 0);
    assert_eq!(congestion.congestion_level(), 1.0);
    assert_eq!(congestion.outgoing_gas_limit(local_shard), 0);

    // release congestion to just below 1.0, which should allow one receipt
    // to be forwarded per round
    apply_state
        .congestion_info
        .get_mut(&0)
        .unwrap()
        .congestion_info
        .remove_delayed_receipt_gas(10)
        .unwrap();

    let min_outgoing_gas: Gas = apply_state.config.congestion_control_config.min_outgoing_gas;
    // Check congestion is less than 1.0
    let congestion = apply_state.congestion_control(receiver_shard, 0);
    assert!(congestion.congestion_level() < 1.0);
    // this exact number does not matter but if it changes the test setup
    // needs to adapt to ensure the number of forwarded receipts is as expected
    assert!(
        congestion.outgoing_gas_limit(local_shard) - min_outgoing_gas < 100 * 10u64.pow(9),
        "allowed forwarding must be less than 100 GGas away from MIN_OUTGOING_GAS"
    );

    // Checking n receipts delayed by 1 + 3 extra
    let forwarded_per_chunk = min_outgoing_gas / MAX_ATTACHED_GAS;
    for i in 1..=n + 3 {
        let prev_receipts = &[];
        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(local_shard_uid, root),
                GlobalShard::new(
                    tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default()),
                ),
                &None,
                &apply_state,
                prev_receipts,
                &[],
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        root = commit_apply_result(&apply_result, &mut apply_state, &tries);

        let state = tries.get_trie_for_shard(local_shard_uid, root);
        let buffers = ShardsOutgoingReceiptBuffer::load(&state).unwrap();

        // (b) check receipts are removed from the buffer
        let max_forwarded = i * forwarded_per_chunk;
        let expected_num_in_buffer = n.saturating_sub(max_forwarded);
        assert_eq!(expected_num_in_buffer, buffers.buffer_len(receiver_shard).unwrap());

        let prev_max_forwarded = (i - 1) * forwarded_per_chunk;
        if prev_max_forwarded >= n {
            // no receipts left to forward
            assert_eq!(0, apply_result.outgoing_receipts.len());
        } else {
            let expected_forwarded =
                std::cmp::min(forwarded_per_chunk, n.saturating_sub(prev_max_forwarded));
            // (c) check the right number of receipts are forwarded
            assert_eq!(expected_forwarded as usize, apply_result.outgoing_receipts.len());
        }
    }
}

// Apply trie changes in `ApplyResult` and update `ApplyState` with new
// congestion info for the next call to apply().
fn commit_apply_result(
    apply_result: &ApplyResult,
    apply_state: &mut ApplyState,
    tries: &ShardTries,
) -> CryptoHash {
    // congestion control requires an update on
    let shard_id = apply_state.shard_id;
    let shard_uid = ShardUId { version: 0, shard_id: shard_id as u32 };
    if let Some(congestion_info) = apply_result.congestion_info {
        apply_state
            .congestion_info
            .insert(shard_id, ExtendedCongestionInfo::new(congestion_info, 0));
    }
    let mut store_update = tries.store_update();
    let root = tries.apply_all(&apply_result.trie_changes, shard_uid, &mut store_update);
    store_update.commit().unwrap();
    return root;
}

impl ApplyState {
    fn congestion_control(&self, shard_id: ShardId, missed_chunks: u64) -> CongestionControl {
        CongestionControl::new(
            self.config.congestion_control_config,
            self.congestion_info.get(&shard_id).unwrap().congestion_info,
            missed_chunks,
        )
    }
}

/// Create a scenario where `apply` is called without congestion info but
/// cross-shard congestion control is enabled, then check what congestion
/// info is in the apply result.
fn check_congestion_info_bootstrapping(is_new_chunk: bool, want: Option<CongestionInfo>) {
    let initial_balance = to_yocto(1_000_000);
    let initial_locked = to_yocto(500_000);
    let gas_limit = 10u64.pow(15);
    let (runtime, tries, root, mut apply_state, _, epoch_info_provider) =
        setup_runtime(initial_balance, initial_locked, gas_limit);

    // Delete previous congestion info to trigger bootstrapping it. An empty
    // shards congestion info map is what we should see in the first chunk
    // with the feature enabled.
    apply_state.congestion_info = BlockCongestionInfo::default();

    // Apply test specific settings
    apply_state.is_new_chunk = is_new_chunk;

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            GlobalShard::new(tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default())),
            &None,
            &apply_state,
            &[],
            &[],
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(want, apply_result.congestion_info);
}

/// Test that applying a new chunk triggers bootstrapping the congestion
/// info but applying an old chunk doesn't. (We don't want bootstrapping to
/// be triggered on missed chunks.)
#[test]
fn test_congestion_info_bootstrapping() {
    if !ProtocolFeature::CongestionControl.enabled(PROTOCOL_VERSION) {
        return;
    }
    let is_new_chunk = true;
    check_congestion_info_bootstrapping(is_new_chunk, Some(CongestionInfo::default()));

    let is_new_chunk = false;
    check_congestion_info_bootstrapping(is_new_chunk, None);
}

#[test]
fn test_deploy_and_call_local_receipt() {
    let (runtime, tries, root, apply_state, signer, epoch_info_provider) =
        setup_runtime(to_yocto(1_000_000), to_yocto(500_000), 10u64.pow(15));

    let tx = SignedTransaction::from_actions(
        1,
        alice_account(),
        alice_account(),
        &*signer,
        vec![
            Action::DeployContract(DeployContractAction {
                code: near_test_contracts::rs_contract().to_vec(),
            }),
            Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "log_something".to_string(),
                args: vec![],
                gas: MAX_ATTACHED_GAS / 2,
                deposit: 0,
            })),
            Action::DeployContract(DeployContractAction {
                code: near_test_contracts::trivial_contract().to_vec(),
            }),
            Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "log_something".to_string(),
                args: vec![],
                gas: MAX_ATTACHED_GAS / 2,
                deposit: 0,
            })),
        ],
        CryptoHash::default(),
        0,
    );

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            GlobalShard::new(tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default())),
            &None,
            &apply_state,
            &[],
            &[tx],
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    let outcome = assert_matches!(
        &apply_result.outcomes[..],
        [_, ExecutionOutcomeWithId { id: _, outcome }] => outcome
    );
    assert_eq!(&outcome.logs[..], ["hello"]);
    let action_error = assert_matches!(
        &outcome.status,
        ExecutionStatus::Failure(TxExecutionError::ActionError(ae)) => ae
    );
    assert_eq!(action_error.index, Some(3));
    assert_matches!(
        action_error.kind,
        ActionErrorKind::FunctionCallError(FunctionCallError::MethodResolveError(_))
    );
}

#[test]
fn test_deploy_and_call_local_receipts() {
    let (runtime, tries, root, apply_state, signer, epoch_info_provider) =
        setup_runtime(to_yocto(1_000_000), to_yocto(500_000), 10u64.pow(15));

    let tx1 = SignedTransaction::from_actions(
        1,
        alice_account(),
        alice_account(),
        &*signer,
        vec![Action::DeployContract(DeployContractAction {
            code: near_test_contracts::rs_contract().to_vec(),
        })],
        CryptoHash::default(),
        0,
    );

    let tx2 = SignedTransaction::from_actions(
        2,
        alice_account(),
        alice_account(),
        &*signer,
        vec![
            Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "log_something".to_string(),
                args: vec![],
                gas: MAX_ATTACHED_GAS / 2,
                deposit: 0,
            })),
            Action::DeployContract(DeployContractAction {
                code: near_test_contracts::trivial_contract().to_vec(),
            }),
            Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "log_something".to_string(),
                args: vec![],
                gas: MAX_ATTACHED_GAS / 2,
                deposit: 0,
            })),
        ],
        CryptoHash::default(),
        0,
    );

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            GlobalShard::new(tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default())),
            &None,
            &apply_state,
            &[],
            &[tx1, tx2],
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    let (o1, o2) = assert_matches!(
        &apply_result.outcomes[..],
        [_, _, ExecutionOutcomeWithId { id: _, outcome: o1 }, ExecutionOutcomeWithId { id: _, outcome: o2 }] => (o1, o2)
    );
    assert_eq!(o1.status, ExecutionStatus::SuccessValue(vec![]));
    assert_eq!(&o2.logs[..], ["hello"]);
    let action_error = assert_matches!(
        &o2.status,
        ExecutionStatus::Failure(TxExecutionError::ActionError(ae)) => ae
    );
    assert_eq!(action_error.index, Some(2));
    assert_matches!(
        action_error.kind,
        ActionErrorKind::FunctionCallError(FunctionCallError::MethodResolveError(_))
    );
}

#[test]
#[cfg(feature = "protocol_feature_global_contracts")]
fn test_deploy_invalid_global_contract() {
    let (runtime, tries, root, apply_state, signer, epoch_info_provider) =
        setup_runtime(to_yocto(1_000_000), to_yocto(500_000), 10u64.pow(15));

    let code = vec![1, 2, 3, 4, 5];
    let tx = SignedTransaction::deploy_permanent_contract(
        1,
        &alice_account(),
        code,
        &*signer,
        CryptoHash::default(),
    );

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            GlobalShard::new(tries.get_trie_for_shard(ShardUId::global(), CryptoHash::default())),
            &None,
            &apply_state,
            &[],
            &[tx],
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    let (o1, o2) = assert_matches!(
        &apply_result.outcomes[..],
        [ExecutionOutcomeWithId { id: _, outcome: o1 }, ExecutionOutcomeWithId { id: _, outcome: o2 }] => (o1, o2)
    );
    assert_matches!(o1.status, ExecutionStatus::SuccessReceiptId(_));
    assert_matches!(o2.status, ExecutionStatus::Failure(TxExecutionError::ActionError(_)));
}

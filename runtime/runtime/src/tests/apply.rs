use super::{GAS_PRICE, to_yocto};
use crate::config::safe_add_gas;
use crate::congestion_control::{compute_receipt_congestion_gas, compute_receipt_size};
use crate::tests::{
    MAX_ATTACHED_GAS, create_receipt_for_create_account, create_receipt_with_actions,
    set_sha256_cost,
};
use crate::{ApplyResult, ApplyState, Runtime, ValidatorAccountsUpdate};
use crate::{SignedValidPeriodTransactions, total_prepaid_exec_fees};
use assert_matches::assert_matches;
use near_crypto::{InMemorySigner, KeyType, PublicKey, Signer};
use near_o11y::testonly::init_test_logger;
use near_parameters::{ActionCosts, RuntimeConfig};
use near_primitives::account::AccessKey;
use near_primitives::action::delegate::{DelegateAction, NonDelegateAction, SignedDelegateAction};
use near_primitives::action::{Action, DeleteAccountAction};
use near_primitives::apply::ApplyChunkReason;
use near_primitives::bandwidth_scheduler::BlockBandwidthRequests;
use near_primitives::congestion_info::{
    BlockCongestionInfo, CongestionControl, CongestionInfo, ExtendedCongestionInfo,
};
use near_primitives::errors::{ActionErrorKind, FunctionCallError, TxExecutionError};
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::receipt::{ActionReceipt, Receipt, ReceiptEnum, ReceiptPriority, ReceiptV0};
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::state::PartialState;
use near_primitives::stateless_validation::contract_distribution::CodeHash;
use near_primitives::test_utils::{MockEpochInfoProvider, account_new};
use near_primitives::transaction::{
    AddKeyAction, DeleteKeyAction, DeployContractAction, ExecutionOutcomeWithId, ExecutionStatus,
    FunctionCallAction, SignedTransaction, TransferAction, ValidatedTransaction,
};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{
    AccountId, Balance, EpochId, EpochInfoProvider, Gas, MerkleHash, ShardId, StateChangeCause,
};
use near_primitives::utils::create_receipt_id_from_transaction;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_store::test_utils::TestTriesBuilder;
use near_store::trie::AccessOptions;
use near_store::trie::receipts_column_helper::ShardsOutgoingReceiptBuffer;
use near_store::{
    MissingTrieValueContext, ShardTries, StorageError, Trie, get_account, set_access_key,
    set_account,
};
use near_vm_runner::{ContractCode, FilesystemContractRuntimeCache};
use std::collections::HashSet;
use std::sync::Arc;
use testlib::runtime_utils::{alice_account, bob_account};

/***************/
/* Apply tests */
/***************/

fn setup_runtime(
    initial_accounts: Vec<AccountId>,
    initial_balance: Balance,
    initial_locked: Balance,
    gas_limit: Gas,
) -> (Runtime, ShardTries, CryptoHash, ApplyState, Vec<Arc<Signer>>, impl EpochInfoProvider) {
    let epoch_info_provider = MockEpochInfoProvider::default();
    let shard_layout = epoch_info_provider.shard_layout(&EpochId::default()).unwrap();
    let shard_uid = shard_layout.shard_uids().next().unwrap();

    let accounts_with_keys = initial_accounts
        .into_iter()
        .map(|account_id| {
            let signer = Arc::new(InMemorySigner::test_signer(&account_id));
            (account_id, vec![signer])
        })
        .collect::<Vec<_>>();

    let (runtime, tries, state_root, apply_state, signers) = setup_runtime_for_shard(
        accounts_with_keys,
        initial_balance,
        initial_locked,
        gas_limit,
        shard_uid,
        &shard_layout,
    );

    (runtime, tries, state_root, apply_state, signers, epoch_info_provider)
}

/// Same general idea as `setup_runtime`, but you can pass multiple keys
/// for each account.
fn setup_runtime_with_keys(
    accounts_with_keys: Vec<(AccountId, Vec<Arc<Signer>>)>,
    initial_balance: Balance,
    initial_locked: Balance,
    gas_limit: Gas,
) -> (Runtime, ShardTries, CryptoHash, ApplyState, Vec<Arc<Signer>>, impl EpochInfoProvider) {
    let epoch_info_provider = MockEpochInfoProvider::default();
    let shard_layout = epoch_info_provider.shard_layout(&EpochId::default()).unwrap();
    let shard_uid = shard_layout.shard_uids().next().unwrap();

    let (runtime, tries, state_root, apply_state, signers) = setup_runtime_for_shard(
        accounts_with_keys,
        initial_balance,
        initial_locked,
        gas_limit,
        shard_uid,
        &shard_layout,
    );

    (runtime, tries, state_root, apply_state, signers, epoch_info_provider)
}

fn setup_runtime_for_shard(
    accounts_with_keys: Vec<(AccountId, Vec<Arc<Signer>>)>,
    initial_balance: Balance,
    initial_locked: Balance,
    gas_limit: Gas,
    shard_uid: ShardUId,
    shard_layout: &ShardLayout,
) -> (Runtime, ShardTries, CryptoHash, ApplyState, Vec<Arc<Signer>>) {
    let tries = TestTriesBuilder::new().build();
    let root = MerkleHash::default();
    let runtime = Runtime::new();
    let mut initial_state = tries.new_trie_update(shard_uid, root);

    let signers = accounts_with_keys
        .into_iter()
        .flat_map(|(account_id, signers_for_account)| {
            let mut initial_account = account_new(initial_balance, CryptoHash::default());

            initial_account.set_storage_usage(182);
            initial_account.set_locked(initial_locked);

            set_account(&mut initial_state, account_id.clone(), &initial_account);

            signers_for_account
                .into_iter()
                .map(|signer| {
                    set_access_key(
                        &mut initial_state,
                        account_id.clone(),
                        signer.public_key(),
                        &AccessKey::full_access(),
                    );
                    signer
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    initial_state.commit(StateChangeCause::InitialState);
    let trie_changes = initial_state.finalize().unwrap().trie_changes;
    let mut store_update = tries.store_update();
    let root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
    store_update.commit().unwrap();
    let contract_cache = FilesystemContractRuntimeCache::test().unwrap();
    let shard_ids = shard_layout.shard_ids();
    let shards_congestion_info =
        shard_ids.map(|shard_id| (shard_id, ExtendedCongestionInfo::default())).collect();
    let congestion_info = BlockCongestionInfo::new(shards_congestion_info);
    let apply_state = ApplyState {
        apply_reason: ApplyChunkReason::UpdateTrackedShard,
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
        congestion_info,
        bandwidth_requests: BlockBandwidthRequests::empty(),
        trie_access_tracker_state: Default::default(),
    };

    (runtime, tries, root, apply_state, signers)
}

#[test]
fn test_apply_no_op() {
    let (runtime, tries, root, apply_state, _, epoch_info_provider) =
        setup_runtime(vec![alice_account()], to_yocto(1_000_000), 0, 10u64.pow(15));
    runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            &apply_state,
            &[],
            SignedValidPeriodTransactions::empty(),
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
        setup_runtime(vec![alice_account()], to_yocto(1_000_000), initial_locked, 10u64.pow(15));

    let validator_accounts_update = ValidatorAccountsUpdate {
        stake_info: vec![(alice_account(), initial_locked)].into_iter().collect(),
        validator_rewards: vec![(alice_account(), reward)].into_iter().collect(),
        last_proposals: Default::default(),
        protocol_treasury_account_id: None,
    };

    runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &Some(validator_accounts_update),
            &apply_state,
            &[Receipt::new_balance_refund(
                &alice_account(),
                small_refund,
                ReceiptPriority::NoPriority,
            )],
            SignedValidPeriodTransactions::empty(),
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
        setup_runtime(vec![alice_account()], initial_balance, initial_locked, gas_limit);

    let n = 10;
    let receipts = generate_refund_receipts(small_transfer, n);
    let shard_uid = ShardUId::single_shard();

    // Checking n receipts delayed
    for i in 1..=n + 3 {
        let prev_receipts: &[Receipt] = if i == 1 { &receipts } else { &[] };
        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(shard_uid, root),
                &None,
                &apply_state,
                prev_receipts,
                SignedValidPeriodTransactions::empty(),
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        root = commit_apply_result(&apply_result, &mut apply_state, &tries, shard_uid);
        let state = tries.new_trie_update(shard_uid, root);
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
    let (runtime, tries, mut root, mut apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        initial_balance,
        initial_locked,
        gas_limit,
    );

    let n = 10;
    let receipts = generate_receipts(small_transfer, n);
    let shard_uid = ShardUId::single_shard();

    // Checking n receipts delayed by 1 + 3 extra
    for i in 1..=n + 3 {
        let prev_receipts: &[Receipt] = if i == 1 { &receipts } else { &[] };
        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(shard_uid, root),
                &None,
                &apply_state,
                prev_receipts,
                SignedValidPeriodTransactions::empty(),
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        root = commit_apply_result(&apply_result, &mut apply_state, &tries, shard_uid);

        let state = tries.new_trie_update(shard_uid, root);
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
        setup_runtime(vec![alice_account(), bob_account()], initial_balance, initial_locked, 1);

    let receipt_gas_cost = apply_state.config.fees.fee(ActionCosts::new_action_receipt).exec_fee()
        + apply_state.config.fees.fee(ActionCosts::transfer).exec_fee();
    apply_state.gas_limit = Some(receipt_gas_cost * 3);

    let n = 40;
    let receipts = generate_receipts(small_transfer, n);
    let mut receipt_chunks = receipts.chunks_exact(4);
    let shard_uid = ShardUId::single_shard();

    // Every time we'll process 3 receipts, so we need n / 3 rounded up. Then we do 3 extra.
    for i in 1..=n / 3 + 3 {
        let prev_receipts: &[Receipt] = receipt_chunks.next().unwrap_or_default();
        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(shard_uid, root),
                &None,
                &apply_state,
                prev_receipts,
                SignedValidPeriodTransactions::empty(),
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        root = commit_apply_result(&apply_result, &mut apply_state, &tries, shard_uid);
        let state = tries.new_trie_update(shard_uid, root);
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
        setup_runtime(vec![alice_account(), bob_account()], initial_balance, initial_locked, 1);

    let receipt_gas_cost = apply_state.config.fees.fee(ActionCosts::new_action_receipt).exec_fee()
        + apply_state.config.fees.fee(ActionCosts::transfer).exec_fee();

    let n = 120;
    let receipts = generate_receipts(small_transfer, n);
    let mut receipt_chunks = receipts.chunks_exact(4);
    let shard_uid = ShardUId::single_shard();

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
                tries.get_trie_for_shard(shard_uid, root),
                &None,
                &apply_state,
                prev_receipts,
                SignedValidPeriodTransactions::empty(),
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        root = commit_apply_result(&apply_result, &mut apply_state, &tries, shard_uid);
        let state = tries.new_trie_update(shard_uid, root);
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
    let signer = Arc::new(InMemorySigner::test_signer(&sender_id));
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
    let (runtime, tries, mut root, mut apply_state, signers, epoch_info_provider) =
        setup_runtime(vec![alice_account(), bob_account()], initial_balance, initial_locked, 1);

    let receipt_exec_gas_fee = 1000;
    let mut free_config = RuntimeConfig::free();
    let fees = Arc::make_mut(&mut free_config.fees);
    fees.action_fees[ActionCosts::new_action_receipt].execution = receipt_exec_gas_fee;
    apply_state.config = Arc::new(free_config);
    // This allows us to execute 3 receipts per apply.
    apply_state.gas_limit = Some(receipt_exec_gas_fee * 3);

    let num_receipts = 6;
    let receipts = generate_receipts(small_transfer, num_receipts);
    let shard_uid = ShardUId::single_shard();

    let num_transactions = 9;
    let local_transactions = (0..num_transactions)
        .map(|i| {
            SignedTransaction::send_money(
                i + 1,
                alice_account(),
                alice_account(),
                &*signers[0],
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
            tries.get_trie_for_shard(shard_uid, root),
            &None,
            &apply_state,
            &receipts[0..2],
            SignedValidPeriodTransactions::new(local_transactions[0..4].to_vec(), vec![true; 4]),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    root = commit_apply_result(&apply_result, &mut apply_state, &tries, shard_uid);

    assert_eq!(
        apply_result.outcomes.iter().map(|o| o.id).collect::<Vec<_>>(),
        vec![
            local_transactions[0].get_hash(), // tx 0
            local_transactions[1].get_hash(), // tx 1
            local_transactions[2].get_hash(), // tx 2
            local_transactions[3].get_hash(), // tx 3 - the TX is processed, but the receipt is delayed
            create_receipt_id_from_transaction(
                PROTOCOL_VERSION,
                ValidatedTransaction::new_for_test(local_transactions[0].clone()).to_hash(),
                &apply_state.block_hash,
                apply_state.block_height,
            ), // receipt for tx 0
            create_receipt_id_from_transaction(
                PROTOCOL_VERSION,
                ValidatedTransaction::new_for_test(local_transactions[1].clone()).to_hash(),
                &apply_state.block_hash,
                apply_state.block_height,
            ), // receipt for tx 1
            create_receipt_id_from_transaction(
                PROTOCOL_VERSION,
                ValidatedTransaction::new_for_test(local_transactions[2].clone()).to_hash(),
                &apply_state.block_hash,
                apply_state.block_height,
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
            tries.get_trie_for_shard(shard_uid, root),
            &None,
            &apply_state,
            &receipts[2..3],
            SignedValidPeriodTransactions::new(local_transactions[4..5].to_vec(), vec![true]),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    let mut store_update = tries.store_update();
    let root = tries.apply_all(&apply_result.trie_changes, shard_uid, &mut store_update);
    store_update.commit().unwrap();

    assert_eq!(
        apply_result.outcomes.iter().map(|o| o.id).collect::<Vec<_>>(),
        vec![
            local_transactions[4].get_hash(), // tx 4
            create_receipt_id_from_transaction(
                PROTOCOL_VERSION,
                ValidatedTransaction::new_for_test(local_transactions[4].clone()).to_hash(),
                &apply_state.block_hash,
                apply_state.block_height,
            ), // receipt for tx 4
            create_receipt_id_from_transaction(
                PROTOCOL_VERSION,
                ValidatedTransaction::new_for_test(local_transactions[3].clone()).to_hash(),
                &apply_state.block_hash,
                apply_state.block_height,
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
            tries.get_trie_for_shard(shard_uid, root),
            &None,
            &apply_state,
            &receipts[3..4],
            SignedValidPeriodTransactions::new(local_transactions[5..9].to_vec(), vec![true; 4]),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    let mut store_update = tries.store_update();
    let root = tries.apply_all(&apply_result.trie_changes, shard_uid, &mut store_update);
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
                ValidatedTransaction::new_for_test(local_transactions[5].clone()).to_hash(),
                &apply_state.block_hash,
                apply_state.block_height,
            ), // receipt for tx 5
            create_receipt_id_from_transaction(
                PROTOCOL_VERSION,
                ValidatedTransaction::new_for_test(local_transactions[6].clone()).to_hash(),
                &apply_state.block_hash,
                apply_state.block_height,
            ), // receipt for tx 6
            create_receipt_id_from_transaction(
                PROTOCOL_VERSION,
                ValidatedTransaction::new_for_test(local_transactions[7].clone()).to_hash(),
                &apply_state.block_hash,
                apply_state.block_height,
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
            tries.get_trie_for_shard(shard_uid, root),
            &None,
            &apply_state,
            &receipts[4..5],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    let mut store_update = tries.store_update();
    let root = tries.apply_all(&apply_result.trie_changes, shard_uid, &mut store_update);
    store_update.commit().unwrap();

    assert_eq!(
        apply_result.outcomes.iter().map(|o| o.id).collect::<Vec<_>>(),
        vec![
            *receipts[1].receipt_id(), // receipt #1
            *receipts[2].receipt_id(), // receipt #2
            create_receipt_id_from_transaction(
                PROTOCOL_VERSION,
                ValidatedTransaction::new_for_test(local_transactions[8].clone()).to_hash(),
                &apply_state.block_hash,
                apply_state.block_height,
            ), // receipt for tx 8
        ],
        "STEP #4 failed",
    );

    // STEP #5. Pass no new TXs and 1 receipt R#5.
    // We process R#3, R#4, R#5.
    // The new delayed queue is empty.
    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(shard_uid, root),
            &None,
            &apply_state,
            &receipts[5..6],
            SignedValidPeriodTransactions::empty(),
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
    let (runtime, tries, root, apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        initial_balance,
        initial_locked,
        gas_limit,
    );

    let n = 1;
    let mut receipts = generate_receipts(small_transfer, n);
    if let ReceiptEnum::Action(action_receipt) = receipts.get_mut(0).unwrap().receipt_mut() {
        action_receipt.gas_price = GAS_PRICE / 10;
    }

    let result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            &apply_state,
            &receipts,
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    assert_eq!(result.stats.balance.gas_deficit_amount, result.stats.balance.tx_burnt_amount * 9)
}

/// Apply a transfer receipt that was purchased at a higher gas price than
/// current, then check that we burn the correct amount.
#[test]
fn test_apply_surplus_gas_for_transfer() {
    let initial_balance = to_yocto(1_000_000);
    let initial_locked = to_yocto(500_000);
    let small_transfer = to_yocto(10_000);
    let gas_limit = 10u64.pow(15);
    let (runtime, tries, root, apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        initial_balance,
        initial_locked,
        gas_limit,
    );
    let gas_price = GAS_PRICE * 10;

    let n = 1;
    let mut receipts = generate_receipts(small_transfer, n);
    if let ReceiptEnum::Action(action_receipt) = receipts.get_mut(0).unwrap().receipt_mut() {
        action_receipt.gas_price = gas_price;
    }

    let result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            &apply_state,
            &receipts,
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    let fees = &apply_state.config.fees;
    let exec_gas = fees.fee(ActionCosts::new_action_receipt).exec_fee()
        + fees.fee(ActionCosts::transfer).exec_fee();

    let expected_burnt_amount = if fees.refund_gas_price_changes {
        Balance::from(exec_gas) * GAS_PRICE
    } else {
        Balance::from(exec_gas) * gas_price
    };
    let expected_receipts = if fees.refund_gas_price_changes {
        // refund the surplus
        1
    } else {
        // don't refund the surplus
        0
    };

    assert_eq!(result.stats.balance.gas_deficit_amount, 0);
    assert_eq!(result.stats.balance.tx_burnt_amount, expected_burnt_amount);
    assert_eq!(result.outgoing_receipts.len(), expected_receipts);
}

#[test]
fn test_apply_deficit_gas_for_function_call_covered() {
    let initial_balance = to_yocto(1_000_000);
    let initial_locked = to_yocto(500_000);
    let gas_limit = 10u64.pow(15);
    let (runtime, tries, root, apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        initial_balance,
        initial_locked,
        gas_limit,
    );

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
    let expected_gas_burnt_amount = if apply_state.config.fees.refund_gas_price_changes {
        Balance::from(expected_gas_burnt) * GAS_PRICE
    } else {
        Balance::from(expected_gas_burnt) * gas_price
    };
    // With gas refund penalties enabled, we should see a reduced refund value
    let unspent_gas = (total_receipt_cost - expected_gas_burnt_amount) / gas_price;
    let refund_penalty = apply_state.config.fees.gas_penalty_for_gas_refund(unspent_gas as u64);
    let expected_refund =
        total_receipt_cost - expected_gas_burnt_amount - Balance::from(refund_penalty) * gas_price;

    let result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            &apply_state,
            &receipts,
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    if apply_state.config.fees.refund_gas_price_changes {
        // We used part of the prepaid gas to paying extra fees.
        assert_eq!(result.stats.balance.gas_deficit_amount, 0);
    } else {
        assert_eq!(
            result.stats.balance.gas_deficit_amount,
            Balance::from(expected_gas_burnt) * (GAS_PRICE - gas_price)
        );
    }
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
    let (runtime, tries, root, apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        initial_balance,
        initial_locked,
        gas_limit,
    );

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
    let expected_deficit = if apply_state.config.fees.refund_gas_price_changes {
        // Used full prepaid gas, but it still not enough to cover deficit.
        let expected_gas_burnt_amount = Balance::from(expected_gas_burnt) * GAS_PRICE;
        expected_gas_burnt_amount - total_receipt_cost
    } else {
        // The "deficit" is simply the value change due to gas price changes
        Balance::from(expected_gas_burnt) * (GAS_PRICE - gas_price)
    };

    let result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            &apply_state,
            &receipts,
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    assert_eq!(result.stats.balance.gas_deficit_amount, expected_deficit);
    if apply_state.config.fees.refund_gas_price_changes {
        // Burnt all the fees + all prepaid gas.
        assert_eq!(result.stats.balance.tx_burnt_amount, total_receipt_cost);
        assert_eq!(result.outgoing_receipts.len(), 0);
    } else {
        // The deficit does not affect refunds in this config, hence we expect a
        // normal refund of the unspent gas. However, this is small enough to
        // cancel out, so we add the refund cost to tx_burnt and expect no
        // refund. Like in the other case, this ends up burning all gas and not
        // refunding anything.
        assert_eq!(result.outgoing_receipts.len(), 0);
        assert_eq!(result.stats.balance.tx_burnt_amount, total_receipt_cost);
    }
}

#[test]
fn test_apply_surplus_gas_for_function_call() {
    let initial_balance = to_yocto(1_000_000);
    let initial_locked = to_yocto(500_000);
    let gas_limit = 10u64.pow(15);
    let (runtime, tries, root, apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        initial_balance,
        initial_locked,
        gas_limit,
    );

    let gas = 2 * 10u64.pow(14);
    let gas_price = GAS_PRICE * 10;
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
    let expected_gas_burnt_amount = if apply_state.config.fees.refund_gas_price_changes {
        Balance::from(expected_gas_burnt) * GAS_PRICE
    } else {
        Balance::from(expected_gas_burnt) * gas_price
    };

    // With gas refund penalties enabled, we should see a reduced refund value
    let unspent_gas = (total_receipt_cost - expected_gas_burnt_amount) / gas_price;
    let refund_penalty = apply_state.config.fees.gas_penalty_for_gas_refund(unspent_gas as u64);
    let expected_refund =
        total_receipt_cost - expected_gas_burnt_amount - Balance::from(refund_penalty) * gas_price;

    let result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            &apply_state,
            &receipts,
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    assert_eq!(result.stats.balance.gas_deficit_amount, 0, "expected surplus");
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
fn test_delete_key_add_key() {
    let initial_locked = to_yocto(500_000);
    let (runtime, tries, root, apply_state, signers, epoch_info_provider) =
        setup_runtime(vec![alice_account()], to_yocto(1_000_000), initial_locked, 10u64.pow(15));

    let state_update = tries.new_trie_update(ShardUId::single_shard(), root);
    let initial_account_state = get_account(&state_update, &alice_account()).unwrap().unwrap();

    let actions = vec![
        Action::DeleteKey(Box::new(DeleteKeyAction { public_key: signers[0].public_key() })),
        Action::AddKey(Box::new(AddKeyAction {
            public_key: signers[0].public_key(),
            access_key: AccessKey::full_access(),
        })),
    ];

    let receipts = vec![create_receipt_with_actions(alice_account(), signers[0].clone(), actions)];

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            &apply_state,
            &receipts,
            SignedValidPeriodTransactions::empty(),
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
    let (runtime, tries, root, apply_state, signers, epoch_info_provider) =
        setup_runtime(vec![alice_account()], to_yocto(1_000_000), initial_locked, 10u64.pow(15));

    let mut state_update = tries.new_trie_update(ShardUId::single_shard(), root);
    let mut initial_account_state = get_account(&state_update, &alice_account()).unwrap().unwrap();
    initial_account_state.set_storage_usage(10);
    set_account(&mut state_update, alice_account(), &initial_account_state);
    state_update.commit(StateChangeCause::InitialState);
    let trie_changes = state_update.finalize().unwrap().trie_changes;
    let mut store_update = tries.store_update();
    let root = tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit().unwrap();

    let actions =
        vec![Action::DeleteKey(Box::new(DeleteKeyAction { public_key: signers[0].public_key() }))];

    let receipts = vec![create_receipt_with_actions(alice_account(), signers[0].clone(), actions)];

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            &apply_state,
            &receipts,
            SignedValidPeriodTransactions::empty(),
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

#[test]
#[cfg(target_arch = "x86_64")]
fn test_contract_precompilation() {
    use super::create_receipt_with_actions;

    let initial_balance = to_yocto(1_000_000);
    let initial_locked = to_yocto(500_000);
    let gas_limit = 10u64.pow(15);
    let (runtime, tries, root, apply_state, signers, epoch_info_provider) =
        setup_runtime(vec![alice_account()], initial_balance, initial_locked, gas_limit);

    let wasm_code = near_test_contracts::rs_contract().to_vec();
    let actions = vec![Action::DeployContract(DeployContractAction { code: wasm_code.clone() })];

    let receipts = vec![create_receipt_with_actions(alice_account(), signers[0].clone(), actions)];

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            &apply_state,
            &receipts,
            SignedValidPeriodTransactions::empty(),
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
    let (runtime, tries, mut root, mut apply_state, signers, epoch_info_provider) =
        setup_runtime(vec![alice_account()], to_yocto(1_000_000), to_yocto(500_000), 1);

    let shard_uid = ShardUId::single_shard();

    let sha256_cost = set_sha256_cost(&mut apply_state, 1_000_000u64, 10_000_000_000_000u64);
    // This allows us to execute 1 receipt with a function call per apply.
    apply_state.gas_limit = Some(sha256_cost.compute);

    let deploy_contract_receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::DeployContract(DeployContractAction {
            code: near_test_contracts::rs_contract().to_vec(),
        })],
    );

    let first_call_receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "ext_sha256".to_string(),
            args: b"first".to_vec(),
            gas: sha256_cost.gas,
            deposit: 0,
        }))],
    );

    let second_call_receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "ext_sha256".to_string(),
            args: b"second".to_vec(),
            gas: sha256_cost.gas,
            deposit: 0,
        }))],
    );

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(shard_uid, root),
            &None,
            &apply_state,
            &[
                deploy_contract_receipt.clone(),
                first_call_receipt.clone(),
                second_call_receipt.clone(),
            ],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    root = commit_apply_result(&apply_result, &mut apply_state, &tries, shard_uid);

    // Only first two receipts should fit into the chunk due to the compute usage limit.
    assert_eq!(apply_result.delayed_receipts_count, 1);
    assert_matches!(&apply_result.outcomes[..], [first, second] => {
        assert_eq!(first.id, *deploy_contract_receipt.receipt_id());
        assert_matches!(first.outcome.status, ExecutionStatus::SuccessValue(_));

        assert_eq!(second.id, *first_call_receipt.receipt_id());
        assert_eq!(second.outcome.compute_usage.unwrap(), sha256_cost.compute);
        assert_matches!(second.outcome.status, ExecutionStatus::SuccessValue(_));
    });

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(shard_uid, root),
            &None,
            &apply_state,
            &[],
            SignedValidPeriodTransactions::empty(),
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
    let (runtime, tries, root, apply_state, signers, epoch_info_provider) =
        setup_runtime(vec![alice_account()], to_yocto(1_000_000), to_yocto(500_000), 10u64.pow(15));

    let deploy_contract_receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::DeployContract(DeployContractAction {
            code: near_test_contracts::rs_contract().to_vec(),
        })],
    );

    let first_call_receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
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
            &None,
            &apply_state,
            &[deploy_contract_receipt.clone(), first_call_receipt.clone()],
            SignedValidPeriodTransactions::empty(),
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
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        to_yocto(1_000_000),
        to_yocto(500_000),
        10u64.pow(15),
    );

    apply_state.config = Arc::new(RuntimeConfig::free());

    let contract_code = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);
    let create_acc_fn = |account_id: AccountId, signer: Arc<Signer>| {
        create_receipt_with_actions(
            account_id,
            signer,
            vec![Action::DeployContract(DeployContractAction {
                code: contract_code.code().to_vec(),
            })],
        )
    };

    let trie = tries
        .get_trie_for_shard(ShardUId::single_shard(), root)
        .recording_reads_with_proof_size_limit(
            apply_state.config.witness_config.main_storage_proof_size_soft_limit,
        );
    let apply_result = runtime
        .apply(
            trie,
            &None,
            &apply_state,
            &[
                create_acc_fn(alice_account(), signers[0].clone()),
                create_acc_fn(bob_account(), signers[1].clone()),
            ],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(apply_result.delayed_receipts_count, 0);

    let mut store_update = tries.store_update();
    let root =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit().unwrap();

    // Change main_storage_proof_size_soft_limit to the storage size in order to let
    // the first receipt go through but not the second one.
    let mut runtime_config = RuntimeConfig::free();
    runtime_config.witness_config.main_storage_proof_size_soft_limit = 300;
    apply_state.config = Arc::new(runtime_config);

    let function_call_fn = |account_id: AccountId, signer: Arc<Signer>| {
        create_receipt_with_actions(
            account_id,
            signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "ext_sha256".to_string(),
                args: b"first".to_vec(),
                gas: 1,
                deposit: 0,
            }))],
        )
    };

    let trie = tries
        .get_trie_for_shard(ShardUId::single_shard(), root)
        .recording_reads_with_proof_size_limit(
            apply_state.config.witness_config.main_storage_proof_size_soft_limit,
        );

    // The function call to bob_account should hit the main_storage_proof_size_soft_limit
    let apply_result = runtime
        .apply(
            trie,
            &None,
            &apply_state,
            &[
                function_call_fn(alice_account(), signers[0].clone()),
                function_call_fn(bob_account(), signers[1].clone()),
            ],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    // We expect function_call_fn(bob_account()) to be in delayed receipts
    assert_eq!(apply_result.delayed_receipts_count, 1);

    // Since contracts are excluded from the partial state, we will get missing trie error below.
    let partial_storage = apply_result.proof.unwrap();
    let storage = Trie::from_recorded_storage(partial_storage, root, false);
    let code_key = TrieKey::ContractCode { account_id: alice_account() };
    assert_matches!(
        storage.get(&code_key.to_vec(), AccessOptions::DEFAULT),
        Err(StorageError::MissingTrieValue(MissingTrieValueContext::TrieMemoryPartialStorage, _))
    );
    let code_key = TrieKey::ContractCode { account_id: bob_account() };
    assert_matches!(
        storage.get(&code_key.to_vec(), AccessOptions::DEFAULT),
        Err(StorageError::MissingTrieValue(MissingTrieValueContext::TrieMemoryPartialStorage, _))
    );
}

// Tests excluding contract code from state witness and recording of contract deployments and function calls.
#[test]
fn test_exclude_contract_code_from_witness() {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        to_yocto(1_000_000),
        to_yocto(500_000),
        10u64.pow(15),
    );

    const CONTRACT_SIZE: usize = 5000;

    // Set the storage proof soft-limit to the size of the contract.
    // Since contract code is not included in the storage proof, both function calls below pass the proof soft-limit.
    let mut runtime_config = RuntimeConfig::test();
    runtime_config.witness_config.main_storage_proof_size_soft_limit = CONTRACT_SIZE;
    apply_state.config = Arc::new(runtime_config);

    let contract_code =
        ContractCode::new(near_test_contracts::sized_contract(CONTRACT_SIZE).to_vec(), None);
    let create_acc_fn = |account_id: AccountId, signer: Arc<Signer>| {
        create_receipt_with_actions(
            account_id,
            signer,
            vec![Action::DeployContract(DeployContractAction {
                code: contract_code.code().to_vec(),
            })],
        )
    };

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &[
                create_acc_fn(alice_account(), signers[0].clone()),
                create_acc_fn(bob_account(), signers[1].clone()),
            ],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(apply_result.delayed_receipts_count, 0);
    assert_eq!(apply_result.contract_updates.contract_accesses, HashSet::new());
    // Since both accounts deploy the same contract, we expect only one contract deploy.
    assert_eq!(
        apply_result.contract_updates.contract_deploy_hashes(),
        HashSet::from([CodeHash(*contract_code.hash())])
    );

    let mut store_update = tries.store_update();
    let root =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit().unwrap();

    let function_call_fn = |account_id: AccountId, signer: Arc<Signer>| {
        create_receipt_with_actions(
            account_id,
            signer,
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
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &[
                function_call_fn(alice_account(), signers[0].clone()),
                function_call_fn(bob_account(), signers[1].clone()),
            ],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    // We expect that both receipts are included since the contract code is not included in the storage proof.
    assert_eq!(apply_result.delayed_receipts_count, 0);

    assert_eq!(apply_result.delayed_receipts_count, 0);
    // Since both accounts call the same contract, we expect only one contract access.
    assert_eq!(
        apply_result.contract_updates.contract_accesses,
        HashSet::from([CodeHash(*contract_code.hash())])
    );
    assert_eq!(apply_result.contract_updates.contract_deploy_hashes(), HashSet::new());

    // Check that the proof size is less than the contract size (since it is not included in the storage proof).
    let partial_storage = apply_result.proof.unwrap();
    let PartialState::TrieValues(storage_proof) = partial_storage.nodes.clone();
    let total_size: usize = storage_proof.iter().map(|v| v.len()).sum();
    assert!(total_size < CONTRACT_SIZE);

    // Check that both contracts are excluded from the storage proof.
    let storage = Trie::from_recorded_storage(partial_storage, root, false);
    let code_key = TrieKey::ContractCode { account_id: alice_account() };
    assert_matches!(
        storage.get(&code_key.to_vec(), AccessOptions::DEFAULT),
        Err(StorageError::MissingTrieValue(MissingTrieValueContext::TrieMemoryPartialStorage, _))
    );
    let code_key = TrieKey::ContractCode { account_id: bob_account() };
    assert_matches!(
        storage.get(&code_key.to_vec(), AccessOptions::DEFAULT),
        Err(StorageError::MissingTrieValue(MissingTrieValueContext::TrieMemoryPartialStorage, _))
    );
}

// Tests excluding contract code from state witness and recording of contract deployments and function calls
// with one of the function calls fail due to exceeding the gas limit.
#[test]
fn test_exclude_contract_code_from_witness_with_failed_call() {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        to_yocto(1_000_000),
        to_yocto(500_000),
        10u64.pow(15),
    );

    let sha256_cost = set_sha256_cost(&mut apply_state, 1_000_000u64, 10_000_000_000_000u64);
    // This allows us to execute 1 receipt with a function call per apply.
    apply_state.gas_limit = Some(sha256_cost.compute);

    let contract_code = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);
    let create_acc_fn = |account_id: AccountId, signer: Arc<Signer>| {
        create_receipt_with_actions(
            account_id,
            signer,
            vec![Action::DeployContract(DeployContractAction {
                code: contract_code.code().to_vec(),
            })],
        )
    };

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &[
                create_acc_fn(alice_account(), signers[0].clone()),
                create_acc_fn(bob_account(), signers[1].clone()),
            ],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(apply_result.delayed_receipts_count, 0);
    assert_eq!(apply_result.contract_updates.contract_accesses, HashSet::new());
    // Since both accounts deploy the same contract, we expect only one contract deploy.
    assert_eq!(
        apply_result.contract_updates.contract_deploy_hashes(),
        HashSet::from([CodeHash(*contract_code.hash())])
    );

    let mut store_update = tries.store_update();
    let root =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit().unwrap();

    let function_call_fn = |account_id: AccountId, signer: Arc<Signer>| {
        create_receipt_with_actions(
            account_id,
            signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "ext_sha256".to_string(),
                args: b"first".to_vec(),
                gas: sha256_cost.gas,
                deposit: 0,
            }))],
        )
    };

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &[
                function_call_fn(alice_account(), signers[0].clone()),
                function_call_fn(bob_account(), signers[1].clone()),
            ],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(apply_result.delayed_receipts_count, 1);
    // Since both accounts call the same contract, we expect only one contract access.
    assert_eq!(
        apply_result.contract_updates.contract_accesses,
        HashSet::from([CodeHash(*contract_code.hash())])
    );
    assert_eq!(apply_result.contract_updates.contract_deploy_hashes(), HashSet::new());

    // Check that both contracts are excluded from the storage proof.
    let partial_storage = apply_result.proof.unwrap();
    let storage = Trie::from_recorded_storage(partial_storage, root, false);
    let code_key = TrieKey::ContractCode { account_id: alice_account() };
    assert_matches!(
        storage.get(&code_key.to_vec(), AccessOptions::DEFAULT),
        Err(StorageError::MissingTrieValue(MissingTrieValueContext::TrieMemoryPartialStorage, _))
    );
    let code_key = TrieKey::ContractCode { account_id: bob_account() };
    assert_matches!(
        storage.get(&code_key.to_vec(), AccessOptions::DEFAULT),
        Err(StorageError::MissingTrieValue(MissingTrieValueContext::TrieMemoryPartialStorage, _))
    );
}

// Tests excluding contract code from state witness and recording of contract deployments and function calls
// where different contracts are deployed to different accounts, to check if we record code-hashes of both contracts.
#[test]
fn test_deploy_and_call_different_contracts() {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        to_yocto(1_000_000),
        to_yocto(500_000),
        1,
    );

    apply_state.config = Arc::new(RuntimeConfig::free());

    // We use different contract to check the code hashes in the output.
    let first_contract_code = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);
    let second_contract_code = ContractCode::new(near_test_contracts::sized_contract(100), None);

    let first_deploy_receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::DeployContract(DeployContractAction {
            code: first_contract_code.code().to_vec(),
        })],
    );

    let first_call_receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "ext_sha256".to_string(),
            args: b"first".to_vec(),
            gas: 1,
            deposit: 0,
        }))],
    );

    let second_deploy_receipt = create_receipt_with_actions(
        bob_account(),
        signers[1].clone(),
        vec![Action::DeployContract(DeployContractAction {
            code: second_contract_code.code().to_vec(),
        })],
    );

    let second_call_receipt = create_receipt_with_actions(
        bob_account(),
        signers[1].clone(),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "main".to_string(),
            args: Vec::new(),
            gas: 1,
            deposit: 0,
        }))],
    );

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &[first_deploy_receipt, second_deploy_receipt],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(apply_result.delayed_receipts_count, 0);
    assert_eq!(apply_result.contract_updates.contract_accesses, HashSet::new());
    assert_eq!(
        apply_result.contract_updates.contract_deploy_hashes(),
        HashSet::from([
            CodeHash(*first_contract_code.hash()),
            CodeHash(*second_contract_code.hash())
        ])
    );

    let mut store_update = tries.store_update();
    let root =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit().unwrap();

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &[first_call_receipt, second_call_receipt],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(apply_result.delayed_receipts_count, 0);
    assert_eq!(
        apply_result.contract_updates.contract_accesses,
        HashSet::from([
            CodeHash(*first_contract_code.hash()),
            CodeHash(*second_contract_code.hash())
        ])
    );
    assert_eq!(apply_result.contract_updates.contract_deploy_hashes(), HashSet::new());
}

// Similar to test_deploy_and_call_different_contracts, but one of the function calls fails.
#[test]
fn test_deploy_and_call_different_contracts_with_failed_call() {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        to_yocto(1_000_000),
        to_yocto(500_000),
        1,
    );

    let sha256_cost = set_sha256_cost(&mut apply_state, 1_000_000u64, 10_000_000_000_000u64);
    // This allows us to execute 1 receipt with a function call per apply.
    apply_state.gas_limit = Some(sha256_cost.compute);

    // We use different contract to check the code hashes in the output.
    let first_contract_code = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);
    let second_contract_code = ContractCode::new(near_test_contracts::sized_contract(100), None);

    let first_deploy_receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::DeployContract(DeployContractAction {
            code: first_contract_code.code().to_vec(),
        })],
    );

    let first_call_receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "ext_sha256".to_string(),
            args: b"first".to_vec(),
            gas: sha256_cost.gas,
            deposit: 0,
        }))],
    );

    let second_deploy_receipt = create_receipt_with_actions(
        bob_account(),
        signers[1].clone(),
        vec![Action::DeployContract(DeployContractAction {
            code: second_contract_code.code().to_vec(),
        })],
    );

    let second_call_receipt = create_receipt_with_actions(
        bob_account(),
        signers[1].clone(),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "main".to_string(),
            args: Vec::new(),
            gas: 1,
            deposit: 0,
        }))],
    );

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &[first_deploy_receipt, second_deploy_receipt],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(apply_result.delayed_receipts_count, 0);
    assert_eq!(apply_result.contract_updates.contract_accesses, HashSet::new());
    assert_eq!(
        apply_result.contract_updates.contract_deploy_hashes(),
        HashSet::from([
            CodeHash(*first_contract_code.hash()),
            CodeHash(*second_contract_code.hash())
        ])
    );

    let mut store_update = tries.store_update();
    let root =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit().unwrap();

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &[first_call_receipt, second_call_receipt],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(apply_result.delayed_receipts_count, 1);
    // Since the second call fails due to insufficient gas, only the first call is recorded.
    assert_eq!(
        apply_result.contract_updates.contract_accesses,
        HashSet::from([CodeHash(*first_contract_code.hash())])
    );
    assert_eq!(apply_result.contract_updates.contract_deploy_hashes(), HashSet::new());
}

// Tests excluding contract code from state witness and recording of contract deployments and function calls
// where different contracts are deployed to different accounts and all receipts are evaluated in the same call to apply.

#[test]
fn test_deploy_and_call_in_apply() {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        to_yocto(1_000_000),
        to_yocto(500_000),
        1,
    );

    apply_state.config = Arc::new(RuntimeConfig::free());

    // We use different contract to check the code hashes in the output.
    let first_contract_code = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);
    let second_contract_code = ContractCode::new(near_test_contracts::sized_contract(100), None);

    let first_deploy_receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::DeployContract(DeployContractAction {
            code: first_contract_code.code().to_vec(),
        })],
    );

    let first_call_receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "ext_sha256".to_string(),
            args: b"first".to_vec(),
            gas: 1,
            deposit: 0,
        }))],
    );

    let second_deploy_receipt = create_receipt_with_actions(
        bob_account(),
        signers[1].clone(),
        vec![Action::DeployContract(DeployContractAction {
            code: second_contract_code.code().to_vec(),
        })],
    );

    let second_call_receipt = create_receipt_with_actions(
        bob_account(),
        signers[1].clone(),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "main".to_string(),
            args: Vec::new(),
            gas: 1,
            deposit: 0,
        }))],
    );

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &[first_deploy_receipt, second_deploy_receipt, first_call_receipt, second_call_receipt],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(apply_result.delayed_receipts_count, 0);
    assert_eq!(apply_result.contract_updates.contract_accesses, HashSet::new());
    assert_eq!(
        apply_result.contract_updates.contract_deploy_hashes(),
        HashSet::from([
            CodeHash(*first_contract_code.hash()),
            CodeHash(*second_contract_code.hash())
        ])
    );
}

// Similar to test_deploy_and_call_in_apply but one of the function calls fail due to exceeding gas limit.
#[test]
fn test_deploy_and_call_in_apply_with_failed_call() {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        to_yocto(1_000_000),
        to_yocto(500_000),
        1,
    );

    let sha256_cost = set_sha256_cost(&mut apply_state, 1_000_000u64, 10_000_000_000_000u64);
    // This allows us to execute 1 receipt with a function call per apply.
    apply_state.gas_limit = Some(sha256_cost.compute);

    // We use different contract to check the code hashes in the output.
    let first_contract_code = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);
    let second_contract_code = ContractCode::new(near_test_contracts::sized_contract(100), None);

    let first_deploy_receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::DeployContract(DeployContractAction {
            code: first_contract_code.code().to_vec(),
        })],
    );

    let first_call_receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "ext_sha256".to_string(),
            args: b"first".to_vec(),
            gas: sha256_cost.gas,
            deposit: 0,
        }))],
    );

    let second_deploy_receipt = create_receipt_with_actions(
        bob_account(),
        signers[1].clone(),
        vec![Action::DeployContract(DeployContractAction {
            code: second_contract_code.code().to_vec(),
        })],
    );

    let second_call_receipt = create_receipt_with_actions(
        bob_account(),
        signers[1].clone(),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "main".to_string(),
            args: Vec::new(),
            gas: 1,
            deposit: 0,
        }))],
    );

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &[first_deploy_receipt, second_deploy_receipt, first_call_receipt, second_call_receipt],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(apply_result.delayed_receipts_count, 1);
    assert_eq!(apply_result.contract_updates.contract_accesses, HashSet::new());
    // We record both deployments even if the function call to one of them fails.
    assert_eq!(
        apply_result.contract_updates.contract_deploy_hashes(),
        HashSet::from([
            CodeHash(*first_contract_code.hash()),
            CodeHash(*second_contract_code.hash())
        ])
    );
}

// Tests that an existing contract is deployed and called from a different account.
#[test]
fn test_deploy_existing_contract_to_different_account() {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        to_yocto(1_000_000),
        to_yocto(500_000),
        10u64.pow(15),
    );

    apply_state.config = Arc::new(RuntimeConfig::free());

    let contract_code = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);

    // First deploy the contract to Alice account and call it.
    let first_deploy_receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::DeployContract(DeployContractAction { code: contract_code.code().to_vec() })],
    );
    let first_call_receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "ext_sha256".to_string(),
            args: b"first".to_vec(),
            gas: 1,
            deposit: 0,
        }))],
    );

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &[first_deploy_receipt, first_call_receipt],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(apply_result.delayed_receipts_count, 0);
    // No contract access is recorded because it was newly deployed.
    assert_eq!(apply_result.contract_updates.contract_accesses, HashSet::new());
    assert_eq!(
        apply_result.contract_updates.contract_deploy_hashes(),
        HashSet::from([CodeHash(*contract_code.hash())])
    );

    let mut store_update = tries.store_update();
    let root =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit().unwrap();

    // Second deploy the contract to Bob account and call it.
    let second_deploy_receipt = create_receipt_with_actions(
        bob_account(),
        signers[1].clone(),
        vec![Action::DeployContract(DeployContractAction { code: contract_code.code().to_vec() })],
    );
    let second_call_receipt = create_receipt_with_actions(
        bob_account(),
        signers[1].clone(),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "ext_sha256".to_string(),
            args: b"first".to_vec(),
            gas: 1,
            deposit: 0,
        }))],
    );

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &[second_deploy_receipt, second_call_receipt],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(apply_result.delayed_receipts_count, 0);
    // No contract access is recorded because it was newly deployed.
    assert_eq!(apply_result.contract_updates.contract_accesses, HashSet::new());
    // The contract deployment is still recorded even if it was deployed to another account before.
    assert_eq!(
        apply_result.contract_updates.contract_deploy_hashes(),
        HashSet::from([CodeHash(*contract_code.hash())])
    );
}

// Tests the case in which deploy and call are contained in the same receipt.
#[test]
fn test_deploy_and_call_in_same_receipt() {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        to_yocto(1_000_000),
        to_yocto(500_000),
        1,
    );

    apply_state.config = Arc::new(RuntimeConfig::free());

    let contract_code = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);
    let receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![
            Action::DeployContract(DeployContractAction { code: contract_code.code().to_vec() }),
            Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "ext_sha256".to_string(),
                args: b"first".to_vec(),
                gas: 1,
                deposit: 0,
            })),
        ],
    );

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &[receipt],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(apply_result.delayed_receipts_count, 0);
    assert_eq!(apply_result.contract_updates.contract_accesses, HashSet::new());
    assert_eq!(
        apply_result.contract_updates.contract_deploy_hashes(),
        HashSet::from([CodeHash(*contract_code.hash()),])
    );
}

// Tests the case in which deploy and call are contained in the same receipt and function call fails due to exceeding gas limit.
#[test]
fn test_deploy_and_call_in_same_receipt_with_failed_call() {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        to_yocto(1_000_000),
        to_yocto(500_000),
        1,
    );

    let sha256_cost = set_sha256_cost(&mut apply_state, 1_000_000u64, 10_000_000_000_000u64);
    // This allows us to execute 1 receipt with a function call per apply.
    apply_state.gas_limit = Some(sha256_cost.compute);

    let contract_code = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);
    let receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![
            Action::DeployContract(DeployContractAction { code: contract_code.code().to_vec() }),
            Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "ext_sha256".to_string(),
                args: b"first".to_vec(),
                gas: 1,
                deposit: 0,
            })),
        ],
    );

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &[receipt],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(apply_result.delayed_receipts_count, 0);
    assert_eq!(apply_result.contract_updates.contract_accesses, HashSet::new());
    assert_eq!(apply_result.contract_updates.contract_deploy_hashes(), HashSet::new());
}

// Tests the case in which a function call is made to an account with no contract deployed.
#[test]
fn test_call_account_without_contract() {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) =
        setup_runtime(vec![alice_account()], to_yocto(1_000_000), to_yocto(500_000), 1);

    apply_state.config = Arc::new(RuntimeConfig::free());

    let receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "main".to_string(),
            args: vec![],
            gas: 1,
            deposit: 0,
        }))],
    );

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &[receipt],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(apply_result.delayed_receipts_count, 0);
    assert_eq!(apply_result.contract_updates.contract_accesses, HashSet::new());
    assert_eq!(apply_result.contract_updates.contract_deploy_hashes(), HashSet::new());
}

/// Tests that we do not record the contract accesses when validating the chunk.
#[test]
fn test_contract_accesses_when_validating_chunk() {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) =
        setup_runtime(vec![alice_account()], to_yocto(1_000_000), to_yocto(500_000), 1);

    apply_state.config = Arc::new(RuntimeConfig::free());

    let contract_code = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);

    let deploy_receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::DeployContract(DeployContractAction { code: contract_code.code().to_vec() })],
    );

    let call_receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "ext_sha256".to_string(),
            args: b"first".to_vec(),
            gas: 1,
            deposit: 0,
        }))],
    );

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &[deploy_receipt],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(apply_result.delayed_receipts_count, 0);
    assert_eq!(
        apply_result.contract_updates.contract_deploy_hashes(),
        HashSet::from([CodeHash(*contract_code.hash())])
    );

    let mut store_update = tries.store_update();
    let root =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit().unwrap();

    // Apply chunk for updating the shard, so the contract accesses are recorded.
    apply_state.apply_reason = ApplyChunkReason::UpdateTrackedShard;

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &[call_receipt.clone()],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(apply_result.delayed_receipts_count, 0);
    assert_eq!(
        apply_result.contract_updates.contract_accesses,
        HashSet::from([CodeHash(*contract_code.hash())])
    );

    // Apply chunk for validating the state witness, so the contract accesses are not recorded.
    apply_state.apply_reason = ApplyChunkReason::ValidateChunkStateWitness;

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &[call_receipt],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(apply_result.delayed_receipts_count, 0);
    assert_eq!(apply_result.contract_updates.contract_accesses, HashSet::new());
}

/// Tests that the existing contract is not recorded in the state witness for a deploy-contract action.
/// For this, it deploys two contracts to the same account and checks the storage proof size after the second deploy action.
#[test]
fn test_exclude_existing_contract_code_for_deploy_action() {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) =
        setup_runtime(vec![alice_account()], to_yocto(1_000_000), to_yocto(500_000), 10u64.pow(15));

    apply_state.config = Arc::new(RuntimeConfig::free());

    // Choose a contract size that is much more than rest of the storage proof size so that we can show that
    // the contract code is not included in the storage proof at the end of the test.
    const PREV_CONTRACT_SIZE: usize = 5000;
    let contract_code1 =
        ContractCode::new(near_test_contracts::sized_contract(PREV_CONTRACT_SIZE).to_vec(), None);
    let deploy_receipt1 = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::DeployContract(DeployContractAction { code: contract_code1.code().to_vec() })],
    );

    // Deploy a different contract by creating one with a different size.
    let contract_code2 = ContractCode::new(
        near_test_contracts::sized_contract(PREV_CONTRACT_SIZE + 100).to_vec(),
        None,
    );
    let deploy_receipt2 = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::DeployContract(DeployContractAction { code: contract_code2.code().to_vec() })],
    );

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &[deploy_receipt1],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(apply_result.delayed_receipts_count, 0);
    assert_eq!(apply_result.contract_updates.contract_accesses, HashSet::new());
    assert_eq!(
        apply_result.contract_updates.contract_deploy_hashes(),
        HashSet::from([CodeHash(*contract_code1.hash())])
    );

    let mut store_update = tries.store_update();
    let root =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit().unwrap();

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &[deploy_receipt2],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(apply_result.delayed_receipts_count, 0);
    assert_eq!(apply_result.contract_updates.contract_accesses, HashSet::new());
    assert_eq!(
        apply_result.contract_updates.contract_deploy_hashes(),
        HashSet::from([CodeHash(*contract_code2.hash())])
    );

    let partial_storage = apply_result.proof.unwrap();
    let PartialState::TrieValues(storage_proof) = partial_storage.nodes;
    let total_size: usize = storage_proof.iter().map(|v| v.len()).sum();
    // Contract size is much larger than the rest of the storage proof, so we compare them to check if the contract is excluded.
    if ProtocolFeature::ExcludeExistingCodeFromWitnessForCodeLen.enabled(PROTOCOL_VERSION) {
        assert!(
            total_size < PREV_CONTRACT_SIZE,
            "Contract code should not be in storage proof. Storage proof size: {}",
            total_size
        );
    } else {
        assert!(
            total_size > PREV_CONTRACT_SIZE,
            "Contract code should be in storage proof. Storage proof size: {}",
            total_size
        );
    }
}

/// Tests that the existing contract is not recorded in the state witness for a delete-account action.
/// For this, it creates an account, deploys a contract to it, and deletes that account, and checks
/// the storage proof size after the delete-account action.
#[test]
fn test_exclude_existing_contract_code_for_delete_account_action() {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) =
        setup_runtime(vec![alice_account()], to_yocto(1_000_000), to_yocto(500_000), 10u64.pow(15));

    apply_state.config = Arc::new(RuntimeConfig::free());

    // Information about the test account (of predecessor "alice.near") that will be used for create, deploy, and delete actions.
    let test_account_id: AccountId =
        ("fake.".to_owned() + alice_account().as_str()).as_str().parse().unwrap();
    let test_account_signer: Arc<Signer> = Arc::new(InMemorySigner::test_signer(&test_account_id));

    // Choose a contract size that is much more than rest of the storage proof size so that we can show that
    // the contract code is not included in the storage proof at the end of the test.
    const CONTRACT_SIZE: usize = 5000;
    let contract_code =
        ContractCode::new(near_test_contracts::sized_contract(CONTRACT_SIZE).to_vec(), None);
    let create_account_receipt = create_receipt_for_create_account(
        alice_account(),
        signers[0].clone(),
        test_account_id.clone(),
        test_account_signer.clone(),
        to_yocto(100_000),
    );
    let deploy_receipt = create_receipt_with_actions(
        test_account_id.clone(),
        test_account_signer.clone(),
        vec![Action::DeployContract(DeployContractAction { code: contract_code.code().to_vec() })],
    );

    let delete_account_receipt = create_receipt_with_actions(
        test_account_id,
        test_account_signer,
        vec![Action::DeleteAccount(DeleteAccountAction { beneficiary_id: alice_account() })],
    );

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &[create_account_receipt, deploy_receipt],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(apply_result.delayed_receipts_count, 0);
    assert_eq!(apply_result.contract_updates.contract_accesses, HashSet::new());
    assert_eq!(
        apply_result.contract_updates.contract_deploy_hashes(),
        HashSet::from([CodeHash(*contract_code.hash())])
    );

    let mut store_update = tries.store_update();
    let root =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit().unwrap();

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &[delete_account_receipt],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(apply_result.delayed_receipts_count, 0);
    assert_eq!(apply_result.contract_updates.contract_accesses, HashSet::new());
    assert_eq!(apply_result.contract_updates.contract_deploy_hashes(), HashSet::new());

    let partial_storage = apply_result.proof.unwrap();
    let PartialState::TrieValues(storage_proof) = partial_storage.nodes;
    let total_size: usize = storage_proof.iter().map(|v| v.len()).sum();
    // Contract size is much larger than the rest of the storage proof, so we compare them to check if the contract is excluded.
    if ProtocolFeature::ExcludeExistingCodeFromWitnessForCodeLen.enabled(PROTOCOL_VERSION) {
        assert!(
            total_size < CONTRACT_SIZE,
            "Contract code should not be in storage proof. Storage proof size: {}",
            total_size
        );
    } else {
        assert!(
            total_size > CONTRACT_SIZE,
            "Contract code should be in storage proof. Storage proof size: {}",
            total_size
        );
    }
}

/// Check that applying nothing does not change the state trie.
/// UPDATE: BandwidthScheduler runs on every height and modifies the state, so this is no longer true for newer protocol versions
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
        setup_runtime(vec![alice_account()], initial_balance, initial_locked, gas_limit);

    let receipts = [];

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root_before),
            &None,
            &apply_state,
            &receipts,
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    let mut store_update = tries.store_update();
    let root_after =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
    assert!(root_before != root_after, "state root not changed - did the bandwidth scheduler run?");
}

/// Test that delayed receipts are accounted for in the congestion info of
/// the ApplyResult.
#[test]
fn test_congestion_delayed_receipts_accounting() {
    let initial_balance = to_yocto(10);
    let initial_locked = to_yocto(0);
    let deposit = to_yocto(1);
    let gas_limit = 1;
    let (runtime, tries, root, apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        initial_balance,
        initial_locked,
        gas_limit,
    );

    let n = 10;
    let receipts = generate_receipts(deposit, n);

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            &apply_state,
            &receipts,
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    assert_eq!(n - 1, apply_result.delayed_receipts_count);
    let congestion = apply_result.congestion_info.unwrap();
    let expected_delayed_gas =
        (n - 1) * compute_receipt_congestion_gas(&receipts[0], &apply_state.config).unwrap();
    let expected_receipts_bytes = (n - 1) * compute_receipt_size(&receipts[0]).unwrap() as u64;

    assert_eq!(expected_delayed_gas as u128, congestion.delayed_receipts_gas());
    assert_eq!(expected_receipts_bytes, congestion.receipt_bytes());
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
    init_test_logger();

    // In the test setup with MockEpochInfoProvider, bob_account is on shard 0 while alice_account
    // is on shard 1. Hence all receipts will be forwarded from shard 1 to shard 0. We don't want local
    // forwarding in the test, hence we need to use a different shard id.
    let version = 3;

    let accounts = vec![alice_account(), bob_account()];
    let shard_layout = ShardLayout::multi_shard_custom(accounts.clone(), version);
    let local_shard = shard_layout.account_id_to_shard_id(&alice_account());
    let local_shard_uid = ShardUId::new(version, local_shard);
    let receiver_shard = shard_layout.account_id_to_shard_id(&bob_account());
    assert_ne!(local_shard, receiver_shard);

    let initial_balance = to_yocto(1_000_000);
    let initial_locked = to_yocto(500_000);
    let deposit = to_yocto(10_000);
    // execute a single receipt per chunk
    let gas_limit = 1;

    let accounts_with_keys = accounts
        .into_iter()
        .map(|account| {
            let signer = Arc::new(InMemorySigner::test_signer(&account));
            (account, vec![signer])
        })
        .collect::<Vec<_>>();

    let (runtime, tries, mut root, mut apply_state, _) = setup_runtime_for_shard(
        accounts_with_keys,
        initial_balance,
        initial_locked,
        gas_limit,
        local_shard_uid,
        &shard_layout,
    );

    // Set account_id_to_shard_id for alice_account delayed receipts handling to work properly
    // setup_runtime_for_shard sets up account for alice on `local_shard_uid`.
    let epoch_info_provider = MockEpochInfoProvider::new(shard_layout);
    apply_state.shard_id = local_shard;

    // Mark receiver shard as congested. Which method we use doesn't matter,
    // this test only checks that receipt buffering works. Unit tests
    // congestion_info.rs test that the congestion level is picked up for all
    // possible congestion conditions.
    let max_congestion_incoming_gas: Gas =
        apply_state.config.congestion_control_config.max_congestion_incoming_gas;
    apply_state
        .congestion_info
        .get_mut(&receiver_shard)
        .unwrap()
        .congestion_info
        .add_delayed_receipt_gas(max_congestion_incoming_gas)
        .unwrap();
    // set allowed shard of the receiver shard to itself to prevent local shard from forwarding
    apply_state
        .congestion_info
        .get_mut(&receiver_shard)
        .unwrap()
        .congestion_info
        .set_allowed_shard(receiver_shard.into());
    apply_state.congestion_info.insert(local_shard, Default::default());

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
                &None,
                &apply_state,
                prev_receipts,
                SignedValidPeriodTransactions::empty(),
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
        .get_mut(&receiver_shard)
        .unwrap()
        .congestion_info
        .remove_delayed_receipt_gas(100)
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
                &None,
                &apply_state,
                prev_receipts,
                SignedValidPeriodTransactions::empty(),
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        root = commit_apply_result(&apply_result, &mut apply_state, &tries, local_shard_uid);

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
    shard_uid: ShardUId,
) -> CryptoHash {
    // congestion control requires an update on
    assert_eq!(shard_uid.shard_id(), apply_state.shard_id);
    if let Some(congestion_info) = apply_result.congestion_info {
        let extended = ExtendedCongestionInfo::new(congestion_info, 0);
        apply_state.congestion_info.insert(shard_uid.shard_id(), extended);
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
        setup_runtime(vec![alice_account()], initial_balance, initial_locked, gas_limit);

    // Delete previous congestion info to trigger bootstrapping it. An empty
    // shards congestion info map is what we should see in the first chunk
    // with the feature enabled.
    apply_state.congestion_info = BlockCongestionInfo::default();

    // Apply test specific settings
    apply_state.is_new_chunk = is_new_chunk;

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            &apply_state,
            &[],
            SignedValidPeriodTransactions::empty(),
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
    let is_new_chunk = true;
    check_congestion_info_bootstrapping(is_new_chunk, Some(CongestionInfo::default()));

    let is_new_chunk = false;
    check_congestion_info_bootstrapping(is_new_chunk, None);
}

#[test]
fn test_deploy_and_call_local_receipt() {
    let (runtime, tries, root, apply_state, signers, epoch_info_provider) =
        setup_runtime(vec![alice_account()], to_yocto(1_000_000), to_yocto(500_000), 10u64.pow(15));

    let tx = SignedTransaction::from_actions(
        1,
        alice_account(),
        alice_account(),
        &*signers[0],
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
            &None,
            &apply_state,
            &[],
            SignedValidPeriodTransactions::new(vec![tx], vec![true]),
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
    let (runtime, tries, root, apply_state, signers, epoch_info_provider) =
        setup_runtime(vec![alice_account()], to_yocto(1_000_000), to_yocto(500_000), 10u64.pow(15));

    let tx1 = SignedTransaction::from_actions(
        1,
        alice_account(),
        alice_account(),
        &*signers[0],
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
        &*signers[0],
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
            &None,
            &apply_state,
            &[],
            SignedValidPeriodTransactions::new(vec![tx1, tx2], vec![true; 2]),
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

/// Verifies that valid transactions from multiple accounts are processed in the correct order,
/// while transactions with an invalid signer are dropped.
#[test]
fn test_transaction_ordering_with_apply() {
    let alice_signer = InMemorySigner::test_signer(&alice_account());
    let bob_signer = InMemorySigner::test_signer(&bob_account());
    let alice_invalid_signer = InMemorySigner::from_seed(alice_account(), KeyType::ED25519, "seed");

    // This transaction should be dropped due to invalid signer.
    let alice_invalid_tx = SignedTransaction::send_money(
        1,
        alice_account(),
        alice_account(),
        &alice_invalid_signer,
        100,
        CryptoHash::default(),
    );
    let alice_tx1 = SignedTransaction::send_money(
        1,
        alice_account(),
        alice_account(),
        &alice_signer,
        200,
        CryptoHash::default(),
    );
    let alice_tx2 = SignedTransaction::send_money(
        2,
        alice_account(),
        bob_account(),
        &alice_signer,
        300,
        CryptoHash::default(),
    );
    let bob_tx1 = SignedTransaction::send_money(
        1,
        bob_account(),
        bob_account(),
        &bob_signer,
        400,
        CryptoHash::default(),
    );
    let bob_tx2 = SignedTransaction::send_money(
        2,
        bob_account(),
        alice_account(),
        &bob_signer,
        500,
        CryptoHash::default(),
    );
    let bob_tx3 = SignedTransaction::send_money(
        3,
        bob_account(),
        bob_account(),
        &bob_signer,
        600,
        CryptoHash::default(),
    );

    let txs = vec![
        bob_tx1.clone(),
        alice_invalid_tx,
        alice_tx1.clone(),
        bob_tx2.clone(),
        alice_tx2.clone(),
        bob_tx3.clone(),
    ];

    let (runtime, tries, root, apply_state, _signers, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        to_yocto(1_000_000),
        to_yocto(500_000),
        10u64.pow(15),
    );

    let validity_flags = vec![true; txs.len()];
    let signed_valid_period_txs = SignedValidPeriodTransactions::new(txs, validity_flags);
    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            &apply_state,
            &[],
            signed_valid_period_txs,
            &epoch_info_provider,
            Default::default(),
        )
        .expect("apply should succeed");

    let expected_order = vec![
        bob_tx1.get_hash(),
        alice_tx1.get_hash(),
        bob_tx2.get_hash(),
        alice_tx2.get_hash(),
        bob_tx3.get_hash(),
    ];

    // Note: The 3 local receipts are generated for valid transactions
    // where signer_id == receiver_id - tx2, tx4, tx6 (not for tx1 as it is dropped).
    assert_eq!(
        apply_result.outcomes.len(),
        8,
        "should have processed 5 transactions and 3 local receipts"
    );
    let tx_outcomes = apply_result.outcomes.iter().take(5).map(|o| o.id).collect::<Vec<_>>();
    assert_eq!(tx_outcomes, expected_order, "outcomes are not in expected sorted order");
}

/// Verifies proper ordering and balance update for transactions signed with multiple keys from one account.
/// Alice is set up with 3 full-access keys.
/// Six transactions from Alice to Bob are submitted using various nonces and keys.
/// The test checks that outcomes are correctly ordered and Alice's final balance is within the expected range.
#[test]
fn test_transaction_multiple_access_keys_with_apply() {
    let alice_signer1 = InMemorySigner::from_seed(alice_account(), KeyType::ED25519, "seed1");
    let alice_signer2 = InMemorySigner::from_seed(alice_account(), KeyType::ED25519, "seed2");
    let alice_signer3 = InMemorySigner::from_seed(alice_account(), KeyType::ED25519, "seed3");

    let send_money_tx = |nonce, key| {
        SignedTransaction::send_money(
            nonce,
            alice_account(),
            bob_account(),
            key,
            to_yocto(1000),
            CryptoHash::default(),
        )
    };

    let txs = vec![
        send_money_tx(1, &alice_signer1),
        send_money_tx(1, &alice_signer2),
        send_money_tx(1, &alice_signer3),
        send_money_tx(2, &alice_signer3),
        send_money_tx(2, &alice_signer1),
        send_money_tx(3, &alice_signer1),
    ];

    let accounts_with_keys = vec![
        (
            alice_account(),
            vec![Arc::new(alice_signer1), Arc::new(alice_signer2), Arc::new(alice_signer3)],
        ),
        (bob_account(), vec![]),
    ];

    let (runtime, tries, root, mut apply_state, _signers, epoch_info_provider) =
        setup_runtime_with_keys(
            accounts_with_keys,
            to_yocto(1_000_000),
            to_yocto(500_000),
            10u64.pow(15),
        );

    let validity_flags = vec![true; txs.len()];
    let signed_valid_period_txs = SignedValidPeriodTransactions::new(txs.clone(), validity_flags);
    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            &apply_state,
            &[],
            signed_valid_period_txs,
            &epoch_info_provider,
            Default::default(),
        )
        .expect("apply should succeed");

    let expected_order = txs.iter().map(|tx| tx.get_hash()).collect::<Vec<_>>();

    assert_eq!(apply_result.outcomes.len(), txs.len(), "should have processed 6 transactions");
    let tx_outcomes = apply_result.outcomes.iter().map(|o| o.id).collect::<Vec<_>>();
    assert_eq!(tx_outcomes, expected_order, "outcomes are not in expected sorted order");

    let shard_uid = ShardUId::single_shard();
    let root = commit_apply_result(&apply_result, &mut apply_state, &tries, shard_uid);
    let state = tries.new_trie_update(shard_uid, root);
    let account = get_account(&state, &alice_account()).unwrap().unwrap();

    assert!(account.amount() < to_yocto(994_000));
    assert!(account.amount() > to_yocto(993_000));
}

#[test]
fn test_expired_transaction() {
    let alice_signer = InMemorySigner::test_signer(&alice_account());
    let expired_tx = vec![SignedTransaction::send_money(
        1,
        alice_account(),
        alice_account(),
        &alice_signer,
        1,
        CryptoHash::default(),
    )];
    let (runtime, tries, root, apply_state, _signers, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        to_yocto(1_000_000),
        to_yocto(500_000),
        10u64.pow(15),
    );
    let signed_valid_period_txs = SignedValidPeriodTransactions::new(expired_tx, vec![false]);
    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            &apply_state,
            &[],
            signed_valid_period_txs,
            &epoch_info_provider,
            Default::default(),
        )
        .expect("apply should succeed");
    assert_eq!(
        apply_result.outcomes.len(),
        0,
        "should have not produced any outcomes for the expired tx"
    );
}

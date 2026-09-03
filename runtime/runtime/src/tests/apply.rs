use super::GAS_PRICE;
use crate::access_keys::initial_nonce_value;
use crate::config::{total_send_fees, tx_cost};
use crate::congestion_control::{compute_receipt_congestion_gas, compute_receipt_size};
use crate::tests::{
    MAX_ATTACHED_GAS, create_receipt_for_create_account, create_receipt_with_actions,
    set_sha256_cost,
};
use crate::{
    ActionResult, ApplyResult, ApplyState, Runtime, ValidatorAccountsUpdate, action_add_key,
};
use crate::{SignedValidPeriodTransactions, total_prepaid_exec_fees};
use assert_matches::assert_matches;
use near_crypto::{InMemorySigner, KeyType, PublicKey, SecretKey, Signer};
use near_o11y::testonly::init_test_logger;
use near_parameters::parameter_table::FeeComponent;
use near_parameters::{ActionCosts, RuntimeConfig};
use near_primitives::account::{
    AccessKey, AccessKeyPermission, Account, AccountContract, FunctionCallPermission,
};
use near_primitives::action::delegate::{DelegateAction, NonDelegateAction, SignedDelegateAction};
use near_primitives::action::{Action, DeleteAccountAction, TransferToGasKeyAction};
use near_primitives::apply::ApplyChunkReason;
use near_primitives::bandwidth_scheduler::BlockBandwidthRequests;
use near_primitives::congestion_info::{
    BlockCongestionInfo, CongestionControl, CongestionInfo, ExtendedCongestionInfo,
};
use near_primitives::errors::{
    ActionError, ActionErrorKind, CompilationError, DepositCostFailureReason, FunctionCallError,
    InvalidTxError, MissingTrieValue, RuntimeError, TxExecutionError,
};
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::receipt::{
    ActionReceipt, DataReceipt, PromiseYieldIndices, Receipt, ReceiptEnum, ReceiptV0,
};
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::state::PartialState;
use near_primitives::stateless_validation::contract_distribution::CodeHash;
use near_primitives::test_utils::{MockEpochInfoProvider, account_new};
use near_primitives::transaction::{
    AddKeyAction, CreateAccountAction, DeleteKeyAction, DeployContractAction, ExecutionMetadata,
    ExecutionOutcome, ExecutionOutcomeWithId, ExecutionStatus, FunctionCallAction,
    SignedTransaction, TransactionNonce, TransferAction,
};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{
    AccountId, Balance, BlockHeight, EpochId, EpochInfoProvider, Gas, MerkleHash, NonceIndex,
    ShardId, StateChangeCause,
};
use near_primitives::utils::create_receipt_id_from_transaction;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature, ProtocolVersion};
use near_store::test_utils::TestTriesBuilder;
use near_store::trie::AccessOptions;
use near_store::trie::receipts_column_helper::ShardsOutgoingReceiptBuffer;
use near_store::{
    MissingTrieValueContext, PartialStorage, ShardTries, StorageError, Trie, get_access_key,
    get_account, get_gas_key_nonce, get_postponed_receipt, get_received_data, remove_account,
    set_access_key, set_account,
};
use near_vm_runner::{ContractCode, FilesystemContractRuntimeCache};
use std::collections::{HashMap, HashSet};
use std::slice::from_ref;
use std::sync::Arc;
use testlib::runtime_utils::{alice_account, bob_account};

/***************/
/* Apply tests */
/***************/

const DEFAULT_MINIMAL_GAS_ATTACHMENT: Gas = Gas::from_gas(1);

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
            initial_account.set_locked(initial_locked).unwrap();

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
    store_update.commit();
    let contract_cache = FilesystemContractRuntimeCache::test().unwrap();
    let shard_ids = shard_layout.shard_ids();
    let shards_congestion_info =
        shard_ids.map(|shard_id| (shard_id, ExtendedCongestionInfo::default())).collect();
    let congestion_info = BlockCongestionInfo::new(shards_congestion_info);
    let apply_state = ApplyState {
        apply_reason: ApplyChunkReason::UpdateTrackedShard,
        block_height: 1,
        prev_block_hash: Default::default(),
        shard_id: shard_uid.shard_id(),
        epoch_id: Default::default(),
        epoch_height: 0,
        gas_price: GAS_PRICE,
        block_timestamp: 100,
        gas_limit: Some(gas_limit),
        random_seed: Default::default(),
        current_protocol_version: PROTOCOL_VERSION,
        config: Arc::new(RuntimeConfig::test()),
        next_wasm_config: None,
        cache: Some(Box::new(contract_cache)),
        is_new_chunk: true,
        save_receipt_to_tx: false,
        congestion_info,
        bandwidth_requests: BlockBandwidthRequests::empty(),
        trie_access_tracker_state: Default::default(),
        on_post_state_ready: None,
    };

    (runtime, tries, root, apply_state, signers)
}

#[test]
fn test_apply_no_op() {
    let (runtime, tries, root, apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        Balance::from_near(1_000_000),
        Balance::ZERO,
        Gas::from_teragas(1000),
    );
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
    let initial_locked = Balance::from_near(500_000);
    let reward = Balance::from_near(10_000_000);
    let small_refund = Balance::from_near(500);
    let (runtime, tries, root, apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        Balance::from_near(1_000_000),
        initial_locked,
        Gas::from_teragas(1000),
    );

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
            &[Receipt::new_balance_refund(&alice_account(), small_refund)],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
}

/// An uninitialized account in `stake_info` must be skipped rather than
/// written to, since it has no locked balance and `set_locked` would
/// fail the whole chunk over it. Skipping is only allowed while nothing
/// is owed, so this covers each of the three amounts that need locked balance:
/// maximum of stakes, reward, and last proposal.
#[test]
fn test_apply_validator_update_uninitialized_account() {
    let (runtime, tries, root, apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        Balance::from_near(1_000_000),
        Balance::ZERO,
        Gas::from_teragas(1000),
    );

    // Put an uninitialized account in the trie and name it in `stake_info` with
    // a zero max stake, which is what the tail of a stake return looks like.
    let uninitialized: AccountId =
        // cspell:disable-next-line
        "0u4bwt6zbknvvcyzmfnfhitcfzatxtthkbzdcm4zwezyf7zwe6pnc4c".parse().unwrap();
    let balance = Balance::from_near(1);
    let mut state = tries.new_trie_update(ShardUId::single_shard(), root);
    set_account(
        &mut state,
        uninitialized.clone(),
        &Account::new_uninitialized(balance, 100, initial_nonce_value(1)),
    );
    state.commit(StateChangeCause::InitialState);
    let trie_changes = state.finalize().unwrap().trie_changes;
    let mut store_update = tries.store_update();
    let root = tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit();

    let apply_with = |max_of_stakes: Balance, reward: Option<Balance>, proposal| {
        let entry = |amount: Option<Balance>| -> HashMap<AccountId, Balance> {
            amount.map(|amount| (uninitialized.clone(), amount)).into_iter().collect()
        };
        let validator_accounts_update = ValidatorAccountsUpdate {
            stake_info: vec![(uninitialized.clone(), max_of_stakes)].into_iter().collect(),
            validator_rewards: entry(reward),
            last_proposals: entry(proposal),
            protocol_treasury_account_id: None,
        };
        runtime.apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &Some(validator_accounts_update),
            &apply_state,
            &[],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
    };

    // Nothing owed is what the skip is for, and applying at all is the property
    // under test: an inconsistent-state error would be a failed chunk.
    let result = apply_with(Balance::ZERO, None, None)
        .expect("an uninitialized account in stake_info must not fail the chunk");
    let mut store_update = tries.store_update();
    let new_root =
        tries.apply_all(&result.trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit();
    let state = tries.new_trie_update(ShardUId::single_shard(), new_root);
    let account = get_account(&state, &uninitialized).unwrap().unwrap();
    assert!(!account.is_initialized(), "the skip must leave the account untouched");
    assert_eq!(account.amount(), balance);

    // A zero-valued entry is ordinary and owes nothing: the reward calculator
    // records a zero reward for a validator that stayed below the online
    // threshold, and a zero proposal is how unstaking is expressed. Testing for
    // the presence of a key rather than a positive amount would fail here.
    apply_with(Balance::ZERO, Some(Balance::ZERO), Some(Balance::ZERO))
        .expect("zero-valued reward and proposal entries must not fail the chunk");

    // Each of the three needs locked balance the account cannot have.
    let owed = Balance::from_near(1);
    let cases = [
        ("a max of stakes", apply_with(owed, None, None)),
        ("a reward", apply_with(Balance::ZERO, Some(owed), None)),
        ("a last proposal", apply_with(Balance::ZERO, None, Some(owed))),
    ];
    for (what, result) in cases {
        let err = result
            .err()
            .unwrap_or_else(|| panic!("{what} owed to an uninitialized account must fail"));
        assert!(
            matches!(err, RuntimeError::StorageError(StorageError::StorageInconsistentState(_))),
            "unexpected error for {what}: {err:?}"
        );
    }
}

#[test]
fn test_apply_refund_receipts() {
    let initial_balance = Balance::from_near(1_000_000);
    let initial_locked = Balance::from_near(500_000);
    let small_transfer = Balance::from_near(10_000);
    let gas_limit = 1;
    let (runtime, tries, mut root, mut apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        initial_balance,
        initial_locked,
        Gas::from_gas(gas_limit),
    );

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
                .checked_add(small_transfer.checked_mul(u128::from(capped_i)).unwrap())
                .unwrap()
                .checked_add(Balance::from_yoctonear(u128::from(capped_i * (capped_i - 1) / 2)))
                .unwrap()
        );
    }
}

#[test]
fn test_apply_delayed_receipts_feed_all_at_once() {
    let initial_balance = Balance::from_near(1_000_000);
    let initial_locked = Balance::from_near(500_000);
    let small_transfer = Balance::from_near(10_000);
    let gas_limit = 1;
    let (runtime, tries, mut root, mut apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        initial_balance,
        initial_locked,
        Gas::from_gas(gas_limit),
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
                .checked_add(small_transfer.checked_mul(u128::from(capped_i)).unwrap())
                .unwrap()
                .checked_add(Balance::from_yoctonear(u128::from(capped_i * (capped_i - 1) / 2)))
                .unwrap()
        );
    }
}

#[test]
fn test_apply_delayed_receipts_add_more_using_chunks() {
    let initial_balance = Balance::from_near(1_000_000);
    let initial_locked = Balance::from_near(500_000);
    let small_transfer = Balance::from_near(10_000);
    let (runtime, tries, mut root, mut apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        initial_balance,
        initial_locked,
        DEFAULT_MINIMAL_GAS_ATTACHMENT,
    );

    let receipt_gas_cost = apply_state
        .config
        .fees
        .fee(ActionCosts::new_action_receipt)
        .exec_fee()
        .checked_add(apply_state.config.fees.fee(ActionCosts::transfer).exec_fee())
        .unwrap()
        .gas;
    apply_state.gas_limit = Some(receipt_gas_cost.checked_mul(3).unwrap());

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
                .checked_add(small_transfer.checked_mul(u128::from(capped_i)).unwrap())
                .unwrap()
                .checked_add(Balance::from_yoctonear(u128::from(capped_i * (capped_i - 1) / 2)))
                .unwrap()
        );
    }
}

#[test]
fn test_apply_delayed_receipts_adjustable_gas_limit() {
    let initial_balance = Balance::from_near(1_000_000);
    let initial_locked = Balance::from_near(500_000);
    let small_transfer = Balance::from_near(10_000);
    let (runtime, tries, mut root, mut apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        initial_balance,
        initial_locked,
        DEFAULT_MINIMAL_GAS_ATTACHMENT,
    );

    let receipt_gas_cost = apply_state
        .config
        .fees
        .fee(ActionCosts::new_action_receipt)
        .exec_fee()
        .checked_add(apply_state.config.fees.fee(ActionCosts::transfer).exec_fee())
        .unwrap()
        .gas;

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
        apply_state.gas_limit = Some(receipt_gas_cost.checked_mul(num_receipts_per_block).unwrap());
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
                .checked_add(
                    small_transfer.checked_mul(num_receipts_processed.try_into().unwrap()).unwrap()
                )
                .unwrap()
                .checked_add(Balance::from_yoctonear(u128::from(
                    num_receipts_processed * (num_receipts_processed - 1) / 2
                )))
                .unwrap()
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

fn generate_receipts(small_transfer: Balance, n: u64) -> Vec<Receipt> {
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
                        deposit: small_transfer
                            .checked_add(Balance::from_yoctonear(u128::from(i)))
                            .unwrap(),
                    })],
                }),
            })
        })
        .collect()
}

fn generate_refund_receipts(small_transfer: Balance, n: u64) -> Vec<Receipt> {
    let mut receipt_id = CryptoHash::default();
    (0..n)
        .map(|i| {
            receipt_id = hash(receipt_id.as_ref());
            Receipt::new_balance_refund(
                &alice_account(),
                small_transfer.checked_add(Balance::from_yoctonear(u128::from(i))).unwrap(),
            )
        })
        .collect()
}

fn generate_delegate_actions(deposit: Balance, n: u64) -> Vec<Receipt> {
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
                deposit: deposit,
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
    let initial_balance = Balance::from_near(1_000_000);
    let initial_locked = Balance::from_near(500_000);
    let small_transfer = Balance::from_near(10_000);
    let (runtime, tries, mut root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        initial_balance,
        initial_locked,
        DEFAULT_MINIMAL_GAS_ATTACHMENT,
    );

    let receipt_exec_gas_fee = Gas::from_gas(1000);
    let mut free_config = RuntimeConfig::free();
    let fees = Arc::make_mut(&mut free_config.fees);
    fees.action_fees[ActionCosts::new_action_receipt].execution =
        FeeComponent::Gas(receipt_exec_gas_fee);
    apply_state.config = Arc::new(free_config);
    // This allows us to execute 3 receipts per apply.
    apply_state.gas_limit = Some(receipt_exec_gas_fee.checked_mul(3).unwrap());

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
                local_transactions[0].hash(),
                apply_state.block_height,
            ), // receipt for tx 0
            create_receipt_id_from_transaction(
                local_transactions[1].hash(),
                apply_state.block_height,
            ), // receipt for tx 1
            create_receipt_id_from_transaction(
                local_transactions[2].hash(),
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
    store_update.commit();

    assert_eq!(
        apply_result.outcomes.iter().map(|o| o.id).collect::<Vec<_>>(),
        vec![
            local_transactions[4].get_hash(), // tx 4
            create_receipt_id_from_transaction(
                local_transactions[4].hash(),
                apply_state.block_height,
            ), // receipt for tx 4
            create_receipt_id_from_transaction(
                local_transactions[3].hash(),
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
    store_update.commit();

    assert_eq!(
        apply_result.outcomes.iter().map(|o| o.id).collect::<Vec<_>>(),
        vec![
            local_transactions[5].get_hash(), // tx 5
            local_transactions[6].get_hash(), // tx 6
            local_transactions[7].get_hash(), // tx 7
            local_transactions[8].get_hash(), // tx 8
            create_receipt_id_from_transaction(
                local_transactions[5].hash(),
                apply_state.block_height,
            ), // receipt for tx 5
            create_receipt_id_from_transaction(
                local_transactions[6].hash(),
                apply_state.block_height,
            ), // receipt for tx 6
            create_receipt_id_from_transaction(
                local_transactions[7].hash(),
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
    store_update.commit();

    assert_eq!(
        apply_result.outcomes.iter().map(|o| o.id).collect::<Vec<_>>(),
        vec![
            *receipts[1].receipt_id(), // receipt #1
            *receipts[2].receipt_id(), // receipt #2
            create_receipt_id_from_transaction(
                local_transactions[8].hash(),
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

// Under AccountCostIncrease the runtime caps gas_burn_price at the receipt's gas_price (no
// deficit ever) and refunds price_surplus through a refund receipt (instead of adding it to
// tx_burnt). The tests below assert on both flavors.
#[test]
fn test_apply_deficit_gas_for_transfer() {
    let initial_balance = Balance::from_near(1_000_000);
    let initial_locked = Balance::from_near(500_000);
    let small_transfer = Balance::from_near(10_000);
    let gas_limit = 10u64.pow(15);
    let (runtime, tries, root, apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        initial_balance,
        initial_locked,
        Gas::from_gas(gas_limit),
    );

    let n = 1;
    let mut receipts = generate_receipts(small_transfer, n);
    if let ReceiptEnum::Action(action_receipt) = receipts.get_mut(0).unwrap().receipt_mut() {
        action_receipt.gas_price = GAS_PRICE.checked_div(10).unwrap();
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
    if ProtocolFeature::AccountCostIncrease.enabled(PROTOCOL_VERSION) {
        // gas_burn_price is capped at the receipt's (lower) gas_price, so the receipt is
        // burnt at exactly what the user paid and no deficit accumulates.
        assert_eq!(result.stats.balance.gas_deficit_amount, Balance::ZERO);
    } else {
        assert_eq!(
            result.stats.balance.gas_deficit_amount,
            result.stats.balance.tx_burnt_amount.checked_mul(9).unwrap()
        )
    }
}

/// Apply a transfer receipt that was purchased at a higher gas price than
/// current, then check that we burn the correct amount.
#[test]
fn test_apply_surplus_gas_for_transfer() {
    let initial_balance = Balance::from_near(1_000_000);
    let initial_locked = Balance::from_near(500_000);
    let small_transfer = Balance::from_near(10_000);
    let gas_limit = 10u64.pow(15);
    let (runtime, tries, root, apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        initial_balance,
        initial_locked,
        Gas::from_gas(gas_limit),
    );
    let gas_price = GAS_PRICE.checked_mul(10).unwrap();

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
    let exec_gas = fees
        .fee(ActionCosts::new_action_receipt)
        .exec_fee()
        .checked_add(fees.fee(ActionCosts::transfer).exec_fee())
        .unwrap()
        .gas;

    assert!(result.stats.balance.gas_deficit_amount.is_zero());
    if ProtocolFeature::AccountCostIncrease.enabled(PROTOCOL_VERSION) {
        // price_surplus is refunded to the signer (1 refund receipt) and only the burn-price
        // portion (= apply_state.gas_price * exec_gas) is added to tx_burnt_amount.
        let expected_burnt_amount =
            apply_state.gas_price.checked_mul(u128::from(exec_gas.as_gas())).unwrap();
        assert_eq!(result.stats.balance.tx_burnt_amount, expected_burnt_amount);
        assert_eq!(result.outgoing_receipts.len(), 1);
    } else {
        // price_surplus is burnt (added to tx_burnt_amount) and no refund is produced.
        let expected_burnt_amount = gas_price.checked_mul(u128::from(exec_gas.as_gas())).unwrap();
        assert_eq!(result.stats.balance.tx_burnt_amount, expected_burnt_amount);
        assert_eq!(result.outgoing_receipts.len(), 0);
    }
}

#[test]
fn test_apply_deficit_gas_for_function_call_covered() {
    let initial_balance = Balance::from_near(1_000_000);
    let initial_locked = Balance::from_near(500_000);
    let gas_limit = 10u64.pow(15);
    let (runtime, tries, root, apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        initial_balance,
        initial_locked,
        Gas::from_gas(gas_limit),
    );

    let gas = 2 * 10u64.pow(14);
    let gas_price = GAS_PRICE.checked_div(10).unwrap();
    let actions = vec![Action::FunctionCall(Box::new(FunctionCallAction {
        method_name: "hello".to_string(),
        args: b"world".to_vec(),
        gas: Gas::from_gas(gas),
        deposit: Balance::ZERO,
    }))];

    let expected_gas_burnt = apply_state
        .config
        .fees
        .fee(ActionCosts::new_action_receipt)
        .exec_fee()
        .checked_add(
            total_prepaid_exec_fees(&apply_state.config, &actions, &alice_account()).unwrap(),
        )
        .unwrap()
        .gas;
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
    let total_receipt_cost = gas_price
        .checked_mul(u128::from(
            Gas::from_gas(gas).checked_add(expected_gas_burnt).unwrap().as_gas(),
        ))
        .unwrap();
    let expected_gas_burnt_amount =
        gas_price.checked_mul(u128::from(expected_gas_burnt.as_gas())).unwrap();
    // With gas refund penalties enabled, we should see a reduced refund value
    let unspent_gas: Gas = Gas::from_gas(
        (total_receipt_cost.checked_sub(expected_gas_burnt_amount).unwrap().as_yoctonear()
            / gas_price.as_yoctonear())
        .try_into()
        .unwrap(),
    );
    let refund_penalty = apply_state.config.fees.gas_penalty_for_gas_refund(unspent_gas);
    let expected_refund = total_receipt_cost
        .checked_sub(expected_gas_burnt_amount)
        .unwrap()
        .checked_sub(gas_price.checked_mul(u128::from(refund_penalty.as_gas())).unwrap())
        .unwrap();

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
    let expected_deficit = if ProtocolFeature::AccountCostIncrease.enabled(PROTOCOL_VERSION) {
        // gas_burn_price is capped at the receipt's gas_price, no deficit can accumulate.
        Balance::ZERO
    } else {
        GAS_PRICE
            .checked_sub(gas_price)
            .unwrap()
            .checked_mul(u128::from(expected_gas_burnt.as_gas()))
            .unwrap()
    };
    assert_eq!(result.stats.balance.gas_deficit_amount, expected_deficit);
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
    let initial_balance = Balance::from_near(1_000_000);
    let initial_locked = Balance::from_near(500_000);
    let gas_limit = 10u64.pow(15);
    let (runtime, tries, root, apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        initial_balance,
        initial_locked,
        Gas::from_gas(gas_limit),
    );

    let gas = 1_000_000;
    let gas_price = GAS_PRICE.checked_div(10).unwrap();
    let actions = vec![Action::FunctionCall(Box::new(FunctionCallAction {
        method_name: "hello".to_string(),
        args: b"world".to_vec(),
        gas: Gas::from_gas(gas),
        deposit: Balance::ZERO,
    }))];

    let expected_gas_burnt = apply_state
        .config
        .fees
        .fee(ActionCosts::new_action_receipt)
        .exec_fee()
        .checked_add(
            total_prepaid_exec_fees(&apply_state.config, &actions, &alice_account()).unwrap(),
        )
        .unwrap()
        .gas;
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
    let total_receipt_cost = gas_price
        .checked_mul(u128::from(
            Gas::from_gas(gas).checked_add(expected_gas_burnt).unwrap().as_gas(),
        ))
        .unwrap();
    let expected_deficit = if ProtocolFeature::AccountCostIncrease.enabled(PROTOCOL_VERSION) {
        Balance::ZERO
    } else {
        GAS_PRICE
            .checked_sub(gas_price)
            .unwrap()
            .checked_mul(u128::from(expected_gas_burnt.as_gas()))
            .unwrap()
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
    // The deficit does not affect refunds, hence we should expect a
    // normal refund of the unspent gas. However, this is small enough to
    // cancel out, so we add the refund cost to tx_burnt and expect no
    // refund. This ends up burning all gas and not refunding anything.
    assert_eq!(result.outgoing_receipts.len(), 0);
    assert_eq!(result.stats.balance.tx_burnt_amount, total_receipt_cost);
}

#[test]
fn test_apply_surplus_gas_for_function_call() {
    let initial_balance = Balance::from_near(1_000_000);
    let initial_locked = Balance::from_near(500_000);
    let gas_limit = 10u64.pow(15);
    let (runtime, tries, root, apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        initial_balance,
        initial_locked,
        Gas::from_gas(gas_limit),
    );

    let gas = 2 * 10u64.pow(14);
    let gas_price = GAS_PRICE.checked_mul(10).unwrap();
    let actions = vec![Action::FunctionCall(Box::new(FunctionCallAction {
        method_name: "hello".to_string(),
        args: b"world".to_vec(),
        gas: Gas::from_gas(gas),
        deposit: Balance::ZERO,
    }))];

    let expected_gas_burnt = apply_state
        .config
        .fees
        .fee(ActionCosts::new_action_receipt)
        .exec_fee()
        .checked_add(
            total_prepaid_exec_fees(&apply_state.config, &actions, &alice_account()).unwrap(),
        )
        .unwrap()
        .gas;
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
    let total_receipt_cost = gas_price
        .checked_mul(u128::from(
            Gas::from_gas(gas).checked_add(expected_gas_burnt).unwrap().as_gas(),
        ))
        .unwrap();
    let expected_gas_burnt_amount =
        gas_price.checked_mul(u128::from(expected_gas_burnt.as_gas())).unwrap();

    // With gas refund penalties enabled, we should see a reduced refund value
    let unspent_gas = Gas::from_gas(
        (total_receipt_cost.checked_sub(expected_gas_burnt_amount).unwrap().as_yoctonear()
            / gas_price.as_yoctonear())
        .try_into()
        .unwrap(),
    );
    let refund_penalty = apply_state.config.fees.gas_penalty_for_gas_refund(unspent_gas);
    // The unspent gas is refunded at the price it was purchased at (the receipt's `gas_price`).
    let unspent_refund = total_receipt_cost.checked_sub(expected_gas_burnt_amount).unwrap();
    let expected_refund = if ProtocolFeature::AccountCostIncrease.enabled(PROTOCOL_VERSION) {
        // The refund penalty is charged at the burn price (`apply_state.gas_price`), not the
        // receipt's (higher) purchase price. Additionally, the price surplus on the burnt gas
        // (= (purchase_price - burn_price) * gas_burnt) is refunded instead of being burnt.
        let penalty =
            apply_state.gas_price.checked_mul(u128::from(refund_penalty.as_gas())).unwrap();
        let price_surplus = gas_price
            .checked_sub(apply_state.gas_price)
            .unwrap()
            .checked_mul(u128::from(expected_gas_burnt.as_gas()))
            .unwrap();
        unspent_refund.checked_sub(penalty).unwrap().checked_add(price_surplus).unwrap()
    } else {
        // The refund penalty is charged at the receipt's gas price.
        let penalty = gas_price.checked_mul(u128::from(refund_penalty.as_gas())).unwrap();
        unspent_refund.checked_sub(penalty).unwrap()
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
    assert_eq!(result.stats.balance.gas_deficit_amount, Balance::ZERO, "expected surplus");
    // The refund is less than the received amount.
    match result.outgoing_receipts[0].receipt() {
        ReceiptEnum::Action(ActionReceipt { actions, .. }) => match &actions[0] {
            Action::Transfer(TransferAction { deposit }) => assert_eq!(*deposit, expected_refund),
            other => panic!("Expected transfer action, got {:?}", other),
        },
        _ => unreachable!(),
    };
}

#[test]
fn test_delete_key_add_key() {
    let initial_locked = Balance::from_near(500_000);
    let (runtime, tries, root, apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        Balance::from_near(1_000_000),
        initial_locked,
        Gas::from_teragas(1000),
    );

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
    store_update.commit();

    let state_update = tries.new_trie_update(ShardUId::single_shard(), root);
    let final_account_state = get_account(&state_update, &alice_account()).unwrap().unwrap();

    assert_eq!(initial_account_state.storage_usage(), final_account_state.storage_usage());
}

#[test]
fn test_delete_key_underflow() {
    let initial_locked = Balance::from_near(500_000);
    let (runtime, tries, root, apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        Balance::from_near(1_000_000),
        initial_locked,
        Gas::from_teragas(1000),
    );

    let mut state_update = tries.new_trie_update(ShardUId::single_shard(), root);
    let mut initial_account_state = get_account(&state_update, &alice_account()).unwrap().unwrap();
    initial_account_state.set_storage_usage(10);
    set_account(&mut state_update, alice_account(), &initial_account_state);
    state_update.commit(StateChangeCause::InitialState);
    let trie_changes = state_update.finalize().unwrap().trie_changes;
    let mut store_update = tries.store_update();
    let root = tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit();

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
    store_update.commit();

    let state_update = tries.new_trie_update(ShardUId::single_shard(), root);
    let final_account_state = get_account(&state_update, &alice_account()).unwrap().unwrap();

    assert_eq!(final_account_state.storage_usage(), 0);
}

#[test]
#[cfg(target_arch = "x86_64")]
fn test_contract_precompilation() {
    use super::create_receipt_with_actions;

    let initial_balance = Balance::from_near(1_000_000);
    let initial_locked = Balance::from_near(500_000);
    let gas_limit = 10u64.pow(15);
    let (runtime, tries, root, apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        initial_balance,
        initial_locked,
        Gas::from_gas(gas_limit),
    );

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
    store_update.commit();

    let contract_code = near_vm_runner::ContractCode::new(wasm_code, None);
    let cached = near_vm_runner::contract_cached(
        Arc::clone(&apply_state.config.wasm_config),
        apply_state.cache.as_deref().unwrap(),
        *contract_code.hash(),
    );
    assert_matches!(cached, Ok(true), "compiled contract should be cached");
}

#[test]
fn test_compute_usage_limit() {
    let (runtime, tries, mut root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        DEFAULT_MINIMAL_GAS_ATTACHMENT,
    );

    let shard_uid = ShardUId::single_shard();

    let sha256_cost = set_sha256_cost(&mut apply_state, 1_000_000u64, 10_000_000_000_000u64);
    // This allows us to execute 1 receipt with a function call per apply.
    apply_state.gas_limit = Some(Gas::from_gas(sha256_cost.compute));

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
            deposit: Balance::ZERO,
        }))],
    );

    let second_call_receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "ext_sha256".to_string(),
            args: b"second".to_vec(),
            gas: sha256_cost.gas,
            deposit: Balance::ZERO,
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
    let (runtime, tries, root, apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
    );

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
            gas: DEFAULT_MINIMAL_GAS_ATTACHMENT,
            deposit: Balance::ZERO,
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
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
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
    store_update.commit();

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
                gas: DEFAULT_MINIMAL_GAS_ATTACHMENT,
                deposit: Balance::ZERO,
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
        Err(StorageError::MissingTrieValue(MissingTrieValue {
            context: MissingTrieValueContext::TrieMemoryPartialStorage,
            hash: _
        }))
    );
    let code_key = TrieKey::ContractCode { account_id: bob_account() };
    assert_matches!(
        storage.get(&code_key.to_vec(), AccessOptions::DEFAULT),
        Err(StorageError::MissingTrieValue(MissingTrieValue {
            context: MissingTrieValueContext::TrieMemoryPartialStorage,
            hash: _
        }))
    );
}

/// Test ProtocolFeature::EnforcePerReceiptStorageProofLimit. A receipt should record at most 4MB of
/// storage proof, no matter how many actions it has.
#[test]
fn test_per_receipt_storage_proof_size_limit() {
    // Number of distinct 1MB values written and then read, one per action.
    const NUM_VALUES: u8 = 5;

    const ACTION_GAS: Gas = Gas::from_teragas(800 / NUM_VALUES as u64);

    assert!(ProtocolFeature::EnforcePerReceiptStorageProofLimit.enabled(PROTOCOL_VERSION));
    let feature_version = ProtocolFeature::EnforcePerReceiptStorageProofLimit.protocol_version();

    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        Balance::from_near(1_000_000),
        Balance::ZERO,
        Gas::from_teragas(1_000),
    );

    let account = alice_account();
    let signer = signers[0].clone();

    // Setup: deploy the contract and write NUM_VALUES 1MB values under keys 0..NUM_VALUES.
    let deploy_receipt = create_receipt_with_actions(
        account.clone(),
        signer.clone(),
        vec![Action::DeployContract(DeployContractAction {
            code: near_test_contracts::rs_contract().to_vec(),
        })],
    );
    let write_receipt = create_receipt_with_actions(
        account.clone(),
        signer.clone(),
        (0..NUM_VALUES)
            .map(|key| {
                Action::FunctionCall(Box::new(FunctionCallAction {
                    method_name: "write_one_megabyte".to_string(),
                    args: vec![key],
                    gas: ACTION_GAS,
                    deposit: Balance::ZERO,
                }))
            })
            .collect(),
    );
    let write_receipt_id = *write_receipt.receipt_id();

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            &apply_state,
            &[deploy_receipt, write_receipt],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    assert_eq!(apply_result.delayed_receipts_count, 0);
    let write_status = apply_result
        .outcomes
        .iter()
        .find(|o| o.id == write_receipt_id)
        .expect("write receipt outcome should be present")
        .outcome
        .status
        .clone();
    assert_matches!(write_status, ExecutionStatus::SuccessValue(_));

    let mut store_update = tries.store_update();
    let root =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit();

    // A single receipt whose actions each read a distinct 1MB value.
    let read_receipt = create_receipt_with_actions(
        account,
        signer,
        (0..NUM_VALUES)
            .map(|key| {
                Action::FunctionCall(Box::new(FunctionCallAction {
                    method_name: "read_n_megabytes".to_string(),
                    args: vec![key, key + 1],
                    gas: ACTION_GAS,
                    deposit: Balance::ZERO,
                }))
            })
            .collect(),
    );
    let read_receipt_id = *read_receipt.receipt_id();

    // Apply `read_receipt` at the given protocol version and return the result.
    let mut apply_read_receipt = |protocol_version: ProtocolVersion| {
        apply_state.current_protocol_version = protocol_version;

        let apply_result = runtime
            .apply(
                tries
                    .get_trie_for_shard(ShardUId::single_shard(), root)
                    .recording_reads_new_recorder(),
                &None,
                &apply_state,
                std::slice::from_ref(&read_receipt),
                SignedValidPeriodTransactions::empty(),
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        apply_result
            .outcomes
            .into_iter()
            .find(|o| o.id == read_receipt_id)
            .expect("read receipt outcome should be present")
            .outcome
            .status
    };

    // Before the fix (per-action limit): every action reads only ~1 MB, which is
    // below the 4 MB limit, so the whole receipt succeeds even though it reads
    // ~5 MB of state in total.
    let status_before = apply_read_receipt(feature_version - 1);
    assert_matches!(status_before, ExecutionStatus::SuccessValue(_));

    // After the fix (per-receipt limit): the cumulative read crosses 4MB, so the receipt fails with
    // RecordedStorageExceeded.
    let status_after = apply_read_receipt(PROTOCOL_VERSION);
    let action_error = assert_matches!(
        status_after,
        ExecutionStatus::Failure(TxExecutionError::ActionError(ae)) => ae
    );
    let error_message = assert_matches!(
        action_error.kind,
        ActionErrorKind::FunctionCallError(FunctionCallError::ExecutionError(msg)) => msg
    );
    assert!(error_message.contains("storage proof"), "unexpected error message: {error_message}");
}

#[test]
fn test_add_keys_after_large_read_exceed_receipt_storage_proof_limit() {
    const NUM_VALUES: u8 = 4;
    // Part of the limit the values leave free. The read's trie nodes fit in it; the
    // `AddKey` actions that follow do not.
    const RESERVED_UNDER_LIMIT: usize = 1_000;
    // Keys `alice` already holds, enough to fill one branch of the access key subtree. An
    // `AddKey` records the subtree nodes its lookup walks that no earlier one did.
    const NUM_EXISTING_KEYS: usize = 16;
    const NUM_ADDED_KEYS: usize = 6;
    const ACTION_GAS: Gas = Gas::from_teragas(100);

    assert!(ProtocolFeature::EnforceStorageProofLimitForAllActions.enabled(PROTOCOL_VERSION));
    let feature_version = ProtocolFeature::EnforceStorageProofLimitForAllActions.protocol_version();

    let shard_uid = ShardUId::single_shard();
    let existing_signers = (0..NUM_EXISTING_KEYS)
        .map(|i| {
            Arc::new(InMemorySigner::from_seed(
                alice_account(),
                KeyType::ED25519,
                &format!("existing{i}"),
            ))
        })
        .collect();
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) =
        setup_runtime_with_keys(
            vec![(alice_account(), existing_signers)],
            Balance::from_near(1_000_000),
            Balance::ZERO,
            Gas::from_teragas(1_000),
        );
    let account = alice_account();
    let signer = signers[0].clone();
    let limit = apply_state.config.wasm_config.limit_config.per_receipt_storage_proof_size_limit;
    let value_size = (limit - RESERVED_UNDER_LIMIT) / NUM_VALUES as usize;

    let apply_receipt = |apply_state: &ApplyState, root: CryptoHash, receipt: &Receipt| {
        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(shard_uid, root).recording_reads_new_recorder(),
                &None,
                apply_state,
                std::slice::from_ref(receipt),
                SignedValidPeriodTransactions::empty(),
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        let status = apply_result
            .outcomes
            .iter()
            .find(|outcome| outcome.id == *receipt.receipt_id())
            .expect("receipt outcome should be present")
            .outcome
            .status
            .clone();
        (apply_result, status)
    };

    // Setup: deploy the contract, then write the values the receipt reads back.
    let setup_receipt = create_receipt_with_actions(
        account.clone(),
        signer.clone(),
        std::iter::once(Action::DeployContract(DeployContractAction {
            code: near_test_contracts::rs_contract().to_vec(),
        }))
        .chain((0..NUM_VALUES).map(|key| {
            let mut args = vec![key];
            args.extend_from_slice(&(value_size as u32).to_le_bytes());
            Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "write_value_of_size".to_string(),
                args,
                gas: ACTION_GAS,
                deposit: Balance::ZERO,
            }))
        }))
        .collect(),
    );
    let (apply_result, setup_status) = apply_receipt(&apply_state, root, &setup_receipt);
    assert_matches!(setup_status, ExecutionStatus::SuccessValue(_));
    let root = commit_apply_result(&apply_result, &mut apply_state, &tries, shard_uid);

    let read_action = Action::FunctionCall(Box::new(FunctionCallAction {
        method_name: "read_values_in_key_range".to_string(),
        args: vec![0, NUM_VALUES],
        gas: ACTION_GAS,
        deposit: Balance::ZERO,
    }));
    let added_keys: Vec<PublicKey> = (0..NUM_ADDED_KEYS)
        .map(|i| {
            InMemorySigner::from_seed(account.clone(), KeyType::ED25519, &format!("added{i}"))
                .public_key()
        })
        .collect();
    let read_receipt =
        create_receipt_with_actions(account.clone(), signer.clone(), vec![read_action.clone()]);
    let read_and_add_keys_receipt = create_receipt_with_actions(
        account.clone(),
        signer,
        std::iter::once(read_action)
            .chain(added_keys.iter().map(|public_key| {
                Action::AddKey(Box::new(AddKeyAction {
                    public_key: public_key.clone(),
                    access_key: AccessKey::full_access(),
                }))
            }))
            .collect(),
    );

    // The read fills the receipt's allowance and still fits, so a real receipt could do it.
    let (_, read_status) = apply_receipt(&apply_state, root, &read_receipt);
    assert_matches!(read_status, ExecutionStatus::SuccessValue(_));

    // Before the feature version only the `FunctionCall` is bounded, so the keys
    // record past the limit unchecked.
    apply_state.current_protocol_version = feature_version - 1;
    let (_, status_before) = apply_receipt(&apply_state, root, &read_and_add_keys_receipt);
    assert_matches!(status_before, ExecutionStatus::SuccessValue(_));

    apply_state.current_protocol_version = PROTOCOL_VERSION;
    let (apply_result, status_after) =
        apply_receipt(&apply_state, root, &read_and_add_keys_receipt);
    let action_error = assert_matches!(
        status_after,
        ExecutionStatus::Failure(TxExecutionError::ActionError(action_error)) => action_error
    );
    assert_eq!(
        action_error.kind,
        ActionErrorKind::ReceiptStorageProofSizeExceeded { limit: limit as u64 }
    );
    let failed_index = action_error.index.unwrap();
    assert!(failed_index > 0, "receipt should fail on an `AddKey`, not on the read");

    let root = commit_apply_result(&apply_result, &mut apply_state, &tries, shard_uid);
    let state = tries.new_trie_update(shard_uid, root);
    for public_key in &added_keys {
        assert_eq!(get_access_key(&state, &account, public_key).unwrap(), None);
    }
}

/// Deploys a contract, records a witness for a call to it (which excludes the
/// contract body), then applies that call over the recorded storage.
fn apply_call_to_contract_missing_from_witness(
    apply_reason: ApplyChunkReason,
) -> Result<ApplyResult, RuntimeError> {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
    );

    let contract_code = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);
    let deploy_receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::DeployContract(DeployContractAction { code: contract_code.code().to_vec() })],
    );
    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            &apply_state,
            &[deploy_receipt],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    let mut store_update = tries.store_update();
    let root =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit();

    let call_receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "log_something".to_string(),
            args: Vec::new(),
            gas: Gas::from_teragas(300),
            deposit: Balance::ZERO,
        }))],
    );
    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            std::slice::from_ref(&call_receipt),
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    assert_eq!(
        apply_result.contract_updates.contract_accesses,
        HashSet::from([CodeHash(*contract_code.hash())])
    );
    let partial_storage = apply_result.proof.unwrap();

    // A validator with an empty compiled-contract cache has no source for the
    // body other than the witness, which excludes it.
    apply_state.cache = Some(Box::new(FilesystemContractRuntimeCache::test().unwrap()));
    apply_state.apply_reason = apply_reason;
    runtime.apply(
        Trie::from_recorded_storage(partial_storage, root, false),
        &None,
        &apply_state,
        std::slice::from_ref(&call_receipt),
        SignedValidPeriodTransactions::empty(),
        &epoch_info_provider,
        Default::default(),
    )
}

#[test]
fn test_validation_rejects_missing_contract_code() {
    let contract_code = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);
    assert_matches!(
        apply_call_to_contract_missing_from_witness(ApplyChunkReason::ValidateChunkStateWitness),
        Err(RuntimeError::StorageError(StorageError::MissingTrieValue(MissingTrieValue {
            context: MissingTrieValueContext::TrieMemoryPartialStorage,
            hash,
        }))) if hash == *contract_code.hash()
    );
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "contract code is missing from the trie")]
fn test_tracked_shard_apply_asserts_on_missing_contract_code() {
    let _ = apply_call_to_contract_missing_from_witness(ApplyChunkReason::UpdateTrackedShard);
}

/// Deploys the test contract to alice and returns the resulting state root.
fn deploy_rs_contract(
    runtime: &Runtime,
    tries: &ShardTries,
    root: CryptoHash,
    apply_state: &ApplyState,
    signer: Arc<Signer>,
    epoch_info_provider: &dyn EpochInfoProvider,
) -> CryptoHash {
    let deploy_receipt = create_receipt_with_actions(
        alice_account(),
        signer,
        vec![Action::DeployContract(DeployContractAction {
            code: near_test_contracts::rs_contract().to_vec(),
        })],
    );
    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            apply_state,
            &[deploy_receipt],
            SignedValidPeriodTransactions::empty(),
            epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    let mut store_update = tries.store_update();
    let root =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit();
    root
}

/// A witness whose recorded state omits the `PromiseYieldIndices` value makes the read in
/// `action_function_call` return `Err(MissingTrieValue)`. The error must propagate; swallowing it
/// with `.unwrap_or_default()` resets the indices to `{0, 0}` and lets `apply` return `Ok`, so a
/// chunk producer could hand out a witness that validates against state it never proved.
///
/// The receipt must create a yield. A call that creates none writes nothing back, and the later
/// `resolve_promise_yield_timeouts` read propagates the same error either way, which would make
/// the two cases indistinguishable.
#[test]
fn test_promise_yield_indices_missing_trie_value_not_swallowed() {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
    );
    // The helper installs `RuntimeConfig::test()`; run the deploy and the yield calls without gas
    // accounting noise instead.
    apply_state.config = Arc::new(RuntimeConfig::free());

    let shard_uid = ShardUId::single_shard();
    let root = deploy_rs_contract(
        &runtime,
        &tries,
        root,
        &apply_state,
        signers[0].clone(),
        &epoch_info_provider,
    );

    let yield_receipt = || {
        create_receipt_with_actions(
            alice_account(),
            signers[0].clone(),
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "call_yield_create_return_data_id".to_string(),
                args: vec![6u8; 16],
                gas: Gas::from_teragas(300),
                deposit: Balance::ZERO,
            }))],
        )
    };

    // First yield call. State now holds indices `{0, 1}` and one timeout entry.
    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(shard_uid, root),
            &None,
            &apply_state,
            &[yield_receipt()],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    // This call writes the `{0, 1}` indices the rest of the test depends on.
    assert_matches!(apply_result.outcomes[0].outcome.status, ExecutionStatus::SuccessValue(_));
    let mut store_update = tries.store_update();
    let root = tries.apply_all(&apply_result.trie_changes, shard_uid, &mut store_update);
    store_update.commit();

    // Replay the same call with recording on, so the proof captures the indices value.
    let second_yield = yield_receipt();
    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(shard_uid, root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            from_ref(&second_yield),
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    // The read under test only runs on the success path, so a failed call would exercise nothing.
    assert_matches!(apply_result.outcomes[0].outcome.status, ExecutionStatus::SuccessValue(_));
    let partial_storage = apply_result.proof.unwrap();
    let PartialState::TrieValues(mut nodes) = partial_storage.nodes;

    // Drop exactly the `PromiseYieldIndices` value blob from the recorded proof.
    let target = hash(
        &borsh::to_vec(&PromiseYieldIndices { first_index: 0, next_available_index: 1 }).unwrap(),
    );
    let before = nodes.len();
    nodes.retain(|value| hash(&value[..]) != target);
    assert_eq!(nodes.len(), before - 1, "expected to remove exactly the indices value blob");

    // Contract code bypasses the trie recorder, so re-add it. The indices blob is now the only
    // node missing from the proof.
    nodes.push(near_test_contracts::rs_contract().to_vec().into());

    let trie = Trie::from_recorded_storage(
        PartialStorage { nodes: PartialState::TrieValues(nodes) },
        root,
        false,
    );
    let result = runtime.apply(
        trie,
        &None,
        &apply_state,
        from_ref(&second_yield),
        SignedValidPeriodTransactions::empty(),
        &epoch_info_provider,
        Default::default(),
    );

    // Bind the missing hash and compare it, so an unrelated omitted blob cannot make this pass.
    let missing_hash = assert_matches!(
        result,
        Err(RuntimeError::StorageError(StorageError::MissingTrieValue(MissingTrieValue {
            context: MissingTrieValueContext::TrieMemoryPartialStorage,
            hash,
        }))) => hash
    );
    assert_eq!(missing_hash, target, "expected the missing value to be the trimmed indices blob");
}

/// The honest path the `?` above could plausibly break: when the `PromiseYieldIndices` key has
/// never been written, the read must still yield the default and the call must succeed.
///
/// This needs a fresh harness. Reusing the state left by the test above would make the assertion
/// vacuous, since the key is already present there.
#[test]
fn test_promise_yield_indices_absent_key_still_applies() {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
    );
    apply_state.config = Arc::new(RuntimeConfig::free());

    let shard_uid = ShardUId::single_shard();
    let root = deploy_rs_contract(
        &runtime,
        &tries,
        root,
        &apply_state,
        signers[0].clone(),
        &epoch_info_provider,
    );

    // `setup_runtime_for_shard` writes accounts and access keys only, and the deploy above does
    // not touch the queue, so the key is genuinely absent.
    assert_eq!(
        tries
            .get_trie_for_shard(shard_uid, root)
            .get(&TrieKey::PromiseYieldIndices.to_vec(), AccessOptions::DEFAULT)
            .unwrap(),
        None,
        "the promise yield indices key must be absent for this control to mean anything"
    );

    // A call that creates no yield leaves the indices untouched, so nothing is written back.
    let call_receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "log_something".to_string(),
            args: Vec::new(),
            gas: Gas::from_teragas(300),
            deposit: Balance::ZERO,
        }))],
    );
    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(shard_uid, root),
            &None,
            &apply_state,
            &[call_receipt],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    assert_matches!(apply_result.outcomes[0].outcome.status, ExecutionStatus::SuccessValue(_));
}

// Tests excluding contract code from state witness and recording of contract deployments and function calls.
#[test]
fn test_exclude_contract_code_from_witness() {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
    );

    const CONTRACT_SIZE: usize = 5000;

    // Set the storage proof soft-limit to the size of the contract.
    // Since contract code is not included in the storage proof, both function calls below pass the proof soft-limit.
    let mut runtime_config = RuntimeConfig::test();
    runtime_config.witness_config.main_storage_proof_size_soft_limit = CONTRACT_SIZE as u64;
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
    store_update.commit();

    let function_call_fn = |account_id: AccountId, signer: Arc<Signer>| {
        create_receipt_with_actions(
            account_id,
            signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "main".to_string(),
                args: Vec::new(),
                gas: DEFAULT_MINIMAL_GAS_ATTACHMENT,
                deposit: Balance::ZERO,
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
        Err(StorageError::MissingTrieValue(MissingTrieValue {
            context: MissingTrieValueContext::TrieMemoryPartialStorage,
            hash: _
        }))
    );
    let code_key = TrieKey::ContractCode { account_id: bob_account() };
    assert_matches!(
        storage.get(&code_key.to_vec(), AccessOptions::DEFAULT),
        Err(StorageError::MissingTrieValue(MissingTrieValue {
            context: MissingTrieValueContext::TrieMemoryPartialStorage,
            hash: _
        }))
    );
}

// Tests excluding contract code from state witness and recording of contract deployments and function calls
// with one of the function calls fail due to exceeding the gas limit.
#[test]
fn test_exclude_contract_code_from_witness_with_failed_call() {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
    );

    let sha256_cost = set_sha256_cost(&mut apply_state, 1_000_000u64, 10_000_000_000_000u64);
    // This allows us to execute 1 receipt with a function call per apply.
    apply_state.gas_limit = Some(Gas::from_gas(sha256_cost.compute));

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
    store_update.commit();

    let function_call_fn = |account_id: AccountId, signer: Arc<Signer>| {
        create_receipt_with_actions(
            account_id,
            signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "ext_sha256".to_string(),
                args: b"first".to_vec(),
                gas: sha256_cost.gas,
                deposit: Balance::ZERO,
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
        Err(StorageError::MissingTrieValue(MissingTrieValue {
            context: MissingTrieValueContext::TrieMemoryPartialStorage,
            hash: _
        }))
    );
    let code_key = TrieKey::ContractCode { account_id: bob_account() };
    assert_matches!(
        storage.get(&code_key.to_vec(), AccessOptions::DEFAULT),
        Err(StorageError::MissingTrieValue(MissingTrieValue {
            context: MissingTrieValueContext::TrieMemoryPartialStorage,
            hash: _
        }))
    );
}

// Tests excluding contract code from state witness and recording of contract deployments and function calls
// where different contracts are deployed to different accounts, to check if we record code-hashes of both contracts.
#[test]
fn test_deploy_and_call_different_contracts() {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        DEFAULT_MINIMAL_GAS_ATTACHMENT,
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
            gas: DEFAULT_MINIMAL_GAS_ATTACHMENT,
            deposit: Balance::ZERO,
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
            gas: DEFAULT_MINIMAL_GAS_ATTACHMENT,
            deposit: Balance::ZERO,
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
    store_update.commit();

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
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        DEFAULT_MINIMAL_GAS_ATTACHMENT,
    );

    let sha256_cost = set_sha256_cost(&mut apply_state, 1_000_000u64, 10_000_000_000_000u64);
    // This allows us to execute 1 receipt with a function call per apply.
    apply_state.gas_limit = Some(Gas::from_gas(sha256_cost.compute));

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
            deposit: Balance::ZERO,
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
            gas: DEFAULT_MINIMAL_GAS_ATTACHMENT,
            deposit: Balance::ZERO,
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
    store_update.commit();

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
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        DEFAULT_MINIMAL_GAS_ATTACHMENT,
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
            gas: DEFAULT_MINIMAL_GAS_ATTACHMENT,
            deposit: Balance::ZERO,
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
            gas: DEFAULT_MINIMAL_GAS_ATTACHMENT,
            deposit: Balance::ZERO,
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
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        DEFAULT_MINIMAL_GAS_ATTACHMENT,
    );

    let sha256_cost = set_sha256_cost(&mut apply_state, 1_000_000u64, 10_000_000_000_000u64);
    // This allows us to execute 1 receipt with a function call per apply.
    apply_state.gas_limit = Some(Gas::from_gas(sha256_cost.compute));

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
            deposit: Balance::ZERO,
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
            gas: DEFAULT_MINIMAL_GAS_ATTACHMENT,
            deposit: Balance::ZERO,
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
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
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
            gas: DEFAULT_MINIMAL_GAS_ATTACHMENT,
            deposit: Balance::ZERO,
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
    store_update.commit();

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
            gas: DEFAULT_MINIMAL_GAS_ATTACHMENT,
            deposit: Balance::ZERO,
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
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        DEFAULT_MINIMAL_GAS_ATTACHMENT,
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
                gas: DEFAULT_MINIMAL_GAS_ATTACHMENT,
                deposit: Balance::ZERO,
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
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        DEFAULT_MINIMAL_GAS_ATTACHMENT,
    );

    let sha256_cost = set_sha256_cost(&mut apply_state, 1_000_000u64, 10_000_000_000_000u64);
    // This allows us to execute 1 receipt with a function call per apply.
    apply_state.gas_limit = Some(Gas::from_gas(sha256_cost.compute));

    let contract_code = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);
    let receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![
            Action::DeployContract(DeployContractAction { code: contract_code.code().to_vec() }),
            Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "ext_sha256".to_string(),
                args: b"first".to_vec(),
                gas: DEFAULT_MINIMAL_GAS_ATTACHMENT,
                deposit: Balance::ZERO,
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
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        DEFAULT_MINIMAL_GAS_ATTACHMENT,
    );

    apply_state.config = Arc::new(RuntimeConfig::free());

    let receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "main".to_string(),
            args: vec![],
            gas: DEFAULT_MINIMAL_GAS_ATTACHMENT,
            deposit: Balance::ZERO,
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
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        DEFAULT_MINIMAL_GAS_ATTACHMENT,
    );

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
            gas: DEFAULT_MINIMAL_GAS_ATTACHMENT,
            deposit: Balance::ZERO,
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
    store_update.commit();

    // Apply chunk for updating the shard, so the contract accesses are recorded.
    apply_state.apply_reason = ApplyChunkReason::UpdateTrackedShard;

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            std::slice::from_ref(&call_receipt),
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
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
    );

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
    store_update.commit();

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
    assert!(
        total_size < PREV_CONTRACT_SIZE,
        "Contract code should not be in storage proof. Storage proof size: {}",
        total_size
    );
}

/// Tests that the existing contract is not recorded in the state witness for a delete-account action.
/// For this, it creates an account, deploys a contract to it, and deletes that account, and checks
/// the storage proof size after the delete-account action.
#[test]
fn test_exclude_existing_contract_code_for_delete_account_action() {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
    );

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
        Balance::from_near(100_000),
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
    store_update.commit();

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
    assert!(
        total_size < CONTRACT_SIZE,
        "Contract code should not be in storage proof. Storage proof size: {}",
        total_size
    );
}

/// Check that applying nothing does not change the state trie.
/// UPDATE: BandwidthScheduler runs on every height and modifies the state, so this is no longer true for newer protocol versions
///
/// This test is useful to check that trie columns are not accidentally
/// initialized. Many integration tests will fail as well if this fails, but
/// those are harder to root cause.
#[test]
fn test_empty_apply() {
    let initial_balance = Balance::from_near(1_000_000);
    let initial_locked = Balance::from_near(500_000);
    let gas_limit = 10u64.pow(15);
    let (runtime, tries, root_before, apply_state, _signer, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        initial_balance,
        initial_locked,
        Gas::from_gas(gas_limit),
    );

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
    let initial_balance = Balance::from_near(10);
    let initial_locked = Balance::from_near(0);
    let deposit = Balance::from_near(1);
    let gas_limit = 1;
    let (runtime, tries, root, apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        initial_balance,
        initial_locked,
        Gas::from_gas(gas_limit),
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
    let expected_delayed_gas = Gas::from_gas(
        (n - 1)
            * compute_receipt_congestion_gas(&receipts[0], &apply_state.config).unwrap().as_gas(),
    );
    let expected_receipts_bytes = (n - 1) * compute_receipt_size(&receipts[0]).unwrap() as u64;

    assert_eq!(u128::from(expected_delayed_gas.as_gas()), congestion.delayed_receipts_gas());
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

    let initial_balance = Balance::from_near(1_000_000);
    let initial_locked = Balance::from_near(500_000);
    let deposit = Balance::from_near(10_000);
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
        Gas::from_gas(gas_limit),
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
        store_update.commit();

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
    assert_eq!(congestion.outgoing_gas_limit(local_shard), Gas::ZERO);

    // release congestion to just below 1.0, which should allow one receipt
    // to be forwarded per round
    apply_state
        .congestion_info
        .get_mut(&receiver_shard)
        .unwrap()
        .congestion_info
        .remove_delayed_receipt_gas(Gas::from_gas(100))
        .unwrap();

    let min_outgoing_gas: Gas = apply_state.config.congestion_control_config.min_outgoing_gas;
    // Check congestion is less than 1.0
    let congestion = apply_state.congestion_control(receiver_shard, 0);
    assert!(congestion.congestion_level() < 1.0);
    // this exact number does not matter but if it changes the test setup
    // needs to adapt to ensure the number of forwarded receipts is as expected
    assert!(
        congestion
            .outgoing_gas_limit(local_shard)
            .as_gas()
            .checked_sub(min_outgoing_gas.as_gas())
            .unwrap()
            < 100 * 10u64.pow(9),
        "allowed forwarding must be less than 100 GGas away from MIN_OUTGOING_GAS"
    );

    // Checking n receipts delayed by 1 + 3 extra
    let forwarded_per_chunk = min_outgoing_gas.as_gas() / MAX_ATTACHED_GAS.as_gas();
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
    store_update.commit();
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
    let initial_balance = Balance::from_near(1_000_000);
    let initial_locked = Balance::from_near(500_000);
    let gas_limit = 10u64.pow(15);
    let (runtime, tries, root, mut apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        initial_balance,
        initial_locked,
        Gas::from_gas(gas_limit),
    );

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
    let (runtime, tries, root, apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
    );

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
                gas: MAX_ATTACHED_GAS.checked_div(2).unwrap(),
                deposit: Balance::ZERO,
            })),
            Action::DeployContract(DeployContractAction {
                code: near_test_contracts::trivial_contract().to_vec(),
            }),
            Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "log_something".to_string(),
                args: vec![],
                gas: MAX_ATTACHED_GAS.checked_div(2).unwrap(),
                deposit: Balance::ZERO,
            })),
        ],
        CryptoHash::default(),
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

fn execution_outcome_contracts(outcome: &ExecutionOutcome) -> Vec<AccountContract> {
    match &outcome.metadata {
        ExecutionMetadata::V4(v4) => v4.contracts.clone(),
        other => panic!("expected V4 metadata, got {other:?}"),
    }
}

#[test]
fn test_deploy_and_call_local_receipts() {
    let (runtime, tries, root, apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
    );

    let tx1 = SignedTransaction::from_actions(
        1,
        alice_account(),
        alice_account(),
        &*signers[0],
        vec![Action::DeployContract(DeployContractAction {
            code: near_test_contracts::rs_contract().to_vec(),
        })],
        CryptoHash::default(),
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
                gas: MAX_ATTACHED_GAS.checked_div(2).unwrap(),
                deposit: Balance::ZERO,
            })),
            Action::DeployContract(DeployContractAction {
                code: near_test_contracts::trivial_contract().to_vec(),
            }),
            Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "log_something".to_string(),
                args: vec![],
                gas: MAX_ATTACHED_GAS.checked_div(2).unwrap(),
                deposit: Balance::ZERO,
            })),
        ],
        CryptoHash::default(),
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

    // V4 metadata: one `contracts` entry per receipt action, recording the
    // contract on the receiver account before that action ran (in receipt
    // order). o1's receipt is a single DeployContract on a fresh alice → the
    // pre-action contract is `None`. o2's receipt is
    // [FunctionCall, DeployContract, FunctionCall]: the first FunctionCall
    // sees rs_contract just deployed by tx1; the DeployContract action also
    // sees rs_contract (it then replaces it with trivial_contract); the
    // trailing FunctionCall sees trivial_contract and records it even though
    // it fails at method-resolve — the contract is captured before the call
    // is dispatched.
    let rs_hash = CryptoHash::hash_bytes(near_test_contracts::rs_contract());
    let trivial_hash = CryptoHash::hash_bytes(near_test_contracts::trivial_contract());
    assert_eq!(execution_outcome_contracts(o1), vec![AccountContract::None]);
    assert_eq!(
        execution_outcome_contracts(o2),
        vec![
            AccountContract::Local(rs_hash),
            AccountContract::Local(rs_hash),
            AccountContract::Local(trivial_hash),
        ],
    );
}

/// When a non-final action errors, the action loop breaks before later
/// actions run. The V4 `contracts` vector is then resized to match the
/// receipt's action count with `AccountContract::None`, so consumers can
/// still index by action position. Here the receipt is
/// [DeployContract, DeleteKey(missing), FunctionCall]: action 0 deploys
/// rs_contract (pre-action contract: `None`), action 1 then fails (pre-action
/// contract: `Local(rs_hash)` — the deploy from action 0 took effect even
/// though the receipt as a whole fails), and the trailing FunctionCall never
/// runs — its slot must land on `None` via the resize pad, not via a real
/// contract resolution. The `Local(rs_hash)` entry in the middle is what
/// distinguishes a real per-action capture from the pad.
#[test]
fn test_apply_v4_metadata_pads_unexecuted_actions() {
    let (runtime, tries, root, apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
    );

    let nonexistent_pk =
        InMemorySigner::from_seed(alice_account(), KeyType::ED25519, "nonexistent").public_key();
    let receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![
            Action::DeployContract(DeployContractAction {
                code: near_test_contracts::rs_contract().to_vec(),
            }),
            Action::DeleteKey(Box::new(DeleteKeyAction { public_key: nonexistent_pk })),
            Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "log_something".to_string(),
                args: vec![],
                gas: MAX_ATTACHED_GAS.checked_div(2).unwrap(),
                deposit: Balance::ZERO,
            })),
        ],
    );

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            &apply_state,
            &[receipt],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    let outcome = assert_matches!(
        &apply_result.outcomes[..],
        [ExecutionOutcomeWithId { id: _, outcome }] => outcome
    );
    let action_error = assert_matches!(
        &outcome.status,
        ExecutionStatus::Failure(TxExecutionError::ActionError(ae)) => ae
    );
    assert_eq!(action_error.index, Some(1));
    assert_matches!(action_error.kind, ActionErrorKind::DeleteKeyDoesNotExist { .. });

    let rs_hash = CryptoHash::hash_bytes(near_test_contracts::rs_contract());
    assert_eq!(
        execution_outcome_contracts(outcome),
        vec![AccountContract::None, AccountContract::Local(rs_hash), AccountContract::None]
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
        Balance::from_yoctonear(100),
        CryptoHash::default(),
    );
    let alice_tx1 = SignedTransaction::send_money(
        1,
        alice_account(),
        alice_account(),
        &alice_signer,
        Balance::from_yoctonear(200),
        CryptoHash::default(),
    );
    let alice_tx2 = SignedTransaction::send_money(
        2,
        alice_account(),
        bob_account(),
        &alice_signer,
        Balance::from_yoctonear(300),
        CryptoHash::default(),
    );
    let bob_tx1 = SignedTransaction::send_money(
        1,
        bob_account(),
        bob_account(),
        &bob_signer,
        Balance::from_yoctonear(400),
        CryptoHash::default(),
    );
    let bob_tx2 = SignedTransaction::send_money(
        2,
        bob_account(),
        alice_account(),
        &bob_signer,
        Balance::from_yoctonear(500),
        CryptoHash::default(),
    );
    let bob_tx3 = SignedTransaction::send_money(
        3,
        bob_account(),
        bob_account(),
        &bob_signer,
        Balance::from_yoctonear(600),
        CryptoHash::default(),
    );

    let txs = vec![
        bob_tx1.clone(),
        alice_invalid_tx.clone(),
        alice_tx1.clone(),
        bob_tx2.clone(),
        alice_tx2.clone(),
        bob_tx3.clone(),
    ];

    let (runtime, tries, root, apply_state, _signers, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
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
        alice_invalid_tx.get_hash(),
        alice_tx1.get_hash(),
        bob_tx2.get_hash(),
        alice_tx2.get_hash(),
        bob_tx3.get_hash(),
    ];

    let num_outcomes = expected_order.len();
    // Note: The 3 local receipts are generated for valid transactions
    // where signer_id == receiver_id - tx2, tx4, tx6 (not for tx1 as it is dropped).
    assert_eq!(
        apply_result.outcomes.len(),
        num_outcomes + 3,
        "should have processed {num_outcomes} transactions and 3 local receipts"
    );
    let tx_outcomes =
        apply_result.outcomes.iter().take(num_outcomes).map(|o| o.id).collect::<Vec<_>>();
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
            Balance::from_near(1000),
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
            Balance::from_near(1_000_000),
            Balance::from_near(500_000),
            Gas::from_teragas(1000),
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

    assert!(account.amount() < Balance::from_near(994_000));
    assert!(account.amount() > Balance::from_near(993_000));
}

/// Tests that a transaction failing after the allowance check does not mutate
/// the access key allowance. Scenario: two function call transactions using the
/// same access key. Tx1 targets the wrong receiver (fails at
/// verify_function_call_permission, which runs after the allowance check). Tx2
/// targets the correct receiver. Since tx1 does not touch the allowance, tx2
/// still sees the full allowance and succeeds.
#[test]
fn test_access_key_allowance_not_mutated_on_failed_tx() {
    let alice_signer = Arc::new(InMemorySigner::test_signer(&alice_account()));

    let config = Arc::new(RuntimeConfig::test());
    // Compute cost of one function call transaction so we can set allowance tightly.
    let sample_tx = SignedTransaction::from_actions(
        1,
        alice_account(),
        bob_account(),
        &*alice_signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "hello".to_string(),
            args: vec![],
            gas: DEFAULT_MINIMAL_GAS_ATTACHMENT,
            deposit: Balance::ZERO,
        }))],
        CryptoHash::default(),
    );
    let sample_cost = crate::config::tx_cost(&config, &sample_tx.transaction, GAS_PRICE).unwrap();
    // Set allowance so it covers exactly one transaction's total_cost.
    let allowance = sample_cost.total_cost;

    // Build state manually with a function call access key.
    let tries = TestTriesBuilder::new().build();
    let shard_uid = ShardUId::single_shard();
    let root = MerkleHash::default();
    let mut initial_state = tries.new_trie_update(shard_uid, root);

    let access_key = AccessKey {
        nonce: 0,
        permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
            allowance: Some(allowance),
            receiver_id: bob_account().into(),
            method_names: vec![],
        }),
    };
    let mut alice = account_new(Balance::from_near(1_000_000), CryptoHash::default());
    alice.set_storage_usage(182);
    set_account(&mut initial_state, alice_account(), &alice);
    set_access_key(&mut initial_state, alice_account(), alice_signer.public_key(), &access_key);
    let bob = account_new(Balance::from_near(1_000_000), CryptoHash::default());
    set_account(&mut initial_state, bob_account(), &bob);

    initial_state.commit(StateChangeCause::InitialState);
    let trie_changes = initial_state.finalize().unwrap().trie_changes;
    let mut store_update = tries.store_update();
    let root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
    store_update.commit();

    let runtime = Runtime::new();
    let contract_cache = FilesystemContractRuntimeCache::test().unwrap();
    let epoch_info_provider = MockEpochInfoProvider::default();
    let shard_layout = epoch_info_provider.shard_layout(&EpochId::default()).unwrap();
    let shard_ids = shard_layout.shard_ids();
    let shards_congestion_info =
        shard_ids.map(|id| (id, ExtendedCongestionInfo::default())).collect();
    let mut apply_state = ApplyState {
        apply_reason: ApplyChunkReason::UpdateTrackedShard,
        block_height: 1,
        prev_block_hash: Default::default(),
        shard_id: shard_uid.shard_id(),
        epoch_id: Default::default(),
        epoch_height: 0,
        gas_price: GAS_PRICE,
        block_timestamp: 100,
        gas_limit: Some(Gas::from_teragas(1000)),
        random_seed: Default::default(),
        current_protocol_version: PROTOCOL_VERSION,
        config,
        next_wasm_config: None,
        cache: Some(Box::new(contract_cache)),
        is_new_chunk: true,
        save_receipt_to_tx: false,
        congestion_info: BlockCongestionInfo::new(shards_congestion_info),
        bandwidth_requests: BlockBandwidthRequests::empty(),
        trie_access_tracker_state: Default::default(),
        on_post_state_ready: None,
    };

    let make_fc_tx = |nonce, receiver| {
        SignedTransaction::from_actions(
            nonce,
            alice_account(),
            receiver,
            &*alice_signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "hello".to_string(),
                args: vec![],
                gas: DEFAULT_MINIMAL_GAS_ATTACHMENT,
                deposit: Balance::ZERO,
            }))],
            CryptoHash::default(),
        )
    };

    // tx1: wrong receiver → fails at verify_function_call_permission
    // tx2: correct receiver → should succeed if allowance is intact
    let tx1 = make_fc_tx(1, alice_account()); // wrong receiver
    let tx2 = make_fc_tx(2, bob_account()); // correct receiver
    let txs = vec![tx1.clone(), tx2.clone()];
    let signed_valid_period_txs = SignedValidPeriodTransactions::new(txs, vec![true, true]);

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(shard_uid, root),
            &None,
            &apply_state,
            &[],
            signed_valid_period_txs,
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    // tx1 fails at verify_function_call_permission (after the allowance check) but
    // does not mutate the allowance, so tx2 still sees the full allowance and succeeds.
    // Both outcomes are recorded.
    assert_eq!(apply_result.outcomes.len(), 2);
    let tx1_outcome = &apply_result.outcomes[0];
    assert_eq!(tx1_outcome.id, tx1.get_hash());
    assert_matches!(
        &tx1_outcome.outcome.status,
        ExecutionStatus::Failure(TxExecutionError::InvalidTxError(
            InvalidTxError::InvalidAccessKeyError(
                near_primitives::errors::InvalidAccessKeyError::ReceiverMismatch { .. }
            )
        ))
    );
    let tx2_outcome = &apply_result.outcomes[1];
    assert_eq!(tx2_outcome.id, tx2.get_hash());
    assert_matches!(&tx2_outcome.outcome.status, ExecutionStatus::SuccessReceiptId(_));

    // Verify the access key state after apply: tx2 succeeded → allowance was consumed
    // and written to trie.
    let root = commit_apply_result(&apply_result, &mut apply_state, &tries, shard_uid);
    let state = tries.new_trie_update(shard_uid, root);
    let ak = get_access_key(&state, &alice_account(), &alice_signer.public_key()).unwrap().unwrap();
    let final_allowance = ak.permission.function_call_permission().unwrap().allowance.unwrap();
    assert!(final_allowance < allowance, "allowance should decrease after successful tx2");
}

#[test]
fn test_expired_transaction() {
    let alice_signer = InMemorySigner::test_signer(&alice_account());
    let expired_tx = vec![SignedTransaction::send_money(
        1,
        alice_account(),
        alice_account(),
        &alice_signer,
        Balance::from_yoctonear(1),
        CryptoHash::default(),
    )];
    let (runtime, tries, root, apply_state, _signers, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
    );
    let signed_valid_period_txs =
        SignedValidPeriodTransactions::new(expired_tx.clone(), vec![false]);
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
        1,
        "should have produced one outcome for the expired tx"
    );
    let outcome = &apply_result.outcomes[0];
    assert_eq!(outcome.id, expired_tx[0].get_hash());
    assert_matches!(
        &outcome.outcome.status,
        ExecutionStatus::Failure(TxExecutionError::InvalidTxError(InvalidTxError::Expired))
    );
}

#[test]
fn test_duplicate_transaction_in_chunk_skipped() {
    let alice_signer = InMemorySigner::test_signer(&alice_account());
    let send_money = |nonce| {
        SignedTransaction::send_money(
            nonce,
            alice_account(),
            bob_account(),
            &alice_signer,
            Balance::from_near(1),
            CryptoHash::default(),
        )
    };
    let tx = send_money(1);
    // A distinct transaction (different nonce, different hash) that must not be skipped.
    let other = send_money(2);
    let (tx_hash, other_hash) = (tx.get_hash(), other.get_hash());
    let (runtime, tries, root, apply_state, _signers, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
    );
    assert!(ProtocolFeature::UniqueChunkTransactions.enabled(PROTOCOL_VERSION));

    // [T, U, T]: the repeat of T is non-adjacent to the original.
    let signed_valid_period_txs =
        SignedValidPeriodTransactions::new(vec![tx.clone(), other, tx], vec![true; 3]);
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

    // The repeat of T is skipped, leaving a single success outcome under its
    // hash rather than a success and a conflicting InvalidNonce failure, while
    // the distinct transaction U is processed normally.
    let tx_outcomes = |id| apply_result.outcomes.iter().filter(|o| o.id == id).collect::<Vec<_>>();
    let (tx_outcomes, other_outcomes) = (tx_outcomes(tx_hash), tx_outcomes(other_hash));
    assert_eq!(tx_outcomes.len(), 1, "duplicate transaction must be skipped");
    assert_matches!(tx_outcomes[0].outcome.status, ExecutionStatus::SuccessReceiptId(_));
    assert_eq!(other_outcomes.len(), 1, "distinct transaction must not be skipped");
    assert_matches!(other_outcomes[0].outcome.status, ExecutionStatus::SuccessReceiptId(_));
}

#[test]
fn test_duplicate_transaction_in_chunk_prior_behavior() {
    let alice_signer = InMemorySigner::test_signer(&alice_account());
    let tx = SignedTransaction::send_money(
        1,
        alice_account(),
        bob_account(),
        &alice_signer,
        Balance::from_near(1),
        CryptoHash::default(),
    );
    let tx_hash = tx.get_hash();
    let (runtime, tries, root, mut apply_state, _signers, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
    );
    apply_state.current_protocol_version =
        ProtocolFeature::UniqueChunkTransactions.protocol_version() - 1;

    let signed_valid_period_txs =
        SignedValidPeriodTransactions::new(vec![tx.clone(), tx], vec![true, true]);
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

    // Without the feature both copies are processed: the second records a
    // conflicting InvalidNonce failure under the same id as the success.
    let tx_outcomes: Vec<_> = apply_result.outcomes.iter().filter(|o| o.id == tx_hash).collect();
    assert_eq!(tx_outcomes.len(), 2);
    assert_matches!(tx_outcomes[0].outcome.status, ExecutionStatus::SuccessReceiptId(_));
    assert_matches!(
        tx_outcomes[1].outcome.status,
        ExecutionStatus::Failure(TxExecutionError::InvalidTxError(
            InvalidTxError::InvalidNonce { .. }
        ))
    );
}

#[test]
fn test_gas_key_burn_not_reported_on_failed_receipt() {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        Balance::from_near(1_000_000),
        Balance::ZERO,
        Gas::from_teragas(1000),
    );
    apply_state.current_protocol_version = ProtocolFeature::GasKeys.protocol_version();

    let gas_key_pk =
        InMemorySigner::from_seed(alice_account(), KeyType::ED25519, "gas_key").public_key();
    let deposit_amount = Balance::from_near(1);

    // Phase 1: Add a gas key and fund it.
    let setup_receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![
            Action::AddKey(Box::new(AddKeyAction {
                public_key: gas_key_pk.clone(),
                access_key: AccessKey::gas_key_full_access(2),
            })),
            Action::TransferToGasKey(Box::new(TransferToGasKeyAction {
                public_key: gas_key_pk.clone(),
                deposit: deposit_amount,
            })),
        ],
    );
    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            &apply_state,
            &[setup_receipt],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    assert!(matches!(apply_result.outcomes[0].outcome.status, ExecutionStatus::SuccessValue(_)));
    let mut store_update = tries.store_update();
    let root =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit();

    // Verify gas key was created and funded.
    let state = tries.new_trie_update(ShardUId::single_shard(), root);
    let access_key = get_access_key(&state, &alice_account(), &gas_key_pk).unwrap().unwrap();
    assert_eq!(access_key.gas_key_info().unwrap().balance, deposit_amount);

    // Phase 2: Multi-action receipt where a gas key deletion is followed by a
    // failing action. The entire receipt should fail and state should roll back,
    // so the gas key balance must NOT be reported as burned.
    let nonexistent_pk =
        InMemorySigner::from_seed(alice_account(), KeyType::ED25519, "nonexistent").public_key();
    let test_receipt = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![
            Action::DeleteKey(Box::new(DeleteKeyAction { public_key: gas_key_pk.clone() })),
            Action::DeleteKey(Box::new(DeleteKeyAction { public_key: nonexistent_pk })),
        ],
    );
    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            &apply_state,
            &[test_receipt],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    let outcome = &apply_result.outcomes[0].outcome;
    assert!(matches!(outcome.status, ExecutionStatus::Failure(_)));
    // tokens_burnt must not include the gas key balance — only gas costs.
    assert!(
        outcome.tokens_burnt < deposit_amount,
        "tokens_burnt ({}) should not include gas key balance ({})",
        outcome.tokens_burnt,
        deposit_amount,
    );

    // Gas key should still exist with its balance after rollback.
    let mut store_update = tries.store_update();
    let root =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit();
    let state = tries.new_trie_update(ShardUId::single_shard(), root);
    let access_key = get_access_key(&state, &alice_account(), &gas_key_pk).unwrap().unwrap();
    assert_eq!(access_key.gas_key_info().unwrap().balance, deposit_amount);
}

const GAS_KEY_BLOCK_HEIGHT: BlockHeight = 10;

struct GasKeyTestSetup {
    runtime: Runtime,
    tries: ShardTries,
    root: CryptoHash,
    apply_state: ApplyState,
    epoch_info_provider: MockEpochInfoProvider,
    gas_key_signer: Arc<Signer>,
    shard_uid: ShardUId,
}

fn setup_gas_key_test(
    gas_key_owner: AccountId,
    accounts: Vec<AccountId>,
    initial_balance: Balance,
    num_nonces: u16,
    gas_key_balance: Balance,
) -> GasKeyTestSetup {
    assert!(accounts.contains(&gas_key_owner), "gas_key_owner must be in accounts");
    let epoch_info_provider = MockEpochInfoProvider::default();
    let shard_layout = epoch_info_provider.shard_layout(&EpochId::default()).unwrap();
    let shard_uid = shard_layout.shard_uids().next().unwrap();
    let accounts_with_keys = accounts
        .into_iter()
        .map(|id| {
            let signer = Arc::new(InMemorySigner::test_signer(&id));
            (id, vec![signer])
        })
        .collect();
    let (runtime, tries, root, mut apply_state, _signers) = setup_runtime_for_shard(
        accounts_with_keys,
        initial_balance,
        Balance::ZERO,
        Gas::from_teragas(1000),
        shard_uid,
        &shard_layout,
    );
    apply_state.current_protocol_version = ProtocolFeature::GasKeys.protocol_version();
    apply_state.block_height = GAS_KEY_BLOCK_HEIGHT;

    let mut state_update = tries.new_trie_update(shard_uid, root);

    let mut account = get_account(&state_update, &gas_key_owner).unwrap().unwrap();
    let gas_key_signer = Arc::new(InMemorySigner::from_seed(
        gas_key_owner.clone(),
        near_crypto::KeyType::ED25519,
        "gas_key_seed",
    ));
    let gas_key = AccessKey::gas_key_full_access(num_nonces);
    let mut result = ActionResult::default();
    action_add_key(
        &apply_state,
        &mut state_update,
        &mut account,
        &mut result,
        &gas_key_owner,
        &AddKeyAction { public_key: gas_key_signer.public_key(), access_key: gas_key },
    )
    .unwrap();

    let mut access_key =
        get_access_key(&state_update, &gas_key_owner, &gas_key_signer.public_key())
            .unwrap()
            .unwrap();
    access_key.gas_key_info_mut().unwrap().balance = gas_key_balance;
    set_access_key(
        &mut state_update,
        gas_key_owner.clone(),
        gas_key_signer.public_key(),
        &access_key,
    );
    set_account(&mut state_update, gas_key_owner, &account);

    state_update.commit(StateChangeCause::InitialState);
    let trie_changes = state_update.finalize().unwrap().trie_changes;
    let mut store_update = tries.store_update();
    let root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
    store_update.commit();

    GasKeyTestSetup {
        runtime,
        tries,
        root,
        apply_state,
        epoch_info_provider,
        gas_key_signer,
        shard_uid,
    }
}

#[test]
fn test_apply_gas_key_transaction() {
    let num_nonces = 3;
    let initial_balance = Balance::from_near(1_000_000);
    let transfer_amount = Balance::from_near(100);
    let gas_key_balance = Balance::from_millinear(1);
    let GasKeyTestSetup {
        runtime,
        tries,
        root,
        mut apply_state,
        epoch_info_provider,
        gas_key_signer,
        shard_uid,
    } = setup_gas_key_test(
        alice_account(),
        vec![alice_account(), bob_account()],
        initial_balance,
        num_nonces,
        gas_key_balance,
    );

    let initial_nonce = initial_nonce_value(GAS_KEY_BLOCK_HEIGHT);
    let nonce_index = 1;

    // Create a gas key transaction
    let gas_key_tx = SignedTransaction::from_actions_v1(
        TransactionNonce::from_nonce_and_index(initial_nonce + 1, nonce_index),
        alice_account(),
        bob_account(),
        &*gas_key_signer,
        vec![Action::Transfer(TransferAction { deposit: transfer_amount })],
        CryptoHash::default(),
    );
    let transaction_cost =
        tx_cost(&apply_state.config, &gas_key_tx.transaction, apply_state.gas_price).unwrap();

    // Apply the transaction
    let signed_valid_period_txs = SignedValidPeriodTransactions::new(vec![gas_key_tx], vec![true]);
    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(shard_uid, root),
            &None,
            &apply_state,
            &[],
            signed_valid_period_txs,
            &epoch_info_provider,
            Default::default(),
        )
        .expect("apply should succeed");

    // Verify transaction produced an outcome
    assert_eq!(apply_result.outcomes.len(), 1, "should have one outcome for gas key tx");
    let outcome = &apply_result.outcomes[0];
    assert_matches!(&outcome.outcome.status, ExecutionStatus::SuccessReceiptId(_));

    // Commit apply result and verify state changes
    let root = commit_apply_result(&apply_result, &mut apply_state, &tries, shard_uid);
    let state = tries.new_trie_update(shard_uid, root);

    // Verify gas key nonce was updated
    let new_nonce =
        get_gas_key_nonce(&state, &alice_account(), &gas_key_signer.public_key(), nonce_index)
            .unwrap()
            .expect("gas key nonce should exist");
    assert_eq!(new_nonce, initial_nonce + 1, "gas key nonce should be updated");

    // Verify other gas key nonces are unchanged
    for index in 0..num_nonces {
        if index == nonce_index {
            continue;
        }
        let other_nonce =
            get_gas_key_nonce(&state, &alice_account(), &gas_key_signer.public_key(), index)
                .unwrap()
                .expect("gas key nonce should exist");
        assert_eq!(other_nonce, initial_nonce, "other gas key nonce should be unchanged");
    }

    assert!(!transaction_cost.gas_cost.is_zero());
    assert_eq!(transaction_cost.deposit_cost, transfer_amount);

    // Verify account pays for deposit, gas key balance pays for gas
    let account = get_account(&state, &alice_account()).unwrap().unwrap();
    assert_eq!(
        account.amount(),
        initial_balance.checked_sub(transaction_cost.deposit_cost).unwrap()
    );
    let access_key =
        get_access_key(&state, &alice_account(), &gas_key_signer.public_key()).unwrap().unwrap();
    let remaining = access_key.gas_key_info().unwrap().balance;
    assert_eq!(remaining, gas_key_balance.checked_sub(transaction_cost.gas_cost).unwrap());
}

#[test]
fn test_gas_refund_to_gas_key() {
    let initial_balance = Balance::from_near(1_000_000);
    let gas_key_balance = Balance::from_millinear(10);
    let GasKeyTestSetup {
        runtime,
        tries,
        root,
        mut apply_state,
        epoch_info_provider,
        gas_key_signer,
        shard_uid,
    } = setup_gas_key_test(
        alice_account(),
        vec![alice_account()],
        initial_balance,
        1,
        gas_key_balance,
    );

    // Create a gas refund receipt targeting alice's gas key
    let refund_amount = Balance::from_millinear(1);
    let gas_refund =
        Receipt::new_gas_refund(&alice_account(), refund_amount, gas_key_signer.public_key());

    // Apply the refund receipt
    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(shard_uid, root),
            &None,
            &apply_state,
            &[gas_refund],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    let root = commit_apply_result(&apply_result, &mut apply_state, &tries, shard_uid);
    let state = tries.new_trie_update(shard_uid, root);

    // Gas key balance should increase by refund amount
    let access_key =
        get_access_key(&state, &alice_account(), &gas_key_signer.public_key()).unwrap().unwrap();
    assert_eq!(
        access_key.gas_key_info().unwrap().balance,
        gas_key_balance.checked_add(refund_amount).unwrap()
    );

    // Account balance should NOT change
    let alice = get_account(&state, &alice_account()).unwrap().unwrap();
    assert_eq!(alice.amount(), initial_balance);
}

#[test]
fn test_gas_refund_unknown_key_falls_back_to_account() {
    let initial_balance = Balance::from_near(1_000_000);
    let (runtime, tries, root, mut apply_state, _signers, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        initial_balance,
        Balance::ZERO,
        Gas::from_teragas(1000),
    );
    apply_state.current_protocol_version = ProtocolFeature::GasKeys.protocol_version();

    // Create a gas refund receipt with a public key that doesn't exist on the account
    let unknown_key = PublicKey::from_seed(KeyType::ED25519, "unknown_key");
    let refund_amount = Balance::from_millinear(1);
    let gas_refund = Receipt::new_gas_refund(&alice_account(), refund_amount, unknown_key);

    let shard_uid = ShardUId::single_shard();
    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(shard_uid, root),
            &None,
            &apply_state,
            &[gas_refund],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    let root = commit_apply_result(&apply_result, &mut apply_state, &tries, shard_uid);
    let state = tries.new_trie_update(shard_uid, root);

    // Account balance should increase as fallback
    let alice = get_account(&state, &alice_account()).unwrap().unwrap();
    assert_eq!(alice.amount(), initial_balance.checked_add(refund_amount).unwrap());
}

#[test]
fn test_gas_key_tx_deposit_insufficient_charges_gas() {
    let num_nonces = 3;
    // Account balance is enough for storage staking but not enough for the transfer.
    let initial_balance = Balance::from_near(1);
    let gas_key_balance = Balance::from_millinear(1);
    let GasKeyTestSetup {
        runtime,
        tries,
        root,
        mut apply_state,
        epoch_info_provider,
        gas_key_signer,
        shard_uid,
    } = setup_gas_key_test(
        alice_account(),
        vec![alice_account(), bob_account()],
        initial_balance,
        num_nonces,
        gas_key_balance,
    );

    let initial_nonce = initial_nonce_value(GAS_KEY_BLOCK_HEIGHT);
    let nonce_index: NonceIndex = 0;

    // Transfer more than account can cover
    let gas_key_tx = SignedTransaction::from_actions_v1(
        TransactionNonce::from_nonce_and_index(initial_nonce + 1, nonce_index),
        alice_account(),
        bob_account(),
        &*gas_key_signer,
        vec![Action::Transfer(TransferAction { deposit: Balance::from_near(1000) })],
        CryptoHash::default(),
    );
    let transaction_cost =
        tx_cost(&apply_state.config, &gas_key_tx.transaction, apply_state.gas_price).unwrap();

    let signed_valid_period_txs = SignedValidPeriodTransactions::new(vec![gas_key_tx], vec![true]);
    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(shard_uid, root),
            &None,
            &apply_state,
            &[],
            signed_valid_period_txs,
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    // Should have one outcome: a failure with gas burnt
    assert_eq!(apply_result.outcomes.len(), 1);
    let outcome = &apply_result.outcomes[0];
    match &outcome.outcome.status {
        ExecutionStatus::Failure(TxExecutionError::InvalidTxError(
            InvalidTxError::NotEnoughBalanceForDeposit { reason, .. },
        )) => {
            assert_eq!(*reason, DepositCostFailureReason::NotEnoughBalance);
        }
        other => panic!("expected NotEnoughBalanceForDeposit, got {:?}", other),
    }
    assert_eq!(outcome.outcome.gas_burnt, transaction_cost.gas_burnt);
    assert_eq!(outcome.outcome.tokens_burnt, transaction_cost.burnt_amount);

    // Commit and verify state
    let root = commit_apply_result(&apply_result, &mut apply_state, &tries, shard_uid);
    let state = tries.new_trie_update(shard_uid, root);

    // Gas key balance was deducted
    let access_key =
        get_access_key(&state, &alice_account(), &gas_key_signer.public_key()).unwrap().unwrap();
    assert_eq!(
        access_key.gas_key_info().unwrap().balance,
        gas_key_balance.checked_sub(transaction_cost.burnt_amount).unwrap()
    );

    // Account balance was NOT deducted
    let account = get_account(&state, &alice_account()).unwrap().unwrap();
    assert_eq!(account.amount(), initial_balance);

    // Nonce was updated
    let new_nonce =
        get_gas_key_nonce(&state, &alice_account(), &gas_key_signer.public_key(), nonce_index)
            .unwrap()
            .unwrap();
    assert_eq!(new_nonce, initial_nonce + 1);
}

/// Test that calling a function that attaches 1 yoctoNEAR via
/// promise_batch_action_function_call_weight on a zero-balance contract
/// records the subsidized amount in BalanceStats and accumulates across
/// multiple receipts from different accounts in the same chunk.
#[test]
fn test_one_yocto_subsidy_tracked_in_stats() {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
    );

    let shard_uid = ShardUId::single_shard();

    // Step 1: deploy the test contract to both accounts.
    let deploy_alice = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::DeployContract(DeployContractAction {
            code: near_test_contracts::rs_contract().to_vec(),
        })],
    );
    let deploy_bob = create_receipt_with_actions(
        bob_account(),
        signers[1].clone(),
        vec![Action::DeployContract(DeployContractAction {
            code: near_test_contracts::rs_contract().to_vec(),
        })],
    );

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(shard_uid, root),
            &None,
            &apply_state,
            &[deploy_alice, deploy_bob],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    let root = commit_apply_result(&apply_result, &mut apply_state, &tries, shard_uid);

    // Step 2: set both accounts' balance to zero so the subsidy kicks in.
    let mut state_update = tries.new_trie_update(shard_uid, root);
    for account_id in &[alice_account(), bob_account()] {
        let mut account = get_account(&state_update, account_id).unwrap().unwrap();
        account.set_amount(Balance::ZERO);
        set_account(&mut state_update, account_id.clone(), &account);
    }
    state_update.commit(StateChangeCause::Migration);
    let trie_changes = state_update.finalize().unwrap().trie_changes;
    let mut store_update = tries.store_update();
    let root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
    store_update.commit();

    // Step 3: call max_self_recursion_delay on both accounts in the same chunk.
    // Each call attaches 1 yoctoNEAR via promise_batch_action_function_call_weight
    // on a zero-balance account, so the subsidized amount should accumulate to
    // 2 yoctoNEAR. Using separate accounts avoids the gas rebate from the first
    // receipt making the account non-zero for the second.
    let call_alice = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "max_self_recursion_delay".to_string(),
            args: 0u32.to_be_bytes().to_vec(),
            gas: Gas::from_teragas(100),
            deposit: Balance::ZERO,
        }))],
    );

    let call_bob = create_receipt_with_actions(
        bob_account(),
        signers[1].clone(),
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "max_self_recursion_delay".to_string(),
            args: 0u32.to_be_bytes().to_vec(),
            gas: Gas::from_teragas(100),
            deposit: Balance::ZERO,
        }))],
    );

    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(shard_uid, root),
            &None,
            &apply_state,
            &[call_alice, call_bob],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    // Both function calls should succeed.
    assert_matches!(&apply_result.outcomes[..], [first, second] => {
        assert_matches!(first.outcome.status, ExecutionStatus::SuccessReceiptId(_),
            "first call: expected success but got {:?}", first.outcome.status);
        assert_matches!(second.outcome.status, ExecutionStatus::SuccessReceiptId(_),
            "second call: expected success but got {:?}", second.outcome.status);
    });

    // The subsidy should accumulate across both receipts.
    assert_eq!(
        apply_result.stats.balance.subsidized_amount,
        Balance::from_yoctonear(2),
        "stats should track 2 yoctoNEAR subsidized across two zero-balance contract calls"
    );
}

// A FunctionCall whose receiver is deleted and recreated within the same chunk must
// resolve to the freshly recreated (no-code) account, not to a stale contract that
// `ReceiptPreparationPipeline` compiled against the receiver's code as resolved at
// preparation time.
#[test]
fn test_function_call_after_same_chunk_delete_recreate_resolves_fresh_code() {
    let parent = alice_account();
    let child: AccountId = "child.alice.near".parse().unwrap();
    // initial_locked must be 0 so the self-DeleteAccount receipt below passes the
    // DeleteAccountStaking check in `check_actor_permissions`.
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![parent.clone(), child.clone()],
        Balance::from_near(1_000_000),
        Balance::ZERO,
        Gas::from_teragas(1000),
    );
    let parent_signer = signers[0].clone();
    let child_signer = signers[1].clone();

    let deploy = create_receipt_with_actions(
        child.clone(),
        child_signer.clone(),
        vec![Action::DeployContract(DeployContractAction {
            code: near_test_contracts::trivial_contract().to_vec(),
        })],
    );
    let deploy_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            &apply_state,
            &[deploy],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    let root =
        commit_apply_result(&deploy_result, &mut apply_state, &tries, ShardUId::single_shard());
    apply_state.block_height += 1;

    let receipt_gas_price = GAS_PRICE.max(apply_state.config.min_gas_purchase_price);
    let build_receipt = |tag: &str, predecessor: AccountId, signer: &Signer, actions| -> Receipt {
        Receipt::V0(ReceiptV0 {
            predecessor_id: predecessor.clone(),
            receiver_id: child.clone(),
            receipt_id: CryptoHash::hash_borsh((tag, &child)),
            receipt: ReceiptEnum::Action(ActionReceipt {
                signer_id: predecessor,
                signer_public_key: signer.public_key(),
                gas_price: receipt_gas_price,
                output_data_receivers: vec![],
                input_data_ids: vec![],
                actions,
            }),
        })
    };
    let delete = build_receipt(
        "delete",
        child.clone(),
        &child_signer,
        vec![Action::DeleteAccount(DeleteAccountAction { beneficiary_id: parent.clone() })],
    );
    let create_and_call = build_receipt(
        "create_and_call",
        parent,
        &parent_signer,
        vec![
            Action::CreateAccount(CreateAccountAction {}),
            Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "main".to_string(),
                args: vec![],
                gas: Gas::from_teragas(10),
                deposit: Balance::ZERO,
            })),
        ],
    );
    let call_id = *create_and_call.receipt_id();

    let result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            &apply_state,
            &[delete, create_and_call],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    let call_outcome = result
        .outcomes
        .iter()
        .find(|outcome| outcome.id == call_id)
        .expect("function call outcome missing");
    assert_matches!(
        &call_outcome.outcome.status,
        ExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::FunctionCallError(FunctionCallError::CompilationError(
                CompilationError::CodeDoesNotExist { .. }
            )),
            ..
        }))
    );
}

/// The promise-input size limit used by these tests (4 MiB).
const PROMISE_INPUT_SIZE_LIMIT: u64 = 4 * 1024 * 1024;

/// Protocol version at which the promise-input size limit activates.
fn promise_input_limit_version() -> ProtocolVersion {
    ProtocolFeature::ReceiptPromiseInputSizeLimit.protocol_version()
}

/// Reconfigures `apply_state` for the promise-input size-limit tests: a free
/// runtime config (no gas costs, to keep the tests focused on the size check)
/// with `max_receipt_total_input_size` set to `PROMISE_INPUT_SIZE_LIMIT`,
/// applied at the given protocol version.
fn setup_promise_input_limit(apply_state: &mut ApplyState, protocol_version: ProtocolVersion) {
    let mut config = RuntimeConfig::free();
    let wasm_config = Arc::make_mut(&mut config.wasm_config);
    wasm_config.limit_config.max_receipt_total_input_size = PROMISE_INPUT_SIZE_LIMIT;
    apply_state.config = Arc::new(config);
    apply_state.current_protocol_version = protocol_version;
}

/// A data receipt delivering `size` bytes to `receiver` under `data_id`.
fn promise_data_receipt(receiver: AccountId, data_id: CryptoHash, size: usize) -> Receipt {
    Receipt::V0(ReceiptV0 {
        predecessor_id: bob_account(),
        receiver_id: receiver,
        receipt_id: data_id,
        receipt: ReceiptEnum::Data(DataReceipt { data_id, data: Some(vec![0u8; size]) }),
    })
}

/// An action receipt (a single zero-value transfer) awaiting `data_ids`.
fn action_receipt_awaiting(
    receiver: AccountId,
    receipt_id: CryptoHash,
    data_ids: Vec<CryptoHash>,
) -> Receipt {
    Receipt::V0(ReceiptV0 {
        predecessor_id: bob_account(),
        receiver_id: receiver,
        receipt_id,
        receipt: ReceiptEnum::Action(ActionReceipt {
            signer_id: bob_account(),
            signer_public_key: PublicKey::empty(KeyType::ED25519),
            gas_price: GAS_PRICE,
            output_data_receivers: vec![],
            input_data_ids: data_ids,
            actions: vec![Action::Transfer(TransferAction { deposit: Balance::ZERO })],
        }),
    })
}

/// A receipt with oversized combined promise inputs fails with
/// `TotalPromiseInputSizeExceeded`, and its received data and postponed
/// bookkeeping are cleaned up from the state.
#[test]
fn test_promise_input_size_limit_exceeded_fails_and_cleans_up() {
    let (runtime, tries, root, mut apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
    );
    setup_promise_input_limit(&mut apply_state, promise_input_limit_version());

    let data_id_1 = hash(b"promise-input-exceeded-1");
    let data_id_2 = hash(b"promise-input-exceeded-2");
    let receipt_id = hash(b"promise-input-exceeded-receipt");
    // Two data receipts, each below `max_length_returned_data` (4 MiB) so they
    // pass receipt validation, but whose combined size exceeds the limit.
    let half = (PROMISE_INPUT_SIZE_LIMIT / 2 + 1) as usize;
    let receipts = vec![
        promise_data_receipt(alice_account(), data_id_1, half),
        promise_data_receipt(alice_account(), data_id_2, half),
        action_receipt_awaiting(alice_account(), receipt_id, vec![data_id_1, data_id_2]),
    ];

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

    let outcome = apply_result
        .outcomes
        .iter()
        .find(|o| o.id == receipt_id)
        .expect("awaiting receipt should have an execution outcome");
    match &outcome.outcome.status {
        ExecutionStatus::Failure(TxExecutionError::ActionError(action_error)) => {
            assert_matches!(
                action_error.kind,
                ActionErrorKind::TotalPromiseInputSizeExceeded { .. }
            );
        }
        other => panic!("expected TotalPromiseInputSizeExceeded failure, got {other:?}"),
    }

    // The received data and postponed bookkeeping must be cleaned up.
    let mut store_update = tries.store_update();
    let new_root =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit();
    let state = tries.new_trie_update(ShardUId::single_shard(), new_root);
    assert_eq!(get_received_data(&state, &alice_account(), data_id_1).unwrap(), None);
    assert_eq!(get_received_data(&state, &alice_account(), data_id_2).unwrap(), None);
    assert_eq!(get_postponed_receipt(&state, &alice_account(), receipt_id).unwrap(), None);
}

/// Failing one receipt for exceeding the promise-input size limit must not
/// affect other receipts processed in the same chunk, and `apply` must succeed
/// (a per-receipt failure, not a chunk-level error).
#[test]
fn test_promise_input_size_limit_does_not_affect_other_receipts() {
    let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
    );
    setup_promise_input_limit(&mut apply_state, promise_input_limit_version());

    let data_id_1 = hash(b"promise-input-isolation-1");
    let data_id_2 = hash(b"promise-input-isolation-2");
    let failing_id = hash(b"promise-input-isolation-failing");
    let half = (PROMISE_INPUT_SIZE_LIMIT / 2 + 1) as usize;

    // A succeeding receipt with no input dependencies, placed both before and
    // after the failing one to guard against order-dependent early returns.
    let before = create_receipt_with_actions(
        alice_account(),
        signers[0].clone(),
        vec![Action::Transfer(TransferAction { deposit: Balance::ZERO })],
    );
    let after = create_receipt_with_actions(
        bob_account(),
        signers[1].clone(),
        vec![Action::Transfer(TransferAction { deposit: Balance::ZERO })],
    );
    let before_id = *before.receipt_id();
    let after_id = *after.receipt_id();

    let receipts = vec![
        before,
        promise_data_receipt(alice_account(), data_id_1, half),
        promise_data_receipt(alice_account(), data_id_2, half),
        action_receipt_awaiting(alice_account(), failing_id, vec![data_id_1, data_id_2]),
        after,
    ];

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

    let status_of = |id: CryptoHash| {
        apply_result
            .outcomes
            .iter()
            .find(|o| o.id == id)
            .map(|o| o.outcome.status.clone())
            .unwrap_or_else(|| panic!("missing outcome for {id}"))
    };
    assert_matches!(status_of(before_id), ExecutionStatus::SuccessValue(_));
    assert_matches!(status_of(after_id), ExecutionStatus::SuccessValue(_));
    assert_matches!(
        status_of(failing_id),
        ExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::TotalPromiseInputSizeExceeded { .. },
            ..
        }))
    );
}

/// A receipt whose combined promise inputs are below the limit executes
/// normally.
#[test]
fn test_promise_input_size_limit_under_limit_succeeds() {
    let (runtime, tries, root, mut apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
    );
    setup_promise_input_limit(&mut apply_state, promise_input_limit_version());

    let data_id_1 = hash(b"promise-input-under-1");
    let data_id_2 = hash(b"promise-input-under-2");
    let receipt_id = hash(b"promise-input-under-receipt");
    // Combined ~2 MiB, comfortably below the 4 MiB limit.
    let quarter = (PROMISE_INPUT_SIZE_LIMIT / 4) as usize;
    let receipts = vec![
        promise_data_receipt(alice_account(), data_id_1, quarter),
        promise_data_receipt(alice_account(), data_id_2, quarter),
        action_receipt_awaiting(alice_account(), receipt_id, vec![data_id_1, data_id_2]),
    ];

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

    let outcome = apply_result
        .outcomes
        .iter()
        .find(|o| o.id == receipt_id)
        .expect("awaiting receipt should have an execution outcome");
    assert_matches!(outcome.outcome.status, ExecutionStatus::SuccessValue(_));
}

/// Before the feature's protocol version the limit is not enforced: the same
/// oversized inputs execute successfully.
#[test]
fn test_promise_input_size_limit_disabled_before_protocol_version() {
    let (runtime, tries, root, mut apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
    );
    setup_promise_input_limit(&mut apply_state, promise_input_limit_version() - 1);

    let data_id_1 = hash(b"promise-input-gated-1");
    let data_id_2 = hash(b"promise-input-gated-2");
    let receipt_id = hash(b"promise-input-gated-receipt");
    let half = (PROMISE_INPUT_SIZE_LIMIT / 2 + 1) as usize;
    let receipts = vec![
        promise_data_receipt(alice_account(), data_id_1, half),
        promise_data_receipt(alice_account(), data_id_2, half),
        action_receipt_awaiting(alice_account(), receipt_id, vec![data_id_1, data_id_2]),
    ];

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

    let outcome = apply_result
        .outcomes
        .iter()
        .find(|o| o.id == receipt_id)
        .expect("awaiting receipt should have an execution outcome");
    assert_matches!(outcome.outcome.status, ExecutionStatus::SuccessValue(_));
}

/// When the received data is already committed to the trie (delivered in an
/// earlier chunk), the failing receipt reads only the sizes and not the values,
/// so the rejected receipt does not pull the (multi-MiB) inputs into the
/// storage proof / state witness.
#[test]
fn test_promise_input_size_limit_does_not_bloat_witness() {
    let (runtime, tries, root, mut apply_state, _, epoch_info_provider) = setup_runtime(
        vec![alice_account(), bob_account()],
        Balance::from_near(1_000_000),
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
    );
    setup_promise_input_limit(&mut apply_state, promise_input_limit_version());

    let data_id_1 = hash(b"promise-input-witness-1");
    let data_id_2 = hash(b"promise-input-witness-2");
    let receipt_id = hash(b"promise-input-witness-receipt");
    let half = (PROMISE_INPUT_SIZE_LIMIT / 2 + 1) as usize;

    // Chunk 1: deliver only the data receipts so the received data is committed
    // to the trie (no awaiting receipt yet, so nothing executes).
    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root),
            &None,
            &apply_state,
            &[
                promise_data_receipt(alice_account(), data_id_1, half),
                promise_data_receipt(alice_account(), data_id_2, half),
            ],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();
    let mut store_update = tries.store_update();
    let root =
        tries.apply_all(&apply_result.trie_changes, ShardUId::single_shard(), &mut store_update);
    store_update.commit();

    // Chunk 2: the awaiting receipt arrives and fails the size check. It reads
    // the input sizes from the trie (recording), but must not deref the values.
    let apply_result = runtime
        .apply(
            tries.get_trie_for_shard(ShardUId::single_shard(), root).recording_reads_new_recorder(),
            &None,
            &apply_state,
            &[action_receipt_awaiting(alice_account(), receipt_id, vec![data_id_1, data_id_2])],
            SignedValidPeriodTransactions::empty(),
            &epoch_info_provider,
            Default::default(),
        )
        .unwrap();

    let outcome = apply_result
        .outcomes
        .iter()
        .find(|o| o.id == receipt_id)
        .expect("awaiting receipt should have an execution outcome");
    assert_matches!(
        outcome.outcome.status,
        ExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::TotalPromiseInputSizeExceeded { .. },
            ..
        }))
    );

    // The storage proof must not contain the (~4 MiB) input values. Assert the
    // recorded proof is far smaller than a single input, proving the values
    // were never dereferenced.
    let partial_storage = apply_result.proof.unwrap();
    let PartialState::TrieValues(storage_proof) = partial_storage.nodes;
    let total_size: usize = storage_proof.iter().map(|v| v.len()).sum();
    assert!(
        (total_size as u64) < PROMISE_INPUT_SIZE_LIMIT / 2,
        "storage proof of {total_size} bytes unexpectedly large; inputs were likely dereferenced"
    );
}

/// A contract that creates an ML-DSA-65 gas key must not leak total supply.
///
/// Before this fix, the pre-execution / refund path priced the key on
/// `trie_id_len()` (33 bytes) while the host path reserved the exec fee on
/// `len()` (1953); the extra reserved gas was neither burnt nor refunded, so
/// total supply silently dropped. This guards against that regression: supply
/// is conserved now that the host exec fee also uses `trie_id_len()`.
#[test]
fn test_gas_key_add_key_conserves_supply() {
    if !ProtocolFeature::FixMlDsaCostCharging.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: FixMlDsaCostCharging not enabled at PROTOCOL_VERSION");
        return;
    }
    let initial_balance = Balance::from_near(1_000_000);
    let (runtime, tries, mut root, mut apply_state, signers, epoch_info_provider) = setup_runtime(
        vec![alice_account()],
        initial_balance,
        Balance::from_near(500_000),
        Gas::from_teragas(1000),
    );
    let shard_uid = ShardUId::single_shard();
    let gas_key: PublicKey = SecretKey::from_seed(KeyType::MLDSA65, "gas-key-seed").public_key();

    let alice_amount = |root: CryptoHash| {
        get_account(&tries.new_trie_update(shard_uid, root), &alice_account())
            .unwrap()
            .unwrap()
            .amount()
    };
    let before = alice_amount(root);

    // Single signed tx (deploy + call), so alice is fully debited up front and the
    // scenario is closed: total supply == alice's amount plus everything burnt.
    // The contract adds an ML-DSA-65 gas key to alice via a self-promise batch.
    use near_primitives::serialize::to_base64;
    let call_promise_args = serde_json::json!([
        {"batch_create": {"account_id": alice_account()}, "id": 0},
        {"action_add_gas_key_with_full_access": {
            "promise_index": 0,
            "public_key": to_base64(&borsh::to_vec(&gas_key).unwrap()),
            "num_nonces": 3,
        }, "id": 0},
    ]);
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
                method_name: "call_promise".to_string(),
                args: serde_json::to_vec(&call_promise_args).unwrap(),
                gas: MAX_ATTACHED_GAS,
                deposit: Balance::ZERO,
            })),
        ],
        CryptoHash::default(),
    );

    let mut incoming: Vec<Receipt> = vec![];
    let mut destroyed = Balance::ZERO;
    let mut settled = false;
    for round in 0..12 {
        let apply_result = runtime
            .apply(
                tries.get_trie_for_shard(shard_uid, root),
                &None,
                &apply_state,
                &incoming,
                if round == 0 {
                    SignedValidPeriodTransactions::new(vec![tx.clone()], vec![true])
                } else {
                    SignedValidPeriodTransactions::empty()
                },
                &epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        // Value that left circulation this round. Receiver/validator rewards stay
        // in accounts (not counted here); subsidies and gas deficit are minted.
        let b = &apply_result.stats.balance;
        destroyed = destroyed
            .checked_add(b.tx_burnt_amount)
            .unwrap()
            .checked_add(b.slashed_burnt_amount)
            .unwrap()
            .checked_add(b.other_burnt_amount)
            .unwrap()
            .checked_sub(b.subsidized_amount)
            .unwrap()
            .checked_sub(b.gas_deficit_amount)
            .unwrap();
        root = commit_apply_result(&apply_result, &mut apply_state, &tries, shard_uid);
        incoming = apply_result.outgoing_receipts.clone();
        apply_state.block_height += 1;
        if round > 0 && incoming.is_empty() && apply_result.delayed_receipts_count == 0 {
            settled = true;
            break;
        }
    }
    // The supply accounting below is only meaningful once the whole receipt
    // cascade has drained; a run that hit the round cap would measure a partial
    // state and could mask (or fake) a leak.
    assert!(settled, "receipt cascade did not settle within the round budget");

    let supply_drop = before.checked_sub(alice_amount(root)).unwrap();
    assert_eq!(
        supply_drop.as_yoctonear(),
        destroyed.as_yoctonear(),
        "supply leak: alice lost {} yocto but only {} was recorded as destroyed",
        supply_drop.as_yoctonear(),
        destroyed.as_yoctonear(),
    );
}

/// The gas-key SEND fee prices bytes put on the wire, so it must scale with
/// `public_key.len()` (1953 for ML-DSA-65), not the on-trie `trie_id_len()`
/// (33). A "use `trie_id_len` everywhere" fix would leave this wrong; only the
/// split (send = `len`, exec = `trie_id_len`) is correct, which is what
/// `config.rs` now does.
#[test]
fn test_gas_key_transfer_send_fee_uses_wire_length() {
    if !ProtocolFeature::FixMlDsaCostCharging.enabled(PROTOCOL_VERSION) {
        tracing::info!("skipping: FixMlDsaCostCharging not enabled at PROTOCOL_VERSION");
        return;
    }
    let config = RuntimeConfig::test();
    let receiver = alice_account();
    let ed25519_key = SecretKey::from_seed(KeyType::ED25519, "gas-key-seed").public_key();
    let ml_dsa_65_key = SecretKey::from_seed(KeyType::MLDSA65, "gas-key-seed").public_key();

    let send_fee = |public_key: &PublicKey| -> u64 {
        let actions = [Action::TransferToGasKey(Box::new(TransferToGasKeyAction {
            public_key: public_key.clone(),
            deposit: Balance::ZERO,
        }))];
        total_send_fees(&config, false, &actions, &receiver).unwrap().gas.as_gas()
    };
    let wire_len = |public_key: &PublicKey| public_key.len() as u64;

    let gas_key_byte_send = config.fees.fee(ActionCosts::gas_key_byte).send_fee(false).gas.as_gas();
    // Send fee scales with wire length, so the ML-DSA-65 vs ed25519 gap is the
    // difference in their `len()`; the (equal) base fee cancels.
    let expected_delta = gas_key_byte_send * (wire_len(&ml_dsa_65_key) - wire_len(&ed25519_key));
    let measured_delta = send_fee(&ml_dsa_65_key) - send_fee(&ed25519_key);
    assert_eq!(
        measured_delta, expected_delta,
        "gas-key send fee must scale with wire length (len), not trie_id_len",
    );
}

/// Self-signed `UniversalStateInit`: the one transaction an account can send
/// before it holds any access key.
///
/// These run at the `Runtime::apply` level rather than through test-loop so a
/// chunk's exact transaction list can be handed over, which matters for the
/// same-chunk and replay cases.
mod self_signed_state_init {
    use super::*;
    use near_crypto::PublicKeyHandle;
    use near_primitives::account::Account;
    use near_primitives::action::{
        AddKeyAction, GlobalContractIdentifier, StakeAction, UniversalStateInitAction,
        UseGlobalContractAction,
    };
    use near_primitives::types::Nonce;
    use near_primitives::universal_state_init::{UniversalStateInit, UniversalStateInitV1};
    use near_primitives::utils::derive_universal_account_id;
    use std::collections::{BTreeMap, BTreeSet};

    /// Height at which the funding receipt is taken to have created the account.
    /// Strictly below `apply_state.block_height`, as it always is in practice:
    /// transactions are converted against the previous chunk's post-state, so an
    /// account cannot be seen in the chunk that created it.
    const CREATION_HEIGHT: BlockHeight = 1;

    fn signer_for(seed: &str) -> Arc<Signer> {
        Arc::new(InMemorySigner::from_secret_key(
            "unused.near".parse().unwrap(),
            SecretKey::from_seed(KeyType::ED25519, seed),
        ))
    }

    fn state_init_for(keys: &[PublicKey]) -> UniversalStateInit {
        UniversalStateInit::V1(UniversalStateInitV1 {
            code: None,
            data: BTreeMap::new(),
            access_keys: keys.iter().cloned().map(PublicKeyHandle::from).collect::<BTreeSet<_>>(),
        })
    }

    /// A runtime whose only account is a funded, uninitialized `0u` account,
    /// exactly as a transfer to an uninitialized `0u` id would leave it.
    fn setup(
        account_id: &AccountId,
        balance: Balance,
    ) -> (Runtime, ShardTries, CryptoHash, ApplyState, impl EpochInfoProvider + use<>) {
        // `use<>`: the provider borrows nothing, but edition 2024 would otherwise
        // capture `account_id`'s lifetime and keep it borrowed for the whole test.
        let epoch_info_provider = MockEpochInfoProvider::default();
        let shard_layout = epoch_info_provider.shard_layout(&EpochId::default()).unwrap();
        let shard_uid = shard_layout.shard_uids().next().unwrap();

        let tries = TestTriesBuilder::new().build();
        let runtime = Runtime::new();
        let mut initial_state = tries.new_trie_update(shard_uid, MerkleHash::default());
        let config = RuntimeConfig::test();
        set_account(
            &mut initial_state,
            account_id.clone(),
            &Account::new_uninitialized(
                balance,
                config.fees.storage_usage_config.num_bytes_account,
                initial_nonce_value(CREATION_HEIGHT),
            ),
        );
        initial_state.commit(StateChangeCause::InitialState);
        let trie_changes = initial_state.finalize().unwrap().trie_changes;
        let mut store_update = tries.store_update();
        let root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        store_update.commit();

        let shards_congestion_info = shard_layout
            .shard_ids()
            .map(|shard_id| (shard_id, ExtendedCongestionInfo::default()))
            .collect();
        let apply_state = ApplyState {
            apply_reason: ApplyChunkReason::UpdateTrackedShard,
            block_height: CREATION_HEIGHT + 1,
            prev_block_hash: Default::default(),
            shard_id: shard_uid.shard_id(),
            epoch_id: Default::default(),
            epoch_height: 0,
            gas_price: GAS_PRICE,
            block_timestamp: 100,
            gas_limit: Some(Gas::from_teragas(1000)),
            random_seed: Default::default(),
            current_protocol_version: PROTOCOL_VERSION,
            config: Arc::new(config),
            next_wasm_config: None,
            cache: Some(Box::new(FilesystemContractRuntimeCache::test().unwrap())),
            is_new_chunk: true,
            save_receipt_to_tx: false,
            congestion_info: BlockCongestionInfo::new(shards_congestion_info),
            bandwidth_requests: BlockBandwidthRequests::empty(),
            trie_access_tracker_state: Default::default(),
            on_post_state_ready: None,
        };
        (runtime, tries, root, apply_state, epoch_info_provider)
    }

    /// The only nonce a bootstrap may carry, since the check is forced Strict.
    fn expected_nonce() -> Nonce {
        initial_nonce_value(CREATION_HEIGHT) + 1
    }

    fn bootstrap_tx(
        signer: &Signer,
        state_init: &UniversalStateInit,
        account_id: &AccountId,
        nonce: Nonce,
        extra_actions: Vec<Action>,
    ) -> SignedTransaction {
        let mut actions = vec![Action::UniversalStateInit(Box::new(UniversalStateInitAction {
            state_init: state_init.to_raw(),
            deposit: Balance::ZERO,
        }))];
        actions.extend(extra_actions);
        SignedTransaction::from_actions(
            nonce,
            account_id.clone(),
            account_id.clone(),
            signer,
            actions,
            CryptoHash::default(),
        )
    }

    fn apply_txs(
        runtime: &Runtime,
        tries: &ShardTries,
        root: CryptoHash,
        apply_state: &ApplyState,
        epoch_info_provider: &impl EpochInfoProvider,
        txs: Vec<SignedTransaction>,
    ) -> (CryptoHash, Vec<ExecutionOutcomeWithId>) {
        let n = txs.len();
        let result = runtime
            .apply(
                tries.get_trie_for_shard(ShardUId::single_shard(), root),
                &None,
                apply_state,
                &[],
                SignedValidPeriodTransactions::new(txs, vec![true; n]),
                epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        let mut store_update = tries.store_update();
        let new_root =
            tries.apply_all(&result.trie_changes, ShardUId::single_shard(), &mut store_update);
        store_update.commit();
        (new_root, result.outcomes)
    }

    fn skip() -> bool {
        if !ProtocolFeature::UniversalAccounts.enabled(PROTOCOL_VERSION) {
            tracing::info!("skipping: UniversalAccounts not enabled at v{PROTOCOL_VERSION}");
            return true;
        }
        false
    }

    /// The happy path: an account with no access key signs for itself, the state
    /// init installs its keys, and the account's pre-key nonce is consumed.
    #[test]
    fn self_signed_init_succeeds_and_consumes_nonce() {
        init_test_logger();
        if skip() {
            return;
        }
        let signer = signer_for("bootstrap-ok");
        let state_init = state_init_for(&[signer.public_key()]);
        let account_id = derive_universal_account_id(&state_init.to_raw());
        let (runtime, tries, root, apply_state, epoch) = setup(&account_id, Balance::from_near(10));

        let tx = bootstrap_tx(&signer, &state_init, &account_id, expected_nonce(), vec![]);
        let (root, outcomes) = apply_txs(&runtime, &tries, root, &apply_state, &epoch, vec![tx]);

        assert_matches!(outcomes[0].outcome.status, ExecutionStatus::SuccessReceiptId(_));
        let state = tries.new_trie_update(ShardUId::single_shard(), root);
        let account = get_account(&state, &account_id).unwrap().unwrap();
        assert!(account.is_initialized(), "the state init must have run");
        // The installed key carries its own nonce from here on, so the account's
        // pre-key nonce is gone.
        assert_eq!(account.bootstrap_nonce(), None);
        assert!(
            get_access_key(&state, &account_id, &signer.public_key()).unwrap().is_some(),
            "the committed key must be installed"
        );
    }

    /// The replay this whole design exists to stop. Re-submitting the identical
    /// signed bytes after a successful init must fail: the account is
    /// initialized, so the ordinary path applies, and the installed key's nonce
    /// already exceeds the one the transaction carries.
    #[test]
    fn same_bootstrap_cannot_be_replayed() {
        init_test_logger();
        if skip() {
            return;
        }
        let signer = signer_for("bootstrap-replay");
        let state_init = state_init_for(&[signer.public_key()]);
        let account_id = derive_universal_account_id(&state_init.to_raw());
        let (runtime, tries, root, mut apply_state, epoch) =
            setup(&account_id, Balance::from_near(10));

        let tx = bootstrap_tx(&signer, &state_init, &account_id, expected_nonce(), vec![]);
        let (root, outcomes) =
            apply_txs(&runtime, &tries, root, &apply_state, &epoch, vec![tx.clone()]);
        assert_matches!(outcomes[0].outcome.status, ExecutionStatus::SuccessReceiptId(_));

        apply_state.block_height += 1;
        let (_, outcomes) = apply_txs(&runtime, &tries, root, &apply_state, &epoch, vec![tx]);
        assert_matches!(
            outcomes[0].outcome.status,
            ExecutionStatus::Failure(TxExecutionError::InvalidTxError(
                InvalidTxError::InvalidNonce { .. }
            ))
        );
    }

    /// Forced Strict means exactly one nonce is admissible. A Monotonic-looking
    /// nonce that is merely "greater than the floor" is what would leave a
    /// replay window once the installed keys take over, so it is rejected.
    #[test]
    fn only_one_nonce_is_admissible() {
        init_test_logger();
        if skip() {
            return;
        }
        let signer = signer_for("bootstrap-strict");
        let state_init = state_init_for(&[signer.public_key()]);
        let account_id = derive_universal_account_id(&state_init.to_raw());

        for nonce in [expected_nonce() - 1, expected_nonce() + 1, expected_nonce() + 999] {
            let (runtime, tries, root, apply_state, epoch) =
                setup(&account_id, Balance::from_near(10));
            let tx = bootstrap_tx(&signer, &state_init, &account_id, nonce, vec![]);
            let (_, outcomes) = apply_txs(&runtime, &tries, root, &apply_state, &epoch, vec![tx]);
            assert_matches!(
                outcomes[0].outcome.status,
                ExecutionStatus::Failure(TxExecutionError::InvalidTxError(
                    InvalidTxError::InvalidNonce { .. }
                )),
                "nonce {nonce} should not be admissible",
            );
        }
    }

    /// Two bootstraps carrying the same nonce in one chunk. The first consumes
    /// it through the cached account, so the second must see the advanced value
    /// rather than a freshly read one and be rejected.
    #[test]
    fn two_bootstraps_with_one_nonce_in_chunk() {
        init_test_logger();
        if skip() {
            return;
        }
        let signer = signer_for("bootstrap-same-chunk");
        let state_init = state_init_for(&[signer.public_key()]);
        let account_id = derive_universal_account_id(&state_init.to_raw());
        let (runtime, tries, root, apply_state, epoch) = setup(&account_id, Balance::from_near(10));

        // Same nonce, different action lists, so they are distinct transactions
        // and in-chunk hash dedup does not hide the nonce check.
        let first = bootstrap_tx(&signer, &state_init, &account_id, expected_nonce(), vec![]);
        let second = bootstrap_tx(
            &signer,
            &state_init,
            &account_id,
            expected_nonce(),
            vec![Action::Transfer(TransferAction { deposit: Balance::from_yoctonear(1) })],
        );
        let (_, outcomes) =
            apply_txs(&runtime, &tries, root, &apply_state, &epoch, vec![first, second]);

        assert_matches!(outcomes[0].outcome.status, ExecutionStatus::SuccessReceiptId(_));
        assert_matches!(
            outcomes[1].outcome.status,
            ExecutionStatus::Failure(TxExecutionError::InvalidTxError(
                InvalidTxError::InvalidNonce { .. }
            )),
            "the second must see the nonce the first consumed"
        );
    }

    /// A bootstrap-shaped transaction carrying a gas-key nonce index. Both other
    /// verifiers assert on that discriminator, so if the branch order is ever
    /// got wrong this is a panic inside `process_transactions`, i.e. a failed
    /// chunk rather than a failed transaction.
    #[test]
    fn gas_key_nonce_index_is_not_bootstrap() {
        init_test_logger();
        if skip() {
            return;
        }
        let signer = signer_for("bootstrap-nonce-index");
        let state_init = state_init_for(&[signer.public_key()]);
        let account_id = derive_universal_account_id(&state_init.to_raw());
        let (runtime, tries, root, apply_state, epoch) = setup(&account_id, Balance::from_near(10));

        let tx = SignedTransaction::from_actions_v1(
            TransactionNonce::from_nonce_and_index(expected_nonce(), 0),
            account_id.clone(),
            account_id,
            &signer,
            vec![Action::UniversalStateInit(Box::new(UniversalStateInitAction {
                state_init: state_init.to_raw(),
                deposit: Balance::ZERO,
            }))],
            CryptoHash::default(),
        );
        let (_, outcomes) = apply_txs(&runtime, &tries, root, &apply_state, &epoch, vec![tx]);

        // Not a bootstrap, so it falls through to the ordinary path and is
        // rejected for the key it does not have.
        assert_matches!(
            outcomes[0].outcome.status,
            ExecutionStatus::Failure(TxExecutionError::InvalidTxError(
                InvalidTxError::InvalidAccessKeyError(_)
            ))
        );
    }

    /// A key the account id does not commit to cannot bootstrap it, even though
    /// the transaction is otherwise shaped like one. Without this the state init
    /// bytes are public, so anyone could spend the account's balance.
    #[test]
    fn uncommitted_key_cannot_bootstrap() {
        init_test_logger();
        if skip() {
            return;
        }
        let committed = signer_for("bootstrap-committed");
        let outsider = signer_for("bootstrap-outsider");
        let state_init = state_init_for(&[committed.public_key()]);
        let account_id = derive_universal_account_id(&state_init.to_raw());
        let (runtime, tries, root, apply_state, epoch) = setup(&account_id, Balance::from_near(10));

        let tx = bootstrap_tx(&outsider, &state_init, &account_id, expected_nonce(), vec![]);
        let (new_root, outcomes) =
            apply_txs(&runtime, &tries, root, &apply_state, &epoch, vec![tx]);

        assert_matches!(
            outcomes[0].outcome.status,
            ExecutionStatus::Failure(TxExecutionError::InvalidTxError(
                InvalidTxError::InvalidAccessKeyError(_)
            ))
        );
        let state = tries.new_trie_update(ShardUId::single_shard(), new_root);
        let account = get_account(&state, &account_id).unwrap().unwrap();
        assert!(!account.is_initialized(), "a rejected bootstrap must not install state");
        assert_eq!(account.bootstrap_nonce(), Some(initial_nonce_value(CREATION_HEIGHT)));
    }

    /// A state init whose storage the account cannot stake for. The action fails
    /// and its state is rolled back, but the conversion was already charged and
    /// committed, so the nonce must be consumed all the same. Otherwise the same
    /// bytes could be resubmitted until the balance was burnt away.
    #[test]
    fn failed_init_still_consumes_nonce() {
        init_test_logger();
        if skip() {
            return;
        }
        let signer = signer_for("bootstrap-too-big");
        // Well past the 770-byte zero-balance exemption, so a real stake is
        // required, and more of it than the account holds.
        let state_init = UniversalStateInit::V1(UniversalStateInitV1 {
            code: None,
            data: BTreeMap::from([(b"k".to_vec(), vec![0u8; 100_000])]),
            access_keys: BTreeSet::from([PublicKeyHandle::from(signer.public_key())]),
        });
        let account_id = derive_universal_account_id(&state_init.to_raw());
        let balance = Balance::from_millinear(500);
        let (runtime, tries, root, mut apply_state, epoch) = setup(&account_id, balance);

        let tx = bootstrap_tx(&signer, &state_init, &account_id, expected_nonce(), vec![]);
        let (root, outcomes) =
            apply_txs(&runtime, &tries, root, &apply_state, &epoch, vec![tx.clone()]);
        // The transaction converted, so it reports a receipt id; the receipt is
        // what fails, and it has to fail on the storage stake rather than on
        // anything else, or the test would pass for the wrong reason.
        assert_matches!(outcomes[0].outcome.status, ExecutionStatus::SuccessReceiptId(_));
        let receipt_outcome = outcomes
            .iter()
            .find(|outcome| matches!(outcome.outcome.status, ExecutionStatus::Failure(_)))
            .expect("the state init receipt must have failed");
        assert_matches!(
            &receipt_outcome.outcome.status,
            ExecutionStatus::Failure(TxExecutionError::ActionError(err))
                if matches!(err.kind, ActionErrorKind::LackBalanceForState { .. }),
            "the init must fail on storage staking, not something else"
        );

        let state = tries.new_trie_update(ShardUId::single_shard(), root);
        let account = get_account(&state, &account_id).unwrap().unwrap();
        assert!(!account.is_initialized(), "the oversized state init must not have installed");
        assert_eq!(
            account.bootstrap_nonce(),
            Some(expected_nonce()),
            "a failed init must still consume the nonce"
        );
        assert!(account.amount() < balance, "conversion fees are charged regardless");

        // And the identical bytes are now dead.
        apply_state.block_height += 1;
        let (_, outcomes) = apply_txs(&runtime, &tries, root, &apply_state, &epoch, vec![tx]);
        assert_matches!(
            outcomes[0].outcome.status,
            ExecutionStatus::Failure(TxExecutionError::InvalidTxError(
                InvalidTxError::InvalidNonce { .. }
            ))
        );
    }

    /// A self-signed state init against an account that is *already* initialized,
    /// signed with a key added later that the account id does not commit to.
    ///
    /// This is legal today and must stay legal: it is how a deposit top-up is
    /// sent, relying on the state init being idempotent. It is only reachable
    /// because the key-membership condition classifies rather than rejects, so
    /// the transaction falls through to the ordinary access-key path.
    #[test]
    fn added_key_can_still_send_idempotent_init() {
        init_test_logger();
        if skip() {
            return;
        }
        let committed = signer_for("idempotent-committed");
        let added = signer_for("idempotent-added");
        let state_init = state_init_for(&[committed.public_key()]);
        let account_id = derive_universal_account_id(&state_init.to_raw());
        let (runtime, tries, root, apply_state, epoch) = setup(&account_id, Balance::from_near(10));

        // Initialize the account, then give it a key outside the state init.
        let mut state = tries.new_trie_update(ShardUId::single_shard(), root);
        let mut account = get_account(&state, &account_id).unwrap().unwrap();
        account.initialize().unwrap();
        set_account(&mut state, account_id.clone(), &account);
        let mut access_key = AccessKey::full_access();
        access_key.nonce = initial_nonce_value(CREATION_HEIGHT);
        set_access_key(&mut state, account_id.clone(), added.public_key(), &access_key);
        state.commit(StateChangeCause::InitialState);
        let trie_changes = state.finalize().unwrap().trie_changes;
        let mut store_update = tries.store_update();
        let root = tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
        store_update.commit();

        let tx = bootstrap_tx(&added, &state_init, &account_id, expected_nonce(), vec![]);
        let (_, outcomes) = apply_txs(&runtime, &tries, root, &apply_state, &epoch, vec![tx]);

        assert_matches!(
            outcomes[0].outcome.status,
            ExecutionStatus::SuccessReceiptId(_),
            "an added key must still be able to send an idempotent state init"
        );
    }

    /// The funding receipt seeds the account's nonce from the height it runs at,
    /// which is what `recreated_account_rejects_old_bootstrap` rests on: a
    /// seed that ignored the height would let the old bootstrap through the second
    /// time. Every other test here writes the account straight into the trie, so
    /// this is the only one that covers the seeding itself.
    #[test]
    fn funding_transfer_seeds_nonce_from_its_height() {
        init_test_logger();
        if skip() {
            return;
        }
        let signer = signer_for("bootstrap-seeded");
        let state_init = state_init_for(&[signer.public_key()]);
        let account_id = derive_universal_account_id(&state_init.to_raw());

        let seeded_at = |height: BlockHeight| -> Nonce {
            let (runtime, tries, root, mut apply_state, signers, epoch_info_provider) =
                setup_runtime(
                    vec![alice_account()],
                    Balance::from_near(1_000_000),
                    Balance::ZERO,
                    Gas::from_teragas(1000),
                );
            apply_state.block_height = height;
            let receipt = Receipt::V0(ReceiptV0 {
                predecessor_id: alice_account(),
                receiver_id: account_id.clone(),
                receipt_id: CryptoHash::hash_borsh(height),
                receipt: ReceiptEnum::Action(ActionReceipt {
                    signer_id: alice_account(),
                    signer_public_key: signers[0].public_key(),
                    gas_price: GAS_PRICE,
                    output_data_receivers: vec![],
                    input_data_ids: vec![],
                    actions: vec![Action::Transfer(TransferAction {
                        deposit: Balance::from_near(1),
                    })],
                }),
            });
            let shard_uid = ShardUId::single_shard();
            let apply_result = runtime
                .apply(
                    tries.get_trie_for_shard(shard_uid, root),
                    &None,
                    &apply_state,
                    &[receipt],
                    SignedValidPeriodTransactions::empty(),
                    &epoch_info_provider,
                    Default::default(),
                )
                .unwrap();
            let root = commit_apply_result(&apply_result, &mut apply_state, &tries, shard_uid);
            let state = tries.new_trie_update(shard_uid, root);
            let account = get_account(&state, &account_id).unwrap().unwrap();
            assert!(!account.is_initialized(), "a transfer alone must not initialize the account");
            account.bootstrap_nonce().expect("an uninitialized account carries the nonce")
        };

        // Never height 1, where the seed is zero and a constant matches it by
        // accident, and two heights, so no single constant fits both.
        for height in [5, 10] {
            assert_eq!(
                seeded_at(height),
                initial_nonce_value(height),
                "the nonce must come from the height the funding receipt ran at"
            );
        }
    }

    /// Delete the account, fund the same `0u` id again, and replay the original
    /// bootstrap. This is the case the account-level nonce exists for: after a
    /// successful init the installed key's nonce would stop a replay, but a
    /// deletion takes every key row with it, so only the re-created record's own
    /// seed stands between the old signed bytes and a second execution.
    ///
    /// Recreation is always at a strictly greater height than the original
    /// creation (a transaction cannot even see the account in the chunk that
    /// created it), so the fresh seed always exceeds the consumed nonce.
    #[test]
    fn recreated_account_rejects_old_bootstrap() {
        init_test_logger();
        if skip() {
            return;
        }
        let signer = signer_for("bootstrap-recreated");
        let state_init = state_init_for(&[signer.public_key()]);
        let account_id = derive_universal_account_id(&state_init.to_raw());
        let (runtime, tries, root, mut apply_state, epoch) =
            setup(&account_id, Balance::from_near(10));

        let tx = bootstrap_tx(&signer, &state_init, &account_id, expected_nonce(), vec![]);
        let (root, outcomes) =
            apply_txs(&runtime, &tries, root, &apply_state, &epoch, vec![tx.clone()]);
        assert_matches!(outcomes[0].outcome.status, ExecutionStatus::SuccessReceiptId(_));

        // Delete the account and fund the same id again, as a deletion followed
        // by a fresh transfer would. The new record is seeded from the later
        // height at which that transfer's receipt ran.
        let recreated_at = apply_state.block_height + 1;
        let mut state = tries.new_trie_update(ShardUId::single_shard(), root);
        remove_account(&mut state, &account_id).unwrap();
        set_account(
            &mut state,
            account_id.clone(),
            &Account::new_uninitialized(
                Balance::from_near(10),
                apply_state.config.fees.storage_usage_config.num_bytes_account,
                initial_nonce_value(recreated_at),
            ),
        );
        state.commit(StateChangeCause::InitialState);
        let trie_changes = state.finalize().unwrap().trie_changes;
        let mut store_update = tries.store_update();
        let root = tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
        store_update.commit();

        apply_state.block_height = recreated_at + 1;
        let (_, outcomes) = apply_txs(&runtime, &tries, root, &apply_state, &epoch, vec![tx]);
        assert_matches!(
            outcomes[0].outcome.status,
            ExecutionStatus::Failure(TxExecutionError::InvalidTxError(
                InvalidTxError::InvalidNonce { .. }
            )),
            "the re-created account must not accept the previous incarnation's bootstrap"
        );
    }

    /// Actions that need a set-up account, placed *before* the state init in a
    /// self-signed transaction. This is the halt vector the uninitialized-account
    /// guard exists for, and self-signing is what makes it reachable: the account
    /// is its own actor, so `check_actor_permissions` waves these through and only
    /// the account-state check stands between them and a setter that an
    /// uninitialized account rejects, which is a failed chunk rather than a failed
    /// action.
    ///
    /// The relayer-path versions of these are covered where the guard lives; this
    /// pins the self-signed path, which did not exist when that guard was written.
    #[test]
    fn owner_only_actions_before_init_fail_gracefully() {
        init_test_logger();
        if skip() {
            return;
        }
        let signer = signer_for("bootstrap-prefix");
        let state_init = state_init_for(&[signer.public_key()]);
        let account_id = derive_universal_account_id(&state_init.to_raw());

        let dangerous = [
            // set_contract on an uninitialized account
            Action::DeployContract(DeployContractAction { code: vec![] }),
            Action::UseGlobalContract(Box::new(UseGlobalContractAction {
                contract_identifier: GlobalContractIdentifier::CodeHash(CryptoHash::default()),
            })),
            // set_locked on an uninitialized account
            Action::Stake(Box::new(StakeAction {
                stake: Balance::from_yoctonear(1),
                public_key: signer.public_key(),
            })),
            // Succeeds silently without the guard, installing a key the account
            // id does not commit to and double-counting its storage.
            Action::AddKey(Box::new(AddKeyAction {
                public_key: signer.public_key(),
                access_key: AccessKey::full_access(),
            })),
        ];
        // `DeleteAccount` is deliberately absent: `DeleteActionMustBeFinal`
        // rejects it before any of this, so it cannot precede the state init.
        // As the *last* action it runs against an account the init has already
        // initialized, and on its own the transaction is not a bootstrap at all.

        for action in dangerous {
            let (runtime, tries, root, apply_state, epoch) =
                setup(&account_id, Balance::from_near(10));
            let mut actions = vec![action.clone()];
            actions.push(Action::UniversalStateInit(Box::new(UniversalStateInitAction {
                state_init: state_init.to_raw(),
                deposit: Balance::ZERO,
            })));
            let tx = SignedTransaction::from_actions(
                expected_nonce(),
                account_id.clone(),
                account_id.clone(),
                &signer,
                actions,
                CryptoHash::default(),
            );

            // `apply` returning Ok at all is the property under test: a
            // StorageError here would be a failed chunk, not a failed action.
            let (new_root, outcomes) =
                apply_txs(&runtime, &tries, root, &apply_state, &epoch, vec![tx]);

            let receipt_outcome = outcomes
                .iter()
                .find(|o| matches!(o.outcome.status, ExecutionStatus::Failure(_)))
                .unwrap_or_else(|| panic!("expected a failed receipt for {action:?}"));
            assert_matches!(
                &receipt_outcome.outcome.status,
                ExecutionStatus::Failure(TxExecutionError::ActionError(err))
                    if matches!(err.kind, ActionErrorKind::AccountNotInitialized { .. }),
                "expected a graceful rejection for {action:?}",
            );

            // The whole receipt rolled back, so the state init did not run.
            let state = tries.new_trie_update(ShardUId::single_shard(), new_root);
            let account = get_account(&state, &account_id).unwrap().unwrap();
            assert!(!account.is_initialized(), "state must not be installed for {action:?}");
            assert!(
                get_access_key(&state, &account_id, &signer.public_key()).unwrap().is_none(),
                "no key may be installed for {action:?}",
            );
        }
    }

    /// The other direction: an owner-only action placed *after* the state init
    /// runs against an account the init has already initialized, so it is
    /// allowed. Actions in one receipt run in order, and the init is what flips
    /// the account, so the guard that refuses these before the init must not
    /// refuse them here.
    #[test]
    fn owner_only_actions_after_init_succeed() {
        init_test_logger();
        if skip() {
            return;
        }
        let signer = signer_for("bootstrap-suffix");
        let other = signer_for("bootstrap-suffix-other");
        let state_init = state_init_for(&[signer.public_key()]);
        let account_id = derive_universal_account_id(&state_init.to_raw());
        let (runtime, tries, root, apply_state, epoch) = setup(&account_id, Balance::from_near(10));

        // A key the state init does not commit to, which only an `AddKey` after
        // the init can install.
        let add_key = Action::AddKey(Box::new(AddKeyAction {
            public_key: other.public_key(),
            access_key: AccessKey::full_access(),
        }));
        let tx = bootstrap_tx(&signer, &state_init, &account_id, expected_nonce(), vec![add_key]);
        let (root, outcomes) = apply_txs(&runtime, &tries, root, &apply_state, &epoch, vec![tx]);

        assert_matches!(outcomes[0].outcome.status, ExecutionStatus::SuccessReceiptId(_));
        let receipt_outcome = outcomes
            .iter()
            .find(|outcome| !matches!(outcome.outcome.status, ExecutionStatus::SuccessReceiptId(_)))
            .expect("the state init receipt must have an outcome");
        assert_matches!(
            &receipt_outcome.outcome.status,
            ExecutionStatus::SuccessValue(_),
            "an AddKey after the init must not be refused as AccountNotInitialized"
        );

        let state = tries.new_trie_update(ShardUId::single_shard(), root);
        let account = get_account(&state, &account_id).unwrap().unwrap();
        assert!(account.is_initialized());
        assert!(
            get_access_key(&state, &account_id, &other.public_key()).unwrap().is_some(),
            "the added key must be installed"
        );
    }

    /// `DeleteAccount` as the final action, which `DeleteActionMustBeFinal`
    /// makes the only position it can take. The init initializes the account and
    /// the delete then removes it, both inside one receipt, so the account is
    /// gone by the end of the chunk.
    #[test]
    fn delete_after_init_removes_account() {
        init_test_logger();
        if skip() {
            return;
        }
        let signer = signer_for("bootstrap-then-delete");
        let state_init = state_init_for(&[signer.public_key()]);
        let account_id = derive_universal_account_id(&state_init.to_raw());
        let balance = Balance::from_near(10);
        let (runtime, tries, root, apply_state, epoch) = setup(&account_id, balance);

        // The beneficiary has to exist, otherwise the balance transfer the delete
        // sends it would come straight back as a refund.
        let beneficiary: AccountId = "beneficiary.near".parse().unwrap();
        let mut state = tries.new_trie_update(ShardUId::single_shard(), root);
        set_account(
            &mut state,
            beneficiary.clone(),
            &Account::new(Balance::from_near(1), Balance::ZERO, AccountContract::None, 100),
        );
        state.commit(StateChangeCause::InitialState);
        let trie_changes = state.finalize().unwrap().trie_changes;
        let mut store_update = tries.store_update();
        let root = tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
        store_update.commit();

        let delete = Action::DeleteAccount(DeleteAccountAction { beneficiary_id: beneficiary });
        let tx = bootstrap_tx(&signer, &state_init, &account_id, expected_nonce(), vec![delete]);
        let (root, outcomes) = apply_txs(&runtime, &tries, root, &apply_state, &epoch, vec![tx]);

        assert_matches!(outcomes[0].outcome.status, ExecutionStatus::SuccessReceiptId(_));
        let receipt_outcome = outcomes
            .iter()
            .find(|outcome| !matches!(outcome.outcome.status, ExecutionStatus::SuccessReceiptId(_)))
            .expect("the state init receipt must have an outcome");
        assert_matches!(
            &receipt_outcome.outcome.status,
            ExecutionStatus::SuccessValue(_),
            "initializing and then deleting in one receipt must succeed"
        );

        let state = tries.new_trie_update(ShardUId::single_shard(), root);
        assert!(
            get_account(&state, &account_id).unwrap().is_none(),
            "the account must be gone once the delete has run"
        );
    }

    /// A committed key that was deleted after the account was initialized cannot
    /// bootstrap it again. The account id commits to the original key set
    /// forever, so without the initialized check a revoked key would be able to
    /// re-authorize itself.
    #[test]
    fn revoked_key_cannot_re_bootstrap() {
        init_test_logger();
        if skip() {
            return;
        }
        let signer = signer_for("bootstrap-revoked");
        let state_init = state_init_for(&[signer.public_key()]);
        let account_id = derive_universal_account_id(&state_init.to_raw());
        let (runtime, tries, root, apply_state, epoch) = setup(&account_id, Balance::from_near(10));

        // Initialize the account but leave it with no access key at all, as a
        // `DeleteKey` of the last committed key would.
        let mut state = tries.new_trie_update(ShardUId::single_shard(), root);
        let mut account = get_account(&state, &account_id).unwrap().unwrap();
        account.initialize().unwrap();
        set_account(&mut state, account_id.clone(), &account);
        state.commit(StateChangeCause::InitialState);
        let trie_changes = state.finalize().unwrap().trie_changes;
        let mut store_update = tries.store_update();
        let root = tries.apply_all(&trie_changes, ShardUId::single_shard(), &mut store_update);
        store_update.commit();

        let tx = bootstrap_tx(&signer, &state_init, &account_id, expected_nonce(), vec![]);
        let (_, outcomes) = apply_txs(&runtime, &tries, root, &apply_state, &epoch, vec![tx]);

        assert_matches!(
            outcomes[0].outcome.status,
            ExecutionStatus::Failure(TxExecutionError::InvalidTxError(
                InvalidTxError::InvalidAccessKeyError(_)
            )),
            "an initialized account is never a bootstrap candidate"
        );
    }

    /// Naming somebody else's uninitialized account as the signer must not let
    /// the attacker spend its balance on their own state init. The transaction is
    /// not a bootstrap, because the state init does not derive to the signer.
    #[test]
    fn victims_account_cannot_be_named_as_signer() {
        init_test_logger();
        if skip() {
            return;
        }
        let attacker = signer_for("bootstrap-attacker");
        let victim_key = signer_for("bootstrap-victim");
        let attacker_init = state_init_for(&[attacker.public_key()]);
        let victim_init = state_init_for(&[victim_key.public_key()]);
        let victim_id = derive_universal_account_id(&victim_init.to_raw());
        let balance = Balance::from_near(10);
        let (runtime, tries, root, apply_state, epoch) = setup(&victim_id, balance);

        // Signed by the attacker's own key, paid for by the victim, installing
        // the attacker's state init.
        let tx = SignedTransaction::from_actions(
            expected_nonce(),
            victim_id.clone(),
            victim_id.clone(),
            &attacker,
            vec![Action::UniversalStateInit(Box::new(UniversalStateInitAction {
                state_init: attacker_init.to_raw(),
                deposit: Balance::ZERO,
            }))],
            CryptoHash::default(),
        );
        let (new_root, outcomes) =
            apply_txs(&runtime, &tries, root, &apply_state, &epoch, vec![tx]);

        assert_matches!(
            outcomes[0].outcome.status,
            ExecutionStatus::Failure(TxExecutionError::InvalidTxError(_))
        );
        let state = tries.new_trie_update(ShardUId::single_shard(), new_root);
        let account = get_account(&state, &victim_id).unwrap().unwrap();
        assert_eq!(account.amount(), balance, "the victim must not be charged");
        assert_eq!(account.bootstrap_nonce(), Some(initial_nonce_value(CREATION_HEIGHT)));
    }

    /// Two self-signed state inits for one account, signed by two different keys
    /// the state init commits to. Only the first can ever take effect; what the
    /// second does depends on the nonce it carries, so both outcomes are pinned.
    #[test]
    fn second_key_cannot_repeat_bootstrap() {
        init_test_logger();
        if skip() {
            return;
        }
        let first = signer_for("two-keys-first");
        let second = signer_for("two-keys-second");
        let state_init = state_init_for(&[first.public_key(), second.public_key()]);
        let account_id = derive_universal_account_id(&state_init.to_raw());

        // Consecutive chunks. By the time the second is converted the account is
        // initialized, so it is no longer a bootstrap: it takes the ordinary path
        // against the key the state init installed, whose nonce is seeded from
        // the execution height and so already exceeds the one it carries.
        let (runtime, tries, root, mut apply_state, epoch) =
            setup(&account_id, Balance::from_near(10));
        let tx1 = bootstrap_tx(&first, &state_init, &account_id, expected_nonce(), vec![]);
        let (root, outcomes) = apply_txs(&runtime, &tries, root, &apply_state, &epoch, vec![tx1]);
        assert_matches!(outcomes[0].outcome.status, ExecutionStatus::SuccessReceiptId(_));

        apply_state.block_height += 1;
        let tx2 = bootstrap_tx(&second, &state_init, &account_id, expected_nonce(), vec![]);
        let (_, outcomes) = apply_txs(&runtime, &tries, root, &apply_state, &epoch, vec![tx2]);
        assert_matches!(
            outcomes[0].outcome.status,
            ExecutionStatus::Failure(TxExecutionError::InvalidTxError(
                InvalidTxError::InvalidNonce { .. }
            )),
            "the second key must not be able to repeat the bootstrap"
        );

        // Same chunk, same nonce: the account is still uninitialized when both
        // are converted, so the second is a bootstrap too, and it is the account
        // nonce the first consumed that stops it.
        let (runtime, tries, root, apply_state, epoch) = setup(&account_id, Balance::from_near(10));
        let tx1 = bootstrap_tx(&first, &state_init, &account_id, expected_nonce(), vec![]);
        let tx2 = bootstrap_tx(&second, &state_init, &account_id, expected_nonce(), vec![]);
        let (_, outcomes) = apply_txs(&runtime, &tries, root, &apply_state, &epoch, vec![tx1, tx2]);
        assert_matches!(outcomes[0].outcome.status, ExecutionStatus::SuccessReceiptId(_));
        assert_matches!(
            outcomes[1].outcome.status,
            ExecutionStatus::Failure(TxExecutionError::InvalidTxError(
                InvalidTxError::InvalidNonce { .. }
            ))
        );

        // Same chunk, sequential nonces: both are admitted and both are charged.
        // The second's state init is a no-op, because by the time its receipt
        // runs the first has already initialized the account. This is the
        // deliberate idempotency that lets a relayer prepend an init it is not
        // sure is needed, so it is not an error.
        let (runtime, tries, root, apply_state, epoch) = setup(&account_id, Balance::from_near(10));
        let tx1 = bootstrap_tx(&first, &state_init, &account_id, expected_nonce(), vec![]);
        let tx2 = bootstrap_tx(&second, &state_init, &account_id, expected_nonce() + 1, vec![]);
        let (new_root, outcomes) =
            apply_txs(&runtime, &tries, root, &apply_state, &epoch, vec![tx1, tx2]);
        assert_matches!(outcomes[0].outcome.status, ExecutionStatus::SuccessReceiptId(_));
        assert_matches!(
            outcomes[1].outcome.status,
            ExecutionStatus::SuccessReceiptId(_),
            "a sequential nonce is admissible; the init itself is idempotent"
        );
        let state = tries.new_trie_update(ShardUId::single_shard(), new_root);
        assert!(get_account(&state, &account_id).unwrap().unwrap().is_initialized());
    }

    /// An account that does not exist at all is not a bootstrap candidate: the
    /// flow requires the account to have been funded first.
    #[test]
    fn missing_account_is_not_bootstrap() {
        init_test_logger();
        if skip() {
            return;
        }
        let signer = signer_for("bootstrap-missing");
        let state_init = state_init_for(&[signer.public_key()]);
        let account_id = derive_universal_account_id(&state_init.to_raw());
        // Set up around a *different* account, so this one was never funded.
        let other = derive_universal_account_id(&state_init_for(&[]).to_raw());
        let (runtime, tries, root, apply_state, epoch) = setup(&other, Balance::from_near(10));

        let tx = bootstrap_tx(&signer, &state_init, &account_id, expected_nonce(), vec![]);
        let (_, outcomes) = apply_txs(&runtime, &tries, root, &apply_state, &epoch, vec![tx]);

        assert_matches!(
            outcomes[0].outcome.status,
            ExecutionStatus::Failure(TxExecutionError::InvalidTxError(
                InvalidTxError::InvalidSignerId { .. } | InvalidTxError::SignerDoesNotExist { .. }
            ))
        );
    }
}

/// A relayer creating a `0u` account from nothing: the transfer that funds the
/// account is also what creates it, so the rest of the batch runs against an
/// account nobody had to set up first.
///
/// This is the one implicit kind whose creating transfer may be followed by other
/// actions, so most of these tests are about what such a batch still may *not*
/// do. They run at the `Runtime::apply` level to keep the receipt's action list
/// exact.
mod relayer_funded_state_init {
    use super::*;
    use near_crypto::PublicKeyHandle;
    use near_primitives::action::UniversalStateInitAction;
    use near_primitives::universal_state_init::{UniversalStateInit, UniversalStateInitV1};
    use near_primitives::utils::{
        derive_eth_implicit_account_id, derive_near_implicit_account_id,
        derive_universal_account_id,
    };
    use std::collections::{BTreeMap, BTreeSet};

    fn funding() -> Balance {
        Balance::from_near(5)
    }

    /// What the relayer starts with, so a test can check the funding came out of
    /// its balance rather than out of nowhere.
    fn relayer_start() -> Balance {
        Balance::from_near(100)
    }

    fn skip() -> bool {
        if !ProtocolFeature::UniversalAccounts.enabled(PROTOCOL_VERSION) {
            tracing::info!("skipping: UniversalAccounts not enabled at v{PROTOCOL_VERSION}");
            return true;
        }
        false
    }

    fn state_init_for(keys: &[PublicKey]) -> UniversalStateInit {
        UniversalStateInit::V1(UniversalStateInitV1 {
            code: None,
            data: BTreeMap::new(),
            access_keys: keys.iter().cloned().map(PublicKeyHandle::from).collect::<BTreeSet<_>>(),
        })
    }

    fn state_init_action(state_init: &UniversalStateInit) -> Action {
        Action::UniversalStateInit(Box::new(UniversalStateInitAction {
            state_init: state_init.to_raw(),
            deposit: Balance::ZERO,
        }))
    }

    fn add_key_action(public_key: &PublicKey) -> Action {
        Action::AddKey(Box::new(AddKeyAction {
            public_key: public_key.clone(),
            access_key: AccessKey::full_access(),
        }))
    }

    /// Apply one relayer-signed transaction of `actions` addressed to `receiver`
    /// and drain the receipts it produces, handing back the state it all left
    /// behind. The signer is not the receiver, so the action receipt is buffered
    /// rather than local and only runs in the round after the transaction.
    fn apply_relayer_batch(
        receiver: &AccountId,
        actions: Vec<Action>,
    ) -> (ShardTries, CryptoHash, Vec<ExecutionOutcomeWithId>) {
        let (runtime, tries, mut root, mut apply_state, signers, epoch) = setup_runtime(
            vec![alice_account()],
            relayer_start(),
            Balance::ZERO,
            Gas::from_teragas(1000),
        );
        let shard_uid = ShardUId::single_shard();
        let tx = SignedTransaction::from_actions(
            1,
            alice_account(),
            receiver.clone(),
            &signers[0],
            actions,
            CryptoHash::default(),
        );

        let mut outcomes = vec![];
        let mut incoming = vec![];
        let mut settled = false;
        // The relayer is not the receiver, so the action receipt is buffered rather
        // than run locally: round 0 converts the transaction, round 1 runs the
        // batch, round 2 delivers the refunds. The fourth is slack, and the loop
        // exits as soon as nothing is left in flight.
        for round in 0..4 {
            let result = runtime
                .apply(
                    tries.get_trie_for_shard(shard_uid, root),
                    &None,
                    &apply_state,
                    &incoming,
                    if round == 0 {
                        SignedValidPeriodTransactions::new(vec![tx.clone()], vec![true])
                    } else {
                        SignedValidPeriodTransactions::empty()
                    },
                    &epoch,
                    Default::default(),
                )
                .unwrap();
            let delayed = result.delayed_receipts_count;
            root = commit_apply_result(&result, &mut apply_state, &tries, shard_uid);
            outcomes.extend(result.outcomes);
            incoming = result.outgoing_receipts;
            apply_state.block_height += 1;
            if round > 0 && incoming.is_empty() && delayed == 0 {
                settled = true;
                break;
            }
        }
        // Every assertion downstream reads the state the cascade left behind, so a
        // run that stopped with a receipt still queued would be measuring a
        // half-finished one, and could mask a failure or invent one.
        assert!(settled, "receipt cascade did not settle within the round budget");
        (tries, root, outcomes)
    }

    /// The outcome of the action receipt the batch ran as, looked up by the id the
    /// transaction's own outcome points at. Taking the first outcome that is not a
    /// `SuccessReceiptId` would be ambiguous: a refund receipt is an ordinary action
    /// receipt and gets a full outcome of its own, with status `SuccessValue`.
    fn receipt_outcome(outcomes: &[ExecutionOutcomeWithId]) -> &ExecutionOutcome {
        let receipt_id = outcomes
            .iter()
            .find_map(|outcome| match outcome.outcome.status {
                ExecutionStatus::SuccessReceiptId(receipt_id) => Some(receipt_id),
                _ => None,
            })
            .expect("the transaction must produce an action receipt");
        outcomes
            .iter()
            .find(|outcome| outcome.id == receipt_id)
            .map(|outcome| &outcome.outcome)
            .expect("the action receipt must have an outcome")
    }

    /// The flow this relaxation is for: one relayer-signed transaction brings a
    /// `0u` account into existence, funds it, and installs the state its id is
    /// the hash of.
    #[test]
    fn batch_creates_funds_and_initializes_account() {
        init_test_logger();
        if skip() {
            return;
        }
        let key = SecretKey::from_seed(KeyType::ED25519, "relayer-funded").public_key();
        let state_init = state_init_for(from_ref(&key));
        let account_id = derive_universal_account_id(&state_init.to_raw());

        let (tries, root, outcomes) = apply_relayer_batch(
            &account_id,
            vec![
                Action::Transfer(TransferAction { deposit: funding() }),
                state_init_action(&state_init),
            ],
        );

        assert_matches!(receipt_outcome(&outcomes).status, ExecutionStatus::SuccessValue(_));
        let state = tries.new_trie_update(ShardUId::single_shard(), root);
        let account = get_account(&state, &account_id).unwrap().unwrap();
        assert!(account.is_initialized(), "the batched state init must have run");
        assert_eq!(account.amount(), funding(), "the transfer that created it must also fund it");
        assert!(
            get_access_key(&state, &account_id, &key).unwrap().is_some(),
            "the committed key must be installed",
        );
        let relayer = get_account(&state, &alice_account()).unwrap().unwrap();
        assert!(
            relayer.amount() <= relayer_start().saturating_sub(funding()),
            "the funding must leave the relayer's balance, not appear out of nowhere",
        );
    }

    /// A lone transfer still creates a `0u` account, uninitialized. The relaxation
    /// only adds a case, so the shape that worked before must keep working, and
    /// this is the only test here that pins the `is_the_only_action` term.
    #[test]
    fn lone_transfer_creates_universal_account() {
        init_test_logger();
        if skip() {
            return;
        }
        let key = SecretKey::from_seed(KeyType::ED25519, "lone-transfer").public_key();
        let account_id = derive_universal_account_id(&state_init_for(from_ref(&key)).to_raw());

        let (tries, root, outcomes) = apply_relayer_batch(
            &account_id,
            vec![Action::Transfer(TransferAction { deposit: funding() })],
        );

        assert_matches!(receipt_outcome(&outcomes).status, ExecutionStatus::SuccessValue(_));
        let state = tries.new_trie_update(ShardUId::single_shard(), root);
        let account = get_account(&state, &account_id).unwrap().unwrap();
        assert!(!account.is_initialized(), "a transfer alone must not install any state");
        assert_eq!(account.amount(), funding());
    }

    /// The relaxation is about implicit ids, not about account creation in general:
    /// a transfer to a name that does not exist must still fail, however lonely it
    /// is. Pins the `is_implicit` term of the gate.
    #[test]
    fn lone_transfer_may_not_create_named_account() {
        init_test_logger();
        let account_id: AccountId = "not-there.near".parse().unwrap();

        let (tries, root, outcomes) = apply_relayer_batch(
            &account_id,
            vec![Action::Transfer(TransferAction { deposit: funding() })],
        );

        assert_matches!(
            &receipt_outcome(&outcomes).status,
            ExecutionStatus::Failure(TxExecutionError::ActionError(err))
                if matches!(err.kind, ActionErrorKind::AccountDoesNotExist { .. }),
            "a transfer must never create a named account",
        );
        let state = tries.new_trie_update(ShardUId::single_shard(), root);
        assert!(get_account(&state, &account_id).unwrap().is_none());
    }

    /// The trap the `is_the_only_action` gate used to close. The state init lifts
    /// the uninitialized guard, so the only thing between a relayer and somebody
    /// else's `0u` address is `actor_id`: creating the account must leave it
    /// pointing at the relayer. Without that, a relayer could add a key the id
    /// does not commit to, or delete the account and take its balance.
    #[test]
    fn batch_may_not_take_over_account_it_creates() {
        init_test_logger();
        if skip() {
            return;
        }
        let owner = SecretKey::from_seed(KeyType::ED25519, "rightful-owner").public_key();
        let relayer_key = SecretKey::from_seed(KeyType::ED25519, "relayer-hijack").public_key();
        let state_init = state_init_for(&[owner]);
        let account_id = derive_universal_account_id(&state_init.to_raw());

        let takeovers = [
            add_key_action(&relayer_key),
            // Last position is the only one `DeleteActionMustBeFinal` allows,
            // and it is where the account is already initialized.
            Action::DeleteAccount(DeleteAccountAction { beneficiary_id: alice_account() }),
        ];

        for takeover in takeovers {
            let (tries, root, outcomes) = apply_relayer_batch(
                &account_id,
                vec![
                    Action::Transfer(TransferAction { deposit: funding() }),
                    state_init_action(&state_init),
                    takeover.clone(),
                ],
            );

            assert_matches!(
                &receipt_outcome(&outcomes).status,
                ExecutionStatus::Failure(TxExecutionError::ActionError(err))
                    if matches!(err.kind, ActionErrorKind::ActorNoPermission { .. }),
                "a relayer must not inherit the authority of the account it created ({takeover:?})",
            );
            // The receipt rolled back, so not even the account survives.
            let state = tries.new_trie_update(ShardUId::single_shard(), root);
            assert!(get_account(&state, &account_id).unwrap().is_none());
        }
    }

    /// Without a state init in front of it, the account the transfer creates is
    /// uninitialized, which is inert for everything but its own init and a further
    /// transfer.
    #[test]
    fn owner_only_action_without_init_is_refused() {
        init_test_logger();
        if skip() {
            return;
        }
        let key = SecretKey::from_seed(KeyType::ED25519, "no-init-first").public_key();
        let account_id = derive_universal_account_id(&state_init_for(from_ref(&key)).to_raw());

        let (tries, root, outcomes) = apply_relayer_batch(
            &account_id,
            vec![Action::Transfer(TransferAction { deposit: funding() }), add_key_action(&key)],
        );

        assert_matches!(
            &receipt_outcome(&outcomes).status,
            ExecutionStatus::Failure(TxExecutionError::ActionError(err))
                if matches!(err.kind, ActionErrorKind::AccountNotInitialized { .. }),
            "an uninitialized account must stay inert for an owner-only action",
        );
        let state = tries.new_trie_update(ShardUId::single_shard(), root);
        assert!(get_account(&state, &account_id).unwrap().is_none());
    }

    /// The relaxation is for `0u` ids only. Every other implicit kind is fully
    /// usable the moment it exists, so a transfer that creates one still has to
    /// be the whole receipt: the batch does not create the account at all, which
    /// leaves nothing for the actions after it to take over.
    ///
    /// Stable behaviour rather than a universal-accounts one, so the two tests
    /// below run at every protocol version.
    fn assert_batch_may_not_create(account_id: &AccountId, key: &PublicKey) {
        let (tries, root, outcomes) = apply_relayer_batch(
            account_id,
            vec![Action::Transfer(TransferAction { deposit: funding() }), add_key_action(key)],
        );

        assert_matches!(
            &receipt_outcome(&outcomes).status,
            ExecutionStatus::Failure(TxExecutionError::ActionError(err))
                if matches!(err.kind, ActionErrorKind::AccountDoesNotExist { .. }),
            "a batched transfer must not create {account_id}",
        );
        let state = tries.new_trie_update(ShardUId::single_shard(), root);
        assert!(get_account(&state, account_id).unwrap().is_none());
    }

    #[test]
    fn batch_may_not_create_near_implicit_account() {
        init_test_logger();
        let key = SecretKey::from_seed(KeyType::ED25519, "near-implicit").public_key();
        assert_batch_may_not_create(
            &derive_near_implicit_account_id(key.unwrap_as_ed25519()),
            &key,
        );
    }

    #[test]
    fn batch_may_not_create_eth_implicit_account() {
        init_test_logger();
        let key = SecretKey::from_seed(KeyType::SECP256K1, "eth-implicit").public_key();
        assert_batch_may_not_create(
            &derive_eth_implicit_account_id(key.unwrap_as_secp256k1()),
            &key,
        );
    }

    /// The other half of the old gate, untouched by the relaxation: refunds are
    /// free, so they must not create an account, a `0u` one included.
    #[test]
    fn refund_may_not_create_universal_account() {
        init_test_logger();
        if skip() {
            return;
        }
        let key = SecretKey::from_seed(KeyType::ED25519, "refund-target").public_key();
        let account_id = derive_universal_account_id(&state_init_for(&[key]).to_raw());
        let (runtime, tries, root, apply_state, _signers, epoch) = setup_runtime(
            vec![alice_account()],
            Balance::from_near(100),
            Balance::ZERO,
            Gas::from_teragas(1000),
        );

        let result = runtime
            .apply(
                tries.get_trie_for_shard(ShardUId::single_shard(), root),
                &None,
                &apply_state,
                from_ref(&Receipt::new_balance_refund(&account_id, funding())),
                SignedValidPeriodTransactions::empty(),
                &epoch,
                Default::default(),
            )
            .unwrap();
        let mut store_update = tries.store_update();
        let new_root =
            tries.apply_all(&result.trie_changes, ShardUId::single_shard(), &mut store_update);
        store_update.commit();

        // Assert on the reason, not just the absence: without this the test would
        // also pass if the refund receipt were dropped instead of refused.
        let [outcome] = &result.outcomes[..] else {
            panic!("the refund receipt must produce exactly one outcome, got {:?}", result.outcomes)
        };
        assert_matches!(
            &outcome.outcome.status,
            ExecutionStatus::Failure(TxExecutionError::ActionError(err))
                if matches!(err.kind, ActionErrorKind::AccountDoesNotExist { .. }),
            "a refund to a missing `0u` id must fail with AccountDoesNotExist",
        );
        let state = tries.new_trie_update(ShardUId::single_shard(), new_root);
        assert!(
            get_account(&state, &account_id).unwrap().is_none(),
            "a refund must not bring a `0u` account into existence",
        );
    }
}

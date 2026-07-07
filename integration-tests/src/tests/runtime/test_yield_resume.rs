use crate::node::{Node, RuntimeNode};
use near_parameters::{RuntimeConfig, RuntimeConfigStore};
use near_primitives::action::{
    Action, DeployGlobalContractAction, GlobalContractDeployMode, GlobalContractIdentifier,
    UseGlobalContractAction,
};
use near_primitives::types::AccountId;
use near_primitives::types::{Balance, Gas};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::FinalExecutionOutcomeView;
use near_primitives::views::FinalExecutionStatus;
use testlib::fees_utils::FeeHelper;

// AccountCostIncrease adds a refund for the purchase/burn price difference.
const fn extra_refund_outcomes() -> usize {
    if ProtocolFeature::AccountCostIncrease.enabled(PROTOCOL_VERSION) { 1 } else { 0 }
}

/// Initial balance used in tests.
pub const TESTING_INIT_BALANCE: Balance = Balance::from_near(1_000_000_000);

/// Max prepaid amount of gas.
const MAX_GAS: Gas = Gas::from_teragas(300);

fn setup_test_contract(wasm_binary: &[u8]) -> RuntimeNode {
    let node = RuntimeNode::new(&"alice.near".parse().unwrap());
    let account_id = node.account_id().unwrap();
    let node_user = node.user();
    let transaction_result = node_user
        .create_account(
            account_id,
            "test_contract.alice.near".parse().unwrap(),
            node.signer().public_key(),
            TESTING_INIT_BALANCE.checked_div(2).unwrap(),
        )
        .unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 1 + extra_refund_outcomes());

    let transaction_result = node_user
        .deploy_contract("test_contract.alice.near".parse().unwrap(), wasm_binary.to_vec())
        .unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 1 + extra_refund_outcomes());

    node
}

#[test]
fn create_then_resume() {
    let node = setup_test_contract(near_test_contracts::rs_contract());

    let yield_payload = vec![6u8; 16];

    // Hardcoded key under which the yield callback will write data.
    // We use this to observe whether the callback has been executed.
    let key = 123u64.to_le_bytes().to_vec();

    // Set up the yield execution
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_yield_create_return_data_id",
            yield_payload.clone(),
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();

    let data_id = match res.status {
        FinalExecutionStatus::SuccessValue(data_id) => data_id,
        _ => {
            panic!("{res:?} unexpected result; expected some data id");
        }
    };

    // Confirm that the yield callback hasn't been executed yet
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "read_value",
            key.clone(),
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();
    assert_eq!(res.status, FinalExecutionStatus::SuccessValue(vec![]), "{res:?} unexpected result",);

    // Call yield resume with the payload followed by the data id
    let args: Vec<u8> = yield_payload.into_iter().chain(data_id.into_iter()).collect();
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_yield_resume",
            args,
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();
    assert_eq!(
        res.status,
        FinalExecutionStatus::SuccessValue(vec![1u8]),
        "{res:?} unexpected result; expected 1",
    );

    // Confirm that the yield callback was executed
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "read_value",
            key,
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();
    assert_eq!(
        res.status,
        FinalExecutionStatus::SuccessValue("Resumed ".as_bytes().to_vec()),
        "{res:?} unexpected result",
    );
}

#[test]
fn create_and_resume_in_one_call() {
    let node = setup_test_contract(near_test_contracts::rs_contract());

    let yield_payload = vec![23u8; 16];

    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_yield_create_and_resume",
            yield_payload,
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();

    // the yield callback is expected to execute successfully,
    // returning twice the value of the first byte of the payload
    assert_eq!(
        res.status,
        FinalExecutionStatus::SuccessValue(vec![16u8]),
        "{res:?} unexpected result; expected 16",
    );
}

#[test]
fn resume_without_yield() {
    let node = setup_test_contract(near_test_contracts::rs_contract());

    // payload followed by data id
    let args: Vec<u8> = vec![42u8; 12].into_iter().chain(vec![23u8; 32].into_iter()).collect();

    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_yield_resume",
            args,
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();

    // expect the execution to succeed, but return 'false'
    assert_eq!(
        res.status,
        FinalExecutionStatus::SuccessValue(vec![0u8]),
        "{res:?} unexpected result; expected 0",
    );
}

fn b64(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn yield_create_with_id_op(yield_id: &[u8; 32], payload: &[u8], id: i64) -> serde_json::Value {
    serde_json::json!({
        "yield_create_with_id": {
            "method_name": "check_promise_result_return_value",
            "arguments": b64(payload),
            "gas": 0,
            "gas_weight": 1,
            "yield_id": b64(yield_id),
        },
        "id": id,
    })
}

fn yield_create_with_id_op_with_amount(
    yield_id: &[u8; 32],
    payload: &[u8],
    amount: u128,
    id: i64,
) -> serde_json::Value {
    serde_json::json!({
        "yield_create_with_id": {
            "method_name": "check_promise_result_return_value",
            "arguments": b64(payload),
            "amount": amount.to_string(),
            "gas": 0,
            "gas_weight": 1,
            "yield_id": b64(yield_id),
        },
        "id": id,
    })
}

fn yield_resume_with_yield_id_op(yield_id: &[u8], payload: &[u8], id: i64) -> serde_json::Value {
    serde_json::json!({
        "yield_resume_with_yield_id": { "yield_id": b64(yield_id), "payload": b64(payload) },
        "id": id,
    })
}

#[test]
fn create_with_id_then_resume_with_yield_id() {
    let node = setup_test_contract(near_test_contracts::rs_contract());

    let yield_payload = vec![6u8; 16];
    let yield_id = [9u8; 32];

    // TX1: Create the yield using yield_create_with_id (via call_promise).
    // Use callback that writes to storage so we can observe execution.
    let create_args = serde_json::json!([{
        "yield_create_with_id": {
            "method_name": "check_promise_result_write_status",
            "arguments": b64(&yield_payload),
            "gas": 0,
            "gas_weight": 1,
            "yield_id": b64(&yield_id),
        },
        "id": 0,
    }]);
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_promise",
            serde_json::to_vec(&create_args).unwrap(),
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();
    assert_eq!(res.status, FinalExecutionStatus::SuccessValue(vec![]), "{res:?} unexpected result");

    // TX2: Resume using yield_resume_with_yield_id (expect success = 1).
    let resume_args =
        serde_json::json!([yield_resume_with_yield_id_op(&yield_id, &yield_payload, 1)]);
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_promise",
            serde_json::to_vec(&resume_args).unwrap(),
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();
    assert_eq!(res.status, FinalExecutionStatus::SuccessValue(vec![]), "{res:?} unexpected result");

    // Confirm the yield callback executed
    let key = 123u64.to_le_bytes().to_vec();
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "read_value",
            key,
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();
    assert_eq!(
        res.status,
        FinalExecutionStatus::SuccessValue("Resumed ".as_bytes().to_vec()),
        "{res:?} unexpected result",
    );
}

#[test]
fn create_with_id_and_resume_with_yield_id_in_one_call() {
    let node = setup_test_contract(near_test_contracts::rs_contract());

    let yield_payload = vec![23u8; 16];
    let yield_id = [3u8; 32];

    // Create yield (id=0, with promise_return), then resume in the same call (id=1 = success).
    let args = serde_json::json!([
        {
            "yield_create_with_id": {
                "method_name": "check_promise_result_return_value",
                "arguments": b64(&yield_payload),
                "gas": 0,
                "gas_weight": 1,
                "yield_id": b64(&yield_id),
            },
            "id": 0,
            "return": true,
        },
        yield_resume_with_yield_id_op(&yield_id, &yield_payload, 1),
    ]);
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_promise",
            serde_json::to_vec(&args).unwrap(),
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();

    // The yield callback returns twice the first byte of the payload (23 * 2 - mod gives 16).
    assert_eq!(
        res.status,
        FinalExecutionStatus::SuccessValue(vec![16u8]),
        "{res:?} unexpected result; expected 16",
    );
}

#[test]
fn resume_with_yield_id_without_yield() {
    let node = setup_test_contract(near_test_contracts::rs_contract());

    // Resume with a yield_id that was never created — expect failure (id=0).
    let args = serde_json::json!([yield_resume_with_yield_id_op(&[23u8; 32], &[42u8; 12], 0)]);
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_promise",
            serde_json::to_vec(&args).unwrap(),
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();

    assert_eq!(res.status, FinalExecutionStatus::SuccessValue(vec![]), "{res:?} unexpected result");
}

#[test]
fn create_with_id_duplicate_in_same_call_returns_sentinel() {
    let node = setup_test_contract(near_test_contracts::rs_contract());

    let yield_payload = vec![6u8; 16];
    let yield_id = [5u8; 32];

    // First create returns 0 (first promise idx); second call with same yield_id returns
    // u64::MAX (-1 as i64) without aborting.
    let args = serde_json::json!([
        yield_create_with_id_op(&yield_id, &yield_payload, 0),
        yield_create_with_id_op(&yield_id, &yield_payload, -1),
    ]);
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_promise",
            serde_json::to_vec(&args).unwrap(),
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();

    assert_eq!(res.status, FinalExecutionStatus::SuccessValue(vec![]), "{res:?} unexpected result");
}

#[test]
fn create_with_id_then_resume_with_yield_id_fails() {
    let node = setup_test_contract(near_test_contracts::rs_contract());

    let yield_payload = vec![6u8; 16];
    let yield_id = [7u8; 32];

    // TX1: Create yield with yield_create_with_id.
    let create_args = serde_json::json!([yield_create_with_id_op(&yield_id, &yield_payload, 0)]);
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_promise",
            serde_json::to_vec(&create_args).unwrap(),
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();
    assert_eq!(res.status, FinalExecutionStatus::SuccessValue(vec![]), "{res:?} unexpected result");

    // TX2: Try to resume using yield_id with the OLD yield_resume (data_id flavor).
    // The yield_id is not a valid data_id, so resume returns 0 (false).
    let resume_args: Vec<u8> = yield_payload.into_iter().chain(yield_id.into_iter()).collect();
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_yield_resume",
            resume_args,
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();
    assert_eq!(
        res.status,
        FinalExecutionStatus::SuccessValue(vec![0u8]),
        "{res:?} unexpected result; expected 0",
    );
}

#[test]
fn create_then_resume_with_yield_id_fails() {
    let node = setup_test_contract(near_test_contracts::rs_contract());

    let yield_payload = vec![6u8; 16];

    // TX1: Create yield with the original yield_create (no yield_id mapping stored).
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_yield_create_return_data_id",
            yield_payload.clone(),
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();
    let data_id = match res.status {
        FinalExecutionStatus::SuccessValue(data_id) => data_id,
        _ => panic!("{res:?} unexpected result"),
    };

    // TX2: Try resuming with yield_resume_with_yield_id using the data_id as a yield_id.
    // No yield_id mapping exists for this data_id → returns 0 (failure).
    let args = serde_json::json!([yield_resume_with_yield_id_op(&data_id, &yield_payload, 0)]);
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            "test_contract.alice.near".parse().unwrap(),
            "call_promise",
            serde_json::to_vec(&args).unwrap(),
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();
    assert_eq!(res.status, FinalExecutionStatus::SuccessValue(vec![]), "{res:?} unexpected result");
}

fn fee_helper(node: &RuntimeNode) -> FeeHelper {
    let store = RuntimeConfigStore::new(None);
    let config = RuntimeConfig::clone(store.get_config(node.genesis().config.protocol_version));
    FeeHelper::new(config, node.genesis().config.min_gas_price)
}

/// The portion of the function call's burnt gas that is credited back to the
/// contract account as a reward (30% of wasm-burnt gas).
fn contract_function_call_reward(
    fee_helper: &FeeHelper,
    res: &FinalExecutionOutcomeView,
    method_name: &str,
    args_len: u64,
) -> Balance {
    let num_bytes = method_name.len() as u64 + args_len;
    let gas_burnt_for_function_call = res.receipts_outcome[0]
        .outcome
        .gas_burnt
        .checked_sub(fee_helper.function_call_exec_gas(num_bytes))
        .unwrap();
    fee_helper.gas_burnt_to_reward(gas_burnt_for_function_call)
}

#[test]
fn create_with_id_deducts_attached_amount() {
    let node = setup_test_contract(near_test_contracts::rs_contract());
    let contract_id: AccountId = "test_contract.alice.near".parse().unwrap();

    let balance_before = node.user().view_account(&contract_id).unwrap().amount;

    let attached = Balance::from_near(1);
    let yield_id = [11u8; 32];
    let yield_payload = vec![6u8; 16];
    let args = serde_json::to_vec(&serde_json::json!([yield_create_with_id_op_with_amount(
        &yield_id,
        &yield_payload,
        attached.as_yoctonear(),
        0,
    )]))
    .unwrap();
    let args_len = args.len() as u64;
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            contract_id.clone(),
            "call_promise",
            args,
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();
    assert_eq!(res.status, FinalExecutionStatus::SuccessValue(vec![]), "{res:?} unexpected result");

    let reward = contract_function_call_reward(&fee_helper(&node), &res, "call_promise", args_len);
    let balance_after = node.user().view_account(&contract_id).unwrap().amount;
    let expected = balance_before.checked_sub(attached).unwrap().checked_add(reward).unwrap();
    assert_eq!(
        balance_after, expected,
        "contract balance should decrease by exactly the attached amount, minus the function-call gas reward",
    );
}

#[test]
fn create_with_id_fails_when_amount_exceeds_balance() {
    let node = setup_test_contract(near_test_contracts::rs_contract());
    let contract_id: AccountId = "test_contract.alice.near".parse().unwrap();

    let balance_before = node.user().view_account(&contract_id).unwrap().amount;
    let too_much = balance_before.checked_add(Balance::from_near(1)).unwrap();

    let yield_id = [12u8; 32];
    let yield_payload = vec![6u8; 16];
    let args = serde_json::to_vec(&serde_json::json!([yield_create_with_id_op_with_amount(
        &yield_id,
        &yield_payload,
        too_much.as_yoctonear(),
        0,
    )]))
    .unwrap();
    let args_len = args.len() as u64;
    let res = node
        .user()
        .function_call(
            "alice.near".parse().unwrap(),
            contract_id.clone(),
            "call_promise",
            args,
            MAX_GAS,
            Balance::ZERO,
        )
        .unwrap();

    let err = match &res.status {
        FinalExecutionStatus::Failure(e) => e,
        other => panic!("expected Failure due to BalanceExceeded, got {other:?}"),
    };
    let err_str = format!("{err:?}");
    assert!(err_str.contains("balance"), "expected a balance-related error, got {err_str}");

    let reward = contract_function_call_reward(&fee_helper(&node), &res, "call_promise", args_len);
    let balance_after = node.user().view_account(&contract_id).unwrap().amount;
    let expected = balance_before.checked_add(reward).unwrap();
    assert_eq!(
        balance_after, expected,
        "failed deduction must leave the contract balance untouched aside from the function-call gas reward",
    );
}

// The 1-yoctoNEAR exemption in VMLogic only fires when `current_account_balance`
// is exactly zero. To reach that state end-to-end the contract must drain its
// entire balance via a Transfer promise in the same wasm call. The runtime's
// `check_storage_stake` would normally reject a contract account with 0 balance,
// but Zero Balance Accounts (NEP-448) — accounts whose `storage_usage` fits
// within `ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT` (770 bytes) — are exempt from
// that check (see `verifier.rs::check_storage_stake`).
//
// The standard `rs_contract` is large (~100 KB), so it can't be a ZBA. To get
// the test contract API on a ZBA, we deploy it once as a *global* contract from
// `alice.near` and have a small account (`zba.alice.near`) reference it via
// `UseGlobalContract` — that leaves the referencing account at ~150 bytes of
// storage, well within the ZBA limit.
fn setup_zba_global_contract() -> (RuntimeNode, AccountId) {
    use testlib::runtime_utils::alice_account;
    let node = RuntimeNode::new(&alice_account());
    let alice_id = alice_account();
    let zba_id: AccountId = "zba.alice.near".parse().unwrap();

    // Deploy rs_contract as a global contract owned by alice.near.
    let deploy = vec![Action::DeployGlobalContract(DeployGlobalContractAction {
        code: near_test_contracts::rs_contract().to_vec().into(),
        deploy_mode: GlobalContractDeployMode::AccountId,
    })];
    let res =
        node.user().sign_and_commit_actions(alice_id.clone(), alice_id.clone(), deploy).unwrap();
    assert!(
        matches!(res.status, FinalExecutionStatus::SuccessValue(_)),
        "DeployGlobalContract failed: {res:?}",
    );

    // Create zba.alice.near with alice's public key as its access key, so the
    // existing node.user() (which signs with alice's key) can later send
    // transactions AS zba.alice.near.
    let res = node
        .user()
        .create_account(
            alice_id.clone(),
            zba_id.clone(),
            node.signer().public_key(),
            TESTING_INIT_BALANCE.checked_div(10).unwrap(),
        )
        .unwrap();
    assert!(
        matches!(res.status, FinalExecutionStatus::SuccessValue(_)),
        "create_account(zba) failed: {res:?}",
    );

    // Wire zba.alice.near to use the global contract.
    let use_global = vec![Action::UseGlobalContract(Box::new(UseGlobalContractAction {
        contract_identifier: GlobalContractIdentifier::AccountId(alice_id),
    }))];
    let res =
        node.user().sign_and_commit_actions(zba_id.clone(), zba_id.clone(), use_global).unwrap();
    assert!(
        matches!(res.status, FinalExecutionStatus::SuccessValue(_)),
        "UseGlobalContract failed: {res:?}",
    );

    // Sanity check: zba must actually qualify as a ZBA (≤770 bytes of storage)
    // for the storage-stake exemption to apply once we drain its balance.
    let view = node.user().view_account(&zba_id).unwrap();
    assert!(
        view.storage_usage <= 770,
        "zba.alice.near is not a zero-balance account: storage_usage = {} > 770",
        view.storage_usage,
    );

    (node, zba_id)
}

/// JSON args for `call_promise`: drain the full account balance via a Transfer
/// promise, then call `yield_create_with_id` attaching `attach_yocto` yoctoNEAR.
fn drain_then_yield_args(
    drain_to: &AccountId,
    drain_amount: Balance,
    yield_id: &[u8; 32],
    payload: &[u8],
    attach_yocto: u128,
) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!([
        {"batch_create": {"account_id": drain_to.to_string()}, "id": 0},
        {"action_transfer": {
            "promise_index": 0,
            "amount": drain_amount.as_yoctonear().to_string(),
        }, "id": 0},
        yield_create_with_id_op_with_amount(yield_id, payload, attach_yocto, 1),
    ]))
    .unwrap()
}

#[test]
fn create_with_id_one_yocto_exemption_on_drained_zba() {
    use testlib::runtime_utils::alice_account;
    let (node, zba_id) = setup_zba_global_contract();
    let alice_id = alice_account();
    let balance_before = node.user().view_account(&zba_id).unwrap().amount;
    let subsidized_before = node.client.read().cumulative_subsidized;

    // Sign as alice (not zba) so that the gas reservation comes out of alice's
    // balance — keeping zba's balance untouched until the wasm runs. The gas
    // reward still goes to the receiver (zba).
    let args = drain_then_yield_args(&alice_id, balance_before, &[20u8; 32], &[6u8; 16], 1);
    let args_len = args.len() as u64;
    let res = node
        .user()
        .function_call(alice_id, zba_id.clone(), "call_promise", args, MAX_GAS, Balance::ZERO)
        .unwrap();

    assert_eq!(
        res.status,
        FinalExecutionStatus::SuccessValue(vec![]),
        "drain + 1-yocto via the exemption should succeed on a ZBA; got {res:?}",
    );

    // The exemption fires exactly once — confirm the runtime tracked it.
    let subsidized_after = node.client.read().cumulative_subsidized;
    let subsidy = subsidized_after.checked_sub(subsidized_before).unwrap();
    assert_eq!(
        subsidy,
        Balance::from_yoctonear(1),
        "expected exactly 1 yoctoNEAR of subsidy (the exemption), got {} yoctoNEAR",
        subsidy.as_yoctonear(),
    );

    // The wasm drains the entire account_balance, then the 1-yocto deduction is
    // skipped via the exemption — leaving VMLogic balance at 0. The runtime
    // sets account.amount = 0, the ZBA check lets that stand, and finally the
    // receiver gas reward is credited on top, so balance_after = reward exactly.
    let reward = contract_function_call_reward(&fee_helper(&node), &res, "call_promise", args_len);
    let view = node.user().view_account(&zba_id).unwrap();
    assert_eq!(
        view.amount, reward,
        "expected zba balance to equal exactly the gas reward (drained + subsidized + reward)",
    );
    assert!(
        view.storage_usage <= 770,
        "zba should still qualify as a ZBA after the call: storage_usage = {}",
        view.storage_usage,
    );
}

#[test]
fn create_with_id_two_yocto_fails_on_drained_zba() {
    use testlib::runtime_utils::alice_account;
    let (node, zba_id) = setup_zba_global_contract();
    let alice_id = alice_account();
    let balance_before = node.user().view_account(&zba_id).unwrap().amount;
    let subsidized_before = node.client.read().cumulative_subsidized;

    // Same drain, but attach 2 yocto: the exemption only covers exactly 1 yocto,
    // so `deduct_balance(2)` on a zero balance fails inside the wasm with
    // BalanceExceeded. The failure rolls back the FunctionCall, including the
    // Transfer, so the zba account keeps its original balance (plus reward).
    let args = drain_then_yield_args(&alice_id, balance_before, &[21u8; 32], &[6u8; 16], 2);
    let args_len = args.len() as u64;
    let res = node
        .user()
        .function_call(alice_id, zba_id.clone(), "call_promise", args, MAX_GAS, Balance::ZERO)
        .unwrap();

    let err = match &res.status {
        FinalExecutionStatus::Failure(e) => e,
        other => panic!("expected Failure for drain + 2 yocto, got {other:?}"),
    };
    let err_str = format!("{err:?}");
    assert!(err_str.contains("balance"), "expected a balance-related error, got {err_str}");

    let subsidized_after = node.client.read().cumulative_subsidized;
    assert_eq!(
        subsidized_after, subsidized_before,
        "no subsidy must be granted when deduct_balance fails outright",
    );

    let reward = contract_function_call_reward(&fee_helper(&node), &res, "call_promise", args_len);
    let view = node.user().view_account(&zba_id).unwrap();
    let expected = balance_before.checked_add(reward).unwrap();
    assert_eq!(
        view.amount, expected,
        "zba balance must equal pre-call balance plus the function-call gas reward",
    );
}

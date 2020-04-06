use crate::runtime_group_tools::RuntimeGroup;
use borsh::ser::BorshSerialize;
use near_primitives::account::{AccessKeyPermission, FunctionCallPermission};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{ActionReceipt, ReceiptEnum};

pub mod runtime_group_tools;

/// Initial balance used in tests.
pub const TESTING_INIT_BALANCE: u128 = 1_000_000_000 * NEAR_BASE;

/// One NEAR, divisible by 10^24.
pub const NEAR_BASE: u128 = 1_000_000_000_000_000_000_000_000;

#[test]
fn test_simple_func_call() {
    let wasm_binary: &[u8] = include_bytes!("../../near-vm-runner/tests/res/test_contract_rs.wasm");
    let group = RuntimeGroup::new(2, 2, wasm_binary);
    let signer_sender = group.runtimes[0].lock().unwrap().signer.clone();
    let signer_receiver = group.runtimes[1].lock().unwrap().signer.clone();

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id.clone(),
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "sum_n".to_string(),
            args: 10u64.to_le_bytes().to_vec(),
            gas: 10u64.pow(19),
            deposit: 0,
        })],
        CryptoHash::default(),
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    assert_receipts!(group, signed_transaction => [r0]);
    assert_receipts!(group, "near_0" => r0 @ "near_1",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{..}), {}
                     => [ref1] );
    assert_refund!(group, ref1 @ "near_0");
}

// single promise, no callback (A->B)
#[test]
fn test_single_promise_no_callback() {
    let wasm_binary: &[u8] = include_bytes!("../../near-vm-runner/tests/res/test_contract_rs.wasm");
    let group = RuntimeGroup::new(3, 3, wasm_binary);
    let signer_sender = group.runtimes[0].lock().unwrap().signer.clone();
    let signer_receiver = group.runtimes[1].lock().unwrap().signer.clone();

    let data = serde_json::json!([
        {"create": {
        "account_id": "near_2",
        "method_name": "call_promise",
        "arguments": [],
        "amount": "0",
        "gas": 10u64.pow(17),
        }, "id": 0 }
    ]);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id.clone(),
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: 10u64.pow(18),
            deposit: 0,
        })],
        CryptoHash::default(),
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    assert_receipts!(group, signed_transaction => [r0]);
    assert_receipts!(group, "near_0" => r0 @ "near_1",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(18));
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, ref0] );
    assert_receipts!(group, "near_1" => r1 @ "near_2",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(17));
                        assert_eq!(*deposit, 0);
                     }
                     => [ref1]);
    assert_refund!(group, ref0 @ "near_0");
    assert_refund!(group, ref1 @ "near_0");
}

// single promise with callback (A->B=>C)
#[test]
fn test_single_promise_with_callback() {
    let wasm_binary: &[u8] = include_bytes!("../../near-vm-runner/tests/res/test_contract_rs.wasm");
    let group = RuntimeGroup::new(4, 4, wasm_binary);
    let signer_sender = group.runtimes[0].lock().unwrap().signer.clone();
    let signer_receiver = group.runtimes[1].lock().unwrap().signer.clone();

    let data = serde_json::json!([
        {"create": {
        "account_id": "near_2",
        "method_name": "call_promise",
        "arguments": [],
        "amount": "0",
        "gas": 10u64.pow(17),
        }, "id": 0 },
        {"then": {
        "promise_index": 0,
        "account_id": "near_3",
        "method_name": "call_promise",
        "arguments": [],
        "amount": "0",
        "gas": 10u64.pow(17),
        }, "id": 1}
    ]);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id.clone(),
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: 10u64.pow(18),
            deposit: 0,
        })],
        CryptoHash::default(),
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    assert_receipts!(group, signed_transaction => [r0]);
    assert_receipts!(group, "near_0" => r0 @ "near_1",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(18));
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, r2, ref0] );
    let data_id;
    assert_receipts!(group, "near_1" => r1 @ "near_2",
                     ReceiptEnum::Action(ActionReceipt{actions, output_data_receivers, ..}), {
                        assert_eq!(output_data_receivers.len(), 1);
                        data_id = output_data_receivers[0].data_id.clone();
                     },
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(17));
                        assert_eq!(*deposit, 0);
                     }
                     => [ref1]);
    assert_receipts!(group, "near_1" => r2 @ "near_3",
                     ReceiptEnum::Action(ActionReceipt{actions, input_data_ids, ..}), {
                        assert_eq!(input_data_ids.len(), 1);
                        assert_eq!(data_id, input_data_ids[0].clone());
                     },
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(17));
                        assert_eq!(*deposit, 0);
                     }
                     => [ref2]);

    assert_refund!(group, ref0 @ "near_0");
    assert_refund!(group, ref1 @ "near_0");
    assert_refund!(group, ref2 @ "near_0");
}

// two promises, no callbacks (A->B->C)
#[test]
fn test_two_promises_no_callbacks() {
    let wasm_binary: &[u8] = include_bytes!("../../near-vm-runner/tests/res/test_contract_rs.wasm");
    let group = RuntimeGroup::new(4, 4, wasm_binary);
    let signer_sender = group.runtimes[0].lock().unwrap().signer.clone();
    let signer_receiver = group.runtimes[1].lock().unwrap().signer.clone();

    let data = serde_json::json!([
        {"create": {
        "account_id": "near_2",
        "method_name": "call_promise",
        "arguments": [
            {"create": {
            "account_id": "near_3",
            "method_name": "call_promise",
            "arguments": [],
            "amount": "0",
            "gas": 10u64.pow(16),
            }, "id": 0}
        ],
        "amount": "0",
        "gas": 10u64.pow(17),
        }, "id": 0 },

    ]);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id.clone(),
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: 10u64.pow(18),
            deposit: 0,
        })],
        CryptoHash::default(),
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    assert_receipts!(group, signed_transaction => [r0]);
    assert_receipts!(group, "near_0" => r0 @ "near_1",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(18));
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, ref0] );
    assert_receipts!(group, "near_1" => r1 @ "near_2",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), { },
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(17));
                        assert_eq!(*deposit, 0);
                     }
                     => [r2, ref1]);
    assert_receipts!(group, "near_2" => r2 @ "near_3",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(16));
                        assert_eq!(*deposit, 0);
                     }
                     => [ref2]);

    assert_refund!(group, ref0 @ "near_0");
    assert_refund!(group, ref1 @ "near_0");
    assert_refund!(group, ref2 @ "near_0");
}

// two promises, with two callbacks (A->B->C=>D=>E) where call to E is initialized by completion of D.
#[test]
fn test_two_promises_with_two_callbacks() {
    let wasm_binary: &[u8] = include_bytes!("../../near-vm-runner/tests/res/test_contract_rs.wasm");
    let group = RuntimeGroup::new(6, 6, wasm_binary);
    let signer_sender = group.runtimes[0].lock().unwrap().signer.clone();
    let signer_receiver = group.runtimes[1].lock().unwrap().signer.clone();

    let data = serde_json::json!([
        {"create": {
        "account_id": "near_2",
        "method_name": "call_promise",
        "arguments": [
            {"create": {
            "account_id": "near_3",
            "method_name": "call_promise",
            "arguments": [],
            "amount": "0",
            "gas": 10u64.pow(16),
            }, "id": 0},

            {"then": {
            "promise_index": 0,
            "account_id": "near_4",
            "method_name": "call_promise",
            "arguments": [],
            "amount": "0",
            "gas": 10u64.pow(16),
            }, "id": 1}
        ],
        "amount": "0",
        "gas": 10u64.pow(17),
        }, "id": 0 },

        {"then": {
        "promise_index": 0,
        "account_id": "near_5",
        "method_name": "call_promise",
        "arguments": [],
        "amount": "0",
        "gas": 10u64.pow(17),
        }, "id": 1}
    ]);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id.clone(),
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: 10u64.pow(18),
            deposit: 0,
        })],
        CryptoHash::default(),
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    assert_receipts!(group, signed_transaction => [r0]);
    assert_receipts!(group, "near_0" => r0 @ "near_1",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(18));
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, cb1, ref0] );
    assert_receipts!(group, "near_1" => r1 @ "near_2",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), { },
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(17));
                        assert_eq!(*deposit, 0);
                     }
                     => [r2, cb2, ref1]);
    assert_receipts!(group, "near_2" => r2 @ "near_3",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(16));
                        assert_eq!(*deposit, 0);
                     }
                     => [ref2]);
    assert_receipts!(group, "near_2" => cb2 @ "near_4",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), { },
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(16));
                        assert_eq!(*deposit, 0);
                     }
                     => [ref3]);
    assert_receipts!(group, "near_1" => cb1 @ "near_5",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), { },
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(17));
                        assert_eq!(*deposit, 0);
                     }
                     => [ref4]);

    assert_refund!(group, ref0 @ "near_0");
    assert_refund!(group, ref1 @ "near_0");
    assert_refund!(group, ref2 @ "near_0");
    assert_refund!(group, ref3 @ "near_0");
    assert_refund!(group, ref4 @ "near_0");
}

// Batch actions tests

// single promise, no callback (A->B) with `promise_batch`
#[test]
fn test_single_promise_no_callback_batch() {
    let wasm_binary: &[u8] = include_bytes!("../../near-vm-runner/tests/res/test_contract_rs.wasm");
    let group = RuntimeGroup::new(3, 3, wasm_binary);
    let signer_sender = group.runtimes[0].lock().unwrap().signer.clone();
    let signer_receiver = group.runtimes[1].lock().unwrap().signer.clone();

    let data = serde_json::json!([
        {"batch_create": {
        "account_id": "near_2",
        }, "id": 0 },
        {"action_function_call": {
        "promise_index": 0,
        "method_name": "call_promise",
        "arguments": [],
        "amount": "0",
        "gas": 10u64.pow(17),
        }, "id": 0 }
    ]);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id.clone(),
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: 10u64.pow(18),
            deposit: 0,
        })],
        CryptoHash::default(),
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    assert_receipts!(group, signed_transaction => [r0]);
    assert_receipts!(group, "near_0" => r0 @ "near_1",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(18));
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, ref0] );
    assert_receipts!(group, "near_1" => r1 @ "near_2",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(17));
                        assert_eq!(*deposit, 0);
                     }
                     => [ref1]);
    assert_refund!(group, ref0 @ "near_0");
    assert_refund!(group, ref1 @ "near_0");
}

// single promise with callback (A->B=>C) with batch actions
#[test]
fn test_single_promise_with_callback_batch() {
    let wasm_binary: &[u8] = include_bytes!("../../near-vm-runner/tests/res/test_contract_rs.wasm");
    let group = RuntimeGroup::new(4, 4, wasm_binary);
    let signer_sender = group.runtimes[0].lock().unwrap().signer.clone();
    let signer_receiver = group.runtimes[1].lock().unwrap().signer.clone();

    let data = serde_json::json!([
        {"batch_create": {
            "account_id": "near_2",
        }, "id": 0 },
        {"action_function_call": {
            "promise_index": 0,
            "method_name": "call_promise",
            "arguments": [],
            "amount": "0",
            "gas": 10u64.pow(17),
        }, "id": 0 },
        {"batch_then": {
            "promise_index": 0,
            "account_id": "near_3",
        }, "id": 1},
        {"action_function_call": {
            "promise_index": 1,
            "method_name": "call_promise",
            "arguments": [],
            "amount": "0",
            "gas": 10u64.pow(17),
        }, "id": 1}
    ]);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id.clone(),
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: 10u64.pow(18),
            deposit: 0,
        })],
        CryptoHash::default(),
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    assert_receipts!(group, signed_transaction => [r0]);
    assert_receipts!(group, "near_0" => r0 @ "near_1",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(18));
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, r2, ref0] );
    let data_id;
    assert_receipts!(group, "near_1" => r1 @ "near_2",
                     ReceiptEnum::Action(ActionReceipt{actions, output_data_receivers, ..}), {
                        assert_eq!(output_data_receivers.len(), 1);
                        data_id = output_data_receivers[0].data_id.clone();
                     },
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(17));
                        assert_eq!(*deposit, 0);
                     }
                     => [ref1]);
    assert_receipts!(group, "near_1" => r2 @ "near_3",
                     ReceiptEnum::Action(ActionReceipt{actions, input_data_ids, ..}), {
                        assert_eq!(input_data_ids.len(), 1);
                        assert_eq!(data_id, input_data_ids[0].clone());
                     },
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(17));
                        assert_eq!(*deposit, 0);
                     }
                     => [ref2]);

    assert_refund!(group, ref0 @ "near_0");
    assert_refund!(group, ref1 @ "near_0");
    assert_refund!(group, ref2 @ "near_0");
}

#[test]
fn test_simple_transfer() {
    let wasm_binary: &[u8] = include_bytes!("../../near-vm-runner/tests/res/test_contract_rs.wasm");
    let group = RuntimeGroup::new(3, 3, wasm_binary);
    let signer_sender = group.runtimes[0].lock().unwrap().signer.clone();
    let signer_receiver = group.runtimes[1].lock().unwrap().signer.clone();

    let data = serde_json::json!([
        {"batch_create": {
            "account_id": "near_2",
        }, "id": 0 },
        {"action_transfer": {
            "promise_index": 0,
            "amount": "1000000000",
        }, "id": 0 }
    ]);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id.clone(),
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: 10u64.pow(18),
            deposit: 0,
        })],
        CryptoHash::default(),
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    assert_receipts!(group, signed_transaction => [r0]);
    assert_receipts!(group, "near_0" => r0 @ "near_1",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(18));
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, ref0] );
    assert_receipts!(group, "near_1" => r1 @ "near_2",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::Transfer(TransferAction{deposit}), {
                        assert_eq!(*deposit, 1000000000);
                     }
                     => [] );

    assert_refund!(group, ref0 @ "near_0");
}

#[test]
fn test_create_account_with_transfer_and_full_key() {
    let wasm_binary: &[u8] = include_bytes!("../../near-vm-runner/tests/res/test_contract_rs.wasm");
    let group = RuntimeGroup::new(3, 2, wasm_binary);
    let signer_sender = group.runtimes[0].lock().unwrap().signer.clone();
    let signer_receiver = group.runtimes[1].lock().unwrap().signer.clone();
    let signer_new_account = group.runtimes[2].lock().unwrap().signer.clone();

    let data = serde_json::json!([
        {"batch_create": {
            "account_id": "near_2",
        }, "id": 0 },
        {"action_create_account": {
            "promise_index": 0,
        }, "id": 0 },
        {"action_transfer": {
            "promise_index": 0,
            "amount": "10000000000000000000000000",
        }, "id": 0 },
        {"action_add_key_with_full_access": {
            "promise_index": 0,
            "public_key": base64::encode(&signer_new_account.public_key.try_to_vec().unwrap()),
            "nonce": 0,
        }, "id": 0 }
    ]);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id.clone(),
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: 10u64.pow(18),
            deposit: 0,
        })],
        CryptoHash::default(),
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    assert_receipts!(group, signed_transaction => [r0]);
    assert_receipts!(group, "near_0" => r0 @ "near_1",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(18));
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, ref0] );
    assert_receipts!(group, "near_1" => r1 @ "near_2",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::CreateAccount(CreateAccountAction{}), {},
                     a1, Action::Transfer(TransferAction{deposit}), {
                        assert_eq!(*deposit, 10000000000000000000000000);
                     },
                     a2, Action::AddKey(AddKeyAction{public_key, access_key}), {
                        assert_eq!(public_key, &signer_new_account.public_key);
                        assert_eq!(access_key.nonce, 0);
                        assert_eq!(access_key.permission, AccessKeyPermission::FullAccess);
                     }
                     => [] );

    assert_refund!(group, ref0 @ "near_0");
}

#[test]
fn test_account_factory() {
    let wasm_binary: &[u8] = include_bytes!("../../near-vm-runner/tests/res/test_contract_rs.wasm");
    let group = RuntimeGroup::new(3, 2, wasm_binary);
    let signer_sender = group.runtimes[0].lock().unwrap().signer.clone();
    let signer_receiver = group.runtimes[1].lock().unwrap().signer.clone();
    let signer_new_account = group.runtimes[2].lock().unwrap().signer.clone();

    let data = serde_json::json!([
        {"batch_create": {
            "account_id": "near_2",
        }, "id": 0 },
        {"action_create_account": {
            "promise_index": 0,
        }, "id": 0 },
        {"action_transfer": {
            "promise_index": 0,
            "amount": format!("{}", TESTING_INIT_BALANCE / 2),
        }, "id": 0 },
        {"action_add_key_with_function_call": {
            "promise_index": 0,
            "public_key": base64::encode(&signer_new_account.public_key.try_to_vec().unwrap()),
            "nonce": 0,
            "allowance": format!("{}", TESTING_INIT_BALANCE / 2),
            "receiver_id": "near_1",
            "method_names": "call_promise,hello"
        }, "id": 0 },
        {"action_deploy_contract": {
            "promise_index": 0,
            "code": base64::encode(wasm_binary),
        }, "id": 0 },
        {"action_function_call": {
            "promise_index": 0,
            "method_name": "call_promise",
            "arguments": [
                {"create": {
                "account_id": "near_0",
                "method_name": "call_promise",
                "arguments": [],
                "amount": "0",
                "gas": 10u64.pow(16),
                }, "id": 0}
            ],
            "amount": "0",
            "gas": 10u64.pow(17),
        }, "id": 0 },

        {"then": {
        "promise_index": 0,
        "account_id": "near_2",
        "method_name": "call_promise",
        "arguments": [
            {"create": {
            "account_id": "near_1",
            "method_name": "call_promise",
            "arguments": [],
            "amount": "0",
            "gas": 10u64.pow(16),
            }, "id": 0}
        ],
        "amount": "0",
        "gas": 10u64.pow(17),
        }, "id": 1}
    ]);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id.clone(),
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: 10u64.pow(18),
            deposit: 0,
        })],
        CryptoHash::default(),
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    assert_receipts!(group, signed_transaction => [r0]);
    assert_receipts!(group, "near_0" => r0 @ "near_1",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(18));
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, r2, ref0] );
    let data_id;
    assert_receipts!(group, "near_1" => r1 @ "near_2",
                     ReceiptEnum::Action(ActionReceipt{actions, output_data_receivers, ..}), {
                        assert_eq!(output_data_receivers.len(), 1);
                        data_id = output_data_receivers[0].data_id.clone();
                        assert_eq!(&output_data_receivers[0].receiver_id, "near_2");
                     },
                     actions,
                     a0, Action::CreateAccount(CreateAccountAction{}), {},
                     a1, Action::Transfer(TransferAction{deposit}), {
                        assert_eq!(*deposit, TESTING_INIT_BALANCE / 2);
                     },
                     a2, Action::AddKey(AddKeyAction{public_key, access_key}), {
                        assert_eq!(public_key, &signer_new_account.public_key);
                        assert_eq!(access_key.nonce, 0);
                        assert_eq!(access_key.permission, AccessKeyPermission::FunctionCall(FunctionCallPermission {
                            allowance: Some(TESTING_INIT_BALANCE / 2),
                            receiver_id: "near_1".to_string(),
                            method_names: vec!["call_promise".to_string(), "hello".to_string()],
                        }));
                     },
                     a3, Action::DeployContract(DeployContractAction{code}), {
                        assert_eq!(code, &wasm_binary);
                     },
                     a4, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(17));
                        assert_eq!(*deposit, 0);
                     }
                     => [r3, ref1] );
    assert_receipts!(group, "near_1" => r2 @ "near_2",
                     ReceiptEnum::Action(ActionReceipt{actions, input_data_ids, ..}), {
                        assert_eq!(input_data_ids, &vec![data_id]);
                     },
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(17));
                        assert_eq!(*deposit, 0);
                     }
                     => [r4, ref2] );
    assert_receipts!(group, "near_2" => r3 @ "near_0",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(16));
                        assert_eq!(*deposit, 0);
                     }
                     => [ref3] );
    assert_receipts!(group, "near_2" => r4 @ "near_1",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(16));
                        assert_eq!(*deposit, 0);
                     }
                     => [ref4] );

    assert_refund!(group, ref0 @ "near_0");
    assert_refund!(group, ref1 @ "near_0");
    assert_refund!(group, ref2 @ "near_0");
    assert_refund!(group, ref3 @ "near_0");
    assert_refund!(group, ref4 @ "near_0");
}

#[test]
fn test_create_account_add_key_call_delete_key_delete_account() {
    let wasm_binary: &[u8] = include_bytes!("../../near-vm-runner/tests/res/test_contract_rs.wasm");
    let group = RuntimeGroup::new(4, 3, wasm_binary);
    let signer_sender = group.runtimes[0].lock().unwrap().signer.clone();
    let signer_receiver = group.runtimes[1].lock().unwrap().signer.clone();
    let signer_new_account = group.runtimes[2].lock().unwrap().signer.clone();

    let data = serde_json::json!([
        {"batch_create": {
            "account_id": "near_3",
        }, "id": 0 },
        {"action_create_account": {
            "promise_index": 0,
        }, "id": 0 },
        {"action_transfer": {
            "promise_index": 0,
            "amount": format!("{}", TESTING_INIT_BALANCE / 2),
        }, "id": 0 },
        {"action_add_key_with_full_access": {
            "promise_index": 0,
            "public_key": base64::encode(&signer_new_account.public_key.try_to_vec().unwrap()),
            "nonce": 1,
        }, "id": 0 },
        {"action_deploy_contract": {
            "promise_index": 0,
            "code": base64::encode(wasm_binary),
        }, "id": 0 },
        {"action_function_call": {
            "promise_index": 0,
            "method_name": "call_promise",
            "arguments": [
                {"create": {
                "account_id": "near_0",
                "method_name": "call_promise",
                "arguments": [],
                "amount": "0",
                "gas": 10u64.pow(16),
                }, "id": 0}
            ],
            "amount": "0",
            "gas": 10u64.pow(17),
        }, "id": 0 },
        {"action_delete_key": {
            "promise_index": 0,
            "public_key": base64::encode(&signer_new_account.public_key.try_to_vec().unwrap()),
            "nonce": 0,
        }, "id": 0 },
        {"action_delete_account": {
            "promise_index": 0,
            "beneficiary_id": "near_2"
        }, "id": 0 },
    ]);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id.clone(),
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: 10u64.pow(18),
            deposit: 0,
        })],
        CryptoHash::default(),
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    assert_receipts!(group, signed_transaction => [r0]);
    assert_receipts!(group, "near_0" => r0 @ "near_1",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(18));
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, ref0] );
    assert_receipts!(group, "near_1" => r1 @ "near_3",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::CreateAccount(CreateAccountAction{}), {},
                     a1, Action::Transfer(TransferAction{deposit}), {
                        assert_eq!(*deposit, TESTING_INIT_BALANCE / 2);
                     },
                     a2, Action::AddKey(AddKeyAction{public_key, access_key}), {
                        assert_eq!(public_key, &signer_new_account.public_key);
                        assert_eq!(access_key.nonce, 1);
                        assert_eq!(access_key.permission, AccessKeyPermission::FullAccess);
                     },
                     a3, Action::DeployContract(DeployContractAction{code}), {
                        assert_eq!(code, &wasm_binary);
                     },
                     a4, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(17));
                        assert_eq!(*deposit, 0);
                     },
                     a5, Action::DeleteKey(DeleteKeyAction{public_key}), {
                        assert_eq!(public_key, &signer_new_account.public_key);
                     },
                     a6, Action::DeleteAccount(DeleteAccountAction{beneficiary_id}), {
                        assert_eq!(beneficiary_id.as_str(), "near_2");
                     }
                     => [r2, ref1, ref2] );
    assert_receipts!(group, "near_3" => r2 @ "near_0",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10u64.pow(16));
                        assert_eq!(*deposit, 0);
                     }
                     => [ref3] );

    assert_refund!(group, ref0 @ "near_0");
    assert_refund!(group, ref1 @ "near_2");
    assert_refund!(group, ref2 @ "near_0");
    assert_refund!(group, ref3 @ "near_0");
}

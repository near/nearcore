use crate::runtime_group_tools::RuntimeGroup;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{ActionReceipt, ReceiptEnum};

pub mod runtime_group_tools;

#[test]
fn test_simple_func_call() {
    let wasm_binary: &[u8] = include_bytes!("../../near-vm-runner/tests/res/test_contract_rs.wasm");
    let group = RuntimeGroup::new(2, wasm_binary);
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
            gas: 1_000_000,
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
    assert_receipts!(group, "near.0" => r0 @ "near.1",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{..}), {}
                     => [ref1] );
    assert_refund!(group, ref1 @ "near.0");
}

// single promise, no callback (A->B)
#[test]
fn test_single_promise_no_callback() {
    let wasm_binary: &[u8] = include_bytes!("../../near-vm-runner/tests/res/test_contract_rs.wasm");
    let group = RuntimeGroup::new(3, wasm_binary);
    let signer_sender = group.runtimes[0].lock().unwrap().signer.clone();
    let signer_receiver = group.runtimes[1].lock().unwrap().signer.clone();

    let data = serde_json::json!([
        {"create": {
        "account_id": "near.2",
        "method_name": "call_promise",
        "arguments": [],
        "amount": 0,
        "gas": 300_000,
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
            gas: 1_000_000,
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
    assert_receipts!(group, "near.0" => r0 @ "near.1",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 1_000_000);
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, ref0] );
    assert_receipts!(group, "near.1" => r1 @ "near.2",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 300_000);
                        assert_eq!(*deposit, 0);
                     }
                     => [ref1]);
    assert_refund!(group, ref0 @ "near.0");
    assert_refund!(group, ref1 @ "near.0");
}

// single promise with callback (A->B=>C)
#[test]
fn test_single_promise_with_callback() {
    let wasm_binary: &[u8] = include_bytes!("../../near-vm-runner/tests/res/test_contract_rs.wasm");
    let group = RuntimeGroup::new(4, wasm_binary);
    let signer_sender = group.runtimes[0].lock().unwrap().signer.clone();
    let signer_receiver = group.runtimes[1].lock().unwrap().signer.clone();

    let data = serde_json::json!([
        {"create": {
        "account_id": "near.2",
        "method_name": "call_promise",
        "arguments": [],
        "amount": 0,
        "gas": 300_000,
        }, "id": 0 },
        {"then": {
        "promise_index": 0,
        "account_id": "near.3",
        "method_name": "call_promise",
        "arguments": [],
        "amount": 0,
        "gas": 300_000,
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
            gas: 1_000_000,
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
    assert_receipts!(group, "near.0" => r0 @ "near.1",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 1_000_000);
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, r2, ref0] );
    let data_id;
    assert_receipts!(group, "near.1" => r1 @ "near.2",
                     ReceiptEnum::Action(ActionReceipt{actions, output_data_receivers, ..}), {
                        assert_eq!(output_data_receivers.len(), 1);
                        data_id = output_data_receivers[0].data_id.clone();
                     },
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 300_000);
                        assert_eq!(*deposit, 0);
                     }
                     => [ref1]);
    assert_receipts!(group, "near.1" => r2 @ "near.3",
                     ReceiptEnum::Action(ActionReceipt{actions, input_data_ids, ..}), {
                        assert_eq!(input_data_ids.len(), 1);
                        assert_eq!(data_id, input_data_ids[0].clone());
                     },
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 300_000);
                        assert_eq!(*deposit, 0);
                     }
                     => [ref2]);

    assert_refund!(group, ref0 @ "near.0");
    assert_refund!(group, ref1 @ "near.0");
    assert_refund!(group, ref2 @ "near.0");
}

// two promises, no callbacks (A->B->C)
#[test]
fn test_two_promises_no_callbacks() {
    let wasm_binary: &[u8] = include_bytes!("../../near-vm-runner/tests/res/test_contract_rs.wasm");
    let group = RuntimeGroup::new(4, wasm_binary);
    let signer_sender = group.runtimes[0].lock().unwrap().signer.clone();
    let signer_receiver = group.runtimes[1].lock().unwrap().signer.clone();

    let data = serde_json::json!([
        {"create": {
        "account_id": "near.2",
        "method_name": "call_promise",
        "arguments": [
            {"create": {
            "account_id": "near.3",
            "method_name": "call_promise",
            "arguments": [],
            "amount": 0,
            "gas": 300_000,
            }, "id": 0}
        ],
        "amount": 0,
        "gas": 600_000,
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
            gas: 1_000_000,
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
    assert_receipts!(group, "near.0" => r0 @ "near.1",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 1_000_000);
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, ref0] );
    assert_receipts!(group, "near.1" => r1 @ "near.2",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), { },
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 600_000);
                        assert_eq!(*deposit, 0);
                     }
                     => [r2, ref1]);
    assert_receipts!(group, "near.2" => r2 @ "near.3",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 300_000);
                        assert_eq!(*deposit, 0);
                     }
                     => [ref2]);

    assert_refund!(group, ref0 @ "near.0");
    assert_refund!(group, ref1 @ "near.0");
    assert_refund!(group, ref2 @ "near.0");
}

// two promises, with two callbacks (A->B->C=>D=>E) where call to E is initialized by completion of D.
#[test]
fn test_two_promises_with_two_callbacks() {
    let wasm_binary: &[u8] = include_bytes!("../../near-vm-runner/tests/res/test_contract_rs.wasm");
    let group = RuntimeGroup::new(6, wasm_binary);
    let signer_sender = group.runtimes[0].lock().unwrap().signer.clone();
    let signer_receiver = group.runtimes[1].lock().unwrap().signer.clone();

    let data = serde_json::json!([
        {"create": {
        "account_id": "near.2",
        "method_name": "call_promise",
        "arguments": [
            {"create": {
            "account_id": "near.3",
            "method_name": "call_promise",
            "arguments": [],
            "amount": 0,
            "gas": 1_000_000,
            }, "id": 0},

            {"then": {
            "promise_index": 0,
            "account_id": "near.4",
            "method_name": "call_promise",
            "arguments": [],
            "amount": 0,
            "gas": 1_000_000,
            }, "id": 1}
        ],
        "amount": 0,
        "gas": 3_000_000,
        }, "id": 0 },

        {"then": {
        "promise_index": 0,
        "account_id": "near.5",
        "method_name": "call_promise",
        "arguments": [],
        "amount": 0,
        "gas": 3_000_000,
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
            gas: 10_000_000,
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
    assert_receipts!(group, "near.0" => r0 @ "near.1",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 10_000_000);
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, cb1, ref0] );
    assert_receipts!(group, "near.1" => r1 @ "near.2",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), { },
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 3_000_000);
                        assert_eq!(*deposit, 0);
                     }
                     => [r2, cb2, ref1]);
    assert_receipts!(group, "near.2" => r2 @ "near.3",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 1_000_000);
                        assert_eq!(*deposit, 0);
                     }
                     => [ref2]);
    assert_receipts!(group, "near.2" => cb2 @ "near.4",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), { },
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 1_000_000);
                        assert_eq!(*deposit, 0);
                     }
                     => [ref3]);
    assert_receipts!(group, "near.1" => cb1 @ "near.5",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), { },
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, 3_000_000);
                        assert_eq!(*deposit, 0);
                     }
                     => [ref4]);

    assert_refund!(group, ref0 @ "near.0");
    assert_refund!(group, ref1 @ "near.0");
    assert_refund!(group, ref2 @ "near.0");
    assert_refund!(group, ref3 @ "near.0");
    assert_refund!(group, ref4 @ "near.0");
}

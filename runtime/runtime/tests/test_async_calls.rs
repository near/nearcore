use crate::runtime_group_tools::RuntimeGroup;
use borsh::ser::BorshSerialize;
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::account::{AccessKeyPermission, FunctionCallPermission};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{ActionReceipt, ReceiptEnum};
use near_primitives::serialize::to_base64;
use near_primitives::types::AccountId;

pub mod runtime_group_tools;

/// Initial balance used in tests.
pub const TESTING_INIT_BALANCE: u128 = 1_000_000_000 * NEAR_BASE;

/// One NEAR, divisible by 10^24.
pub const NEAR_BASE: u128 = 1_000_000_000_000_000_000_000_000;

const GAS_1: u64 = 900_000_000_000_000;
const GAS_2: u64 = GAS_1 / 3;
const GAS_3: u64 = GAS_2 / 3;

#[test]
fn test_simple_func_call() {
    let group = RuntimeGroup::new(2, 2, near_test_contracts::rs_contract());
    let signer_sender = group.signers[0].clone();
    let signer_receiver = group.signers[1].clone();

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id,
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "sum_n".to_string(),
            args: 10u64.to_le_bytes().to_vec(),
            gas: GAS_1,
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
    let group = RuntimeGroup::new(3, 3, near_test_contracts::rs_contract());
    let signer_sender = group.signers[0].clone();
    let signer_receiver = group.signers[1].clone();

    let data = serde_json::json!([
        {"create": {
        "account_id": "near_2",
        "method_name": "call_promise",
        "arguments": [],
        "amount": "0",
        "gas": GAS_2,
        }, "id": 0 }
    ]);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id,
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
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
                        assert_eq!(*gas, GAS_1);
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, ref0] );
    assert_receipts!(group, "near_1" => r1 @ "near_2",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, GAS_2);
                        assert_eq!(*deposit, 0);
                     }
                     => [ref1]);
    assert_refund!(group, ref0 @ "near_0");
    assert_refund!(group, ref1 @ "near_0");
}

// single promise with callback (A->B=>C)
#[test]
fn test_single_promise_with_callback() {
    let group = RuntimeGroup::new(4, 4, near_test_contracts::rs_contract());
    let signer_sender = group.signers[0].clone();
    let signer_receiver = group.signers[1].clone();

    let data = serde_json::json!([
        {"create": {
        "account_id": "near_2",
        "method_name": "call_promise",
        "arguments": [],
        "amount": "0",
        "gas": GAS_2,
        }, "id": 0 },
        {"then": {
        "promise_index": 0,
        "account_id": "near_3",
        "method_name": "call_promise",
        "arguments": [],
        "amount": "0",
        "gas": GAS_2,
        }, "id": 1}
    ]);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id,
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
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
                        assert_eq!(*gas, GAS_1);
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, r2, ref0] );
    let data_id;
    assert_receipts!(group, "near_1" => r1 @ "near_2",
                     ReceiptEnum::Action(ActionReceipt{actions, output_data_receivers, ..}), {
                        assert_eq!(output_data_receivers.len(), 1);
                        data_id = output_data_receivers[0].data_id;
                     },
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, GAS_2);
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
                        assert_eq!(*gas, GAS_2);
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
    let group = RuntimeGroup::new(4, 4, near_test_contracts::rs_contract());
    let signer_sender = group.signers[0].clone();
    let signer_receiver = group.signers[1].clone();

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
            "gas": GAS_3,
            }, "id": 0}
        ],
        "amount": "0",
        "gas": GAS_2,
        }, "id": 0 },

    ]);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id,
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
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
                        assert_eq!(*gas, GAS_1);
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, ref0] );
    assert_receipts!(group, "near_1" => r1 @ "near_2",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), { },
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, GAS_2);
                        assert_eq!(*deposit, 0);
                     }
                     => [r2, ref1]);
    assert_receipts!(group, "near_2" => r2 @ "near_3",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, GAS_3);
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
    let group = RuntimeGroup::new(6, 6, near_test_contracts::rs_contract());
    let signer_sender = group.signers[0].clone();
    let signer_receiver = group.signers[1].clone();

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
            "gas": GAS_3,
            }, "id": 0},

            {"then": {
            "promise_index": 0,
            "account_id": "near_4",
            "method_name": "call_promise",
            "arguments": [],
            "amount": "0",
            "gas": GAS_3,
            }, "id": 1}
        ],
        "amount": "0",
        "gas": GAS_2,
        }, "id": 0 },

        {"then": {
        "promise_index": 0,
        "account_id": "near_5",
        "method_name": "call_promise",
        "arguments": [],
        "amount": "0",
        "gas": GAS_2,
        }, "id": 1}
    ]);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id,
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
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
                        assert_eq!(*gas, GAS_1);
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, cb1, ref0] );
    assert_receipts!(group, "near_1" => r1 @ "near_2",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), { },
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, GAS_2);
                        assert_eq!(*deposit, 0);
                     }
                     => [r2, cb2, ref1]);
    assert_receipts!(group, "near_2" => r2 @ "near_3",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, GAS_3);
                        assert_eq!(*deposit, 0);
                     }
                     => [ref2]);
    assert_receipts!(group, "near_2" => cb2 @ "near_4",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), { },
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, GAS_3);
                        assert_eq!(*deposit, 0);
                     }
                     => [ref3]);
    assert_receipts!(group, "near_1" => cb1 @ "near_5",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), { },
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, GAS_2);
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
    let group = RuntimeGroup::new(3, 3, near_test_contracts::rs_contract());
    let signer_sender = group.signers[0].clone();
    let signer_receiver = group.signers[1].clone();

    let data = serde_json::json!([
        {"batch_create": {
        "account_id": "near_2",
        }, "id": 0 },
        {"action_function_call": {
        "promise_index": 0,
        "method_name": "call_promise",
        "arguments": [],
        "amount": "0",
        "gas": GAS_2,
        }, "id": 0 }
    ]);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id,
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
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
                        assert_eq!(*gas, GAS_1);
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, ref0] );
    assert_receipts!(group, "near_1" => r1 @ "near_2",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, GAS_2);
                        assert_eq!(*deposit, 0);
                     }
                     => [ref1]);
    assert_refund!(group, ref0 @ "near_0");
    assert_refund!(group, ref1 @ "near_0");
}

// single promise with callback (A->B=>C) with batch actions
#[test]
fn test_single_promise_with_callback_batch() {
    let group = RuntimeGroup::new(4, 4, near_test_contracts::rs_contract());
    let signer_sender = group.signers[0].clone();
    let signer_receiver = group.signers[1].clone();

    let data = serde_json::json!([
        {"batch_create": {
            "account_id": "near_2",
        }, "id": 0 },
        {"action_function_call": {
            "promise_index": 0,
            "method_name": "call_promise",
            "arguments": [],
            "amount": "0",
            "gas": GAS_2,
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
            "gas": GAS_2,
        }, "id": 1}
    ]);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id,
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
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
                        assert_eq!(*gas, GAS_1);
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, r2, ref0] );
    let data_id;
    assert_receipts!(group, "near_1" => r1 @ "near_2",
                     ReceiptEnum::Action(ActionReceipt{actions, output_data_receivers, ..}), {
                        assert_eq!(output_data_receivers.len(), 1);
                        data_id = output_data_receivers[0].data_id;
                     },
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, GAS_2);
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
                        assert_eq!(*gas, GAS_2);
                        assert_eq!(*deposit, 0);
                     }
                     => [ref2]);

    assert_refund!(group, ref0 @ "near_0");
    assert_refund!(group, ref1 @ "near_0");
    assert_refund!(group, ref2 @ "near_0");
}

#[test]
fn test_simple_transfer() {
    let group = RuntimeGroup::new(3, 3, near_test_contracts::rs_contract());
    let signer_sender = group.signers[0].clone();
    let signer_receiver = group.signers[1].clone();

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
        signer_receiver.account_id,
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
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
                        assert_eq!(*gas, GAS_1);
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, ref0] );
    assert_receipts!(group, "near_1" => r1 @ "near_2",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::Transfer(TransferAction{deposit}), {
                        assert_eq!(*deposit, 1000000000);
                     }
                     => [ref1] );

    assert_refund!(group, ref0 @ "near_0");
    // For gas price difference
    assert_refund!(group, ref1 @ "near_0");
}

#[test]
fn test_create_account_with_transfer_and_full_key() {
    let group = RuntimeGroup::new(3, 2, near_test_contracts::rs_contract());
    let signer_sender = group.signers[0].clone();
    let signer_receiver = group.signers[1].clone();
    let signer_new_account = group.signers[2].clone();

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
            "public_key": to_base64(&signer_new_account.public_key.try_to_vec().unwrap()),
            "nonce": 0,
        }, "id": 0 }
    ]);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id,
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
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
                        assert_eq!(*gas, GAS_1);
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
                     => [ref1] );

    assert_refund!(group, ref0 @ "near_0");
    // For gas price difference
    assert_refund!(group, ref1 @ "near_0");
}

#[test]
fn test_account_factory() {
    let group = RuntimeGroup::new(3, 2, near_test_contracts::rs_contract());
    let signer_sender = group.signers[0].clone();
    let signer_receiver = group.signers[1].clone();
    let signer_new_account = group.signers[2].clone();

    let data = serde_json::json!([
        {"batch_create": {
            "account_id": "near_2",
        }, "id": 0 },
        {"action_create_account": {
            "promise_index": 0,
        }, "id": 0 },
        {"action_transfer": {
            "promise_index": 0,
            "amount": (TESTING_INIT_BALANCE / 2).to_string(),
        }, "id": 0 },
        {"action_add_key_with_function_call": {
            "promise_index": 0,
            "public_key": to_base64(&signer_new_account.public_key.try_to_vec().unwrap()),
            "nonce": 0,
            "allowance": (TESTING_INIT_BALANCE / 2).to_string(),
            "receiver_id": "near_1",
            "method_names": "call_promise,hello"
        }, "id": 0 },
        {"action_deploy_contract": {
            "promise_index": 0,
            "code": to_base64(near_test_contracts::rs_contract()),
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
                "gas": GAS_3,
                }, "id": 0}
            ],
            "amount": "0",
            "gas": GAS_2,
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
            "gas": GAS_3,
            }, "id": 0}
        ],
        "amount": "0",
        "gas": GAS_2,
        }, "id": 1}
    ]);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id,
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
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
                        assert_eq!(*gas, GAS_1);
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, r2, ref0] );

    let data_id;
    assert_receipts!(group, "near_1" => r1 @ "near_2",
                     ReceiptEnum::Action(ActionReceipt{actions, output_data_receivers, ..}), {
                        assert_eq!(output_data_receivers.len(), 1);
                        data_id = output_data_receivers[0].data_id;
                        assert_eq!(output_data_receivers[0].receiver_id.as_ref(), "near_2");
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
                            receiver_id: "near_1".parse().unwrap(),
                            method_names: vec!["call_promise".to_string(), "hello".to_string()],
                        }));
                     },
                     a3, Action::DeployContract(DeployContractAction{code}), {
                        assert_eq!(code, near_test_contracts::rs_contract());
                     },
                     a4, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, GAS_2);
                        assert_eq!(*deposit, 0);
                     }
                     => [r3, ref1] );
    assert_receipts!(group, "near_1" => r2 @ "near_2",
                     ReceiptEnum::Action(ActionReceipt{actions, input_data_ids, ..}), {
                        assert_eq!(input_data_ids, &[data_id]);
                     },
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, GAS_2);
                        assert_eq!(*deposit, 0);
                     }
                     => [r4, ref2] );
    assert_receipts!(group, "near_2" => r3 @ "near_0",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, GAS_3);
                        assert_eq!(*deposit, 0);
                     }
                     => [ref3] );
    assert_receipts!(group, "near_2" => r4 @ "near_1",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, GAS_3);
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
    let group = RuntimeGroup::new(4, 3, near_test_contracts::rs_contract());
    let signer_sender = group.signers[0].clone();
    let signer_receiver = group.signers[1].clone();
    let signer_new_account = group.signers[2].clone();

    let data = serde_json::json!([
        {"batch_create": {
            "account_id": "near_3",
        }, "id": 0 },
        {"action_create_account": {
            "promise_index": 0,
        }, "id": 0 },
        {"action_transfer": {
            "promise_index": 0,
            "amount": (TESTING_INIT_BALANCE / 2).to_string(),
        }, "id": 0 },
        {"action_add_key_with_full_access": {
            "promise_index": 0,
            "public_key": to_base64(&signer_new_account.public_key.try_to_vec().unwrap()),
            "nonce": 1,
        }, "id": 0 },
        {"action_deploy_contract": {
            "promise_index": 0,
            "code": to_base64(near_test_contracts::rs_contract()),
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
                "gas": GAS_3,
                }, "id": 0}
            ],
            "amount": "0",
            "gas": GAS_2,
        }, "id": 0 },
        {"action_delete_key": {
            "promise_index": 0,
            "public_key": to_base64(&signer_new_account.public_key.try_to_vec().unwrap()),
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
        signer_receiver.account_id,
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
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
                        assert_eq!(*gas, GAS_1);
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
                        assert_eq!(code, near_test_contracts::rs_contract());
                     },
                     a4, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, GAS_2);
                        assert_eq!(*deposit, 0);
                     },
                     a5, Action::DeleteKey(DeleteKeyAction{public_key}), {
                        assert_eq!(public_key, &signer_new_account.public_key);
                     },
                     a6, Action::DeleteAccount(DeleteAccountAction{beneficiary_id}), {
                        assert_eq!(beneficiary_id.as_ref(), "near_2");
                     }
                     => [r2, r3, ref1] );

    assert_receipts!(group, "near_3" => r2 @ "near_0",
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::FunctionCall(FunctionCallAction{gas, deposit, ..}), {
                        assert_eq!(*gas, GAS_3);
                        assert_eq!(*deposit, 0);
                     }
                     => [ref2] );
    assert_refund!(group, r3 @ "near_2");

    assert_refund!(group, ref0 @ "near_0");
    assert_refund!(group, ref1 @ "near_0");
    assert_refund!(group, ref2 @ "near_0");
}

#[test]
fn test_transfer_64len_hex() {
    let pk = InMemorySigner::from_seed("test_hex".parse().unwrap(), KeyType::ED25519, "test_hex");
    let account_id = AccountId::try_from(hex::encode(pk.public_key.unwrap_as_ed25519().0)).unwrap();

    let group = RuntimeGroup::new_with_account_ids(
        vec!["near_0".parse().unwrap(), "near_1".parse().unwrap(), account_id.clone()],
        2,
        near_test_contracts::rs_contract(),
    );
    let signer_sender = group.signers[0].clone();
    let signer_receiver = group.signers[1].clone();

    let data = serde_json::json!([
        {"batch_create": {
            "account_id": account_id,
        }, "id": 0 },
        {"action_transfer": {
            "promise_index": 0,
            "amount": (TESTING_INIT_BALANCE / 2).to_string(),
        }, "id": 0 },
    ]);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id,
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
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
                        assert_eq!(*gas, GAS_1);
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, ref0] );
    assert_receipts!(group, "near_1" => r1 @ account_id.as_ref(),
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::Transfer(TransferAction{deposit}), {
                        assert_eq!(*deposit, TESTING_INIT_BALANCE / 2);
                     }
                     => [ref1] );
    assert_refund!(group, ref0 @ "near_0");
    assert_refund!(group, ref1 @ "near_0");
}

#[test]
fn test_create_transfer_64len_hex_fail() {
    let pk = InMemorySigner::from_seed("test_hex".parse().unwrap(), KeyType::ED25519, "test_hex");
    let account_id = AccountId::try_from(hex::encode(pk.public_key.unwrap_as_ed25519().0)).unwrap();

    let group = RuntimeGroup::new_with_account_ids(
        vec!["near_0".parse().unwrap(), "near_1".parse().unwrap(), account_id.clone()],
        2,
        near_test_contracts::rs_contract(),
    );
    let signer_sender = group.signers[0].clone();
    let signer_receiver = group.signers[1].clone();

    let data = serde_json::json!([
        {"batch_create": {
            "account_id": account_id,
        }, "id": 0 },
        {"action_create_account": {
            "promise_index": 0,
        }, "id": 0 },
        {"action_transfer": {
            "promise_index": 0,
            "amount": (TESTING_INIT_BALANCE / 2).to_string(),
        }, "id": 0 },
    ]);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id,
        &signer_sender,
        vec![Action::FunctionCall(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
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
                        assert_eq!(*gas, GAS_1);
                        assert_eq!(*deposit, 0);
                     }
                     => [r1, ref0] );
    assert_receipts!(group, "near_1" => r1 @ account_id.as_ref(),
                     ReceiptEnum::Action(ActionReceipt{actions, ..}), {},
                     actions,
                     a0, Action::CreateAccount(CreateAccountAction{}), {},
                     a1, Action::Transfer(TransferAction{deposit}), {
                        assert_eq!(*deposit, TESTING_INIT_BALANCE / 2);
                     }
                     => [ref1, ref2] );
    assert_refund!(group, ref0 @ "near_0");
    assert_refund!(group, ref1 @ "near_1");
    assert_refund!(group, ref2 @ "near_0");
}

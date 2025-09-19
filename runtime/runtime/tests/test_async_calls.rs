use crate::runtime_group_tools::RuntimeGroup;

use near_crypto::InMemorySigner;
use near_primitives::account::{AccessKeyPermission, FunctionCallPermission};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{ActionReceipt, ActionReceiptV2, ReceiptEnum};
use near_primitives::serialize::to_base64;
use near_primitives::types::AccountId;
use near_primitives::types::Gas;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};

pub mod runtime_group_tools;

/// Initial balance used in tests.
pub const TESTING_INIT_BALANCE: u128 = 1_000_000_000 * NEAR_BASE;

/// One NEAR, divisible by 10^24.
pub const NEAR_BASE: u128 = 1_000_000_000_000_000_000_000_000;

const GAS_1: Gas = Gas::from_teragas(900);
const GAS_2: Gas = GAS_1.checked_div(3).unwrap();
const GAS_3: Gas = GAS_2.checked_div(3).unwrap();

#[test]
fn test_simple_func_call() {
    let group = RuntimeGroup::new(2, 2, near_test_contracts::rs_contract());
    let signer_sender = group.signers[0].clone();
    let signer_receiver = group.signers[1].clone();

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.get_account_id(),
        signer_receiver.get_account_id(),
        &signer_sender,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "sum_n".to_string(),
            args: 10u64.to_le_bytes().to_vec(),
            gas: GAS_1,
            deposit: 0,
        }))],
        CryptoHash::default(),
        0,
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    let [r0] = &*assert_receipts!(group, signed_transaction) else {
        panic!("Incorrect number of produced receipts")
    };
    let receipts = &*assert_receipts!(group,
        "near_0" => r0 @ "near_1",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(_function_call_action), {}
    );
    assert_single_refund_prior_to_nep536(&group, &receipts);
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
        signer_sender.get_account_id(),
        signer_receiver.get_account_id(),
        &signer_sender,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
            deposit: 0,
        }))],
        CryptoHash::default(),
        0,
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    let [r0] = &*assert_receipts!(group, signed_transaction) else {
        panic!("Incorrect number of produced receipts")
    };
    let receipts = &*assert_receipts!(group, "near_0" => r0 @ "near_1",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(function_call_action),
        {
            assert_eq!(function_call_action.gas, GAS_1);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    let [r1, refunds @ ..] = &receipts else { panic!("Incorrect number of produced receipts") };
    assert_single_refund_prior_to_nep536(&group, &refunds);

    let receipts = &*assert_receipts!(group, "near_1" => r1 @ "near_2",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(function_call_action), {
        assert_eq!(function_call_action.gas, GAS_2);
        assert_eq!(function_call_action.deposit, 0);
    });
    assert_single_refund_prior_to_nep536(&group, &receipts);
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
        signer_sender.get_account_id(),
        signer_receiver.get_account_id(),
        &signer_sender,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
            deposit: 0,
        }))],
        CryptoHash::default(),
        0,
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    let [r0] = &*assert_receipts!(group, signed_transaction) else {
        panic!("Incorrect number of produced receipts")
    };
    let receipts = &*assert_receipts!(
        group, "near_0" => r0 @ "near_1",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_1);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    let [r1, r2, refunds @ ..] = &receipts else { panic!("Incorrect number of produced receipts") };
    assert_single_refund_prior_to_nep536(&group, &refunds);

    let data_id;

    let receipts = &*assert_receipts!(group, "near_1" => r1 @ "near_2",
    ReceiptEnum::Action(ActionReceipt{actions, output_data_receivers, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, output_data_receivers, ..}),
    {
        assert_eq!(output_data_receivers.len(), 1);
        data_id = output_data_receivers[0].data_id;
    },
    actions,
    a0, Action::FunctionCall(function_call_action), {
        assert_eq!(function_call_action.gas, GAS_2);
        assert_eq!(function_call_action.deposit, 0);
    });
    assert_single_refund_prior_to_nep536(&group, &receipts);

    let receipts = &*assert_receipts!(group, "near_1" => r2 @ "near_3",
        ReceiptEnum::Action(ActionReceipt{actions, input_data_ids, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, input_data_ids, ..}),
        {
            assert_eq!(input_data_ids.len(), 1);
            assert_eq!(data_id, input_data_ids[0].clone());
        },
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_2);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    assert_single_refund_prior_to_nep536(&group, &receipts);
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
        signer_sender.get_account_id(),
        signer_receiver.get_account_id(),
        &signer_sender,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
            deposit: 0,
        }))],
        CryptoHash::default(),
        0,
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    let [r0] = &*assert_receipts!(group, signed_transaction) else {
        panic!("Incorrect number of produced receipts")
    };
    let receipts = &*assert_receipts!(group, "near_0" => r0 @ "near_1",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_1);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    let [r1, refunds @ ..] = &receipts else { panic!("must have outgoing receipt") };
    assert_single_refund_prior_to_nep536(&group, &refunds);

    let receipts = &*assert_receipts!(group, "near_1" => r1 @ "near_2",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_2);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    let [r2, refunds @ ..] = &receipts else { panic!("Incorrect number of produced receipts") };
    assert_single_refund_prior_to_nep536(&group, &refunds);

    let receipts = &*assert_receipts!(group, "near_2" => r2 @ "near_3",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_3);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    assert_single_refund_prior_to_nep536(&group, receipts);
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
        signer_sender.get_account_id(),
        signer_receiver.get_account_id(),
        &signer_sender,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
            deposit: 0,
        }))],
        CryptoHash::default(),
        0,
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    let [r0] = &*assert_receipts!(group, signed_transaction) else {
        panic!("Incorrect number of produced receipts")
    };

    let receipts = &*assert_receipts!(group, "near_0" => r0 @ "near_1",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_1);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    let [r1, cb1, refunds @ ..] = &receipts else {
        panic!("Incorrect number of produced receipts")
    };
    assert_single_refund_prior_to_nep536(&group, refunds);

    let receipts = &*assert_receipts!(group, "near_1" => r1 @ "near_2",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_2);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    let [r2, cb2, refunds @ ..] = &receipts else {
        panic!("Incorrect number of produced receipts")
    };
    assert_single_refund_prior_to_nep536(&group, refunds);

    let receipts = &*assert_receipts!(group, "near_2" => r2 @ "near_3",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_3);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    assert_single_refund_prior_to_nep536(&group, receipts);

    let receipts = &*assert_receipts!(group, "near_2" => cb2 @ "near_4",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        { },
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_3);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    assert_single_refund_prior_to_nep536(&group, receipts);

    let receipts = &*assert_receipts!(group, "near_1" => cb1 @ "near_5",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_2);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    assert_single_refund_prior_to_nep536(&group, receipts);
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
        signer_sender.get_account_id(),
        signer_receiver.get_account_id(),
        &signer_sender,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
            deposit: 0,
        }))],
        CryptoHash::default(),
        0,
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    let [r0] = &*assert_receipts!(group, signed_transaction) else {
        panic!("Incorrect number of produced receipts")
    };
    let receipts = &*assert_receipts!(group, "near_0" => r0 @ "near_1",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_1);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    let [r1, refunds @ ..] = &receipts else { panic!("Incorrect number of produced receipts") };
    assert_single_refund_prior_to_nep536(&group, &refunds);

    let receipts = &*assert_receipts!(group, "near_1" => r1 @ "near_2",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_2);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    assert_single_refund_prior_to_nep536(&group, &receipts);
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
        signer_sender.get_account_id(),
        signer_receiver.get_account_id(),
        &signer_sender,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
            deposit: 0,
        }))],
        CryptoHash::default(),
        0,
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    let [r0] = &*assert_receipts!(group, signed_transaction) else {
        panic!("Incorrect number of produced receipts")
    };

    let receipts = &*assert_receipts!(group, "near_0" => r0 @ "near_1",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_1);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    let [r1, r2, refunds @ ..] = &receipts else { panic!("Incorrect number of produced receipts") };
    assert_single_refund_prior_to_nep536(&group, &refunds);

    let data_id;
    let receipts = &*assert_receipts!(group, "near_1" => r1 @ "near_2",
        ReceiptEnum::Action(ActionReceipt{actions, output_data_receivers, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions,  output_data_receivers, ..}),
        {
            assert_eq!(output_data_receivers.len(), 1);
            data_id = output_data_receivers[0].data_id;
        },
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_2);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    assert_single_refund_prior_to_nep536(&group, &receipts);

    let receipts = &*assert_receipts!(group, "near_1" => r2 @ "near_3",
        ReceiptEnum::Action(ActionReceipt{actions, input_data_ids, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, input_data_ids, ..}),
        {
            assert_eq!(input_data_ids.len(), 1);
            assert_eq!(data_id, input_data_ids[0].clone());
        },
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_2);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    if ProtocolFeature::ReducedGasRefunds.enabled(PROTOCOL_VERSION) {
        assert_eq!(receipts, [], "refund should have been avoided");
    } else {
        let [ref1] = &*receipts else { panic!("Incorrect number of refunds") };
        assert_refund!(group, ref1 @ "near_0");
    }
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
        signer_sender.get_account_id(),
        signer_receiver.get_account_id(),
        &signer_sender,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
            deposit: 0,
        }))],
        CryptoHash::default(),
        0,
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    let [r0] = &*assert_receipts!(group, signed_transaction) else {
        panic!("Incorrect number of produced receipts")
    };

    let receipts = &*assert_receipts!(group, "near_0" => r0 @ "near_1",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_1);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    let [r1, refunds @ ..] = &receipts else { panic!("Incorrect number of produced receipts") };

    assert_single_refund_prior_to_nep536(&group, &refunds);

    let refunds = assert_receipts!(group, "near_1" => r1 @ "near_2",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::Transfer(TransferAction{deposit}), {
            assert_eq!(*deposit, 1000000000);
        }
    );
    // For gas price difference
    if ProtocolFeature::ReducedGasRefunds.enabled(PROTOCOL_VERSION) {
        assert_eq!(refunds, [], "refund should have been avoided");
    } else {
        let [ref1] = &*refunds else { panic!("Incorrect number of refunds") };
        assert_refund!(group, ref1 @ "near_0");
    }
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
            "public_key": to_base64(&borsh::to_vec(&signer_new_account.public_key()).unwrap()),
            "nonce": 0,
        }, "id": 0 }
    ]);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.get_account_id(),
        signer_receiver.get_account_id(),
        &signer_sender,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
            deposit: 0,
        }))],
        CryptoHash::default(),
        0,
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    let [r0] = &*assert_receipts!(group, signed_transaction) else {
        panic!("Incorrect number of produced receipts")
    };
    let receipts = &*assert_receipts!(group, "near_0" => r0 @ "near_1",
    ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_1);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    let [r1, refunds @ ..] = &receipts else { panic!("Incorrect number of produced receipts") };
    assert_single_refund_prior_to_nep536(&group, refunds);

    let refunds = assert_receipts!(group, "near_1" => r1 @ "near_2",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::CreateAccount(CreateAccountAction{}), {},
        a1, Action::Transfer(TransferAction{deposit}), {
            assert_eq!(*deposit, 10000000000000000000000000);
        },
        a2, Action::AddKey(add_key_action), {
            assert_eq!(add_key_action.public_key, signer_new_account.public_key());
            assert_eq!(add_key_action.access_key.nonce, 0);
            assert_eq!(add_key_action.access_key.permission, AccessKeyPermission::FullAccess);
        }
    );

    // For gas price difference
    if ProtocolFeature::ReducedGasRefunds.enabled(PROTOCOL_VERSION) {
        assert_eq!(refunds, [], "refund should have been avoided");
    } else {
        let [ref1] = &*refunds else { panic!("Incorrect number of refunds") };
        assert_refund!(group, ref1 @ "near_0");
    }
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
            "public_key": to_base64(&borsh::to_vec(&signer_new_account.public_key()).unwrap()),
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
        signer_sender.get_account_id(),
        signer_receiver.get_account_id(),
        &signer_sender,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
            deposit: 0,
        }))],
        CryptoHash::default(),
        0,
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    let receipts = &*assert_receipts!(group, signed_transaction);
    let [r0] = receipts else { panic!("Incorrect number of produced receipts") };

    let receipts = &*assert_receipts!(group, "near_0" => r0 @ "near_1",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_1);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    let [r1, r2, refunds @ ..] = &receipts else { panic!("Incorrect number of produced receipts") };
    // For gas price difference
    if ProtocolFeature::ReducedGasRefunds.enabled(PROTOCOL_VERSION) {
        assert_eq!(refunds, [], "refund should have been avoided");
    } else {
        let [refund] = &refunds else { panic!("Incorrect number of refunds") };
        assert_refund!(group, refund @ "near_0");
    }

    let data_id;
    let receipts = &*assert_receipts!(group, "near_1" => r1 @ "near_2",
        ReceiptEnum::Action(ActionReceipt{actions, output_data_receivers, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, output_data_receivers, ..}),
        {
            assert_eq!(output_data_receivers.len(), 1);
            data_id = output_data_receivers[0].data_id;
            assert_eq!(output_data_receivers[0].receiver_id, "near_2");
        },
        actions,
        a0, Action::CreateAccount(CreateAccountAction{}), {},
        a1, Action::Transfer(TransferAction{deposit}), {
            assert_eq!(*deposit, TESTING_INIT_BALANCE / 2);
        },
        a2, Action::AddKey(add_key_action), {
            assert_eq!(add_key_action.public_key, signer_new_account.public_key());
            assert_eq!(add_key_action.access_key.nonce, 0);
            assert_eq!(add_key_action.access_key.permission, AccessKeyPermission::FunctionCall(FunctionCallPermission {
                allowance: Some(TESTING_INIT_BALANCE / 2),
                receiver_id: "near_1".parse().unwrap(),
                method_names: vec!["call_promise".to_string(), "hello".to_string()],
            }));
        },
        a3, Action::DeployContract(DeployContractAction{code}), {
            assert_eq!(code, near_test_contracts::rs_contract());
        },
        a4, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_2);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    let [r3, refunds @ ..] = &receipts else { panic!("Incorrect number of produced receipts") };
    // For gas price difference
    assert_single_refund_prior_to_nep536(&group, &refunds);

    let receipts = &*assert_receipts!(group, "near_1" => r2 @ "near_2",
        ReceiptEnum::Action(ActionReceipt{actions, input_data_ids, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, input_data_ids, ..}),
        {
            assert_eq!(input_data_ids, &[data_id]);
        },
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_2);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    let [r4, refunds @ ..] = &receipts else { panic!("Incorrect number of produced receipts") };
    // For gas price difference
    assert_single_refund_prior_to_nep536(&group, &refunds);

    let receipts = &*assert_receipts!(group, "near_2" => r3 @ "near_0",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_3);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    assert_single_refund_prior_to_nep536(&group, receipts);

    let receipts = &*assert_receipts!(group, "near_2" => r4 @ "near_1",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0,
        Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_3);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    assert_single_refund_prior_to_nep536(&group, receipts);
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
            "public_key": to_base64(&borsh::to_vec(&signer_new_account.public_key()).unwrap()),
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
            "public_key": to_base64(&borsh::to_vec(&signer_new_account.public_key()).unwrap()),
            "nonce": 0,
        }, "id": 0 },
        {"action_delete_account": {
            "promise_index": 0,
            "beneficiary_id": "near_2"
        }, "id": 0 },
    ]);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.get_account_id(),
        signer_receiver.get_account_id(),
        &signer_sender,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
            deposit: 0,
        }))],
        CryptoHash::default(),
        0,
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    let [r0] = &*assert_receipts!(group, signed_transaction) else {
        panic!("Incorrect number of produced receipts")
    };
    let receipts = &*assert_receipts!(group, "near_0" => r0 @ "near_1",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_1);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    let [r1, refunds @ ..] = &receipts else { panic!("must have outgoing receipt") };

    // For gas price difference
    assert_single_refund_prior_to_nep536(&group, &refunds);

    let receipts = &*assert_receipts!(group, "near_1" => r1 @ "near_3",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::CreateAccount(CreateAccountAction{}), {},
        a1, Action::Transfer(TransferAction{deposit}), {
            assert_eq!(*deposit, TESTING_INIT_BALANCE / 2);
        },
        a2, Action::AddKey(add_key_action), {
            assert_eq!(add_key_action.public_key, signer_new_account.public_key());
            assert_eq!(add_key_action.access_key.nonce, 1);
            assert_eq!(add_key_action.access_key.permission, AccessKeyPermission::FullAccess);
        },
        a3, Action::DeployContract(DeployContractAction{code}), {
            assert_eq!(code, near_test_contracts::rs_contract());
        },
        a4, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_2);
            assert_eq!(function_call_action.deposit, 0);
        },
        a5, Action::DeleteKey(delete_key_action), {
            assert_eq!(delete_key_action.public_key, signer_new_account.public_key());
        },
        a6, Action::DeleteAccount(DeleteAccountAction{beneficiary_id}), {
            assert_eq!(beneficiary_id, "near_2");
        }
    );

    let [r2, r3, refunds @ ..] = &receipts else { panic!("must have 2 outgoing receipts") };
    // For gas price difference
    if ProtocolFeature::ReducedGasRefunds.enabled(PROTOCOL_VERSION) {
        assert_eq!(refunds, [], "refund should have been avoided");
    } else {
        let [refund] = &refunds else { panic!("Incorrect number of refunds") };
        assert_refund!(group, refund @ "near_0");
    }

    let receipts = &*assert_receipts!(group, "near_3" => r2 @ "near_0",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_3);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    // For gas price difference
    if ProtocolFeature::ReducedGasRefunds.enabled(PROTOCOL_VERSION) {
        assert_eq!(receipts[..], [], "refund should have been avoided");
    } else {
        let [refund] = &receipts[..] else { panic!("Incorrect number of refunds") };
        assert_refund!(group, refund @ "near_0");
    }

    // last receipt is refund for deleted account balance, going to near_2
    assert_refund!(group, r3 @ "near_2");
}

#[test]
fn test_transfer_64len_hex() {
    let pk = InMemorySigner::test_signer(&"test_hex".parse().unwrap());
    let account_id =
        AccountId::try_from(hex::encode(pk.public_key().unwrap_as_ed25519().0)).unwrap();

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
        signer_sender.get_account_id(),
        signer_receiver.get_account_id(),
        &signer_sender,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
            deposit: 0,
        }))],
        CryptoHash::default(),
        0,
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    let [r0] = &*assert_receipts!(group, signed_transaction) else {
        panic!("Incorrect number of produced receipts")
    };
    let receipts = &*assert_receipts!(group, "near_0" => r0 @ "near_1",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_1);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    let [r1, refunds @ ..] = &receipts else { panic!("Incorrect number of produced receipts") };
    assert_single_refund_prior_to_nep536(&group, &refunds);

    let refunds = assert_receipts!(group, "near_1" => r1 @ account_id.as_str(),
    ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::Transfer(TransferAction{deposit}), {
        assert_eq!(*deposit, TESTING_INIT_BALANCE / 2);
       }
    );

    if ProtocolFeature::ReducedGasRefunds.enabled(PROTOCOL_VERSION) {
        assert_eq!(refunds, [], "refund should have been avoided");
    } else {
        let [ref1] = &*refunds else { panic!("Incorrect number of refunds") };
        assert_refund!(group, ref1 @ "near_0");
    }
}

#[test]
fn test_create_transfer_64len_hex_fail() {
    let pk = InMemorySigner::test_signer(&"test_hex".parse().unwrap());
    let account_id =
        AccountId::try_from(hex::encode(pk.public_key().unwrap_as_ed25519().0)).unwrap();

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
        signer_sender.get_account_id(),
        signer_receiver.get_account_id(),
        &signer_sender,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
            deposit: 0,
        }))],
        CryptoHash::default(),
        0,
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    let [r0] = &*assert_receipts!(group, signed_transaction) else {
        panic!("Incorrect number of produced receipts")
    };
    let receipts = &*assert_receipts!(group, "near_0" => r0 @ "near_1",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_1);
            assert_eq!(function_call_action.deposit, 0);
        }
    );
    let [r1, refunds @ ..] = &receipts else { panic!("Incorrect number of produced receipts") };
    assert_single_refund_prior_to_nep536(&group, &refunds);

    let receipts = &*assert_receipts!(group, "near_1" => r1 @ account_id.as_str(),
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::CreateAccount(CreateAccountAction{}), {},
        a1, Action::Transfer(TransferAction{deposit}), {
            assert_eq!(*deposit, TESTING_INIT_BALANCE / 2);
        }
    );

    let [deposit_refund, refunds @ ..] = &receipts else {
        panic!("Incorrect number of produced receipts")
    };
    assert_refund!(group, deposit_refund @ "near_1");

    // For gas price difference
    assert_single_refund_prior_to_nep536(&group, &refunds);
}

// redirect the balance refund using `promise_refund_to`
#[test]
fn test_refund_to() {
    let test_contract = if ProtocolFeature::DeterministicAccountIds.enabled(PROTOCOL_VERSION) {
        near_test_contracts::nightly_rs_contract()
    } else {
        near_test_contracts::rs_contract()
    };
    let group = RuntimeGroup::new(4, 4, &test_contract);

    let signer_sender = group.signers[0].clone();
    let signer_receiver = group.signers[1].clone();
    let deposit = 1000;

    let data = serde_json::json!([
        {
            "batch_create": {
                "account_id": "near_2",
            },
            "id": 0
        },
        {
            "action_function_call": {
                "promise_index": 0,
                "method_name": "non_existing_function",
                "arguments": [],
                "amount": deposit.to_string(),
                "gas": GAS_2,
            },
            "id": 0
        },
        {
            "set_refund_to": {
                "promise_index": 0,
                "beneficiary_id": "near_3"
            }, "id": 0
        }
    ]);

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.get_account_id(),
        signer_receiver.get_account_id(),
        &signer_sender,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&data).unwrap(),
            gas: GAS_1,
            deposit,
        }))],
        CryptoHash::default(),
        0,
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }

    use near_primitives::transaction::*;
    let [r0] = &*assert_receipts!(group, signed_transaction) else {
        panic!("Incorrect number of produced receipts")
    };

    let receipts = &*assert_receipts!(group, "near_0" => r0 @ "near_1",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_1);
            assert_eq!(function_call_action.deposit, deposit);
            assert_eq!(function_call_action.method_name, "call_promise");
        }
    );
    let [r1] = &receipts else { panic!("Incorrect number of produced receipts") };

    let receipts = &*assert_receipts!(group, "near_1" => r1 @ "near_2",
        ReceiptEnum::Action(ActionReceipt{actions, ..}) | ReceiptEnum::ActionV2(ActionReceiptV2{actions, ..}),
        {},
        actions,
        a0, Action::FunctionCall(function_call_action), {
            assert_eq!(function_call_action.gas, GAS_2);
            assert_eq!(function_call_action.deposit, deposit);
            assert_eq!(function_call_action.method_name, "non_existing_function");
        }
    );
    let [deposit_refund] = &receipts else { panic!("Incorrect number of produced receipts") };

    // This is the redirected refund
    if ProtocolFeature::DeterministicAccountIds.enabled(PROTOCOL_VERSION) {
        assert_refund!(group, deposit_refund @ "near_3");
    } else {
        assert_refund!(group, deposit_refund @ "near_1");
    }
}

#[track_caller]
fn assert_single_refund_prior_to_nep536(group: &RuntimeGroup, receipts: &[CryptoHash]) {
    use near_primitives::transaction::*;

    if ProtocolFeature::ReducedGasRefunds.enabled(PROTOCOL_VERSION) {
        assert_eq!(receipts, [], "refund should have been avoided");
    } else {
        let [refund] = &receipts[..] else { panic!("Incorrect number of refunds") };
        assert_refund!(group, refund @ "near_0");
    }
}

use near_primitives::transaction::{
    CreateAccountTransaction, DeployContractTransaction, TransactionBody,
};
use near_primitives::types::Balance;
use testlib::node::{Node, RuntimeNode};

const FUNCTION_CALL_AMOUNT: Balance = 1_000_000_000;

fn setup_test_contract(wasm_binary: &[u8]) -> RuntimeNode {
    let node = RuntimeNode::new(&"alice.near".to_string());
    let account_id = node.account_id().unwrap();
    let transaction = TransactionBody::CreateAccount(CreateAccountTransaction {
        nonce: node.get_account_nonce(&account_id).unwrap_or_default() + 1,
        originator_id: account_id.clone(),
        new_account_id: "test_contract".to_string(),
        public_key: node.signer().public_key().0[..].to_vec(),
        amount: 0,
    })
    .sign(&*node.signer());
    let user = node.user();
    user.add_transaction(transaction).unwrap();

    let transaction = TransactionBody::DeployContract(DeployContractTransaction {
        nonce: node.get_account_nonce(&account_id).unwrap_or_default() + 1,
        contract_id: "test_contract".to_string(),
        wasm_byte_array: wasm_binary.to_vec(),
    })
    .sign(&*node.signer());
    user.add_transaction(transaction).unwrap();
    node
}

#[test]
fn test_evil_deep_trie() {
    let node = setup_test_contract(include_bytes!("../../../tests/hello.wasm"));
    (0..50).for_each(|i| {
        println!("insertStrings #{}", i);
        let input_data = format!("{{\"from\": {}, \"to\": {}}}", i * 10, (i + 1) * 10);
        node.call_function(
            "test_contract",
            "insertStrings",
            input_data.as_bytes().to_vec(),
            FUNCTION_CALL_AMOUNT,
        );
    });
    (0..50).rev().for_each(|i| {
        println!("deleteStrings #{}", i);
        let input_data = format!("{{\"from\": {}, \"to\": {}}}", i * 10, (i + 1) * 10);
        node.call_function(
            "test_contract",
            "deleteStrings",
            input_data.as_bytes().to_vec(),
            FUNCTION_CALL_AMOUNT,
        );
    });
}

#[test]
fn test_evil_deep_recursion() {
    let node = setup_test_contract(include_bytes!("../../../tests/hello.wasm"));
    [100, 1000, 10000, 100000, 1000000].iter().for_each(|n| {
        println!("{}", n);
        let input_data = format!("{{\"n\": {}}}", n);
        node.call_function(
            "test_contract",
            "recurse",
            input_data.as_bytes().to_vec(),
            FUNCTION_CALL_AMOUNT,
        );
    });
}

use crate::node::{Node, RuntimeNode};
use crate::test_helpers::wait;
use crate::user::User;
use crate::runtime_utils::{bob_account, default_code_hash, encode_int, eve_account};
use node_runtime::chain_spec::{TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
use node_runtime::state_viewer::AccountViewCallResult;
use primitives::crypto::signer::InMemorySigner;
use primitives::hash::{hash, CryptoHash};
use primitives::serialize::Decode;
use primitives::transaction::{
    AddKeyTransaction, AsyncCall, Callback, CallbackInfo, CallbackResult, CreateAccountTransaction,
    DeleteKeyTransaction, DeployContractTransaction, FinalTransactionStatus,
    FunctionCallTransaction, ReceiptBody, ReceiptTransaction, SwapKeyTransaction, TransactionBody,
    TransactionStatus,
};
use primitives::types::AccountingInfo;
use primitives::utils::key_for_callback;
use storage::set;

/// validate transaction result in the case that it is successfully and generate one receipt which
/// itself generates another receipt. sfdsa
pub fn validate_tx_result(node_user: Box<User>, root: CryptoHash, hash: &CryptoHash) {
    let transaction_result = node_user.get_transaction_result(&hash);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert_eq!(transaction_result.receipts.len(), 1);
    let transaction_result = node_user.get_transaction_result(&transaction_result.receipts[0]);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert_eq!(transaction_result.receipts.len(), 1);
    let transaction_result = node_user.get_transaction_result(&transaction_result.receipts[0]);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert_eq!(transaction_result.receipts.len(), 0);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

/// Wait until transaction finishes (either succeeds or fails).
pub fn wait_for_transaction(node_user: &Box<User>, hash: &CryptoHash) {
    wait(
        || match node_user.get_transaction_final_result(hash).status {
            FinalTransactionStatus::Unknown | FinalTransactionStatus::Started => false,
            _ => true,
        },
        500,
        60000,
    );
}

pub fn test_smart_contract_simple(node: impl Node) {
    let account_id = &node.signer().account_id;
    let transaction = TransactionBody::FunctionCall(FunctionCallTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        contract_id: bob_account(),
        method_name: "run_test".as_bytes().to_vec(),
        args: vec![],
        amount: 0,
    })
    .sign(&*node.signer());

    let node_user = node.user();
    let hash = transaction.get_hash();
    let root = node_user.get_state_root();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &hash);
    validate_tx_result(node_user, root, &hash);
}

pub fn test_smart_contract_bad_method_name(node: impl Node) {
    let account_id = &node.signer().account_id;
    let transaction = TransactionBody::FunctionCall(FunctionCallTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        contract_id: bob_account(),
        method_name: "_run_test".as_bytes().to_vec(),
        args: vec![],
        amount: 0,
    })
    .sign(&*node.signer());

    let node_user = node.user();
    let hash = transaction.get_hash();
    let root = node_user.get_state_root();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &hash);

    let transaction_result = node_user.get_transaction_result(&hash);
    assert_eq!(transaction_result.status, TransactionStatus::Failed);
    assert_eq!(transaction_result.receipts.len(), 0);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_smart_contract_empty_method_name_with_no_tokens(node: impl Node) {
    let account_id = &node.signer().account_id;
    let transaction = TransactionBody::FunctionCall(FunctionCallTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        contract_id: bob_account(),
        method_name: vec![],
        args: vec![],
        amount: 0,
    })
    .sign(&*node.signer());

    let node_user = node.user();
    let hash = transaction.get_hash();
    let root = node_user.get_state_root();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &hash);

    let transaction_result = node_user.get_transaction_result(&hash);
    assert_eq!(transaction_result.status, TransactionStatus::Failed);
    assert_eq!(transaction_result.receipts.len(), 0);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_smart_contract_empty_method_name_with_tokens(node: impl Node) {
    let account_id = &node.signer().account_id;
    let transaction = TransactionBody::FunctionCall(FunctionCallTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        contract_id: bob_account(),
        method_name: vec![],
        args: vec![],
        amount: 10,
    })
    .sign(&*node.signer());

    let node_user = node.user();
    let hash = transaction.get_hash();
    let root = node_user.get_state_root();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &hash);
    validate_tx_result(node_user, root, &hash);
}

pub fn test_smart_contract_with_args(node: impl Node) {
    let account_id = &node.signer().account_id;
    let transaction = TransactionBody::FunctionCall(FunctionCallTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        contract_id: bob_account(),
        method_name: "run_test".as_bytes().to_vec(),
        args: (2..4).flat_map(|x| encode_int(x).to_vec()).collect(),
        amount: 0,
    })
    .sign(&*node.signer());

    let node_user = node.user();
    let hash = transaction.get_hash();
    let root = node_user.get_state_root();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &hash);
    validate_tx_result(node_user, root, &hash);
}

pub fn test_async_call_with_no_callback(node: impl Node) {
    let account_id = &node.signer().account_id;
    let nonce = hash(&[1, 2, 3]);
    let receipt = ReceiptTransaction {
        originator: account_id.clone(),
        receiver: bob_account(),
        nonce,
        body: ReceiptBody::NewCall(AsyncCall::new(
            "run_test".as_bytes().to_vec(),
            vec![],
            0,
            0,
            AccountingInfo { originator: account_id.clone(), contract_id: None },
        )),
    };

    let node_user = node.user();
    let hash = receipt.nonce;
    let root = node_user.get_state_root();
    node_user.add_receipt(receipt).unwrap();
    wait_for_transaction(&node_user, &hash);

    let transaction_result = node_user.get_transaction_result(&hash);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert_eq!(transaction_result.receipts.len(), 1);
    let transaction_result = node_user.get_transaction_result(&transaction_result.receipts[0]);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert_eq!(transaction_result.receipts.len(), 0);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_async_call_with_callback(node: impl Node) {
    let account_id = &node.signer().account_id;
    let args = (7..9).flat_map(|x| encode_int(x).to_vec()).collect();
    let accounting_info = AccountingInfo { originator: account_id.clone(), contract_id: None };
    let mut callback = Callback::new(b"sum_with_input".to_vec(), args, 0, accounting_info.clone());
    callback.results.resize(1, None);
    let callback_id = [0; 32].to_vec();
    let mut async_call =
        AsyncCall::new(b"run_test".to_vec(), vec![], 0, 0, accounting_info.clone());
    let callback_info = CallbackInfo::new(callback_id.clone(), 0, account_id.clone());
    async_call.callback = Some(callback_info.clone());
    let receipt = ReceiptTransaction::new(
        account_id.clone(),
        bob_account(),
        hash(&[1, 2, 3]).into(),
        ReceiptBody::NewCall(async_call),
    );

    let node_user = node.user();
    let hash = receipt.nonce;
    node_user.add_receipt(receipt).unwrap();
    wait_for_transaction(&node_user, &hash);

    let transaction_result = node_user.get_transaction_result(&hash);
    assert_eq!(transaction_result.result, Some(encode_int(10).to_vec()));
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert_eq!(transaction_result.receipts.len(), 2);

    let receipt_info = node_user.get_receipt_info(&transaction_result.receipts[0]).unwrap();
    assert_eq!(receipt_info.receipt.originator, bob_account());
    assert_eq!(receipt_info.receipt.receiver, account_id.clone());
    let callback_res = CallbackResult::new(callback_info.clone(), Some(encode_int(10).to_vec()));
    assert_eq!(receipt_info.receipt.body, ReceiptBody::Callback(callback_res));

    let receipt_result = node_user.get_transaction_result(&transaction_result.receipts[0]);
    assert_eq!(receipt_result.receipts.len(), 0);

    let receipt_info = node_user.get_receipt_info(&transaction_result.receipts[1]).unwrap();
    assert_eq!(receipt_info.receipt.originator, bob_account());
    assert_eq!(receipt_info.receipt.receiver, account_id.clone());
    if let ReceiptBody::ManaAccounting(ref mana_accounting) = receipt_info.receipt.body {
        assert_eq!(mana_accounting.mana_refund, 0);
        assert!(mana_accounting.gas_used > 0);
        assert_eq!(mana_accounting.accounting_info, accounting_info);
    } else {
        assert!(false);
    }
}

pub fn test_async_call_with_logs(node: impl Node) {
    let account_id = &node.signer().account_id;
    let nonce = hash(&[1, 2, 3]);
    let receipt = ReceiptTransaction {
        originator: account_id.clone(),
        receiver: bob_account(),
        nonce,
        body: ReceiptBody::NewCall(AsyncCall::new(
            "log_something".as_bytes().to_vec(),
            vec![],
            0,
            0,
            AccountingInfo { originator: account_id.clone(), contract_id: None },
        )),
    };

    let node_user = node.user();
    let hash = receipt.nonce;
    let root = node_user.get_state_root();
    node_user.add_receipt(receipt).unwrap();
    wait_for_transaction(&node_user, &hash);

    let transaction_result = node_user.get_transaction_result(&hash);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert_eq!(transaction_result.receipts.len(), 1);
    assert_eq!(transaction_result.logs[0], "LOG: hello".to_string());
    let transaction_result = node_user.get_transaction_result(&transaction_result.receipts[0]);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert_eq!(transaction_result.receipts.len(), 0);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_deposit_with_callback(node: impl Node) {
    let account_id = &node.signer().account_id;
    let args = (7..9).flat_map(|x| encode_int(x).to_vec()).collect();
    let accounting_info = AccountingInfo { originator: account_id.clone(), contract_id: None };
    let mut callback = Callback::new(b"sum_with_input".to_vec(), args, 0, accounting_info.clone());
    callback.results.resize(1, None);
    let callback_id = [0; 32].to_vec();
    let mut async_call = AsyncCall::new(vec![], vec![], 0, 0, accounting_info.clone());
    let callback_info = CallbackInfo::new(callback_id.clone(), 0, account_id.clone());
    async_call.callback = Some(callback_info.clone());
    let receipt = ReceiptTransaction::new(
        account_id.clone(),
        bob_account(),
        hash(&[1, 2, 3]).into(),
        ReceiptBody::NewCall(async_call),
    );

    let node_user = node.user();
    let hash = receipt.nonce;
    node_user.add_receipt(receipt).unwrap();
    wait_for_transaction(&node_user, &hash);

    let transaction_result = node_user.get_transaction_result(&hash);
    assert_eq!(transaction_result.result, Some(vec![]));

    let receipt_info = node_user.get_receipt_info(&transaction_result.receipts[0]).unwrap();
    assert_eq!(receipt_info.receipt.originator, bob_account());
    assert_eq!(receipt_info.receipt.receiver, account_id.clone());
    let callback_res = CallbackResult::new(callback_info.clone(), Some(vec![]));
    assert_eq!(receipt_info.receipt.body, ReceiptBody::Callback(callback_res));
}

// This test only works with RuntimeNode because it requires modifying state.
pub fn test_callback(node: RuntimeNode) {
    let account_id = &node.signer().account_id;
    let accounting_info = AccountingInfo { originator: account_id.clone(), contract_id: None };
    let mut callback =
        Callback::new(b"run_test_with_storage_change".to_vec(), vec![], 0, accounting_info.clone());
    callback.results.resize(1, None);
    let callback_id = [0; 32].to_vec();

    let mut state_update = node.client.read().unwrap().get_state_update();
    set(&mut state_update, &key_for_callback(&callback_id), &callback);
    let (root, transaction) = state_update.finalize();
    {
        let mut client = node.client.write().unwrap();
        client.state_root = root;
        client.trie.apply_changes(transaction).unwrap();
    }

    let callback_info = CallbackInfo::new(callback_id.clone(), 0, account_id.clone());
    let receipt = ReceiptTransaction::new(
        account_id.clone(),
        bob_account(),
        hash(&[1, 2, 3]).into(),
        ReceiptBody::Callback(CallbackResult::new(callback_info, None)),
    );

    let hash = receipt.nonce;
    let node_user = node.user();
    node_user.add_receipt(receipt).unwrap();
    wait_for_transaction(&node_user, &hash);

    let transaction_result = node_user.get_transaction_result(&hash);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    let callback: Option<Callback> = node_user
        .view_state(account_id)
        .unwrap()
        .values
        .get(&key_for_callback(&callback_id))
        .and_then(|data| Decode::decode(&data).ok());
    assert!(callback.is_none());
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_callback_failure(node: RuntimeNode) {
    let account_id = &node.signer().account_id;
    let accounting_info = AccountingInfo { originator: account_id.clone(), contract_id: None };
    let mut callback = Callback::new(
        b"a_function_that_does_not_exist".to_vec(),
        vec![],
        0,
        accounting_info.clone(),
    );
    callback.results.resize(1, None);
    let callback_id = [0; 32].to_vec();
    let mut state_update = node.client.read().unwrap().get_state_update();
    set(&mut state_update, &key_for_callback(&callback_id.clone()), &callback);
    let (root, transaction) = state_update.finalize();
    {
        let mut client = node.client.write().unwrap();
        client.state_root = root;
        client.trie.apply_changes(transaction).unwrap();
    }

    let callback_info = CallbackInfo::new(callback_id.clone(), 0, account_id.clone());
    let receipt = ReceiptTransaction::new(
        account_id.clone(),
        bob_account(),
        hash(&[1, 2, 3]).into(),
        ReceiptBody::Callback(CallbackResult::new(callback_info, None)),
    );

    let hash = receipt.nonce;
    let node_user = node.user();
    node_user.add_receipt(receipt).unwrap();
    wait_for_transaction(&node_user, &hash);

    let transaction_result = node_user.get_transaction_result(&hash);
    assert_eq!(transaction_result.status, TransactionStatus::Failed);
    let callback: Option<Callback> = node_user
        .view_state(account_id)
        .unwrap()
        .values
        .get(&key_for_callback(&callback_id))
        .and_then(|data| Decode::decode(&data).ok());
    assert!(callback.is_none());
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_nonce_update_when_deploying_contract(node: impl Node) {
    let account_id = &node.signer().account_id;
    let wasm_binary = include_bytes!("../../../runtime/wasm/runtest/res/wasm_with_mem.wasm");
    let transaction = TransactionBody::DeployContract(DeployContractTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        contract_id: account_id.clone(),
        wasm_byte_array: wasm_binary.to_vec(),
    })
    .sign(&*node.signer());

    let node_user = node.user();
    let hash = transaction.get_hash();
    let root = node_user.get_state_root();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &hash);

    let transaction_result = node_user.get_transaction_result(&hash);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert_eq!(node_user.get_account_nonce(account_id).unwrap(), 1);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_nonce_updated_when_tx_failed(node: impl Node) {
    let account_id = &node.signer().account_id;
    let transaction = TransactionBody::send_money(
        node.get_account_nonce(account_id).unwrap_or_default() + 1,
        account_id,
        &bob_account(),
        TESTING_INIT_BALANCE + 1,
    )
    .sign(&*node.signer());

    let node_user = node.user();
    let hash = transaction.get_hash();
    let root = node_user.get_state_root();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &hash);

    let transaction_result = node_user.get_transaction_result(&hash);
    assert_eq!(transaction_result.status, TransactionStatus::Failed);
    assert_eq!(node_user.get_account_nonce(account_id).unwrap(), 1);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_upload_contract(node: impl Node) {
    let account_id = &node.signer().account_id;
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction = TransactionBody::CreateAccount(CreateAccountTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        new_account_id: eve_account(),
        public_key: node.signer().public_key.0[..].to_vec(),
        amount: 10,
    })
    .sign(&*node.signer());
    let tx_hash = transaction.get_hash();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &tx_hash);

    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    let wasm_binary = include_bytes!("../../../runtime/wasm/runtest/res/wasm_with_mem.wasm");
    let transaction = TransactionBody::DeployContract(DeployContractTransaction {
        nonce: 1,
        contract_id: eve_account(),
        wasm_byte_array: wasm_binary.to_vec(),
    })
    .sign(&*node.signer());

    let tx_hash = transaction.get_hash();
    let root = node_user.get_state_root();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &tx_hash);

    let transaction_result = node_user.get_transaction_result(&tx_hash);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert!(transaction_result.receipts.is_empty());
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    let account = node_user.view_account(&eve_account()).unwrap();
    assert_eq!(account.code_hash, hash(wasm_binary));
}

pub fn test_redeploy_contract(node: impl Node) {
    let account_id = &node.signer().account_id;
    let node_user = node.user();
    let test_binary = b"test_binary";
    let transaction = TransactionBody::DeployContract(DeployContractTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        contract_id: account_id.clone(),
        wasm_byte_array: test_binary.to_vec(),
    })
    .sign(&*node.signer());
    let tx_hash = transaction.get_hash();
    let root = node_user.get_state_root();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &tx_hash);

    let transaction_result = node_user.get_transaction_result(&tx_hash);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert!(transaction_result.receipts.is_empty());
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.code_hash, hash(test_binary));
}

pub fn test_send_money(node: impl Node) {
    let account_id = &node.signer().account_id;
    let node_user = node.user();
    let money_used = 10;
    let transaction = TransactionBody::send_money(
        node.get_account_nonce(account_id).unwrap_or_default() + 1,
        account_id,
        &bob_account(),
        money_used,
    )
    .sign(&*node.signer());

    let tx_hash = transaction.get_hash();
    let root = node_user.get_state_root();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &tx_hash);

    let transaction_result = node_user.get_transaction_result(&tx_hash);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    let result1 = node_user.view_account(account_id);
    assert_eq!(
        result1.unwrap(),
        AccountViewCallResult {
            nonce: 1,
            account_id: account_id.clone(),
            public_keys: vec![node.signer().public_key],
            amount: TESTING_INIT_BALANCE - money_used,
            stake: TESTING_INIT_STAKE,
            code_hash: default_code_hash(),
        }
    );
    let result2 = node_user.view_account(&bob_account()).unwrap();
    let public_keys = result2.public_keys.clone();
    assert_eq!(
        result2,
        AccountViewCallResult {
            nonce: 0,
            account_id: bob_account(),
            public_keys,
            amount: TESTING_INIT_BALANCE + money_used,
            stake: TESTING_INIT_STAKE,
            code_hash: default_code_hash(),
        }
    );
}

pub fn test_send_money_over_balance(node: impl Node) {
    let account_id = &node.signer().account_id;
    let node_user = node.user();
    let money_used = TESTING_INIT_BALANCE + 1;
    let transaction = TransactionBody::send_money(
        node.get_account_nonce(account_id).unwrap_or_default() + 1,
        account_id,
        &bob_account(),
        money_used,
    )
    .sign(&*node.signer());

    let tx_hash = transaction.get_hash();
    let root = node_user.get_state_root();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &tx_hash);

    let transaction_result = node_user.get_transaction_result(&tx_hash);
    assert_eq!(transaction_result.status, TransactionStatus::Failed);
    assert!(transaction_result.receipts.is_empty());
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    let result1 = node_user.view_account(account_id);
    assert_eq!(
        result1.unwrap(),
        AccountViewCallResult {
            nonce: 1,
            account_id: account_id.clone(),
            public_keys: vec![node.signer().public_key],
            amount: TESTING_INIT_BALANCE,
            stake: TESTING_INIT_STAKE,
            code_hash: default_code_hash(),
        }
    );
    let result2 = node_user.view_account(&bob_account()).unwrap();
    let public_keys = result2.public_keys.clone();
    assert_eq!(
        result2,
        AccountViewCallResult {
            nonce: 0,
            account_id: bob_account(),
            public_keys,
            amount: TESTING_INIT_BALANCE,
            stake: TESTING_INIT_STAKE,
            code_hash: default_code_hash(),
        }
    );
}

pub fn test_refund_on_send_money_to_non_existent_account(node: impl Node) {
    let account_id = &node.signer().account_id;
    let node_user = node.user();
    let money_used = 10;
    let transaction = TransactionBody::send_money(
        node.get_account_nonce(account_id).unwrap_or_default() + 1,
        account_id,
        &eve_account(),
        money_used,
    )
    .sign(&*node.signer());

    let tx_hash = transaction.get_hash();
    let root = node_user.get_state_root();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &tx_hash);

    let transaction_result = node_user.get_transaction_result(&tx_hash);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert_eq!(transaction_result.receipts.len(), 1);
    let transaction_result = node_user.get_transaction_result(&transaction_result.receipts[0]);
    assert_eq!(transaction_result.status, TransactionStatus::Failed);
    assert_eq!(transaction_result.receipts.len(), 1);
    wait_for_transaction(&node_user, &transaction_result.receipts[0]);
    let transaction_result = node_user.get_transaction_result(&transaction_result.receipts[0]);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert!(transaction_result.receipts.is_empty());
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    let result1 = node_user.view_account(account_id);
    assert_eq!(
        result1.unwrap(),
        AccountViewCallResult {
            nonce: 1,
            account_id: account_id.clone(),
            public_keys: vec![node.signer().public_key],
            amount: TESTING_INIT_BALANCE,
            stake: TESTING_INIT_STAKE,
            code_hash: default_code_hash(),
        }
    );
    let result2 = node_user.view_account(&eve_account());
    assert!(result2.is_err());
}

pub fn test_create_account(node: impl Node) {
    let account_id = &node.signer().account_id;
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = 10;
    let transaction = TransactionBody::CreateAccount(CreateAccountTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        new_account_id: eve_account(),
        public_key: node.signer().public_key.0[..].to_vec(),
        amount: money_used,
    })
    .sign(&*node.signer());
    let tx_hash = transaction.get_hash();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &tx_hash);

    let transaction_result = node_user.get_transaction_result(&tx_hash);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert_eq!(transaction_result.receipts.len(), 1);
    let transaction_result = node_user.get_transaction_result(&transaction_result.receipts[0]);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert!(transaction_result.receipts.is_empty());
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let result1 = node_user.view_account(account_id);
    assert_eq!(
        result1.unwrap(),
        AccountViewCallResult {
            nonce: 1,
            account_id: account_id.clone(),
            public_keys: vec![node.signer().public_key],
            amount: TESTING_INIT_BALANCE - money_used,
            stake: TESTING_INIT_STAKE,
            code_hash: default_code_hash(),
        }
    );

    let result2 = node_user.view_account(&eve_account()).unwrap();
    let public_keys = result2.public_keys.clone();
    assert_eq!(
        result2,
        AccountViewCallResult {
            nonce: 0,
            account_id: eve_account(),
            public_keys,
            amount: money_used,
            stake: 0,
            code_hash: hash(b""),
        }
    );
}

pub fn test_create_account_again(node: impl Node) {
    let account_id = &node.signer().account_id;
    let node_user = node.user();
    let money_used = 10;
    let transaction = TransactionBody::CreateAccount(CreateAccountTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        new_account_id: eve_account(),
        public_key: node.signer().public_key.0[..].to_vec(),
        amount: money_used,
    })
    .sign(&*node.signer());
    let tx_hash = transaction.get_hash();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &tx_hash);

    let result1 = node_user.view_account(account_id);
    assert_eq!(
        result1.unwrap(),
        AccountViewCallResult {
            nonce: 1,
            account_id: account_id.clone(),
            public_keys: vec![node.signer().public_key],
            amount: TESTING_INIT_BALANCE - money_used,
            stake: TESTING_INIT_STAKE,
            code_hash: default_code_hash(),
        }
    );

    let result2 = node_user.view_account(&eve_account()).unwrap();
    let public_keys = result2.public_keys.clone();
    assert_eq!(
        result2,
        AccountViewCallResult {
            nonce: 0,
            account_id: eve_account(),
            public_keys,
            amount: money_used,
            stake: 0,
            code_hash: hash(b""),
        }
    );

    let transaction = TransactionBody::CreateAccount(CreateAccountTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        new_account_id: eve_account(),
        public_key: node.signer().public_key.0[..].to_vec(),
        amount: money_used,
    })
    .sign(&*node.signer());
    let tx_hash = transaction.get_hash();
    let root = node_user.get_state_root();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &tx_hash);

    let transaction_result = node_user.get_transaction_result(&tx_hash);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert_eq!(transaction_result.receipts.len(), 1);
    let transaction_result = node_user.get_transaction_result(&transaction_result.receipts[0]);
    assert_eq!(transaction_result.status, TransactionStatus::Failed);
    assert_eq!(transaction_result.receipts.len(), 1);
    wait_for_transaction(&node_user, &transaction_result.receipts[0]);
    let transaction_result = node_user.get_transaction_result(&transaction_result.receipts[0]);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert!(transaction_result.receipts.is_empty());
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let result1 = node_user.view_account(account_id);
    assert_eq!(
        result1.unwrap(),
        AccountViewCallResult {
            nonce: 2,
            account_id: account_id.clone(),
            public_keys: vec![node.signer().public_key],
            amount: TESTING_INIT_BALANCE - money_used,
            stake: TESTING_INIT_STAKE,
            code_hash: default_code_hash(),
        }
    );
}

pub fn test_create_account_failure_invalid_name(node: impl Node) {
    let account_id = &node.signer().account_id;
    let node_user = node.user();
    let mut root = node_user.get_state_root();
    let money_used = 10;
    let mut counter = 0;
    for invalid_account_name in vec![
        "eve",                               // too short
        "Alice.near",                        // capital letter
        "alice(near)",                       // brackets are invalid
        "long_of_the_name_for_real_is_hard", // too long
        "qq@qq*qq",                          // * is invalid
    ] {
        counter += 1;
        let transaction = TransactionBody::CreateAccount(CreateAccountTransaction {
            nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
            originator: account_id.clone(),
            new_account_id: invalid_account_name.to_string(),
            public_key: node.signer().public_key.0[..].to_vec(),
            amount: money_used,
        })
        .sign(&*node.signer());
        let tx_hash = transaction.get_hash();
        node_user.add_transaction(transaction).unwrap();
        wait_for_transaction(&node_user, &tx_hash);

        let new_root = node_user.get_state_root();
        assert_ne!(root, new_root);
        root = new_root;
        let transaction_result = node_user.get_transaction_result(&tx_hash);
        assert_eq!(transaction_result.status, TransactionStatus::Failed);
        let account = node_user.view_account(account_id).unwrap();
        assert_eq!(
            account,
            AccountViewCallResult {
                nonce: counter,
                account_id: account_id.clone(),
                public_keys: vec![node.signer().public_key.clone()],
                amount: TESTING_INIT_BALANCE,
                stake: TESTING_INIT_STAKE,
                code_hash: default_code_hash(),
            }
        );
    }
}

pub fn test_create_account_failure_already_exists(node: impl Node) {
    let account_id = &node.signer().account_id;
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = 10;
    let transaction = TransactionBody::CreateAccount(CreateAccountTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        new_account_id: bob_account(),
        public_key: node.signer().public_key.0[..].to_vec(),
        amount: money_used,
    })
    .sign(&*node.signer());
    let tx_hash = transaction.get_hash();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &tx_hash);

    let transaction_result = node_user.get_transaction_result(&tx_hash);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert_eq!(transaction_result.receipts.len(), 1);
    let transaction_result = node_user.get_transaction_result(&transaction_result.receipts[0]);
    assert_eq!(transaction_result.status, TransactionStatus::Failed);
    assert_eq!(transaction_result.receipts.len(), 1);
    wait_for_transaction(&node_user, &transaction_result.receipts[0]);
    let transaction_result = node_user.get_transaction_result(&transaction_result.receipts[0]);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert!(transaction_result.receipts.is_empty());
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let result1 = node_user.view_account(account_id);
    assert_eq!(
        result1.unwrap(),
        AccountViewCallResult {
            nonce: 1,
            account_id: account_id.clone(),
            public_keys: vec![node.signer().public_key],
            amount: TESTING_INIT_BALANCE,
            stake: TESTING_INIT_STAKE,
            code_hash: default_code_hash(),
        }
    );

    let result2 = node_user.view_account(&bob_account()).unwrap();
    let public_keys = result2.public_keys.clone();
    assert_eq!(
        result2,
        AccountViewCallResult {
            nonce: 0,
            account_id: bob_account(),
            public_keys,
            amount: TESTING_INIT_BALANCE,
            stake: TESTING_INIT_STAKE,
            code_hash: default_code_hash(),
        }
    );
}

pub fn test_swap_key(node: impl Node) {
    let account_id = &node.signer().account_id;
    let signer2 = InMemorySigner::from_random();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = 10;
    let transaction = TransactionBody::CreateAccount(CreateAccountTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        new_account_id: eve_account(),
        public_key: node.signer().public_key.0[..].to_vec(),
        amount: money_used,
    })
    .sign(&*node.signer());
    let tx_hash = transaction.get_hash();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &tx_hash);

    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    let transaction = TransactionBody::SwapKey(SwapKeyTransaction {
        nonce: node.get_account_nonce(&eve_account()).unwrap_or_default() + 1,
        originator: eve_account(),
        cur_key: node.signer().public_key.0[..].to_vec(),
        new_key: signer2.public_key.0[..].to_vec(),
    })
    .sign(&*node.signer());
    let tx_hash = transaction.get_hash();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &tx_hash);

    let transaction_result = node_user.get_transaction_result(&tx_hash);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert_eq!(transaction_result.receipts.len(), 0);
    let new_root1 = node_user.get_state_root();
    assert_ne!(new_root, new_root1);

    let account = node_user.view_account(&eve_account()).unwrap();
    assert_eq!(account.public_keys, vec![signer2.public_key]);
}

pub fn test_add_key(node: impl Node) {
    let account_id = &node.signer().account_id;
    let signer2 = InMemorySigner::from_random();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction = TransactionBody::AddKey(AddKeyTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        new_key: signer2.public_key.0[..].to_vec(),
    })
    .sign(&*node.signer());
    let tx_hash = transaction.get_hash();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &tx_hash);

    let transaction_result = node_user.get_transaction_result(&tx_hash);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert_eq!(transaction_result.receipts.len(), 0);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.public_keys.len(), 2);
    assert_eq!(account.public_keys[1].clone(), signer2.public_key);
}

pub fn test_add_existing_key(node: impl Node) {
    let account_id = &node.signer().account_id;
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction = TransactionBody::AddKey(AddKeyTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        new_key: node.signer().public_key.0[..].to_vec(),
    })
    .sign(&*node.signer());
    let tx_hash = transaction.get_hash();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &tx_hash);

    let transaction_result = node_user.get_transaction_result(&tx_hash);
    assert_eq!(transaction_result.status, TransactionStatus::Failed);
    assert_eq!(transaction_result.receipts.len(), 0);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.public_keys.len(), 1);
}

pub fn test_delete_key(node: impl Node) {
    let account_id = &node.signer().account_id;
    let signer2 = InMemorySigner::from_random();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction = TransactionBody::AddKey(AddKeyTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        new_key: signer2.public_key.0[..].to_vec(),
    })
    .sign(&*node.signer());
    let tx_hash = transaction.get_hash();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &tx_hash);

    let transaction_result = node_user.get_transaction_result(&tx_hash);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert_eq!(transaction_result.receipts.len(), 0);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let transaction = TransactionBody::DeleteKey(DeleteKeyTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        cur_key: node.signer().public_key.0[..].to_vec(),
    })
    .sign(&*node.signer());
    let tx_hash = transaction.get_hash();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &tx_hash);

    let transaction_result = node_user.get_transaction_result(&tx_hash);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert_eq!(transaction_result.receipts.len(), 0);
    let new_root1 = node_user.get_state_root();
    assert_ne!(new_root1, new_root);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.public_keys.len(), 1);
    assert_eq!(account.public_keys[0].clone(), signer2.public_key);
}

pub fn test_delete_key_not_owned(node: impl Node) {
    let account_id = &node.signer().account_id;
    let signer2 = InMemorySigner::from_random();
    let node_user = node.user();
    let root = node_user.get_state_root();

    let transaction = TransactionBody::DeleteKey(DeleteKeyTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        cur_key: signer2.public_key.0[..].to_vec(),
    })
    .sign(&*node.signer());
    let tx_hash = transaction.get_hash();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &tx_hash);

    let transaction_result = node_user.get_transaction_result(&tx_hash);
    assert_eq!(transaction_result.status, TransactionStatus::Failed);
    assert_eq!(transaction_result.receipts.len(), 0);
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.public_keys.len(), 1);
}

pub fn test_delete_key_no_key_left(node: impl Node) {
    let account_id = &node.signer().account_id;
    let node_user = node.user();
    let root = node_user.get_state_root();

    let transaction = TransactionBody::DeleteKey(DeleteKeyTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        cur_key: node.signer().public_key.0[..].to_vec(),
    })
    .sign(&*node.signer());
    let tx_hash = transaction.get_hash();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &tx_hash);

    let transaction_result = node_user.get_transaction_result(&tx_hash);
    assert_eq!(transaction_result.status, TransactionStatus::Failed);
    assert_eq!(transaction_result.receipts.len(), 0);
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.public_keys.len(), 1);
}

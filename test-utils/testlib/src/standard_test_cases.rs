use near::config::{TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
use near_primitives::account::AccessKey;
use near_primitives::crypto::signer::InMemorySigner;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::rpc::AccountViewCallResult;
use near_primitives::serialize::Decode;
use near_primitives::transaction::{
    AddKeyTransaction, AsyncCall, Callback, CallbackInfo, CallbackResult, CreateAccountTransaction,
    DeleteKeyTransaction, DeployContractTransaction, FinalTransactionStatus,
    FunctionCallTransaction, ReceiptBody, ReceiptTransaction, SwapKeyTransaction, TransactionBody,
    TransactionStatus,
};
use near_primitives::types::Balance;
use near_primitives::utils::key_for_callback;
use near_store::set;

use crate::node::{Node, RuntimeNode};
use crate::runtime_utils::{bob_account, default_code_hash, encode_int, eve_account};
use crate::test_helpers::wait;
use crate::user::User;

/// The amount to send with function call.
const FUNCTION_CALL_AMOUNT: Balance = 1_000_000_000_000;

/// validate transaction result in the case that it is successful and generates given number of receipts
/// recursively.
pub fn validate_tx_result(
    node_user: Box<dyn User>,
    root: CryptoHash,
    hash: &CryptoHash,
    receipt_depth: usize,
) {
    let mut transaction_result = node_user.get_transaction_result(&hash);
    if transaction_result.status != TransactionStatus::Completed {
        println!("Tx failed: {:?}", transaction_result);
    }
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    for _ in 0..receipt_depth {
        assert_eq!(transaction_result.receipts.len(), 1);
        transaction_result = node_user.get_transaction_result(&transaction_result.receipts[0]);
        if transaction_result.status != TransactionStatus::Completed {
            println!("Tx failed: {:?}", transaction_result);
        }
        assert_eq!(transaction_result.status, TransactionStatus::Completed);
    }
    assert_eq!(transaction_result.receipts.len(), 0);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

/// Adds given access key to the given account_id using signer2. Ruturns account_id and signer.
#[allow(clippy::borrowed_box)]
fn add_access_key(
    node: &impl Node,
    node_user: &Box<dyn User>,
    access_key: &AccessKey,
    signer2: &InMemorySigner,
) {
    let root = node_user.get_state_root();
    let account_id = &node.account_id().unwrap();
    let transaction = TransactionBody::AddKey(AddKeyTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        new_key: signer2.public_key.0[..].to_vec(),
        access_key: Some(access_key.clone()),
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
}

/// Wait until transaction finishes (either succeeds or fails).
#[allow(clippy::borrowed_box)]
pub fn wait_for_transaction(node_user: &Box<dyn User>, hash: &CryptoHash) {
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
    let account_id = &node.account_id().unwrap();
    let transaction = TransactionBody::FunctionCall(FunctionCallTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        contract_id: bob_account(),
        method_name: b"run_test".to_vec(),
        args: vec![],
        amount: FUNCTION_CALL_AMOUNT,
    })
    .sign(&*node.signer());

    let node_user = node.user();
    let hash = transaction.get_hash();
    let root = node_user.get_state_root();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &hash);
    validate_tx_result(node_user, root, &hash, 2);
}

pub fn test_smart_contract_self_call(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let transaction = TransactionBody::FunctionCall(FunctionCallTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        contract_id: account_id.clone(),
        method_name: b"run_test".to_vec(),
        args: vec![],
        amount: FUNCTION_CALL_AMOUNT,
    })
    .sign(&*node.signer());

    let node_user = node.user();
    let hash = transaction.get_hash();
    let root = node_user.get_state_root();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &hash);
    validate_tx_result(node_user, root, &hash, 0);
}

pub fn test_smart_contract_bad_method_name(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let transaction = TransactionBody::FunctionCall(FunctionCallTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        contract_id: bob_account(),
        method_name: b"_run_test".to_vec(),
        args: vec![],
        amount: FUNCTION_CALL_AMOUNT,
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
    let account_id = &node.account_id().unwrap();
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
    let account_id = &node.account_id().unwrap();
    let transaction = TransactionBody::FunctionCall(FunctionCallTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        contract_id: bob_account(),
        method_name: vec![],
        args: vec![],
        amount: FUNCTION_CALL_AMOUNT,
    })
    .sign(&*node.signer());

    let node_user = node.user();
    let hash = transaction.get_hash();
    let root = node_user.get_state_root();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &hash);
    validate_tx_result(node_user, root, &hash, 1);
}

pub fn test_smart_contract_with_args(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let transaction = TransactionBody::FunctionCall(FunctionCallTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        contract_id: bob_account(),
        method_name: b"run_test".to_vec(),
        args: (2..4).flat_map(|x| encode_int(x).to_vec()).collect(),
        amount: FUNCTION_CALL_AMOUNT,
    })
    .sign(&*node.signer());

    let node_user = node.user();
    let hash = transaction.get_hash();
    let root = node_user.get_state_root();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &hash);
    validate_tx_result(node_user, root, &hash, 2);
}

pub fn test_async_call_with_no_callback(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let nonce = hash(&[1, 2, 3]);
    let receipt = ReceiptTransaction {
        originator: account_id.clone(),
        receiver: bob_account(),
        nonce,
        body: ReceiptBody::NewCall(AsyncCall::new(
            b"run_test".to_vec(),
            vec![],
            FUNCTION_CALL_AMOUNT,
            account_id.clone(),
            account_id.clone(),
            node.signer().public_key().clone(),
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
    let account_id = &node.account_id().unwrap();
    let args = (7..9).flat_map(|x| encode_int(x).to_vec()).collect();
    let refund_account = account_id;
    let mut callback = Callback::new(
        b"sum_with_input".to_vec(),
        args,
        FUNCTION_CALL_AMOUNT,
        refund_account.clone(),
        account_id.clone(),
        node.signer().public_key().clone(),
    );
    callback.results.resize(1, None);
    let callback_id = [0; 32].to_vec();
    let mut async_call = AsyncCall::new(
        b"run_test".to_vec(),
        vec![],
        FUNCTION_CALL_AMOUNT,
        refund_account.clone(),
        account_id.clone(),
        node.signer().public_key().clone(),
    );
    let callback_info = CallbackInfo::new(callback_id.clone(), 0, account_id.clone());
    async_call.callback = Some(callback_info.clone());
    let receipt = ReceiptTransaction::new(
        account_id.clone(),
        bob_account(),
        hash(&[1, 2, 3]),
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
    // TODO: Check refund receipt.
}

pub fn test_async_call_with_logs(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let nonce = hash(&[1, 2, 3]);
    let receipt = ReceiptTransaction {
        originator: account_id.clone(),
        receiver: bob_account(),
        nonce,
        body: ReceiptBody::NewCall(AsyncCall::new(
            b"log_something".to_vec(),
            vec![],
            FUNCTION_CALL_AMOUNT,
            account_id.clone(),
            account_id.clone(),
            node.signer().public_key().clone(),
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
    let account_id = &node.account_id().unwrap();
    let args = (7..9).flat_map(|x| encode_int(x).to_vec()).collect();
    let refund_account = account_id;
    let mut callback = Callback::new(
        b"sum_with_input".to_vec(),
        args,
        0,
        refund_account.clone(),
        account_id.clone(),
        node.signer().public_key().clone(),
    );
    callback.results.resize(1, None);
    let callback_id = [0; 32].to_vec();
    let mut async_call = AsyncCall::new(
        vec![],
        vec![],
        0,
        refund_account.clone(),
        account_id.clone(),
        node.signer().public_key().clone(),
    );
    let callback_info = CallbackInfo::new(callback_id.clone(), 0, account_id.clone());
    async_call.callback = Some(callback_info.clone());
    let receipt = ReceiptTransaction::new(
        account_id.clone(),
        bob_account(),
        hash(&[1, 2, 3]),
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
    let account_id = &node.account_id().unwrap();
    let refund_account = account_id;
    let mut callback = Callback::new(
        b"run_test_with_storage_change".to_vec(),
        vec![],
        FUNCTION_CALL_AMOUNT,
        refund_account.clone(),
        account_id.clone(),
        node.signer().public_key().clone(),
    );
    callback.results.resize(1, None);
    let callback_id = [0; 32].to_vec();

    let mut state_update = node.client.read().unwrap().get_state_update();
    set(&mut state_update, key_for_callback(&callback_id), &callback);
    let (transaction, root) =
        state_update.finalize().unwrap().into(node.client.read().unwrap().trie.clone()).unwrap();
    {
        let mut client = node.client.write().unwrap();
        client.state_root = root;
        transaction.commit().unwrap();
    }

    let callback_info = CallbackInfo::new(callback_id.clone(), 0, account_id.clone());
    let receipt = ReceiptTransaction::new(
        account_id.clone(),
        bob_account(),
        hash(&[1, 2, 3]),
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
    let account_id = &node.account_id().unwrap();
    let refund_account = account_id;
    let mut callback = Callback::new(
        b"a_function_that_does_not_exist".to_vec(),
        vec![],
        0,
        refund_account.clone(),
        account_id.clone(),
        node.signer().public_key().clone(),
    );
    callback.results.resize(1, None);
    let callback_id = [0; 32].to_vec();
    let mut state_update = node.client.read().unwrap().get_state_update();
    set(&mut state_update, key_for_callback(&callback_id.clone()), &callback);
    let (transaction, root) =
        state_update.finalize().unwrap().into(node.client.read().unwrap().trie.clone()).unwrap();
    {
        let mut client = node.client.write().unwrap();
        client.state_root = root;
        transaction.commit().unwrap();
    }

    let callback_info = CallbackInfo::new(callback_id.clone(), 0, account_id.clone());
    let receipt = ReceiptTransaction::new(
        account_id.clone(),
        bob_account(),
        hash(&[1, 2, 3]),
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
    let account_id = &node.account_id().unwrap();
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
    let account_id = &node.account_id().unwrap();
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
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction = TransactionBody::CreateAccount(CreateAccountTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        new_account_id: eve_account(),
        public_key: node.signer().public_key().0[..].to_vec(),
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
    let account_id = &node.account_id().unwrap();
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
    let account_id = &node.account_id().unwrap();
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
            public_keys: vec![node.signer().public_key()],
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
    let account_id = &node.account_id().unwrap();
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
            public_keys: vec![node.signer().public_key()],
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
    let account_id = &node.account_id().unwrap();
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
            public_keys: vec![node.signer().public_key()],
            amount: TESTING_INIT_BALANCE,
            stake: TESTING_INIT_STAKE,
            code_hash: default_code_hash(),
        }
    );
    let result2 = node_user.view_account(&eve_account());
    assert!(result2.is_err());
}

pub fn test_create_account(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = 10;
    let transaction = TransactionBody::CreateAccount(CreateAccountTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        new_account_id: eve_account(),
        public_key: node.signer().public_key().0[..].to_vec(),
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
            public_keys: vec![node.signer().public_key()],
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
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let money_used = 10;
    let transaction = TransactionBody::CreateAccount(CreateAccountTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        new_account_id: eve_account(),
        public_key: node.signer().public_key().0[..].to_vec(),
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
            public_keys: vec![node.signer().public_key()],
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
        public_key: node.signer().public_key().0[..].to_vec(),
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
            public_keys: vec![node.signer().public_key()],
            amount: TESTING_INIT_BALANCE - money_used,
            stake: TESTING_INIT_STAKE,
            code_hash: default_code_hash(),
        }
    );
}

pub fn test_create_account_failure_invalid_name(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let mut root = node_user.get_state_root();
    let money_used = 10;
    for (counter, invalid_account_name) in [
        "eve",                               // too short
        "Alice.near",                        // capital letter
        "alice(near)",                       // brackets are invalid
        "long_of_the_name_for_real_is_hard", // too long
        "qq@qq*qq",                          // * is invalid
    ]
    .iter()
    .enumerate()
    {
        let transaction = TransactionBody::CreateAccount(CreateAccountTransaction {
            nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
            originator: account_id.clone(),
            new_account_id: invalid_account_name.to_string(),
            public_key: node.signer().public_key().0[..].to_vec(),
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
                nonce: counter as u64 + 1,
                account_id: account_id.clone(),
                public_keys: vec![node.signer().public_key()],
                amount: TESTING_INIT_BALANCE,
                stake: TESTING_INIT_STAKE,
                code_hash: default_code_hash(),
            }
        );
    }
}

pub fn test_create_account_failure_already_exists(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = 10;
    let transaction = TransactionBody::CreateAccount(CreateAccountTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        new_account_id: bob_account(),
        public_key: node.signer().public_key().0[..].to_vec(),
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
            public_keys: vec![node.signer().public_key()],
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
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = 10;
    let transaction = TransactionBody::CreateAccount(CreateAccountTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        new_account_id: eve_account(),
        public_key: node.signer().public_key().0[..].to_vec(),
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
        cur_key: node.signer().public_key().0[..].to_vec(),
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
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction = TransactionBody::AddKey(AddKeyTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        new_key: signer2.public_key.0[..].to_vec(),
        access_key: None,
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
    assert_eq!(account.public_keys[1], signer2.public_key);
}

pub fn test_add_existing_key(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction = TransactionBody::AddKey(AddKeyTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        new_key: node.signer().public_key().0[..].to_vec(),
        access_key: None,
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
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction = TransactionBody::AddKey(AddKeyTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        new_key: signer2.public_key.0[..].to_vec(),
        access_key: None,
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
        cur_key: node.signer().public_key().0[..].to_vec(),
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
    assert_eq!(account.public_keys[0], signer2.public_key);
}

pub fn test_delete_key_not_owned(node: impl Node) {
    let account_id = &node.account_id().unwrap();
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

pub fn test_delete_key_last(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();

    let transaction = TransactionBody::DeleteKey(DeleteKeyTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        cur_key: node.signer().public_key().0[..].to_vec(),
    })
    .sign(&*node.signer());
    let tx_hash = transaction.get_hash();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &tx_hash);

    let transaction_result = node_user.get_transaction_result(&tx_hash);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert_eq!(transaction_result.receipts.len(), 0);
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.public_keys.len(), 0);
}

pub fn test_add_access_key(node: impl Node) {
    let node_user = node.user();
    let access_key =
        AccessKey { amount: 0, balance_owner: None, contract_id: None, method_name: None };
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random();
    add_access_key(&node, &node_user, &access_key, &signer2);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.public_keys.len(), 1);

    let view_access_key = node_user.get_access_key(account_id, &signer2.public_key).unwrap();
    assert_eq!(view_access_key, Some(access_key));
}

pub fn test_delete_access_key(node: impl Node) {
    let node_user = node.user();
    let access_key =
        AccessKey { amount: 0, balance_owner: None, contract_id: None, method_name: None };
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random();
    add_access_key(&node, &node_user, &access_key, &signer2);

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
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert_eq!(transaction_result.receipts.len(), 0);
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.public_keys.len(), 1);
    assert_eq!(account.public_keys[0], node.signer().public_key());

    let view_access_key = node_user.get_access_key(account_id, &signer2.public_key).unwrap();
    assert_eq!(view_access_key, None);
}

pub fn test_add_access_key_with_funding(node: impl Node) {
    let access_key =
        AccessKey { amount: 10, balance_owner: None, contract_id: None, method_name: None };
    let node_user = node.user();
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random();
    let account = node_user.view_account(account_id).unwrap();
    let initial_balance = account.amount;
    add_access_key(&node, &node_user, &access_key, &signer2);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.public_keys.len(), 1);
    assert_eq!(account.amount, initial_balance - 10);

    let view_access_key = node_user.get_access_key(account_id, &signer2.public_key).unwrap();
    assert_eq!(view_access_key, Some(access_key));
}

pub fn test_delete_access_key_with_owner_refund(node: impl Node) {
    let access_key =
        AccessKey { amount: 10, balance_owner: None, contract_id: None, method_name: None };
    let node_user = node.user();
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random();
    let account = node_user.view_account(account_id).unwrap();
    let initial_balance = account.amount;
    add_access_key(&node, &node_user, &access_key, &signer2);

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
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert_eq!(transaction_result.receipts.len(), 0);
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.public_keys.len(), 1);
    assert_eq!(account.public_keys[0], node.signer().public_key());
    assert_eq!(account.amount, initial_balance);

    let view_access_key = node_user.get_access_key(account_id, &signer2.public_key).unwrap();
    assert_eq!(view_access_key, None);
}

pub fn test_delete_access_key_with_bob_refund(node: impl Node) {
    let access_key = AccessKey {
        amount: 10,
        balance_owner: Some(bob_account()),
        contract_id: None,
        method_name: None,
    };
    let node_user = node.user();
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random();
    let account = node_user.view_account(account_id).unwrap();
    let initial_balance = account.amount;
    let bob_account_result = node_user.view_account(&bob_account()).unwrap();
    let bobs_initial_balance = bob_account_result.amount;
    add_access_key(&node, &node_user, &access_key, &signer2);

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
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert_eq!(transaction_result.receipts.len(), 1);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.public_keys.len(), 1);
    assert_eq!(account.public_keys[0], node.signer().public_key());
    assert_eq!(account.amount, initial_balance - 10);

    let view_access_key = node_user.get_access_key(account_id, &signer2.public_key).unwrap();
    assert_eq!(view_access_key, None);

    wait_for_transaction(&node_user, &transaction_result.receipts[0]);
    let transaction_result = node_user.get_transaction_result(&transaction_result.receipts[0]);
    assert_eq!(transaction_result.status, TransactionStatus::Completed);
    assert!(transaction_result.receipts.is_empty());
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    let bob_account_result = node_user.view_account(&bob_account()).unwrap();
    assert_eq!(bob_account_result.amount, bobs_initial_balance + 10);
}

pub fn test_access_key_smart_contract(node: impl Node) {
    let access_key = AccessKey {
        amount: FUNCTION_CALL_AMOUNT,
        balance_owner: None,
        contract_id: Some(bob_account()),
        method_name: None,
    };
    let node_user = node.user();
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random();
    add_access_key(&node, &node_user, &access_key, &signer2);

    let transaction = TransactionBody::FunctionCall(FunctionCallTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        contract_id: bob_account(),
        method_name: b"run_test".to_vec(),
        args: vec![],
        amount: FUNCTION_CALL_AMOUNT,
    })
    .sign(&signer2);

    let hash = transaction.get_hash();
    let root = node_user.get_state_root();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &hash);
    validate_tx_result(node_user, root, &hash, 2);
}

pub fn test_access_key_smart_contract_reject_method_name(node: impl Node) {
    let access_key = AccessKey {
        amount: 0,
        balance_owner: None,
        contract_id: Some(bob_account()),
        method_name: Some(b"log_something".to_vec()),
    };
    let node_user = node.user();
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random();
    add_access_key(&node, &node_user, &access_key, &signer2);

    let transaction = TransactionBody::FunctionCall(FunctionCallTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        contract_id: bob_account(),
        method_name: b"run_test".to_vec(),
        args: vec![],
        amount: FUNCTION_CALL_AMOUNT,
    })
    .sign(&signer2);

    let hash = transaction.get_hash();
    let root = node_user.get_state_root();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &hash);
    let transaction_result = node_user.get_transaction_result(&hash);
    assert_eq!(transaction_result.status, TransactionStatus::Failed);
    assert_eq!(transaction_result.receipts.len(), 0);
    let new_root = node_user.get_state_root();
    assert_eq!(root, new_root);
}

pub fn test_access_key_smart_contract_reject_contract_id(node: impl Node) {
    let access_key = AccessKey {
        amount: 0,
        balance_owner: None,
        contract_id: Some(bob_account()),
        method_name: None,
    };
    let node_user = node.user();
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random();
    add_access_key(&node, &node_user, &access_key, &signer2);

    let transaction = TransactionBody::FunctionCall(FunctionCallTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        contract_id: eve_account(),
        method_name: b"run_test".to_vec(),
        args: vec![],
        amount: FUNCTION_CALL_AMOUNT,
    })
    .sign(&signer2);

    let hash = transaction.get_hash();
    let root = node_user.get_state_root();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &hash);
    let transaction_result = node_user.get_transaction_result(&hash);
    assert_eq!(transaction_result.status, TransactionStatus::Failed);
    assert_eq!(transaction_result.receipts.len(), 0);
    let new_root = node_user.get_state_root();
    assert_eq!(root, new_root);
}

pub fn test_access_key_reject_non_function_call(node: impl Node) {
    let access_key =
        AccessKey { amount: 0, balance_owner: None, contract_id: None, method_name: None };
    let node_user = node.user();
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random();
    add_access_key(&node, &node_user, &access_key, &signer2);

    let transaction = TransactionBody::DeleteKey(DeleteKeyTransaction {
        nonce: node.get_account_nonce(account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        cur_key: node.signer().public_key().0[..].to_vec(),
    })
    .sign(&signer2);

    let hash = transaction.get_hash();
    let root = node_user.get_state_root();
    node_user.add_transaction(transaction).unwrap();
    wait_for_transaction(&node_user, &hash);
    let transaction_result = node_user.get_transaction_result(&hash);
    assert_eq!(transaction_result.status, TransactionStatus::Failed);
    assert_eq!(transaction_result.receipts.len(), 0);
    let new_root = node_user.get_state_root();
    assert_eq!(root, new_root);
}

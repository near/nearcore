use storage::{StateDbUpdate};
use primitives::types::{AccountId, AccountingInfo, AuthorityStake};
use primitives::traits::{Encode, Decode};
use primitives::hash::{hash, CryptoHash};
use primitives::signature::PublicKey;
use primitives::utils::is_valid_account_id;
use transaction::{
    AsyncCall, ReceiptTransaction, SendMoneyTransaction,
    ReceiptBody, StakeTransaction, CreateAccountTransaction,
    DeployContractTransaction, SwapKeyTransaction
};

use super::{COL_ACCOUNT, COL_CODE, set, account_id_to_bytes, Account, create_nonce_with_nonce};
use crate::{TxTotalStake, get_tx_stake_key};

/// const does not allow function call, so have to resort to this
pub fn system_account() -> AccountId { "system".to_string() }

pub const SYSTEM_METHOD_CREATE_ACCOUNT: &[u8] = b"_sys:create_account";
pub const SYSTEM_METHOD_DEPLOY: &[u8] = b"_sys:deploy";

pub fn send_money(
    state_update: &mut StateDbUpdate,
    transaction: &SendMoneyTransaction,
    hash: CryptoHash,
    sender: &mut Account,
    accounting_info: AccountingInfo,
) -> Result<Vec<ReceiptTransaction>, String> {
    if transaction.amount == 0 {
        return Err("Sending 0 tokens".to_string());
    }
    if sender.amount >= transaction.amount {
        sender.amount -= transaction.amount;
        set(state_update, &account_id_to_bytes(COL_ACCOUNT, &transaction.originator), sender);
        let receipt = ReceiptTransaction::new(
            transaction.originator.clone(),
            transaction.receiver.clone(),
            create_nonce_with_nonce(&hash, 0),
            ReceiptBody::NewCall(AsyncCall::new(
                // Empty method name is used for deposit
                vec![],
                vec![],
                transaction.amount,
                0,
                accounting_info,
            ))
        );
        Ok(vec![receipt])
    } else {
        Err(
            format!(
                "Account {} tries to send {}, but has staked {} and only has {}",
                transaction.originator,
                transaction.amount,
                sender.staked,
                sender.amount,
            )
        )
    }
}

pub fn staking(
    state_update: &mut StateDbUpdate,
    body: &StakeTransaction,
    sender_account_id: &AccountId,
    sender: &mut Account,
    authority_proposals: &mut Vec<AuthorityStake>,
) -> Result<Vec<ReceiptTransaction>, String> {
    if sender.amount >= body.amount && sender.public_keys.is_empty() {
        authority_proposals.push(AuthorityStake {
            account_id: sender_account_id.clone(),
            public_key: sender.public_keys[0],
            amount: body.amount,
        });
        sender.amount -= body.amount;
        sender.staked += body.amount;
        set(state_update, &account_id_to_bytes(COL_ACCOUNT, sender_account_id), &sender);
        Ok(vec![])
    } else if sender.amount < body.amount {
        let err_msg = format!(
            "Account {} tries to stake {}, but has staked {} and only has {}",
            body.originator,
            body.amount,
            sender.staked,
            sender.amount,
        );
        Err(err_msg)
    } else {
        Err(format!("Account {} already staked", body.originator))
    }
}

pub fn deposit(
    state_update: &mut StateDbUpdate,
    amount: u64,
    receiver_id: &AccountId,
    receiver: &mut Account
) -> Result<Vec<ReceiptTransaction>, String> {
    receiver.amount += amount;
    set(
        state_update,
        &account_id_to_bytes(COL_ACCOUNT, &receiver_id),
        receiver
    );
    Ok(vec![])
}

pub fn create_account(
    state_update: &mut StateDbUpdate,
    body: &CreateAccountTransaction,
    hash: CryptoHash,
    sender: &mut Account,
    accounting_info: AccountingInfo,
) -> Result<Vec<ReceiptTransaction>, String> {
    if !is_valid_account_id(&body.new_account_id) {
        return Err(format!("Account {} does not match requirements", body.new_account_id));
    }
    if sender.amount >= body.amount {
        sender.amount -= body.amount;
        set(
            state_update,
            &account_id_to_bytes(COL_ACCOUNT, &body.originator),
            &sender
        );
        let new_nonce = create_nonce_with_nonce(&hash, 0);
        let receipt = ReceiptTransaction::new(
            body.originator.clone(),
            body.new_account_id.clone(),
            new_nonce,
            ReceiptBody::NewCall(AsyncCall::new(
                SYSTEM_METHOD_CREATE_ACCOUNT.to_vec(),
                body.public_key.clone(),
                body.amount,
                0,
                accounting_info,
            ))
        );
        Ok(vec![receipt])
    } else {
        Err(
            format!(
                "Account {} tries to create new account with {}, but only has {}",
                body.originator,
                body.amount,
                sender.amount
            )
        )
    }
}

pub fn deploy(
    body: &DeployContractTransaction,
    hash: CryptoHash,
    accounting_info: AccountingInfo,
) -> Result<Vec<ReceiptTransaction>, String> {
    // TODO: check signature
    
    let new_nonce = create_nonce_with_nonce(&hash, 0);
    let args = Encode::encode(&(&body.public_key, &body.wasm_byte_array))
        .map_err(|_| "cannot encode args")?;
    let receipt = ReceiptTransaction::new(
        body.originator.clone(),
        body.contract_id.clone(),
        new_nonce,
        ReceiptBody::NewCall(AsyncCall::new(
            SYSTEM_METHOD_DEPLOY.to_vec(),
            args,
            0,
            0,
            accounting_info,
        ))
    );
    Ok(vec![receipt])
}

pub fn swap_key(
    state_update: &mut StateDbUpdate,
    body: &SwapKeyTransaction,
    account: &mut Account,
) -> Result<Vec<ReceiptTransaction>, String> {
    let cur_key = Decode::decode(&body.cur_key).map_err(|_| "cannot decode public key")?;
    let new_key = Decode::decode(&body.new_key).map_err(|_| "cannot decode public key")?;
    let num_keys = account.public_keys.len();
    account.public_keys.retain(|&x| x != cur_key);
    if account.public_keys.len() == num_keys {
        return Err(format!("Account {} does not have public key {}", body.originator, cur_key));
    }
    account.public_keys.push(new_key);
    set(
        state_update,
        &account_id_to_bytes(COL_ACCOUNT, &body.originator),
        &account
    );
    Ok(vec![])
}



pub fn system_create_account(
    state_update: &mut StateDbUpdate,
    call: &AsyncCall,
    account_id: &AccountId,
) -> Result<Vec<ReceiptTransaction>, String> {
    if !is_valid_account_id(account_id) {
        return Err(format!("Account {} does not match requirements", account_id));
    }
    let account_id_bytes = account_id_to_bytes(COL_ACCOUNT, &account_id);
   
    let public_key = PublicKey::new(&call.args)?;
    let new_account = Account::new(
        vec![public_key],
        call.amount,
        hash(&[])
    );
    set(
        state_update,
        &account_id_bytes,
        &new_account
    );
    // TODO(#347): Remove default TX staking once tx staking is properly implemented
    let mut tx_total_stake = TxTotalStake::new(0);
    tx_total_stake.add_active_stake(100);
    set(
        state_update,
        &get_tx_stake_key(&account_id, &None),
        &tx_total_stake,
    );
    Ok(vec![])
}


pub fn system_deploy(
    state_update: &mut StateDbUpdate,
    call: &AsyncCall,
    account_id: &AccountId,
) -> Result<Vec<ReceiptTransaction>, String> {
    let (public_key, code): (Vec<u8>, Vec<u8>) =
        Decode::decode(&call.args).map_err(|_| "cannot decode public key")?;
    let public_key = PublicKey::new(&public_key)?;
    let new_account = Account::new(
        vec![public_key],
        call.amount,
        hash(&code),
    );
    set(
        state_update,
        &account_id_to_bytes(COL_ACCOUNT, account_id),
        &new_account
    );
    set(
        state_update,
        &account_id_to_bytes(COL_CODE, account_id),
        &code
    );
    Ok(vec![])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::*;
    use primitives::hash::hash;
    use primitives::signature::{get_key_pair, sign};
    use transaction::{TransactionBody, SignedTransaction};
    use crate::state_viewer::{AccountViewCallResult, StateDbViewer};
    use crate::{get, ApplyState};
    use transaction::TransactionStatus;

    #[test]
    fn test_upload_contract() {
        let (runtime, state_db, root) = get_runtime_and_state_db();
        let wasm_binary = include_bytes!("../../../core/wasm/runtest/res/wasm_with_mem.wasm");
        let mut alice = User::new(runtime, &alice_account(), state_db.clone());
        let (public_key, _) = get_key_pair();
        let (new_root, mut apply_results) = alice.deploy_contract(
            root, &eve_account(), public_key, wasm_binary
        );
        let apply_result = apply_results.pop().unwrap();
        assert_eq!(apply_result.tx_result[0].status, TransactionStatus::Completed);
        assert_eq!(apply_result.new_receipts.len(), 0);
        assert_ne!(root, new_root);
        let mut new_state_update = StateDbUpdate::new(state_db, new_root);
        let code: Vec<u8> = get(
            &mut new_state_update,
            &account_id_to_bytes(COL_CODE, &eve_account())
        ).unwrap();
        assert_eq!(code, wasm_binary.to_vec());
    }

    #[test]
    fn test_redeploy_contract() {
        let test_binary = b"test_binary";
        let (runtime, state_db, root) = get_runtime_and_state_db();
        let mut state_update = StateDbUpdate::new(state_db.clone(), root);
        let account: Account = get(
            &mut state_update,
            &account_id_to_bytes(COL_ACCOUNT, &bob_account())
        ).unwrap();
        let mut bob = User::new(runtime, &bob_account(), state_db.clone());
        let (new_root, mut apply_results) = bob.deploy_contract(
            root, &bob_account(), account.public_keys[0], test_binary
        );
        let apply_result = apply_results.pop().unwrap();
        assert_eq!(apply_result.tx_result[0].status, TransactionStatus::Completed);
        assert_eq!(apply_result.new_receipts.len(), 0);
        assert_ne!(root, new_root);
        let mut new_state_update = StateDbUpdate::new(state_db, new_root);
        let code: Vec<u8> = get(
            &mut new_state_update,
            &account_id_to_bytes(COL_CODE, &bob_account())
        ).unwrap();
        assert_eq!(code, test_binary.to_vec())
    }

    #[test]
    fn test_send_money() {
        let (runtime, state_db, root) = get_runtime_and_state_db();
        let mut alice = User::new(runtime, &alice_account(), state_db.clone());
        let (new_root, apply_results) = alice.send_money(root, &bob_account(), 10);
        for apply_result in apply_results {
            assert_eq!(apply_result.tx_result[0].status, TransactionStatus::Completed);
        }
        assert_ne!(root, new_root);
        let viewer = StateDbViewer::new(state_db.clone());
        let result1 = viewer.view_account(new_root, &alice_account());
        assert_eq!(
            result1.unwrap(),
            AccountViewCallResult {
                nonce: 1,
                account: alice_account(),
                amount: 90,
                stake: 50,
                code_hash: default_code_hash(),
            }
        );
        let result2 = viewer.view_account(new_root, &bob_account());
        assert_eq!(
            result2.unwrap(),
            AccountViewCallResult {
                nonce: 0,
                account: bob_account(),
                amount: 10,
                stake: 0,
                code_hash: default_code_hash(),
            }
        );
    }

    #[test]
    fn test_send_money_over_balance() {
        let (runtime, state_db, root) = get_runtime_and_state_db();
        let mut alice = User::new(runtime, &alice_account(), state_db.clone());
        let (new_root, mut apply_results) = alice.send_money(root, &bob_account(), 1000);
        let apply_result = apply_results.pop().unwrap();
        assert_eq!(apply_result.tx_result[0].status, TransactionStatus::Failed);
        assert_eq!(apply_result.new_receipts.len(), 0);
        assert_eq!(root, new_root);
        let viewer = StateDbViewer::new(state_db.clone());
        let result1 = viewer.view_account(new_root, &alice_account());
        assert_eq!(
            result1.unwrap(),
            AccountViewCallResult {
                nonce: 0,
                account: alice_account(),
                amount: 100,
                stake: 50,
                code_hash: default_code_hash(),
            }
        );
        let result2 = viewer.view_account(apply_result.root, &bob_account());
        assert_eq!(
            result2.unwrap(),
            AccountViewCallResult {
                nonce: 0,
                account: bob_account(),
                amount: 0,
                stake: 0,
                code_hash: default_code_hash(),
            }
        );
    }

    #[test]
    fn test_refund_on_send_money_to_non_existent_account() {
        let (runtime, state_db, root) = get_runtime_and_state_db();
        let mut alice = User::new(runtime, &alice_account(), state_db.clone());
        let (new_root, apply_results) = alice.send_money(root, &eve_account(), 10);
        // 3 results: signed tx, deposit receipt, refund
        assert_eq!(apply_results.len(), 3);
        assert_eq!(apply_results[0].tx_result[0].status, TransactionStatus::Completed);
        // deposit receipt failed because account does not exist
        assert_eq!(apply_results[1].tx_result[0].status, TransactionStatus::Failed);
        assert_eq!(apply_results[2].tx_result[0].status, TransactionStatus::Completed);
        // assert_eq!(apply_result.new_receipts.len(), 0);
        let viewer = StateDbViewer::new(state_db.clone());
        let result1 = viewer.view_account(new_root, &alice_account());
        assert_eq!(
            result1.unwrap(),
            AccountViewCallResult {
                nonce: 1,
                account: alice_account(),
                amount: 100,
                stake: 50,
                code_hash: default_code_hash(),
            }
        );
        let result2 = viewer.view_account(new_root, &eve_account());
        assert!(result2.is_err());
    }

    #[test]
    fn test_create_account() {
        let (runtime, state_db, root) = get_runtime_and_state_db();
        let mut alice = User::new(runtime, &alice_account(), state_db.clone());
        let (new_root, _) = alice.create_account(root, &eve_account(), 10);
        assert_ne!(root, new_root);
        let viewer = StateDbViewer::new(state_db.clone());
        let result1 = viewer.view_account(new_root, &alice_account());
        assert_eq!(
            result1.unwrap(),
            AccountViewCallResult {
                nonce: 1,
                account: alice_account(),
                amount: 90,
                stake: 50,
                code_hash: default_code_hash(),
            }
        );
        let result2 = viewer.view_account(new_root, &eve_account());
        assert_eq!(
            result2.unwrap(),
            AccountViewCallResult {
                nonce: 0,
                account: eve_account(),
                amount: 10,
                stake: 0,
                code_hash: hash(b""),
            }
        );
    }

    #[test]
    fn test_create_account_failure_invalid_name() {
        let (runtime, state_db, root) = get_runtime_and_state_db();
        let mut alice = User::new(runtime, &alice_account(), state_db.clone());
        for invalid_account_name in vec![
                "eve", // too short
                "Alice.near", // capital letter
                "alice(near)", // brackets are invalid
                "long_of_the_name_for_real_is_hard", // too long
                "qq@qq*qq" // * is invalid
        ] { 
            let (new_root, _) = alice.create_account(root, invalid_account_name, 10);
            assert_eq!(root, new_root);
            let viewer = StateDbViewer::new(state_db.clone());
            let result1 = viewer.view_account(new_root, &alice_account());
            assert_eq!(
                result1.unwrap(),
                AccountViewCallResult {
                    nonce: 0,
                    account: alice_account(),
                    amount: 100,
                    stake: 50,
                    code_hash: default_code_hash(),
                }
            );
        }
    }

    #[test]
    fn test_create_account_failure_already_exists() {
        let (runtime, state_db, root) = get_runtime_and_state_db();
        let mut alice = User::new(runtime, &alice_account(), state_db.clone());
        let (new_root, _) = alice.create_account(root, &bob_account(), 10);
        assert_ne!(root, new_root);
        let viewer = StateDbViewer::new(state_db.clone());
        let result1 = viewer.view_account(new_root, &alice_account());
        assert_eq!(
            result1.unwrap(),
            AccountViewCallResult {
                nonce: 1,
                account: alice_account(),
                amount: 100,
                stake: 50,
                code_hash: default_code_hash(),
            }
        );
        let result2 = viewer.view_account(new_root, &bob_account());
        assert_eq!(
            result2.unwrap(),
            AccountViewCallResult {
                nonce: 0,
                account: bob_account(),
                amount: 0,
                stake: 0,
                code_hash: default_code_hash(),
            }
        );
    }

    #[test]
    fn test_swap_key() {
        let (runtime, state_db, root) = get_runtime_and_state_db();
        let (pub_key1, secret_key1) = get_key_pair();
        let (pub_key2, _) = get_key_pair();
        let mut alice = User::new(runtime.clone(), &alice_account(), state_db.clone());
        let (new_root, apply_results) = alice.create_account_with_key(
            root, &eve_account(), 10, pub_key1
        );
        for apply_result in apply_results {
            assert_eq!(apply_result.tx_result[0].status, TransactionStatus::Completed);
        }
        assert_ne!(root, new_root);
        let tx_body = TransactionBody::SwapKey(SwapKeyTransaction {
            nonce: 2,
            originator: eve_account(),
            cur_key: pub_key1.encode().unwrap(),
            new_key: pub_key2.encode().unwrap(),
        });
        let data = tx_body.encode().unwrap();
        let signature = sign(&data, &secret_key1);
        let transaction1 = SignedTransaction::new(signature, tx_body);
        let apply_state = ApplyState {
            shard_id: 0,
            root: new_root,
            parent_block_hash: CryptoHash::default(),
            block_index: 0,
        };
        let state_update = StateDbUpdate::new(state_db.clone(), new_root);
        let apply_result = runtime.apply(
            state_update, &apply_state, &[], &[transaction1],
        );
        state_db.commit(apply_result.db_changes).unwrap();
        let mut new_state_update = StateDbUpdate::new(state_db.clone(), apply_result.root);
        let account = get::<Account>(
            &mut new_state_update,
            &account_id_to_bytes(COL_ACCOUNT, &eve_account()),
        ).unwrap();
        assert_eq!(account.public_keys, vec![pub_key2]);
    }
}
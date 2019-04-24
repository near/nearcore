use primitives::account::Account;
use primitives::crypto::aggregate_signature::BlsPublicKey;
use primitives::crypto::signature::PublicKey;
use primitives::hash::{hash, CryptoHash};
use primitives::traits::Base58Encoded;
use primitives::transaction::{
    AddKeyTransaction, AsyncCall, CallbackInfo, CallbackResult, CreateAccountTransaction,
    DeleteKeyTransaction, ReceiptBody, ReceiptTransaction, SendMoneyTransaction, StakeTransaction,
    SwapKeyTransaction,
};
use primitives::types::{AccountId, AccountingInfo, AuthorityStake};
use primitives::utils::{is_valid_account_id, create_nonce_with_nonce, key_for_code, key_for_account,
    key_for_tx_stake};
use std::convert::TryFrom;
use storage::{set, TrieUpdate};

use crate::TxTotalStake;
use wasm::types::ContractCode;

/// const does not allow function call, so have to resort to this
pub fn system_account() -> AccountId {
    "system".to_string()
}

pub const SYSTEM_METHOD_CREATE_ACCOUNT: &[u8] = b"_sys:create_account";

pub fn send_money(
    state_update: &mut TrieUpdate,
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
        set(state_update, &key_for_account(&transaction.originator), sender);
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
            )),
        );
        Ok(vec![receipt])
    } else {
        Err(format!(
            "Account {} tries to send {}, but has staked {} and only has {}",
            transaction.originator, transaction.amount, sender.staked, sender.amount,
        ))
    }
}

pub fn staking(
    state_update: &mut TrieUpdate,
    body: &StakeTransaction,
    sender_account_id: &AccountId,
    sender: &mut Account,
    authority_proposals: &mut Vec<AuthorityStake>,
) -> Result<Vec<ReceiptTransaction>, String> {
    if sender.amount >= body.amount {
        authority_proposals.push(AuthorityStake {
            account_id: sender_account_id.clone(),
            public_key: PublicKey::try_from(body.public_key.as_str())?,
            bls_public_key: BlsPublicKey::from_base58(&body.bls_public_key)
                .map_err(|e| format!("{}", e))?,
            amount: body.amount,
        });
        sender.amount -= body.amount;
        sender.staked += body.amount;
        set(state_update, &key_for_account(sender_account_id), &sender);
        Ok(vec![])
    } else {
        let err_msg = format!(
            "Account {} tries to stake {}, but has staked {} and only has {}",
            body.originator, body.amount, sender.staked, sender.amount,
        );
        Err(err_msg)
    }
}

pub fn deposit(
    state_update: &mut TrieUpdate,
    amount: u64,
    callback_info: &Option<CallbackInfo>,
    receiver_id: &AccountId,
    nonce: &CryptoHash,
    receiver: &mut Account,
) -> Result<Vec<ReceiptTransaction>, String> {
    let mut receipts = vec![];
    if let Some(callback_info) = callback_info {
        let new_nonce = create_nonce_with_nonce(&nonce, 0);
        let new_receipt = ReceiptTransaction::new(
            receiver_id.clone(),
            callback_info.receiver.clone(),
            new_nonce,
            ReceiptBody::Callback(CallbackResult::new(callback_info.clone(), Some(vec![]))),
        );
        receipts.push(new_receipt);
    }

    if amount > 0 {
        receiver.amount += amount;
        set(state_update, &key_for_account(&receiver_id), receiver);
    }
    Ok(receipts)
}

pub fn create_account(
    state_update: &mut TrieUpdate,
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
        set(state_update, &key_for_account(&body.originator), &sender);
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
            )),
        );
        Ok(vec![receipt])
    } else {
        Err(format!(
            "Account {} tries to create new account with {}, but only has {}",
            body.originator, body.amount, sender.amount
        ))
    }
}

pub fn deploy(
    state_update: &mut TrieUpdate,
    sender_id: &AccountId,
    code: &[u8],
    sender: &mut Account,
) -> Result<Vec<ReceiptTransaction>, String> {
    let code = ContractCode::new(code.to_vec());
    // Signature should be already checked at this point
    sender.code_hash = code.get_hash();
    set(state_update, &key_for_code(&sender_id), &code);
    set(state_update, &key_for_account(&sender_id), &sender);
    Ok(vec![])
}

pub fn swap_key(
    state_update: &mut TrieUpdate,
    body: &SwapKeyTransaction,
    account: &mut Account,
) -> Result<Vec<ReceiptTransaction>, String> {
    let cur_key = PublicKey::try_from(&body.cur_key as &[u8]).map_err(|e| format!("{}", e))?;
    let new_key = PublicKey::try_from(&body.new_key as &[u8]).map_err(|e| format!("{}", e))?;
    let num_keys = account.public_keys.len();
    account.public_keys.retain(|&x| x != cur_key);
    if account.public_keys.len() == num_keys {
        return Err(format!("Account {} does not have public key {}", body.originator, cur_key));
    }
    account.public_keys.push(new_key);
    set(state_update, &key_for_account(&body.originator), &account);
    Ok(vec![])
}

pub fn add_key(
    state_update: &mut TrieUpdate,
    body: &AddKeyTransaction,
    account: &mut Account,
) -> Result<Vec<ReceiptTransaction>, String> {
    let new_key = PublicKey::try_from(&body.new_key as &[u8]).map_err(|e| format!("{}", e))?;
    let num_keys = account.public_keys.len();
    account.public_keys.retain(|&x| x != new_key);
    if account.public_keys.len() < num_keys {
        return Err("Cannot add key that already exists".to_string());
    }
    account.public_keys.push(new_key);
    set(state_update, &key_for_account(&body.originator), &account);
    Ok(vec![])
}

pub fn delete_key(
    state_update: &mut TrieUpdate,
    body: &DeleteKeyTransaction,
    account: &mut Account,
) -> Result<Vec<ReceiptTransaction>, String> {
    let cur_key = PublicKey::try_from(&body.cur_key as &[u8]).map_err(|e| format!("{}", e))?;
    let num_keys = account.public_keys.len();
    account.public_keys.retain(|&x| x != cur_key);
    if account.public_keys.len() == num_keys {
        return Err(format!(
            "Account {} tries to remove a key that it does not own",
            body.originator
        ));
    }
    if account.public_keys.is_empty() {
        return Err("Account must have at least one public key".to_string());
    }
    set(state_update, &key_for_account(&body.originator), &account);
    Ok(vec![])
}

pub fn system_create_account(
    state_update: &mut TrieUpdate,
    call: &AsyncCall,
    account_id: &AccountId,
) -> Result<Vec<ReceiptTransaction>, String> {
    if !is_valid_account_id(account_id) {
        return Err(format!("Account {} does not match requirements", account_id));
    }
    let account_id_bytes = key_for_account(&account_id);

    let public_key = PublicKey::try_from(&call.args as &[u8]).map_err(|e| format!("{}", e))?;
    let new_account = Account::new(vec![public_key], call.amount, hash(&[]));
    set(state_update, &account_id_bytes, &new_account);
    // TODO(#347): Remove default TX staking once tx staking is properly implemented
    let mut tx_total_stake = TxTotalStake::new(0);
    tx_total_stake.add_active_stake(100);
    set(state_update, &key_for_tx_stake(&account_id, &None), &tx_total_stake);
    Ok(vec![])
}


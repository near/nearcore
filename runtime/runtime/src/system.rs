use std::convert::TryFrom;

use near_primitives::account::{AccessKey, Account};
use near_primitives::crypto::signature::PublicKey;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::transaction::{
    AddKeyTransaction, AsyncCall, CallbackInfo, CallbackResult, CreateAccountTransaction,
    DeleteKeyTransaction, ReceiptBody, ReceiptTransaction, SendMoneyTransaction, StakeTransaction,
    SwapKeyTransaction,
};
use near_primitives::types::{AccountId, Balance, ValidatorStake};
use near_primitives::utils::{
    create_nonce_with_nonce, is_valid_account_id, key_for_access_key, key_for_account, key_for_code,
};
use near_store::{get, set, TrieUpdate};
use wasm::types::ContractCode;

pub const SYSTEM_METHOD_CREATE_ACCOUNT: &[u8] = b"_sys:create_account";

const INVALID_ACCOUNT_ID: &str =
    "does not match requirements. Must be 5-32 characters (lower case letters/numbers or '@._-')";

pub fn send_money(
    state_update: &mut TrieUpdate,
    transaction: &SendMoneyTransaction,
    hash: CryptoHash,
    sender: &mut Account,
    refund_account_id: &AccountId,
    public_key: PublicKey,
) -> Result<Vec<ReceiptTransaction>, String> {
    if transaction.amount == 0 {
        return Err("Sending 0 tokens".to_string());
    }
    if sender.amount >= transaction.amount {
        sender.amount -= transaction.amount;
        set(state_update, key_for_account(&transaction.originator), sender);
        let receipt = ReceiptTransaction::new(
            transaction.originator.clone(),
            transaction.receiver.clone(),
            create_nonce_with_nonce(&hash, 0),
            ReceiptBody::NewCall(AsyncCall::new(
                // Empty method name is used for deposit
                vec![],
                vec![],
                transaction.amount,
                refund_account_id.clone(),
                transaction.originator.clone(),
                public_key,
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
    validator_proposals: &mut Vec<ValidatorStake>,
) -> Result<Vec<ReceiptTransaction>, String> {
    if sender.amount >= body.amount {
        validator_proposals.push(ValidatorStake {
            account_id: sender_account_id.clone(),
            public_key: PublicKey::try_from(body.public_key.as_str())
                .map_err(|err| err.to_string())?,
            amount: body.amount,
        });
        sender.amount -= body.amount;
        sender.staked += body.amount;
        set(state_update, key_for_account(sender_account_id), &sender);
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
    amount: Balance,
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
        set(state_update, key_for_account(&receiver_id), receiver);
    }
    Ok(receipts)
}

pub fn create_account(
    state_update: &mut TrieUpdate,
    body: &CreateAccountTransaction,
    hash: CryptoHash,
    sender: &mut Account,
    refund_account_id: &AccountId,
    public_key: PublicKey,
) -> Result<Vec<ReceiptTransaction>, String> {
    if !is_valid_account_id(&body.new_account_id) {
        return Err(format!("Account name {} {}", body.new_account_id, INVALID_ACCOUNT_ID));
    }
    if sender.amount >= body.amount {
        sender.amount -= body.amount;
        set(state_update, key_for_account(&body.originator), &sender);
        let new_nonce = create_nonce_with_nonce(&hash, 0);
        let receipt = ReceiptTransaction::new(
            body.originator.clone(),
            body.new_account_id.clone(),
            new_nonce,
            ReceiptBody::NewCall(AsyncCall::new(
                SYSTEM_METHOD_CREATE_ACCOUNT.to_vec(),
                body.public_key.clone(),
                body.amount,
                refund_account_id.clone(),
                body.originator.clone(),
                public_key,
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
    set(state_update, key_for_code(&sender_id), &code);
    set(state_update, key_for_account(&sender_id), &sender);
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
    set(state_update, key_for_account(&body.originator), &account);
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
        return Err("Cannot add a public key that already exists on the account".to_string());
    }
    if get::<AccessKey>(&state_update, &key_for_access_key(&body.originator, &new_key)).is_some() {
        return Err("Cannot add a public key that already used for an access key".to_string());
    }
    if let Some(access_key) = &body.access_key {
        if account.amount >= access_key.amount {
            if access_key.amount > 0 {
                account.amount -= access_key.amount;
                set(state_update, key_for_account(&body.originator), &account);
            }
        } else {
            return Err(format!(
                "Account {} tries to create new access key with {} amount, but only has {}",
                body.originator, access_key.amount, account.amount
            ));
        }
        if let Some(ref balance_owner) = access_key.balance_owner {
            if !is_valid_account_id(balance_owner) {
                return Err("Invalid account ID for balance owner in the access key".to_string());
            }
        }
        if let Some(ref contract_id) = access_key.contract_id {
            if !is_valid_account_id(contract_id) {
                return Err("Invalid account ID for contract ID in the access key".to_string());
            }
        }
        set(state_update, key_for_access_key(&body.originator, &new_key), access_key);
    } else {
        account.public_keys.push(new_key);
        set(state_update, key_for_account(&body.originator), &account);
    }
    Ok(vec![])
}

pub fn delete_key(
    state_update: &mut TrieUpdate,
    body: &DeleteKeyTransaction,
    account: &mut Account,
    nonce: CryptoHash,
) -> Result<Vec<ReceiptTransaction>, String> {
    let cur_key = PublicKey::try_from(&body.cur_key as &[u8]).map_err(|e| format!("{}", e))?;
    let num_keys = account.public_keys.len();
    let mut new_receipts = vec![];
    account.public_keys.retain(|&x| x != cur_key);
    if account.public_keys.len() == num_keys {
        let access_key: AccessKey = get(
            &state_update,
            &key_for_access_key(&body.originator, &cur_key),
        )
        .ok_or_else(|| {
            format!("Account {} tries to remove a public key that it does not own", body.originator)
        })?;
        if access_key.amount > 0 {
            let balance_owner_id: &AccountId =
                access_key.balance_owner.as_ref().unwrap_or(&body.originator);
            if balance_owner_id != &body.originator {
                let new_receipt = ReceiptTransaction::new(
                    body.originator.clone(),
                    balance_owner_id.clone(),
                    create_nonce_with_nonce(&nonce, 0),
                    ReceiptBody::Refund(access_key.amount),
                );
                new_receipts.push(new_receipt);
            } else {
                account.amount += access_key.amount;
                set(state_update, key_for_account(&body.originator), &account);
            }
        }
        // Remove access key
        state_update.remove(&key_for_access_key(&body.originator, &cur_key));
    } else {
        set(state_update, key_for_account(&body.originator), &account);
    }
    Ok(new_receipts)
}

pub fn system_create_account(
    state_update: &mut TrieUpdate,
    call: &AsyncCall,
    account_id: &AccountId,
) -> Result<Vec<ReceiptTransaction>, String> {
    if !is_valid_account_id(account_id) {
        return Err(format!("Account name {} {}", account_id, INVALID_ACCOUNT_ID));
    }
    let account_id_bytes = key_for_account(&account_id);

    let public_key = PublicKey::try_from(&call.args as &[u8]).map_err(|e| format!("{}", e))?;
    let new_account = Account::new(vec![public_key], call.amount, hash(&[]));
    set(state_update, account_id_bytes, &new_account);
    Ok(vec![])
}

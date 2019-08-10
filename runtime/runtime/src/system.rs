use std::convert::TryFrom;

use near_primitives::account::{AccessKey, Account};
use near_primitives::contract::ContractCode;
use near_primitives::crypto::signature::PublicKey;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::transaction::{
    AddKeyTransaction, AsyncCall, CallbackInfo, CallbackResult, CreateAccountTransaction,
    DeleteAccountTransaction, DeleteKeyTransaction, ReceiptBody, ReceiptTransaction,
    SendMoneyTransaction, StakeTransaction, SwapKeyTransaction,
};
use near_primitives::types::{AccountId, Balance, BlockIndex, ValidatorStake};
use near_primitives::utils::{create_nonce_with_nonce, is_valid_account_id, key_for_access_key};
use near_store::{
    get_access_key, remove_account, set_access_key, set_account, set_code, TrieUpdate,
};

use crate::check_rent;
use crate::config::RuntimeConfig;

pub const SYSTEM_METHOD_CREATE_ACCOUNT: &[u8] = b"_sys:create_account";
pub const SYSTEM_METHOD_DELETE_ACCOUNT: &[u8] = b"_sys:delete_account";

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
        set_account(state_update, &transaction.originator, sender);
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
    let increment = if body.amount > sender.staked { body.amount - sender.staked } else { 0 };
    if sender.amount >= increment {
        if sender.staked == 0 && body.amount == 0 {
            // if the account hasn't staked, it cannot unstake
            return Err(format!(
                "Account {} is not yet staked, but tries to unstake",
                sender_account_id
            ));
        }
        validator_proposals.push(ValidatorStake {
            account_id: sender_account_id.clone(),
            public_key: PublicKey::try_from(body.public_key.as_str())
                .map_err(|err| err.to_string())?,
            amount: body.amount,
        });
        if sender.staked < body.amount {
            sender.amount -= increment;
            sender.staked = body.amount;
            set_account(state_update, sender_account_id, &sender);
        }
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
        set_account(state_update, &receiver_id, receiver);
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
        set_account(state_update, &body.originator, &sender);
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
    set_code(state_update, &sender_id, &code);
    set_account(state_update, &sender_id, &sender);
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
    set_account(state_update, &body.originator, &account);
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
    if get_access_key(state_update, &body.originator, &new_key).is_some() {
        return Err("Cannot add a public key that already used for an access key".to_string());
    }
    if let Some(access_key) = &body.access_key {
        if account.amount >= access_key.amount {
            if access_key.amount > 0 {
                account.amount -= access_key.amount;
                set_account(state_update, &body.originator, &account);
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
        set_access_key(state_update, &body.originator, &new_key, access_key);
    } else {
        account.public_keys.push(new_key);
        set_account(state_update, &body.originator, &account);
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
        let access_key: AccessKey = get_access_key(state_update, &body.originator, &cur_key)
            .ok_or_else(|| {
                format!(
                    "Account {} tries to remove a public key that it does not own",
                    body.originator
                )
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
                set_account(state_update, &body.originator, &account);
            }
        }
        // Remove access key
        state_update.remove(&key_for_access_key(&body.originator, &cur_key));
    } else {
        set_account(state_update, &body.originator, &account);
    }
    Ok(new_receipts)
}

pub fn delete_account(
    body: &DeleteAccountTransaction,
    hash: CryptoHash,
    public_key: PublicKey,
) -> Result<Vec<ReceiptTransaction>, String> {
    let new_nonce = create_nonce_with_nonce(&hash, 0);
    let receipt = ReceiptTransaction::new(
        body.originator_id.clone(),
        body.receiver_id.clone(),
        new_nonce,
        ReceiptBody::NewCall(AsyncCall::new(
            SYSTEM_METHOD_DELETE_ACCOUNT.to_vec(),
            vec![],
            0,
            body.originator_id.clone(),
            body.originator_id.clone(),
            public_key,
        )),
    );
    Ok(vec![receipt])
}

/// System call to create an account.
pub fn system_create_account(
    state_update: &mut TrieUpdate,
    call: &AsyncCall,
    account_id: &AccountId,
) -> Result<Vec<ReceiptTransaction>, String> {
    if !is_valid_account_id(account_id) {
        return Err(format!("Account name {} {}", account_id, INVALID_ACCOUNT_ID));
    }
    let public_key = PublicKey::try_from(&call.args as &[u8]).map_err(|e| format!("{}", e))?;
    let new_account = Account::new(vec![public_key], call.amount, hash(&[]));
    set_account(state_update, &account_id, &new_account);
    Ok(vec![])
}

/// System call to delete given account initiated by an originator.
/// Allow to delete account if:
///  * User has less than `storage_price` * `state_size` * `poke_threshold` and not staking.
///  * Otherwise delete account and refund the rest of the money to originator.
pub fn system_delete_account(
    state_update: &mut TrieUpdate,
    call: &AsyncCall,
    nonce: &CryptoHash,
    account_id: &AccountId,
    account: &mut Account,
    runtime_config: &RuntimeConfig,
    epoch_length: BlockIndex,
) -> Result<Vec<ReceiptTransaction>, String> {
    if account.staked != 0 {
        return Err(format!("Account {} is staking, can not be deleted.", account_id));
    }
    if check_rent(account_id, account, runtime_config, epoch_length) {
        return Err(format!(
            "Account {} has {}, which is more than enough to cover the storage for next {} blocks.",
            account_id, account.amount, runtime_config.poke_threshold
        ));
    }
    let new_nonce = create_nonce_with_nonce(nonce, 0);
    // We use current amount as a reward, because this account's storage rent was updated before
    // calling this function.
    let receipt = ReceiptTransaction::new(
        call.originator_id.clone(),
        call.originator_id.clone(),
        new_nonce,
        ReceiptBody::Refund(account.amount),
    );
    remove_account(state_update, account_id)
        .map_err(|err| format!("Failed to delete all account data: {}", err))?;
    Ok(vec![receipt])
}

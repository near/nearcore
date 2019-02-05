use storage::{StateDbUpdate};
use primitives::types::AccountId;
use primitives::traits::Decode;
use primitives::hash::hash;
use primitives::signature::PublicKey;
use primitives::utils::is_valid_account_id;
use transaction::{AsyncCall, ReceiptTransaction};

use super::{COL_ACCOUNT, COL_CODE, set, account_id_to_bytes, Account};
use crate::{TxTotalStake, get_tx_stake_key};

/// const does not allow function call, so have to resort to this
pub fn system_account() -> AccountId { "system".to_string() }

pub const SYSTEM_METHOD_CREATE_ACCOUNT: &[u8] = b"_sys:create_account";
pub const SYSTEM_METHOD_DEPLOY: &[u8] = b"_sys:deploy";

pub fn create_account(
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

pub fn deploy(
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
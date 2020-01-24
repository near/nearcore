use borsh::{BorshDeserialize, BorshSerialize};

use crate::serialize::u128_dec_format;

use crate::hash::CryptoHash;
use crate::types::{AccountId, Balance, BlockHeight, Nonce, StorageUsage};

/// Per account information stored in the state.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct Account {
    /// The total not locked tokens.
    #[serde(with = "u128_dec_format")]
    pub amount: Balance,
    /// The amount locked due to staking
    #[serde(with = "u128_dec_format")]
    pub locked: Balance,
    /// Hash of the code stored in the storage for this account.
    pub code_hash: CryptoHash,
    /// Storage used by the given account (in bytes).
    pub storage_usage: StorageUsage,
    /// Last height at which the storage was paid for.
    pub storage_paid_at: BlockHeight,
}

impl Account {
    pub fn new(amount: Balance, code_hash: CryptoHash, storage_paid_at: BlockHeight) -> Self {
        Account { amount, locked: 0, code_hash, storage_usage: 0, storage_paid_at }
    }
}

/// Calculates the storage and the name rent for the given account
/// for period from `account.storage_paid_at` to the current `block_height`
pub fn calculate_rent(
    account_id: &AccountId,
    account: &Account,
    block_height: BlockHeight,
    account_length_baseline_cost_per_block: Balance,
    storage_cost_byte_per_block: Balance,
) -> Balance {
    let charge = u128::from(block_height.saturating_sub(account.storage_paid_at))
        * rent_per_block(
            account_id,
            account,
            account_length_baseline_cost_per_block,
            storage_cost_byte_per_block,
        );
    let actual_charge = std::cmp::min(account.amount, charge);
    actual_charge
}

/// Calculates an account rent per block
pub fn rent_per_block(
    account_id: &AccountId,
    account: &Account,
    account_length_baseline_cost_per_block: Balance,
    storage_cost_byte_per_block: Balance,
) -> Balance {
    let account_length_cost_per_block = if account_id.len() > 10 {
        0
    } else {
        account_length_baseline_cost_per_block / 3_u128.pow(account_id.len() as u32 - 2)
    };

    let storage_cost_per_block = u128::from(account.storage_usage) * storage_cost_byte_per_block;

    account_length_cost_per_block + storage_cost_per_block
}

/// Access key provides limited access to an account. Each access key belongs to some account and
/// is identified by a unique (within the account) public key. One account may have large number of
/// access keys. Access keys allow to act on behalf of the account by restricting transactions
/// that can be issued.
/// `account_id,public_key` is a key in the state
#[derive(BorshSerialize, BorshDeserialize, Serialize, PartialEq, Eq, Hash, Clone, Debug)]
pub struct AccessKey {
    /// The nonce for this access key.
    /// NOTE: In some cases the access key needs to be recreated. If the new access key reuses the
    /// same public key, the nonce of the new access key should be equal to the nonce of the old
    /// access key. It's required to avoid replaying old transactions again.
    pub nonce: Nonce,

    /// Defines permissions for this access key.
    pub permission: AccessKeyPermission,
}

impl AccessKey {
    pub fn full_access() -> Self {
        Self { nonce: 0, permission: AccessKeyPermission::FullAccess }
    }
}

/// Defines permissions for AccessKey
#[derive(BorshSerialize, BorshDeserialize, Serialize, PartialEq, Eq, Hash, Clone, Debug)]
pub enum AccessKeyPermission {
    FunctionCall(FunctionCallPermission),

    /// Grants full access to the account.
    /// NOTE: It's used to replace account-level public keys.
    FullAccess,
}

/// Grants limited permission to make transactions with FunctionCallActions
/// The permission can limit the allowed balance to be spent on the prepaid gas.
/// It also restrict the account ID of the receiver for this function call.
/// It also can restrict the method name for the allowed function calls.
#[derive(BorshSerialize, BorshDeserialize, Serialize, PartialEq, Eq, Hash, Clone, Debug)]
pub struct FunctionCallPermission {
    /// Allowance is a balance limit to use by this access key to pay for function call gas and
    /// transaction fees. When this access key is used, both account balance and the allowance is
    /// decreased by the same value.
    /// `None` means unlimited allowance.
    /// NOTE: To change or increase the allowance, the old access key needs to be deleted and a new
    /// access key should be created.
    pub allowance: Option<Balance>,

    /// The access key only allows transactions with the given receiver's account id.
    pub receiver_id: AccountId,

    /// A list of method names that can be used. The access key only allows transactions with the
    /// function call of one of the given method names.
    /// Empty list means any method name can be used.
    pub method_names: Vec<String>,
}

#[cfg(test)]
mod tests {
    use borsh::BorshSerialize;

    use crate::hash::hash;
    use crate::serialize::to_base;

    use super::*;

    #[test]
    fn test_account_serialization() {
        let acc = Account {
            amount: 1_000_000,
            locked: 1_000_000,
            code_hash: CryptoHash::default(),
            storage_usage: 100,
            storage_paid_at: 1_123_321,
        };
        let bytes = acc.try_to_vec().unwrap();
        assert_eq!(to_base(&hash(&bytes)), "DzpbYEwBoiKa3DRTgK2L8fBq3QRfGSoUkTXrTYxwBt17");
    }
}

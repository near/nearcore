use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

/// Key type defines the access for the key owner.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, BorshDeserialize, BorshSerialize)]
pub enum KeyType {
    /// The key belongs to the owner of the Vesting Contract. The key provides the ability to
    /// stake and withdraw available funds.
    Owner,
    /// The key belongs to the NEAR foundation. The key provides ability to stop vesting in
    /// case the contract owner's vesting agreement was terminated, e.g. the employee left the
    /// company before the end of the vesting period.
    Foundation,
}

impl KeyType {
    /// Provides allowed methods name in a comma separated format used by `FunctionCallPermission`
    /// within an `AccessKey`.
    pub fn allowed_methods(&self) -> Vec<u8> {
        match self {
            KeyType::Owner => b"stake,transfer".to_vec(),
            KeyType::Foundation => b"permanently_unstake,terminate".to_vec(),
        }
    }
}

/// A key is a sequence of bytes, potentially including the prefix determining the cryptographic type
/// of the key. For forward compatibility we do not enforce any specific length.
pub type PublicKey = Vec<u8>;

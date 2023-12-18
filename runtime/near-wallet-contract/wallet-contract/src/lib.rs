//! Temporary implementation of the Wallet Contract.
//! See https://github.com/near/NEPs/issues/518.
//! Must not use in production!
// TODO(eth-implicit) Change to a real Wallet Contract implementation.

use hex;
use near_sdk::borsh::{self, BorshDeserialize, BorshSerialize};
use near_sdk::{env, near_bindgen, AccountId, Promise};
use rlp::Rlp;

#[near_bindgen]
#[derive(Default, BorshDeserialize, BorshSerialize)]
pub struct WalletContract {}

#[near_bindgen]
impl WalletContract {
    /// For the sake of this placeholder implementation, we assume simplified version of the `rlp_transaction`
    /// that only has 3 values: `To`, `Value`, and `PublicKey`. We assume this is a transfer transaction.
    /// The real implementation would obtain the public key from `Signature`.
    pub fn execute_rlp(&self, target: AccountId, rlp_transaction: Vec<u8>) {
        let rlp = Rlp::new(&rlp_transaction);

        let to: String = match rlp.val_at(0) {
            Ok(to) => to,
            _ => env::panic_str("Missing `to` field in RLP-encoded transaction."),
        };
        if target.to_string() != to {
            env::panic_str("`target` not equal to transaction's `To` address.");
        }

        let value_bytes: Vec<u8> = match rlp.val_at(1) {
            Ok(value_bytes) => value_bytes,
            _ => env::panic_str("Missing `value` field in RLP-encoded transaction."),
        };
        let value = u128::from_be_bytes(
            value_bytes.try_into().expect("Incorrect `value` field in RLP-encoded transaction."),
        );

        let signer_public_key_bytes: Vec<u8> = match rlp.val_at(2) {
            Ok(signer_public_key_bytes) => signer_public_key_bytes,
            _ => env::panic_str("Signature extraction failed for RLP-encoded transaction."),
        };

        let hash = env::keccak256(&signer_public_key_bytes);
        let signer_address = format!("0x{}", hex::encode(&hash[12..32]));

        if signer_address != env::current_account_id().to_string() {
            env::panic_str("Public key does not match the Wallet Contract address.");
        }

        Promise::new(target).transfer(value);
    }
}

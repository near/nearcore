//! Temporary implementation of the Wallet Contract.
//! See https://github.com/near/NEPs/issues/518.
//! Must not use in production!

// TODO(eth-implicit) Change to a real Wallet Contract implementation.
use near_sdk::borsh::{self, BorshDeserialize, BorshSerialize};
use near_sdk::{near_bindgen, AccountId, Promise, Balance};

#[near_bindgen]
#[derive(Default, BorshDeserialize, BorshSerialize)]
pub struct WalletContract { }

#[near_bindgen]
impl WalletContract {
  pub fn transfer(&self, to: AccountId, amount: Balance){
    Promise::new(to).transfer(amount);
  }
}

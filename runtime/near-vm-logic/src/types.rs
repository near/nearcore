use crate::{Actions, ExtCosts};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::rc::Rc;

pub type AccountId = String;
pub type PublicKey = Vec<u8>;
pub type BlockHeight = u64;
pub type EpochHeight = u64;
pub type Balance = u128;
pub type Gas = u64;
pub type PromiseIndex = u64;
pub type ReceiptIndex = u64;
pub type IteratorIndex = u64;
pub type StorageUsage = u64;

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum ReturnData {
    /// Method returned some value or data.
    #[serde(with = "crate::serde_with::bytes_as_str")]
    Value(Vec<u8>),

    /// The return value of the method should be taken from the return value of another method
    /// identified through receipt index.
    ReceiptIndex(ReceiptIndex),

    /// Method hasn't returned any data or promise.
    None,
}

/// When there is a callback attached to one or more contract calls the execution results of these
/// calls are available to the contract invoked through the callback.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum PromiseResult {
    /// Current version of the protocol never returns `PromiseResult::NotReady`.
    NotReady,
    #[serde(with = "crate::serde_with::bytes_as_str")]
    Successful(Vec<u8>),
    Failed,
}

/// Profile of gas consumption, +1 for Wasm bytecode execution cost.
pub type ProfileData = Rc<RefCell<[u64; Actions::count() + ExtCosts::count() + 1]>>;

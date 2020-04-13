use std::borrow::Borrow;
use std::fmt;

use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use near_crypto::{KeyType, PublicKey};

use crate::hash::CryptoHash;
use crate::logging;
use crate::serialize::{option_base64_format, u128_dec_format_compatible};
use crate::transaction::{Action, TransferAction};
use crate::types::{AccountId, Balance};
use crate::utils::system_account;

/// Receipts are used for a cross-shard communication.
/// Receipts could be 2 types (determined by a `ReceiptEnum`): `ReceiptEnum::Action` of `ReceiptEnum::Data`.
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Receipt {
    /// An issuer account_id of a particular receipt.
    /// `predecessor_id` could be either `Transaction` `signer_id` or intermediate contract's `account_id`.
    pub predecessor_id: AccountId,
    /// `receiver_id` is a receipt destination.
    pub receiver_id: AccountId,
    /// An unique id for the receipt
    pub receipt_id: CryptoHash,
    /// A receipt type
    pub receipt: ReceiptEnum,
}

impl Borrow<CryptoHash> for Receipt {
    fn borrow(&self) -> &CryptoHash {
        &self.receipt_id
    }
}

impl Receipt {
    /// It's not a content hash, but receipt_id is unique.
    pub fn get_hash(&self) -> CryptoHash {
        self.receipt_id
    }

    /// Generates a receipt with a transfer from system for a given balance without a receipt_id
    pub fn new_refund(receiver_id: &AccountId, refund: Balance) -> Self {
        Receipt {
            predecessor_id: system_account(),
            receiver_id: receiver_id.clone(),
            receipt_id: CryptoHash::default(),

            receipt: ReceiptEnum::Action(ActionReceipt {
                signer_id: system_account(),
                signer_public_key: PublicKey::empty(KeyType::ED25519),
                gas_price: 0,
                output_data_receivers: vec![],
                input_data_ids: vec![],
                actions: vec![Action::Transfer(TransferAction { deposit: refund })],
            }),
        }
    }
}

/// Receipt could be either ActionReceipt or DataReceipt
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ReceiptEnum {
    Action(ActionReceipt),
    Data(DataReceipt),
}

/// ActionReceipt is derived from an Action from `Transaction or from Receipt`
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct ActionReceipt {
    /// A signer of the original transaction
    pub signer_id: AccountId,
    /// An access key which was used to sign the original transaction
    pub signer_public_key: PublicKey,
    /// A gas_price which has been used to buy gas in the original transaction
    #[serde(with = "u128_dec_format_compatible")]
    pub gas_price: Balance,
    /// If present, where to route the output data
    pub output_data_receivers: Vec<DataReceiver>,
    /// A list of the input data dependencies for this Receipt to process.
    /// If all `input_data_ids` for this receipt are delivered to the account
    /// that means we have all the `ReceivedData` input which will be than converted to a
    /// `PromiseResult::Successful(value)` or `PromiseResult::Failed`
    /// depending on `ReceivedData` is `Some(_)` or `None`
    pub input_data_ids: Vec<CryptoHash>,
    /// A list of actions to process when all input_data_ids are filled
    pub actions: Vec<Action>,
}

/// An incoming (ingress) `DataReceipt` which is going to a Receipt's `receiver` input_data_ids
/// Which will be converted to `PromiseResult::Successful(value)` or `PromiseResult::Failed`
#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Hash, PartialEq, Eq, Clone)]
pub struct DataReceipt {
    pub data_id: CryptoHash,
    #[serde(with = "option_base64_format")]
    pub data: Option<Vec<u8>>,
}

/// The outgoing (egress) data which will be transformed
/// to a `DataReceipt` to be sent to a `receipt.receiver`
#[derive(
    BorshSerialize, BorshDeserialize, Serialize, Deserialize, Hash, Clone, Debug, PartialEq, Eq,
)]
pub struct DataReceiver {
    pub data_id: CryptoHash,
    pub receiver_id: AccountId,
}

impl fmt::Debug for DataReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataReceipt")
            .field("data_id", &self.data_id)
            .field("data", &format_args!("{}", logging::pretty_result(&self.data)))
            .finish()
    }
}

/// A temporary data which is created by processing of DataReceipt
/// stored in a state trie with a key = `account_id` + `data_id` until
/// `input_data_ids` of all incoming Receipts are satisfied
/// None means data retrieval was failed
#[derive(BorshSerialize, BorshDeserialize, Serialize, Hash, PartialEq, Eq, Clone)]
pub struct ReceivedData {
    #[serde(with = "option_base64_format")]
    pub data: Option<Vec<u8>>,
}

impl fmt::Debug for ReceivedData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReceivedData")
            .field("data", &format_args!("{}", logging::pretty_result(&self.data)))
            .finish()
    }
}

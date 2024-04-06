use crate::hash::CryptoHash;
use crate::serialize::dec_format;
use crate::transaction::{Action, TransferAction};
use crate::types::{AccountId, Balance, BlockHeight, ShardId};
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::{KeyType, PublicKey};
use near_fmt::AbbrBytes;
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt;
use std::io::{self, BufRead, BufReader, Read};
use std::io::{Error, ErrorKind};

/// The outgoing (egress) data which will be transformed
/// to a `DataReceipt` to be sent to a `receipt.receiver`
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Hash,
    Clone,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct DataReceiver {
    pub data_id: CryptoHash,
    pub receiver_id: AccountId,
}

/// Receipts are used for a cross-shard communication.
/// Receipts could be 2 types (determined by a `ReceiptEnum`): `ReceiptEnum::Action` of `ReceiptEnum::Data`.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    PartialEq,
    Eq,
    Clone,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct ReceiptV0 {
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

#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    PartialEq,
    Eq,
    Clone,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct ReceiptV1 {
    /// An issuer account_id of a particular receipt.
    /// `predecessor_id` could be either `Transaction` `signer_id` or intermediate contract's `account_id`.
    pub predecessor_id: AccountId,
    /// `receiver_id` is a receipt destination.
    pub receiver_id: AccountId,
    /// An unique id for the receipt
    pub receipt_id: CryptoHash,
    /// A receipt type
    pub receipt: ReceiptEnum,
    /// Priority of a receipt
    pub priority: u64,
}

#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize)]
pub enum Receipt {
    V0(ReceiptV0),
    V1(ReceiptV1),
}

impl BorshSerialize for Receipt {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            Receipt::V0(receipt) => receipt.serialize(writer),
            Receipt::V1(receipt) => {
                BorshSerialize::serialize(&1_u8, writer)?;
                receipt.serialize(writer)
            }
        }
    }
}

impl BorshDeserialize for Receipt {
    /// Deserialize based on the first byte of the buffer. If the first byte is 0, it is a V0
    /// Otherwise, it is a V1. For V0, we do backward compatible deserialization by deserializing
    /// the entire stream into V0. For V1, we consume the first byte and then deserialize the rest
    /// No conflict is possible because the first field of Receipt is an `AccountId` which starts
    /// with a `usize` but can only be at most 64 bytes long, so the highest byte is always 0.
    fn deserialize_reader<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        // Note: the following implementation is inefficient because it copies data twice.
        // and should be replaced once the protocol switches to the new receipt format entirely.
        let mut buf_reader = BufReader::new(reader);
        let buffer = buf_reader.fill_buf()?; // Fill the buffer
        if buffer.is_empty() {
            return Err(Error::new(ErrorKind::UnexpectedEof, "No data to read"));
        } else {
            let first_byte = buffer[0];
            if first_byte == 0 {
                let receipt = ReceiptV0::deserialize_reader(&mut buf_reader)?;
                Ok(Receipt::V0(receipt))
            } else if first_byte == 1 {
                let _ = buf_reader.consume(1); // Consume the first byte
                let receipt = ReceiptV1::deserialize_reader(&mut buf_reader)?;
                Ok(Receipt::V1(receipt))
            } else {
                Err(Error::new(ErrorKind::InvalidData, "Invalid version"))
            }
        }
    }
}

pub enum ReceiptPriority {
    /// Used in ReceiptV1
    Priority(u64),
    /// Used in ReceiptV0
    NoPriority,
}

impl ReceiptPriority {
    pub fn value(&self) -> u64 {
        match self {
            ReceiptPriority::Priority(value) => *value,
            ReceiptPriority::NoPriority => 0,
        }
    }
}

impl Borrow<CryptoHash> for Receipt {
    fn borrow(&self) -> &CryptoHash {
        match self {
            Receipt::V0(receipt) => &receipt.receipt_id,
            Receipt::V1(receipt) => &receipt.receipt_id,
        }
    }
}

impl Receipt {
    pub fn receiver_id(&self) -> &AccountId {
        match self {
            Receipt::V0(receipt) => &receipt.receiver_id,
            Receipt::V1(receipt) => &receipt.receiver_id,
        }
    }

    pub fn set_receiver_id(&mut self, receiver_id: AccountId) {
        match self {
            Receipt::V0(receipt) => receipt.receiver_id = receiver_id,
            Receipt::V1(receipt) => receipt.receiver_id = receiver_id,
        }
    }

    pub fn predecessor_id(&self) -> &AccountId {
        match self {
            Receipt::V0(receipt) => &receipt.predecessor_id,
            Receipt::V1(receipt) => &receipt.predecessor_id,
        }
    }

    pub fn set_predecessor_id(&mut self, predecessor_id: AccountId) {
        match self {
            Receipt::V0(receipt) => receipt.predecessor_id = predecessor_id,
            Receipt::V1(receipt) => receipt.predecessor_id = predecessor_id,
        }
    }

    pub fn receipt(&self) -> &ReceiptEnum {
        match self {
            Receipt::V0(receipt) => &receipt.receipt,
            Receipt::V1(receipt) => &receipt.receipt,
        }
    }

    pub fn receipt_mut(&mut self) -> &mut ReceiptEnum {
        match self {
            Receipt::V0(receipt) => &mut receipt.receipt,
            Receipt::V1(receipt) => &mut receipt.receipt,
        }
    }

    pub fn take_receipt(self) -> ReceiptEnum {
        match self {
            Receipt::V0(receipt) => receipt.receipt,
            Receipt::V1(receipt) => receipt.receipt,
        }
    }

    pub fn receipt_id(&self) -> &CryptoHash {
        match self {
            Receipt::V0(receipt) => &receipt.receipt_id,
            Receipt::V1(receipt) => &receipt.receipt_id,
        }
    }

    pub fn set_receipt_id(&mut self, receipt_id: CryptoHash) {
        match self {
            Receipt::V0(receipt) => receipt.receipt_id = receipt_id,
            Receipt::V1(receipt) => receipt.receipt_id = receipt_id,
        }
    }

    pub fn priority(&self) -> ReceiptPriority {
        match self {
            Receipt::V0(_) => ReceiptPriority::NoPriority,
            Receipt::V1(receipt) => ReceiptPriority::Priority(receipt.priority),
        }
    }

    /// It's not a content hash, but receipt_id is unique.
    pub fn get_hash(&self) -> CryptoHash {
       *self.receipt_id()
    }

    /// Generates a receipt with a transfer from system for a given balance without a receipt_id.
    /// This should be used for token refunds instead of gas refunds. It inherits priority from the parent receipt.
    /// It doesn't refund the allowance of the access key. For gas refunds use `new_gas_refund`.
    pub fn new_balance_refund(
        receiver_id: &AccountId,
        refund: Balance,
        priority: ReceiptPriority,
    ) -> Self {
        match priority {
            ReceiptPriority::Priority(priority) => Receipt::V1(ReceiptV1 {
                predecessor_id: "system".parse().unwrap(),
                receiver_id: receiver_id.clone(),
                receipt_id: CryptoHash::default(),

                receipt: ReceiptEnum::Action(ActionReceipt {
                    signer_id: "system".parse().unwrap(),
                    signer_public_key: PublicKey::empty(KeyType::ED25519),
                    gas_price: 0,
                    output_data_receivers: vec![],
                    input_data_ids: vec![],
                    actions: vec![Action::Transfer(TransferAction { deposit: refund })],
                }),
                priority,
            }),
            ReceiptPriority::NoPriority => Receipt::V0(ReceiptV0 {
                predecessor_id: "system".parse().unwrap(),
                receiver_id: receiver_id.clone(),
                receipt_id: CryptoHash::default(),

                receipt: ReceiptEnum::Action(ActionReceipt {
                    signer_id: "system".parse().unwrap(),
                    signer_public_key: PublicKey::empty(KeyType::ED25519),
                    gas_price: 0,
                    output_data_receivers: vec![],
                    input_data_ids: vec![],
                    actions: vec![Action::Transfer(TransferAction { deposit: refund })],
                }),
            }),
        }
    }

    /// Generates a receipt with a transfer action from system for a given balance without a
    /// receipt_id. It contains `signer_id` and `signer_public_key` to indicate this is a gas
    /// refund. The execution of this receipt will try to refund the allowance of the
    /// access key with the given public key.
    /// Gas refund does not inherit priority from its parent receipt and has no priority associated with it
    /// NOTE: The access key may be replaced by the owner, so the execution can't rely that the
    /// access key is the same and it should use best effort for the refund.
    pub fn new_gas_refund(
        receiver_id: &AccountId,
        refund: Balance,
        signer_public_key: PublicKey,
        priority: ReceiptPriority,
    ) -> Self {
        match priority {
            ReceiptPriority::Priority(priority) => Receipt::V1(ReceiptV1 {
                predecessor_id: "system".parse().unwrap(),
                receiver_id: receiver_id.clone(),
                receipt_id: CryptoHash::default(),

                receipt: ReceiptEnum::Action(ActionReceipt {
                    signer_id: receiver_id.clone(),
                    signer_public_key,
                    gas_price: 0,
                    output_data_receivers: vec![],
                    input_data_ids: vec![],
                    actions: vec![Action::Transfer(TransferAction { deposit: refund })],
                }),
                priority,
            }),
            ReceiptPriority::NoPriority => Receipt::V0(ReceiptV0 {
                predecessor_id: "system".parse().unwrap(),
                receiver_id: receiver_id.clone(),
                receipt_id: CryptoHash::default(),

                receipt: ReceiptEnum::Action(ActionReceipt {
                    signer_id: receiver_id.clone(),
                    signer_public_key,
                    gas_price: 0,
                    output_data_receivers: vec![],
                    input_data_ids: vec![],
                    actions: vec![Action::Transfer(TransferAction { deposit: refund })],
                }),
            }),
        }
    }
}

/// Receipt could be either ActionReceipt or DataReceipt
#[derive(BorshSerialize, Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReceiptEnum {
    Action(ActionReceipt),
    Data(DataReceipt),
    PromiseYield(ActionReceipt),
    PromiseResume(DataReceipt),
}

impl BorshDeserialize for ReceiptEnum {
    fn deserialize_reader<R: io::Read>(rd: &mut R) -> io::Result<Self> {
        // after we stabilize yield_resume we can simply derive BorshDeserialize trait again
        let ordinal = u8::deserialize_reader(rd)?;
        match ordinal {
            0u8 => Ok(ReceiptEnum::Action(ActionReceipt::deserialize_reader(rd)?)),
            1u8 => Ok(ReceiptEnum::Data(DataReceipt::deserialize_reader(rd)?)),
            #[cfg(feature = "yield_resume")]
            2u8 => Ok(ReceiptEnum::PromiseYield(ActionReceipt::deserialize_reader(rd)?)),
            #[cfg(feature = "yield_resume")]
            3u8 => Ok(ReceiptEnum::PromiseResume(DataReceipt::deserialize_reader(rd)?)),
            _ => Err(Error::from(ErrorKind::InvalidData)),
        }
    }
}

/// ActionReceipt is derived from an Action from `Transaction or from Receipt`
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    PartialEq,
    Eq,
    Clone,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct ActionReceipt {
    /// A signer of the original transaction
    pub signer_id: AccountId,
    /// An access key which was used to sign the original transaction
    pub signer_public_key: PublicKey,
    /// A gas_price which has been used to buy gas in the original transaction
    #[serde(with = "dec_format")]
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
#[serde_as]
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Hash,
    PartialEq,
    Eq,
    Clone,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct DataReceipt {
    pub data_id: CryptoHash,
    #[serde_as(as = "Option<Base64>")]
    pub data: Option<Vec<u8>>,
}

impl fmt::Debug for DataReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataReceipt")
            .field("data_id", &self.data_id)
            .field("data", &format_args!("{}", AbbrBytes(self.data.as_deref())))
            .finish()
    }
}

/// A temporary data which is created by processing of DataReceipt
/// stored in a state trie with a key = `account_id` + `data_id` until
/// `input_data_ids` of all incoming Receipts are satisfied
/// None means data retrieval was failed
#[derive(BorshSerialize, BorshDeserialize, Hash, PartialEq, Eq, Clone)]
pub struct ReceivedData {
    pub data: Option<Vec<u8>>,
}

impl fmt::Debug for ReceivedData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReceivedData")
            .field("data", &format_args!("{}", AbbrBytes(self.data.as_deref())))
            .finish()
    }
}

/// Stores indices for a persistent queue for delayed receipts that didn't fit into a block.
#[derive(Default, BorshSerialize, BorshDeserialize, Clone, PartialEq, Debug)]
pub struct DelayedReceiptIndices {
    // First inclusive index in the queue.
    pub first_index: u64,
    // Exclusive end index of the queue
    pub next_available_index: u64,
}

impl DelayedReceiptIndices {
    pub fn len(&self) -> u64 {
        self.next_available_index - self.first_index
    }
}

/// Stores indices for a persistent queue for PromiseYield timeouts.
#[derive(Default, BorshSerialize, BorshDeserialize, Clone, PartialEq, Debug)]
pub struct PromiseYieldIndices {
    // First inclusive index in the queue.
    pub first_index: u64,
    // Exclusive end index of the queue
    pub next_available_index: u64,
}

impl PromiseYieldIndices {
    pub fn len(&self) -> u64 {
        self.next_available_index - self.first_index
    }
}

/// Entries in the queue of PromiseYield timeouts.
#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Debug)]
pub struct PromiseYieldTimeout {
    /// The account on which the yielded promise was created
    pub account_id: AccountId,
    /// The `data_id` used to identify the awaited input data
    pub data_id: CryptoHash,
    /// The block height before which the data must be submitted
    pub expires_at: BlockHeight,
}

/// Map of shard to list of receipts to send to it.
pub type ReceiptResult = HashMap<ShardId, Vec<Receipt>>;

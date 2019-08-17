use std::borrow::Borrow;
use std::fmt;

use borsh::{BorshDeserialize, BorshSerialize};

use crate::crypto::signature::PublicKey;
use crate::hash::CryptoHash;
use crate::logging;
use crate::rpc::{ActionView, CryptoHashView, PublicKeyView};
use crate::serialize::{option_base64_format, u128_dec_format};
use crate::transaction::{Action, TransactionResult, TransferAction};
use crate::types::{AccountId, Balance};
use crate::utils::system_account;
use std::convert::{TryFrom, TryInto};

#[derive(Debug, Clone, PartialEq)]
pub struct ReceiptInfo {
    pub receipt: Receipt,
    pub block_index: u64,
    pub result: TransactionResult,
}

#[derive(BorshSerialize, BorshDeserialize, Hash, Debug, PartialEq, Eq, Clone)]
pub struct Receipt {
    pub predecessor_id: AccountId,
    pub receiver_id: AccountId,
    pub receipt_id: CryptoHash,

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
                signer_public_key: PublicKey::empty(),
                gas_price: 0,
                output_data_receivers: vec![],
                input_data_ids: vec![],
                actions: vec![Action::Transfer(TransferAction { deposit: refund })],
            }),
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Hash, Clone, Debug, PartialEq, Eq)]
pub enum ReceiptEnum {
    Action(ActionReceipt),
    Data(DataReceipt),
}

#[derive(BorshSerialize, BorshDeserialize, Hash, Debug, PartialEq, Eq, Clone)]
pub struct ActionReceipt {
    pub signer_id: AccountId,
    pub signer_public_key: PublicKey,

    pub gas_price: Balance,

    /// If present, where to route the output data
    pub output_data_receivers: Vec<DataReceiver>,

    pub input_data_ids: Vec<CryptoHash>,

    pub actions: Vec<Action>,
}

#[derive(BorshSerialize, BorshDeserialize, Hash, Clone, Debug, PartialEq, Eq)]
pub struct DataReceiver {
    pub data_id: CryptoHash,
    pub receiver_id: AccountId,
}

#[derive(BorshSerialize, BorshDeserialize, Hash, PartialEq, Eq, Clone)]
pub struct DataReceipt {
    pub data_id: CryptoHash,
    pub data: Option<Vec<u8>>,
}

impl fmt::Debug for DataReceipt {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("DataReceipt")
            .field("data_id", &self.data_id)
            .field("data", &format_args!("{}", logging::pretty_result(&self.data)))
            .finish()
    }
}

#[derive(BorshSerialize, BorshDeserialize, Hash, PartialEq, Eq, Clone)]
pub struct ReceivedData {
    pub data: Option<Vec<u8>>,
}

impl fmt::Debug for ReceivedData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ReceivedData")
            .field("data", &format_args!("{}", logging::pretty_result(&self.data)))
            .finish()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ReceiptView {
    pub predecessor_id: AccountId,
    pub receiver_id: AccountId,
    pub receipt_id: CryptoHashView,

    pub receipt: ReceiptEnumView,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DataReceiverView {
    pub data_id: CryptoHashView,
    pub receiver_id: AccountId,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ReceiptEnumView {
    Action {
        signer_id: AccountId,
        signer_public_key: PublicKeyView,
        #[serde(with = "u128_dec_format")]
        gas_price: Balance,
        output_data_receivers: Vec<DataReceiverView>,
        input_data_ids: Vec<CryptoHashView>,
        actions: Vec<ActionView>,
    },
    Data {
        data_id: CryptoHashView,
        #[serde(with = "option_base64_format")]
        data: Option<Vec<u8>>,
    },
}

impl From<Receipt> for ReceiptView {
    fn from(receipt: Receipt) -> Self {
        ReceiptView {
            predecessor_id: receipt.predecessor_id,
            receiver_id: receipt.receiver_id,
            receipt_id: receipt.receipt_id.into(),
            receipt: match receipt.receipt {
                ReceiptEnum::Action(action_receipt) => ReceiptEnumView::Action {
                    signer_id: action_receipt.signer_id,
                    signer_public_key: action_receipt.signer_public_key.into(),
                    gas_price: action_receipt.gas_price,
                    output_data_receivers: action_receipt
                        .output_data_receivers
                        .into_iter()
                        .map(|data_receiver| DataReceiverView {
                            data_id: data_receiver.data_id.into(),
                            receiver_id: data_receiver.receiver_id,
                        })
                        .collect(),
                    input_data_ids: action_receipt
                        .input_data_ids
                        .into_iter()
                        .map(Into::into)
                        .collect(),
                    actions: action_receipt.actions.into_iter().map(Into::into).collect(),
                },
                ReceiptEnum::Data(data_receipt) => ReceiptEnumView::Data {
                    data_id: data_receipt.data_id.into(),
                    data: data_receipt.data,
                },
            },
        }
    }
}

impl TryFrom<ReceiptView> for Receipt {
    type Error = Box<dyn std::error::Error>;

    fn try_from(receipt_view: ReceiptView) -> Result<Self, Self::Error> {
        Ok(Receipt {
            predecessor_id: receipt_view.predecessor_id,
            receiver_id: receipt_view.receiver_id,
            receipt_id: receipt_view.receipt_id.into(),
            receipt: match receipt_view.receipt {
                ReceiptEnumView::Action {
                    signer_id,
                    signer_public_key,
                    gas_price,
                    output_data_receivers,
                    input_data_ids,
                    actions,
                } => ReceiptEnum::Action(ActionReceipt {
                    signer_id,
                    signer_public_key: signer_public_key.into(),
                    gas_price,
                    output_data_receivers: output_data_receivers
                        .into_iter()
                        .map(|data_receiver_view| DataReceiver {
                            data_id: data_receiver_view.data_id.into(),
                            receiver_id: data_receiver_view.receiver_id,
                        })
                        .collect(),
                    input_data_ids: input_data_ids.into_iter().map(Into::into).collect(),
                    actions: actions
                        .into_iter()
                        .map(TryInto::try_into)
                        .collect::<Result<Vec<_>, _>>()?,
                }),
                ReceiptEnumView::Data { data_id, data } => {
                    ReceiptEnum::Data(DataReceipt { data_id: data_id.into(), data })
                }
            },
        })
    }
}

use std::borrow::Borrow;
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::iter::FromIterator;

use protobuf::well_known_types::BytesValue;
use protobuf::{RepeatedField, SingularPtrField};

use near_protos::receipt as receipt_proto;

use crate::crypto::signature::PublicKey;
use crate::hash::CryptoHash;
use crate::logging;
use crate::serialize::{option_bytes_format, u128_dec_format};
use crate::transaction::{Action, TransactionResult, TransferAction};
use crate::types::{AccountId, Balance};
use crate::utils::{proto_to_type, proto_to_vec, system_account};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ReceiptInfo {
    pub receipt: Receipt,
    pub block_index: u64,
    pub result: TransactionResult,
}

#[derive(Hash, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Receipt {
    pub predecessor_id: AccountId,
    pub receiver_id: AccountId,
    pub receipt_id: CryptoHash,

    pub receipt: ReceiptEnum,
}

impl TryFrom<receipt_proto::Receipt> for Receipt {
    type Error = Box<dyn std::error::Error>;

    fn try_from(r: receipt_proto::Receipt) -> Result<Self, Self::Error> {
        Ok(Receipt {
            predecessor_id: r.predecessor_id,
            receiver_id: r.receiver_id,
            receipt_id: r.receipt_id.try_into()?,
            receipt: match r.receipt {
                Some(receipt_proto::Receipt_oneof_receipt::action(action)) => {
                    action.try_into().map(ReceiptEnum::Action)
                }
                Some(receipt_proto::Receipt_oneof_receipt::data(data)) => {
                    data.try_into().map(ReceiptEnum::Data)
                }
                None => Err("No such receipt enum".into()),
            }?,
        })
    }
}

impl From<Receipt> for receipt_proto::Receipt {
    fn from(r: Receipt) -> Self {
        let receipt = match r.receipt {
            ReceiptEnum::Action(action) => {
                receipt_proto::Receipt_oneof_receipt::action(action.into())
            }
            ReceiptEnum::Data(data) => receipt_proto::Receipt_oneof_receipt::data(data.into()),
        };
        receipt_proto::Receipt {
            predecessor_id: r.predecessor_id,
            receiver_id: r.receiver_id,
            receipt_id: r.receipt_id.into(),
            receipt: Some(receipt),

            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
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

#[derive(Hash, Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum ReceiptEnum {
    Action(ActionReceipt),
    Data(DataReceipt),
}

#[derive(Hash, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct ActionReceipt {
    pub signer_id: AccountId,
    pub signer_public_key: PublicKey,

    #[serde(with = "u128_dec_format")]
    pub gas_price: Balance,

    /// If present, where to route the output data
    pub output_data_receivers: Vec<DataReceiver>,

    pub input_data_ids: Vec<CryptoHash>,

    pub actions: Vec<Action>,
}

impl TryFrom<receipt_proto::ActionReceipt> for ActionReceipt {
    type Error = Box<dyn std::error::Error>;

    fn try_from(a: receipt_proto::ActionReceipt) -> Result<Self, Self::Error> {
        Ok(ActionReceipt {
            signer_id: a.signer_id,
            signer_public_key: proto_to_type(a.signer_public_key)?,
            gas_price: proto_to_type(a.gas_price)?,
            output_data_receivers: proto_to_vec(a.output_data_receivers)?,
            input_data_ids: proto_to_vec(a.input_data_ids)?,
            actions: proto_to_vec(a.actions)?,
        })
    }
}

impl From<ActionReceipt> for receipt_proto::ActionReceipt {
    fn from(a: ActionReceipt) -> receipt_proto::ActionReceipt {
        receipt_proto::ActionReceipt {
            signer_id: a.signer_id,
            signer_public_key: SingularPtrField::some(a.signer_public_key.into()),
            gas_price: SingularPtrField::some(a.gas_price.into()),
            output_data_receivers: RepeatedField::from_iter(
                a.output_data_receivers.into_iter().map(std::convert::Into::into),
            ),
            input_data_ids: RepeatedField::from_iter(
                a.input_data_ids.into_iter().map(std::convert::Into::into),
            ),
            actions: RepeatedField::from_iter(a.actions.into_iter().map(std::convert::Into::into)),

            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

#[derive(Hash, Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct DataReceiver {
    pub data_id: CryptoHash,
    pub receiver_id: AccountId,
}

impl TryFrom<receipt_proto::ActionReceipt_DataReceiver> for DataReceiver {
    type Error = Box<dyn std::error::Error>;

    fn try_from(d: receipt_proto::ActionReceipt_DataReceiver) -> Result<Self, Self::Error> {
        Ok(DataReceiver { data_id: d.data_id.try_into()?, receiver_id: d.receiver_id })
    }
}

impl From<DataReceiver> for receipt_proto::ActionReceipt_DataReceiver {
    fn from(d: DataReceiver) -> receipt_proto::ActionReceipt_DataReceiver {
        receipt_proto::ActionReceipt_DataReceiver {
            data_id: d.data_id.into(),
            receiver_id: d.receiver_id,

            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct DataReceipt {
    pub data_id: CryptoHash,
    #[serde(with = "option_bytes_format")]
    pub data: Option<Vec<u8>>,
}

impl TryFrom<receipt_proto::DataReceipt> for DataReceipt {
    type Error = Box<dyn std::error::Error>;

    fn try_from(d: receipt_proto::DataReceipt) -> Result<Self, Self::Error> {
        Ok(DataReceipt {
            data_id: d.data_id.try_into()?,
            data: d.data.into_option().map(|v| v.value),
        })
    }
}

impl From<DataReceipt> for receipt_proto::DataReceipt {
    fn from(d: DataReceipt) -> receipt_proto::DataReceipt {
        receipt_proto::DataReceipt {
            data_id: d.data_id.into(),
            data: SingularPtrField::from_option(d.data.map(|v| {
                let mut res = BytesValue::new();
                res.set_value(v);
                res
            })),

            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

impl fmt::Debug for DataReceipt {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("DataReceipt")
            .field("data_id", &self.data_id)
            .field("data", &format_args!("{}", logging::pretty_result(&self.data)))
            .finish()
    }
}

use crate::action::GlobalContractIdentifier;
use crate::errors::{EpochError, IntegerOverflowError};
use crate::fees::{compute_receipt_congestion_gas, compute_receipt_size};
use crate::hash::CryptoHash;
use crate::shard_layout::ShardLayout;
use crate::transaction::{Action, TransferAction};
use crate::types::{AccountId, Balance, BlockHeight, ProtocolVersion, ShardId};
use crate::version::ProtocolFeature;
use borsh::{BorshDeserialize, BorshSerialize};
use itertools::Itertools;
use near_crypto::{KeyType, PublicKey};
use near_fmt::AbbrBytes;
use near_parameters::RuntimeConfig;
use near_primitives_core::types::Gas;
use near_schema_checker_lib::ProtocolSchema;
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::io::{self, Read};
use std::io::{Error, ErrorKind};
use std::sync::Arc;

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
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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

/// A Receipt wrapper. Currently only V0 is used. V1 was prepared for a priority
/// feature that was never activated and has been removed.
#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize, ProtocolSchema)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(untagged)]
pub enum Receipt {
    V0(ReceiptV0),
    // To introduce a V1 here, we need custom implementation of BorshSerialize
    // and BorshDeserialize (aka "hackery") to maintain backwards compatibility.
    //
    // Prior this was used: First field in `ReceiptV0` is an `AccountId`, which
    // is at most 64 bytes. For all valid `ReceiptV0`, the second byte must be 0
    // because of the little endian encoding of the length of the account id.
    // On the other hand, for `ReceiptV1`, always set the first byte to 1, and
    // `AccountId` must have nonzero length, so the second byte must not be
    // zero.
}

/// A receipt that is stored in the state with added metadata. A receipt may be
/// stored in the state as a delayed receipt, buffered receipt or a promise
/// yield receipt. The metadata contains additional information about receipt
///
/// Please note that the StateStoredReceipt implements custom serialization and
/// deserialization. The encoding starts with two [STATE_STORED_RECEIPT_TAG]
/// bytes that distinguish it from the plain [Receipt] encoding that was used
/// in state before protocol version 72. Please see the [BorshSerialize] impl
/// for the exact format.
///
/// This struct is versioned so that it can be enhanced in the future.
#[derive(PartialEq, Eq, Debug, ProtocolSchema)]
pub enum StateStoredReceipt<'a> {
    V1(StateStoredReceiptV1<'a>),
}

/// The V1 of StateStoredReceipt. It contains the receipt and metadata.
///
/// V0 held the same data but predated the outgoing buffer metadata, which was
/// therefore not updated when a V0 receipt was stored or removed. V0 has not
/// been written since the bandwidth scheduler was enabled (release 2.5.0) and
/// is no longer supported.
#[derive(BorshDeserialize, BorshSerialize, PartialEq, Eq, Debug, ProtocolSchema)]
pub struct StateStoredReceiptV1<'a> {
    pub receipt: Cow<'a, Receipt>,
    pub metadata: StateStoredReceiptMetadata,
}

/// The metadata associated with the receipt stored in state.
#[derive(BorshDeserialize, BorshSerialize, PartialEq, Eq, Debug, ProtocolSchema)]
pub struct StateStoredReceiptMetadata {
    /// The congestion gas of the receipt when it was stored in the state.
    /// Please see [compute_receipt_congestion_gas] for more details.
    pub congestion_gas: Gas,
    /// The congestion size of the receipt when it was stored in the state.
    /// Please see [compute_receipt_size] for more details.
    pub congestion_size: u64,
}

impl StateStoredReceiptMetadata {
    /// Computes the metadata of a receipt that is about to be stored in a
    /// state queue, the same way the runtime does when it delays or buffers
    /// the receipt.
    pub fn compute(
        receipt: &Receipt,
        config: &RuntimeConfig,
    ) -> Result<Self, IntegerOverflowError> {
        let congestion_gas = compute_receipt_congestion_gas(receipt, config)?;
        let congestion_size = compute_receipt_size(receipt)?;
        Ok(Self { congestion_gas, congestion_size })
    }
}

/// The tag that is used to differentiate between the Receipt and StateStoredReceipt.
const STATE_STORED_RECEIPT_TAG: u8 = u8::MAX;

impl<'a> StateStoredReceipt<'a> {
    pub fn new_owned(receipt: Receipt, metadata: StateStoredReceiptMetadata) -> Self {
        let receipt = Cow::Owned(receipt);
        Self::V1(StateStoredReceiptV1 { receipt, metadata })
    }

    pub fn new_borrowed(receipt: &'a Receipt, metadata: StateStoredReceiptMetadata) -> Self {
        let receipt = Cow::Borrowed(receipt);

        Self::V1(StateStoredReceiptV1 { receipt, metadata })
    }

    pub fn into_receipt(self) -> Receipt {
        match self {
            StateStoredReceipt::V1(v1) => v1.receipt.into_owned(),
        }
    }

    pub fn get_receipt(&self) -> &Receipt {
        match self {
            StateStoredReceipt::V1(v1) => &v1.receipt,
        }
    }

    pub fn metadata(&self) -> &StateStoredReceiptMetadata {
        match self {
            StateStoredReceipt::V1(v1) => &v1.metadata,
        }
    }
}

impl BorshSerialize for Receipt {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            Receipt::V0(receipt) => BorshSerialize::serialize(&receipt, writer),
        }
    }
}

impl BorshDeserialize for Receipt {
    fn deserialize_reader<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        Ok(Receipt::V0(ReceiptV0::deserialize_reader(reader)?))
    }
}

impl BorshSerialize for StateStoredReceipt<'_> {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        // The serialization format for StateStored receipt is as follows:
        // Byte 1: STATE_STORED_RECEIPT_TAG
        // Byte 2: STATE_STORED_RECEIPT_TAG
        // Byte 3: enum version (e.g. V1 => 1_u8)
        // serialized variant value

        BorshSerialize::serialize(&STATE_STORED_RECEIPT_TAG, writer)?;
        BorshSerialize::serialize(&STATE_STORED_RECEIPT_TAG, writer)?;
        match self {
            StateStoredReceipt::V1(v1) => {
                BorshSerialize::serialize(&1_u8, writer)?;
                BorshSerialize::serialize(&v1, writer)?;
            }
        }
        Ok(())
    }
}

impl BorshDeserialize for StateStoredReceipt<'_> {
    fn deserialize_reader<R: Read>(reader: &mut R) -> io::Result<Self> {
        let u1 = u8::deserialize_reader(reader)?;
        let u2 = u8::deserialize_reader(reader)?;
        let u3 = u8::deserialize_reader(reader)?;

        if u1 != STATE_STORED_RECEIPT_TAG || u2 != STATE_STORED_RECEIPT_TAG {
            let error = format!(
                "Invalid tag found when deserializing StateStoredReceipt. Found: {}, {}. Expected: {}, {}",
                u1, u2, STATE_STORED_RECEIPT_TAG, STATE_STORED_RECEIPT_TAG
            );
            let error = Error::new(ErrorKind::Other, error);
            return Err(io::Error::new(ErrorKind::InvalidData, error));
        }

        match u3 {
            1 => {
                let v1 = StateStoredReceiptV1::deserialize_reader(reader)?;
                Ok(StateStoredReceipt::V1(v1))
            }
            v => {
                let error = format!(
                    "Invalid version found when deserializing StateStoredReceipt. Found: {}. Expected: 1",
                    v
                );
                let error = Error::new(ErrorKind::Other, error);
                Err(io::Error::new(ErrorKind::InvalidData, error))
            }
        }
    }
}

impl Receipt {
    /// Converts a transaction into a receipt created at `protocol_version`.
    pub fn from_tx(
        receipt_id: CryptoHash,
        tx_signer_id: AccountId,
        tx_receiver_id: AccountId,
        signer_public_key: PublicKey,
        gas_price: Balance,
        actions: Vec<Action>,
        protocol_version: ProtocolVersion,
    ) -> Self {
        let authorization_context = AuthorizationContext::AccessKey(signer_public_key);
        let creation_protocol_version = CreationProtocolVersion(protocol_version);
        let receipt = ReceiptEnum::new_action_receipt(
            protocol_version,
            creation_protocol_version,
            tx_signer_id.clone(),
            authorization_context,
            gas_price,
            actions,
        );
        Receipt::V0(ReceiptV0 {
            predecessor_id: tx_signer_id,
            receiver_id: tx_receiver_id,
            receipt_id,
            receipt,
        })
    }

    pub fn receiver_id(&self) -> &AccountId {
        let Receipt::V0(receipt) = self;
        &receipt.receiver_id
    }

    pub fn set_receiver_id(&mut self, receiver_id: AccountId) {
        let Receipt::V0(receipt) = self;
        receipt.receiver_id = receiver_id;
    }

    pub fn predecessor_id(&self) -> &AccountId {
        let Receipt::V0(receipt) = self;
        &receipt.predecessor_id
    }

    pub fn set_predecessor_id(&mut self, predecessor_id: AccountId) {
        let Receipt::V0(receipt) = self;
        receipt.predecessor_id = predecessor_id;
    }

    pub fn receipt(&self) -> &ReceiptEnum {
        let Receipt::V0(receipt) = self;
        &receipt.receipt
    }

    pub fn versioned_receipt(&self) -> VersionedReceiptEnum<'_> {
        let Receipt::V0(receipt) = self;
        VersionedReceiptEnum::from(&receipt.receipt)
    }

    /// The action receipt of any version, or `None` if this is not an action
    /// receipt. A promise yield receipt is not an action receipt here.
    pub fn action_receipt(&self) -> Option<VersionedActionReceipt<'_>> {
        match self.versioned_receipt() {
            VersionedReceiptEnum::Action(action_receipt) => Some(action_receipt),
            VersionedReceiptEnum::Data(_)
            | VersionedReceiptEnum::PromiseYield(_)
            | VersionedReceiptEnum::PromiseResume(_)
            | VersionedReceiptEnum::GlobalContractDistribution(_) => None,
        }
    }

    pub fn receipt_mut(&mut self) -> &mut ReceiptEnum {
        let Receipt::V0(receipt) = self;
        &mut receipt.receipt
    }

    pub fn take_versioned_receipt<'a>(self) -> VersionedReceiptEnum<'a> {
        let Receipt::V0(receipt) = self;
        VersionedReceiptEnum::from(receipt.receipt)
    }

    pub fn receipt_id(&self) -> &CryptoHash {
        let Receipt::V0(receipt) = self;
        &receipt.receipt_id
    }

    pub fn set_receipt_id(&mut self, receipt_id: CryptoHash) {
        let Receipt::V0(receipt) = self;
        receipt.receipt_id = receipt_id;
    }

    pub fn refund_to(&self) -> &Option<AccountId> {
        match self.receipt() {
            ReceiptEnum::Action(_)
            | ReceiptEnum::Data(_)
            | ReceiptEnum::PromiseYield(_)
            | ReceiptEnum::PromiseResume(_)
            | ReceiptEnum::GlobalContractDistribution(_) => &None,
            ReceiptEnum::ActionV2(action_receipt_v2)
            | ReceiptEnum::PromiseYieldV2(action_receipt_v2) => &action_receipt_v2.refund_to,
            ReceiptEnum::ActionV3(action_receipt_v3)
            | ReceiptEnum::PromiseYieldV3(action_receipt_v3) => &action_receipt_v3.refund_to,
        }
    }

    pub fn balance_refund_receiver(&self) -> &AccountId {
        self.refund_to().as_ref().unwrap_or_else(|| self.predecessor_id())
    }

    /// It's not a content hash, but receipt_id is unique.
    pub fn get_hash(&self) -> CryptoHash {
        *self.receipt_id()
    }

    pub fn receiver_shard_id(&self, shard_layout: &ShardLayout) -> Result<ShardId, EpochError> {
        let shard_id = match self.receipt() {
            ReceiptEnum::Action(_)
            | ReceiptEnum::ActionV2(_)
            | ReceiptEnum::ActionV3(_)
            | ReceiptEnum::Data(_)
            | ReceiptEnum::PromiseYield(_)
            | ReceiptEnum::PromiseYieldV2(_)
            | ReceiptEnum::PromiseYieldV3(_)
            | ReceiptEnum::PromiseResume(_) => {
                shard_layout.account_id_to_shard_id(self.receiver_id())
            }
            ReceiptEnum::GlobalContractDistribution(receipt) => {
                let target_shard = receipt.target_shard();
                if shard_layout.shard_ids().contains(&target_shard) {
                    target_shard
                } else {
                    // The target shard may be from an arbitrarily old layout (the receipt could
                    // have been delayed across multiple resharding events). resolve_to_current_shard
                    // will find a shard descendant in the current layout.
                    let Some(current_shard) = shard_layout.resolve_to_current_shard(target_shard)
                    else {
                        return Err(EpochError::ShardingError(format!(
                            "Shard {target_shard} does not exist in the shard layout or its split history",
                        )));
                    };
                    current_shard
                }
            }
        };
        Ok(shard_id)
    }

    /// An instant receipt is a receipt which should be processed immediately after the receipt that
    /// produced it, in the same chunk, irrespective of the gas limit.
    /// The expectation is that applying an instant receipt is a quick operation (e.g. setting a few values in the state).
    /// Instant receipts generally shouldn't emit new instant receipts, as it could lead to
    /// infinitely many receipts being executed in a single chunk.
    pub fn is_instant_receipt(&self) -> bool {
        match self.versioned_receipt() {
            VersionedReceiptEnum::PromiseYield(_) => {
                // PromiseYield receipts are instant receipts.
                // Applying a PromiseYield receipt is one trie write, it's okay to make it an instant receipt.
                true
            }
            VersionedReceiptEnum::Action(action_receipt) => {
                // Action receipts containing a single DeleteAccount action and no input
                // promises are instant receipts.
                // Deleting an account is a quick trie operation, it's okay to make it instant.
                matches!(action_receipt.actions(), [Action::DeleteAccount(_)])
                    && action_receipt.input_data_ids().is_empty()
            }
            VersionedReceiptEnum::Data(_)
            | VersionedReceiptEnum::PromiseResume(_)
            | VersionedReceiptEnum::GlobalContractDistribution(_) => false,
        }
    }

    /// Generates a receipt with a balance refund transfer from the system
    /// account.
    ///
    /// This should be used for token refunds instead of gas refunds.
    /// It doesn't refund the allowance of the access key. For gas refunds use `new_gas_refund`.
    pub fn new_balance_refund(
        receiver_id: &AccountId,
        refund: Balance,
        protocol_version: ProtocolVersion,
    ) -> Self {
        let actions = vec![Action::Transfer(TransferAction { deposit: refund })];
        let signer_public_key = PublicKey::empty(KeyType::ED25519);
        let authorization_context = AuthorizationContext::AccessKey(signer_public_key);
        let creation_protocol_version = CreationProtocolVersion(protocol_version);
        let receipt = ReceiptEnum::new_action_receipt(
            protocol_version,
            creation_protocol_version,
            "system".parse().unwrap(),
            authorization_context,
            Balance::ZERO,
            actions,
        );
        Receipt::V0(ReceiptV0 {
            predecessor_id: "system".parse().unwrap(),
            receiver_id: receiver_id.clone(),
            receipt_id: CryptoHash::default(),
            receipt,
        })
    }

    /// Generates a receipt with a gas refund transfer from the system account.
    ///
    /// Its `signer_id` is the receiver, which marks it as a gas refund, and it
    /// carries `authorization_context`, the context of the chain that bought
    /// the gas. The execution of this receipt will try to refund the allowance
    /// of the access key, if any.
    ///
    /// NOTE: The access key may be replaced by the owner, so the execution
    /// can't rely that the access key is the same and it should use best effort
    /// for the refund.
    pub fn new_gas_refund(
        receiver_id: &AccountId,
        refund: Balance,
        authorization_context: AuthorizationContext,
        protocol_version: ProtocolVersion,
    ) -> Self {
        let actions = vec![Action::Transfer(TransferAction { deposit: refund })];
        let creation_protocol_version = CreationProtocolVersion(protocol_version);
        let receipt = ReceiptEnum::new_action_receipt(
            protocol_version,
            creation_protocol_version,
            receiver_id.clone(),
            authorization_context,
            Balance::ZERO,
            actions,
        );
        Receipt::V0(ReceiptV0 {
            predecessor_id: "system".parse().unwrap(),
            receiver_id: receiver_id.clone(),
            receipt_id: CryptoHash::default(),
            receipt,
        })
    }

    pub fn new_global_contract_distribution(
        predecessor_id: AccountId,
        receipt: GlobalContractDistributionReceipt,
    ) -> Self {
        Self::V0(ReceiptV0 {
            predecessor_id,
            receiver_id: "system".parse().unwrap(),
            receipt_id: CryptoHash::default(),
            receipt: ReceiptEnum::GlobalContractDistribution(receipt),
        })
    }
}

/// Receipt could be either ActionReceipt or DataReceipt
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum ReceiptEnum {
    Action(ActionReceipt) = 0,
    Data(DataReceipt) = 1,
    PromiseYield(ActionReceipt) = 2,
    PromiseResume(DataReceipt) = 3,
    GlobalContractDistribution(GlobalContractDistributionReceipt) = 4,
    ActionV2(ActionReceiptV2) = 5,
    PromiseYieldV2(ActionReceiptV2) = 6,
    ActionV3(ActionReceiptV3) = 7,
    PromiseYieldV3(ActionReceiptV3) = 8,
}

impl ReceiptEnum {
    /// Creates the action receipt of a transaction, of the inner actions of a
    /// meta transaction or of a refund at `protocol_version`, the current
    /// protocol version.
    ///
    /// Since `PinReceiptFees` it is an `ActionReceiptV3` that records
    /// `creation_protocol_version`. Before it, it is an `ActionReceipt` in a
    /// chain an access key authorized and an `ActionReceiptV3` in the chain of
    /// an external call.
    pub fn new_action_receipt(
        protocol_version: ProtocolVersion,
        creation_protocol_version: CreationProtocolVersion,
        signer_id: AccountId,
        authorization_context: AuthorizationContext,
        gas_price: Balance,
        actions: Vec<Action>,
    ) -> Self {
        let pin_receipt_fees = ProtocolFeature::PinReceiptFees.enabled(protocol_version);
        match authorization_context {
            AuthorizationContext::AccessKey(signer_public_key) if !pin_receipt_fees => {
                let action_receipt = ActionReceipt {
                    signer_id,
                    signer_public_key,
                    gas_price,
                    output_data_receivers: vec![],
                    input_data_ids: vec![],
                    actions,
                };
                ReceiptEnum::Action(action_receipt)
            }
            authorization_context => {
                let action_receipt = ActionReceiptV3 {
                    signer_id,
                    refund_to: None, // The refunds should go to the predecessor.
                    authorization_context,
                    gas_price,
                    creation_protocol_version,
                    output_data_receivers: vec![],
                    input_data_ids: vec![],
                    actions,
                };
                ReceiptEnum::ActionV3(action_receipt)
            }
        }
    }

    /// Creates the action receipt, or the promise yield receipt if
    /// `is_promise_yield`, of a promise that a contract created at
    /// `protocol_version`, the current protocol version.
    ///
    /// Since `PinReceiptFees` it is an `ActionReceiptV3` that records
    /// `protocol_version`. Before it, it is an `ActionReceiptV2` in a chain an
    /// access key authorized and an `ActionReceiptV3` in the chain of an
    /// external call. It records the current version rather than the version
    /// of the transaction that started the call chain, because the contract
    /// paid the static fees of the receipt from attached gas at the current
    /// fees.
    pub fn new_action_receipt_from_promise(
        protocol_version: ProtocolVersion,
        signer_id: AccountId,
        refund_to: Option<AccountId>,
        authorization_context: AuthorizationContext,
        gas_price: Balance,
        output_data_receivers: Vec<DataReceiver>,
        input_data_ids: Vec<CryptoHash>,
        actions: Vec<Action>,
        is_promise_yield: bool,
    ) -> Self {
        let pin_receipt_fees = ProtocolFeature::PinReceiptFees.enabled(protocol_version);
        let creation_protocol_version = CreationProtocolVersion(protocol_version);
        match authorization_context {
            AuthorizationContext::AccessKey(signer_public_key) if !pin_receipt_fees => {
                let action_receipt = ActionReceiptV2 {
                    signer_id,
                    refund_to,
                    signer_public_key,
                    gas_price,
                    output_data_receivers,
                    input_data_ids,
                    actions,
                };
                if is_promise_yield {
                    ReceiptEnum::PromiseYieldV2(action_receipt)
                } else {
                    ReceiptEnum::ActionV2(action_receipt)
                }
            }
            authorization_context => {
                let action_receipt = ActionReceiptV3 {
                    signer_id,
                    refund_to,
                    authorization_context,
                    gas_price,
                    creation_protocol_version,
                    output_data_receivers,
                    input_data_ids,
                    actions,
                };
                if is_promise_yield {
                    ReceiptEnum::PromiseYieldV3(action_receipt)
                } else {
                    ReceiptEnum::ActionV3(action_receipt)
                }
            }
        }
    }
}

/// ActionReceipt is derived from an Action from `Transaction or from Receipt`
/// TODO(#14709): deprecate in favor of `ActionReceiptV2`.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    PartialEq,
    Eq,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ActionReceipt {
    /// A signer of the original transaction
    pub signer_id: AccountId,
    /// An access key which was used to sign the original transaction
    pub signer_public_key: PublicKey,
    /// A gas_price which has been used to buy gas in the original transaction
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

/// ActionReceipt is derived from a set of Actions from `Transaction or from Receipt`
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    PartialEq,
    Eq,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ActionReceiptV2 {
    /// A signer of the original transaction
    pub signer_id: AccountId,
    /// The receiver of any balance refunds from this receipt if it is different from receiver_id.
    pub refund_to: Option<AccountId>,
    /// An access key which was used to sign the original transaction
    pub signer_public_key: PublicKey,
    /// A gas_price which has been used to buy gas in the original transaction
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

/// An action receipt that records what authorized its chain of receipts and
/// the protocol version at which it was created.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    PartialEq,
    Eq,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ActionReceiptV3 {
    /// A signer of the original transaction, or the contract that accepted an external call
    pub signer_id: AccountId,
    /// The receiver of any balance refunds from this receipt if it is different from receiver_id.
    pub refund_to: Option<AccountId>,
    /// What authorized the chain of receipts this receipt belongs to
    pub authorization_context: AuthorizationContext,
    /// A gas_price which has been used to buy gas in the original transaction or external call
    pub gas_price: Balance,
    /// The protocol version at which this receipt was created.
    ///
    /// The static fees of the receipt were paid with the fees of this version, so they are also
    /// charged and refunded with them, and a receipt that executes after a fee change spends
    /// exactly the gas that was paid for. Refunds are free, but record their version too.
    ///
    /// A receipt created by a contract records the version it was created at, not the version of
    /// the transaction that started the call chain, since the contract paid its static fees from
    /// attached gas at the fees of that version. The receipt of the inner actions of a meta
    /// transaction is the exception: it records the creation version of the receipt that carried
    /// the meta transaction, since the relayer prepaid its static fees with that receipt.
    pub creation_protocol_version: CreationProtocolVersion,
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

/// The protocol version at which a receipt was created.
/// See [`ActionReceiptV3::creation_protocol_version`].
///
/// It is a separate type so that it can't be mixed up with the current protocol version. It is
/// serialized in the same way as a [`ProtocolVersion`].
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    PartialEq,
    Eq,
    Clone,
    Copy,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(transparent)]
pub struct CreationProtocolVersion(pub ProtocolVersion);

/// What authorized a chain of receipts: an access key that signed a transaction, or an
/// external call its contract accepted.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    PartialEq,
    Eq,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum AuthorizationContext {
    AccessKey(PublicKey) = 0,
    ExternalContractCall = 1,
}

/// Convenience wrapper for common logic accessing fields on action receipts of
/// different versions.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum VersionedActionReceipt<'a> {
    V1(Cow<'a, ActionReceipt>),
    V2(Cow<'a, ActionReceiptV2>),
    V3(Cow<'a, ActionReceiptV3>),
}

impl VersionedActionReceipt<'_> {
    /// A signer of the original transaction, or the contract that accepted an external call
    pub fn signer_id(&self) -> &AccountId {
        match self {
            VersionedActionReceipt::V1(action_receipt) => &action_receipt.signer_id,
            VersionedActionReceipt::V2(action_receipt) => &action_receipt.signer_id,
            VersionedActionReceipt::V3(action_receipt) => &action_receipt.signer_id,
        }
    }

    /// The receiver of any balance refunds from this receipt if it is different from receiver_id.
    pub fn refund_to(&self) -> &Option<AccountId> {
        match self {
            VersionedActionReceipt::V1(_) => &None,
            VersionedActionReceipt::V2(action_receipt) => &action_receipt.refund_to,
            VersionedActionReceipt::V3(action_receipt) => &action_receipt.refund_to,
        }
    }

    /// The protocol version at which the receipt was created, if recorded.
    /// See [`ActionReceiptV3::creation_protocol_version`].
    pub fn creation_protocol_version(&self) -> Option<CreationProtocolVersion> {
        match self {
            VersionedActionReceipt::V1(_) | VersionedActionReceipt::V2(_) => None,
            VersionedActionReceipt::V3(action_receipt) => {
                Some(action_receipt.creation_protocol_version)
            }
        }
    }

    /// An access key which was used to sign the original transaction, absent in the chain of an
    /// external call
    pub fn signer_public_key(&self) -> Option<&PublicKey> {
        match self {
            VersionedActionReceipt::V1(action_receipt) => Some(&action_receipt.signer_public_key),
            VersionedActionReceipt::V2(action_receipt) => Some(&action_receipt.signer_public_key),
            VersionedActionReceipt::V3(action_receipt) => {
                match &action_receipt.authorization_context {
                    AuthorizationContext::AccessKey(public_key) => Some(public_key),
                    AuthorizationContext::ExternalContractCall => None,
                }
            }
        }
    }

    /// What authorized the chain of receipts this receipt belongs to
    pub fn authorization_context(&self) -> Cow<'_, AuthorizationContext> {
        match self {
            VersionedActionReceipt::V1(action_receipt) => Cow::Owned(
                AuthorizationContext::AccessKey(action_receipt.signer_public_key.clone()),
            ),
            VersionedActionReceipt::V2(action_receipt) => Cow::Owned(
                AuthorizationContext::AccessKey(action_receipt.signer_public_key.clone()),
            ),
            VersionedActionReceipt::V3(action_receipt) => {
                Cow::Borrowed(&action_receipt.authorization_context)
            }
        }
    }

    /// A gas_price which has been used to buy gas in the original transaction or external call
    pub fn gas_price(&self) -> Balance {
        match self {
            VersionedActionReceipt::V1(action_receipt) => action_receipt.gas_price,
            VersionedActionReceipt::V2(action_receipt) => action_receipt.gas_price,
            VersionedActionReceipt::V3(action_receipt) => action_receipt.gas_price,
        }
    }

    /// If present, where to route the output data
    pub fn output_data_receivers(&self) -> &[DataReceiver] {
        match self {
            VersionedActionReceipt::V1(action_receipt) => &action_receipt.output_data_receivers,
            VersionedActionReceipt::V2(action_receipt) => &action_receipt.output_data_receivers,
            VersionedActionReceipt::V3(action_receipt) => &action_receipt.output_data_receivers,
        }
    }

    /// A list of the input data dependencies for this Receipt to process.
    /// If all `input_data_ids` for this receipt are delivered to the account
    /// that means we have all the `ReceivedData` input which will be than converted to a
    /// `PromiseResult::Successful(value)` or `PromiseResult::Failed`
    /// depending on `ReceivedData` is `Some(_)` or `None`
    pub fn input_data_ids(&self) -> &[CryptoHash] {
        match self {
            VersionedActionReceipt::V1(action_receipt) => &action_receipt.input_data_ids,
            VersionedActionReceipt::V2(action_receipt) => &action_receipt.input_data_ids,
            VersionedActionReceipt::V3(action_receipt) => &action_receipt.input_data_ids,
        }
    }

    /// A list of actions to process when all input_data_ids are filled
    pub fn actions(&self) -> &[Action] {
        match self {
            VersionedActionReceipt::V1(action_receipt) => &action_receipt.actions,
            VersionedActionReceipt::V2(action_receipt) => &action_receipt.actions,
            VersionedActionReceipt::V3(action_receipt) => &action_receipt.actions,
        }
    }
}

impl<'a> From<&'a ActionReceipt> for VersionedActionReceipt<'a> {
    fn from(other: &'a ActionReceipt) -> Self {
        VersionedActionReceipt::V1(Cow::Borrowed(other))
    }
}

impl From<ActionReceipt> for VersionedActionReceipt<'_> {
    fn from(other: ActionReceipt) -> Self {
        VersionedActionReceipt::V1(Cow::Owned(other))
    }
}

impl<'a> From<&'a ActionReceiptV2> for VersionedActionReceipt<'a> {
    fn from(other: &'a ActionReceiptV2) -> Self {
        VersionedActionReceipt::V2(Cow::Borrowed(other))
    }
}

impl From<ActionReceiptV2> for VersionedActionReceipt<'_> {
    fn from(other: ActionReceiptV2) -> Self {
        VersionedActionReceipt::V2(Cow::Owned(other))
    }
}

impl<'a> From<&'a ActionReceiptV3> for VersionedActionReceipt<'a> {
    fn from(other: &'a ActionReceiptV3) -> Self {
        VersionedActionReceipt::V3(Cow::Borrowed(other))
    }
}

impl From<ActionReceiptV3> for VersionedActionReceipt<'_> {
    fn from(other: ActionReceiptV3) -> Self {
        VersionedActionReceipt::V3(Cow::Owned(other))
    }
}

/// Convenience wrapper for common logic accessing fields on [`ReceiptEnum`] of
/// different versions of its variants.
///
/// Note: So far, only action receipts are versioned. Other versioned variants
/// can be added later if and when necessary. Then, the Cow pointer might be
/// better pushed down, as done it [`VersionedActionReceipt`]. (Cow pointers are
/// useful here to allow using the wrapper with owned and borrowed values
/// without cloning.)
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum VersionedReceiptEnum<'a> {
    Action(VersionedActionReceipt<'a>),
    Data(Cow<'a, DataReceipt>),
    PromiseYield(VersionedActionReceipt<'a>),
    PromiseResume(Cow<'a, DataReceipt>),
    GlobalContractDistribution(Cow<'a, GlobalContractDistributionReceipt>),
}

impl<'a> From<&'a ReceiptEnum> for VersionedReceiptEnum<'a> {
    fn from(other: &'a ReceiptEnum) -> Self {
        match other {
            ReceiptEnum::Action(action_receipt) => {
                VersionedReceiptEnum::Action(action_receipt.into())
            }
            ReceiptEnum::Data(data_receipt) => {
                VersionedReceiptEnum::Data(Cow::Borrowed(data_receipt))
            }
            ReceiptEnum::PromiseYield(action_receipt) => {
                VersionedReceiptEnum::PromiseYield(action_receipt.into())
            }
            ReceiptEnum::PromiseResume(data_receipt) => {
                VersionedReceiptEnum::PromiseResume(Cow::Borrowed(data_receipt))
            }
            ReceiptEnum::GlobalContractDistribution(global_contract_distribution_receipt) => {
                VersionedReceiptEnum::GlobalContractDistribution(Cow::Borrowed(
                    global_contract_distribution_receipt,
                ))
            }
            ReceiptEnum::ActionV2(action_receipt) => {
                VersionedReceiptEnum::Action(action_receipt.into())
            }
            ReceiptEnum::PromiseYieldV2(action_receipt) => {
                VersionedReceiptEnum::PromiseYield(action_receipt.into())
            }
            ReceiptEnum::ActionV3(action_receipt) => {
                VersionedReceiptEnum::Action(action_receipt.into())
            }
            ReceiptEnum::PromiseYieldV3(action_receipt) => {
                VersionedReceiptEnum::PromiseYield(action_receipt.into())
            }
        }
    }
}

impl<'a> From<ReceiptEnum> for VersionedReceiptEnum<'a> {
    fn from(other: ReceiptEnum) -> Self {
        match other {
            ReceiptEnum::Action(action_receipt) => {
                VersionedReceiptEnum::Action(action_receipt.into())
            }
            ReceiptEnum::Data(data_receipt) => VersionedReceiptEnum::Data(Cow::Owned(data_receipt)),
            ReceiptEnum::PromiseYield(action_receipt) => {
                VersionedReceiptEnum::PromiseYield(action_receipt.into())
            }
            ReceiptEnum::PromiseResume(data_receipt) => {
                VersionedReceiptEnum::PromiseResume(Cow::Owned(data_receipt))
            }
            ReceiptEnum::GlobalContractDistribution(global_contract_distribution_receipt) => {
                VersionedReceiptEnum::GlobalContractDistribution(Cow::Owned(
                    global_contract_distribution_receipt,
                ))
            }
            ReceiptEnum::ActionV2(action_receipt) => {
                VersionedReceiptEnum::Action(action_receipt.into())
            }
            ReceiptEnum::PromiseYieldV2(action_receipt) => {
                VersionedReceiptEnum::PromiseYield(action_receipt.into())
            }
            ReceiptEnum::ActionV3(action_receipt) => {
                VersionedReceiptEnum::Action(action_receipt.into())
            }
            ReceiptEnum::PromiseYieldV3(action_receipt) => {
                VersionedReceiptEnum::PromiseYield(action_receipt.into())
            }
        }
    }
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
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct DataReceipt {
    pub data_id: CryptoHash,
    #[serde_as(as = "Option<Base64>")]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
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
#[derive(BorshSerialize, BorshDeserialize, Hash, PartialEq, Eq, Clone, ProtocolSchema)]
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

#[derive(
    BorshSerialize,
    BorshDeserialize,
    Hash,
    PartialEq,
    Eq,
    Clone,
    Debug,
    serde::Deserialize,
    serde::Serialize,
    ProtocolSchema,
)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum GlobalContractDistributionReceipt {
    V1(GlobalContractDistributionReceiptV1) = 0,
    V2(GlobalContractDistributionReceiptV2) = 1,
}

impl GlobalContractDistributionReceipt {
    pub fn new(
        id: GlobalContractIdentifier,
        target_shard: ShardId,
        already_delivered_shards: Vec<ShardId>,
        code: Arc<[u8]>,
        nonce: u64,
    ) -> Self {
        Self::new_v2(id, target_shard, already_delivered_shards, code, nonce)
    }

    pub fn new_v1(
        id: GlobalContractIdentifier,
        target_shard: ShardId,
        already_delivered_shards: Vec<ShardId>,
        code: Arc<[u8]>,
    ) -> Self {
        Self::V1(GlobalContractDistributionReceiptV1 {
            id,
            target_shard,
            already_delivered_shards,
            code,
        })
    }

    pub fn new_v2(
        id: GlobalContractIdentifier,
        target_shard: ShardId,
        already_delivered_shards: Vec<ShardId>,
        code: Arc<[u8]>,
        nonce: u64,
    ) -> Self {
        Self::V2(GlobalContractDistributionReceiptV2 {
            id,
            target_shard,
            already_delivered_shards,
            code,
            nonce,
        })
    }

    pub fn id(&self) -> &GlobalContractIdentifier {
        match &self {
            Self::V1(v1) => &v1.id,
            Self::V2(v2) => &v2.id,
        }
    }

    pub fn target_shard(&self) -> ShardId {
        match &self {
            Self::V1(v1) => v1.target_shard,
            Self::V2(v2) => v2.target_shard,
        }
    }

    pub fn already_delivered_shards(&self) -> &[ShardId] {
        match &self {
            Self::V1(v1) => &v1.already_delivered_shards,
            Self::V2(v2) => &v2.already_delivered_shards,
        }
    }

    pub fn code(&self) -> &Arc<[u8]> {
        match &self {
            Self::V1(v1) => &v1.code,
            Self::V2(v2) => &v2.code,
        }
    }

    /// Returns the nonce of the distribution.
    /// V1 receipts return 0, V2 receipts return their stored nonce.
    pub fn nonce(&self) -> u64 {
        match &self {
            Self::V1(_) => 0,
            Self::V2(v2) => v2.nonce,
        }
    }

    /// Returns the nonce of the distribution.
    /// V1 receipts return None, V2 receipts return their stored nonce.
    pub fn maybe_nonce(&self) -> Option<u64> {
        match &self {
            Self::V1(_) => None,
            Self::V2(v2) => Some(v2.nonce),
        }
    }

    /// Clones the receipt with an updated target shard and already-delivered shards list.
    pub fn forward(&self, target_shard: ShardId, already_delivered_shards: Vec<ShardId>) -> Self {
        match self {
            Self::V1(v1) => Self::V1(GlobalContractDistributionReceiptV1 {
                target_shard,
                already_delivered_shards,
                ..v1.clone()
            }),
            Self::V2(v2) => Self::V2(GlobalContractDistributionReceiptV2 {
                target_shard,
                already_delivered_shards,
                ..v2.clone()
            }),
        }
    }
}

#[serde_as]
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Hash,
    PartialEq,
    Eq,
    Clone,
    serde::Deserialize,
    serde::Serialize,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct GlobalContractDistributionReceiptV1 {
    id: GlobalContractIdentifier,
    target_shard: ShardId,
    already_delivered_shards: Vec<ShardId>,
    #[serde_as(as = "Base64")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    code: Arc<[u8]>,
}

impl fmt::Debug for GlobalContractDistributionReceiptV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GlobalContractDistributionReceipt")
            .field("id", &self.id)
            .field("target_shard", &self.target_shard)
            .field("already_delivered_shards", &self.already_delivered_shards)
            .field("code", &..)
            .finish()
    }
}

#[serde_as]
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Hash,
    PartialEq,
    Eq,
    Clone,
    serde::Deserialize,
    serde::Serialize,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct GlobalContractDistributionReceiptV2 {
    id: GlobalContractIdentifier,
    target_shard: ShardId,
    already_delivered_shards: Vec<ShardId>,
    #[serde_as(as = "Base64")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    code: Arc<[u8]>,
    nonce: u64,
}

impl fmt::Debug for GlobalContractDistributionReceiptV2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GlobalContractDistributionReceiptV2")
            .field("id", &self.id)
            .field("target_shard", &self.target_shard)
            .field("already_delivered_shards", &self.already_delivered_shards)
            .field("code", &..)
            .field("nonce", &self.nonce)
            .finish()
    }
}

/// Stores indices for a persistent queue for delayed receipts that didn't fit into a block.
#[derive(Default, BorshSerialize, BorshDeserialize, Clone, PartialEq, Debug, ProtocolSchema)]
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
#[derive(Default, BorshSerialize, BorshDeserialize, Clone, PartialEq, Debug, ProtocolSchema)]
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
#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Debug, ProtocolSchema)]
pub struct PromiseYieldTimeout {
    /// The account on which the yielded promise was created
    pub account_id: AccountId,
    /// The `data_id` used to identify the awaited input data
    pub data_id: CryptoHash,
    /// The block height before which the data must be submitted
    pub expires_at: BlockHeight,
}

/// Stores indices for a persistent queue in the state trie.
#[derive(Default, BorshSerialize, BorshDeserialize, Clone, PartialEq, Debug, ProtocolSchema)]
pub struct TrieQueueIndices {
    // First inclusive index in the queue.
    pub first_index: u64,
    // Exclusive end index of the queue
    pub next_available_index: u64,
}

impl TrieQueueIndices {
    pub fn len(&self) -> u64 {
        self.next_available_index - self.first_index
    }

    pub fn is_default(&self) -> bool {
        self.next_available_index == 0
    }
}

impl From<DelayedReceiptIndices> for TrieQueueIndices {
    fn from(other: DelayedReceiptIndices) -> Self {
        Self { first_index: other.first_index, next_available_index: other.next_available_index }
    }
}

impl From<TrieQueueIndices> for DelayedReceiptIndices {
    fn from(other: TrieQueueIndices) -> Self {
        Self { first_index: other.first_index, next_available_index: other.next_available_index }
    }
}

/// Stores indices for a persistent queue for buffered receipts that couldn't be
/// forwarded.
///
/// This is the singleton value stored in the `BUFFERED_RECEIPT_INDICES` trie
/// column.
#[derive(Default, BorshSerialize, BorshDeserialize, Clone, PartialEq, Debug, ProtocolSchema)]
pub struct BufferedReceiptIndices {
    pub shard_buffers: BTreeMap<ShardId, TrieQueueIndices>,
}

/// Map of shard to list of receipts to send to it.
pub type ReceiptResult = HashMap<ShardId, Vec<Receipt>>;

#[cfg(test)]
mod tests {
    use super::*;

    fn get_receipt_v0() -> Receipt {
        let receipt_v0 = Receipt::V0(ReceiptV0 {
            predecessor_id: "predecessor_id".parse().unwrap(),
            receiver_id: "receiver_id".parse().unwrap(),
            receipt_id: CryptoHash::default(),
            receipt: ReceiptEnum::Action(ActionReceipt {
                signer_id: "signer_id".parse().unwrap(),
                signer_public_key: PublicKey::empty(KeyType::ED25519),
                gas_price: Balance::ZERO,
                output_data_receivers: vec![],
                input_data_ids: vec![],
                actions: vec![Action::Transfer(TransferAction { deposit: Balance::ZERO })],
            }),
        });
        receipt_v0
    }

    /// Before `PinReceiptFees`, a gas refund of a chain an access key authorized keeps its V1
    /// shape, with the key.
    #[test]
    fn test_gas_refund_with_access_key_is_v1() {
        let protocol_version = ProtocolFeature::PinReceiptFees.protocol_version() - 1;
        let receiver_id: AccountId = "alice.near".parse().unwrap();
        let signer_public_key = PublicKey::from_seed(KeyType::ED25519, "alice");
        let refund = Balance::from_yoctonear(7);
        let authorization_context = AuthorizationContext::AccessKey(signer_public_key.clone());
        let expected = Receipt::V0(ReceiptV0 {
            predecessor_id: "system".parse().unwrap(),
            receiver_id: receiver_id.clone(),
            receipt_id: CryptoHash::default(),
            receipt: ReceiptEnum::Action(ActionReceipt {
                signer_id: receiver_id.clone(),
                signer_public_key,
                gas_price: Balance::ZERO,
                output_data_receivers: vec![],
                input_data_ids: vec![],
                actions: vec![Action::Transfer(TransferAction { deposit: refund })],
            }),
        });
        let gas_refund =
            Receipt::new_gas_refund(&receiver_id, refund, authorization_context, protocol_version);
        assert_eq!(borsh::to_vec(&gas_refund).unwrap(), borsh::to_vec(&expected).unwrap());
    }

    /// A gas refund of a chain an external call started is V3, with the contract as signer and
    /// no key.
    #[test]
    fn test_gas_refund_of_external_call_is_v3() {
        let protocol_version = ProtocolFeature::ExternalContractCalls.protocol_version();
        let contract_id: AccountId = "contract.near".parse().unwrap();
        let refund = Balance::from_yoctonear(7);
        let authorization_context = AuthorizationContext::ExternalContractCall;
        let gas_refund =
            Receipt::new_gas_refund(&contract_id, refund, authorization_context, protocol_version);
        let expected = Receipt::V0(ReceiptV0 {
            predecessor_id: "system".parse().unwrap(),
            receiver_id: contract_id.clone(),
            receipt_id: CryptoHash::default(),
            receipt: ReceiptEnum::ActionV3(ActionReceiptV3 {
                signer_id: contract_id,
                refund_to: None,
                authorization_context: AuthorizationContext::ExternalContractCall,
                gas_price: Balance::ZERO,
                creation_protocol_version: CreationProtocolVersion(protocol_version),
                output_data_receivers: vec![],
                input_data_ids: vec![],
                actions: vec![Action::Transfer(TransferAction { deposit: refund })],
            }),
        });
        assert_eq!(gas_refund, expected);
        let VersionedReceiptEnum::Action(action_receipt) = gas_refund.versioned_receipt() else {
            panic!("expected an action receipt, got {gas_refund:?}");
        };
        assert_eq!(action_receipt.signer_public_key(), None);
    }

    /// The creation protocol version is serialized in the same way as a plain protocol version.
    #[test]
    fn test_creation_protocol_version_serialization() {
        let protocol_version: ProtocolVersion = 89;
        let creation_protocol_version = CreationProtocolVersion(protocol_version);
        assert_eq!(
            borsh::to_vec(&creation_protocol_version).unwrap(),
            borsh::to_vec(&protocol_version).unwrap()
        );
        assert_eq!(
            serde_json::to_string(&creation_protocol_version).unwrap(),
            serde_json::to_string(&protocol_version).unwrap()
        );
    }

    #[test]
    fn test_receipt_v0_serialization() {
        let receipt_v0 = get_receipt_v0();
        let serialized_receipt = borsh::to_vec(&receipt_v0).unwrap();
        let receipt2 = Receipt::try_from_slice(&serialized_receipt).unwrap();
        assert_eq!(receipt_v0, receipt2);
    }

    #[test]
    fn test_state_stored_receipt_serialization() {
        let receipt = get_receipt_v0();
        let metadata =
            StateStoredReceiptMetadata { congestion_gas: Gas::from_gas(42), congestion_size: 43 };
        let receipt = StateStoredReceipt::new_owned(receipt, metadata);

        let serialized_receipt = borsh::to_vec(&receipt).unwrap();
        let deserialized_receipt = StateStoredReceipt::try_from_slice(&serialized_receipt).unwrap();

        assert_eq!(receipt, deserialized_receipt);
    }

    #[test]
    fn test_global_contract_distribution_receipt_v1_nonce() {
        let receipt = GlobalContractDistributionReceipt::new_v1(
            GlobalContractIdentifier::AccountId("test.near".parse().unwrap()),
            ShardId::new(0),
            vec![],
            vec![0u8; 10].into(),
        );
        // V1 receipts have no nonce, should return 0
        assert_eq!(receipt.nonce(), 0);
    }

    #[test]
    fn test_global_contract_distribution_receipt_v2_nonce() {
        let receipt = GlobalContractDistributionReceipt::new_v2(
            GlobalContractIdentifier::AccountId("test.near".parse().unwrap()),
            ShardId::new(0),
            vec![],
            vec![0u8; 10].into(),
            42,
        );
        assert_eq!(receipt.nonce(), 42);
    }

    #[test]
    fn test_global_contract_distribution_receipt_v2_serialization() {
        let receipt = GlobalContractDistributionReceipt::new_v2(
            GlobalContractIdentifier::AccountId("test.near".parse().unwrap()),
            ShardId::new(1),
            vec![ShardId::new(0)],
            vec![1u8, 2, 3].into(),
            100,
        );
        let serialized = borsh::to_vec(&receipt).unwrap();
        let deserialized = GlobalContractDistributionReceipt::try_from_slice(&serialized).unwrap();
        assert_eq!(receipt, deserialized);
        assert_eq!(deserialized.nonce(), 100);
        assert_eq!(deserialized.target_shard(), ShardId::new(1));
    }

    #[test]
    fn test_plain_receipt_is_not_a_state_stored_receipt() {
        // Before protocol version 72 receipts were stored in state without
        // the tag and the metadata. That encoding must not be mistaken for a
        // StateStoredReceipt.
        let receipt = get_receipt_v0();
        let serialized_receipt = borsh::to_vec(&receipt).unwrap();
        assert!(StateStoredReceipt::try_from_slice(&serialized_receipt).is_err());
    }

    #[test]
    fn receipt_to_tx_info_borsh_roundtrip_from_transaction() {
        let info = ReceiptToTxInfo::V1(ReceiptToTxInfoV1 {
            origin: ReceiptOrigin::FromTransaction(ReceiptOriginTransaction {
                tx_hash: CryptoHash::hash_bytes(b"tx"),
                sender_account_id: "alice.near".parse().unwrap(),
            }),
            receiver_account_id: "bob.near".parse().unwrap(),
            shard_id: ShardId::new(0),
        });
        let bytes = borsh::to_vec(&info).unwrap();
        let decoded: ReceiptToTxInfo = borsh::from_slice(&bytes).unwrap();
        assert_eq!(info, decoded);
    }

    #[test]
    fn receipt_to_tx_info_borsh_roundtrip_from_receipt() {
        let info = ReceiptToTxInfo::V1(ReceiptToTxInfoV1 {
            origin: ReceiptOrigin::FromReceipt(ReceiptOriginReceipt {
                parent_receipt_id: CryptoHash::hash_bytes(b"parent"),
                parent_predecessor_id: "alice.near".parse().unwrap(),
            }),
            receiver_account_id: "contract.near".parse().unwrap(),
            shard_id: ShardId::new(1),
        });
        let bytes = borsh::to_vec(&info).unwrap();
        let decoded: ReceiptToTxInfo = borsh::from_slice(&bytes).unwrap();
        assert_eq!(info, decoded);
    }
}

/// Source of a processed receipt, used to track how a receipt was applied.
///
/// Stored in `DBCol::ProcessedReceiptIds` (local-only, not part of consensus).
/// Adding new variants is safe for protocol but may cause deserialization
/// failures if the node rolls back to an older binary. Recovery via epoch sync.
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum ReceiptSource {
    Local = 0,
    Delayed = 1,
    Instant = 2,
    /// Marker for receipt IDs that only need their `ReceiptToTx` index entry
    /// garbage-collected. Used for receipts whose `ReceiptToTx` mapping was saved
    /// on the source shard but that don't appear in `OutcomeIds` (e.g. data
    /// receipts, PromiseResume, cross-shard receipts on a source-only node,
    /// GlobalContractDistribution receipts).
    ReceiptToTxGc = 3,
}

impl ReceiptSource {
    /// Whether an entry tagged with this source records a receipt the chunk processed,
    /// rather than an index entry to garbage-collect.
    pub fn has_receipt_body(&self) -> bool {
        match self {
            ReceiptSource::Local | ReceiptSource::Delayed | ReceiptSource::Instant => true,
            ReceiptSource::ReceiptToTxGc => false,
        }
    }
}

/// A processed receipt together with its source. Runtime-only struct, not serialized to DB.
#[derive(Debug)]
pub struct ProcessedReceipt {
    pub receipt: Receipt,
    pub source: ReceiptSource,
}

/// Lightweight metadata about a processed receipt, stored instead of the full receipt.
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum ProcessedReceiptMetadata {
    V0(ProcessedReceiptMetadataV0) = 0,
}

impl ProcessedReceiptMetadata {
    pub fn new(receipt_id: CryptoHash, source: ReceiptSource) -> Self {
        Self::V0(ProcessedReceiptMetadataV0 { receipt_id, source })
    }

    pub fn receipt_id(&self) -> &CryptoHash {
        match self {
            Self::V0(v0) => &v0.receipt_id,
        }
    }

    pub fn source(&self) -> &ReceiptSource {
        match self {
            Self::V0(v0) => &v0.source,
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq, ProtocolSchema)]
pub struct ProcessedReceiptMetadataV0 {
    pub receipt_id: CryptoHash,
    pub source: ReceiptSource,
}

/// Describes the origin of a receipt: either created directly from a transaction,
/// or spawned as a child of another receipt.
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum ReceiptOrigin {
    FromTransaction(ReceiptOriginTransaction) = 0,
    FromReceipt(ReceiptOriginReceipt) = 1,
}

/// A receipt that was created directly from a signed transaction.
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq, ProtocolSchema)]
pub struct ReceiptOriginTransaction {
    pub tx_hash: CryptoHash,
    pub sender_account_id: AccountId,
}

/// A receipt that was spawned as a child of another receipt.
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq, ProtocolSchema)]
pub struct ReceiptOriginReceipt {
    pub parent_receipt_id: CryptoHash,
    pub parent_predecessor_id: AccountId,
}

/// Versioned mapping from receipt_id to its origin information.
/// Stored in `DBCol::ReceiptToTx` to enable reverse lookups from receipt to originating transaction.
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum ReceiptToTxInfo {
    V1(ReceiptToTxInfoV1) = 0,
}

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq, ProtocolSchema)]
pub struct ReceiptToTxInfoV1 {
    pub origin: ReceiptOrigin,
    pub receiver_account_id: AccountId,
    pub shard_id: ShardId,
}

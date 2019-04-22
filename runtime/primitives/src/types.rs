use std::convert::TryFrom;

use crate::crypto::aggregate_signature::{BlsPublicKey, BlsSignature};
use crate::crypto::signature::{bs58_serializer, PublicKey, Signature};
use crate::hash::CryptoHash;
use crate::traits::Base58Encoded;
use crate::utils::{proto_to_result, to_string_value};
use near_protos::receipt as receipt_proto;
use near_protos::types as types_proto;
use protobuf::SingularPtrField;

/// Public key alias. Used to human readable public key.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
pub struct ReadablePublicKey(pub String);
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
pub struct ReadableBlsPublicKey(pub String);
/// Account identifier. Provides access to user's state.
pub type AccountId = String;
// TODO: Separate cryptographic hash from the hashmap hash.
/// Signature of a struct, i.e. signature of the struct's hash. It is a simple signature, not to be
/// confused with the multisig.
pub type StructSignature = Signature;
/// Hash used by a struct implementing the Merkle tree.
pub type MerkleHash = CryptoHash;
/// Authority identifier in current group.
pub type AuthorityId = usize;
/// Mask which authorities participated in multi sign.
pub type AuthorityMask = Vec<bool>;
/// Part of the signature.
pub type PartialSignature = BlsSignature;
/// Monetary balance of an account or an amount for transfer.
pub type Balance = u64;
/// MANA points for async calls and callbacks.
pub type Mana = u32;
/// Gas type is used to count the compute and storage within smart contract execution.
pub type Gas = u64;
/// Nonce for transactions.
pub type Nonce = u64;

pub type BlockIndex = u64;

pub type ShardId = u32;

pub type ReceiptId = Vec<u8>;
pub type CallbackId = Vec<u8>;

/// epoch for authority
pub type Epoch = u64;
/// slot for authority
pub type Slot = u64;

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub enum PromiseId {
    Receipt(ReceiptId),
    Callback(CallbackId),
    Joiner(Vec<ReceiptId>),
}

// Accounting Info contains the originator account id information required
// to identify quota that was used to issue the original signed transaction.
#[derive(Hash, Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Default)]
pub struct AccountingInfo {
    pub originator: AccountId,
    pub contract_id: Option<AccountId>,
    // TODO(#260): Add QuotaID to identify which quota was used for the call.
}

impl From<receipt_proto::AccountingInfo> for AccountingInfo {
    fn from(proto: receipt_proto::AccountingInfo) -> Self {
        AccountingInfo {
            originator: proto.originator,
            contract_id: proto.contract_id.into_option().map(|s| s.value),
        }
    }
}

impl From<AccountingInfo> for receipt_proto::AccountingInfo {
    fn from(info: AccountingInfo) -> Self {
        let contract_id = SingularPtrField::from_option(info.contract_id.map(to_string_value));
        receipt_proto::AccountingInfo {
            originator: info.originator,
            contract_id,
            ..Default::default()
        }
    }
}

#[derive(Hash, Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Default)]
pub struct ManaAccounting {
    pub accounting_info: AccountingInfo,
    pub mana_refund: Mana,
    pub gas_used: Gas,
}

impl TryFrom<receipt_proto::ManaAccounting> for ManaAccounting {
    type Error = String;

    fn try_from(proto: receipt_proto::ManaAccounting) -> Result<Self, Self::Error> {
        match proto_to_result(proto.accounting_info) {
            Ok(info) => Ok(ManaAccounting {
                accounting_info: info.into(),
                mana_refund: proto.mana_refund,
                gas_used: proto.gas_used,
            }),
            Err(e) => Err(e),
        }
    }
}

impl From<ManaAccounting> for receipt_proto::ManaAccounting {
    fn from(accounting: ManaAccounting) -> Self {
        receipt_proto::ManaAccounting {
            accounting_info: SingularPtrField::some(accounting.accounting_info.into()),
            mana_refund: accounting.mana_refund,
            gas_used: accounting.gas_used,
            ..Default::default()
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Hash, Clone)]
pub enum BlockId {
    Number(BlockIndex),
    Hash(CryptoHash),
}

/// Stores authority and its stake.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AuthorityStake {
    /// Account that stakes money.
    pub account_id: AccountId,
    /// ED25591 Public key of the proposed authority.
    pub public_key: PublicKey,
    /// BLS Public key of the proposed authority.
    #[serde(with = "bs58_serializer")]
    pub bls_public_key: BlsPublicKey,
    /// Stake / weight of the authority.
    pub amount: u64,
}

impl TryFrom<types_proto::AuthorityStake> for AuthorityStake {
    type Error = String;

    fn try_from(proto: types_proto::AuthorityStake) -> Result<Self, Self::Error> {
        let bls_key = BlsPublicKey::from_base58(&proto.bls_public_key)
            .map_err(|e| format!("cannot decode signature {:?}", e))?;
        Ok(AuthorityStake {
            account_id: proto.account_id,
            public_key: PublicKey::try_from(proto.public_key.as_str())?,
            bls_public_key: bls_key,
            amount: proto.amount,
        })
    }
}

impl From<AuthorityStake> for types_proto::AuthorityStake {
    fn from(authority: AuthorityStake) -> Self {
        types_proto::AuthorityStake {
            account_id: authority.account_id,
            public_key: authority.public_key.to_string(),
            bls_public_key: authority.bls_public_key.to_base58(),
            amount: authority.amount,
            ..Default::default()
        }
    }
}

impl PartialEq for AuthorityStake {
    fn eq(&self, other: &Self) -> bool {
        self.account_id == other.account_id && self.public_key == other.public_key
    }
}

impl Eq for AuthorityStake {}

// network types (put here to avoid cyclic dependency)
/// unique identifier for nodes on the network
// Use hash for now
pub type PeerId = CryptoHash;

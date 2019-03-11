use crate::logging;
use std::fmt;

use crate::aggregate_signature::{
    BlsAggregatePublicKey, BlsAggregateSignature, BlsPublicKey, BlsSignature,
};
use crate::hash::CryptoHash;
use crate::signature::{bs58_serializer, PublicKey, Signature};

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

pub type BlockIndex = u64;

pub type ShardId = u32;

pub type ReceiptId = Vec<u8>;
pub type CallbackId = Vec<u8>;

/// epoch for authority
pub type Epoch = u64;
/// slot for authority
pub type Slot = u64;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GroupSignature {
    #[serde(with = "bs58_serializer")]
    pub signature: BlsSignature,
    pub authority_mask: AuthorityMask,
}

impl fmt::Debug for GroupSignature {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", logging::pretty_serializable(&self))
    }
}

impl GroupSignature {
    // TODO (optimization): It's better to keep the signature in projective coordinates while
    // building it, then switch to affine coordinates at the end.  For the time being we just keep
    // it in affine coordinates always.
    pub fn add_signature(&mut self, signature: &PartialSignature, authority_id: usize) {
        if authority_id >= self.authority_mask.len() {
            self.authority_mask.resize(authority_id + 1, false);
        }
        if self.authority_mask[authority_id] {
            return;
        }
        let mut new_sig = BlsAggregateSignature::new();
        new_sig.aggregate(&signature);
        new_sig.aggregate(&self.signature);
        self.signature = new_sig.get_signature();
        self.authority_mask[authority_id] = true;
    }

    pub fn authority_count(&self) -> usize {
        self.authority_mask.iter().filter(|&x| *x).count()
    }

    pub fn verify(&self, keys: &Vec<BlsPublicKey>, message: &[u8]) -> bool {
        if keys.len() < self.authority_mask.len() {
            return false;
        }
        // Empty signature + empty public key would pass verification
        if self.authority_count() == 0 {
            return false;
        }
        let mut group_key = BlsAggregatePublicKey::new();
        for (index, key) in keys.iter().enumerate() {
            if let Some(true) = self.authority_mask.get(index) {
                group_key.aggregate(&key);
            }
        }
        group_key.get_key().verify(message, &self.signature)
    }
}

impl Default for GroupSignature {
    fn default() -> Self {
        GroupSignature {
            signature: BlsSignature::empty(),
            authority_mask: AuthorityMask::default(),
        }
    }
}

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

#[derive(Hash, Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Default)]
pub struct ManaAccounting {
    pub accounting_info: AccountingInfo,
    pub mana_refund: Mana,
    pub gas_used: Gas,
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

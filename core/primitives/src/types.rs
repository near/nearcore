use hash::{CryptoHash, hash, hash_struct};
use signature::{PublicKey, Signature};
use signature::DEFAULT_SIGNATURE;
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::collections::HashSet;

/// User identifier. Currently derived tfrom the user's public key.
pub type UID = u64;
/// Account alias. Can be an easily identifiable string, when hashed creates the AccountId.
pub type AccountAlias = String;
/// Public key alias. Used to human readable public key.
pub type ReadablePublicKey = String;
/// Account identifier. Provides access to user's state.
pub type AccountId = CryptoHash;
// TODO: Separate cryptographic hash from the hashmap hash.
/// Signature of a struct, i.e. signature of the struct's hash. It is a simple signature, not to be
/// confused with the multisig.
pub type StructSignature = Signature;
/// Hash used by a struct implementing the Merkle tree.
pub type MerkleHash = CryptoHash;
/// Part of the BLS signature.
pub type BLSSignature = Signature;
/// ID of a promise
pub type PromiseId = Vec<u8>;

impl<'a> From<&'a AccountAlias> for AccountId {
    fn from(alias: &AccountAlias) -> Self {
        hash(alias.as_bytes())
    }
}

impl<'a> From<&'a ReadablePublicKey> for PublicKey {
    fn from(alias: &ReadablePublicKey) -> Self {
        PublicKey::from(alias)
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum BlockId {
    Number(u64),
    Hash(CryptoHash),
}

// 1. Transaction structs.

/// Call view function in the contracts.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct ViewCall {
    pub account: AccountId,
    pub method_name: String,
    pub args: Vec<Vec<u8>>,
}

impl ViewCall {
    pub fn balance(account: AccountId) -> Self {
        ViewCall { account, method_name: String::new(), args: vec![] }
    }
    pub fn func_call(account: AccountId, method_name: String, args: Vec<Vec<u8>>) -> Self {
        ViewCall { account, method_name, args }
    }
}

/// Result of view call.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct ViewCallResult {
    pub account: AccountId,
    pub nonce: u64,
    pub amount: u64,
    pub stake: u64,
    pub result: Vec<u8>,
}

/// TODO: Call non-view function in the contracts.
#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct TransactionBody {
    pub nonce: u64,
    pub sender: AccountId,
    pub receiver: AccountId,
    pub amount: u64,
    pub method_name: String,
    pub args: Vec<Vec<u8>>,
}

impl TransactionBody {
    pub fn new(
        nonce: u64,
        sender: AccountId,
        receiver: AccountId,
        amount: u64,
        method_name: String,
        args: Vec<Vec<u8>>,
    ) -> Self {
        TransactionBody { nonce, sender, receiver, amount, method_name, args }
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct SignedTransaction {
    pub sender_sig: StructSignature,
    pub hash: CryptoHash,
    pub body: TransactionBody,
}

impl SignedTransaction {
    pub fn new(
        sender_sig: StructSignature,
        body: TransactionBody,
    ) -> SignedTransaction {
        SignedTransaction {
            sender_sig,
            hash: hash_struct(&body),
            body,
        }
    }

    // this is for tests
    pub fn empty() -> SignedTransaction {
        let body = TransactionBody {
            nonce: 0,
            sender: AccountId::default(),
            receiver: AccountId::default(),
            amount: 1,
            method_name: String::new(),
            args: vec![],
        };
        SignedTransaction { sender_sig: DEFAULT_SIGNATURE, hash: hash_struct(&body), body }
    }
}

// 4. TxFlow-specific structs.

pub type TxFlowHash = u64;

// 4.1 DAG-specific structs.

/// Endorsement of a representative message. Includes the epoch of the message that it endorses as
/// well as the BLS signature part. The leader should also include such self-endorsement upon
/// creation of the representative message.
#[derive(Hash, Debug, Clone)]
pub struct Endorsement {
    pub epoch: u64,
    pub signature: BLSSignature,
}

#[derive(Hash, Debug)]
pub struct BeaconChainPayload {
    pub body: Vec<SignedTransaction>,
}

#[derive(Debug, Clone)]
/// Not signed data representing TxFlow message.
pub struct MessageDataBody<P> {
    pub owner_uid: UID,
    pub parents: HashSet<TxFlowHash>,
    pub epoch: u64,
    pub payload: P,
    /// Optional endorsement of this or other representative block.
    pub endorsements: Vec<Endorsement>,
}

impl<P: Hash> Hash for MessageDataBody<P> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.owner_uid.hash(state);
        let mut vec: Vec<_> = self.parents.clone().into_iter().collect();
        vec.sort();
        for h in vec {
            h.hash(state);
        }
        self.epoch.hash(state);
        //self.payload.hash(state);
        // TODO: Hash endorsements.
    }
}

#[derive(Debug, Clone)]
pub struct SignedMessageData<P> {
    /// Signature of the hash.
    pub owner_sig: StructSignature,
    /// Hash of the body.
    pub hash: TxFlowHash,
    pub body: MessageDataBody<P>,
}

impl<P> Hash for SignedMessageData<P> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl<P> Borrow<TxFlowHash> for SignedMessageData<P> {
    fn borrow(&self) -> &TxFlowHash {
        &self.hash
    }
}

impl<P> PartialEq for SignedMessageData<P> {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl<P> Eq for SignedMessageData<P> {}

#[derive(Hash, Debug)]
pub struct ConsensusBlockHeader {
    pub body_hash: CryptoHash,
    pub prev_block_body_hash: CryptoHash,
}

#[derive(Hash, Debug)]
pub struct ConsensusBlockBody<P> {
    /// TxFlow messages that constitute that consensus block together with the endorsements.
    pub messages: Vec<SignedMessageData<P>>,
}

// 4.2 Gossip-specific structs.
#[derive(Hash, Debug)]
pub enum GossipBody<P> {
    /// A gossip with a single `SignedMessageData` that one participant decided to share with another.
    Unsolicited(SignedMessageData<P>),
    /// A reply to an unsolicited gossip with the `SignedMessageData`.
    UnsolicitedReply(SignedMessageData<P>),
    /// A request to provide a list of `SignedMessageData`'s with the following hashes.
    Fetch(Vec<TxFlowHash>),
    /// A response to the fetch request providing the requested messages.
    FetchReply(Vec<SignedMessageData<P>>),
}

/// A single unit of communication between the TxFlow participants.
#[derive(Hash, Debug)]
pub struct Gossip<P> {
    pub sender_uid: UID,
    pub receiver_uid: UID,
    pub sender_sig: StructSignature,
    pub body: GossipBody<P>,
}

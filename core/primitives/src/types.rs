use std::borrow::Borrow;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::io;

use near_protos::txflow as txflow_proto;

use crate::hash::CryptoHash;
use crate::signature::{PublicKey, Signature, DEFAULT_SIGNATURE};
use crate::serialize::{
    decode_proto, encode_proto, Encode, Decode, EncodeResult, DecodeResult
};

/// User identifier. Currently derived tfrom the user's public key.
pub type UID = u64;
/// Public key alias. Used to human readable public key.
pub type ReadablePublicKey = String;
/// Account identifier. Provides access to user's state.
pub type AccountId = String;
// TODO: Separate cryptographic hash from the hashmap hash.
/// Signature of a struct, i.e. signature of the struct's hash. It is a simple signature, not to be
/// confused with the multisig.
pub type StructSignature = Signature;
/// Hash used by a struct implementing the Merkle tree.
pub type MerkleHash = CryptoHash;
/// Mask which authorities participated in multi sign.
pub type AuthorityMask = Vec<bool>;
/// Part of the signature.
pub type PartialSignature = Signature;
/// Whole multi signature.
pub type MultiSignature = Vec<Signature>;
/// Monetary balance of an account or an amount for transfer.
pub type Balance = u64;
/// MANA points for async calls and callbacks.
pub type Mana = u32;
/// Gas type is used to count the compute and storage within smart contract execution.
pub type Gas = u64;

pub type BlockIndex = u64;

pub type ShardId = u32;

impl<'a> From<&'a ReadablePublicKey> for PublicKey {
    fn from(alias: &ReadablePublicKey) -> Self {
        PublicKey::from(alias)
    }
}

pub type ReceiptId = Vec<u8>;
pub type CallbackId = Vec<u8>;

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

// TxFlow-specific structs.

pub type TxFlowHash = u64;

// DAG-specific structs.

/// Endorsement of a representative message. Includes the epoch of the message that it endorses as
/// well as the BLS signature part. The leader should also include such self-endorsement upon
/// creation of the representative message.
#[derive(Hash, Debug, Clone, PartialEq, Eq)]
pub struct Endorsement {
    pub epoch: u64,
    pub signature: MultiSignature,
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

impl<P: Hash> PartialEq for MessageDataBody<P> {
    fn eq(&self, other: &Self) -> bool {
        let mut parents: Vec<_> = self.parents.clone().into_iter().collect();
        parents.sort();

        let mut other_parents: Vec<_> = other.parents.clone().into_iter().collect();
        other_parents.sort();

        self.owner_uid == other.owner_uid
            && self.epoch == other.epoch
            && parents == other_parents
    }
}

impl<P: Hash> Eq for MessageDataBody<P> {}

impl<P: Encode> Encode for MessageDataBody<P> {
    fn encode(&self) -> EncodeResult {
        let mut m = txflow_proto::MessageDataBody::new();
        m.set_owner_uid(self.owner_uid);
        for p in self.parents.iter() {
            m.mut_parents().push(p.clone());
        }
        m.set_epoch(self.epoch);
        m.set_payload(self.payload.encode()?);
        for e in self.endorsements.iter() {
            let mut endorsement = txflow_proto::Endorsement::new();
            endorsement.set_epoch(e.epoch);
            // TODO: fix when mutli signature is BLS.
            endorsement.set_signature(e.signature[0].as_ref().to_vec());
            m.mut_endorsements().push(endorsement);
        }
        encode_proto(&m)
    }
}

impl<P: Decode> Decode for MessageDataBody<P> {
    fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let m: txflow_proto::MessageDataBody = decode_proto(bytes)?;
        Ok(MessageDataBody {
            owner_uid: m.get_owner_uid(),
            parents: m.get_parents().iter().cloned().collect(),
            epoch: m.get_epoch(),
            payload: Decode::decode(m.get_payload())?,
            // TODO: fix when mutli signature is BLS.
            endorsements: m.get_endorsements().iter().map(|x| Endorsement { epoch: x.get_epoch(), signature: vec![DEFAULT_SIGNATURE] }).collect(),
        })
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

impl<P: Encode> Encode for SignedMessageData<P> {
    fn encode(&self) -> EncodeResult {
        let mut m = txflow_proto::SignedMessageData::new();
        m.set_owner_sig(self.owner_sig.as_ref().to_vec());
        m.set_hash(self.hash);
        m.set_body(self.body.encode()?);
        encode_proto(&m)
    }
}

impl<P: Decode> Decode for SignedMessageData<P> {
    fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let m: txflow_proto::SignedMessageData = decode_proto(bytes)?;
        Ok(SignedMessageData {
            owner_sig: DEFAULT_SIGNATURE,
            hash: m.get_hash(),
            body: Decode::decode(m.get_body())?
        })
    }
}

#[derive(Hash, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConsensusBlockHeader {
    pub body_hash: CryptoHash,
    pub prev_block_body_hash: CryptoHash,
}

#[derive(Hash, Debug, PartialEq, Eq)]
pub struct ConsensusBlockBody<P> {
    /// TxFlow messages that constitute that consensus block together with the endorsements.
    pub messages: Vec<SignedMessageData<P>>,
}

impl<P: Encode> Encode for ConsensusBlockBody<P> {
    fn encode(&self) -> EncodeResult {
        let mut m = txflow_proto::ConsensusBlockBody::new();
        for message in self.messages.iter() {
            m.mut_messages().push(message.encode()?);
        }
        encode_proto(&m)
    }
}

impl<P: Decode> Decode for ConsensusBlockBody<P> {
    fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let m: txflow_proto::ConsensusBlockBody = decode_proto(bytes)?;
        let mut messages = vec![];
        for x in m.get_messages().iter() {
            messages.push(Decode::decode(x)?);
        }
        Ok(ConsensusBlockBody {
            messages
        })
    }
}

// Gossip-specific structs.

#[derive(Hash, Debug, PartialEq, Eq)]
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
#[derive(Hash, Debug, PartialEq, Eq)]
pub struct Gossip<P> {
    pub sender_uid: UID,
    pub receiver_uid: UID,
    pub sender_sig: StructSignature,
    pub body: GossipBody<P>,
}

impl<P: Encode> Encode for Gossip<P> {
    fn encode(&self) -> EncodeResult {
        let mut m = txflow_proto::Gossip::new();
        m.set_sender_uid(self.sender_uid);
        m.set_receiver_uid(self.receiver_uid);
        m.set_sender_sig(self.sender_sig.as_ref().to_vec());
        match &self.body {
            GossipBody::Unsolicited(message) => m.set_unsolicited(message.encode()?),
            GossipBody::UnsolicitedReply(reply) => m.set_unsolicited_reply(reply.encode()?),
            GossipBody::Fetch(fetch) => {
                let mut f = txflow_proto::Fetch::new();
                for x in fetch.iter() {
                    f.mut_hashes().push(x.clone());
                }
                m.set_fetch(f);
            },
            GossipBody::FetchReply(fetch_reply) => {
                let mut f = txflow_proto::FetchReply::new();
                for x in fetch_reply.iter() {
                    f.mut_messages().push(x.encode()?);
                }
                m.set_fetch_reply(f);
            }
        };
        encode_proto(&m)
    }
}

impl<P: Decode> Decode for Gossip<P> {
    fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let m: txflow_proto::Gossip = decode_proto(bytes)?;
        let body = match &m.body {
            Some(txflow_proto::Gossip_oneof_body::unsolicited(message)) => GossipBody::Unsolicited(Decode::decode(&message)?),
            Some(txflow_proto::Gossip_oneof_body::unsolicited_reply(message)) => GossipBody::UnsolicitedReply(Decode::decode(&message)?),
            Some(txflow_proto::Gossip_oneof_body::fetch(fetch)) => GossipBody::Fetch(fetch.get_hashes().to_vec()),
            Some(txflow_proto::Gossip_oneof_body::fetch_reply(reply)) => {
                let mut messages = vec![];
                for m in reply.get_messages().iter() {
                    messages.push(Decode::decode(m)?);
                }
                GossipBody::FetchReply(messages)
            },
            _ => return Err(io::Error::new(io::ErrorKind::Other, "Failed to deserialize"))
        };
        Ok(Gossip {
            sender_uid: m.get_sender_uid(),
            receiver_uid: m.get_receiver_uid(),
            sender_sig: DEFAULT_SIGNATURE,
            body,
        })
    }
}
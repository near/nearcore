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

pub type ReceiptId = Vec<u8>;
pub type CallbackId = Vec<u8>;

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub enum PromiseId {
    Receipt(ReceiptId),
    Callback(CallbackId),
    Joiner(Vec<ReceiptId>),
}

pub type ShardId = u32;

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

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct StakeTransaction {
    pub nonce: u64,
    pub staker: AccountId,
    pub amount: u64,
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct SendMoneyTransaction {
    pub nonce: u64,
    pub sender: AccountId,
    pub receiver: AccountId,
    pub amount: u64,
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct DeployContractTransaction {
    pub nonce: u64,
    pub owner: AccountId,
    pub contract_id: AccountId,
    pub wasm_byte_array: Vec<u8>,
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct FunctionCallTransaction {
    pub nonce: u64,
    pub originator: AccountId,
    pub contract_id: AccountId,
    pub method_name: Vec<u8>,
    pub args: Vec<u8>,
}

/// TODO: Call non-view function in the contracts.
#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub enum TransactionBody {
    Stake(StakeTransaction),
    SendMoney(SendMoneyTransaction),
    DeployContract(DeployContractTransaction),
    FunctionCall(FunctionCallTransaction),
}

impl TransactionBody {
    pub fn get_nonce(&self) -> u64 {
        match self {
            TransactionBody::Stake(t) => t.nonce,
            TransactionBody::SendMoney(t) => t.nonce,
            TransactionBody::DeployContract(t) => t.nonce,
            TransactionBody::FunctionCall(t) => t.nonce,
        }
    }

    pub fn get_sender(&self) -> AccountId {
        match self {
            TransactionBody::Stake(t) => t.staker,
            TransactionBody::SendMoney(t) => t.sender,
            TransactionBody::DeployContract(t) => t.owner,
            TransactionBody::FunctionCall(t) => t.originator,
        }
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
        let body = TransactionBody::SendMoney(SendMoneyTransaction {
            nonce: 0,
            sender: AccountId::default(),
            receiver: AccountId::default(),
            amount: 0,
        });
        SignedTransaction { sender_sig: DEFAULT_SIGNATURE, hash: hash_struct(&body), body }
    }
}

#[derive(Hash, Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum ReceiptBody {
    NewCall(AsyncCall),
    Callback(CallbackResult),
    Refund,
}

#[derive(Hash, Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct AsyncCall {
    pub amount: u64,
    pub mana: u32,
    pub method_name: Vec<u8>,
    pub args: Vec<u8>,
    pub callback: Option<CallbackInfo>,
}

impl AsyncCall {
    pub fn new(method_name: Vec<u8>, args: Vec<u8>, amount: u64, mana: u32) -> Self {
        AsyncCall {
            amount,
            mana,
            method_name,
            args,
            callback: None,
        }
    }
}

pub struct Callback {
    pub method_name: Vec<u8>,
    pub args: Vec<u8>,
    pub results: Vec<Option<Vec<u8>>>,
    pub mana: u32,
}

impl Callback {
    pub fn new(method_name: Vec<u8>, args: Vec<u8>, mana: u32) -> Self {
        Callback {
            method_name,
            args,
            results: vec![],
            mana,
        }
    }
}

#[derive(Hash, Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct CallbackInfo {
    // callback id
    pub id: CallbackId,
    // index to write to
    pub result_index: usize,
    // shard that it came from
    pub shard_id: ShardId,
}

impl CallbackInfo {
    pub fn new(id: CallbackId, result_index: usize, shard_id: ShardId) -> Self {
        CallbackInfo { id, result_index, shard_id }
    }
}

#[derive(Hash, Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct CallbackResult {
    // callback id
    pub id: CallbackId,
    // callback result
    pub result: Option<Vec<u8>>,
    // index to write to
    pub result_index: usize,
}

impl CallbackResult {
    pub fn new(id: CallbackId, result: Option<Vec<u8>>, result_index: usize) -> Self {
        CallbackResult { id, result, result_index }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct ReceiptTransaction {
    // sender is the immediate predecessor
    pub sender: AccountId,
    pub receiver: AccountId,
    // nonce will be a hash
    pub nonce: Vec<u8>,
    pub body: ReceiptBody,
}

impl ReceiptTransaction {
    pub fn new(
        sender: AccountId,
        receiver: AccountId,
        nonce: Vec<u8>,
        body: ReceiptBody
    ) -> Self {
        ReceiptTransaction {
            sender,
            receiver,
            nonce,
            body,
        }
    }
}

// 2. State structs.

#[derive(Hash, Debug)]
pub struct State {
    // TODO: Fill in.
}

// 3. Epoch blocks produced by verifiers running inside a shard.

#[derive(Hash, Debug, Serialize, Deserialize)]
pub struct EpochBlockHeader {
    pub shard_id: u32,
    pub verifier_epoch: u64,
    pub txflow_epoch: u64,
    pub prev_header_hash: CryptoHash,

    pub states_merkle_root: MerkleHash,
    pub new_transactions_merkle_root: MerkleHash,
    pub cancelled_transactions_merkle_root: MerkleHash,
}

#[derive(Hash, Debug)]
pub struct SignedEpochBlockHeader {
    pub bls_sig: BLSSignature,
    pub epoch_block_header: EpochBlockHeader,
}

#[derive(Hash, Debug)]
pub struct FullEpochBlockBody {
    states: Vec<State>,
    new_transactions: Vec<SignedTransaction>,
    cancelled_transactions: Vec<SignedTransaction>,
}

#[derive(Hash, Debug)]
pub enum MerkleStateNode {
    Hash(MerkleHash),
    State(State),
}

#[derive(Hash, Debug)]
pub enum MerkleSignedTransactionNode {
    Hash(MerkleHash),
    SignedTransaction(SignedTransaction),
}

#[derive(Hash, Debug)]
pub struct ShardedEpochBlockBody {
    states_subtree: Vec<MerkleStateNode>,
    new_transactions_subtree: Vec<MerkleSignedTransactionNode>,
    cancelled_transactions_subtree: Vec<MerkleSignedTransactionNode>,
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

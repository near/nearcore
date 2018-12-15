use std::borrow::Borrow;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::fmt;

use ::traits::Payload;
use hash::{CryptoHash, hash, hash_struct};
use signature::{PublicKey, Signature};
use signature::DEFAULT_SIGNATURE;

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

// Transaction structs.

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct StakeTransaction {
    pub nonce: u64,
    pub staker: AccountId,
    pub amount: Balance,
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct SendMoneyTransaction {
    pub nonce: u64,
    pub sender: AccountId,
    pub receiver: AccountId,
    pub amount: Balance,
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct DeployContractTransaction {
    pub nonce: u64,
    pub sender: AccountId,
    pub contract_id: AccountId,
    pub wasm_byte_array: Vec<u8>,
    pub public_key: Vec<u8>,
}

impl fmt::Debug for DeployContractTransaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DeployContractTransaction {{ nonce: {}, sender: {}, contract_id: {}, wasm_byte_array: ... }}", self.nonce, self.sender, self.contract_id)
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct FunctionCallTransaction {
    pub nonce: u64,
    pub originator: AccountId,
    pub contract_id: AccountId,
    pub method_name: Vec<u8>,
    pub args: Vec<u8>,
    pub amount: Balance,
}

impl fmt::Debug for FunctionCallTransaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FunctionCallTransaction {{ nonce: {}, originator: {}, contract_id: {}, method_name: {:?}, args: ..., amount: {} }}", self.nonce, self.originator, self.contract_id, String::from_utf8(self.method_name.clone()), self.amount)
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct CreateAccountTransaction {
    pub nonce: u64,
    pub sender: AccountId,
    pub new_account_id: AccountId,
    pub amount: u64,
    pub public_key: Vec<u8>,
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct SwapKeyTransaction {
    pub nonce: u64,
    pub sender: AccountId,
    // current key to the account.
    // sender must sign the transaction with this key
    pub cur_key: Vec<u8>,
    pub new_key: Vec<u8>,
}

/// TODO: Call non-view function in the contracts.
#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub enum TransactionBody {
    Stake(StakeTransaction),
    SendMoney(SendMoneyTransaction),
    DeployContract(DeployContractTransaction),
    FunctionCall(FunctionCallTransaction),
    CreateAccount(CreateAccountTransaction),
    SwapKey(SwapKeyTransaction),
}

impl TransactionBody {
    pub fn get_nonce(&self) -> u64 {
        match self {
            TransactionBody::Stake(t) => t.nonce,
            TransactionBody::SendMoney(t) => t.nonce,
            TransactionBody::DeployContract(t) => t.nonce,
            TransactionBody::FunctionCall(t) => t.nonce,
            TransactionBody::CreateAccount(t) => t.nonce,
            TransactionBody::SwapKey(t) => t.nonce,
        }
    }

    pub fn get_sender(&self) -> AccountId {
        match self {
            TransactionBody::Stake(t) => t.staker,
            TransactionBody::SendMoney(t) => t.sender,
            TransactionBody::DeployContract(t) => t.sender,
            TransactionBody::FunctionCall(t) => t.originator,
            TransactionBody::CreateAccount(t) => t.sender,
            TransactionBody::SwapKey(t) => t.sender,
        }
    }
}

#[derive(Serialize, Deserialize, Eq, Debug, Clone)]
pub struct SignedTransaction {
    pub body: TransactionBody,
    pub sender_sig: StructSignature,
}

impl SignedTransaction {
    pub fn new(
        sender_sig: StructSignature,
        body: TransactionBody,
    ) -> SignedTransaction {
        SignedTransaction {
            sender_sig,
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
        SignedTransaction { sender_sig: DEFAULT_SIGNATURE, body }
    }

    pub fn transaction_hash(&self) -> CryptoHash {
        // TODO(#227): Fill in hash when deserializing.
        hash_struct(&self.body)
    }
}

impl Hash for SignedTransaction {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(hash_struct(&self.body).as_ref());
    }
}

impl PartialEq for SignedTransaction {
    fn eq(&self, other: &SignedTransaction) -> bool {
        self.body == other.body && self.sender_sig == other.sender_sig
    }
}

#[derive(Hash, Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum ReceiptBody {
    NewCall(AsyncCall),
    Callback(CallbackResult),
    Refund(u64),
}

#[derive(Hash, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AsyncCall {
    pub amount: Balance,
    pub mana: Mana,
    pub method_name: Vec<u8>,
    pub args: Vec<u8>,
    pub callback: Option<CallbackInfo>,
}

impl AsyncCall {
    pub fn new(method_name: Vec<u8>, args: Vec<u8>, amount: Balance, mana: Mana) -> Self {
        AsyncCall {
            amount,
            mana,
            method_name,
            args,
            callback: None,
        }
    }
}

impl fmt::Debug for AsyncCall {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncCall {{ amount: {}, mana: {}, method_name: {:?}, args: ..., callback: {:?} }}", self.amount, self.mana, String::from_utf8(self.method_name.clone()), self.callback)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Callback {
    pub method_name: Vec<u8>,
    pub args: Vec<u8>,
    pub results: Vec<Option<Vec<u8>>>,
    pub mana: Mana,
    pub callback: Option<CallbackInfo>,
    pub result_counter: usize,
}

impl Callback {
    pub fn new(method_name: Vec<u8>, args: Vec<u8>, mana: Mana) -> Self {
        Callback {
            method_name,
            args,
            results: vec![],
            mana,
            callback: None,
            result_counter: 0,
        }
    }
}

impl fmt::Debug for Callback {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Callback {{ method_name: {:?}, args: ..., results: ..., mana: {}, callback: {:?}, result_counter: {} }}", String::from_utf8(self.method_name.clone()), self.mana, self.callback, self.result_counter)
    }
}

#[derive(Hash, Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct CallbackInfo {
    // callback id
    pub id: CallbackId,
    // index to write to
    pub result_index: usize,
    // receiver
    pub receiver: AccountId,
}

impl CallbackInfo {
    pub fn new(id: CallbackId, result_index: usize, receiver: AccountId) -> Self {
        CallbackInfo { id, result_index, receiver }
    }
}

#[derive(Hash, Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct CallbackResult {
    // callback id
    pub info: CallbackInfo,
    // callback result
    pub result: Option<Vec<u8>>,
}

impl CallbackResult {
    pub fn new(info: CallbackInfo, result: Option<Vec<u8>>) -> Self {
        CallbackResult { info, result }
    }
}

#[derive(Hash, Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
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
        body: ReceiptBody,
    ) -> Self {
        ReceiptTransaction {
            sender,
            receiver,
            nonce,
            body,
        }
    }
}

#[derive(Hash, Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub enum Transaction {
    SignedTransaction(SignedTransaction),
    Receipt(ReceiptTransaction),
}

// TxFlow-specific structs.

pub type TxFlowHash = u64;

// DAG-specific structs.

/// Endorsement of a representative message. Includes the epoch of the message that it endorses as
/// well as the BLS signature part. The leader should also include such self-endorsement upon
/// creation of the representative message.
#[derive(Hash, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Endorsement {
    pub epoch: u64,
    pub signature: MultiSignature,
}

#[derive(Hash, Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct BeaconChainPayload {
    pub body: Vec<SignedTransaction>,
}

impl Payload for BeaconChainPayload {
    fn verify(&self) -> Result<(), &'static str> {
        Ok(())
    }

    fn union_update(&mut self, mut other: Self) {
        self.body.extend(other.body.drain(..));
    }

    fn is_empty(&self) -> bool {
        self.body.is_empty()
    }

    fn new() -> Self {
        Self {
            body: vec![]
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Hash, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConsensusBlockHeader {
    pub body_hash: CryptoHash,
    pub prev_block_body_hash: CryptoHash,
}

#[derive(Hash, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConsensusBlockBody<P> {
    /// TxFlow messages that constitute that consensus block together with the endorsements.
    pub messages: Vec<SignedMessageData<P>>,
}

// Gossip-specific structs.

#[derive(Hash, Debug, Serialize, Deserialize, PartialEq, Eq)]
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
#[derive(Hash, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Gossip<P> {
    pub sender_uid: UID,
    pub receiver_uid: UID,
    pub sender_sig: StructSignature,
    pub body: GossipBody<P>,
}

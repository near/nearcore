use crate::types::AccountId;
use crate::{
    action::GlobalContractIdentifier,
    hash::{CryptoHash, YieldId},
};
use borsh::{BorshDeserialize, BorshSerialize};
#[cfg(test)]
use near_crypto::PublicKey;
use near_crypto::PublicKeyHandle;
use near_primitives_core::trie_key::access_key_key_len;
use near_primitives_core::types::{NonceIndex, ShardId};
use near_schema_checker_lib::ProtocolSchema;
use std::mem::size_of;

pub const ACCOUNT_DATA_SEPARATOR: u8 = b',';
// The use of `ACCESS_KEY` as a separator is a historical artefact.
// Changing it would require a very long DB migration for basically no benefits.
pub const ACCESS_KEY_SEPARATOR: u8 = col::ACCESS_KEY;

/// Type identifiers used for DB key generation to store values in the key-value storage.
pub mod col {
    /// This column id is used when storing `primitives::account::Account` type about a given
    /// `account_id`.
    pub const ACCOUNT: u8 = 0;
    /// This column id is used when storing contract blob for a given `account_id`.
    pub const CONTRACT_CODE: u8 = 1;
    /// This column id is used when storing `primitives::account::AccessKey` type for a given
    /// `account_id`.
    pub const ACCESS_KEY: u8 = 2;
    /// This column id is used when storing `primitives::receipt::ReceivedData` type (data received
    /// for a key `data_id`). The required postponed receipt might be still not received or requires
    /// more pending input data.
    pub const RECEIVED_DATA: u8 = 3;
    /// This column id is used when storing `primitives::hash::CryptoHash` (ReceiptId) type. The
    /// ReceivedData is not available and is needed for the postponed receipt to execute.
    pub const POSTPONED_RECEIPT_ID: u8 = 4;
    /// This column id is used when storing the number of missing data inputs that are still not
    /// available for a key `receipt_id`.
    pub const PENDING_DATA_COUNT: u8 = 5;
    /// This column id is used when storing the postponed receipts (`primitives::receipt::Receipt`).
    pub const POSTPONED_RECEIPT: u8 = 6;
    /// This column id is used when storing:
    /// * the indices of the delayed receipts queue (a singleton per shard)
    /// * the delayed receipts themselves
    /// The identifier is shared between two different key types for historical reasons. It
    /// is valid because the length of `TrieKey::DelayedReceipt` is always greater than
    /// `TrieKey::DelayedReceiptIndices` when serialized to bytes.
    pub const DELAYED_RECEIPT_OR_INDICES: u8 = 7;
    /// This column id is used when storing Key-Value data from a contract on an `account_id`.
    pub const CONTRACT_DATA: u8 = 9;
    /// This column id is used when storing the indices of the PromiseYield timeout queue
    pub const PROMISE_YIELD_INDICES: u8 = 10;
    /// This column id is used when storing the PromiseYield timeouts
    pub const PROMISE_YIELD_TIMEOUT: u8 = 11;
    /// This column id is used when storing the postponed PromiseYield receipts
    /// (`primitives::receipt::Receipt`).
    pub const PROMISE_YIELD_RECEIPT: u8 = 12;
    /// Indices of outgoing receipts. A singleton per shard.
    /// (`primitives::receipt::BufferedReceiptIndices`)
    pub const BUFFERED_RECEIPT_INDICES: u8 = 13;
    /// Outgoing receipts that need to be buffered due to congestion +
    /// backpressure on the receiving shard.
    /// (`primitives::receipt::Receipt`).
    pub const BUFFERED_RECEIPT: u8 = 14;
    pub const BANDWIDTH_SCHEDULER_STATE: u8 = 15;
    /// Stores `ReceiptGroupsQueueData` for the receipt groups queue
    /// which corresponds to the buffered receipts to `receiver_shard`.
    pub const BUFFERED_RECEIPT_GROUPS_QUEUE_DATA: u8 = 16;
    /// A single item of `ReceiptGroupsQueue`. Values are of type `ReceiptGroup`.
    pub const BUFFERED_RECEIPT_GROUPS_QUEUE_ITEM: u8 = 17;
    /// Global contract code instance. Values are contract blobs,
    /// the same as for `CONTRACT_CODE`.
    pub const GLOBAL_CONTRACT_CODE: u8 = 18;
    /// Global contract deployment nonce. Values are u64.
    pub const GLOBAL_CONTRACT_NONCE: u8 = 19;
    /// Status of a yielded receipt. Values are of type `PromiseYieldStatus`.
    pub const PROMISE_YIELD_STATUS: u8 = 20;
    // Reserved: byte 21 is the `TrieKey::GasKeyNonce` enum discriminant.
    // GasKeyNonce rows live on disk under `ACCESS_KEY` (extending the access-key
    // trie key with a `NonceIndex` suffix), so no `col` constant exists for byte
    // 21. Do not introduce one without coordinating with the `TrieKey` repr.
    /// Mapping from user-provided yield ID to runtime data ID.
    pub const YIELD_ID_TO_DATA_ID: u8 = 22;
    /// Reverse mapping from runtime data ID to user-provided yield ID.
    pub const DATA_ID_TO_YIELD_ID: u8 = 23;

    /// All columns except those used for the delayed receipts queue, the yielded promises
    /// queue, and the outgoing receipts buffer, which are global state for the shard.
    pub const COLUMNS_WITH_ACCOUNT_ID_IN_KEY: [(u8, &str); 12] = [
        (ACCOUNT, "Account"),
        (CONTRACT_CODE, "ContractCode"),
        (ACCESS_KEY, "AccessKey"),
        (RECEIVED_DATA, "ReceivedData"),
        (POSTPONED_RECEIPT_ID, "PostponedReceiptId"),
        (PENDING_DATA_COUNT, "PendingDataCount"),
        (POSTPONED_RECEIPT, "PostponedReceipt"),
        (CONTRACT_DATA, "ContractData"),
        (PROMISE_YIELD_RECEIPT, "PromiseYieldReceipt"),
        (PROMISE_YIELD_STATUS, "PromiseYieldStatus"),
        (YIELD_ID_TO_DATA_ID, "YieldIdToDataId"),
        (DATA_ID_TO_YIELD_ID, "DataIdToYieldId"),
    ];

    pub const ALL_COLUMNS_WITH_NAMES: [(u8, &'static str); 22] = [
        (ACCOUNT, "Account"),
        (CONTRACT_CODE, "ContractCode"),
        (ACCESS_KEY, "AccessKey"),
        (RECEIVED_DATA, "ReceivedData"),
        (POSTPONED_RECEIPT_ID, "PostponedReceiptId"),
        (PENDING_DATA_COUNT, "PendingDataCount"),
        (POSTPONED_RECEIPT, "PostponedReceipt"),
        (DELAYED_RECEIPT_OR_INDICES, "DelayedReceiptOrIndices"),
        (CONTRACT_DATA, "ContractData"),
        (PROMISE_YIELD_INDICES, "PromiseYieldIndices"),
        (PROMISE_YIELD_TIMEOUT, "PromiseYieldTimeout"),
        (PROMISE_YIELD_RECEIPT, "PromiseYieldReceipt"),
        (BUFFERED_RECEIPT_INDICES, "BufferedReceiptIndices"),
        (BUFFERED_RECEIPT, "BufferedReceipt"),
        (BANDWIDTH_SCHEDULER_STATE, "BandwidthSchedulerState"),
        (BUFFERED_RECEIPT_GROUPS_QUEUE_DATA, "BufferedReceiptGroupsQueueData"),
        (BUFFERED_RECEIPT_GROUPS_QUEUE_ITEM, "BufferedReceiptGroupsQueueItem"),
        (GLOBAL_CONTRACT_CODE, "GlobalContractCode"),
        (GLOBAL_CONTRACT_NONCE, "GlobalContractNonce"),
        (PROMISE_YIELD_STATUS, "PromiseYieldStatus"),
        (YIELD_ID_TO_DATA_ID, "YieldIdToDataId"),
        (DATA_ID_TO_YIELD_ID, "DataIdToYieldId"),
    ];
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, BorshDeserialize, BorshSerialize, ProtocolSchema,
)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum GlobalContractCodeIdentifier {
    CodeHash(CryptoHash) = 0,
    AccountId(AccountId) = 1,
}

impl GlobalContractCodeIdentifier {
    pub fn len(&self) -> usize {
        1 + match self {
            Self::CodeHash(hash) => hash.as_bytes().len(),
            Self::AccountId(account_id) => {
                // Corresponds to String repr in borsh spec
                size_of::<u32>() + account_id.len()
            }
        }
    }

    pub fn append_into(&self, buf: &mut impl trie_key_buffer::TrieKeyBuffer) {
        borsh::to_writer(buf.borsh_writer(), self).unwrap()
    }
}

impl From<GlobalContractIdentifier> for GlobalContractCodeIdentifier {
    fn from(identifier: GlobalContractIdentifier) -> Self {
        match identifier {
            GlobalContractIdentifier::CodeHash(hash) => {
                GlobalContractCodeIdentifier::CodeHash(hash)
            }
            GlobalContractIdentifier::AccountId(account_id) => {
                GlobalContractCodeIdentifier::AccountId(account_id)
            }
        }
    }
}

/// Describes the key of a specific key-value record in a state trie.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, BorshDeserialize, BorshSerialize, ProtocolSchema,
)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum TrieKey {
    /// Used to store `primitives::account::Account` struct for a given `AccountId`.
    Account {
        account_id: AccountId,
    } = col::ACCOUNT,
    /// Used to store `Vec<u8>` contract code for a given `AccountId`.
    ContractCode {
        account_id: AccountId,
    } = col::CONTRACT_CODE,
    /// Used to store `primitives::account::AccessKey` struct for a given `AccountId` and
    /// a given key handle (the on-trie identifier of the access key - for
    /// ed25519/secp256k1 this is the full public key; for ML-DSA-65 it is
    /// a SHA3-256 hash of the public key).
    AccessKey {
        account_id: AccountId,
        key_handle: PublicKeyHandle,
    } = col::ACCESS_KEY,
    /// Used to store `primitives::receipt::ReceivedData` struct for a given receiver's `AccountId`
    /// of `DataReceipt` and a given `data_id` (the unique identifier for the data).
    /// NOTE: This is one of the input data for some action receipt.
    /// The action receipt might be still not be received or requires more pending input data.
    ReceivedData {
        receiver_id: AccountId,
        data_id: CryptoHash,
    } = col::RECEIVED_DATA,
    /// Used to store receipt ID `primitives::hash::CryptoHash` for a given receiver's `AccountId`
    /// of the receipt and a given `data_id` (the unique identifier for the required input data).
    /// NOTE: This receipt ID indicates the postponed receipt. We store `receipt_id` for performance
    /// purposes to avoid deserializing the entire receipt.
    PostponedReceiptId {
        receiver_id: AccountId,
        data_id: CryptoHash,
    } = col::POSTPONED_RECEIPT_ID,
    /// Used to store the number of still missing input data `u32` for a given receiver's
    /// `AccountId` and a given `receipt_id` of the receipt.
    PendingDataCount {
        receiver_id: AccountId,
        receipt_id: CryptoHash,
    } = col::PENDING_DATA_COUNT,
    /// Used to store the postponed receipt `primitives::receipt::Receipt` for a given receiver's
    /// `AccountId` and a given `receipt_id` of the receipt.
    PostponedReceipt {
        receiver_id: AccountId,
        receipt_id: CryptoHash,
    } = col::POSTPONED_RECEIPT,
    /// Used to store indices of the delayed receipts queue (`node-runtime::DelayedReceiptIndices`).
    /// NOTE: It is a singleton per shard.
    DelayedReceiptIndices = col::DELAYED_RECEIPT_OR_INDICES,
    /// Used to store a delayed receipt `primitives::receipt::Receipt` for a given index `u64`
    /// in a delayed receipt queue. The queue is unique per shard.
    DelayedReceipt {
        index: u64,
    } = 8,
    /// Used to store a key-value record `Vec<u8>` within a contract deployed on a given `AccountId`
    /// and a given key.
    ContractData {
        account_id: AccountId,
        key: Vec<u8>,
    } = col::CONTRACT_DATA,
    /// Used to store head and tail indices of the PromiseYield timeout queue.
    /// NOTE: It is a singleton per shard.
    PromiseYieldIndices = col::PROMISE_YIELD_INDICES,
    /// Used to store the element at given index `u64` in the PromiseYield timeout queue.
    /// The queue is unique per shard.
    PromiseYieldTimeout {
        index: u64,
    } = col::PROMISE_YIELD_TIMEOUT,
    /// Used to store the postponed promise yield receipt `primitives::receipt::Receipt`
    /// for a given receiver's `AccountId` and a given `data_id`.
    PromiseYieldReceipt {
        receiver_id: AccountId,
        data_id: CryptoHash,
    } = col::PROMISE_YIELD_RECEIPT,
    /// Used to store indices of the buffered receipts queues per shard.
    /// NOTE: It is a singleton per shard, holding indices for all outgoing shards.
    BufferedReceiptIndices = col::BUFFERED_RECEIPT_INDICES,
    /// Used to store a buffered receipt `primitives::receipt::Receipt` for a
    /// given index `u64` and receiving shard. There is one unique queue
    /// per ordered shard pair. The trie for shard X stores all queues for pairs
    /// (X,*) without (X,X).
    BufferedReceipt {
        receiving_shard: ShardId,
        index: u64,
    } = col::BUFFERED_RECEIPT,
    BandwidthSchedulerState = col::BANDWIDTH_SCHEDULER_STATE,
    /// Stores `ReceiptGroupsQueueData` for the receipt groups queue
    /// which corresponds to the buffered receipts to `receiver_shard`.
    BufferedReceiptGroupsQueueData {
        receiving_shard: ShardId,
    } = col::BUFFERED_RECEIPT_GROUPS_QUEUE_DATA,
    /// A single item of `ReceiptGroupsQueue`. Values are of type `ReceiptGroup`.
    BufferedReceiptGroupsQueueItem {
        receiving_shard: ShardId,
        index: u64,
    } = col::BUFFERED_RECEIPT_GROUPS_QUEUE_ITEM,
    GlobalContractCode {
        identifier: GlobalContractCodeIdentifier,
    } = col::GLOBAL_CONTRACT_CODE,
    /// Global contract deployment nonce. Stores the nonce of the last
    /// deployment for nonce-based idempotency during distribution.
    GlobalContractNonce {
        identifier: GlobalContractCodeIdentifier,
    } = col::GLOBAL_CONTRACT_NONCE,
    PromiseYieldStatus {
        receiver_id: AccountId,
        data_id: CryptoHash,
    } = col::PROMISE_YIELD_STATUS,
    /// Mapping from user-provided yield ID to runtime-generated data ID.
    /// Used by `promise_yield_create_with_id` for duplicate detection.
    YieldIdToDataId {
        receiver_id: AccountId,
        yield_id: YieldId,
    } = col::YIELD_ID_TO_DATA_ID,
    /// Reverse mapping from runtime-generated data ID to user-provided yield ID.
    /// Used to clean up `YieldIdToDataId` when a yield is resumed or times out.
    DataIdToYieldId {
        receiver_id: AccountId,
        data_id: CryptoHash,
    } = col::DATA_ID_TO_YIELD_ID,
    /// Represents a single nonce for a gas key. Stored under `col::ACCESS_KEY`
    /// with a special key format: If an access key is used as a gas key, the
    /// keys used to store its nonces extend the access key trie key with a
    /// `NonceIndex` suffix.
    GasKeyNonce {
        account_id: AccountId,
        key_handle: PublicKeyHandle,
        index: NonceIndex,
    } = 21,
}

/// Provides `len` function.
///
/// This trait exists purely so that we can do `col::ACCOUNT.len()` rather than
/// using naked `1` when we calculate lengths and refer to lengths of slices.
/// See [`TrieKey::len`] for an example.
trait Byte {
    fn len(self) -> usize;
}

impl Byte for u8 {
    fn len(self) -> usize {
        1
    }
}

/// Convenience common alias for storage of encoded `TrieKey`s in `SmallVec`s.
pub type SmallKeyVec = smallvec::SmallVec<[u8; 64]>;

/// Returns the length of the trie key for a gas key nonce.
pub fn gas_key_nonce_key_len(account_id: &AccountId, key_handle: &PublicKeyHandle) -> usize {
    access_key_key_len(account_id.len(), key_handle.trie_id_len()) + size_of::<NonceIndex>()
}

/// Append the on-trie identifier of `key_handle` into the given buffer.
/// The on-trie bytes are exactly `PublicKeyHandle`'s borsh encoding, so we
/// delegate to `BorshSerialize` rather than duplicating the layout here.
/// For ed25519 / secp256k1 the identifier is the full `PublicKey`; for
/// ML-DSA-65 it is `[tag=3] || sha3_256(domain || raw_pubkey)` - the
/// full ML-DSA-65 pubkey never enters the trie.
fn append_key_handle_trie_id(
    buf: &mut impl trie_key_buffer::TrieKeyBuffer,
    key_handle: &PublicKeyHandle,
) {
    borsh::to_writer(buf.borsh_writer(), key_handle).unwrap()
}

impl TrieKey {
    /// Constructor for [`TrieKey::AccessKey`] that accepts anything
    /// convertible into [`PublicKeyHandle`] (notably a `PublicKey` or `&PublicKey`).
    /// Encapsulates the pubkey → trie-storage-handle conversion so call
    /// sites don't have to remember the `.into()`.
    pub fn access_key(account_id: AccountId, key_handle: impl Into<PublicKeyHandle>) -> Self {
        Self::AccessKey { account_id, key_handle: key_handle.into() }
    }

    /// Constructor for [`TrieKey::GasKeyNonce`] that accepts anything
    /// convertible into [`PublicKeyHandle`].
    pub fn gas_key_nonce(
        account_id: AccountId,
        key_handle: impl Into<PublicKeyHandle>,
        index: NonceIndex,
    ) -> Self {
        Self::GasKeyNonce { account_id, key_handle: key_handle.into(), index }
    }

    pub fn len(&self) -> usize {
        match self {
            TrieKey::Account { account_id } => col::ACCOUNT.len() + account_id.len(),
            TrieKey::ContractCode { account_id } => col::CONTRACT_CODE.len() + account_id.len(),
            TrieKey::AccessKey { account_id, key_handle } => {
                access_key_key_len(account_id.len(), key_handle.trie_id_len())
            }
            TrieKey::ReceivedData { receiver_id, data_id } => {
                col::RECEIVED_DATA.len()
                    + receiver_id.len()
                    + ACCOUNT_DATA_SEPARATOR.len()
                    + data_id.as_ref().len()
            }
            TrieKey::PostponedReceiptId { receiver_id, data_id } => {
                col::POSTPONED_RECEIPT_ID.len()
                    + receiver_id.len()
                    + ACCOUNT_DATA_SEPARATOR.len()
                    + data_id.as_ref().len()
            }
            TrieKey::PendingDataCount { receiver_id, receipt_id } => {
                col::PENDING_DATA_COUNT.len()
                    + receiver_id.len()
                    + ACCOUNT_DATA_SEPARATOR.len()
                    + receipt_id.as_ref().len()
            }
            TrieKey::PostponedReceipt { receiver_id, receipt_id } => {
                col::POSTPONED_RECEIPT.len()
                    + receiver_id.len()
                    + ACCOUNT_DATA_SEPARATOR.len()
                    + receipt_id.as_ref().len()
            }
            TrieKey::DelayedReceiptIndices => col::DELAYED_RECEIPT_OR_INDICES.len(),
            TrieKey::DelayedReceipt { .. } => {
                col::DELAYED_RECEIPT_OR_INDICES.len() + size_of::<u64>()
            }
            TrieKey::PromiseYieldIndices => col::PROMISE_YIELD_INDICES.len(),
            TrieKey::PromiseYieldTimeout { .. } => {
                col::PROMISE_YIELD_TIMEOUT.len() + size_of::<u64>()
            }
            TrieKey::PromiseYieldReceipt { receiver_id, data_id } => {
                col::PROMISE_YIELD_RECEIPT.len()
                    + receiver_id.len()
                    + ACCOUNT_DATA_SEPARATOR.len()
                    + data_id.as_ref().len()
            }
            TrieKey::ContractData { account_id, key } => {
                col::CONTRACT_DATA.len()
                    + account_id.len()
                    + ACCOUNT_DATA_SEPARATOR.len()
                    + key.len()
            }
            TrieKey::BufferedReceiptIndices => col::BUFFERED_RECEIPT_INDICES.len(),
            TrieKey::BufferedReceipt { index, .. } => {
                col::BUFFERED_RECEIPT.len()
                    + std::mem::size_of::<u16>()
                    + std::mem::size_of_val(index)
            }
            TrieKey::BandwidthSchedulerState => col::BANDWIDTH_SCHEDULER_STATE.len(),
            TrieKey::BufferedReceiptGroupsQueueData { .. } => {
                col::BUFFERED_RECEIPT_GROUPS_QUEUE_DATA.len() + std::mem::size_of::<u64>()
            }
            TrieKey::BufferedReceiptGroupsQueueItem { index, .. } => {
                col::BUFFERED_RECEIPT_GROUPS_QUEUE_ITEM.len()
                    + std::mem::size_of::<u64>()
                    + std::mem::size_of_val(index)
            }
            TrieKey::GlobalContractCode { identifier } => {
                col::GLOBAL_CONTRACT_CODE.len() + identifier.len()
            }
            TrieKey::GasKeyNonce { account_id, key_handle, index: _index } => {
                gas_key_nonce_key_len(account_id, key_handle)
            }
            TrieKey::GlobalContractNonce { identifier } => {
                col::GLOBAL_CONTRACT_NONCE.len() + identifier.len()
            }
            TrieKey::PromiseYieldStatus { receiver_id, data_id } => {
                col::PROMISE_YIELD_STATUS.len()
                    + receiver_id.len()
                    + ACCOUNT_DATA_SEPARATOR.len()
                    + data_id.as_ref().len()
            }
            TrieKey::YieldIdToDataId { receiver_id, yield_id } => {
                col::YIELD_ID_TO_DATA_ID.len()
                    + receiver_id.len()
                    + ACCOUNT_DATA_SEPARATOR.len()
                    + yield_id.as_ref().len()
            }
            TrieKey::DataIdToYieldId { receiver_id, data_id } => {
                col::DATA_ID_TO_YIELD_ID.len()
                    + receiver_id.len()
                    + ACCOUNT_DATA_SEPARATOR.len()
                    + data_id.as_ref().len()
            }
        }
    }

    pub fn append_into(&self, buf: &mut impl trie_key_buffer::TrieKeyBuffer) {
        let expected_len = self.len();
        let start_len = buf.len();
        buf.reserve(self.len());
        match self {
            TrieKey::Account { account_id } => {
                buf.push(col::ACCOUNT);
                buf.extend(account_id.as_bytes());
            }
            TrieKey::ContractCode { account_id } => {
                buf.push(col::CONTRACT_CODE);
                buf.extend(account_id.as_bytes());
            }
            TrieKey::AccessKey { account_id, key_handle } => {
                buf.push(col::ACCESS_KEY);
                buf.extend(account_id.as_bytes());
                buf.push(ACCESS_KEY_SEPARATOR);
                append_key_handle_trie_id(buf, key_handle);
            }
            TrieKey::ReceivedData { receiver_id, data_id } => {
                buf.push(col::RECEIVED_DATA);
                buf.extend(receiver_id.as_bytes());
                buf.push(ACCOUNT_DATA_SEPARATOR);
                buf.extend(data_id.as_ref());
            }
            TrieKey::PostponedReceiptId { receiver_id, data_id } => {
                buf.push(col::POSTPONED_RECEIPT_ID);
                buf.extend(receiver_id.as_bytes());
                buf.push(ACCOUNT_DATA_SEPARATOR);
                buf.extend(data_id.as_ref());
            }
            TrieKey::PendingDataCount { receiver_id, receipt_id } => {
                buf.push(col::PENDING_DATA_COUNT);
                buf.extend(receiver_id.as_bytes());
                buf.push(ACCOUNT_DATA_SEPARATOR);
                buf.extend(receipt_id.as_ref());
            }
            TrieKey::PostponedReceipt { receiver_id, receipt_id } => {
                buf.push(col::POSTPONED_RECEIPT);
                buf.extend(receiver_id.as_bytes());
                buf.push(ACCOUNT_DATA_SEPARATOR);
                buf.extend(receipt_id.as_ref());
            }
            TrieKey::DelayedReceiptIndices => {
                buf.push(col::DELAYED_RECEIPT_OR_INDICES);
            }
            TrieKey::DelayedReceipt { index } => {
                buf.push(col::DELAYED_RECEIPT_OR_INDICES);
                buf.extend(&index.to_le_bytes());
            }
            TrieKey::ContractData { account_id, key } => {
                buf.push(col::CONTRACT_DATA);
                buf.extend(account_id.as_bytes());
                buf.push(ACCOUNT_DATA_SEPARATOR);
                buf.extend(key);
            }
            TrieKey::PromiseYieldIndices => {
                buf.push(col::PROMISE_YIELD_INDICES);
            }
            TrieKey::PromiseYieldTimeout { index } => {
                buf.push(col::PROMISE_YIELD_TIMEOUT);
                buf.extend(&index.to_le_bytes());
            }
            TrieKey::PromiseYieldReceipt { receiver_id, data_id } => {
                buf.push(col::PROMISE_YIELD_RECEIPT);
                buf.extend(receiver_id.as_bytes());
                buf.push(ACCOUNT_DATA_SEPARATOR);
                buf.extend(data_id.as_ref());
            }
            TrieKey::BufferedReceiptIndices => buf.push(col::BUFFERED_RECEIPT_INDICES),
            TrieKey::BufferedReceipt { index, receiving_shard } => {
                let receiving_shard = *receiving_shard;
                buf.push(col::BUFFERED_RECEIPT);
                // Use  u16 for shard id to reduce depth in trie.
                let receiving_shard: u64 = receiving_shard.into();
                assert!(receiving_shard <= u16::MAX as u64, "Shard ID too big.");
                let receiving_shard: u16 = receiving_shard as u16;
                buf.extend(&receiving_shard.to_le_bytes());
                buf.extend(&index.to_le_bytes());
            }
            TrieKey::BandwidthSchedulerState => buf.push(col::BANDWIDTH_SCHEDULER_STATE),
            TrieKey::BufferedReceiptGroupsQueueData { receiving_shard } => {
                buf.push(col::BUFFERED_RECEIPT_GROUPS_QUEUE_DATA);
                buf.extend(&receiving_shard.to_le_bytes());
            }
            TrieKey::BufferedReceiptGroupsQueueItem { receiving_shard, index } => {
                buf.push(col::BUFFERED_RECEIPT_GROUPS_QUEUE_ITEM);
                buf.extend(&receiving_shard.to_le_bytes());
                buf.extend(&index.to_le_bytes());
            }
            TrieKey::GlobalContractCode { identifier } => {
                buf.push(col::GLOBAL_CONTRACT_CODE);
                identifier.append_into(buf);
            }
            TrieKey::GasKeyNonce { account_id, key_handle, index: nonce_index } => {
                buf.push(col::ACCESS_KEY);
                buf.extend(account_id.as_bytes());
                buf.push(ACCESS_KEY_SEPARATOR);
                append_key_handle_trie_id(buf, key_handle);
                buf.extend(&nonce_index.to_le_bytes());
            }
            TrieKey::GlobalContractNonce { identifier } => {
                buf.push(col::GLOBAL_CONTRACT_NONCE);
                identifier.append_into(buf);
            }
            TrieKey::PromiseYieldStatus { receiver_id, data_id } => {
                buf.push(col::PROMISE_YIELD_STATUS);
                buf.extend(receiver_id.as_bytes());
                buf.push(ACCOUNT_DATA_SEPARATOR);
                buf.extend(data_id.as_ref());
            }
            TrieKey::YieldIdToDataId { receiver_id, yield_id } => {
                buf.push(col::YIELD_ID_TO_DATA_ID);
                buf.extend(receiver_id.as_bytes());
                buf.push(ACCOUNT_DATA_SEPARATOR);
                buf.extend(yield_id.as_ref());
            }
            TrieKey::DataIdToYieldId { receiver_id, data_id } => {
                buf.push(col::DATA_ID_TO_YIELD_ID);
                buf.extend(receiver_id.as_bytes());
                buf.push(ACCOUNT_DATA_SEPARATOR);
                buf.extend(data_id.as_ref());
            }
        };
        debug_assert_eq!(expected_len, buf.len() - start_len);
    }

    pub fn to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.len());
        self.append_into(&mut buf);
        buf
    }

    /// Extracts account id from a TrieKey if available.
    pub fn get_account_id(&self) -> Option<AccountId> {
        match self {
            TrieKey::Account { account_id, .. } => Some(account_id.clone()),
            TrieKey::ContractCode { account_id, .. } => Some(account_id.clone()),
            TrieKey::AccessKey { account_id, .. } => Some(account_id.clone()),
            TrieKey::GasKeyNonce { account_id, .. } => Some(account_id.clone()),
            TrieKey::ReceivedData { receiver_id, .. } => Some(receiver_id.clone()),
            TrieKey::PostponedReceiptId { receiver_id, .. } => Some(receiver_id.clone()),
            TrieKey::PendingDataCount { receiver_id, .. } => Some(receiver_id.clone()),
            TrieKey::PostponedReceipt { receiver_id, .. } => Some(receiver_id.clone()),
            TrieKey::DelayedReceiptIndices => None,
            TrieKey::DelayedReceipt { .. } => None,
            TrieKey::ContractData { account_id, .. } => Some(account_id.clone()),
            TrieKey::PromiseYieldIndices => None,
            TrieKey::PromiseYieldTimeout { .. } => None,
            TrieKey::PromiseYieldReceipt { receiver_id, .. } => Some(receiver_id.clone()),
            TrieKey::BufferedReceiptIndices => None,
            TrieKey::BufferedReceipt { .. } => None,
            TrieKey::BandwidthSchedulerState => None,
            TrieKey::BufferedReceiptGroupsQueueData { .. } => None,
            TrieKey::BufferedReceiptGroupsQueueItem { .. } => None,
            // Even though global contract code might be deployed under account id, it doesn't
            // correspond to the data stored for that account id, so always returning None here.
            TrieKey::GlobalContractCode { .. } => None,
            TrieKey::GlobalContractNonce { .. } => None,
            TrieKey::PromiseYieldStatus { receiver_id, .. } => Some(receiver_id.clone()),
            TrieKey::YieldIdToDataId { receiver_id, .. } => Some(receiver_id.clone()),
            TrieKey::DataIdToYieldId { receiver_id, .. } => Some(receiver_id.clone()),
        }
    }
}

mod trie_key_buffer {
    /// Buffers into which [`TrieKey`s](super::TrieKey) can be encoded.
    pub trait TrieKeyBuffer {
        fn len(&self) -> usize;
        fn reserve(&mut self, additional: usize);
        fn push(&mut self, byte: u8);
        fn extend(&mut self, bytes: &[u8]);

        type BorshWriter<'a>: borsh::io::Write
        where
            Self: 'a;
        fn borsh_writer(&mut self) -> Self::BorshWriter<'_>;
    }

    impl TrieKeyBuffer for Vec<u8> {
        fn len(&self) -> usize {
            Self::len(self)
        }
        fn reserve(&mut self, additional: usize) {
            Self::reserve(self, additional)
        }
        fn push(&mut self, byte: u8) {
            Self::push(self, byte)
        }
        fn extend(&mut self, bytes: &[u8]) {
            Self::extend_from_slice(self, bytes)
        }
        type BorshWriter<'a> = &'a mut Self;
        fn borsh_writer(&mut self) -> Self::BorshWriter<'_> {
            self
        }
    }

    impl<A: smallvec::Array<Item = u8>> TrieKeyBuffer for smallvec::SmallVec<A> {
        fn len(&self) -> usize {
            Self::len(self)
        }
        fn reserve(&mut self, additional: usize) {
            Self::reserve(self, additional)
        }
        fn push(&mut self, byte: u8) {
            Self::push(self, byte)
        }
        fn extend(&mut self, bytes: &[u8]) {
            Self::extend_from_slice(self, bytes)
        }
        type BorshWriter<'a>
            = &'a mut Self
        where
            A: 'a;
        fn borsh_writer(&mut self) -> Self::BorshWriter<'_> {
            self
        }
    }
}

// TODO: Remove once we switch to non-raw keys everywhere.
pub mod trie_key_parsers {
    use super::*;

    /// Parse the on-trie identifier of an access-key entry out of the raw
    /// key bytes. Returns the same `PublicKeyHandle` shape that `append_into`
    /// wrote: `PublicKeyHandle::ED25519(..)` / `PublicKeyHandle::SECP256K1(..)` for
    /// classical entries, and `PublicKeyHandle::MlDsa65(..)` for ML-DSA-65
    /// entries.
    pub fn parse_key_handle_from_access_key_key(
        raw_key: &[u8],
        account_id: &AccountId,
    ) -> Result<PublicKeyHandle, std::io::Error> {
        let prefix_len = col::ACCESS_KEY.len() * 2 + account_id.len();
        if raw_key.len() < prefix_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key is too short for TrieKey::AccessKey",
            ));
        }
        // `PublicKeyHandle`'s borsh tag layout (0 / 1 / 3) matches the on-trie
        // encoding produced by `append_key_handle_trie_id`, so we can
        // delegate to its borsh deserializer.
        let mut buf = &raw_key[prefix_len..];
        PublicKeyHandle::deserialize(&mut buf)
    }

    /// Parses the nonce index from a gas key raw key. Note that each nonce gas key
    /// extends the corresponding access key trie key with a `NonceIndex` suffix.
    pub fn parse_nonce_index_from_gas_key_key(
        raw_key: &[u8],
        account_id: &AccountId,
        key_handle: &PublicKeyHandle,
    ) -> Result<Option<NonceIndex>, std::io::Error> {
        let prefix_len = access_key_key_len(account_id.len(), key_handle.trie_id_len());
        if raw_key.len() < prefix_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key is too short for TrieKey::GasKeyNonce",
            ));
        } else if raw_key.len() == prefix_len {
            return Ok(None);
        }
        NonceIndex::try_from_slice(&raw_key[prefix_len..]).map(Some)
    }

    pub fn parse_data_key_from_contract_data_key<'a>(
        raw_key: &'a [u8],
        account_id: &AccountId,
    ) -> Result<&'a [u8], std::io::Error> {
        let prefix_len = col::CONTRACT_DATA.len() + account_id.len() + ACCOUNT_DATA_SEPARATOR.len();
        if raw_key.len() < prefix_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key is too short for TrieKey::ContractData",
            ));
        }
        Ok(&raw_key[prefix_len..])
    }

    pub fn parse_account_id_prefix<'a>(
        column: u8,
        raw_key: &'a [u8],
    ) -> Result<&'a [u8], std::io::Error> {
        let prefix = std::slice::from_ref(&column);
        if let Some(tail) = raw_key.strip_prefix(prefix) {
            Ok(tail)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key is does not start with a proper column marker",
            ))
        }
    }

    fn parse_account_id_from_slice(
        data: &[u8],
        trie_key: &str,
    ) -> Result<AccountId, std::io::Error> {
        std::str::from_utf8(data)
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "raw key AccountId has invalid UTF-8 format to be TrieKey::{}",
                        trie_key
                    ),
                )
            })?
            .parse()
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("raw key does not have a valid AccountId to be TrieKey::{}", trie_key),
                )
            })
    }

    /// Returns next `separator`-terminated token in `data`.
    ///
    /// In other words, returns slice of `data` from its start up to but
    /// excluding first occurrence of `separator`.  Returns `None` if `data`
    /// does not contain `separator`.
    fn next_token(data: &[u8], separator: u8) -> Option<&[u8]> {
        data.iter().position(|&byte| byte == separator).map(|idx| &data[..idx])
    }

    pub fn parse_account_id_from_contract_data_key(
        raw_key: &[u8],
    ) -> Result<AccountId, std::io::Error> {
        let account_id_prefix = parse_account_id_prefix(col::CONTRACT_DATA, raw_key)?;
        if let Some(account_id) = next_token(account_id_prefix, ACCOUNT_DATA_SEPARATOR) {
            parse_account_id_from_slice(account_id, "ContractData")
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key does not have ACCOUNT_DATA_SEPARATOR to be TrieKey::ContractData",
            ))
        }
    }

    pub fn parse_account_id_from_account_key(raw_key: &[u8]) -> Result<AccountId, std::io::Error> {
        let account_id = parse_account_id_prefix(col::ACCOUNT, raw_key)?;
        parse_account_id_from_slice(account_id, "Account")
    }

    pub fn parse_account_id_from_access_key_key(
        raw_key: &[u8],
    ) -> Result<AccountId, std::io::Error> {
        let account_id_prefix = parse_account_id_prefix(col::ACCESS_KEY, raw_key)?;
        if let Some(account_id) = next_token(account_id_prefix, ACCESS_KEY_SEPARATOR) {
            parse_account_id_from_slice(account_id, "AccessKey")
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key does not have public key to be TrieKey::AccessKey",
            ))
        }
    }

    pub fn parse_index_from_delayed_receipt_key(raw_key: &[u8]) -> Result<u64, std::io::Error> {
        // The length of TrieKey::DelayedReceipt { .. } should be 9 since it's a single byte for the
        // column and then 8 bytes for a u64 index.
        if raw_key.len() != 9 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unexpected raw key len of {} for delayed receipt index", raw_key.len()),
            ));
        }
        let index = raw_key[1..9].try_into().unwrap();
        Ok(u64::from_le_bytes(index))
    }

    pub fn parse_account_id_from_contract_code_key(
        raw_key: &[u8],
    ) -> Result<AccountId, std::io::Error> {
        let account_id = parse_account_id_prefix(col::CONTRACT_CODE, raw_key)?;
        parse_account_id_from_slice(account_id, "ContractCode")
    }

    pub fn parse_account_id_from_raw_key(
        raw_key: &[u8],
    ) -> Result<Option<AccountId>, std::io::Error> {
        for (col, col_name) in col::COLUMNS_WITH_ACCOUNT_ID_IN_KEY {
            if parse_account_id_prefix(col, raw_key).is_err() {
                continue;
            }
            let account_id = match col {
                col::ACCOUNT => parse_account_id_from_account_key(raw_key)?,
                col::CONTRACT_CODE => parse_account_id_from_contract_code_key(raw_key)?,
                col::ACCESS_KEY => parse_account_id_from_access_key_key(raw_key)?,
                _ => parse_account_id_from_trie_key_with_separator(col, raw_key, col_name)?,
            };
            return Ok(Some(account_id));
        }
        Ok(None)
    }

    pub fn parse_account_id_from_trie_key_with_separator(
        col: u8,
        raw_key: &[u8],
        col_name: &str,
    ) -> Result<AccountId, std::io::Error> {
        let account_id_prefix = parse_account_id_prefix(col, raw_key)?;
        if let Some(account_id) = next_token(account_id_prefix, ACCOUNT_DATA_SEPARATOR) {
            parse_account_id_from_slice(account_id, col_name)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("raw key does not have ACCOUNT_DATA_SEPARATOR to be TrieKey::{}", col_name),
            ))
        }
    }

    pub fn parse_account_id_from_received_data_key(
        raw_key: &[u8],
    ) -> Result<AccountId, std::io::Error> {
        parse_account_id_from_trie_key_with_separator(col::RECEIVED_DATA, raw_key, "ReceivedData")
    }

    pub fn parse_data_id_from_received_data_key(
        raw_key: &[u8],
        account_id: &AccountId,
    ) -> Result<CryptoHash, std::io::Error> {
        let prefix_len = col::ACCESS_KEY.len() * 2 + account_id.len();
        if raw_key.len() < prefix_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key is too short for TrieKey::ReceivedData",
            ));
        }
        CryptoHash::try_from(&raw_key[prefix_len..]).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Can't parse CryptoHash for TrieKey::ReceivedData",
            )
        })
    }

    pub fn get_raw_prefix_for_access_keys(account_id: &AccountId) -> Vec<u8> {
        let mut res = Vec::with_capacity(col::ACCESS_KEY.len() * 2 + account_id.len());
        res.push(col::ACCESS_KEY);
        res.extend(account_id.as_bytes());
        res.push(col::ACCESS_KEY);
        res
    }

    pub fn get_raw_prefix_for_contract_data(account_id: &AccountId, prefix: &[u8]) -> Vec<u8> {
        let mut res = Vec::with_capacity(
            col::CONTRACT_DATA.len()
                + account_id.len()
                + ACCOUNT_DATA_SEPARATOR.len()
                + prefix.len(),
        );
        res.push(col::CONTRACT_DATA);
        res.extend(account_id.as_bytes());
        res.push(ACCOUNT_DATA_SEPARATOR);
        res.extend(prefix);
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_crypto::KeyType;

    // cspell:ignore cheapaccounts lols skidanov
    const OK_ACCOUNT_IDS: &[&str] = &[
        "aa",
        "a-a",
        "a-aa",
        "100",
        "0o",
        "com",
        "near",
        "bowen",
        "b-o_w_e-n",
        "b.owen",
        "bro.wen",
        "a.ha",
        "a.b-a.ra",
        "system",
        "over.9000",
        "google.com",
        "illia.cheapaccounts.near",
        "0o0ooo00oo00o",
        "alex-skidanov",
        "10-4.8-2",
        "b-o_w_e-n",
        "no_lols",
        "0123456789012345678901234567890123456789012345678901234567890123",
        // Valid, but can't be created
        "near.a",
    ];

    #[test]
    fn test_key_for_account_consistency() {
        for account_id in OK_ACCOUNT_IDS.iter().map(|x| x.parse::<AccountId>().unwrap()) {
            let key = TrieKey::Account { account_id: account_id.clone() };
            let raw_key = key.to_vec();
            assert_eq!(raw_key.len(), key.len());
            assert_eq!(
                trie_key_parsers::parse_account_id_from_account_key(&raw_key).unwrap(),
                account_id
            );
            assert_eq!(
                trie_key_parsers::parse_account_id_from_raw_key(&raw_key).unwrap().unwrap(),
                account_id
            );
        }
    }

    #[test]
    fn test_key_for_access_key_consistency() {
        let public_key = PublicKey::empty(KeyType::ED25519);
        let key_handle: PublicKeyHandle = (&public_key).into();
        for account_id in OK_ACCOUNT_IDS.iter().map(|x| x.parse::<AccountId>().unwrap()) {
            let key = TrieKey::access_key(account_id.clone(), key_handle.clone());
            let raw_key = key.to_vec();
            assert_eq!(raw_key.len(), key.len());
            assert_eq!(
                trie_key_parsers::parse_account_id_from_access_key_key(&raw_key).unwrap(),
                account_id
            );
            assert_eq!(
                trie_key_parsers::parse_key_handle_from_access_key_key(&raw_key, &account_id)
                    .unwrap(),
                key_handle
            );
            assert_eq!(
                trie_key_parsers::parse_account_id_from_raw_key(&raw_key).unwrap().unwrap(),
                account_id
            );
        }
    }

    /// Encoding an `AccessKey` trie key with a full `MLDSA65` pubkey must
    /// write the SHA3-256 hash form, not the 1953-byte raw bytes. Parsing
    /// the resulting raw key returns `PublicKeyHandle::MlDsa65` carrying the
    /// matching hash.
    #[cfg(feature = "rand")]
    #[test]
    fn test_key_for_access_key_ml_dsa_65_hashes() {
        use near_crypto::{ML_DSA_65_HASH_LENGTH, SecretKey};
        let sk = SecretKey::from_seed(KeyType::MLDSA65, "trie-roundtrip");
        let full_pk = sk.public_key();
        let expected_hash = match &full_pk {
            PublicKey::MLDSA65(k) => k.to_public_key_handle(),
            _ => unreachable!(),
        };

        let account_id: AccountId = "alice.near".parse().unwrap();
        let key = TrieKey::access_key(account_id.clone(), &full_pk);
        let raw_key = key.to_vec();

        // Length matches the *hash* form, not the full-pubkey form.
        assert_eq!(raw_key.len(), key.len());
        assert_eq!(
            raw_key.len(),
            col::ACCESS_KEY.len() * 2 + account_id.len() + 1 + ML_DSA_65_HASH_LENGTH,
        );

        // Tag byte after the prefix is 3 (hashed ML-DSA-65).
        let prefix_len = col::ACCESS_KEY.len() * 2 + account_id.len();
        assert_eq!(raw_key[prefix_len], 3u8);

        // Bytes after the tag match the expected hash.
        assert_eq!(&raw_key[prefix_len + 1..], expected_hash.as_ref());

        // Parsing yields `PublicKeyHandle::MlDsa65`, NOT a full pubkey.
        let parsed =
            trie_key_parsers::parse_key_handle_from_access_key_key(&raw_key, &account_id).unwrap();
        assert_eq!(parsed, PublicKeyHandle::MlDsa65(expected_hash));
    }

    #[test]
    fn test_key_for_data_consistency() {
        let data_key = b"0123456789" as &[u8];
        for account_id in OK_ACCOUNT_IDS.iter().map(|x| x.parse::<AccountId>().unwrap()) {
            let key =
                TrieKey::ContractData { account_id: account_id.clone(), key: data_key.to_vec() };
            let raw_key = key.to_vec();
            assert_eq!(raw_key.len(), key.len());
            assert_eq!(
                trie_key_parsers::parse_account_id_from_contract_data_key(&raw_key).unwrap(),
                account_id
            );
            assert_eq!(
                trie_key_parsers::parse_data_key_from_contract_data_key(&raw_key, &account_id)
                    .unwrap(),
                data_key
            );
            assert_eq!(
                trie_key_parsers::parse_account_id_from_raw_key(&raw_key).unwrap().unwrap(),
                account_id
            );
        }
    }

    #[test]
    fn test_key_for_code_consistency() {
        for account_id in OK_ACCOUNT_IDS.iter().map(|x| x.parse::<AccountId>().unwrap()) {
            let key = TrieKey::ContractCode { account_id: account_id.clone() };
            let raw_key = key.to_vec();
            assert_eq!(raw_key.len(), key.len());
            assert_eq!(
                trie_key_parsers::parse_account_id_from_contract_code_key(&raw_key).unwrap(),
                account_id
            );
            assert_eq!(
                trie_key_parsers::parse_account_id_from_raw_key(&raw_key).unwrap().unwrap(),
                account_id
            );
        }
    }

    #[test]
    fn test_key_for_received_data_consistency() {
        for account_id in OK_ACCOUNT_IDS.iter().map(|x| x.parse::<AccountId>().unwrap()) {
            let key = TrieKey::ReceivedData {
                receiver_id: account_id.clone(),
                data_id: CryptoHash::default(),
            };
            let raw_key = key.to_vec();
            assert_eq!(raw_key.len(), key.len());
            assert_eq!(
                trie_key_parsers::parse_account_id_from_received_data_key(&raw_key).unwrap(),
                account_id
            );
            assert_eq!(
                trie_key_parsers::parse_account_id_from_raw_key(&raw_key).unwrap().unwrap(),
                account_id
            );
            assert_eq!(
                trie_key_parsers::parse_data_id_from_received_data_key(&raw_key, &account_id)
                    .unwrap(),
                CryptoHash::default(),
            );
        }
    }

    #[test]
    fn test_key_for_postponed_receipt_consistency() {
        for account_id in OK_ACCOUNT_IDS.iter().map(|x| x.parse::<AccountId>().unwrap()) {
            let key = TrieKey::PostponedReceipt {
                receiver_id: account_id.clone(),
                receipt_id: CryptoHash::default(),
            };
            let raw_key = key.to_vec();
            assert_eq!(raw_key.len(), key.len());
            assert_eq!(
                trie_key_parsers::parse_account_id_from_raw_key(&raw_key).unwrap().unwrap(),
                account_id
            );
        }
    }

    #[test]
    fn test_key_for_postponed_receipt_id_consistency() {
        for account_id in OK_ACCOUNT_IDS.iter().map(|x| x.parse::<AccountId>().unwrap()) {
            let key = TrieKey::PostponedReceiptId {
                receiver_id: account_id.clone(),
                data_id: CryptoHash::default(),
            };
            let raw_key = key.to_vec();
            assert_eq!(raw_key.len(), key.len());
            assert_eq!(
                trie_key_parsers::parse_account_id_from_raw_key(&raw_key).unwrap().unwrap(),
                account_id
            );
        }
    }

    #[test]
    fn test_key_for_pending_data_count_consistency() {
        for account_id in OK_ACCOUNT_IDS.iter().map(|x| x.parse::<AccountId>().unwrap()) {
            let key = TrieKey::PendingDataCount {
                receiver_id: account_id.clone(),
                receipt_id: CryptoHash::default(),
            };
            let raw_key = key.to_vec();
            assert_eq!(raw_key.len(), key.len());
            assert_eq!(
                trie_key_parsers::parse_account_id_from_raw_key(&raw_key).unwrap().unwrap(),
                account_id
            );
        }
    }

    #[test]
    fn test_key_for_delayed_receipts_consistency() {
        let key = TrieKey::DelayedReceiptIndices;
        let raw_key = key.to_vec();
        assert!(trie_key_parsers::parse_account_id_from_raw_key(&raw_key).unwrap().is_none());
        let key = TrieKey::DelayedReceipt { index: 123 };
        let raw_key = key.to_vec();
        assert!(trie_key_parsers::parse_account_id_from_raw_key(&raw_key).unwrap().is_none());
        assert_eq!(trie_key_parsers::parse_index_from_delayed_receipt_key(&raw_key).unwrap(), 123);
    }

    #[test]
    fn test_key_for_promise_yield_consistency() {
        let key = TrieKey::PromiseYieldIndices;
        let raw_key = key.to_vec();
        assert!(trie_key_parsers::parse_account_id_from_raw_key(&raw_key).unwrap().is_none());
        let key = TrieKey::PromiseYieldTimeout { index: 0 };
        let raw_key = key.to_vec();
        assert!(trie_key_parsers::parse_account_id_from_raw_key(&raw_key).unwrap().is_none());
        for account_id in OK_ACCOUNT_IDS.iter().map(|x| x.parse::<AccountId>().unwrap()) {
            let key = TrieKey::PromiseYieldReceipt {
                receiver_id: account_id.clone(),
                data_id: CryptoHash::default(),
            };
            let raw_key = key.to_vec();
            assert_eq!(raw_key.len(), key.len());
            assert_eq!(
                trie_key_parsers::parse_account_id_from_raw_key(&raw_key).unwrap().unwrap(),
                account_id
            );
        }
    }

    #[test]
    fn test_account_id_from_trie_key() {
        for account_id_str in OK_ACCOUNT_IDS {
            let account_id = account_id_str.parse::<AccountId>().unwrap();

            assert_eq!(
                TrieKey::Account { account_id: account_id.clone() }.get_account_id(),
                Some(account_id.clone())
            );
            assert_eq!(
                TrieKey::ContractCode { account_id: account_id.clone() }.get_account_id(),
                Some(account_id.clone())
            );
            assert_eq!(
                TrieKey::access_key(account_id.clone(), &PublicKey::empty(KeyType::ED25519))
                    .get_account_id(),
                Some(account_id.clone())
            );
            assert_eq!(
                TrieKey::ReceivedData {
                    receiver_id: account_id.clone(),
                    data_id: Default::default()
                }
                .get_account_id(),
                Some(account_id.clone())
            );
            assert_eq!(
                TrieKey::PostponedReceiptId {
                    receiver_id: account_id.clone(),
                    data_id: Default::default()
                }
                .get_account_id(),
                Some(account_id.clone())
            );
            assert_eq!(
                TrieKey::PendingDataCount {
                    receiver_id: account_id.clone(),
                    receipt_id: Default::default()
                }
                .get_account_id(),
                Some(account_id.clone())
            );
            assert_eq!(
                TrieKey::PostponedReceipt {
                    receiver_id: account_id.clone(),
                    receipt_id: Default::default()
                }
                .get_account_id(),
                Some(account_id.clone())
            );
            assert_eq!(
                TrieKey::DelayedReceipt { index: Default::default() }.get_account_id(),
                None
            );
            assert_eq!(TrieKey::DelayedReceiptIndices.get_account_id(), None);
            assert_eq!(
                TrieKey::PromiseYieldTimeout { index: Default::default() }.get_account_id(),
                None
            );
            assert_eq!(TrieKey::PromiseYieldIndices.get_account_id(), None);
            assert_eq!(
                TrieKey::PromiseYieldReceipt {
                    receiver_id: account_id.clone(),
                    data_id: CryptoHash::new(),
                }
                .get_account_id(),
                Some(account_id.clone())
            );
            assert_eq!(
                TrieKey::ContractData { account_id: account_id.clone(), key: Default::default() }
                    .get_account_id(),
                Some(account_id)
            );
        }
    }

    #[test]
    fn test_key_for_gas_key_nonce_consistency() {
        let public_key = PublicKey::empty(KeyType::ED25519);
        let key_handle: PublicKeyHandle = (&public_key).into();
        let nonce_index: NonceIndex = 2; // Arbitrary nonce index for testing.
        for account_id in OK_ACCOUNT_IDS.iter().map(|x| x.parse::<AccountId>().unwrap()) {
            let access_key = TrieKey::access_key(account_id.clone(), key_handle.clone());
            let gas_key_nonce =
                TrieKey::gas_key_nonce(account_id.clone(), key_handle.clone(), nonce_index);
            let raw_key = gas_key_nonce.to_vec();
            assert_eq!(raw_key.len(), gas_key_nonce.len());

            // Gas key nonce raw key extends access key raw key with a NonceIndex suffix.
            let access_key_raw = access_key.to_vec();
            assert!(raw_key.starts_with(&access_key_raw));
            assert_eq!(raw_key.len(), access_key_raw.len() + size_of::<NonceIndex>());

            // Parsing the account id from a gas key nonce raw key should work.
            assert_eq!(
                trie_key_parsers::parse_account_id_from_access_key_key(&raw_key).unwrap(),
                account_id
            );

            // Parsing the key handle from a gas key nonce raw key should work.
            // This is important: the raw key has extra bytes (the nonce index)
            // after the key handle.
            assert_eq!(
                trie_key_parsers::parse_key_handle_from_access_key_key(&raw_key, &account_id)
                    .unwrap(),
                key_handle
            );

            // Parsing the nonce index from a gas key nonce raw key should work.
            assert_eq!(
                trie_key_parsers::parse_nonce_index_from_gas_key_key(
                    &raw_key,
                    &account_id,
                    &key_handle
                )
                .unwrap(),
                Some(nonce_index)
            );

            // Parsing nonce index from an access key raw key should return None.
            assert_eq!(
                trie_key_parsers::parse_nonce_index_from_gas_key_key(
                    &access_key_raw,
                    &account_id,
                    &key_handle
                )
                .unwrap(),
                None
            );

            // GasKeyNonce should return the account id.
            assert_eq!(gas_key_nonce.get_account_id(), Some(account_id.clone()));
        }
    }

    /// Verifies that `near_primitives_core::trie_key::access_key_key_len` matches
    /// the actual serialized `TrieKey::AccessKey` length. This guards against the
    /// primitives-core function getting out of sync with the trie key format.
    #[test]
    fn test_access_key_key_len_matches_trie_key() {
        for key_type in [KeyType::ED25519, KeyType::SECP256K1] {
            let public_key = PublicKey::empty(key_type);
            let key_handle: PublicKeyHandle = (&public_key).into();
            for account_id in OK_ACCOUNT_IDS.iter().map(|x| x.parse::<AccountId>().unwrap()) {
                let key = TrieKey::access_key(account_id.clone(), key_handle.clone());
                let raw_key = key.to_vec();
                assert_eq!(
                    raw_key.len(),
                    access_key_key_len(account_id.len(), key_handle.trie_id_len()),
                    "access_key_key_len mismatch for account_id={account_id}, key_type={key_type:?}"
                );
            }
        }
    }

    #[test]
    fn test_global_contract_code_identifier_len() {
        check_global_contract_code_identifier_len(GlobalContractCodeIdentifier::CodeHash(
            CryptoHash::hash_bytes(&[42]),
        ));
        check_global_contract_code_identifier_len(GlobalContractCodeIdentifier::AccountId(
            "alice.near".parse().unwrap(),
        ));
    }

    fn check_global_contract_code_identifier_len(identifier: GlobalContractCodeIdentifier) {
        let mut buf = Vec::new();
        identifier.append_into(&mut buf);
        assert_eq!(buf.len(), identifier.len());
    }
}

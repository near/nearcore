use crate::hash::CryptoHash;
use crate::types::AccountId;
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::PublicKey;
use near_primitives_core::types::ShardId;
use std::mem::size_of;

pub(crate) const ACCOUNT_DATA_SEPARATOR: u8 = b',';
// The use of `ACCESS_KEY` as a separator is a historical artefact.
// Changing it would require a very long DB migration for basically no benefits.
pub(crate) const ACCESS_KEY_SEPARATOR: u8 = col::ACCESS_KEY;

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
    /// All columns except those used for the delayed receipts queue, the yielded promises
    /// queue, and the outgoing receipts buffer, which are global state for the shard.

    // NOTE: NEW_COLUMN = 15 will be the last unique nibble in the trie!
    // Consider demultiplexing on 15 and using 2-nibble prefixes.
    pub const COLUMNS_WITH_ACCOUNT_ID_IN_KEY: [(u8, &str); 9] = [
        (ACCOUNT, "Account"),
        (CONTRACT_CODE, "ContractCode"),
        (ACCESS_KEY, "AccessKey"),
        (RECEIVED_DATA, "ReceivedData"),
        (POSTPONED_RECEIPT_ID, "PostponedReceiptId"),
        (PENDING_DATA_COUNT, "PendingDataCount"),
        (POSTPONED_RECEIPT, "PostponedReceipt"),
        (CONTRACT_DATA, "ContractData"),
        (PROMISE_YIELD_RECEIPT, "PromiseYieldReceipt"),
    ];
}

/// Describes the key of a specific key-value record in a state trie.
#[derive(Debug, Clone, PartialEq, Eq, BorshDeserialize, BorshSerialize)]
pub enum TrieKey {
    /// Used to store `primitives::account::Account` struct for a given `AccountId`.
    Account { account_id: AccountId },
    /// Used to store `Vec<u8>` contract code for a given `AccountId`.
    ContractCode { account_id: AccountId },
    /// Used to store `primitives::account::AccessKey` struct for a given `AccountId` and
    /// a given `public_key` of the `AccessKey`.
    AccessKey { account_id: AccountId, public_key: PublicKey },
    /// Used to store `primitives::receipt::ReceivedData` struct for a given receiver's `AccountId`
    /// of `DataReceipt` and a given `data_id` (the unique identifier for the data).
    /// NOTE: This is one of the input data for some action receipt.
    /// The action receipt might be still not be received or requires more pending input data.
    ReceivedData { receiver_id: AccountId, data_id: CryptoHash },
    /// Used to store receipt ID `primitives::hash::CryptoHash` for a given receiver's `AccountId`
    /// of the receipt and a given `data_id` (the unique identifier for the required input data).
    /// NOTE: This receipt ID indicates the postponed receipt. We store `receipt_id` for performance
    /// purposes to avoid deserializing the entire receipt.
    PostponedReceiptId { receiver_id: AccountId, data_id: CryptoHash },
    /// Used to store the number of still missing input data `u32` for a given receiver's
    /// `AccountId` and a given `receipt_id` of the receipt.
    PendingDataCount { receiver_id: AccountId, receipt_id: CryptoHash },
    /// Used to store the postponed receipt `primitives::receipt::Receipt` for a given receiver's
    /// `AccountId` and a given `receipt_id` of the receipt.
    PostponedReceipt { receiver_id: AccountId, receipt_id: CryptoHash },
    /// Used to store indices of the delayed receipts queue (`node-runtime::DelayedReceiptIndices`).
    /// NOTE: It is a singleton per shard.
    DelayedReceiptIndices,
    /// Used to store a delayed receipt `primitives::receipt::Receipt` for a given index `u64`
    /// in a delayed receipt queue. The queue is unique per shard.
    DelayedReceipt { index: u64 },
    /// Used to store a key-value record `Vec<u8>` within a contract deployed on a given `AccountId`
    /// and a given key.
    ContractData { account_id: AccountId, key: Vec<u8> },
    /// Used to store head and tail indices of the PromiseYield timeout queue.
    /// NOTE: It is a singleton per shard.
    PromiseYieldIndices,
    /// Used to store the element at given index `u64` in the PromiseYield timeout queue.
    /// The queue is unique per shard.
    PromiseYieldTimeout { index: u64 },
    /// Used to store the postponed promise yield receipt `primitives::receipt::Receipt`
    /// for a given receiver's `AccountId` and a given `data_id`.
    PromiseYieldReceipt { receiver_id: AccountId, data_id: CryptoHash },
    /// Used to store indices of the buffered receipts queues per shard.
    /// NOTE: It is a singleton per shard, holding indices for all outgoing shards.
    BufferedReceiptIndices,
    /// Used to store a buffered receipt `primitives::receipt::Receipt` for a
    /// given index `u64` and receiving shard. There is one unique queue
    /// per ordered shard pair. The trie for shard X stores all queues for pairs
    /// (X,*) without (X,X).
    BufferedReceipt { receiving_shard: ShardId, index: u64 },
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

impl TrieKey {
    pub fn len(&self) -> usize {
        match self {
            TrieKey::Account { account_id } => col::ACCOUNT.len() + account_id.len(),
            TrieKey::ContractCode { account_id } => col::CONTRACT_CODE.len() + account_id.len(),
            TrieKey::AccessKey { account_id, public_key } => {
                col::ACCESS_KEY.len() * 2 + account_id.len() + public_key.len()
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
        }
    }

    pub fn append_into(&self, buf: &mut Vec<u8>) {
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
            TrieKey::AccessKey { account_id, public_key } => {
                buf.push(col::ACCESS_KEY);
                buf.extend(account_id.as_bytes());
                buf.push(ACCESS_KEY_SEPARATOR);
                buf.extend(borsh::to_vec(&public_key).unwrap());
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
                buf.push(col::BUFFERED_RECEIPT);
                // Use  u16 for shard id to reduce depth in trie.
                assert!(*receiving_shard <= u16::MAX as u64, "Shard ID too big.");
                buf.extend(&(*receiving_shard as u16).to_le_bytes());
                buf.extend(&index.to_le_bytes());
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
        }
    }
}

// TODO: Remove once we switch to non-raw keys everywhere.
pub mod trie_key_parsers {
    use super::*;

    pub fn parse_public_key_from_access_key_key(
        raw_key: &[u8],
        account_id: &AccountId,
    ) -> Result<PublicKey, std::io::Error> {
        let prefix_len = col::ACCESS_KEY.len() * 2 + account_id.len();
        if raw_key.len() < prefix_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key is too short for TrieKey::AccessKey",
            ));
        }
        PublicKey::try_from_slice(&raw_key[prefix_len..])
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

    pub fn parse_account_id_from_contract_code_key(
        raw_key: &[u8],
    ) -> Result<AccountId, std::io::Error> {
        let account_id = parse_account_id_prefix(col::CONTRACT_CODE, raw_key)?;
        parse_account_id_from_slice(account_id, "ContractCode")
    }

    pub fn parse_trie_key_access_key_from_raw_key(
        raw_key: &[u8],
    ) -> Result<TrieKey, std::io::Error> {
        let account_id = parse_account_id_from_access_key_key(raw_key)?;
        let public_key = parse_public_key_from_access_key_key(raw_key, &account_id)?;
        Ok(TrieKey::AccessKey { account_id, public_key })
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
    use crate::shard_layout::ShardLayout;
    use near_crypto::KeyType;
    use near_primitives_core::version::PROTOCOL_VERSION;

    use super::*;

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
        for account_id in OK_ACCOUNT_IDS.iter().map(|x| x.parse::<AccountId>().unwrap()) {
            let key = TrieKey::AccessKey {
                account_id: account_id.clone(),
                public_key: public_key.clone(),
            };
            let raw_key = key.to_vec();
            assert_eq!(raw_key.len(), key.len());
            assert_eq!(
                trie_key_parsers::parse_trie_key_access_key_from_raw_key(&raw_key).unwrap(),
                key
            );
            assert_eq!(
                trie_key_parsers::parse_account_id_from_access_key_key(&raw_key).unwrap(),
                account_id
            );
            assert_eq!(
                trie_key_parsers::parse_public_key_from_access_key_key(&raw_key, &account_id)
                    .unwrap(),
                public_key
            );
            assert_eq!(
                trie_key_parsers::parse_account_id_from_raw_key(&raw_key).unwrap().unwrap(),
                account_id
            );
        }
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
        let key = TrieKey::DelayedReceipt { index: 0 };
        let raw_key = key.to_vec();
        assert!(trie_key_parsers::parse_account_id_from_raw_key(&raw_key).unwrap().is_none());
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
                TrieKey::AccessKey {
                    account_id: account_id.clone(),
                    public_key: PublicKey::empty(KeyType::ED25519)
                }
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
    fn test_shard_id_u16_optimization() {
        let shard_layout = ShardLayout::for_protocol_version(PROTOCOL_VERSION);
        let max_id = shard_layout.shard_ids().max().unwrap();
        assert!(
            max_id <= u16::MAX as u64,
            "buffered receipt trie key optimization broken, must fit in a u16"
        );
    }
}

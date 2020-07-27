use crate::hash::CryptoHash;
use crate::types::AccountId;
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::PublicKey;
use std::mem::size_of;

pub(crate) const ACCOUNT_DATA_SEPARATOR: &[u8; 1] = b",";

/// Type identifiers used for DB key generation to store values in the key-value storage.
pub(crate) mod col {
    /// This column id is used when storing `primitives::account::Account` type about a given
    /// `account_id`.
    pub const ACCOUNT: &[u8] = &[0];
    /// This column id is used when storing contract blob for a given `account_id`.
    pub const CONTRACT_CODE: &[u8] = &[1];
    /// This column id is used when storing `primitives::account::AccessKey` type for a given
    /// `account_id`.
    pub const ACCESS_KEY: &[u8] = &[2];
    /// This column id is used when storing `primitives::receipt::ReceivedData` type (data received
    /// for a key `data_id`). The required postponed receipt might be still not received or requires
    /// more pending input data.
    pub const RECEIVED_DATA: &[u8] = &[3];
    /// This column id is used when storing `primitives::hash::CryptoHash` (ReceiptId) type. The
    /// ReceivedData is not available and is needed for the postponed receipt to execute.
    pub const POSTPONED_RECEIPT_ID: &[u8] = &[4];
    /// This column id is used when storing the number of missing data inputs that are still not
    /// available for a key `receipt_id`.
    pub const PENDING_DATA_COUNT: &[u8] = &[5];
    /// This column id is used when storing the postponed receipts (`primitives::receipt::Receipt`).
    pub const POSTPONED_RECEIPT: &[u8] = &[6];
    /// This column id is used when storing the indices of the delayed receipts queue.
    /// NOTE: It is a singleton per shard.
    pub const DELAYED_RECEIPT_INDICES: &[u8] = &[7];
    /// This column id is used when storing delayed receipts, because the shard is overwhelmed.
    pub const DELAYED_RECEIPT: &[u8] = &[8];
    /// This column id is used when storing Key-Value data from a contract on an `account_id`.
    pub const CONTRACT_DATA: &[u8] = &[9];
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
            TrieKey::DelayedReceiptIndices => col::DELAYED_RECEIPT_INDICES.len(),
            TrieKey::DelayedReceipt { .. } => col::DELAYED_RECEIPT.len() + size_of::<u64>(),
            TrieKey::ContractData { account_id, key } => {
                col::CONTRACT_DATA.len()
                    + account_id.len()
                    + ACCOUNT_DATA_SEPARATOR.len()
                    + key.len()
            }
        }
    }

    pub fn to_vec(&self) -> Vec<u8> {
        let expected_len = self.len();
        let mut res = Vec::with_capacity(expected_len);
        match self {
            TrieKey::Account { account_id } => {
                res.extend(col::ACCOUNT);
                res.extend(account_id.as_bytes());
            }
            TrieKey::ContractCode { account_id } => {
                res.extend(col::CONTRACT_CODE);
                res.extend(account_id.as_bytes());
            }
            TrieKey::AccessKey { account_id, public_key } => {
                res.extend(col::ACCESS_KEY);
                res.extend(account_id.as_bytes());
                res.extend(col::ACCESS_KEY);
                res.extend(public_key.try_to_vec().unwrap());
            }
            TrieKey::ReceivedData { receiver_id, data_id } => {
                res.extend(col::RECEIVED_DATA);
                res.extend(receiver_id.as_bytes());
                res.extend(ACCOUNT_DATA_SEPARATOR);
                res.extend(data_id.as_ref());
            }
            TrieKey::PostponedReceiptId { receiver_id, data_id } => {
                res.extend(col::POSTPONED_RECEIPT_ID);
                res.extend(receiver_id.as_bytes());
                res.extend(ACCOUNT_DATA_SEPARATOR);
                res.extend(data_id.as_ref());
            }
            TrieKey::PendingDataCount { receiver_id, receipt_id } => {
                res.extend(col::PENDING_DATA_COUNT);
                res.extend(receiver_id.as_bytes());
                res.extend(ACCOUNT_DATA_SEPARATOR);
                res.extend(receipt_id.as_ref());
            }
            TrieKey::PostponedReceipt { receiver_id, receipt_id } => {
                res.extend(col::POSTPONED_RECEIPT);
                res.extend(receiver_id.as_bytes());
                res.extend(ACCOUNT_DATA_SEPARATOR);
                res.extend(receipt_id.as_ref());
            }
            TrieKey::DelayedReceiptIndices => {
                res.extend(col::DELAYED_RECEIPT_INDICES);
            }
            TrieKey::DelayedReceipt { index } => {
                res.extend(col::DELAYED_RECEIPT_INDICES);
                res.extend(&index.to_le_bytes());
            }
            TrieKey::ContractData { account_id, key } => {
                res.extend(col::CONTRACT_DATA);
                res.extend(account_id.as_bytes());
                res.extend(ACCOUNT_DATA_SEPARATOR);
                res.extend(key);
            }
        };
        debug_assert_eq!(res.len(), expected_len);
        res
    }
}

// TODO: Remove once we switch to non-raw keys everywhere.
pub mod trie_key_parsers {
    use super::*;
    use std::convert::TryFrom;

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
        column: &[u8],
        raw_key: &'a [u8],
    ) -> Result<&'a [u8], std::io::Error> {
        if !raw_key.starts_with(column) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key is does not start with a proper column marker",
            ));
        }
        Ok(&raw_key[column.len()..])
    }

    pub fn parse_account_id_from_contract_data_key(
        raw_key: &[u8],
    ) -> Result<AccountId, std::io::Error> {
        let account_id_prefix = parse_account_id_prefix(col::CONTRACT_DATA, raw_key)?;
        // To simplify things, we assume that the data separator is a single byte.
        debug_assert_eq!(ACCOUNT_DATA_SEPARATOR.len(), 1);
        let account_data_separator_position = if let Some(index) = account_id_prefix
            .iter()
            .enumerate()
            .find(|(_, c)| **c == ACCOUNT_DATA_SEPARATOR[0])
            .map(|(index, _)| index)
        {
            index
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key does not have ACCOUNT_DATA_SEPARATOR to be TrieKey::ContractData",
            ));
        };
        let account_id_prefix = &account_id_prefix[..account_data_separator_position];
        Ok(AccountId::from(std::str::from_utf8(account_id_prefix).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key does not have a valid AccountId to be TrieKey::ContractData",
            )
        })?))
    }

    pub fn parse_account_id_from_account_key(raw_key: &[u8]) -> Result<AccountId, std::io::Error> {
        let account_id = parse_account_id_prefix(col::ACCOUNT, raw_key)?;
        Ok(AccountId::from(std::str::from_utf8(account_id).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key does not have a valid AccountId to be TrieKey::Account",
            )
        })?))
    }

    pub fn parse_account_id_from_access_key_key(
        raw_key: &[u8],
    ) -> Result<AccountId, std::io::Error> {
        let account_id_prefix = parse_account_id_prefix(col::ACCESS_KEY, raw_key)?;
        // To simplify things, we assume that the data separator is a single byte.
        debug_assert_eq!(col::ACCESS_KEY.len(), 1);
        let public_key_position = if let Some(index) = account_id_prefix
            .iter()
            .enumerate()
            .find(|(_, c)| **c == col::ACCESS_KEY[0])
            .map(|(index, _)| index)
        {
            index
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key does not have public key to be TrieKey::AccessKey",
            ));
        };
        let account_id = &account_id_prefix[..public_key_position];
        Ok(AccountId::from(std::str::from_utf8(account_id).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key does not have a valid AccountId to be TrieKey::AccessKey",
            )
        })?))
    }

    pub fn parse_account_id_from_contract_code_key(
        raw_key: &[u8],
    ) -> Result<AccountId, std::io::Error> {
        let account_id = parse_account_id_prefix(col::CONTRACT_CODE, raw_key)?;
        Ok(AccountId::from(std::str::from_utf8(account_id).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key does not have a valid AccountId to be TrieKey::ContractCode",
            )
        })?))
    }

    pub fn parse_trie_key_access_key_from_raw_key(
        raw_key: &[u8],
    ) -> Result<TrieKey, std::io::Error> {
        let account_id = parse_account_id_from_access_key_key(raw_key)?;
        let public_key = parse_public_key_from_access_key_key(raw_key, &account_id)?;
        Ok(TrieKey::AccessKey { account_id, public_key })
    }

    pub fn parse_account_id_from_received_data_key(
        raw_key: &[u8],
    ) -> Result<AccountId, std::io::Error> {
        let account_id_prefix = parse_account_id_prefix(col::RECEIVED_DATA, raw_key)?;
        // To simplify things, we assume that the data separator is a single byte.
        debug_assert_eq!(ACCOUNT_DATA_SEPARATOR.len(), 1);
        let account_data_separator_position = if let Some(index) = account_id_prefix
            .iter()
            .enumerate()
            .find(|(_, c)| **c == ACCOUNT_DATA_SEPARATOR[0])
            .map(|(index, _)| index)
        {
            index
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key does not have ACCOUNT_DATA_SEPARATOR to be TrieKey::ReceivedData",
            ));
        };
        let account_id_prefix = &account_id_prefix[..account_data_separator_position];
        Ok(AccountId::from(std::str::from_utf8(account_id_prefix).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key does not have a valid AccountId to be TrieKey::ReceivedData",
            )
        })?))
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
        res.extend(col::ACCESS_KEY);
        res.extend(account_id.as_bytes());
        res.extend(col::ACCESS_KEY);
        res
    }

    pub fn get_raw_prefix_for_contract_data(account_id: &AccountId, prefix: &[u8]) -> Vec<u8> {
        let mut res = Vec::with_capacity(
            col::CONTRACT_DATA.len()
                + account_id.len()
                + ACCOUNT_DATA_SEPARATOR.len()
                + prefix.len(),
        );
        res.extend(col::CONTRACT_DATA);
        res.extend(account_id.as_bytes());
        res.extend(ACCOUNT_DATA_SEPARATOR);
        res.extend(prefix);
        res
    }
}

#[cfg(test)]
mod tests {
    use near_crypto::KeyType;

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
        for account_id in OK_ACCOUNT_IDS.iter().map(|x| AccountId::from(*x)) {
            let key = TrieKey::Account { account_id: account_id.clone() };
            let raw_key = key.to_vec();
            assert_eq!(raw_key.len(), key.len());
            assert_eq!(
                trie_key_parsers::parse_account_id_from_account_key(&raw_key).unwrap(),
                account_id
            );
        }
    }

    #[test]
    fn test_key_for_access_key_consistency() {
        let public_key = PublicKey::empty(KeyType::ED25519);
        for account_id in OK_ACCOUNT_IDS.iter().map(|x| AccountId::from(*x)) {
            let key = TrieKey::AccessKey {
                account_id: account_id.clone(),
                public_key: public_key.clone(),
            };
            let raw_key = key.to_vec();
            assert_eq!(raw_key.len(), key.len());
            assert_eq!(
                trie_key_parsers::parse_account_id_from_access_key_key(&raw_key).unwrap(),
                account_id
            );
            assert_eq!(
                trie_key_parsers::parse_public_key_from_access_key_key(&raw_key, &account_id)
                    .unwrap(),
                public_key
            );
        }
    }

    #[test]
    fn test_key_for_data_consistency() {
        let data_key = b"0123456789" as &[u8];
        for account_id in OK_ACCOUNT_IDS.iter().map(|x| AccountId::from(*x)) {
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
        }
    }

    #[test]
    fn test_key_for_code_consistency() {
        for account_id in OK_ACCOUNT_IDS.iter().map(|x| AccountId::from(*x)) {
            let key = TrieKey::ContractCode { account_id: account_id.clone() };
            let raw_key = key.to_vec();
            assert_eq!(raw_key.len(), key.len());
            assert_eq!(
                trie_key_parsers::parse_account_id_from_contract_code_key(&raw_key).unwrap(),
                account_id
            );
        }
    }
}

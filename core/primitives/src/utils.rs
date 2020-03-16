use std::cmp::max;
use std::convert::AsRef;
use std::fmt;

use borsh::{BorshDeserialize, BorshSerialize};
use byteorder::{LittleEndian, WriteBytesExt};
use chrono::{DateTime, NaiveDateTime, Utc};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use regex::Regex;
use serde;

use lazy_static::lazy_static;
use near_crypto::PublicKey;

use crate::hash::{hash, CryptoHash};
use crate::types::{AccountId, NumSeats, NumShards};

pub const ACCOUNT_DATA_SEPARATOR: &[u8; 1] = b",";
pub const MIN_ACCOUNT_ID_LEN: usize = 2;
pub const MAX_ACCOUNT_ID_LEN: usize = 64;

/// Number of nano seconds in a second.
const NS_IN_SECOND: u64 = 1_000_000_000;

/// Type identifiers used for DB key generation to store values in the key-value storage.
pub mod col {
    /// This column id is used when storing `primitives::account::Account` type about a given
    /// `account_id`.
    pub const ACCOUNT: &[u8] = &[0];
    /// This column id is used when storing contract blob for a given `account_id`.
    pub const CODE: &[u8] = &[1];
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
}

#[derive(derive_more::AsRef, derive_more::Into)]
struct KeyForColumnAccountId(Vec<u8>);

impl KeyForColumnAccountId {
    pub fn estimate_len(column: &[u8], account_id: &AccountId) -> usize {
        column.len() + account_id.len()
    }

    pub fn with_capacity(column: &[u8], account_id: &AccountId, reserve_capacity: usize) -> Self {
        let mut key =
            Vec::with_capacity(Self::estimate_len(&column, &account_id) + reserve_capacity);
        key.extend(column);
        key.extend(account_id.as_bytes());
        debug_assert_eq!(key.len(), Self::estimate_len(&column, &account_id));
        Self(key)
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
}

#[derive(derive_more::AsRef, derive_more::Into)]
#[as_ref(forward)]
pub struct KeyForAccount(Vec<u8>);

impl KeyForAccount {
    pub fn estimate_len(account_id: &AccountId) -> usize {
        KeyForColumnAccountId::estimate_len(col::ACCOUNT, account_id)
    }

    pub fn with_capacity(account_id: &AccountId, reserve_capacity: usize) -> Self {
        let key = KeyForColumnAccountId::with_capacity(col::ACCOUNT, account_id, reserve_capacity);
        debug_assert_eq!(key.0.len(), Self::estimate_len(&account_id));
        Self(key.into())
    }

    pub fn new(account_id: &AccountId) -> Self {
        Self::with_capacity(&account_id, 0)
    }

    pub fn parse_account_id<K: AsRef<[u8]>>(raw_key: K) -> Result<AccountId, std::io::Error> {
        let account_id =
            KeyForColumnAccountId::parse_account_id_prefix(col::ACCOUNT, raw_key.as_ref())?;
        Ok(AccountId::from(std::str::from_utf8(account_id).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key does not have a valid AccountId to be KeyForAccount",
            )
        })?))
    }
}

#[derive(derive_more::AsRef, derive_more::Into)]
#[as_ref(forward)]
pub struct KeyForAccessKey(Vec<u8>);

impl KeyForAccessKey {
    fn estimate_prefix_len(account_id: &AccountId) -> usize {
        KeyForColumnAccountId::estimate_len(col::ACCESS_KEY, account_id) + col::ACCESS_KEY.len()
    }

    pub fn estimate_len(account_id: &AccountId, public_key: &PublicKey) -> usize {
        let serialized_public_key =
            public_key.try_to_vec().expect("Failed to serialize public key");
        Self::estimate_prefix_len(account_id) + serialized_public_key.len()
    }

    pub fn get_prefix_with_capacity(account_id: &AccountId, reserved_capacity: usize) -> Self {
        let mut key: Vec<u8> = KeyForColumnAccountId::with_capacity(
            col::ACCESS_KEY,
            account_id,
            col::ACCESS_KEY.len() + reserved_capacity,
        )
        .into();
        key.extend(col::ACCESS_KEY);
        Self(key)
    }

    pub fn get_prefix(account_id: &AccountId) -> Self {
        Self::get_prefix_with_capacity(account_id, 0)
    }

    pub fn new(account_id: &AccountId, public_key: &PublicKey) -> Self {
        let serialized_public_key =
            public_key.try_to_vec().expect("Failed to serialize public key");
        let mut key = Self::get_prefix_with_capacity(&account_id, serialized_public_key.len());
        key.0.extend(&serialized_public_key);
        debug_assert_eq!(key.0.len(), Self::estimate_len(&account_id, &public_key));
        key
    }

    pub fn parse_account_id<K: AsRef<[u8]>>(raw_key: K) -> Result<AccountId, std::io::Error> {
        let account_id_prefix =
            KeyForColumnAccountId::parse_account_id_prefix(col::ACCESS_KEY, raw_key.as_ref())?;
        let public_key_position = if let Some(index) =
            account_id_prefix.iter().enumerate().find(|(_, c)| **c == 2).map(|(index, _)| index)
        {
            index
        } else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key does not have public key to be KeyForAccessKey",
            ));
        };
        let account_id = &account_id_prefix[..public_key_position];
        Ok(AccountId::from(std::str::from_utf8(account_id).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key does not have a valid AccountId to be KeyForAccessKey",
            )
        })?))
    }

    pub fn parse_public_key(
        raw_key: &[u8],
        account_id: &AccountId,
    ) -> Result<PublicKey, std::io::Error> {
        let prefix_len = Self::estimate_prefix_len(account_id);
        if raw_key.len() < prefix_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key is too short for KeyForAccessKey",
            ));
        }
        PublicKey::try_from_slice(&raw_key[prefix_len..])
    }
}

#[derive(derive_more::AsRef, derive_more::Into)]
#[as_ref(forward)]
pub struct KeyForData(Vec<u8>);

impl KeyForData {
    pub fn estimate_len(account_id: &AccountId, data: &[u8]) -> usize {
        KeyForAccount::estimate_len(&account_id) + ACCOUNT_DATA_SEPARATOR.len() + data.len()
    }

    pub fn get_prefix_with_capacity(account_id: &AccountId, reserved_capacity: usize) -> Self {
        let mut prefix: Vec<u8> = KeyForAccount::with_capacity(
            account_id,
            ACCOUNT_DATA_SEPARATOR.len() + reserved_capacity,
        )
        .into();
        prefix.extend(ACCOUNT_DATA_SEPARATOR);
        Self(prefix)
    }

    pub fn get_prefix(account_id: &AccountId) -> Self {
        Self::get_prefix_with_capacity(account_id, 0)
    }

    pub fn new(account_id: &AccountId, data: &[u8]) -> Self {
        let mut key = Self::get_prefix_with_capacity(&account_id, data.len());
        key.0.extend(data);
        debug_assert_eq!(key.0.len(), Self::estimate_len(&account_id, &data));
        key
    }

    pub fn parse_account_id<K: AsRef<[u8]>>(raw_key: K) -> Result<AccountId, std::io::Error> {
        let account_id_prefix =
            KeyForColumnAccountId::parse_account_id_prefix(col::ACCOUNT, raw_key.as_ref())?;
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
                "raw key does not have ACCOUNT_DATA_SEPARATOR to be KeyForData",
            ));
        };
        let account_id_prefix = &account_id_prefix[..account_data_separator_position];
        Ok(AccountId::from(std::str::from_utf8(account_id_prefix).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key does not have a valid AccountId to be KeyForData",
            )
        })?))
    }

    pub fn parse_data_key<'a>(
        raw_key: &'a [u8],
        account_id: &AccountId,
    ) -> Result<&'a [u8], std::io::Error> {
        let prefix_len = Self::estimate_len(account_id, &[]);
        if raw_key.len() < prefix_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key is too short for KeyForData",
            ));
        }
        Ok(&raw_key[prefix_len..])
    }
}

#[derive(derive_more::AsRef, derive_more::Into)]
#[as_ref(forward)]
pub struct KeyForCode(Vec<u8>);

impl KeyForCode {
    pub fn new(account_id: &AccountId) -> Self {
        Self(KeyForColumnAccountId::with_capacity(col::CODE, account_id, 0).into())
    }

    pub fn parse_account_id<K: AsRef<[u8]>>(raw_key: K) -> Result<AccountId, std::io::Error> {
        let account_id =
            KeyForColumnAccountId::parse_account_id_prefix(col::CODE, raw_key.as_ref())?;
        Ok(AccountId::from(std::str::from_utf8(account_id).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raw key does not have a valid AccountId to be KeyForCode",
            )
        })?))
    }
}

#[derive(derive_more::AsRef, derive_more::Into)]
#[as_ref(forward)]
pub struct KeyForReceivedData(Vec<u8>);

impl KeyForReceivedData {
    pub fn new(account_id: &AccountId, data_id: &CryptoHash) -> Self {
        let mut key: Vec<u8> = KeyForColumnAccountId::with_capacity(
            col::RECEIVED_DATA,
            account_id,
            ACCOUNT_DATA_SEPARATOR.len() + data_id.as_ref().len(),
        )
        .into();
        key.extend(ACCOUNT_DATA_SEPARATOR);
        key.extend(data_id.as_ref());
        Self(key)
    }
}

#[derive(derive_more::AsRef, derive_more::Into)]
#[as_ref(forward)]
pub struct KeyForPostponedReceiptId(Vec<u8>);

impl KeyForPostponedReceiptId {
    pub fn new(account_id: &AccountId, data_id: &CryptoHash) -> Self {
        let mut key: Vec<u8> = KeyForColumnAccountId::with_capacity(
            col::POSTPONED_RECEIPT_ID,
            account_id,
            ACCOUNT_DATA_SEPARATOR.len() + data_id.as_ref().len(),
        )
        .into();
        key.extend(ACCOUNT_DATA_SEPARATOR);
        key.extend(data_id.as_ref());
        Self(key)
    }
}

#[derive(derive_more::AsRef, derive_more::Into)]
#[as_ref(forward)]
pub struct KeyForPendingDataCount(Vec<u8>);

impl KeyForPendingDataCount {
    pub fn new(account_id: &AccountId, receipt_id: &CryptoHash) -> Self {
        let mut key: Vec<u8> = KeyForColumnAccountId::with_capacity(
            col::PENDING_DATA_COUNT,
            account_id,
            ACCOUNT_DATA_SEPARATOR.len() + receipt_id.as_ref().len(),
        )
        .into();
        key.extend(ACCOUNT_DATA_SEPARATOR);
        key.extend(receipt_id.as_ref());
        Self(key)
    }
}

#[derive(derive_more::AsRef, derive_more::Into)]
#[as_ref(forward)]
pub struct KeyForPostponedReceipt(Vec<u8>);

impl KeyForPostponedReceipt {
    pub fn new(account_id: &AccountId, receipt_id: &CryptoHash) -> Self {
        let mut key: Vec<u8> = KeyForColumnAccountId::with_capacity(
            col::POSTPONED_RECEIPT,
            account_id,
            ACCOUNT_DATA_SEPARATOR.len() + receipt_id.as_ref().len(),
        )
        .into();
        key.extend(ACCOUNT_DATA_SEPARATOR);
        key.extend(receipt_id.as_ref());
        Self(key)
    }
}

#[derive(derive_more::AsRef, derive_more::Into)]
#[as_ref(forward)]
pub struct KeyForDelayedReceipt(Vec<u8>);

impl KeyForDelayedReceipt {
    pub fn new(index: u64) -> Self {
        let index_bytes = index.to_le_bytes();
        let mut key = Vec::with_capacity(col::DELAYED_RECEIPT.len() + index_bytes.len());
        key.extend(col::DELAYED_RECEIPT);
        key.extend(&index_bytes);
        Self(key)
    }
}

pub fn create_nonce_with_nonce(base: &CryptoHash, salt: u64) -> CryptoHash {
    let mut nonce: Vec<u8> = base.as_ref().to_owned();
    nonce.extend(index_to_bytes(salt));
    hash(&nonce)
}

pub fn index_to_bytes(index: u64) -> Vec<u8> {
    let mut bytes = vec![];
    bytes.write_u64::<LittleEndian>(index).expect("writing to bytes failed");
    bytes
}

lazy_static! {
    /// See NEP#0006
    static ref VALID_ACCOUNT_ID: Regex =
        Regex::new(r"^(([a-z\d]+[\-_])*[a-z\d]+\.)*([a-z\d]+[\-_])*[a-z\d]+$").unwrap();
    /// Represents a part of an account ID with a suffix of as a separator `.`.
    static ref VALID_ACCOUNT_PART_ID_WITH_TAIL_SEPARATOR: Regex =
        Regex::new(r"^([a-z\d]+[\-_])*[a-z\d]+\.$").unwrap();
    /// Represents a top level account ID.
    static ref VALID_TOP_LEVEL_ACCOUNT_ID: Regex =
        Regex::new(r"^([a-z\d]+[\-_])*[a-z\d]+$").unwrap();
}

/// const does not allow function call, so have to resort to this
pub fn system_account() -> AccountId {
    "system".to_string()
}

pub fn is_valid_account_id(account_id: &AccountId) -> bool {
    account_id.len() >= MIN_ACCOUNT_ID_LEN
        && account_id.len() <= MAX_ACCOUNT_ID_LEN
        && VALID_ACCOUNT_ID.is_match(account_id)
}

pub fn is_valid_top_level_account_id(account_id: &AccountId) -> bool {
    account_id.len() >= MIN_ACCOUNT_ID_LEN
        && account_id.len() <= MAX_ACCOUNT_ID_LEN
        && account_id != &system_account()
        && VALID_TOP_LEVEL_ACCOUNT_ID.is_match(account_id)
}

/// Returns true if the signer_id can create a direct sub-account with the given account Id.
/// It assumes the signer_id is a valid account_id
pub fn is_valid_sub_account_id(signer_id: &AccountId, sub_account_id: &AccountId) -> bool {
    if !is_valid_account_id(sub_account_id) {
        return false;
    }
    if signer_id.len() >= sub_account_id.len() {
        return false;
    }
    // Will not panic, since valid account id is utf-8 only and the length is checked above.
    // e.g. when `near` creates `aa.near`, it splits into `aa.` and `near`
    let (prefix, suffix) = sub_account_id.split_at(sub_account_id.len() - signer_id.len());
    if suffix != signer_id {
        return false;
    }
    VALID_ACCOUNT_PART_ID_WITH_TAIL_SEPARATOR.is_match(prefix)
}

/// A wrapper around Option<T> that provides native Display trait.
/// Simplifies propagating automatic Display trait on parent structs.
pub struct DisplayOption<T>(pub Option<T>);

impl<T: fmt::Display> fmt::Display for DisplayOption<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.0 {
            Some(ref v) => write!(f, "Some({})", v),
            None => write!(f, "None"),
        }
    }
}

impl<T> DisplayOption<T> {
    pub fn into(self) -> Option<T> {
        self.0
    }
}

impl<T> AsRef<Option<T>> for DisplayOption<T> {
    fn as_ref(&self) -> &Option<T> {
        &self.0
    }
}

impl<T: fmt::Display> From<Option<T>> for DisplayOption<T> {
    fn from(o: Option<T>) -> Self {
        DisplayOption(o)
    }
}

/// Macro to either return value if the result is Ok, or exit function logging error.
#[macro_export]
macro_rules! unwrap_or_return {
    ($obj: expr, $ret: expr) => {
        match $obj {
            Ok(value) => value,
            Err(err) => {
                error!(target: "client", "Unwrap error: {}", err);
                return $ret;
            }
        }
    };
    ($obj: expr) => {
        match $obj {
            Ok(value) => value,
            Err(err) => {
                error!(target: "client", "Unwrap error: {}", err);
                return;
            }
        }
    };
}

/// Macro to either return value if the result is Some, or exit function.
#[macro_export]
macro_rules! unwrap_option_or_return {
    ($obj: expr, $ret: expr) => {
        match $obj {
            Some(value) => value,
            None => {
                return $ret;
            }
        }
    };
    ($obj: expr) => {
        match $obj {
            Some(value) => value,
            None => {
                return;
            }
        }
    };
}

/// Converts timestamp in ns into DateTime UTC time.
pub fn from_timestamp(timestamp: u64) -> DateTime<Utc> {
    DateTime::from_utc(
        NaiveDateTime::from_timestamp(
            (timestamp / NS_IN_SECOND) as i64,
            (timestamp % NS_IN_SECOND) as u32,
        ),
        Utc,
    )
}

/// Converts DateTime UTC time into timestamp in ns.
pub fn to_timestamp(time: DateTime<Utc>) -> u64 {
    time.timestamp_nanos() as u64
}

/// Compute number of seats per shard for given total number of seats and number of shards.
pub fn get_num_seats_per_shard(num_shards: NumShards, num_seats: NumSeats) -> Vec<NumSeats> {
    (0..num_shards)
        .map(|i| {
            let remainder = num_seats % num_shards;
            let num = if i < remainder as u64 {
                num_seats / num_shards + 1
            } else {
                num_seats / num_shards
            };
            max(num, 1)
        })
        .collect()
}

/// Generate random string of given length
pub fn generate_random_string(len: usize) -> String {
    thread_rng().sample_iter(&Alphanumeric).take(len).collect::<String>()
}

pub struct Serializable<'a, T>(&'a T);

impl<'a, T> fmt::Display for Serializable<'a, T>
where
    T: serde::Serialize,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", serde_json::to_string(&self.0).unwrap())
    }
}

/// Wrap an object that implements Serialize into another object
/// that implements Display. When used display in this object
/// it shows its json representation. It is used to display complex
/// objects using tracing.
///
/// tracing::debug!(target: "diagnostic", value=%ser(&object));
pub fn ser<'a, T>(object: &'a T) -> Serializable<'a, T>
where
    T: serde::Serialize,
{
    Serializable(object)
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
            let key = KeyForAccount::new(&account_id);
            assert_eq!((key.as_ref() as &[u8]).len(), KeyForAccount::estimate_len(&account_id));
            assert_eq!(KeyForAccount::parse_account_id(&key).unwrap(), account_id);
        }
    }

    #[test]
    fn test_key_for_access_key_consistency() {
        let public_key = PublicKey::empty(KeyType::ED25519);
        for account_id in OK_ACCOUNT_IDS.iter().map(|x| AccountId::from(*x)) {
            let key_prefix = KeyForAccessKey::get_prefix(&account_id);
            assert_eq!(
                (key_prefix.as_ref() as &[u8]).len(),
                KeyForAccessKey::estimate_prefix_len(&account_id)
            );
            let key = KeyForAccessKey::new(&account_id, &public_key);
            assert_eq!(
                (key.as_ref() as &[u8]).len(),
                KeyForAccessKey::estimate_len(&account_id, &public_key)
            );
            assert_eq!(KeyForAccessKey::parse_account_id(&key).unwrap(), account_id);
            assert_eq!(
                KeyForAccessKey::parse_public_key(key.as_ref(), &account_id).unwrap(),
                public_key
            );
        }
    }

    #[test]
    fn test_key_for_data_consistency() {
        let data_key = b"0123456789" as &[u8];
        for account_id in OK_ACCOUNT_IDS.iter().map(|x| AccountId::from(*x)) {
            let key_prefix = KeyForData::get_prefix(&account_id);
            assert_eq!(
                (key_prefix.as_ref() as &[u8]).len(),
                KeyForData::estimate_len(&account_id, &[])
            );
            let key = KeyForData::new(&account_id, &data_key);
            assert_eq!(
                (key.as_ref() as &[u8]).len(),
                KeyForData::estimate_len(&account_id, &data_key)
            );
            assert_eq!(KeyForData::parse_account_id(&key).unwrap(), account_id);
            assert_eq!(KeyForData::parse_data_key(key.as_ref(), &account_id).unwrap(), data_key);
        }
    }

    #[test]
    fn test_key_for_code_consistency() {
        for account_id in OK_ACCOUNT_IDS.iter().map(|x| AccountId::from(*x)) {
            let key = KeyForCode::new(&account_id);
            assert_eq!(KeyForCode::parse_account_id(&key).unwrap(), account_id);
        }
    }

    #[test]
    fn test_is_valid_account_id() {
        for account_id in OK_ACCOUNT_IDS {
            assert!(
                is_valid_account_id(&account_id.to_string()),
                "Valid account id {:?} marked invalid",
                account_id
            );
        }

        let bad_account_ids = vec![
            "a",
            "A",
            "Abc",
            "-near",
            "near-",
            "-near-",
            "near.",
            ".near",
            "near@",
            "@near",
            "неар",
            "@@@@@",
            "0__0",
            "0_-_0",
            "0_-_0",
            "..",
            "a..near",
            "nEar",
            "_bowen",
            "hello world",
            "abcdefghijklmnopqrstuvwxyz.abcdefghijklmnopqrstuvwxyz.abcdefghijklmnopqrstuvwxyz",
            "01234567890123456789012345678901234567890123456789012345678901234",
            // `@` separators are banned now
            "some-complex-address@gmail.com",
            "sub.buy_d1gitz@atata@b0-rg.c_0_m",
        ];
        for account_id in bad_account_ids {
            assert!(
                !is_valid_account_id(&account_id.to_string()),
                "Invalid account id {:?} marked valid",
                account_id
            );
        }
    }

    #[test]
    fn test_is_valid_top_level_account_id() {
        let ok_top_level_account_ids = vec![
            "aa",
            "a-a",
            "a-aa",
            "100",
            "0o",
            "com",
            "near",
            "bowen",
            "b-o_w_e-n",
            "0o0ooo00oo00o",
            "alex-skidanov",
            "b-o_w_e-n",
            "no_lols",
            "0123456789012345678901234567890123456789012345678901234567890123",
        ];
        for account_id in ok_top_level_account_ids {
            assert!(
                is_valid_top_level_account_id(&account_id.to_string()),
                "Valid top level account id {:?} marked invalid",
                account_id
            );
        }

        let bad_top_level_account_ids = vec![
            "near.a",
            "b.owen",
            "bro.wen",
            "a.ha",
            "a.b-a.ra",
            "some-complex-address@gmail.com",
            "sub.buy_d1gitz@atata@b0-rg.c_0_m",
            "over.9000",
            "google.com",
            "illia.cheapaccounts.near",
            "10-4.8-2",
            "a",
            "A",
            "Abc",
            "-near",
            "near-",
            "-near-",
            "near.",
            ".near",
            "near@",
            "@near",
            "неар",
            "@@@@@",
            "0__0",
            "0_-_0",
            "0_-_0",
            "..",
            "a..near",
            "nEar",
            "_bowen",
            "hello world",
            "abcdefghijklmnopqrstuvwxyz.abcdefghijklmnopqrstuvwxyz.abcdefghijklmnopqrstuvwxyz",
            "01234567890123456789012345678901234567890123456789012345678901234",
            // Valid regex and length, but reserved
            "system",
        ];
        for account_id in bad_top_level_account_ids {
            assert!(
                !is_valid_top_level_account_id(&account_id.to_string()),
                "Invalid top level account id {:?} marked valid",
                account_id
            );
        }
    }

    #[test]
    fn test_is_valid_sub_account_id() {
        let ok_pairs = vec![
            ("test", "a.test"),
            ("test-me", "abc.test-me"),
            ("gmail.com", "abc.gmail.com"),
            ("gmail.com", "abc-lol.gmail.com"),
            ("gmail.com", "abc_lol.gmail.com"),
            ("gmail.com", "bro-abc_lol.gmail.com"),
            ("g0", "0g.g0"),
            ("1g", "1g.1g"),
            ("5-3", "4_2.5-3"),
        ];
        for (signer_id, sub_account_id) in ok_pairs {
            assert!(
                is_valid_sub_account_id(&signer_id.to_string(), &sub_account_id.to_string()),
                "Failed to create sub-account {:?} by account {:?}",
                sub_account_id,
                signer_id
            );
        }

        let bad_pairs = vec![
            ("test", ".test"),
            ("test", "test"),
            ("test", "est"),
            ("test", ""),
            ("test", "st"),
            ("test5", "ббб"),
            ("test", "a-test"),
            ("test", "etest"),
            ("test", "a.etest"),
            ("test", "retest"),
            ("test-me", "abc-.test-me"),
            ("test-me", "Abc.test-me"),
            ("test-me", "-abc.test-me"),
            ("test-me", "a--c.test-me"),
            ("test-me", "a_-c.test-me"),
            ("test-me", "a-_c.test-me"),
            ("test-me", "_abc.test-me"),
            ("test-me", "abc_.test-me"),
            ("test-me", "..test-me"),
            ("test-me", "a..test-me"),
            ("gmail.com", "a.abc@gmail.com"),
            ("gmail.com", ".abc@gmail.com"),
            ("gmail.com", ".abc@gmail@com"),
            ("gmail.com", "abc@gmail@com"),
            ("test", "a@test"),
            ("test_me", "abc@test_me"),
            ("gmail.com", "abc@gmail.com"),
            ("gmail@com", "abc.gmail@com"),
            ("gmail.com", "abc-lol@gmail.com"),
            ("gmail@com", "abc_lol.gmail@com"),
            ("gmail@com", "bro-abc_lol.gmail@com"),
            ("gmail.com", "123456789012345678901234567890123456789012345678901234567890@gmail.com"),
            (
                "123456789012345678901234567890123456789012345678901234567890",
                "1234567890.123456789012345678901234567890123456789012345678901234567890",
            ),
            ("aa", "ъ@aa"),
            ("aa", "ъ.aa"),
        ];
        for (signer_id, sub_account_id) in bad_pairs {
            assert!(
                !is_valid_sub_account_id(&signer_id.to_string(), &sub_account_id.to_string()),
                "Invalid sub-account {:?} created by account {:?}",
                sub_account_id,
                signer_id
            );
        }
    }

    #[test]
    fn test_num_chunk_producers() {
        for num_seats in 1..50 {
            for num_shards in 1..50 {
                let assignment = get_num_seats_per_shard(num_shards, num_seats);
                assert_eq!(assignment.iter().sum::<u64>(), max(num_seats, num_shards));
            }
        }
    }
}

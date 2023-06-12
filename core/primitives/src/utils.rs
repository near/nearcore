use std::cmp::max;
use std::convert::AsRef;
use std::fmt;

use chrono;
use chrono::{DateTime, NaiveDateTime};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde;

use crate::hash::{hash, CryptoHash};
use crate::receipt::Receipt;
use crate::transaction::SignedTransaction;
use crate::types::{NumSeats, NumShards, ShardId};
use crate::version::{
    ProtocolVersion, CORRECT_RANDOM_VALUE_PROTOCOL_VERSION, CREATE_HASH_PROTOCOL_VERSION,
    CREATE_RECEIPT_ID_SWITCH_TO_CURRENT_BLOCK_VERSION,
};
use std::mem::size_of;
use std::ops::Deref;

pub mod min_heap;

/// Number of nano seconds in a second.
const NS_IN_SECOND: u64 = 1_000_000_000;

/// A data structure for tagging data as already being validated to prevent
/// redundant work.
///
/// # Example
///
/// ```ignore
/// struct Foo;
/// struct Error;
///
/// /// Performs expensive validation of `foo`.
/// fn validate_foo(foo: &Foo) -> Result<bool, Error>;
///
/// fn do_stuff(foo: Foo) {
///     let foo = MaybeValidated::from(foo);
///     do_stuff_with_foo(&foo);
///     if foo.validate_with(validate_foo) {
///         println!("^_^");
///     }
/// }
///
/// fn do_stuff_with_foo(foo: &MaybeValidated<Foo) {
///     // …
///     if maybe_do_something && foo.validate_with(validate_foo) {
///         println!("@_@");
///     }
///     // …
/// }
/// ```
#[derive(Clone)]
pub struct MaybeValidated<T> {
    validated: std::cell::Cell<bool>,
    payload: T,
}

impl<T> MaybeValidated<T> {
    /// Creates new MaybeValidated object marking payload as validated.  No
    /// verification is performed; it’s caller’s responsibility to make sure the
    /// payload has indeed been validated.
    ///
    /// # Example
    ///
    /// ```
    /// use near_primitives::utils::MaybeValidated;
    ///
    /// let value = MaybeValidated::from_validated(42);
    /// assert!(value.is_validated());
    /// assert_eq!(Ok(true), value.validate_with::<(), _>(|_| panic!()));
    /// ```
    pub fn from_validated(payload: T) -> Self {
        Self { validated: std::cell::Cell::new(true), payload }
    }

    /// Validates payload with given `validator` function and returns result of
    /// the validation.  If payload has already been validated returns
    /// `Ok(true)`.  Note that this method changes the internal validated flag
    /// so it’s probably incorrect to call it with different `validator`
    /// functions.
    ///
    /// # Example
    ///
    /// ```
    /// use near_primitives::utils::MaybeValidated;
    ///
    /// let value = MaybeValidated::from(42);
    /// assert_eq!(Err(()), value.validate_with(|_| Err(())));
    /// assert_eq!(Ok(false), value.validate_with::<(), _>(|v| Ok(*v == 24)));
    /// assert!(!value.is_validated());
    /// assert_eq!(Ok(true), value.validate_with::<(), _>(|v| Ok(*v == 42)));
    /// assert!(value.is_validated());
    /// assert_eq!(Ok(true), value.validate_with::<(), _>(|_| panic!()));
    /// ```
    pub fn validate_with<E, F: FnOnce(&T) -> Result<bool, E>>(
        &self,
        validator: F,
    ) -> Result<bool, E> {
        if self.validated.get() {
            Ok(true)
        } else {
            let res = validator(&self.payload);
            self.validated.set(*res.as_ref().unwrap_or(&false));
            res
        }
    }

    /// Marks the payload as valid.  No verification is performed; it’s caller’s
    /// responsibility to make sure the payload has indeed been validated.
    pub fn mark_as_valid(&self) {
        self.validated.set(true);
    }

    /// Applies function to the payload (whether it’s been validated or not) and
    /// returns new object with result of the function as payload.  Validated
    /// state is not changed.
    ///
    /// # Example
    ///
    /// ```
    /// use near_primitives::utils::MaybeValidated;
    ///
    /// let value = MaybeValidated::from(42);
    /// assert_eq!("42", value.map(|v| v.to_string()).into_inner());
    /// ```
    pub fn map<U, F: FnOnce(T) -> U>(self, validator: F) -> MaybeValidated<U> {
        MaybeValidated { validated: self.validated, payload: validator(self.payload) }
    }

    /// Returns a new object storing reference to this object’s payload.  Note
    /// that the two objects do not share the validated state so calling
    /// `validate_with` on one of them does not affect the other.
    ///
    /// # Example
    ///
    /// ```
    /// use near_primitives::utils::MaybeValidated;
    ///
    /// let value = MaybeValidated::from(42);
    /// let value_as_ref = value.as_ref();
    /// assert_eq!(Ok(true), value_as_ref.validate_with::<(), _>(|&&v| Ok(v == 42)));
    /// assert!(value_as_ref.is_validated());
    /// assert!(!value.is_validated());
    /// ```
    pub fn as_ref(&self) -> MaybeValidated<&T> {
        MaybeValidated { validated: self.validated.clone(), payload: &self.payload }
    }

    /// Returns whether the payload has been validated.
    pub fn is_validated(&self) -> bool {
        self.validated.get()
    }

    /// Extracts the payload whether or not it’s been validated.
    pub fn into_inner(self) -> T {
        self.payload
    }

    /// Returns a reference to the payload
    pub fn get_inner(&self) -> &T {
        &self.payload
    }
}

impl<T> From<T> for MaybeValidated<T> {
    /// Creates new MaybeValidated object marking payload as not validated.
    fn from(payload: T) -> Self {
        Self { validated: std::cell::Cell::new(false), payload }
    }
}

impl<T: Sized> Deref for MaybeValidated<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

pub fn get_block_shard_id(block_hash: &CryptoHash, shard_id: ShardId) -> Vec<u8> {
    let mut res = Vec::with_capacity(40);
    res.extend_from_slice(block_hash.as_ref());
    res.extend_from_slice(&shard_id.to_le_bytes());
    res
}

pub fn get_block_shard_id_rev(
    key: &[u8],
) -> Result<(CryptoHash, ShardId), Box<dyn std::error::Error + Send + Sync>> {
    if key.len() != 40 {
        return Err(
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid key length").into()
        );
    }
    let (block_hash_bytes, shard_id_bytes) = key.split_at(32);
    let block_hash = CryptoHash::try_from(block_hash_bytes)?;
    let shard_id = ShardId::from_le_bytes(shard_id_bytes.try_into()?);
    Ok((block_hash, shard_id))
}

pub fn get_outcome_id_block_hash(outcome_id: &CryptoHash, block_hash: &CryptoHash) -> Vec<u8> {
    let mut res = Vec::with_capacity(64);
    res.extend_from_slice(outcome_id.as_ref());
    res.extend_from_slice(block_hash.as_ref());
    res
}

pub fn get_outcome_id_block_hash_rev(key: &[u8]) -> std::io::Result<(CryptoHash, CryptoHash)> {
    if key.len() != 64 {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid key length"));
    }
    let outcome_id = CryptoHash::try_from(&key[..32]).unwrap();
    let block_hash = CryptoHash::try_from(&key[32..]).unwrap();
    Ok((outcome_id, block_hash))
}

/// Creates a new Receipt ID from a given signed transaction and a block hash.
/// This method is backward compatible, so it takes the current protocol version.
pub fn create_receipt_id_from_transaction(
    protocol_version: ProtocolVersion,
    signed_transaction: &SignedTransaction,
    prev_block_hash: &CryptoHash,
    block_hash: &CryptoHash,
) -> CryptoHash {
    create_hash_upgradable(
        protocol_version,
        &signed_transaction.get_hash(),
        prev_block_hash,
        block_hash,
        0,
    )
}

/// Creates a new Receipt ID from a given receipt, a block hash and a new receipt index.
/// This method is backward compatible, so it takes the current protocol version.
pub fn create_receipt_id_from_receipt(
    protocol_version: ProtocolVersion,
    receipt: &Receipt,
    prev_block_hash: &CryptoHash,
    block_hash: &CryptoHash,
    receipt_index: usize,
) -> CryptoHash {
    create_hash_upgradable(
        protocol_version,
        &receipt.receipt_id,
        prev_block_hash,
        block_hash,
        receipt_index as u64,
    )
}

/// Creates a new action_hash from a given receipt, a block hash and an action index.
/// This method is backward compatible, so it takes the current protocol version.
pub fn create_action_hash(
    protocol_version: ProtocolVersion,
    receipt: &Receipt,
    prev_block_hash: &CryptoHash,
    block_hash: &CryptoHash,
    action_index: usize,
) -> CryptoHash {
    // Action hash uses the same input as a new receipt ID, so to avoid hash conflicts we use the
    // salt starting from the `u64` going backward.
    let salt = u64::MAX.wrapping_sub(action_index as u64);
    create_hash_upgradable(protocol_version, &receipt.receipt_id, prev_block_hash, block_hash, salt)
}

/// Creates a new `data_id` from a given action hash, a block hash and a data index.
/// This method is backward compatible, so it takes the current protocol version.
pub fn create_data_id(
    protocol_version: ProtocolVersion,
    action_hash: &CryptoHash,
    prev_block_hash: &CryptoHash,
    block_hash: &CryptoHash,
    data_index: usize,
) -> CryptoHash {
    create_hash_upgradable(
        protocol_version,
        action_hash,
        prev_block_hash,
        block_hash,
        data_index as u64,
    )
}

/// Creates a unique random seed to be provided to `VMContext` from a give `action_hash` and
/// a given `random_seed`.
/// This method is backward compatible, so it takes the current protocol version.
pub fn create_random_seed(
    protocol_version: ProtocolVersion,
    action_hash: CryptoHash,
    random_seed: CryptoHash,
) -> Vec<u8> {
    let res = if protocol_version < CORRECT_RANDOM_VALUE_PROTOCOL_VERSION {
        action_hash
    } else if protocol_version < CREATE_HASH_PROTOCOL_VERSION {
        random_seed
    } else {
        // Generates random seed from random_seed and action_hash.
        // Since every action hash is unique, the seed will be unique per receipt and even
        // per action within a receipt.
        const BYTES_LEN: usize = size_of::<CryptoHash>() + size_of::<CryptoHash>();
        let mut bytes: Vec<u8> = Vec::with_capacity(BYTES_LEN);
        bytes.extend_from_slice(action_hash.as_ref());
        bytes.extend_from_slice(random_seed.as_ref());
        hash(&bytes)
    };
    res.as_ref().to_vec()
}

/// Creates a new CryptoHash ID based on the protocol version.
/// Before `CREATE_HASH_PROTOCOL_VERSION` it uses `create_nonce_with_nonce` with
/// just `base` and `salt`. But after `CREATE_HASH_PROTOCOL_VERSION` it uses
/// `extra_hash` in addition to the `base` and `salt`.
/// E.g. this `extra_hash` can be a block hash to distinguish receipts between forks.
fn create_hash_upgradable(
    protocol_version: ProtocolVersion,
    base: &CryptoHash,
    extra_hash_old: &CryptoHash,
    extra_hash: &CryptoHash,
    salt: u64,
) -> CryptoHash {
    if protocol_version < CREATE_HASH_PROTOCOL_VERSION {
        create_nonce_with_nonce(base, salt)
    } else {
        const BYTES_LEN: usize =
            size_of::<CryptoHash>() + size_of::<CryptoHash>() + size_of::<u64>();
        let mut bytes: Vec<u8> = Vec::with_capacity(BYTES_LEN);
        bytes.extend_from_slice(base.as_ref());
        let extra_hash_used =
            if protocol_version < CREATE_RECEIPT_ID_SWITCH_TO_CURRENT_BLOCK_VERSION {
                extra_hash_old
            } else {
                extra_hash
            };
        bytes.extend_from_slice(extra_hash_used.as_ref());
        bytes.extend(index_to_bytes(salt));
        hash(&bytes)
    }
}

/// Deprecated. Please use `create_hash_upgradable`
fn create_nonce_with_nonce(base: &CryptoHash, salt: u64) -> CryptoHash {
    let mut nonce: Vec<u8> = base.as_ref().to_owned();
    nonce.extend(index_to_bytes(salt));
    hash(&nonce)
}

pub fn index_to_bytes(index: u64) -> [u8; 8] {
    index.to_le_bytes()
}

/// A wrapper around Option<T> that provides native Display trait.
/// Simplifies propagating automatic Display trait on parent structs.
pub struct DisplayOption<T>(pub Option<T>);

impl<T: fmt::Display> fmt::Display for DisplayOption<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
                tracing::error!(target: "client", "Unwrap error: {}", err);
                return $ret;
            }
        }
    };
    ($obj: expr) => {
        match $obj {
            Ok(value) => value,
            Err(err) => {
                tracing::error!(target: "client", "Unwrap error: {}", err);
                return;
            }
        }
    };
}

/// Converts timestamp in ns into DateTime UTC time.
pub fn from_timestamp(timestamp: u64) -> DateTime<chrono::Utc> {
    let secs = (timestamp / NS_IN_SECOND) as i64;
    let nsecs = (timestamp % NS_IN_SECOND) as u32;
    let naive = NaiveDateTime::from_timestamp_opt(secs, nsecs).unwrap();
    DateTime::from_utc(naive, chrono::Utc)
}

/// Converts DateTime UTC time into timestamp in ns.
pub fn to_timestamp(time: DateTime<chrono::Utc>) -> u64 {
    time.timestamp_nanos() as u64
}

/// Compute number of seats per shard for given total number of seats and number of shards.
pub fn get_num_seats_per_shard(num_shards: NumShards, num_seats: NumSeats) -> Vec<NumSeats> {
    (0..num_shards)
        .map(|shard_id| {
            let remainder =
                num_seats.checked_rem(num_shards).expect("num_shards ≠ 0 is guaranteed here");
            let quotient =
                num_seats.checked_div(num_shards).expect("num_shards ≠ 0 is guaranteed here");
            let num = quotient
                .checked_add(if shard_id < remainder { 1 } else { 0 })
                .expect("overflow is impossible here");
            max(num, 1)
        })
        .collect()
}

/// Generate random string of given length
pub fn generate_random_string(len: usize) -> String {
    let bytes = thread_rng().sample_iter(&Alphanumeric).take(len).collect();
    String::from_utf8(bytes).unwrap()
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
pub fn ser<T>(object: &T) -> Serializable<'_, T>
where
    T: serde::Serialize,
{
    Serializable(object)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_num_chunk_producers() {
        for num_seats in 1..50 {
            for num_shards in 1..50 {
                let assignment = get_num_seats_per_shard(num_shards, num_seats);
                assert_eq!(assignment.iter().sum::<u64>(), max(num_seats, num_shards));
            }
        }
    }

    #[test]
    fn test_create_hash_upgradable() {
        let base = hash(b"atata");
        let extra_base = hash(b"hohoho");
        let other_extra_base = hash(b"banana");
        let salt = 3;
        assert_eq!(
            create_nonce_with_nonce(&base, salt),
            create_hash_upgradable(
                CREATE_HASH_PROTOCOL_VERSION - 1,
                &base,
                &extra_base,
                &extra_base,
                salt,
            )
        );
        assert_ne!(
            create_nonce_with_nonce(&base, salt),
            create_hash_upgradable(
                CREATE_HASH_PROTOCOL_VERSION,
                &base,
                &extra_base,
                &extra_base,
                salt,
            )
        );
        assert_ne!(
            create_hash_upgradable(
                CREATE_HASH_PROTOCOL_VERSION,
                &base,
                &extra_base,
                &extra_base,
                salt,
            ),
            create_hash_upgradable(
                CREATE_HASH_PROTOCOL_VERSION,
                &base,
                &other_extra_base,
                &other_extra_base,
                salt,
            )
        );
        assert_ne!(
            create_hash_upgradable(
                CREATE_RECEIPT_ID_SWITCH_TO_CURRENT_BLOCK_VERSION - 1,
                &base,
                &extra_base,
                &other_extra_base,
                salt,
            ),
            create_hash_upgradable(
                CREATE_RECEIPT_ID_SWITCH_TO_CURRENT_BLOCK_VERSION,
                &base,
                &extra_base,
                &other_extra_base,
                salt,
            )
        );
        assert_eq!(
            create_hash_upgradable(
                CREATE_RECEIPT_ID_SWITCH_TO_CURRENT_BLOCK_VERSION,
                &base,
                &extra_base,
                &other_extra_base,
                salt,
            ),
            create_hash_upgradable(
                CREATE_RECEIPT_ID_SWITCH_TO_CURRENT_BLOCK_VERSION,
                &base,
                &other_extra_base,
                &other_extra_base,
                salt
            )
        );
    }
}

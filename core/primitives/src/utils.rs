#[cfg(feature = "clock")]
use crate::block::BlockHeader;
use crate::hash::{CryptoHash, hash};
use crate::transaction::ValidatedTransactionHash;
use crate::types::{NumSeats, NumShards, ShardId};
use crate::version::ProtocolVersion;
use chrono;
use chrono::DateTime;
use near_crypto::{ED25519PublicKey, Secp256K1PublicKey};
use near_primitives_core::account::id::{AccountId, AccountType};
use near_primitives_core::types::BlockHeight;
use near_primitives_core::version::ProtocolFeature;
use serde;
use std::cmp::max;
use std::convert::AsRef;
use std::fmt;
use std::mem::size_of;
use std::ops::Deref;

pub mod compression;
pub mod io;
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

/// Creates a new Receipt ID from a given signed transaction and a block height or hash.
/// This method is backward compatible, so it takes the current protocol version.
pub fn create_receipt_id_from_transaction(
    protocol_version: ProtocolVersion,
    tx_hash: ValidatedTransactionHash,
    block_hash: &CryptoHash,
    block_height: BlockHeight,
) -> CryptoHash {
    create_hash_upgradable(protocol_version, &tx_hash.get_hash(), block_hash, block_height, 0)
}

/// Creates a new Receipt ID from a given receipt id, a block height or hash and a new receipt index.
/// This method is backward compatible, so it takes the current protocol version.
pub fn create_receipt_id_from_receipt_id(
    protocol_version: ProtocolVersion,
    receipt_id: &CryptoHash,
    block_hash: &CryptoHash,
    block_height: BlockHeight,
    receipt_index: usize,
) -> CryptoHash {
    create_hash_upgradable(
        protocol_version,
        receipt_id,
        block_hash,
        block_height,
        receipt_index as u64,
    )
}

/// Creates a new action_hash from a given receipt, a block height or hash and an action index.
/// This method is backward compatible, so it takes the current protocol version.
pub fn create_action_hash_from_receipt_id(
    protocol_version: ProtocolVersion,
    receipt_id: &CryptoHash,
    block_hash: &CryptoHash,
    block_height: BlockHeight,
    action_index: usize,
) -> CryptoHash {
    // Action hash uses the same input as a new receipt ID, so to avoid hash conflicts we use the
    // salt starting from the `u64` going backward.
    let salt = u64::MAX.wrapping_sub(action_index as u64);
    create_hash_upgradable(protocol_version, receipt_id, block_hash, block_height, salt)
}

/// Creates a new Receipt ID from a given action hash, a block height or hash and a new receipt index.
/// This method is backward compatible, so it takes the current protocol version.
pub fn create_receipt_id_from_action_hash(
    protocol_version: ProtocolVersion,
    action_hash: &CryptoHash,
    block_hash: &CryptoHash,
    block_height: BlockHeight,
    receipt_index: u64,
) -> CryptoHash {
    create_hash_upgradable(protocol_version, action_hash, block_hash, block_height, receipt_index)
}

/// Creates a unique random seed to be provided to `VMContext` from a give `action_hash` and
/// a given `random_seed`.
/// This method is backward compatible, so it takes the current protocol version.
pub fn create_random_seed(action_hash: CryptoHash, random_seed: CryptoHash) -> Vec<u8> {
    // Generates random seed from random_seed and action_hash.
    // Since every action hash is unique, the seed will be unique per receipt and even
    // per action within a receipt.
    const BYTES_LEN: usize = size_of::<CryptoHash>() + size_of::<CryptoHash>();
    let mut bytes: Vec<u8> = Vec::with_capacity(BYTES_LEN);
    bytes.extend_from_slice(action_hash.as_ref());
    bytes.extend_from_slice(random_seed.as_ref());
    let res = hash(&bytes);
    res.as_ref().to_vec()
}

/// Creates a new CryptoHash ID based on the protocol version.
/// Before `CREATE_HASH_PROTOCOL_VERSION` it uses `create_nonce_with_nonce` with
/// just `base` and `salt`.
/// After `CREATE_HASH_PROTOCOL_VERSION` it uses `extra_hash` in addition to the `base` and `salt`.
/// E.g. this `extra_hash` can be a block hash to distinguish receipts between forks.
/// After ProtocolFeature::BlockHeightForReceiptId, the code uses `block_height` instead of `extra_hash`.
/// This enables applying chunks using only the optimistic block, which does not yet have a block hash.
fn create_hash_upgradable(
    protocol_version: ProtocolVersion,
    base: &CryptoHash,
    extra_hash: &CryptoHash,
    block_height: BlockHeight,
    salt: u64,
) -> CryptoHash {
    const BYTES_LEN: usize = size_of::<CryptoHash>() + size_of::<CryptoHash>() + size_of::<u64>();
    let mut bytes: Vec<u8> = Vec::with_capacity(BYTES_LEN);
    bytes.extend_from_slice(base.as_ref());
    if ProtocolFeature::BlockHeightForReceiptId.enabled(protocol_version) {
        bytes.extend_from_slice(block_height.to_le_bytes().as_ref())
    } else {
        bytes.extend_from_slice(extra_hash.as_ref())
    }
    bytes.extend(index_to_bytes(salt));
    hash(&bytes)
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
                tracing::error!(target: "near", "Unwrap error: {}", err);
                return $ret;
            }
        }
    };
    ($obj: expr) => {
        match $obj {
            Ok(value) => value,
            Err(err) => {
                tracing::error!(target: "near", "Unwrap error: {}", err);
                return;
            }
        }
    };
}

/// Converts timestamp in ns into DateTime UTC time.
pub fn from_timestamp(timestamp: u64) -> DateTime<chrono::Utc> {
    // cspell:ignore nsecs
    let secs = (timestamp / NS_IN_SECOND) as i64;
    let nsecs = (timestamp % NS_IN_SECOND) as u32;
    DateTime::from_timestamp(secs, nsecs).unwrap()
}

/// Converts DateTime UTC time into timestamp in ns.
pub fn to_timestamp(time: DateTime<chrono::Utc>) -> u64 {
    // The unwrap will be safe for all dates between 1678 and 2261.
    time.timestamp_nanos_opt().unwrap() as u64
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
#[cfg(feature = "rand")]
pub fn generate_random_string(len: usize) -> String {
    use rand::distributions::Alphanumeric;
    use rand::{Rng, thread_rng};

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

/// From `near-account-id` version `1.0.0-alpha.2`, `is_implicit` returns true for ETH-implicit accounts.
/// This function is a wrapper for `is_implicit` method so that we can easily differentiate its behavior
/// based on whether ETH-implicit accounts are enabled.
pub fn account_is_implicit(account_id: &AccountId, eth_implicit_accounts_enabled: bool) -> bool {
    if eth_implicit_accounts_enabled {
        account_id.get_account_type().is_implicit()
    } else {
        account_id.get_account_type() == AccountType::NearImplicitAccount
    }
}

/// Returns hex-encoded copy of the public key.
/// This is a NEAR-implicit account ID which can be controlled by the corresponding ED25519 private key.
pub fn derive_near_implicit_account_id(public_key: &ED25519PublicKey) -> AccountId {
    hex::encode(public_key).parse().unwrap()
}

/// Returns '0x' + keccak256(public_key)[12:32].hex().
/// This is an ETH-implicit account ID which can be controlled by the corresponding Secp256K1 private key.
pub fn derive_eth_implicit_account_id(public_key: &Secp256K1PublicKey) -> AccountId {
    use sha3::Digest;
    let pk_hash = sha3::Keccak256::digest(&public_key);
    format!("0x{}", hex::encode(&pk_hash[12..32])).parse().unwrap()
}

/// Returns the block metadata used to create an optimistic block.
#[cfg(feature = "clock")]
pub fn get_block_metadata(
    prev_block_header: &BlockHeader,
    signer: &crate::validator_signer::ValidatorSigner,
    now: u64,
    sandbox_delta_time: Option<near_time::Duration>,
) -> (u64, near_crypto::vrf::Value, near_crypto::vrf::Proof, CryptoHash) {
    #[cfg(feature = "sandbox")]
    let now = now + sandbox_delta_time.unwrap().whole_nanoseconds() as u64;
    #[cfg(not(feature = "sandbox"))]
    debug_assert!(sandbox_delta_time.is_none());
    let time = if now <= prev_block_header.raw_timestamp() {
        prev_block_header.raw_timestamp() + 1
    } else {
        now
    };

    let (vrf_value, vrf_proof) =
        signer.compute_vrf_with_proof(prev_block_header.random_value().as_ref());
    let random_value = hash(vrf_value.0.as_ref());
    (time, vrf_value, vrf_proof, random_value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_crypto::{KeyType, PublicKey};

    #[test]
    fn test_derive_near_implicit_account_id() {
        let public_key = PublicKey::from_seed(KeyType::ED25519, "test");
        let expected: AccountId =
            "bb4dc639b212e075a751685b26bdcea5920a504181ff2910e8549742127092a0".parse().unwrap();
        let account_id = derive_near_implicit_account_id(public_key.unwrap_as_ed25519());
        assert_eq!(account_id, expected);
    }

    #[test]
    fn test_derive_eth_implicit_account_id() {
        let public_key = PublicKey::from_seed(KeyType::SECP256K1, "test");
        let expected: AccountId = "0x96791e923f8cf697ad9c3290f2c9059f0231b24c".parse().unwrap();
        let account_id = derive_eth_implicit_account_id(public_key.unwrap_as_secp256k1());
        assert_eq!(account_id, expected);
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

    #[test]
    fn test_create_hash_upgradable() {
        // cspell:ignore atata hohoho
        let base = hash(b"atata");
        let extra_base = hash(b"hohoho");
        let other_extra_base = hash(b"banana");
        let block_height: BlockHeight = 123_456_789;
        let other_block_height: BlockHeight = 123_123_123;
        let salt = 3;

        // Check that for protocol versions post BlockHeightForReceiptId, the hash does not depend on block hash.
        assert_eq!(
            create_hash_upgradable(
                ProtocolFeature::BlockHeightForReceiptId.protocol_version(),
                &base,
                &extra_base,
                block_height,
                salt,
            ),
            create_hash_upgradable(
                ProtocolFeature::BlockHeightForReceiptId.protocol_version(),
                &base,
                &other_extra_base,
                block_height,
                salt
            )
        );
        // Check that for protocol versions pre BlockHeightForReceiptId, the hash does not depend on block height.
        assert_eq!(
            create_hash_upgradable(
                ProtocolFeature::BlockHeightForReceiptId.protocol_version() - 1,
                &base,
                &other_extra_base,
                block_height,
                salt,
            ),
            create_hash_upgradable(
                ProtocolFeature::BlockHeightForReceiptId.protocol_version() - 1,
                &base,
                &other_extra_base,
                other_block_height,
                salt,
            )
        );
        // Check that for protocol versions post BlockHeightForReceiptId, the hash changes if block height changes.
        assert_ne!(
            create_hash_upgradable(
                ProtocolFeature::BlockHeightForReceiptId.protocol_version(),
                &base,
                &other_extra_base,
                block_height,
                salt,
            ),
            create_hash_upgradable(
                ProtocolFeature::BlockHeightForReceiptId.protocol_version(),
                &base,
                &other_extra_base,
                other_block_height,
                salt,
            )
        );
    }
}

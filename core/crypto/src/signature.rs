use crate::hash_domain::HashDomainTag;
use crate::util::try_fixed_array;
use aws_lc_rs::signature::UnparsedPublicKey;
use aws_lc_rs::unstable::signature::{ML_DSA_65, ML_DSA_65_SIGNING, PqdsaKeyPair};
use borsh::{BorshDeserialize, BorshSerialize};
use ed25519_dalek::ed25519::signature::{Signer, Verifier};
use near_schema_checker_lib::ProtocolSchema;
use primitive_types::U256;
use secp256k1::Message;
use std::convert::AsRef;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::io::{Error, ErrorKind, Read, Write};
use std::str::FromStr;
use std::sync::LazyLock;

pub static SECP256K1: LazyLock<secp256k1::Secp256k1<secp256k1::All>> =
    LazyLock::new(secp256k1::Secp256k1::new);

/// ML-DSA-65 public key length in bytes.
pub const ML_DSA_65_PUBLIC_KEY_LENGTH: usize = 1952;
/// ML-DSA-65 raw private key length in bytes.
pub const ML_DSA_65_SECRET_KEY_LENGTH: usize = 4032;
/// ML-DSA-65 signature length in bytes.
pub const ML_DSA_65_SIGNATURE_LENGTH: usize = 3309;
/// FIPS 204 seed length in bytes (used to derive an ML-DSA private key).
#[cfg(feature = "rand")]
pub const ML_DSA_65_SEED_LENGTH: usize = 32;
/// SHA3-256 output length used as the on-trie identifier for an
/// ML-DSA-65 access key. The same digest is expected to be reused as
/// the account-id payload for ML-DSA-65 implicit accounts when that
/// feature lands; update this comment then.
pub const ML_DSA_65_HASH_LENGTH: usize = 32;
/// Wire-format prefix for an ML-DSA-65 access-key identifier (the SHA3-256
/// digest, not the full pubkey). Used in `view_access_key_list` responses
/// and accepted by `PublicKeyHandle::from_str`.
const ML_DSA_65_HASH_PREFIX: &str = "ml-dsa-65-hash:";

#[derive(Debug, Copy, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(test, derive(bolero::TypeGenerator))]
pub enum KeyType {
    ED25519 = 0,
    SECP256K1 = 1,
    MLDSA65 = 2,
}

impl KeyType {
    /// Returns `true` if this key type belongs to a post-quantum signature
    /// scheme. Exhaustive match by design: adding a new `KeyType` variant
    /// forces a compile-time decision about whether it is post-quantum.
    pub fn is_post_quantum(&self) -> bool {
        match self {
            KeyType::ED25519 | KeyType::SECP256K1 => false,
            KeyType::MLDSA65 => true,
        }
    }
}

impl Display for KeyType {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str(match self {
            KeyType::ED25519 => "ed25519",
            KeyType::SECP256K1 => "secp256k1",
            KeyType::MLDSA65 => "ml-dsa-65",
        })
    }
}

impl FromStr for KeyType {
    type Err = crate::errors::ParseKeyTypeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let lowercase_key_type = value.to_ascii_lowercase();
        match lowercase_key_type.as_str() {
            "ed25519" => Ok(KeyType::ED25519),
            "secp256k1" => Ok(KeyType::SECP256K1),
            "ml-dsa-65" => Ok(KeyType::MLDSA65),
            _ => Err(Self::Err::UnknownKeyType { unknown_key_type: lowercase_key_type }),
        }
    }
}

impl TryFrom<u8> for KeyType {
    type Error = crate::errors::ParseKeyTypeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(KeyType::ED25519),
            1 => Ok(KeyType::SECP256K1),
            2 => Ok(KeyType::MLDSA65),
            unknown_key_type => {
                Err(Self::Error::UnknownKeyType { unknown_key_type: unknown_key_type.to_string() })
            }
        }
    }
}

fn split_key_type_data(value: &str) -> Result<(KeyType, &str), crate::errors::ParseKeyTypeError> {
    if let Some((prefix, key_data)) = value.split_once(':') {
        Ok((KeyType::from_str(prefix)?, key_data))
    } else {
        // If there is no prefix then we Default to ED25519.
        Ok((KeyType::ED25519, value))
    }
}

#[derive(
    Clone, Eq, Ord, PartialEq, PartialOrd, derive_more::AsRef, derive_more::From, ProtocolSchema,
)]
#[cfg_attr(test, derive(bolero::TypeGenerator))]
#[as_ref(forward)]
pub struct Secp256K1PublicKey([u8; 64]);

impl TryFrom<&[u8]> for Secp256K1PublicKey {
    type Error = crate::errors::ParseKeyError;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self(try_fixed_array(data)?))
    }
}

impl std::fmt::Debug for Secp256K1PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        Display::fmt(&Bs58(&self.0), f)
    }
}

#[derive(
    Clone, Eq, Ord, PartialEq, PartialOrd, derive_more::AsRef, derive_more::From, ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(test, derive(bolero::TypeGenerator))]
#[as_ref(forward)]
pub struct ED25519PublicKey(pub [u8; ed25519_dalek::PUBLIC_KEY_LENGTH]);

impl TryFrom<&[u8]> for ED25519PublicKey {
    type Error = crate::errors::ParseKeyError;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self(try_fixed_array(data)?))
    }
}

impl std::fmt::Debug for ED25519PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        Display::fmt(&Bs58(&self.0), f)
    }
}

/// ML-DSA-65 public key (1952 bytes).
///
/// Boxed to keep `PublicKey` enum size bounded - the inline form would bloat
/// every `PublicKey` holder to ~2 KiB regardless of variant.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Hash, derive_more::AsRef, ProtocolSchema)]
#[as_ref(forward)]
pub struct MlDsa65PublicKey(pub Box<[u8; ML_DSA_65_PUBLIC_KEY_LENGTH]>);

impl MlDsa65PublicKey {
    /// Compute the on-trie [`MlDsa65PublicKeyHandle`] for this public key -
    /// the SHA3-256 of (domain-separation tag || raw pubkey bytes).
    ///
    /// This is the form an ML-DSA-65 access key takes inside the trie: the
    /// full pubkey lives only on the wire (in transactions and actions),
    /// never in state. The full pubkey can be derived from the handle only
    /// by brute force.
    pub fn to_public_key_handle(&self) -> MlDsa65PublicKeyHandle {
        use sha3::{Digest, Sha3_256};
        let mut hasher = Sha3_256::new();
        hasher.update(HashDomainTag::MlDsa65PubkeyV1.as_bytes());
        hasher.update(&self.0[..]);
        let mut out = [0u8; ML_DSA_65_HASH_LENGTH];
        out.copy_from_slice(&hasher.finalize());
        MlDsa65PublicKeyHandle(out)
    }
}

impl TryFrom<&[u8]> for MlDsa65PublicKey {
    type Error = crate::errors::ParseKeyError;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self(Box::new(try_fixed_array(data)?)))
    }
}

impl Debug for MlDsa65PublicKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        Display::fmt(&Bs58(self.0.as_ref()), f)
    }
}

#[cfg(test)]
impl bolero::TypeGenerator for MlDsa65PublicKey {
    fn generate<D: bolero::Driver>(driver: &mut D) -> Option<Self> {
        let mut buf = Box::new([0u8; ML_DSA_65_PUBLIC_KEY_LENGTH]);
        for byte in &mut buf[..] {
            *byte = u8::generate(driver)?;
        }
        Some(MlDsa65PublicKey(buf))
    }
}

/// On-trie identifier of an ML-DSA-65 access key - and, in the future,
/// the basis for ML-DSA-65 implicit-account ids. Stored as the SHA3-256
/// digest of (domain-tag || raw pubkey) because the full 1952-byte pubkey
/// would be prohibitive to keep in state. Cannot sign or verify; only
/// appears as a lookup key and in view-API responses.
#[derive(
    Clone,
    Copy,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Hash,
    derive_more::AsRef,
    derive_more::From,
    ProtocolSchema,
)]
#[as_ref(forward)]
pub struct MlDsa65PublicKeyHandle(pub [u8; ML_DSA_65_HASH_LENGTH]);

impl TryFrom<&[u8]> for MlDsa65PublicKeyHandle {
    type Error = crate::errors::ParseKeyError;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self(try_fixed_array(data)?))
    }
}

impl Debug for MlDsa65PublicKeyHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        Display::fmt(&Bs58(&self.0), f)
    }
}

#[cfg(test)]
impl bolero::TypeGenerator for MlDsa65PublicKeyHandle {
    fn generate<D: bolero::Driver>(driver: &mut D) -> Option<Self> {
        let mut buf = [0u8; ML_DSA_65_HASH_LENGTH];
        for byte in &mut buf {
            *byte = u8::generate(driver)?;
        }
        Some(MlDsa65PublicKeyHandle(buf))
    }
}

/// Public key container supporting different curves.
#[derive(Clone, PartialEq, PartialOrd, Ord, Eq, ProtocolSchema)]
#[cfg_attr(test, derive(bolero::TypeGenerator))]
pub enum PublicKey {
    /// 256 bit elliptic curve based public-key.
    ED25519(ED25519PublicKey),
    /// 512 bit elliptic curve based public-key used in Bitcoin's public-key cryptography.
    SECP256K1(Secp256K1PublicKey),
    /// FIPS 204 ML-DSA-65 post-quantum public key (1952 bytes).
    MLDSA65(MlDsa65PublicKey),
}

impl PublicKey {
    /// Length of this public key's borsh encoding, in bytes - that is,
    /// the on-the-wire size of the raw key bytes plus a 1-byte borsh
    /// discriminant tag (the leading `+ 1` in each arm).
    ///
    /// For storage-fee accounting use [`PublicKey::trie_id_len`] instead;
    /// for ML-DSA-65 those two diverge (1953 wire vs 33 on-trie).
    // `is_empty` always returns false, so there is no point in adding it
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        match self {
            Self::ED25519(_) => 1 + ed25519_dalek::PUBLIC_KEY_LENGTH,
            Self::SECP256K1(_) => 1 + 64,
            Self::MLDSA65(_) => 1 + ML_DSA_65_PUBLIC_KEY_LENGTH,
        }
    }

    pub fn empty(key_type: KeyType) -> Self {
        match key_type {
            KeyType::ED25519 => {
                PublicKey::ED25519(ED25519PublicKey([0u8; ed25519_dalek::PUBLIC_KEY_LENGTH]))
            }
            KeyType::SECP256K1 => PublicKey::SECP256K1(Secp256K1PublicKey([0u8; 64])),
            KeyType::MLDSA65 => {
                PublicKey::MLDSA65(MlDsa65PublicKey(Box::new([0u8; ML_DSA_65_PUBLIC_KEY_LENGTH])))
            }
        }
    }

    pub fn key_type(&self) -> KeyType {
        match self {
            Self::ED25519(_) => KeyType::ED25519,
            Self::SECP256K1(_) => KeyType::SECP256K1,
            Self::MLDSA65(_) => KeyType::MLDSA65,
        }
    }

    fn key_tag(&self) -> KeyTag {
        match self {
            PublicKey::ED25519(_) => KeyTag::Ed25519,
            PublicKey::SECP256K1(_) => KeyTag::Secp256k1,
            PublicKey::MLDSA65(_) => KeyTag::MlDsa65Full,
        }
    }

    pub fn key_data(&self) -> &[u8] {
        match self {
            Self::ED25519(key) => key.as_ref(),
            Self::SECP256K1(key) => key.as_ref(),
            Self::MLDSA65(key) => key.as_ref(),
        }
    }

    pub fn unwrap_as_ed25519(&self) -> &ED25519PublicKey {
        match self {
            Self::ED25519(key) => key,
            Self::SECP256K1(_) | Self::MLDSA65(_) => panic!(),
        }
    }

    pub fn unwrap_as_secp256k1(&self) -> &Secp256K1PublicKey {
        match self {
            Self::SECP256K1(key) => key,
            Self::ED25519(_) | Self::MLDSA65(_) => panic!(),
        }
    }

    /// Length, in bytes, of the on-trie identifier for an access-key
    /// entry owned by this public key. For ed25519/secp256k1 this matches
    /// `len()`; for ML-DSA-65 the trie stores a SHA3-256 hash (33 bytes
    /// including the type tag), not the 1953-byte borsh-encoded pubkey.
    /// Used by storage-fee calculations on the runtime side; cheap to call
    /// (no hashing) - for ML-DSA-65 this returns the size of the digest
    /// form without actually hashing the pubkey.
    pub fn trie_id_len(&self) -> usize {
        match self {
            Self::ED25519(_) => 1 + ed25519_dalek::PUBLIC_KEY_LENGTH,
            Self::SECP256K1(_) => 1 + 64,
            Self::MLDSA65(_) => 1 + ML_DSA_65_HASH_LENGTH,
        }
    }
}

/// Borsh/`Hash` discriminant tags shared by `PublicKey` and
/// `PublicKeyHandle`. The two enums use disjoint tag spaces so a value of
/// one can never silently decode as the other:
///   `PublicKey`       owns {Ed25519, Secp256k1, MlDsa65Full} = {0,1,2}
///   `PublicKeyHandle` owns {Ed25519, Secp256k1, MlDsa65Hash} = {0,1,3}
/// `MlDsa65Full` (the 1952-byte key) must never appear in a handle;
/// `MlDsa65Hash` (the 32-byte digest) must never appear on the wire.
/// Both deserializers match this enum exhaustively, so a future scheme
/// cannot silently reuse a reserved tag: the compiler forces a decision
/// in both enums.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum KeyTag {
    Ed25519 = 0,
    Secp256k1 = 1,
    MlDsa65Full = 2,
    MlDsa65Hash = 3,
}

impl TryFrom<u8> for KeyTag {
    type Error = Error;

    fn try_from(tag: u8) -> Result<Self, Self::Error> {
        match tag {
            0 => Ok(Self::Ed25519),
            1 => Ok(Self::Secp256k1),
            2 => Ok(Self::MlDsa65Full),
            3 => Ok(Self::MlDsa65Hash),
            _ => Err(Error::new(ErrorKind::InvalidData, format!("unknown KeyTag: {tag}"))),
        }
    }
}

// This `Hash` implementation is safe since it retains the property
// `k1 == k2 ⇒ hash(k1) == hash(k2)`.
impl Hash for PublicKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u8(self.key_tag() as u8);
        state.write(self.key_data());
    }
}

impl Display for PublicKey {
    fn fmt(&self, fmt: &mut Formatter) -> std::fmt::Result {
        match self {
            PublicKey::ED25519(pk) => write!(fmt, "{}:{}", KeyType::ED25519, Bs58(&pk.0[..])),
            PublicKey::SECP256K1(pk) => write!(fmt, "{}:{}", KeyType::SECP256K1, Bs58(&pk.0[..])),
            PublicKey::MLDSA65(pk) => write!(fmt, "{}:{}", KeyType::MLDSA65, Bs58(&pk.0[..])),
        }
    }
}

impl Debug for PublicKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        Display::fmt(self, f)
    }
}

impl BorshSerialize for PublicKey {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        BorshSerialize::serialize(&(self.key_tag() as u8), writer)?;
        writer.write_all(self.key_data())?;
        Ok(())
    }
}

impl BorshDeserialize for PublicKey {
    fn deserialize_reader<R: Read>(rd: &mut R) -> std::io::Result<Self> {
        let raw_tag = u8::deserialize_reader(rd)?;
        let tag = raw_tag.try_into()?;
        match tag {
            KeyTag::Ed25519 => {
                Ok(PublicKey::ED25519(ED25519PublicKey(BorshDeserialize::deserialize_reader(rd)?)))
            }
            KeyTag::Secp256k1 => Ok(PublicKey::SECP256K1(Secp256K1PublicKey(
                BorshDeserialize::deserialize_reader(rd)?,
            ))),
            KeyTag::MlDsa65Full => {
                let mut buf = Box::new([0u8; ML_DSA_65_PUBLIC_KEY_LENGTH]);
                rd.read_exact(buf.as_mut())?;
                Ok(PublicKey::MLDSA65(MlDsa65PublicKey(buf)))
            }
            KeyTag::MlDsa65Hash => Err(Error::new(
                ErrorKind::InvalidData,
                "borsh tag 3 is reserved for PublicKeyHandle (ML-DSA-65 hash); not a valid PublicKey",
            )),
        }
    }
}

impl serde::Serialize for PublicKey {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> Result<<S as serde::Serializer>::Ok, <S as serde::Serializer>::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as serde::Deserializer<'de>>::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        s.parse()
            .map_err(|err: crate::errors::ParseKeyError| serde::de::Error::custom(err.to_string()))
    }
}

impl FromStr for PublicKey {
    type Err = crate::errors::ParseKeyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (key_type, key_data) = split_key_type_data(value)?;
        Ok(match key_type {
            KeyType::ED25519 => Self::ED25519(ED25519PublicKey(decode_bs58(key_data)?)),
            KeyType::SECP256K1 => Self::SECP256K1(Secp256K1PublicKey(decode_bs58(key_data)?)),
            KeyType::MLDSA65 => Self::MLDSA65(MlDsa65PublicKey(Box::new(decode_bs58(key_data)?))),
        })
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for PublicKey {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "PublicKey".to_string().into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        String::json_schema(generator)
    }
}

impl From<ED25519PublicKey> for PublicKey {
    fn from(ed25519: ED25519PublicKey) -> Self {
        Self::ED25519(ed25519)
    }
}

impl From<Secp256K1PublicKey> for PublicKey {
    fn from(secp256k1: Secp256K1PublicKey) -> Self {
        Self::SECP256K1(secp256k1)
    }
}

impl From<MlDsa65PublicKey> for PublicKey {
    fn from(ml_dsa: MlDsa65PublicKey) -> Self {
        Self::MLDSA65(ml_dsa)
    }
}

/// How an access-key entry is referred to in the trie.
///
/// The trie stores ed25519 and secp256k1 access keys as their full
/// public keys, but ML-DSA-65 access keys as a SHA3-256 hash of the
/// pubkey (storage-efficiency optimization - see
/// `docs/architecture/how/post_quantum_signatures.md`).
///
/// `PublicKeyHandle` is the type the trie-key layer reads and writes, and the
/// type used by view-API responses that list an account's keys. It is
/// NOT a public key - the `MlDsa65` variant cannot verify a
/// signature. To verify, the caller needs the full pubkey, which is
/// carried separately on the wire (in the transaction or action).
///
/// The variant set mirrors [`PublicKey`] for schemes that store the
/// full key in the trie (ed25519, secp256k1), and replaces ML-DSA-65's
/// full-key variant with the SHA3-256 hash actually stored. This makes
/// "a full ML-DSA-65 key in the trie" unrepresentable in the type
/// system - the encoding/decoding round-trip becomes lossless and
/// invalid combinations cannot be constructed.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, ProtocolSchema)]
pub enum PublicKeyHandle {
    /// Full ed25519 public key, as stored in the trie.
    ED25519(ED25519PublicKey),
    /// Full secp256k1 public key, as stored in the trie.
    SECP256K1(Secp256K1PublicKey),
    /// SHA3-256 hash of an ML-DSA-65 public key. The full pubkey is not
    /// stored on-chain; only this hash appears in the trie.
    MlDsa65(MlDsa65PublicKeyHandle),
}

// `Hash` is implemented manually because `ED25519PublicKey` and
// `Secp256K1PublicKey` don't derive `Hash` (they predate the need).
// The encoding mirrors `PublicKey`'s manual `Hash` impl so an
// ed25519/secp256k1 `PublicKeyHandle` hashes to the same bytes as the
// corresponding `PublicKey` would, plus a distinct tag for the
// `MlDsa65` variant.
impl Hash for PublicKeyHandle {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u8(self.key_tag() as u8);
        state.write(self.key_data());
    }
}

impl PublicKeyHandle {
    /// Length, in bytes, of this handle's on-trie borsh encoding: the raw
    /// handle bytes plus a 1-byte borsh discriminant tag (the leading
    /// `+ 1` in each arm). For ML-DSA-65 the handle is the 32-byte
    /// SHA3-256 digest, not the full 1952-byte pubkey.
    pub fn trie_id_len(&self) -> usize {
        match self {
            Self::ED25519(_) => 1 + ed25519_dalek::PUBLIC_KEY_LENGTH,
            Self::SECP256K1(_) => 1 + 64,
            Self::MlDsa65(_) => 1 + ML_DSA_65_HASH_LENGTH,
        }
    }

    fn key_tag(&self) -> KeyTag {
        match self {
            PublicKeyHandle::ED25519(_) => KeyTag::Ed25519,
            PublicKeyHandle::SECP256K1(_) => KeyTag::Secp256k1,
            PublicKeyHandle::MlDsa65(_) => KeyTag::MlDsa65Hash,
        }
    }

    fn key_data(&self) -> &[u8] {
        match self {
            PublicKeyHandle::ED25519(k) => &k.0,
            PublicKeyHandle::SECP256K1(k) => &k.0,
            PublicKeyHandle::MlDsa65(k) => &k.0,
        }
    }

    /// Key-type tag for this handle. All ML-DSA-65 entries report
    /// `KeyType::MLDSA65`; the underlying storage is the hash but the
    /// scheme is the same.
    pub fn key_type(&self) -> KeyType {
        match self {
            Self::ED25519(_) => KeyType::ED25519,
            Self::SECP256K1(_) => KeyType::SECP256K1,
            Self::MlDsa65(_) => KeyType::MLDSA65,
        }
    }

    /// Returns the underlying full public key, if this handle carries
    /// one (ed25519 / secp256k1). For ML-DSA-65 entries the trie stores
    /// only a hash, so the full pubkey is not recoverable - returns
    /// `None` in that case.
    ///
    /// TODO(post-quantum): every current caller treats the `None` case as "skip
    /// this entry" (fork-network and mirror tools), which means
    /// ML-DSA-65 access keys are silently dropped during network
    /// forking and mirroring. Grep for `TODO(post-quantum)` to find the call
    /// sites that need to be taught to recover the full pubkey (e.g.
    /// via a side index of pubkey-hash → pubkey, or via RPC lookup).
    pub fn full_pubkey(&self) -> Option<PublicKey> {
        match self {
            Self::ED25519(k) => Some(PublicKey::ED25519(k.clone())),
            Self::SECP256K1(k) => Some(PublicKey::SECP256K1(k.clone())),
            Self::MlDsa65(_) => None,
        }
    }
}

impl From<PublicKey> for PublicKeyHandle {
    fn from(pk: PublicKey) -> Self {
        match pk {
            PublicKey::ED25519(k) => Self::ED25519(k),
            PublicKey::SECP256K1(k) => Self::SECP256K1(k),
            PublicKey::MLDSA65(k) => Self::MlDsa65(k.to_public_key_handle()),
        }
    }
}

impl From<&PublicKey> for PublicKeyHandle {
    fn from(pk: &PublicKey) -> Self {
        match pk {
            PublicKey::ED25519(k) => Self::ED25519(k.clone()),
            PublicKey::SECP256K1(k) => Self::SECP256K1(k.clone()),
            PublicKey::MLDSA65(k) => Self::MlDsa65(k.to_public_key_handle()),
        }
    }
}

impl From<MlDsa65PublicKeyHandle> for PublicKeyHandle {
    fn from(h: MlDsa65PublicKeyHandle) -> Self {
        Self::MlDsa65(h)
    }
}

impl Display for PublicKeyHandle {
    fn fmt(&self, fmt: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::ED25519(k) => write!(fmt, "{}:{}", KeyType::ED25519, Bs58(&k.0[..])),
            Self::SECP256K1(k) => write!(fmt, "{}:{}", KeyType::SECP256K1, Bs58(&k.0[..])),
            Self::MlDsa65(h) => write!(fmt, "{ML_DSA_65_HASH_PREFIX}{}", Bs58(&h.0)),
        }
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for PublicKeyHandle {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "PublicKeyHandle".to_string().into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        String::json_schema(generator)
    }
}

impl Debug for PublicKeyHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        Display::fmt(self, f)
    }
}

impl FromStr for PublicKeyHandle {
    type Err = crate::errors::ParseKeyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if let Some(data) = value.strip_prefix(ML_DSA_65_HASH_PREFIX) {
            return Ok(Self::MlDsa65(MlDsa65PublicKeyHandle(try_fixed_array(
                &bs58::decode(data)
                    .into_vec()
                    .map_err(|err| Self::Err::InvalidData { error_message: err.to_string() })?,
            )?)));
        }
        let (key_type, key_data) = split_key_type_data(value)?;
        match key_type {
            KeyType::ED25519 => Ok(Self::ED25519(ED25519PublicKey(decode_bs58(key_data)?))),
            KeyType::SECP256K1 => {
                Ok(Self::SECP256K1(Secp256K1PublicKey::from(decode_bs58::<64>(key_data)?)))
            }
            // Full ML-DSA-65 keys never appear on the wire in this form -
            // they would be unrepresentable in `PublicKeyHandle`. The caller
            // should hash the pubkey first (via `From<&PublicKey>`) or
            // pass the `ml-dsa-65-hash:` form directly.
            KeyType::MLDSA65 => Err(Self::Err::InvalidData {
                error_message: "full ML-DSA-65 keys cannot appear in a PublicKeyHandle; \
                                use the `ml-dsa-65-hash:` form instead"
                    .to_string(),
            }),
        }
    }
}

impl serde::Serialize for PublicKeyHandle {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for PublicKeyHandle {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = <String as serde::Deserialize>::deserialize(d)?;
        s.parse()
            .map_err(|err: crate::errors::ParseKeyError| serde::de::Error::custom(err.to_string()))
    }
}

impl BorshSerialize for PublicKeyHandle {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        BorshSerialize::serialize(&(self.key_tag() as u8), writer)?;
        writer.write_all(self.key_data())?;
        Ok(())
    }
}

impl BorshDeserialize for PublicKeyHandle {
    fn deserialize_reader<R: Read>(rd: &mut R) -> std::io::Result<Self> {
        let raw_tag = u8::deserialize_reader(rd)?;
        let tag = raw_tag.try_into()?;
        match tag {
            KeyTag::Ed25519 => {
                Ok(Self::ED25519(ED25519PublicKey(BorshDeserialize::deserialize_reader(rd)?)))
            }
            KeyTag::Secp256k1 => Ok(Self::SECP256K1(Secp256K1PublicKey::from(
                <[u8; 64] as BorshDeserialize>::deserialize_reader(rd)?,
            ))),
            KeyTag::MlDsa65Full => Err(Error::new(
                ErrorKind::InvalidData,
                "borsh tag 2 is reserved for PublicKey (full ML-DSA-65 key); not a valid PublicKeyHandle",
            )),
            KeyTag::MlDsa65Hash => {
                let mut buf = [0u8; ML_DSA_65_HASH_LENGTH];
                rd.read_exact(&mut buf)?;
                Ok(Self::MlDsa65(MlDsa65PublicKeyHandle(buf)))
            }
        }
    }
}

#[derive(Clone, Eq)]
// This is actually a keypair, because ed25519_dalek api only has keypair.sign
// From ed25519_dalek doc: The first SECRET_KEY_LENGTH of bytes is the SecretKey
// The last PUBLIC_KEY_LENGTH of bytes is the public key, in total it's KEYPAIR_LENGTH
pub struct ED25519SecretKey(pub [u8; ed25519_dalek::KEYPAIR_LENGTH]);

impl PartialEq for ED25519SecretKey {
    fn eq(&self, other: &Self) -> bool {
        self.0[..ed25519_dalek::SECRET_KEY_LENGTH] == other.0[..ed25519_dalek::SECRET_KEY_LENGTH]
    }
}

impl std::fmt::Debug for ED25519SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        Display::fmt(&Bs58(&self.0[..ed25519_dalek::SECRET_KEY_LENGTH]), f)
    }
}

/// FIPS 204 ML-DSA-65 raw private key (4032 bytes).
#[derive(Clone, PartialEq, Eq)]
pub struct MlDsa65SecretKey(pub Box<[u8; ML_DSA_65_SECRET_KEY_LENGTH]>);

impl Debug for MlDsa65SecretKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        // Avoid printing key material; show only the type tag.
        f.write_str("MlDsa65SecretKey(<redacted>)")
    }
}

/// Secret key container supporting different curves.
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum SecretKey {
    ED25519(ED25519SecretKey),
    SECP256K1(secp256k1::SecretKey),
    MLDSA65(MlDsa65SecretKey),
}

impl SecretKey {
    pub fn key_type(&self) -> KeyType {
        match self {
            SecretKey::ED25519(_) => KeyType::ED25519,
            SecretKey::SECP256K1(_) => KeyType::SECP256K1,
            SecretKey::MLDSA65(_) => KeyType::MLDSA65,
        }
    }

    #[cfg(feature = "rand")]
    pub fn from_random(key_type: KeyType) -> SecretKey {
        use secp256k1::rand::rngs::OsRng;

        match key_type {
            KeyType::ED25519 => {
                let keypair = ed25519_dalek::SigningKey::generate(&mut OsRng);
                SecretKey::ED25519(ED25519SecretKey(keypair.to_keypair_bytes()))
            }
            KeyType::SECP256K1 => SecretKey::SECP256K1(secp256k1::SecretKey::new(&mut OsRng)),
            KeyType::MLDSA65 => {
                let kp =
                    PqdsaKeyPair::generate(&ML_DSA_65_SIGNING).expect("ML-DSA-65 keygen failed");
                SecretKey::MLDSA65(ml_dsa_65_secret_from_keypair(&kp))
            }
        }
    }

    pub fn sign(&self, data: &[u8]) -> Signature {
        match &self {
            SecretKey::ED25519(secret_key) => {
                let keypair = ed25519_dalek::SigningKey::from_keypair_bytes(&secret_key.0).unwrap();
                Signature::ED25519(keypair.sign(data))
            }

            SecretKey::SECP256K1(secret_key) => {
                let signature = SECP256K1.sign_ecdsa_recoverable(
                    &secp256k1::Message::from_slice(data).expect("32 bytes"),
                    secret_key,
                );
                let (rec_id, data) = signature.serialize_compact();
                let mut buf = [0; 65];
                buf[0..64].copy_from_slice(&data[0..64]);
                buf[64] = rec_id.to_i32() as u8;
                Signature::SECP256K1(Secp256K1Signature(buf))
            }

            SecretKey::MLDSA65(secret_key) => {
                let kp = PqdsaKeyPair::from_raw_private_key(&ML_DSA_65_SIGNING, &secret_key.0[..])
                    .expect("invalid ML-DSA-65 raw private key");
                let mut sig_buf = Box::new([0u8; ML_DSA_65_SIGNATURE_LENGTH]);
                let n = kp.sign(data, sig_buf.as_mut()).expect("ML-DSA-65 sign failed");
                debug_assert_eq!(n, ML_DSA_65_SIGNATURE_LENGTH);
                Signature::MLDSA65(MlDsa65Signature(sig_buf))
            }
        }
    }

    pub fn public_key(&self) -> PublicKey {
        match &self {
            SecretKey::ED25519(secret_key) => PublicKey::ED25519(ED25519PublicKey(
                secret_key.0[ed25519_dalek::SECRET_KEY_LENGTH..].try_into().unwrap(),
            )),
            SecretKey::SECP256K1(secret_key) => {
                let pk = secp256k1::PublicKey::from_secret_key(&SECP256K1, secret_key);
                let serialized = pk.serialize_uncompressed();
                let mut public_key = Secp256K1PublicKey([0; 64]);
                public_key.0.copy_from_slice(&serialized[1..65]);
                PublicKey::SECP256K1(public_key)
            }
            SecretKey::MLDSA65(secret_key) => {
                let kp = PqdsaKeyPair::from_raw_private_key(&ML_DSA_65_SIGNING, &secret_key.0[..])
                    .expect("invalid ML-DSA-65 raw private key");
                use aws_lc_rs::signature::KeyPair;
                let pk_bytes: &[u8] = kp.public_key().as_ref();
                let mut buf = Box::new([0u8; ML_DSA_65_PUBLIC_KEY_LENGTH]);
                buf.copy_from_slice(pk_bytes);
                PublicKey::MLDSA65(MlDsa65PublicKey(buf))
            }
        }
    }

    pub fn unwrap_as_ed25519(&self) -> &ED25519SecretKey {
        match self {
            SecretKey::ED25519(key) => key,
            SecretKey::SECP256K1(_) | SecretKey::MLDSA65(_) => panic!(),
        }
    }
}

/// Helper: extract the 4032-byte raw private key from a freshly-generated keypair.
#[cfg(feature = "rand")]
fn ml_dsa_65_secret_from_keypair(kp: &PqdsaKeyPair) -> MlDsa65SecretKey {
    use aws_lc_rs::encoding::{AsRawBytes, PqdsaPrivateKeyRaw};
    let raw: PqdsaPrivateKeyRaw<'static> =
        kp.private_key().as_raw_bytes().expect("ML-DSA-65 raw private export failed");
    let bytes: &[u8] = raw.as_ref();
    debug_assert_eq!(bytes.len(), ML_DSA_65_SECRET_KEY_LENGTH);
    let mut buf = Box::new([0u8; ML_DSA_65_SECRET_KEY_LENGTH]);
    buf.copy_from_slice(bytes);
    MlDsa65SecretKey(buf)
}

/// Helper: build an `MlDsa65SecretKey` from a 32-byte seed (deterministic).
#[cfg(feature = "rand")]
fn ml_dsa_65_secret_from_seed(
    seed: &[u8; ML_DSA_65_SEED_LENGTH],
) -> Result<MlDsa65SecretKey, crate::errors::ParseKeyError> {
    use aws_lc_rs::encoding::{AsRawBytes, PqdsaPrivateKeyRaw};
    let kp = PqdsaKeyPair::from_seed(&ML_DSA_65_SIGNING, &seed[..]).map_err(|err| {
        crate::errors::ParseKeyError::InvalidData { error_message: err.to_string() }
    })?;
    let raw: PqdsaPrivateKeyRaw<'static> = kp.private_key().as_raw_bytes().map_err(|err| {
        crate::errors::ParseKeyError::InvalidData { error_message: err.to_string() }
    })?;
    let bytes: &[u8] = raw.as_ref();
    let arr: [u8; ML_DSA_65_SECRET_KEY_LENGTH] = try_fixed_array(bytes)?;
    Ok(MlDsa65SecretKey(Box::new(arr)))
}

/// Build an [`MlDsa65SecretKey`] from a 32-byte seed.
///
/// Wraps `aws_lc_rs::unstable::signature::PqdsaKeyPair::from_seed`.
#[cfg(feature = "rand")]
pub fn ml_dsa_65_from_seed(
    seed: &[u8; ML_DSA_65_SEED_LENGTH],
) -> Result<SecretKey, crate::errors::ParseKeyError> {
    Ok(SecretKey::MLDSA65(ml_dsa_65_secret_from_seed(seed)?))
}

impl std::fmt::Display for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let (key_type, key_data) = match self {
            SecretKey::ED25519(secret_key) => (KeyType::ED25519, &secret_key.0[..]),
            SecretKey::SECP256K1(secret_key) => (KeyType::SECP256K1, &secret_key[..]),
            SecretKey::MLDSA65(secret_key) => (KeyType::MLDSA65, &secret_key.0[..]),
        };
        write!(f, "{}:{}", key_type, Bs58(key_data))
    }
}

impl FromStr for SecretKey {
    type Err = crate::errors::ParseKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (key_type, key_data) = split_key_type_data(s)?;
        Ok(match key_type {
            KeyType::ED25519 => Self::ED25519(ED25519SecretKey(decode_bs58(key_data)?)),
            KeyType::SECP256K1 => {
                let data = decode_bs58::<{ secp256k1::constants::SECRET_KEY_SIZE }>(key_data)?;
                let sk = secp256k1::SecretKey::from_slice(&data)
                    .map_err(|err| Self::Err::InvalidData { error_message: err.to_string() })?;
                Self::SECP256K1(sk)
            }
            KeyType::MLDSA65 => {
                let data = decode_bs58::<ML_DSA_65_SECRET_KEY_LENGTH>(key_data)?;
                // Mirror SECP256K1: validate the bytes form a valid
                // private key by handing them to the library. Catches
                // malformed-but-correct-length blobs at parse time
                // rather than blowing up later in `sign()`.
                PqdsaKeyPair::from_raw_private_key(&ML_DSA_65_SIGNING, &data[..])
                    .map_err(|err| Self::Err::InvalidData { error_message: err.to_string() })?;
                Self::MLDSA65(MlDsa65SecretKey(Box::new(data)))
            }
        })
    }
}

impl serde::Serialize for SecretKey {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> Result<<S as serde::Serializer>::Ok, <S as serde::Serializer>::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for SecretKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as serde::Deserializer<'de>>::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        Self::from_str(&s).map_err(|err| serde::de::Error::custom(err.to_string()))
    }
}

const SECP256K1_N: U256 =
    U256([0xbfd25e8cd0364141, 0xbaaedce6af48a03b, 0xfffffffffffffffe, 0xffffffffffffffff]);

// Half of SECP256K1_N + 1.
const SECP256K1_N_HALF_ONE: U256 =
    U256([0xdfe92f46681b20a1, 0x5d576e7357a4501d, 0xffffffffffffffff, 0x7fffffffffffffff]);

const SECP256K1_SIGNATURE_LENGTH: usize = 65;

#[derive(Clone, Eq, PartialEq, Hash, derive_more::From, derive_more::Into, ProtocolSchema)]
pub struct Secp256K1Signature([u8; SECP256K1_SIGNATURE_LENGTH]);

impl Secp256K1Signature {
    pub fn check_signature_values(&self, reject_upper: bool) -> bool {
        let mut r_bytes = [0u8; 32];
        r_bytes.copy_from_slice(&self.0[0..32]);
        let r = U256::from(r_bytes);

        let mut s_bytes = [0u8; 32];
        s_bytes.copy_from_slice(&self.0[32..64]);
        let s = U256::from(s_bytes);

        let s_check = if reject_upper {
            // Reject upper range of s values (ECDSA malleability)
            SECP256K1_N_HALF_ONE
        } else {
            SECP256K1_N
        };

        r < SECP256K1_N && s < s_check
    }

    pub fn recover(
        &self,
        msg: [u8; 32],
    ) -> Result<Secp256K1PublicKey, crate::errors::ParseSignatureError> {
        let recoverable_sig = secp256k1::ecdsa::RecoverableSignature::from_compact(
            &self.0[0..64],
            secp256k1::ecdsa::RecoveryId::from_i32(i32::from(self.0[64])).unwrap(),
        )
        .map_err(|err| crate::errors::ParseSignatureError::InvalidData {
            error_message: err.to_string(),
        })?;
        let msg = Message::from_slice(&msg).unwrap();

        let res = SECP256K1
            .recover_ecdsa(&msg, &recoverable_sig)
            .map_err(|err| crate::errors::ParseSignatureError::InvalidData {
                error_message: err.to_string(),
            })?
            .serialize_uncompressed();

        // Can not fail
        let pk = Secp256K1PublicKey::try_from(&res[1..65]).unwrap();

        Ok(pk)
    }
}

impl TryFrom<&[u8]> for Secp256K1Signature {
    type Error = crate::errors::ParseSignatureError;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self(try_fixed_array(data)?))
    }
}

impl Debug for Secp256K1Signature {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        Display::fmt(&Bs58(&self.0), f)
    }
}

/// FIPS 204 ML-DSA-65 signature (3309 bytes).
#[derive(Clone, Eq, PartialEq, Hash, derive_more::AsRef, ProtocolSchema)]
#[as_ref(forward)]
pub struct MlDsa65Signature(pub Box<[u8; ML_DSA_65_SIGNATURE_LENGTH]>);

impl TryFrom<&[u8]> for MlDsa65Signature {
    type Error = crate::errors::ParseSignatureError;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self(Box::new(try_fixed_array(data)?)))
    }
}

impl Debug for MlDsa65Signature {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        Display::fmt(&Bs58(&self.0[..]), f)
    }
}

/// Signature container supporting different curves.
#[derive(Clone, PartialEq, Eq, ProtocolSchema)]
pub enum Signature {
    ED25519(ed25519_dalek::Signature),
    SECP256K1(Secp256K1Signature),
    MLDSA65(MlDsa65Signature),
}

// This `Hash` implementation is safe since it retains the property
// `k1 == k2 ⇒ hash(k1) == hash(k2)`.
impl Hash for Signature {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Signature::ED25519(sig) => sig.to_bytes().hash(state),
            Signature::SECP256K1(sig) => sig.hash(state),
            Signature::MLDSA65(sig) => sig.hash(state),
        };
    }
}

impl Signature {
    /// Construct Signature from key type and raw signature blob
    pub fn from_parts(
        signature_type: KeyType,
        signature_data: &[u8],
    ) -> Result<Self, crate::errors::ParseSignatureError> {
        match signature_type {
            KeyType::ED25519 => Ok(Signature::ED25519(ed25519_dalek::Signature::from_bytes(
                <&[u8; ed25519_dalek::SIGNATURE_LENGTH]>::try_from(signature_data).map_err(
                    |err| crate::errors::ParseSignatureError::InvalidData {
                        error_message: err.to_string(),
                    },
                )?,
            ))),
            KeyType::SECP256K1 => {
                Ok(Signature::SECP256K1(Secp256K1Signature::try_from(signature_data).map_err(
                    |_| crate::errors::ParseSignatureError::InvalidData {
                        error_message: "invalid Secp256k1 signature length".to_string(),
                    },
                )?))
            }
            KeyType::MLDSA65 => Ok(Signature::MLDSA65(MlDsa65Signature::try_from(signature_data)?)),
        }
    }

    /// Verifies that this signature is indeed signs the data with given public key.
    /// Also if public key doesn't match on the curve returns `false`.
    pub fn verify(&self, data: &[u8], public_key: &PublicKey) -> bool {
        match (&self, public_key) {
            (Signature::ED25519(signature), PublicKey::ED25519(public_key)) => {
                match ed25519_dalek::VerifyingKey::from_bytes(&public_key.0) {
                    Err(_) => false,
                    Ok(public_key) => public_key.verify(data, signature).is_ok(),
                }
            }
            (Signature::SECP256K1(signature), PublicKey::SECP256K1(public_key)) => {
                // cspell:ignore rsig pdata
                let rec_id =
                    match secp256k1::ecdsa::RecoveryId::from_i32(i32::from(signature.0[64])) {
                        Ok(r) => r,
                        Err(_) => return false,
                    };
                let rsig = match secp256k1::ecdsa::RecoverableSignature::from_compact(
                    &signature.0[0..64],
                    rec_id,
                ) {
                    Ok(r) => r,
                    Err(_) => return false,
                };
                let sig = rsig.to_standard();
                let pdata: [u8; 65] = {
                    // code borrowed from https://github.com/openethereum/openethereum/blob/98b7c07171cd320f32877dfa5aa528f585dc9a72/ethkey/src/signature.rs#L210
                    let mut temp = [4u8; 65];
                    temp[1..65].copy_from_slice(&public_key.0);
                    temp
                };
                let message = match secp256k1::Message::from_slice(data) {
                    Ok(m) => m,
                    Err(_) => return false,
                };
                let pub_key = match secp256k1::PublicKey::from_slice(&pdata) {
                    Ok(p) => p,
                    Err(_) => return false,
                };
                SECP256K1.verify_ecdsa(&message, &sig, &pub_key).is_ok()
            }
            (Signature::MLDSA65(signature), PublicKey::MLDSA65(public_key)) => {
                let unparsed = UnparsedPublicKey::new(&ML_DSA_65, &public_key.0[..]);
                unparsed.verify(data, &signature.0[..]).is_ok()
            }
            _ => false,
        }
    }

    pub fn key_type(&self) -> KeyType {
        match self {
            Signature::ED25519(_) => KeyType::ED25519,
            Signature::SECP256K1(_) => KeyType::SECP256K1,
            Signature::MLDSA65(_) => KeyType::MLDSA65,
        }
    }
}

impl Default for Signature {
    fn default() -> Self {
        Signature::empty(KeyType::ED25519)
    }
}

impl BorshSerialize for Signature {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        match self {
            Signature::ED25519(signature) => {
                BorshSerialize::serialize(&0u8, writer)?;
                writer.write_all(&signature.to_bytes())?;
            }
            Signature::SECP256K1(signature) => {
                BorshSerialize::serialize(&1u8, writer)?;
                writer.write_all(&signature.0)?;
            }
            Signature::MLDSA65(signature) => {
                BorshSerialize::serialize(&2u8, writer)?;
                writer.write_all(&signature.0[..])?;
            }
        }
        Ok(())
    }
}

impl BorshDeserialize for Signature {
    fn deserialize_reader<R: Read>(rd: &mut R) -> std::io::Result<Self> {
        let key_type = KeyType::try_from(u8::deserialize_reader(rd)?)
            .map_err(|err| Error::new(ErrorKind::InvalidData, err.to_string()))?;
        match key_type {
            KeyType::ED25519 => {
                let array: [u8; ed25519_dalek::SIGNATURE_LENGTH] =
                    BorshDeserialize::deserialize_reader(rd)?;
                // Sanity-check that was performed by ed25519-dalek in from_bytes before version 2,
                // but was removed with version 2. It is not actually any good a check, but we have
                // it here in case we need to keep backward compatibility. Maybe this check is not
                // actually required, but please think carefully before removing it.
                if array[ed25519_dalek::SIGNATURE_LENGTH - 1] & 0b1110_0000 != 0 {
                    return Err(Error::new(ErrorKind::InvalidData, "signature error"));
                }
                Ok(Signature::ED25519(ed25519_dalek::Signature::from_bytes(&array)))
            }
            KeyType::SECP256K1 => {
                let array: [u8; 65] = BorshDeserialize::deserialize_reader(rd)?;
                Ok(Signature::SECP256K1(Secp256K1Signature(array)))
            }
            KeyType::MLDSA65 => {
                let mut buf = Box::new([0u8; ML_DSA_65_SIGNATURE_LENGTH]);
                rd.read_exact(buf.as_mut())?;
                Ok(Signature::MLDSA65(MlDsa65Signature(buf)))
            }
        }
    }
}

impl Display for Signature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let buf;
        let (key_type, key_data) = match self {
            Signature::ED25519(signature) => {
                buf = signature.to_bytes();
                (KeyType::ED25519, &buf[..])
            }
            Signature::SECP256K1(signature) => (KeyType::SECP256K1, &signature.0[..]),
            Signature::MLDSA65(signature) => (KeyType::MLDSA65, &signature.0[..]),
        };
        write!(f, "{}:{}", key_type, Bs58(&key_data))
    }
}

impl Debug for Signature {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        Display::fmt(self, f)
    }
}

impl serde::Serialize for Signature {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> Result<<S as serde::Serializer>::Ok, <S as serde::Serializer>::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl FromStr for Signature {
    type Err = crate::errors::ParseSignatureError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (sig_type, sig_data) = split_key_type_data(value)?;
        Ok(match sig_type {
            KeyType::ED25519 => {
                let data = decode_bs58::<{ ed25519_dalek::SIGNATURE_LENGTH }>(sig_data)?;
                let sig = ed25519_dalek::Signature::from_bytes(&data);
                Signature::ED25519(sig)
            }
            KeyType::SECP256K1 => Signature::SECP256K1(Secp256K1Signature(decode_bs58(sig_data)?)),
            KeyType::MLDSA65 => {
                let data = decode_bs58::<ML_DSA_65_SIGNATURE_LENGTH>(sig_data)?;
                Signature::MLDSA65(MlDsa65Signature(Box::new(data)))
            }
        })
    }
}

impl<'de> serde::Deserialize<'de> for Signature {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as serde::Deserializer<'de>>::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        s.parse().map_err(|err: crate::errors::ParseSignatureError| {
            serde::de::Error::custom(err.to_string())
        })
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for Signature {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Signature".to_string().into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        String::json_schema(generator)
    }
}

/// Helper struct which provides Display implementation for bytes slice
/// encoding them using base58.
// TODO(mina86): Get rid of it once bs58 has this feature.  There’s currently PR
// for that: https://github.com/Nullus157/bs58-rs/pull/97
struct Bs58<'a>(&'a [u8]);

impl<'a> core::fmt::Display for Bs58<'a> {
    fn fmt(&self, fmt: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // Base58 inflates input by less than 40%; round up generously and add
        // a small fixed overhead for short inputs.  This avoids stack-buffer
        // assumptions that broke for ML-DSA-65 keys/signatures (>1 KiB).
        let buf_len = self.0.len().saturating_mul(2).saturating_add(8);
        let mut buf = vec![0u8; buf_len];
        let len = bs58::encode(self.0).into(&mut buf[..]).unwrap();
        let output = &buf[..len];
        // SAFETY: we know that alphabet can only include ASCII characters
        // thus our result is an ASCII string.
        fmt.write_str(unsafe { std::str::from_utf8_unchecked(output) })
    }
}

/// Helper which decodes fixed-length base58-encoded data.
///
/// If the encoded string decodes into a buffer of different length than `N`,
/// returns error.  Similarly returns error if decoding fails.
fn decode_bs58<const N: usize>(encoded: &str) -> Result<[u8; N], DecodeBs58Error> {
    let mut buffer = [0u8; N];
    decode_bs58_impl(&mut buffer[..], encoded)?;
    Ok(buffer)
}

fn decode_bs58_impl(dst: &mut [u8], encoded: &str) -> Result<(), DecodeBs58Error> {
    let expected = dst.len();
    match bs58::decode(encoded).into(dst) {
        Ok(received) if received == expected => Ok(()),
        Ok(received) => Err(DecodeBs58Error::BadLength { expected, received }),
        Err(bs58::decode::Error::BufferTooSmall) => {
            Err(DecodeBs58Error::BadLength { expected, received: expected.saturating_add(1) })
        }
        Err(err) => Err(DecodeBs58Error::BadData(err.to_string())),
    }
}

enum DecodeBs58Error {
    BadLength { expected: usize, received: usize },
    BadData(String),
}

impl std::convert::From<DecodeBs58Error> for crate::errors::ParseKeyError {
    fn from(err: DecodeBs58Error) -> Self {
        match err {
            DecodeBs58Error::BadLength { expected, received } => {
                crate::errors::InvalidLength { expected, received }.into()
            }
            DecodeBs58Error::BadData(error_message) => Self::InvalidData { error_message },
        }
    }
}

impl std::convert::From<DecodeBs58Error> for crate::errors::ParseSignatureError {
    fn from(err: DecodeBs58Error) -> Self {
        match err {
            DecodeBs58Error::BadLength { expected, received } => {
                crate::errors::InvalidLength { expected, received }.into()
            }
            DecodeBs58Error::BadData(error_message) => Self::InvalidData { error_message },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{KeyType, PublicKey, SecretKey, Signature};

    #[cfg(feature = "rand")]
    #[test]
    fn test_sign_verify() {
        for key_type in [KeyType::ED25519, KeyType::SECP256K1, KeyType::MLDSA65] {
            let secret_key = SecretKey::from_random(key_type);
            let public_key = secret_key.public_key();
            use sha2::Digest;
            let data = sha2::Sha256::digest(b"123").to_vec();
            let signature = secret_key.sign(&data);
            assert!(signature.verify(&data, &public_key));
        }
    }

    #[test]
    fn signature_verify_fuzzer() {
        bolero::check!().with_type().for_each(
            |(key_type, sign, data, public_key): &(KeyType, [u8; 65], Vec<u8>, PublicKey)| {
                let signature = match key_type {
                    KeyType::ED25519 => {
                        Signature::from_parts(KeyType::ED25519, &sign[..64]).unwrap()
                    }
                    KeyType::SECP256K1 => {
                        Signature::from_parts(KeyType::SECP256K1, &sign[..65]).unwrap()
                    }
                    // ML-DSA-65 signatures are 3309 bytes - the bolero
                    // [u8; 65] generator above can't construct one, so we
                    // exercise this variant in dedicated tests instead.
                    KeyType::MLDSA65 => return,
                };
                let _ = signature.verify(&data, &public_key);
            },
        );
    }

    #[test]
    fn regression_signature_verification_originally_failed() {
        let signature = Signature::from_parts(KeyType::SECP256K1, &[4; 65]).unwrap();
        let _ = signature.verify(&[], &PublicKey::empty(KeyType::SECP256K1));
    }

    #[cfg(feature = "rand")]
    #[test]
    fn test_json_serialize_ed25519() {
        let sk = SecretKey::from_seed(KeyType::ED25519, "test");
        let pk = sk.public_key();
        let expected = "\"ed25519:DcA2MzgpJbrUATQLLceocVckhhAqrkingax4oJ9kZ847\"";
        assert_eq!(serde_json::to_string(&pk).unwrap(), expected);
        assert_eq!(pk, serde_json::from_str(expected).unwrap());
        assert_eq!(
            pk,
            serde_json::from_str("\"DcA2MzgpJbrUATQLLceocVckhhAqrkingax4oJ9kZ847\"").unwrap()
        );
        let pk2: PublicKey = pk.to_string().parse().unwrap();
        assert_eq!(pk, pk2);

        let expected = "\"ed25519:3KyUuch8pYP47krBq4DosFEVBMR5wDTMQ8AThzM8kAEcBQEpsPdYTZ2FPX5ZnSoLrerjwg66hwwJaW1wHzprd5k3\"";
        assert_eq!(serde_json::to_string(&sk).unwrap(), expected);
        assert_eq!(sk, serde_json::from_str(expected).unwrap());

        let signature = sk.sign(b"123");
        let expected = "\"ed25519:3s1dvZdQtcAjBksMHFrysqvF63wnyMHPA4owNQmCJZ2EBakZEKdtMsLqrHdKWQjJbSRN6kRknN2WdwSBLWGCokXj\"";
        assert_eq!(serde_json::to_string(&signature).unwrap(), expected);
        assert_eq!(signature, serde_json::from_str(expected).unwrap());
        let signature_str: String = signature.to_string();
        let signature2: Signature = signature_str.parse().unwrap();
        assert_eq!(signature, signature2);
    }

    #[cfg(feature = "rand")]
    #[test]
    fn test_json_serialize_secp256k1() {
        use sha2::Digest;
        let data = sha2::Sha256::digest(b"123").to_vec();

        let sk = SecretKey::from_seed(KeyType::SECP256K1, "test");
        let pk = sk.public_key();
        // cspell:disable-next-line
        let expected = "\"secp256k1:5ftgm7wYK5gtVqq1kxMGy7gSudkrfYCbpsjL6sH1nwx2oj5NR2JktohjzB6fbEhhRERQpiwJcpwnQjxtoX3GS3cQ\"";
        assert_eq!(serde_json::to_string(&pk).unwrap(), expected);
        assert_eq!(pk, serde_json::from_str(expected).unwrap());
        let pk2: PublicKey = pk.to_string().parse().unwrap();
        assert_eq!(pk, pk2);

        let expected = "\"secp256k1:X4ETFKtQkSGVoZEnkn7bZ3LyajJaK2b3eweXaKmynGx\"";
        assert_eq!(serde_json::to_string(&sk).unwrap(), expected);
        assert_eq!(sk, serde_json::from_str(expected).unwrap());

        let signature = sk.sign(&data);
        let expected = "\"secp256k1:5N5CB9H1dmB9yraLGCo4ZCQTcF24zj4v2NT14MHdH3aVhRoRXrX3AhprHr2w6iXNBZDmjMS1Ntzjzq8Bv6iBvwth6\"";
        assert_eq!(serde_json::to_string(&signature).unwrap(), expected);
        assert_eq!(signature, serde_json::from_str(expected).unwrap());
        let signature_str: String = signature.to_string();
        let signature2: Signature = signature_str.parse().unwrap();
        assert_eq!(signature, signature2);
    }

    #[cfg(feature = "rand")]
    #[test]
    fn test_borsh_serialization() {
        use borsh::BorshDeserialize;
        use sha2::Digest;

        let data = sha2::Sha256::digest(b"123").to_vec();
        for key_type in [KeyType::ED25519, KeyType::SECP256K1, KeyType::MLDSA65] {
            let sk = SecretKey::from_seed(key_type, "test");
            let pk = sk.public_key();
            let bytes = borsh::to_vec(&pk).unwrap();
            assert_eq!(PublicKey::try_from_slice(&bytes).unwrap(), pk);

            let signature = sk.sign(&data);
            let bytes = borsh::to_vec(&signature).unwrap();
            assert_eq!(Signature::try_from_slice(&bytes).unwrap(), signature);

            assert!(PublicKey::try_from_slice(&[0]).is_err());
            assert!(Signature::try_from_slice(&[0]).is_err());
        }
    }

    #[test]
    fn test_invalid_data() {
        // cspell:disable-next-line
        let invalid = "\"secp256k1:2xVqteU8PWhadHTv99TGh3bSf\"";
        assert!(serde_json::from_str::<PublicKey>(invalid).is_err());
        assert!(serde_json::from_str::<SecretKey>(invalid).is_err());
        assert!(serde_json::from_str::<Signature>(invalid).is_err());
    }

    /// ML-DSA-65: borsh-serialized public key must be exactly
    /// `1 + ML_DSA_65_PUBLIC_KEY_LENGTH = 1953` bytes (one tag byte + the
    /// raw key bytes), and the leading tag must be `2`.
    #[cfg(feature = "rand")]
    #[test]
    fn test_ml_dsa_65_borsh_tag_and_length() {
        use super::ML_DSA_65_PUBLIC_KEY_LENGTH;
        let sk = SecretKey::from_seed(KeyType::MLDSA65, "tag-test");
        let pk = sk.public_key();
        let bytes = borsh::to_vec(&pk).unwrap();
        assert_eq!(bytes.len(), 1 + ML_DSA_65_PUBLIC_KEY_LENGTH);
        assert_eq!(bytes[0], 2u8, "ML-DSA-65 borsh tag must be 2");
    }

    /// Bs58 helper used to assume a 96-byte stack buffer (debug_assert on
    /// input length ≤ 65). ML-DSA-65 pubkeys are 1952 bytes; encoding them
    /// must work in both display and parse-roundtrip.
    #[cfg(feature = "rand")]
    #[test]
    fn test_ml_dsa_65_display_roundtrip() {
        let sk = SecretKey::from_seed(KeyType::MLDSA65, "display-test");
        let pk = sk.public_key();
        let s = pk.to_string();
        assert!(s.starts_with("ml-dsa-65:"));
        let pk2: PublicKey = s.parse().expect("parse roundtrip");
        assert_eq!(pk, pk2);

        use sha2::Digest;
        let data = sha2::Sha256::digest(b"display").to_vec();
        let sig = sk.sign(&data);
        let s = sig.to_string();
        assert!(s.starts_with("ml-dsa-65:"));
        let sig2: Signature = s.parse().expect("sig parse roundtrip");
        assert_eq!(sig, sig2);
    }

    /// Construct an ML-DSA-sized borsh blob with the ED25519 tag and
    /// confirm it is rejected. ED25519 reads a fixed 32-byte pubkey, so
    /// the remaining 1920 bytes are leftover and `try_from_slice` must
    /// fail with a "not all bytes read" error rather than silently
    /// decoding to a different variant.
    #[test]
    fn test_ml_dsa_65_tag_mismatch() {
        use borsh::BorshDeserialize;
        let mut buf = vec![0u8; 1 + super::ML_DSA_65_PUBLIC_KEY_LENGTH];
        buf[0] = 0u8;
        assert!(PublicKey::try_from_slice(&buf).is_err());
    }

    /// ML-DSA-65 verify must reject a signature produced by a different key.
    #[cfg(feature = "rand")]
    #[test]
    fn test_ml_dsa_65_wrong_key_rejected() {
        use sha2::Digest;
        let data = sha2::Sha256::digest(b"hello world").to_vec();
        let sk1 = SecretKey::from_seed(KeyType::MLDSA65, "alice");
        let sk2 = SecretKey::from_seed(KeyType::MLDSA65, "bob");
        let sig = sk1.sign(&data);
        assert!(sig.verify(&data, &sk1.public_key()), "should verify with own key");
        assert!(!sig.verify(&data, &sk2.public_key()), "must not verify with a different key");
    }

    /// Tampering with the message must invalidate the signature.
    #[cfg(feature = "rand")]
    #[test]
    fn test_ml_dsa_65_tampered_message_rejected() {
        use sha2::Digest;
        let data = sha2::Sha256::digest(b"original").to_vec();
        let sk = SecretKey::from_seed(KeyType::MLDSA65, "tamper-test");
        let pk = sk.public_key();
        let sig = sk.sign(&data);
        let tampered = sha2::Sha256::digest(b"tampered").to_vec();
        assert!(sig.verify(&data, &pk));
        assert!(!sig.verify(&tampered, &pk));
    }

    /// Tampering with the signature bytes must cause verify to return false.
    #[cfg(feature = "rand")]
    #[test]
    fn test_ml_dsa_65_tampered_signature_rejected() {
        use borsh::BorshDeserialize;
        use sha2::Digest;
        let data = sha2::Sha256::digest(b"sig-tamper").to_vec();
        let sk = SecretKey::from_seed(KeyType::MLDSA65, "sig-tamper-test");
        let pk = sk.public_key();
        let sig = sk.sign(&data);
        let mut sig_bytes = borsh::to_vec(&sig).unwrap();
        // Flip a byte mid-signature (skip tag at index 0).
        sig_bytes[100] ^= 0xff;
        let tampered_sig = Signature::try_from_slice(&sig_bytes).unwrap();
        assert!(!tampered_sig.verify(&data, &pk));
    }

    /// `from_seed` must be deterministic - same seed in, same key out.
    #[cfg(feature = "rand")]
    #[test]
    fn test_ml_dsa_65_from_seed_deterministic() {
        let sk1 = SecretKey::from_seed(KeyType::MLDSA65, "deterministic-seed");
        let sk2 = SecretKey::from_seed(KeyType::MLDSA65, "deterministic-seed");
        assert_eq!(sk1.public_key(), sk2.public_key());
    }

    /// Cross-scheme verification must always fail (signature.verify against a
    /// different-curve pubkey returns false, never panics).
    #[cfg(feature = "rand")]
    #[test]
    fn test_cross_scheme_verify_returns_false() {
        use sha2::Digest;
        let data = sha2::Sha256::digest(b"x-scheme").to_vec();
        let ed_sk = SecretKey::from_seed(KeyType::ED25519, "x");
        let pq_sk = SecretKey::from_seed(KeyType::MLDSA65, "x");

        let ed_sig = ed_sk.sign(&data);
        let pq_sig = pq_sk.sign(&data);

        // ed25519 sig against ML-DSA pubkey: false
        assert!(!ed_sig.verify(&data, &pq_sk.public_key()));
        // ML-DSA sig against ed25519 pubkey: false
        assert!(!pq_sig.verify(&data, &ed_sk.public_key()));
    }

    /// Verify pubkey/signature length invariants for ML-DSA-65.
    #[cfg(feature = "rand")]
    #[test]
    fn test_ml_dsa_65_byte_lengths() {
        use super::{ML_DSA_65_PUBLIC_KEY_LENGTH, ML_DSA_65_SIGNATURE_LENGTH};
        use sha2::Digest;
        let sk = SecretKey::from_seed(KeyType::MLDSA65, "len-test");
        let pk = sk.public_key();
        // PublicKey::len() includes the 1-byte type tag.
        assert_eq!(pk.len(), ML_DSA_65_PUBLIC_KEY_LENGTH + 1);
        let data = sha2::Sha256::digest(b"sized").to_vec();
        let sig = sk.sign(&data);
        // Signature serialized via borsh: 1-byte tag + signature bytes.
        let sig_bytes = borsh::to_vec(&sig).unwrap();
        assert_eq!(sig_bytes.len(), 1 + ML_DSA_65_SIGNATURE_LENGTH);
    }

    /// Truncated ML-DSA pubkey must fail to deserialize.
    #[test]
    fn test_ml_dsa_65_truncated_pubkey_rejected() {
        use borsh::BorshDeserialize;
        // Tag 2 + only half the payload.
        let mut buf = vec![0u8; 1 + super::ML_DSA_65_PUBLIC_KEY_LENGTH / 2];
        buf[0] = 2u8;
        let res = PublicKey::try_from_slice(&buf);
        assert!(res.is_err(), "truncated pubkey must be rejected");
    }

    /// Backwards-compatibility: an existing ed25519 borsh blob must still
    /// decode unchanged after the ML-DSA-65 variant was added.
    #[cfg(feature = "rand")]
    #[test]
    fn test_existing_ed25519_borsh_still_decodes() {
        use borsh::BorshDeserialize;
        let sk = SecretKey::from_seed(KeyType::ED25519, "bc-test");
        let pk = sk.public_key();
        let bytes = borsh::to_vec(&pk).unwrap();
        let pk2 = PublicKey::try_from_slice(&bytes).unwrap();
        assert_eq!(pk, pk2);
        assert!(matches!(pk2, PublicKey::ED25519(_)));
    }

    /// `MlDsa65PublicKey::to_public_key_handle()` must be deterministic, 32 bytes,
    /// and distinct between different keys.
    #[cfg(feature = "rand")]
    #[test]
    fn test_ml_dsa_65_pubkey_hash_deterministic() {
        let pk1 = SecretKey::from_seed(KeyType::MLDSA65, "hash-1").public_key();
        let pk2 = SecretKey::from_seed(KeyType::MLDSA65, "hash-2").public_key();
        let h1a = match &pk1 {
            PublicKey::MLDSA65(k) => k.to_public_key_handle(),
            _ => unreachable!(),
        };
        let h1b = match &pk1 {
            PublicKey::MLDSA65(k) => k.to_public_key_handle(),
            _ => unreachable!(),
        };
        let h2 = match &pk2 {
            PublicKey::MLDSA65(k) => k.to_public_key_handle(),
            _ => unreachable!(),
        };
        assert_eq!(h1a.0.len(), super::ML_DSA_65_HASH_LENGTH);
        assert_eq!(h1a, h1b, "hash must be deterministic for the same key");
        assert_ne!(h1a, h2, "different keys must produce different hashes");
    }

    /// `ml-dsa-65-hash:` must not be parseable as a `KeyType` and must not
    /// accidentally resolve to `KeyType::MLDSA65` despite the shared prefix.
    /// Likewise, `PublicKey::from_str("ml-dsa-65-hash:...")` must fail
    /// loudly - the hash form is a `PublicKeyHandle` concept only.
    #[test]
    fn test_ml_dsa_65_hash_prefix_not_a_key_type() {
        // `ml-dsa-65-hash` is not a valid KeyType discriminator.
        assert!("ml-dsa-65-hash".parse::<KeyType>().is_err());
        // `PublicKey::from_str` must reject the hash-form prefix (no
        // `MlDsa65`-shaped variant exists on PublicKey).
        assert!("ml-dsa-65-hash:abc".parse::<PublicKey>().is_err());
    }

    /// `PublicKeyHandle::MlDsa65` display/parse roundtrip.
    #[test]
    fn test_key_handle_hash_display_roundtrip() {
        use super::PublicKeyHandle;
        let hash = super::MlDsa65PublicKeyHandle([0xA5u8; super::ML_DSA_65_HASH_LENGTH]);
        let kh = PublicKeyHandle::MlDsa65(hash);
        let s = kh.to_string();
        assert!(s.starts_with("ml-dsa-65-hash:"));
        let kh2: PublicKeyHandle = s.parse().expect("parse roundtrip");
        assert_eq!(kh, kh2);
    }

    /// Borsh roundtrip of `PublicKeyHandle::MlDsa65`. Bytes must be tag 3
    /// followed by the 32-byte hash so that the trie encoding matches.
    #[test]
    fn test_key_handle_hash_borsh_roundtrip() {
        use super::PublicKeyHandle;
        use borsh::BorshDeserialize;
        let hash = super::MlDsa65PublicKeyHandle([0x5Au8; super::ML_DSA_65_HASH_LENGTH]);
        let kh = PublicKeyHandle::MlDsa65(hash);
        let bytes = borsh::to_vec(&kh).unwrap();
        assert_eq!(bytes.len(), 1 + super::ML_DSA_65_HASH_LENGTH);
        assert_eq!(bytes[0], 3u8);
        let kh2 = PublicKeyHandle::try_from_slice(&bytes).unwrap();
        assert_eq!(kh, kh2);
    }

    /// Tag reservation is enforced both ways: tag 3 (the on-trie ML-DSA-65
    /// hash) must never decode as a `PublicKey`, and tag 2 (the full
    /// ML-DSA-65 pubkey) must never decode as a `PublicKeyHandle`.
    #[cfg(feature = "rand")]
    #[test]
    fn test_reserved_tags_rejected_both_ways() {
        use super::{KeyTag, PublicKeyHandle};
        use borsh::BorshDeserialize;

        let full_key = SecretKey::from_seed(KeyType::MLDSA65, "x").public_key();
        let mut full_key_bytes = borsh::to_vec(&full_key).unwrap();
        let key_handle: PublicKeyHandle = full_key.into();
        let mut key_handle_bytes = borsh::to_vec(&key_handle).unwrap();

        // Swap the tags, both should fail to deserialize
        full_key_bytes[0] = KeyTag::MlDsa65Hash as u8;
        key_handle_bytes[0] = KeyTag::MlDsa65Full as u8;

        assert!(PublicKey::try_from_slice(&full_key_bytes).is_err());
        assert!(PublicKeyHandle::try_from_slice(&key_handle_bytes).is_err());
    }

    /// `PublicKeyHandle::trie_id_len` matches the hash size for ML-DSA-65 and
    /// the borsh length for the other schemes.
    #[cfg(feature = "rand")]
    #[test]
    fn test_key_handle_trie_id_len_per_scheme() {
        use super::PublicKeyHandle;
        let ed: PublicKeyHandle = SecretKey::from_seed(KeyType::ED25519, "x").public_key().into();
        let sk: PublicKeyHandle = SecretKey::from_seed(KeyType::SECP256K1, "x").public_key().into();
        let pq: PublicKeyHandle = SecretKey::from_seed(KeyType::MLDSA65, "x").public_key().into();
        assert_eq!(ed.trie_id_len(), 33);
        assert_eq!(sk.trie_id_len(), 65);
        assert_eq!(pq.trie_id_len(), 33); // 1 + 32
    }

    /// Backwards-compat: borsh-encoded `PublicKeyHandle::ED25519`/`SECP256K1`
    /// matches the borsh encoding of the corresponding `PublicKey`. This
    /// guarantees that switching `TrieKey::AccessKey` from `PublicKey` to
    /// `PublicKeyHandle` does NOT change the bytes written to the trie for
    /// existing ed25519/secp256k1 access keys.
    #[cfg(feature = "rand")]
    #[test]
    fn test_key_handle_backwards_compat_ed25519_secp256k1_bytes() {
        use super::PublicKeyHandle;
        for key_type in [KeyType::ED25519, KeyType::SECP256K1] {
            let pk = SecretKey::from_seed(key_type, "bc").public_key();
            let pk_bytes = borsh::to_vec(&pk).unwrap();
            let kh: PublicKeyHandle = (&pk).into();
            let kh_bytes = borsh::to_vec(&kh).unwrap();
            assert_eq!(
                pk_bytes, kh_bytes,
                "PublicKeyHandle full-key encoding must match PublicKey encoding for {key_type:?}"
            );
        }
    }

    /// Known-Answer Test pinning the (seed → public key) mapping and the
    /// sign/verify round-trip on a fixed message. If `aws-lc-rs` ever
    /// changes the bytes it emits for ML-DSA-65 keygen, or makes verify
    /// reject something it used to accept (or vice versa), this test
    /// fails - preventing a silent fork between nodes on different
    /// `aws-lc-rs` versions.
    ///
    /// Only the public key is byte-pinned: ML-DSA-65 signatures *can*
    /// be non-deterministic in some library configurations, but verify
    /// across versions must always agree on the same (pk, msg, sig)
    /// triple. The round-trip below catches verify regressions.
    #[cfg(feature = "rand")]
    #[test]
    fn test_ml_dsa_65_known_answer() {
        const KAT_SEED: &str = "kat-seed-v1";
        const KAT_PUBKEY: &str = "ml-dsa-65:JX86tc6EwW1EFL5Q9B84bQPXeApzaVdJdog2uQNMsXpuwNKHPFozN1tpyhQj1btbSwGaJE6cqHdr8Y1Et6xPDHLyNmPhTKtmeX1YBe8LaQocTk9uYedMRGuhGQ7gJESeuDfuxtiz6C4chg6R1951dmCZN3hvdCwv4DojCt8w2Bj2TNES8tSqKAn9upkxVbbpx6SxGhxtbTreWKr2CGg6gJLMuZAoDsJd4yyw4gvYugoqPMUYUW1CGYLmMatX7NrbLpPbPDJriX92vWj9i4gNa4S3ZhrpjZEiN4zTZQ9DXb3bmU3yTkNhGYBSTyCB86FDjVzMpTMcN7u5XvP2usjZDoXz2iqP7iw9ZZDWqsJyQxsBZrUqrnG6m6vAAf8cEmoyv4mSYzFm3ABQ8fSvKxFaP5w1xE3jR274n5uj6f8AHEsZXLqLQB48LafYvew8vZKrtZoTnFjk58xkPHcg14HQ5GaRHrxwzSJCrZiQTUH9JB9otriiiBxtNhHH3t2bJZmMqYpoN7RYahMHETzq3DSk8HnJY8FFzEaR1BwHjszBxw2My7pPAgKrwq8ug1QarFP5AdfQvfVJYdQFbbpMCTPLZBP8aMqm2VGN453EBiua1PZDjhAWGDAVidnNZMDyYLyNjfwbbnFveXwwqj9o8Mg9g5A4PrqDUvnud5vdM41jdJh1Rik4qMbn6u1EBQwHcsKFJUWnoesTaxTHkLsSnAufFg5vJwYvEL7DBAToAw3wp5GwphjXPxWRw5zn2iaJXQVgo8VwLLgpwHoxXScw1hAuyDDkFTjdJJhKjAp9vzQC6MaerGjDAd2GnUh67LndncqDaVGP6RB4vowSXudZHNPdWtJN8aG1WWsrwbbLeBu3agyGi9S14ZBKUo5amRgvrSuoxUfFZtKn3Gs6rMf5CD3NBp5AeQLWXxE2mQydwhFphTMh6DkTpZmQ1pbuPA42MwhM87RvjmY5vJSWQcZB1afsn6ccjQTu83fvN8bX5gfkS3EWLd329nxdiYBJTF5ujbaTLwSK5DD45PyjVqZZqFHUJki5wwZ8TxpX9HZwzRfE6CZEw3y3mk4AA15jWhqokwnZvxG1dbYCYQfNXTR7TQXxfkMdTJ9xNNaZK6brbKT2pF4nA5iwBkdqXyKuGEgMfV5Tb9HeVoEc1dHqLUnmhe7bpRhgK7LtLAcEBYbajcPWLeDzvXVPS2nVXrg2nzTNz6N58QmFo22k54s8M6AKnipboKsDGZ7GN5f32JoMBkgD5ZrxLdMTtp2aWp3scGMrL6CLfN5uBLPhx92EhowFgB5jA4RsHTBPurT7h3rDCPRimYYYHQemdF1UF5Hkhdk3c2JsApXvMYd6Y5JEKYBUHN78ewK3pq3AvE9xRTXnxq6staTGpt7GD6e1EHPMjCe7JUuYoh3smdRrv2WoNXNhuehiQqCApdaqpyF7VJZfsMhVqNhLx9tUV4rKE9a1SMc4qDe2eN6SacepGQgXVopXxJWVShsap8z7Pa3eWQvfMNLA9V1jhDBCyijacZ53d3nUcHzLoaReu5MPsC9mnQ3rRdzjbG2CB4B2hzEgw3Y6ZrUquLyWoWvC7rzxVteg6yN3fwg9NpX866S8nRFEGg5XYjyMFK7Vuv6dnmfBNH6cPW289wRGecaLiwYP7pgQo9WBjPdHJtjb95Wbj95QttsTpT7CX5isFy6ZAoHqQpFLBiEcBAymUac7XvV8qJXctoVpaaySj9zrsHfMwnXyppgTyBKzc2pa4zTqbd9rW9QjgPdvD6aovN183stH2kFVsXFbYXryLDE3P7gieMwcuwC9EAUvvRddT2G1ntQxeWxcYFoeSVgRNz8x4cFZFHXG77LjuUdWLjR7niLS9wDVwbtvc9koEsPo9Qt3ByZPimFpcYzPfwQgovWPYfADJdGfG5rLNK9vWghZBqz6JCVASgZ3wCP2oj3ZVdFicoshvpEMxX29qfSa81tGWTQbAxd33GL9vD53suGqU5L1UbGGe9WZGC1S9QhS6Wf3jn9JM9fhUaMr5hMRJuVvSrJuS2fPPT5SDRv4fLQxGVx36sTDdU33Kc85t4H5BpYydqz2YMu7zZZQs8jS8UEcRXcxdJ6qKvoG8duHbEQzKLwbozQvqXufHuNQSjEDnRj8WAM6TC86SkgPpnVV1bDy6LAxZiny2nrNjhrpSS8GFd4LipuVApUqHTc2D5GRacGL8uGMz2QaZDb9EbdLsQyv8sZSGpswoyc5zWZfWGgk2LyQvm16TcL6spPzq6fAY9GQVcHdkT8r8tdojGTAsD5WXGkKrJwQtGZVm47ph1Sg31Q2wYmCEzm67irqTmmzibR22QaQZFMhcNNFU5baNYe1R21oU1ZffewiU27cPXwrTbM42eNdnWfcxCA7PAKU3C7xZhUUQzcQPwf2oFhiNmactJ4ZQKq4UWfcitYfATtq1wbA35Mm9Hu22jq2EatzcHGJySdUvczsyAebGCT5ASfVy6JMiL1kXP5UaZZ4moYMnDGCpZCo78XPgdtwZvqS1bDs8Eg5XkZn93Z9MnfTvVDi9w9A9jex4i3yXR69SpuSaHvnnUPSgvbcHjjHXr9yNdeYMqTN751i3MBoNE5qL4HqkovvMiepHsSes66j26UQHb8fNJ4YJQpZtY6tnq1DVS9Yaie6RugDdf8t6h";
        const KAT_MESSAGE: &[u8] = b"kat-message-v1";

        let sk = SecretKey::from_seed(KeyType::MLDSA65, KAT_SEED);
        let pk = sk.public_key();
        assert_eq!(
            pk.to_string(),
            KAT_PUBKEY,
            "seed → pubkey mapping changed; possible aws-lc-rs upgrade fork"
        );
        let sig = sk.sign(KAT_MESSAGE);
        assert!(sig.verify(KAT_MESSAGE, &pk), "self-produced signature must verify");
    }
}

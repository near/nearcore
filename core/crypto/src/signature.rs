use std::cmp::Ordering;
use std::convert::AsRef;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::io::{Error, ErrorKind, Write};
use std::str::FromStr;

use borsh::{BorshDeserialize, BorshSerialize};
use ed25519_dalek::ed25519::signature::{Signer, Verifier};
use once_cell::sync::Lazy;
use primitive_types::U256;
use rand::rngs::OsRng;
use secp256k1::Message;
use serde::{Deserialize, Serialize};

pub static SECP256K1: Lazy<secp256k1::Secp256k1<secp256k1::All>> =
    Lazy::new(secp256k1::Secp256k1::new);

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum KeyType {
    ED25519 = 0,
    SECP256K1 = 1,
}

impl Display for KeyType {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "{}",
            match self {
                KeyType::ED25519 => "ed25519",
                KeyType::SECP256K1 => "secp256k1",
            },
        )
    }
}

impl FromStr for KeyType {
    type Err = crate::errors::ParseKeyTypeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let lowercase_key_type = value.to_ascii_lowercase();
        match lowercase_key_type.as_str() {
            "ed25519" => Ok(KeyType::ED25519),
            "secp256k1" => Ok(KeyType::SECP256K1),
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
            unknown_key_type => {
                Err(Self::Error::UnknownKeyType { unknown_key_type: unknown_key_type.to_string() })
            }
        }
    }
}

fn split_key_type_data(value: &str) -> Result<(KeyType, &str), crate::errors::ParseKeyTypeError> {
    if let Some(idx) = value.find(':') {
        let (prefix, key_data) = value.split_at(idx);
        Ok((KeyType::from_str(prefix)?, &key_data[1..]))
    } else {
        // If there is no prefix then we Default to ED25519.
        Ok((KeyType::ED25519, value))
    }
}

#[derive(Clone)]
pub struct Secp256K1PublicKey([u8; 64]);

#[cfg(feature = "deepsize_feature")]
impl deepsize::DeepSizeOf for Secp256K1PublicKey {
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        0
    }
}

impl From<[u8; 64]> for Secp256K1PublicKey {
    fn from(data: [u8; 64]) -> Self {
        Self(data)
    }
}

impl TryFrom<&[u8]> for Secp256K1PublicKey {
    type Error = crate::errors::ParseKeyError;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        // It is suboptimal, but optimized implementation in Rust standard
        // library only implements TryFrom for arrays up to 32 elements at
        // the moment. Once https://github.com/rust-lang/rust/pull/74254
        // lands, we can use the following impl:
        //
        // Ok(Self(data.try_into().map_err(|_| TryFromSliceError(()))?))
        if data.len() != 64 {
            return Err(Self::Error::InvalidLength {
                expected_length: 64,
                received_length: data.len(),
            });
        }
        let mut public_key = Self([0; 64]);
        public_key.0.copy_from_slice(data);
        Ok(public_key)
    }
}

impl AsRef<[u8]> for Secp256K1PublicKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for Secp256K1PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", bs58::encode(&self.0.to_vec()).into_string())
    }
}

impl From<Secp256K1PublicKey> for [u8; 64] {
    fn from(pubkey: Secp256K1PublicKey) -> Self {
        pubkey.0
    }
}

impl PartialEq for Secp256K1PublicKey {
    fn eq(&self, other: &Self) -> bool {
        self.0[..] == other.0[..]
    }
}

impl PartialOrd for Secp256K1PublicKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0[..].partial_cmp(&other.0[..])
    }
}

impl Eq for Secp256K1PublicKey {}

impl Ord for Secp256K1PublicKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0[..].cmp(&other.0[..])
    }
}

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Clone, derive_more::AsRef)]
#[as_ref(forward)]
pub struct ED25519PublicKey(pub [u8; ed25519_dalek::PUBLIC_KEY_LENGTH]);

impl From<[u8; ed25519_dalek::PUBLIC_KEY_LENGTH]> for ED25519PublicKey {
    fn from(data: [u8; ed25519_dalek::PUBLIC_KEY_LENGTH]) -> Self {
        Self(data)
    }
}

impl TryFrom<&[u8]> for ED25519PublicKey {
    type Error = crate::errors::ParseKeyError;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self(data.try_into().map_err(|_| crate::errors::ParseKeyError::InvalidLength {
            expected_length: ed25519_dalek::PUBLIC_KEY_LENGTH,
            received_length: data.len(),
        })?))
    }
}

impl std::fmt::Debug for ED25519PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", bs58::encode(&self.0.to_vec()).into_string())
    }
}

impl PartialEq for ED25519PublicKey {
    fn eq(&self, other: &Self) -> bool {
        self.0[..] == other.0[..]
    }
}

impl PartialOrd for ED25519PublicKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0[..].partial_cmp(&other.0[..])
    }
}

impl Eq for ED25519PublicKey {}

impl Ord for ED25519PublicKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0[..].cmp(&other.0[..])
    }
}

/// Public key container supporting different curves.
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Clone, PartialEq, PartialOrd, Ord, Eq)]
pub enum PublicKey {
    /// 256 bit elliptic curve based public-key.
    ED25519(ED25519PublicKey),
    /// 512 bit elliptic curve based public-key used in Bitcoin's public-key cryptography.
    SECP256K1(Secp256K1PublicKey),
}

impl PublicKey {
    pub fn len(&self) -> usize {
        match self {
            Self::ED25519(_) => ed25519_dalek::PUBLIC_KEY_LENGTH + 1,
            Self::SECP256K1(_) => 65,
        }
    }

    pub fn empty(key_type: KeyType) -> Self {
        match key_type {
            KeyType::ED25519 => {
                PublicKey::ED25519(ED25519PublicKey([0u8; ed25519_dalek::PUBLIC_KEY_LENGTH]))
            }
            KeyType::SECP256K1 => PublicKey::SECP256K1(Secp256K1PublicKey([0u8; 64])),
        }
    }

    pub fn key_type(&self) -> KeyType {
        match self {
            Self::ED25519(_) => KeyType::ED25519,
            Self::SECP256K1(_) => KeyType::SECP256K1,
        }
    }

    pub fn key_data(&self) -> &[u8] {
        match self {
            Self::ED25519(key) => key.as_ref(),
            Self::SECP256K1(key) => key.as_ref(),
        }
    }

    pub fn unwrap_as_ed25519(&self) -> &ED25519PublicKey {
        match self {
            Self::ED25519(key) => key,
            Self::SECP256K1(_) => panic!(),
        }
    }
}

// This `Hash` implementation is safe since it retains the property
// `k1 == k2 ⇒ hash(k1) == hash(k2)`.
#[allow(clippy::derive_hash_xor_eq)]
impl Hash for PublicKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            PublicKey::ED25519(public_key) => {
                state.write_u8(0u8);
                state.write(&public_key.0);
            }
            PublicKey::SECP256K1(public_key) => {
                state.write_u8(1u8);
                state.write(&public_key.0);
            }
        }
    }
}

impl Display for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", String::from(self))
    }
}

impl Debug for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", String::from(self))
    }
}

impl BorshSerialize for PublicKey {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        match self {
            PublicKey::ED25519(public_key) => {
                BorshSerialize::serialize(&0u8, writer)?;
                writer.write_all(&public_key.0)?;
            }
            PublicKey::SECP256K1(public_key) => {
                BorshSerialize::serialize(&1u8, writer)?;
                writer.write_all(&public_key.0)?;
            }
        }
        Ok(())
    }
}

impl BorshDeserialize for PublicKey {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, Error> {
        let key_type = KeyType::try_from(<u8 as BorshDeserialize>::deserialize(buf)?)
            .map_err(|err| Error::new(ErrorKind::InvalidData, err.to_string()))?;
        match key_type {
            KeyType::ED25519 => {
                Ok(PublicKey::ED25519(ED25519PublicKey(BorshDeserialize::deserialize(buf)?)))
            }
            KeyType::SECP256K1 => {
                Ok(PublicKey::SECP256K1(Secp256K1PublicKey(BorshDeserialize::deserialize(buf)?)))
            }
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
        serializer.serialize_str(&String::from(self))
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

impl From<&PublicKey> for String {
    fn from(public_key: &PublicKey) -> Self {
        match public_key {
            PublicKey::ED25519(public_key) => {
                format!("{}:{}", KeyType::ED25519, bs58::encode(&public_key.0).into_string())
            }
            PublicKey::SECP256K1(public_key) => format!(
                "{}:{}",
                KeyType::SECP256K1,
                bs58::encode(&public_key.0.to_vec()).into_string()
            ),
        }
    }
}

impl FromStr for PublicKey {
    type Err = crate::errors::ParseKeyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (key_type, key_data) = split_key_type_data(value)?;
        match key_type {
            KeyType::ED25519 => {
                let mut array = [0; ed25519_dalek::PUBLIC_KEY_LENGTH];
                let length = bs58::decode(key_data)
                    .into(&mut array)
                    .map_err(|err| Self::Err::InvalidData { error_message: err.to_string() })?;
                if length != ed25519_dalek::PUBLIC_KEY_LENGTH {
                    return Err(Self::Err::InvalidLength {
                        expected_length: ed25519_dalek::PUBLIC_KEY_LENGTH,
                        received_length: length,
                    });
                }
                Ok(PublicKey::ED25519(ED25519PublicKey(array)))
            }
            KeyType::SECP256K1 => {
                let mut array = [0; 64];
                let length = bs58::decode(key_data)
                    .into(&mut array[..])
                    .map_err(|err| Self::Err::InvalidData { error_message: err.to_string() })?;
                if length != 64 {
                    return Err(Self::Err::InvalidLength {
                        expected_length: 64,
                        received_length: length,
                    });
                }
                Ok(PublicKey::SECP256K1(Secp256K1PublicKey(array)))
            }
        }
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

#[derive(Clone)]
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
        write!(
            f,
            "{}",
            bs58::encode(&self.0[..ed25519_dalek::SECRET_KEY_LENGTH].to_vec()).into_string()
        )
    }
}

impl Eq for ED25519SecretKey {}

/// Secret key container supporting different curves.
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum SecretKey {
    ED25519(ED25519SecretKey),
    SECP256K1(secp256k1::SecretKey),
}

impl SecretKey {
    pub fn key_type(&self) -> KeyType {
        match self {
            SecretKey::ED25519(_) => KeyType::ED25519,
            SecretKey::SECP256K1(_) => KeyType::SECP256K1,
        }
    }

    pub fn from_random(key_type: KeyType) -> SecretKey {
        match key_type {
            KeyType::ED25519 => {
                let keypair = ed25519_dalek::Keypair::generate(&mut OsRng);
                SecretKey::ED25519(ED25519SecretKey(keypair.to_bytes()))
            }
            KeyType::SECP256K1 => {
                SecretKey::SECP256K1(secp256k1::SecretKey::new(&mut secp256k1::rand::rngs::OsRng))
            }
        }
    }

    pub fn sign(&self, data: &[u8]) -> Signature {
        match &self {
            SecretKey::ED25519(secret_key) => {
                let keypair = ed25519_dalek::Keypair::from_bytes(&secret_key.0).unwrap();
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
        }
    }

    pub fn unwrap_as_ed25519(&self) -> &ED25519SecretKey {
        match self {
            SecretKey::ED25519(key) => key,
            SecretKey::SECP256K1(_) => panic!(),
        }
    }
}

impl std::fmt::Display for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let data = match self {
            SecretKey::ED25519(secret_key) => bs58::encode(&secret_key.0[..]).into_string(),
            SecretKey::SECP256K1(secret_key) => bs58::encode(&secret_key[..]).into_string(),
        };
        write!(f, "{}:{}", self.key_type(), data)
    }
}

impl FromStr for SecretKey {
    type Err = crate::errors::ParseKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (key_type, key_data) = split_key_type_data(s)?;
        match key_type {
            KeyType::ED25519 => {
                let mut array = [0; ed25519_dalek::KEYPAIR_LENGTH];
                let length = bs58::decode(key_data)
                    .into(&mut array[..])
                    .map_err(|err| Self::Err::InvalidData { error_message: err.to_string() })?;
                if length != ed25519_dalek::KEYPAIR_LENGTH {
                    return Err(Self::Err::InvalidLength {
                        expected_length: ed25519_dalek::KEYPAIR_LENGTH,
                        received_length: length,
                    });
                }
                Ok(Self::ED25519(ED25519SecretKey(array)))
            }
            KeyType::SECP256K1 => {
                let mut array = [0; secp256k1::constants::SECRET_KEY_SIZE];
                let length = bs58::decode(key_data)
                    .into(&mut array[..])
                    .map_err(|err| Self::Err::InvalidData { error_message: err.to_string() })?;
                if length != secp256k1::constants::SECRET_KEY_SIZE {
                    return Err(Self::Err::InvalidLength {
                        expected_length: secp256k1::constants::SECRET_KEY_SIZE,
                        received_length: length,
                    });
                }
                Ok(Self::SECP256K1(
                    secp256k1::SecretKey::from_slice(&array)
                        .map_err(|err| Self::Err::InvalidData { error_message: err.to_string() })?,
                ))
            }
        }
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
        let data = match self {
            SecretKey::ED25519(secret_key) => bs58::encode(&secret_key.0[..]).into_string(),
            SecretKey::SECP256K1(secret_key) => bs58::encode(&secret_key[..]).into_string(),
        };
        serializer.serialize_str(&format!("{}:{}", self.key_type(), data))
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

#[derive(Clone, Hash)]
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

impl From<[u8; 65]> for Secp256K1Signature {
    fn from(data: [u8; 65]) -> Self {
        Self(data)
    }
}

impl TryFrom<&[u8]> for Secp256K1Signature {
    type Error = crate::errors::ParseSignatureError;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        // It is suboptimal, but optimized implementation in Rust standard
        // library only implements TryFrom for arrays up to 32 elements at
        // the moment. Once https://github.com/rust-lang/rust/pull/74254
        // lands, we can use the following impl:
        //
        // Ok(Self(data.try_into().map_err(|_| Self::Error::InvalidLength { expected_length: 65, received_length: data.len() })?))
        if data.len() != 65 {
            return Err(Self::Error::InvalidLength {
                expected_length: 65,
                received_length: data.len(),
            });
        }
        let mut signature = Self([0; 65]);
        signature.0.copy_from_slice(data);
        Ok(signature)
    }
}

impl Eq for Secp256K1Signature {}

impl PartialEq for Secp256K1Signature {
    fn eq(&self, other: &Self) -> bool {
        self.0[..].eq(&other.0[..])
    }
}

impl Debug for Secp256K1Signature {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", bs58::encode(&self.0.to_vec()).into_string())
    }
}

impl From<Secp256K1Signature> for [u8; 65] {
    fn from(sig: Secp256K1Signature) -> [u8; 65] {
        sig.0
    }
}

/// Signature container supporting different curves.
#[derive(Clone, PartialEq, Eq)]
pub enum Signature {
    ED25519(ed25519_dalek::Signature),
    SECP256K1(Secp256K1Signature),
}

#[cfg(feature = "deepsize_feature")]
impl deepsize::DeepSizeOf for Signature {
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        match self {
            Signature::ED25519(_) => ed25519_dalek::SIGNATURE_LENGTH,
            Signature::SECP256K1(_) => SECP256K1_SIGNATURE_LENGTH,
        }
    }
}

impl Hash for Signature {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Signature::ED25519(sig) => sig.to_bytes().hash(state),
            Signature::SECP256K1(sig) => sig.hash(state),
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
            KeyType::ED25519 => Ok(Signature::ED25519(
                ed25519_dalek::Signature::from_bytes(signature_data).map_err(|err| {
                    crate::errors::ParseSignatureError::InvalidData {
                        error_message: err.to_string(),
                    }
                })?,
            )),
            KeyType::SECP256K1 => {
                Ok(Signature::SECP256K1(Secp256K1Signature::try_from(signature_data).map_err(
                    |_| crate::errors::ParseSignatureError::InvalidData {
                        error_message: "invalid Secp256k1 signature length".to_string(),
                    },
                )?))
            }
        }
    }

    /// Verifies that this signature is indeed signs the data with given public key.
    /// Also if public key doesn't match on the curve returns `false`.
    pub fn verify(&self, data: &[u8], public_key: &PublicKey) -> bool {
        match (&self, public_key) {
            (Signature::ED25519(signature), PublicKey::ED25519(public_key)) => {
                match ed25519_dalek::PublicKey::from_bytes(&public_key.0) {
                    Err(_) => false,
                    Ok(public_key) => public_key.verify(data, signature).is_ok(),
                }
            }
            (Signature::SECP256K1(signature), PublicKey::SECP256K1(public_key)) => {
                let rsig = secp256k1::ecdsa::RecoverableSignature::from_compact(
                    &signature.0[0..64],
                    secp256k1::ecdsa::RecoveryId::from_i32(i32::from(signature.0[64])).unwrap(),
                )
                .unwrap();
                let sig = rsig.to_standard();
                let pdata: [u8; 65] = {
                    // code borrowed from https://github.com/openethereum/openethereum/blob/98b7c07171cd320f32877dfa5aa528f585dc9a72/ethkey/src/signature.rs#L210
                    let mut temp = [4u8; 65];
                    temp[1..65].copy_from_slice(&public_key.0);
                    temp
                };
                SECP256K1
                    .verify_ecdsa(
                        &secp256k1::Message::from_slice(data).expect("32 bytes"),
                        &sig,
                        &secp256k1::PublicKey::from_slice(&pdata).unwrap(),
                    )
                    .is_ok()
            }
            _ => false,
        }
    }

    pub fn key_type(&self) -> KeyType {
        match self {
            Signature::ED25519(_) => KeyType::ED25519,
            Signature::SECP256K1(_) => KeyType::SECP256K1,
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
        }
        Ok(())
    }
}

impl BorshDeserialize for Signature {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, Error> {
        let key_type = KeyType::try_from(<u8 as BorshDeserialize>::deserialize(buf)?)
            .map_err(|err| Error::new(ErrorKind::InvalidData, err.to_string()))?;
        match key_type {
            KeyType::ED25519 => {
                let array: [u8; ed25519_dalek::SIGNATURE_LENGTH] =
                    BorshDeserialize::deserialize(buf)?;
                Ok(Signature::ED25519(
                    ed25519_dalek::Signature::from_bytes(&array)
                        .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?,
                ))
            }
            KeyType::SECP256K1 => {
                let array: [u8; 65] = BorshDeserialize::deserialize(buf)?;
                Ok(Signature::SECP256K1(Secp256K1Signature(array)))
            }
        }
    }
}

impl Display for Signature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let data = match self {
            Signature::ED25519(signature) => {
                bs58::encode(&signature.to_bytes().to_vec()).into_string()
            }
            Signature::SECP256K1(signature) => bs58::encode(&signature.0[..]).into_string(),
        };
        write!(f, "{}", format!("{}:{}", self.key_type(), data))
    }
}

impl Debug for Signature {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self)
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
        serializer.serialize_str(&self.to_string())
    }
}

impl FromStr for Signature {
    type Err = crate::errors::ParseSignatureError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (sig_type, sig_data) = split_key_type_data(value)?;
        match sig_type {
            KeyType::ED25519 => {
                let mut array = [0; ed25519_dalek::SIGNATURE_LENGTH];
                let length = bs58::decode(sig_data)
                    .into(&mut array[..])
                    .map_err(|err| Self::Err::InvalidData { error_message: err.to_string() })?;
                if length != ed25519_dalek::SIGNATURE_LENGTH {
                    return Err(Self::Err::InvalidLength {
                        expected_length: ed25519_dalek::SIGNATURE_LENGTH,
                        received_length: length,
                    });
                }
                Ok(Signature::ED25519(
                    ed25519_dalek::Signature::from_bytes(&array)
                        .map_err(|err| Self::Err::InvalidData { error_message: err.to_string() })?,
                ))
            }
            KeyType::SECP256K1 => {
                let mut array = [0; 65];
                let length = bs58::decode(sig_data)
                    .into(&mut array[..])
                    .map_err(|err| Self::Err::InvalidData { error_message: err.to_string() })?;
                if length != 65 {
                    return Err(Self::Err::InvalidLength {
                        expected_length: 65,
                        received_length: length,
                    });
                }
                Ok(Signature::SECP256K1(Secp256K1Signature(array)))
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign_verify() {
        for key_type in vec![KeyType::ED25519, KeyType::SECP256K1] {
            let secret_key = SecretKey::from_random(key_type);
            let public_key = secret_key.public_key();
            use sha2::Digest;
            let data = sha2::Sha256::digest(b"123").to_vec();
            let signature = secret_key.sign(&data);
            assert!(signature.verify(&data, &public_key));
        }
    }

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

    #[test]
    fn test_json_serialize_secp256k1() {
        use sha2::Digest;
        let data = sha2::Sha256::digest(b"123").to_vec();

        let sk = SecretKey::from_seed(KeyType::SECP256K1, "test");
        let pk = sk.public_key();
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

    #[test]
    fn test_borsh_serialization() {
        use sha2::Digest;
        let data = sha2::Sha256::digest(b"123").to_vec();
        for key_type in vec![KeyType::ED25519, KeyType::SECP256K1] {
            let sk = SecretKey::from_seed(key_type, "test");
            let pk = sk.public_key();
            let bytes = pk.try_to_vec().unwrap();
            assert_eq!(PublicKey::try_from_slice(&bytes).unwrap(), pk);

            let signature = sk.sign(&data);
            let bytes = signature.try_to_vec().unwrap();
            assert_eq!(Signature::try_from_slice(&bytes).unwrap(), signature);

            assert!(PublicKey::try_from_slice(&[0]).is_err());
            assert!(Signature::try_from_slice(&[0]).is_err());
        }
    }

    #[test]
    fn test_invalid_data() {
        let invalid = "\"secp256k1:2xVqteU8PWhadHTv99TGh3bSf\"";
        assert!(serde_json::from_str::<PublicKey>(invalid).is_err());
        assert!(serde_json::from_str::<SecretKey>(invalid).is_err());
        assert!(serde_json::from_str::<Signature>(invalid).is_err());
    }
}

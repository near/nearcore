use std::cmp::Ordering;
use std::convert::{TryFrom, TryInto};
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::io::{Error, ErrorKind, Read, Write};

use borsh::{BorshDeserialize, BorshSerialize};
use rand::rngs::{OsRng, StdRng};
use rand::SeedableRng;
use serde_derive::{Deserialize, Serialize};

use lazy_static::lazy_static;

lazy_static! {
    pub static ref SECP256K1: secp256k1::Secp256k1 = secp256k1::Secp256k1::new();
}

#[derive(Debug, Serialize, Deserialize)]
pub enum KeyType {
    ED25519 = 0,
    SECP256K1 = 1,
}

impl Display for KeyType {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
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

impl TryFrom<String> for KeyType {
    type Error = Box<dyn std::error::Error>;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "ed25519" => Ok(KeyType::ED25519),
            "secp256k1" => Ok(KeyType::SECP256K1),
            _ => Err(format!("Unknown curve kind {}", value).into()),
        }
    }
}

impl TryFrom<u8> for KeyType {
    type Error = Box<dyn std::error::Error>;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(KeyType::ED25519),
            1 => Ok(KeyType::SECP256K1),
            _ => Err(format!("Unknown curve id {}", value).into()),
        }
    }
}

fn split_key_type_data(value: &str) -> Result<(KeyType, &str), Box<dyn std::error::Error>> {
    if let Some(idx) = value.find(':') {
        let (prefix, key_data) = value.split_at(idx);
        Ok((KeyType::try_from(prefix.to_string())?, &key_data[1..]))
    } else {
        // If there is no Default is ED25519.
        Ok((KeyType::ED25519, value))
    }
}

#[derive(Copy, Clone)]
pub struct Secp256K1PublicKey([u8; 64]);

impl std::fmt::Debug for Secp256K1PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", bs58::encode(&self.0.to_vec()).into_string())
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

#[derive(Copy, Clone)]
pub struct ED25519PublicKey(pub [u8; ed25519_dalek::PUBLIC_KEY_LENGTH]);

impl std::fmt::Debug for ED25519PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
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
#[derive(Clone, PartialEq, PartialOrd, Ord, Eq)]
pub enum PublicKey {
    ED25519(ED25519PublicKey),
    SECP256K1(Secp256K1PublicKey),
}

impl PublicKey {
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
            PublicKey::ED25519(_) => KeyType::ED25519,
            PublicKey::SECP256K1(_) => KeyType::SECP256K1,
        }
    }
}

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
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", String::from(self))
    }
}

impl Debug for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", String::from(self))
    }
}

impl BorshSerialize for PublicKey {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        match self {
            PublicKey::ED25519(public_key) => {
                0u8.serialize(writer)?;
                writer.write_all(&public_key.0)?;
            }
            PublicKey::SECP256K1(public_key) => {
                1u8.serialize(writer)?;
                writer.write_all(&public_key.0)?;
            }
        }
        Ok(())
    }
}

impl BorshDeserialize for PublicKey {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, Error> {
        let key_type = KeyType::try_from(u8::deserialize(reader)?)
            .map_err(|err| Error::new(ErrorKind::InvalidData, err.to_string()))?;
        match key_type {
            KeyType::ED25519 => {
                let mut array = [0; ed25519_dalek::PUBLIC_KEY_LENGTH];
                reader.read_exact(&mut array)?;
                Ok(PublicKey::ED25519(ED25519PublicKey(array)))
            }
            KeyType::SECP256K1 => {
                let mut array = [0; 64];
                reader.read_exact(&mut array)?;
                Ok(PublicKey::SECP256K1(Secp256K1PublicKey(array)))
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
        s.try_into()
            .map_err(|err: Box<dyn std::error::Error>| serde::de::Error::custom(err.to_string()))
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

impl TryFrom<String> for PublicKey {
    type Error = Box<dyn std::error::Error>;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl TryFrom<&str> for PublicKey {
    type Error = Box<dyn std::error::Error>;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let (key_type, key_data) = split_key_type_data(&value)?;
        match key_type {
            KeyType::ED25519 => {
                let mut array = [0; ed25519_dalek::PUBLIC_KEY_LENGTH];
                let length = bs58::decode(key_data).into(&mut array)?;
                if length != ed25519_dalek::PUBLIC_KEY_LENGTH {
                    return Err(format!("Invalid length {} of ED25519 public key", length).into());
                }
                Ok(PublicKey::ED25519(ED25519PublicKey(array)))
            }
            KeyType::SECP256K1 => {
                let mut array = [0; 64];
                let length = bs58::decode(key_data).into(&mut array[..])?;
                if length != 64 {
                    return Err(format!("Invalid length {} of SECP256K1 public key", length).into());
                }
                Ok(PublicKey::SECP256K1(Secp256K1PublicKey(array)))
            }
        }
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
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
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
    SECP256K1(secp256k1::key::SecretKey),
}

impl SecretKey {
    pub fn key_type(&self) -> KeyType {
        match self {
            SecretKey::ED25519(_) => KeyType::ED25519,
            SecretKey::SECP256K1(_) => KeyType::SECP256K1,
        }
    }

    pub fn from_random(key_type: KeyType) -> SecretKey {
        let mut rng = StdRng::from_rng(OsRng::default()).unwrap();
        match key_type {
            KeyType::ED25519 => {
                let keypair = ed25519_dalek::Keypair::generate(&mut rng);
                SecretKey::ED25519(ED25519SecretKey(keypair.to_bytes()))
            }
            KeyType::SECP256K1 => {
                SecretKey::SECP256K1(secp256k1::key::SecretKey::new(&SECP256K1, &mut rng))
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
                let signature = SECP256K1
                    .sign_recoverable(
                        &secp256k1::Message::from_slice(data).expect("32 bytes"),
                        secret_key,
                    )
                    .expect("Failed to sign");
                let (rec_id, data) = signature.serialize_compact(&SECP256K1);
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
                let pk =
                    secp256k1::key::PublicKey::from_secret_key(&SECP256K1, secret_key).unwrap();
                let serialized = pk.serialize_vec(&SECP256K1, false);
                let mut public_key = Secp256K1PublicKey([0; 64]);
                public_key.0.copy_from_slice(&serialized[1..65]);
                PublicKey::SECP256K1(public_key)
            }
        }
    }
}

impl std::fmt::Display for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        let data = match self {
            SecretKey::ED25519(secret_key) => bs58::encode(&secret_key.0[..]).into_string(),
            SecretKey::SECP256K1(secret_key) => bs58::encode(&secret_key[..]).into_string(),
        };
        write!(f, "{}:{}", self.key_type(), data)
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
        let (key_type, key_data) =
            split_key_type_data(&s).map_err(|err| serde::de::Error::custom(err.to_string()))?;
        match key_type {
            KeyType::ED25519 => {
                let mut array = [0; ed25519_dalek::KEYPAIR_LENGTH];
                let length = bs58::decode(key_data)
                    .into(&mut array[..])
                    .map_err(|err| serde::de::Error::custom(err.to_string()))?;
                if length != ed25519_dalek::KEYPAIR_LENGTH {
                    return Err(serde::de::Error::custom(format!(
                        "Invalid length {} of ED25519 secret key",
                        length
                    )));
                }
                Ok(SecretKey::ED25519(ED25519SecretKey(array)))
            }
            _ => {
                let mut array = [0; secp256k1::constants::SECRET_KEY_SIZE];
                let length = bs58::decode(key_data)
                    .into(&mut array[..])
                    .map_err(|err| serde::de::Error::custom(err.to_string()))?;
                if length != secp256k1::constants::SECRET_KEY_SIZE {
                    return Err(serde::de::Error::custom(format!(
                        "Invalid length {} of SECP256K1 secret key",
                        length
                    )));
                }
                Ok(SecretKey::SECP256K1(
                    secp256k1::key::SecretKey::from_slice(&SECP256K1, &array)
                        .map_err(|err| serde::de::Error::custom(err.to_string()))?,
                ))
            }
        }
    }
}

#[derive(Clone)]
pub struct Secp256K1Signature([u8; 65]);

impl Eq for Secp256K1Signature {}

impl PartialEq for Secp256K1Signature {
    fn eq(&self, other: &Self) -> bool {
        self.0[..].eq(&other.0[..])
    }
}

impl Debug for Secp256K1Signature {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", bs58::encode(&self.0.to_vec()).into_string())
    }
}

/// Signature container supporting different curves.
#[derive(Clone, PartialEq, Eq)]
pub enum Signature {
    ED25519(ed25519_dalek::Signature),
    SECP256K1(Secp256K1Signature),
}

impl Signature {
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
                let rsig = secp256k1::RecoverableSignature::from_compact(
                    &SECP256K1,
                    &signature.0[0..64],
                    secp256k1::RecoveryId::from_i32(i32::from(signature.0[64])).unwrap(),
                )
                .unwrap();
                let sig = rsig.to_standard(&SECP256K1);
                let pdata: [u8; 65] = {
                    // code borrowed from https://github.com/paritytech/parity-ethereum/blob/98b7c07171cd320f32877dfa5aa528f585dc9a72/ethkey/src/signature.rs#L210
                    let mut temp = [4u8; 65];
                    temp[1..65].copy_from_slice(&public_key.0);
                    temp
                };
                SECP256K1
                    .verify(
                        &secp256k1::Message::from_slice(data).expect("32 bytes"),
                        &sig,
                        &secp256k1::key::PublicKey::from_slice(&SECP256K1, &pdata).unwrap(),
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
                0u8.serialize(writer)?;
                writer.write_all(&signature.to_bytes())?;
            }
            Signature::SECP256K1(signature) => {
                1u8.serialize(writer)?;
                writer.write_all(&signature.0)?;
            }
        }
        Ok(())
    }
}

impl BorshDeserialize for Signature {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, Error> {
        let key_type = KeyType::try_from(u8::deserialize(reader)?)
            .map_err(|err| Error::new(ErrorKind::InvalidData, err.to_string()))?;
        match key_type {
            KeyType::ED25519 => {
                let mut array = [0; ed25519_dalek::SIGNATURE_LENGTH];
                reader.read_exact(&mut array)?;
                Ok(Signature::ED25519(
                    ed25519_dalek::Signature::from_bytes(&array)
                        .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?,
                ))
            }
            KeyType::SECP256K1 => {
                let mut array = [0; 65];
                reader.read_exact(&mut array)?;
                Ok(Signature::SECP256K1(Secp256K1Signature(array)))
            }
        }
    }
}

impl Display for Signature {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
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
        serializer.serialize_str(&format!("{}", self))
    }
}

impl<'de> serde::Deserialize<'de> for Signature {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as serde::Deserializer<'de>>::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        let (key_type, key_data) =
            split_key_type_data(&s).map_err(|err| serde::de::Error::custom(err.to_string()))?;
        match key_type {
            KeyType::ED25519 => {
                let mut array = [0; ed25519_dalek::SIGNATURE_LENGTH];
                let length = bs58::decode(key_data)
                    .into(&mut array[..])
                    .map_err(|err| serde::de::Error::custom(err.to_string()))?;
                if length != ed25519_dalek::SIGNATURE_LENGTH {
                    return Err(serde::de::Error::custom(format!(
                        "Invalid length {} of ED25519 signature",
                        length,
                    )));
                }
                Ok(Signature::ED25519(ed25519_dalek::Signature::from_bytes(&array).map_err(
                    |e| {
                        serde::de::Error::custom(format!(
                            "Invalid ED25519 signature: {}",
                            e.to_string(),
                        ))
                    },
                )?))
            }
            _ => {
                let mut array = [0; 65];
                let length = bs58::decode(key_data)
                    .into(&mut array[..])
                    .map_err(|err| serde::de::Error::custom(err.to_string()))?;
                if length != 65 {
                    return Err(serde::de::Error::custom(format!(
                        "Invalid length {} of SECP256K1 signature",
                        length
                    )));
                }
                Ok(Signature::SECP256K1(Secp256K1Signature(array)))
            }
        }
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

        let expected = "\"ed25519:3KyUuch8pYP47krBq4DosFEVBMR5wDTMQ8AThzM8kAEcBQEpsPdYTZ2FPX5ZnSoLrerjwg66hwwJaW1wHzprd5k3\"";
        assert_eq!(serde_json::to_string(&sk).unwrap(), expected);
        assert_eq!(sk, serde_json::from_str(expected).unwrap());

        let signature = sk.sign(b"123");
        let expected = "\"ed25519:3s1dvZdQtcAjBksMHFrysqvF63wnyMHPA4owNQmCJZ2EBakZEKdtMsLqrHdKWQjJbSRN6kRknN2WdwSBLWGCokXj\"";
        assert_eq!(serde_json::to_string(&signature).unwrap(), expected);
        assert_eq!(signature, serde_json::from_str(expected).unwrap());
    }

    #[test]
    fn test_json_serialize_secp256k1() {
        use sha2::Digest;
        let data = sha2::Sha256::digest(b"123").to_vec();

        let sk = SecretKey::from_seed(KeyType::SECP256K1, "test");
        let pk = sk.public_key();
        let expected = "\"secp256k1:BtJtBjukUQbcipnS78adSwUKE38sdHnk7pTNZH7miGXfodzUunaAcvY43y37nm7AKbcTQycvdgUzFNWsd7dgPZZ\"";
        assert_eq!(serde_json::to_string(&pk).unwrap(), expected);
        assert_eq!(pk, serde_json::from_str(expected).unwrap());

        let expected = "\"secp256k1:9ZNzLxNff6ohoFFGkbfMBAFpZgD7EPoWeiuTpPAeeMRV\"";
        assert_eq!(serde_json::to_string(&sk).unwrap(), expected);
        assert_eq!(sk, serde_json::from_str(expected).unwrap());

        let signature = sk.sign(&data);
        let expected = "\"secp256k1:7iA75xRmHw17MbUkSpHxBHFVTuJW6jngzbuJPJutwb3EAwVw21wrjpMHU7fFTAqH7D3YEma8utCdvdtsqcAWqnC7r\"";
        assert_eq!(serde_json::to_string(&signature).unwrap(), expected);
        assert_eq!(signature, serde_json::from_str(expected).unwrap());
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

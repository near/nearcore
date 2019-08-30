use std::convert::{TryFrom, TryInto};
use std::fmt::{Display, Formatter};
use std::io::{Error, ErrorKind, Read, Write};

use borsh::{BorshDeserialize, BorshSerialize};
use serde_derive::{Deserialize, Serialize};

/// Public key represented as string of format <curve>:<base58 of the key>.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct ReadablePublicKey(String);

impl ReadablePublicKey {
    pub fn new(s: &str) -> Self {
        Self { 0: s.to_string() }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum KeyType {
    ED25519 = 0,
    SEPK281 = 1,
}

impl Display for KeyType {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "{}",
            match self {
                KeyType::ED25519 => "ed25519",
                KeyType::SEPK281 => "sepk",
            },
        )
    }
}

impl TryFrom<String> for KeyType {
    type Error = Box<std::error::Error>;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "ed25519" => Ok(KeyType::ED25519),
            "sepk" => Ok(KeyType::SEPK281),
            _ => Err(format!("Unknown curve kind {}", value).into()),
        }
    }
}

impl TryFrom<u8> for KeyType {
    type Error = Box<std::error::Error>;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(KeyType::ED25519),
            1 => Ok(KeyType::SEPK281),
            _ => Err(format!("Unknown curve id {}", value).into()),
        }
    }
}

fn split_key_type_data(value: String) -> Result<(KeyType, String), Box<std::error::Error>> {
    if let Some(idx) = value.find(":") {
        let (prefix, key_data) = value.split_at(idx);
        Ok((KeyType::try_from(prefix.to_string())?, key_data[1..].to_string()))
    } else {
        // If there is no Default is ED25519.
        Ok((KeyType::ED25519, value))
    }
}

/// Public key container supporting different curves.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PublicKey {
    ED25519(sodiumoxide::crypto::sign::ed25519::PublicKey),
    SEPK281([u8; 32]),
}

impl PublicKey {
    pub fn empty(key_type: KeyType) -> Self {
        match key_type {
            KeyType::ED25519 => PublicKey::ED25519(sodiumoxide::crypto::sign::ed25519::PublicKey(
                [0u8; sodiumoxide::crypto::sign::PUBLICKEYBYTES],
            )),
            _ => unimplemented!(),
        }
    }

    pub fn key_type(&self) -> KeyType {
        match self {
            PublicKey::ED25519(_) => KeyType::ED25519,
            PublicKey::SEPK281(_) => KeyType::SEPK281,
        }
    }
}

impl Display for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", ReadablePublicKey::from(self.clone()).0)
    }
}

impl BorshSerialize for PublicKey {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        match self {
            PublicKey::ED25519(public_key) => {
                0u8.serialize(writer)?;
                writer.write(&public_key.0)?;
            }
            _ => unimplemented!(),
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
                let mut array = [0; sodiumoxide::crypto::sign::ed25519::PUBLICKEYBYTES];
                reader.read(&mut array)?;
                Ok(PublicKey::ED25519(sodiumoxide::crypto::sign::ed25519::PublicKey(array)))
            }
            _ => unimplemented!(),
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
        serializer.serialize_str(&ReadablePublicKey::from(self.clone()).0)
    }
}

impl<'de> serde::Deserialize<'de> for PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as serde::Deserializer<'de>>::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        ReadablePublicKey::new(&s.as_str())
            .try_into()
            .map_err(|err: Box<std::error::Error>| serde::de::Error::custom(err.to_string()))
    }
}
impl From<PublicKey> for ReadablePublicKey {
    fn from(public_key: PublicKey) -> Self {
        match public_key {
            PublicKey::ED25519(public_key) => ReadablePublicKey(format!(
                "{}:{}",
                KeyType::ED25519,
                bs58::encode(&public_key.0).into_string()
            )),
            _ => unimplemented!(),
        }
    }
}

impl TryFrom<ReadablePublicKey> for PublicKey {
    type Error = Box<std::error::Error>;

    fn try_from(value: ReadablePublicKey) -> Result<Self, Self::Error> {
        let (key_type, key_data) = split_key_type_data(value.0)?;
        match key_type {
            KeyType::ED25519 => {
                let mut array = [0; sodiumoxide::crypto::sign::ed25519::PUBLICKEYBYTES];
                bs58::decode(key_data).into(&mut array)?;
                Ok(PublicKey::ED25519(sodiumoxide::crypto::sign::ed25519::PublicKey(array)))
            }
            _ => unimplemented!(),
        }
    }
}

/// Secret key container supporting different curves.
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum SecretKey {
    ED25519(sodiumoxide::crypto::sign::ed25519::SecretKey),
    SEPK281([u8; 32]),
}

impl SecretKey {
    pub fn key_type(&self) -> KeyType {
        match self {
            SecretKey::ED25519(_) => KeyType::ED25519,
            SecretKey::SEPK281(_) => KeyType::SEPK281,
        }
    }

    pub fn from_random(key_type: KeyType) -> SecretKey {
        match key_type {
            KeyType::ED25519 => {
                let (_, secret_key) = sodiumoxide::crypto::sign::ed25519::gen_keypair();
                SecretKey::ED25519(secret_key)
            }
            _ => unimplemented!(),
        }
    }

    pub fn sign(&self, data: &[u8]) -> Signature {
        match &self {
            SecretKey::ED25519(secret_key) => Signature::ED25519(
                sodiumoxide::crypto::sign::ed25519::sign_detached(data, &secret_key),
            ),
            _ => unimplemented!(),
        }
    }

    pub fn public_key(&self) -> PublicKey {
        match &self {
            SecretKey::ED25519(secret_key) => {
                let mut seed = [0u8; sodiumoxide::crypto::sign::ed25519::SEEDBYTES];
                seed.copy_from_slice(
                    &secret_key.0[..sodiumoxide::crypto::sign::ed25519::SEEDBYTES],
                );
                let (public_key, _) = sodiumoxide::crypto::sign::ed25519::keypair_from_seed(
                    &sodiumoxide::crypto::sign::ed25519::Seed(seed),
                );
                PublicKey::ED25519(public_key)
            }
            _ => unimplemented!(),
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
            _ => unimplemented!(),
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
            split_key_type_data(s).map_err(|err| serde::de::Error::custom(err.to_string()))?;
        match key_type {
            KeyType::ED25519 => {
                let mut array = [0; sodiumoxide::crypto::sign::ed25519::SECRETKEYBYTES];
                bs58::decode(key_data)
                    .into(&mut array[..])
                    .map_err(|err| serde::de::Error::custom(err.to_string()))?;
                Ok(SecretKey::ED25519(sodiumoxide::crypto::sign::ed25519::SecretKey(array)))
            }
            _ => unimplemented!(),
        }
    }
}

/// Signature container supporting different curves.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Signature {
    ED25519(sodiumoxide::crypto::sign::ed25519::Signature),
    SEPK281([u8; 32]),
}

impl Signature {
    /// Verifies that this signature is indeed signs the data with given public key.
    /// Also if public key doesn't match on the curve returns `false`.
    pub fn verify(&self, data: &[u8], public_key: &PublicKey) -> bool {
        match (&self, public_key) {
            (Signature::ED25519(signature), PublicKey::ED25519(public_key)) => {
                sodiumoxide::crypto::sign::ed25519::verify_detached(signature, data, &public_key)
            }
            _ => unimplemented!(),
        }
    }

    pub fn key_type(&self) -> KeyType {
        match self {
            Signature::ED25519(_) => KeyType::ED25519,
            Signature::SEPK281(_) => KeyType::SEPK281,
        }
    }
}

impl BorshSerialize for Signature {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        match self {
            Signature::ED25519(signature) => {
                0u8.serialize(writer)?;
                writer.write(&signature.0)?;
            }
            _ => unimplemented!(),
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
                let mut array = [0; sodiumoxide::crypto::sign::ed25519::SIGNATUREBYTES];
                reader.read(&mut array)?;
                Ok(Signature::ED25519(sodiumoxide::crypto::sign::ed25519::Signature(array)))
            }
            _ => unimplemented!(),
        }
    }
}

impl Display for Signature {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        let data = match self {
            Signature::ED25519(signature) => bs58::encode(&signature.0[..]).into_string(),
            _ => unimplemented!(),
        };
        write!(f, "{}", format!("{}:{}", self.key_type(), data))
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
            split_key_type_data(s).map_err(|err| serde::de::Error::custom(err.to_string()))?;
        match key_type {
            KeyType::ED25519 => {
                let mut array = [0; sodiumoxide::crypto::sign::ed25519::SIGNATUREBYTES];
                bs58::decode(key_data)
                    .into(&mut array[..])
                    .map_err(|err| serde::de::Error::custom(err.to_string()))?;
                Ok(Signature::ED25519(sodiumoxide::crypto::sign::ed25519::Signature(array)))
            }
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ed25519_sign_verify() {
        let secret_key = SecretKey::from_random(KeyType::ED25519);
        let public_key = secret_key.public_key();
        let data = b"123";
        let signature = secret_key.sign(data);
        assert!(signature.verify(data, &public_key));
    }

    #[test]
    fn test_json_serialize() {
        let sk = SecretKey::from_seed(KeyType::ED25519, "test");
        let pk = sk.public_key();
        let expected = "\"ed25519:DcA2MzgpJbrUATQLLceocVckhhAqrkingax4oJ9kZ847\"";
        assert_eq!(serde_json::to_string(&pk).unwrap(), expected);
        let pk2 = serde_json::from_str(expected).unwrap();
        assert_eq!(pk, pk2);
        let pk3 = serde_json::from_str("\"DcA2MzgpJbrUATQLLceocVckhhAqrkingax4oJ9kZ847\"").unwrap();
        assert_eq!(pk, pk3);

        let expected = "\"ed25519:3KyUuch8pYP47krBq4DosFEVBMR5wDTMQ8AThzM8kAEcBQEpsPdYTZ2FPX5ZnSoLrerjwg66hwwJaW1wHzprd5k3\"";
        assert_eq!(serde_json::to_string(&sk).unwrap(), expected);
        let sk2 = serde_json::from_str(expected).unwrap();
        assert_eq!(sk, sk2);

        let signature = sk.sign(b"123");
        let expected = "\"ed25519:3s1dvZdQtcAjBksMHFrysqvF63wnyMHPA4owNQmCJZ2EBakZEKdtMsLqrHdKWQjJbSRN6kRknN2WdwSBLWGCokXj\"";
        assert_eq!(serde_json::to_string(&signature).unwrap(), expected);
        let signature2 = serde_json::from_str(expected).unwrap();
        assert_eq!(signature, signature2);
    }
}

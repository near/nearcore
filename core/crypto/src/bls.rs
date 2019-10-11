use std::cmp::Ordering;
use std::convert::TryFrom;
use std::io::{Error, ErrorKind, Read, Write};

use borsh::{BorshDeserialize, BorshSerialize};
use milagro_bls::AggregatePublicKey;

const BLS_DOMAIN: u64 = 42;
const BLS_PUBLIC_KEY_LENGTH: usize = 48;
const BLS_SECRET_KEY_LENGTH: usize = 48;
const BLS_SIGNATURE_LENGTH: usize = 96;

#[derive(Clone, PartialEq, Eq)]
pub struct BlsPublicKey(milagro_bls::PublicKey);

fn str_to_public_key(s: &str) -> Result<BlsPublicKey, String> {
    let mut array = [0; BLS_PUBLIC_KEY_LENGTH];
    let length = bs58::decode(s).into(&mut array[..]).map_err(|err| err.to_string())?;
    if length != BLS_PUBLIC_KEY_LENGTH {
        return Err(format!("Invalid length {} of BLS public key", length));
    }
    milagro_bls::PublicKey::from_bytes(&array).map(BlsPublicKey).map_err(|err| format!("{:?}", err))
}

impl BlsPublicKey {
    pub fn empty() -> Self {
        BlsSecretKey::from_seed("").public_key()
    }
}

impl PartialOrd for BlsPublicKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.as_bytes().partial_cmp(&other.0.as_bytes())
    }
}

impl Ord for BlsPublicKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.as_bytes().cmp(&other.0.as_bytes())
    }
}

impl std::fmt::Display for BlsPublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", bs58::encode(self.0.as_bytes()).into_string())
    }
}

impl std::fmt::Debug for BlsPublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", bs58::encode(self.0.as_bytes()).into_string())
    }
}

impl TryFrom<String> for BlsPublicKey {
    type Error = String;

    fn try_from(pk: String) -> Result<Self, Self::Error> {
        str_to_public_key(&pk)
    }
}

impl From<BlsPublicKey> for String {
    fn from(pk: BlsPublicKey) -> Self {
        bs58::encode(pk.0.as_bytes()).into_string()
    }
}

impl BorshSerialize for BlsPublicKey {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        writer.write(&self.0.as_bytes()).map(|_| ())
    }
}

impl BorshDeserialize for BlsPublicKey {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, Error> {
        let mut buf = [0; BLS_PUBLIC_KEY_LENGTH];
        reader.read_exact(&mut buf)?;
        milagro_bls::PublicKey::from_bytes(&buf)
            .map(BlsPublicKey)
            .map_err(|err| Error::new(ErrorKind::Other, format!("{:?}", err)))
    }
}

impl serde::Serialize for BlsPublicKey {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> Result<<S as serde::Serializer>::Ok, <S as serde::Serializer>::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&bs58::encode(self.0.as_bytes()).into_string())
    }
}

impl<'de> serde::Deserialize<'de> for BlsPublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as serde::Deserializer<'de>>::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        str_to_public_key(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BlsSecretKey(pub milagro_bls::SecretKey);

impl BlsSecretKey {
    pub fn from_random() -> BlsSecretKey {
        let sk = milagro_bls::SecretKey::random(&mut rand::thread_rng());
        BlsSecretKey(sk)
    }

    pub fn public_key(&self) -> BlsPublicKey {
        BlsPublicKey(milagro_bls::PublicKey::from_secret_key(&self.0))
    }

    pub fn sign(&self, data: &[u8]) -> BlsSignature {
        let mut agg_sig = milagro_bls::AggregateSignature::new();
        agg_sig.add(&milagro_bls::Signature::new(&data, BLS_DOMAIN, &self.0));
        BlsSignature(agg_sig)
    }
}

impl BorshSerialize for BlsSecretKey {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        writer.write(&self.0.as_bytes()).map(|_| ())
    }
}

impl BorshDeserialize for BlsSecretKey {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, Error> {
        let mut buf = [0; BLS_SECRET_KEY_LENGTH];
        reader.read_exact(&mut buf)?;
        milagro_bls::SecretKey::from_bytes(&buf)
            .map(BlsSecretKey)
            .map_err(|err| Error::new(ErrorKind::Other, format!("{:?}", err)))
    }
}

impl serde::Serialize for BlsSecretKey {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> Result<<S as serde::Serializer>::Ok, <S as serde::Serializer>::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&bs58::encode(self.0.as_bytes()).into_string())
    }
}

impl<'de> serde::Deserialize<'de> for BlsSecretKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as serde::Deserializer<'de>>::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        let mut array = [0; BLS_SECRET_KEY_LENGTH];
        let length = bs58::decode(s)
            .into(&mut array[..])
            .map_err(|err| serde::de::Error::custom(err.to_string()))?;
        if length != BLS_SECRET_KEY_LENGTH {
            return Err(serde::de::Error::custom(format!(
                "Invalid length {} of BLS secret key",
                length,
            )));
        }
        milagro_bls::SecretKey::from_bytes(&array)
            .map(BlsSecretKey)
            .map_err(|err| serde::de::Error::custom(format!("{:?}", err)))
    }
}

impl std::fmt::Display for BlsSignature {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", bs58::encode(self.0.as_bytes()).into_string())
    }
}

impl std::fmt::Debug for BlsSignature {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", bs58::encode(self.0.as_bytes()).into_string())
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct BlsSignature(milagro_bls::AggregateSignature);

impl BlsSignature {
    pub fn empty() -> BlsSignature {
        BlsSignature(milagro_bls::AggregateSignature::new())
    }

    pub fn add(&mut self, signature: &BlsSignature) {
        self.0.add_aggregate(&signature.0);
    }

    pub fn verify_single(&self, data: &[u8], public_key: &BlsPublicKey) -> bool {
        let mut agg_pk = AggregatePublicKey::new();
        agg_pk.add(&public_key.0);
        self.0.verify(data, BLS_DOMAIN, &agg_pk)
    }

    pub fn verify_aggregate(&self, data: &[u8], public_keys: &[BlsPublicKey]) -> bool {
        let mut agg_pk = AggregatePublicKey::new();
        for public_key in public_keys.iter() {
            agg_pk.add(&public_key.0);
        }
        self.0.verify(data, BLS_DOMAIN, &agg_pk)
    }
}

impl BorshSerialize for BlsSignature {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        writer.write(&self.0.as_bytes()).map(|_| ())
    }
}

impl BorshDeserialize for BlsSignature {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, Error> {
        let mut buf = [0; BLS_SIGNATURE_LENGTH];
        reader.read_exact(&mut buf)?;
        milagro_bls::AggregateSignature::from_bytes(&buf)
            .map(BlsSignature)
            .map_err(|err| Error::new(ErrorKind::Other, format!("{:?}", err)))
    }
}

impl serde::Serialize for BlsSignature {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> Result<<S as serde::Serializer>::Ok, <S as serde::Serializer>::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&bs58::encode(self.0.as_bytes()).into_string())
    }
}

impl<'de> serde::Deserialize<'de> for BlsSignature {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as serde::Deserializer<'de>>::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        let mut array = [0; BLS_SIGNATURE_LENGTH];
        let length = bs58::decode(s)
            .into(&mut array[..])
            .map_err(|err| serde::de::Error::custom(err.to_string()))?;
        if length != BLS_SIGNATURE_LENGTH {
            return Err(serde::de::Error::custom(format!(
                "Invalid length {} of BLS signature",
                length,
            )));
        }
        milagro_bls::AggregateSignature::from_bytes(&array)
            .map(BlsSignature)
            .map_err(|err| serde::de::Error::custom(format!("{:?}", err)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seed() {
        let sk = BlsSecretKey::from_seed("test");
        let sk2 = BlsSecretKey::from_seed("test");
        assert_eq!(sk, sk2);
    }

    #[test]
    fn test_sign_verify() {
        let sk = BlsSecretKey::from_random();
        let message = b"123".to_vec();
        let mut sig = sk.sign(&message);
        assert!(sig.verify_single(&message, &sk.public_key()));
        let sk2 = BlsSecretKey::from_random();
        let sig2 = sk2.sign(&message);
        sig.add(&sig2);
        assert!(sig.verify_aggregate(&message, &[sk.public_key(), sk2.public_key()]));
    }

    #[test]
    fn test_serialize() {
        let sk = BlsSecretKey::from_random();
        let bytes = sk.try_to_vec().unwrap();
        let sk1 = BlsSecretKey::try_from_slice(&bytes).unwrap();
        assert_eq!(sk, sk1);

        let pk = sk.public_key();
        let bytes = pk.try_to_vec().unwrap();
        let pk1 = BlsPublicKey::try_from_slice(&bytes).unwrap();
        assert_eq!(pk, pk1);

        let sig = sk.sign(b"message");
        let bytes = sig.try_to_vec().unwrap();
        let sig1 = BlsSignature::try_from_slice(&bytes).unwrap();
        assert_eq!(sig, sig1);
    }

    #[test]
    fn test_to_string() {
        let sk = BlsSecretKey::from_random();
        let pk = sk.public_key();
        let s = serde_json::to_string_pretty(&pk).unwrap();
        let pk1: BlsPublicKey = serde_json::from_str(&s).unwrap();
        assert_eq!(pk1, pk);
    }
}

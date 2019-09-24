use borsh::{BorshDeserialize, BorshSerialize};
use milagro_bls::AggregatePublicKey;
use std::io::{Error, ErrorKind, Read, Write};

const BLS_DOMAIN: u64 = 42;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BlsPublicKey(milagro_bls::PublicKey);

impl BorshSerialize for BlsPublicKey {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        writer.write(&self.0.as_bytes()).map(|_| ())
    }
}

impl BorshDeserialize for BlsPublicKey {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, Error> {
        let mut buf = [0; 48];
        reader.read(&mut buf)?;
        milagro_bls::PublicKey::from_bytes(&buf)
            .map(|pk| BlsPublicKey(pk))
            .map_err(|err| Error::new(ErrorKind::Other, format!("{:?}", err)))
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BlsSecretKey(milagro_bls::SecretKey);

impl BlsSecretKey {
    pub fn random() -> BlsSecretKey {
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
        let mut buf = [0; 48];
        reader.read(&mut buf)?;
        milagro_bls::SecretKey::from_bytes(&buf)
            .map(|sk| BlsSecretKey(sk))
            .map_err(|err| Error::new(ErrorKind::Other, format!("{:?}", err)))
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BlsSignature(milagro_bls::AggregateSignature);

impl BlsSignature {
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
        let mut buf = [0; 96];
        reader.read(&mut buf)?;
        milagro_bls::AggregateSignature::from_bytes(&buf)
            .map(|sig| BlsSignature(sig))
            .map_err(|err| Error::new(ErrorKind::Other, format!("{:?}", err)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign_verify() {
        let sk = BlsSecretKey::random();
        let message = b"123".to_vec();
        let mut sig = sk.sign(&message);
        assert!(sig.verify_single(&message, &sk.public_key()));
        let sk2 = BlsSecretKey::random();
        let sig2 = sk2.sign(&message);
        sig.add(&sig2);
        assert!(sig.verify_aggregate(&message, &[sk.public_key(), sk2.public_key()]));
    }

    #[test]
    fn test_serialize() {
        let sk = BlsSecretKey::random();
        let bytes = sk.try_to_vec().unwrap();
        let sk1 = BlsSecretKey::try_from_slice(&bytes).unwrap();
        assert_eq!(sk, sk1);

        let pk = sk.public_key();
        let bytes = pk.try_to_vec().unwrap();
        let pk1 = BlsPublicKey::try_from_slice(&bytes).unwrap();
        assert_eq!(pk, pk1);

        let sig = sk.sign(b"message");
        let bytes = sig.try_to_vec().unwrap();
        println!("{}", bytes.len());
        let sig1 = BlsSignature::try_from_slice(&bytes).unwrap();
        assert_eq!(sig, sig1);
    }
}

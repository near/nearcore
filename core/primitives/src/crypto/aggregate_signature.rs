use std::convert::TryFrom;
use std::error::Error;
use std::fmt;
use std::io::Cursor;

use pairing::bls12_381::Bls12;
use pairing::{
    CurveAffine, CurveProjective, EncodedPoint, Engine, Field, GroupDecodingError, PrimeField,
    PrimeFieldRepr, Rand,
};
use rand::rngs::OsRng;
use rand::Rng;

use crate::serialize::{to_base, BaseDecode};
use crate::types::ReadableBlsPublicKey;

const DOMAIN_SIGNATURE: &[u8] = b"_s";
const DOMAIN_PROOF_OF_POSSESSION: &[u8] = b"_p";

pub fn get_bls_key_pair() -> (BlsPublicKey, BlsSecretKey) {
    let secret_key = BlsSecretKey::generate();
    let public_key = secret_key.get_public_key();
    (public_key, secret_key)
}

#[derive(Clone, Debug)]
pub struct SecretKey<E: Engine> {
    scalar: E::Fr,
}

#[derive(Clone)]
pub struct PublicKey<E: Engine> {
    // G1 is the small-and-fast group.  G2 is the big-and-slow group.  Either one can be used for
    // public keys, and the other for signatures.  Since signature aggregation only needs to be
    // performed by provers, but pubkey aggregation needs to be done by verifiers, we choose the
    // small-and-fast group for public keys.
    point: E::G1Affine,
}

#[derive(Clone, Debug)]
pub struct Signature<E: Engine> {
    // A point on the G2 curve, but not necessarily in the correct G2 subgroup.
    point: E::G2Affine,
}

// TODO: it will usually be desirable to store pubkeys and signatures in compressed form, even in
// memory.  The compressed representations are half the size.
#[derive(Clone)]
pub struct CompressedPublicKey<E: Engine>(<E::G1Affine as CurveAffine>::Compressed);

#[derive(Clone)]
pub struct CompressedSignature<E: Engine>(<E::G2Affine as CurveAffine>::Compressed);

// For those times when time is more important than space, UncompressedSignature is 192 bytes, twice
// as large as CompressedSignature but much faster (about 250x) to decode.
#[derive(Clone)]
pub struct UncompressedSignature<E: Engine>(<E::G2Affine as CurveAffine>::Uncompressed);

impl<E: Engine> SecretKey<E> {
    /// Generate a new secret key from the OS rng.  Panics if OS is unable to provide randomness
    pub fn generate() -> Self {
        let mut rng = OsRng::new().expect("Unable to generate random numbers");
        Self::generate_from_rng(&mut rng)
    }

    pub fn generate_from_rng<R: Rng>(csprng: &mut R) -> Self {
        SecretKey { scalar: E::Fr::rand(csprng) }
    }

    pub fn empty() -> Self {
        SecretKey { scalar: E::Fr::zero() }
    }

    pub fn get_public_key(&self) -> PublicKey<E> {
        PublicKey { point: E::G1Affine::one().mul(self.scalar).into_affine() }
    }

    pub fn sign(&self, message: &[u8]) -> Signature<E> {
        self.sign_domain(message, DOMAIN_SIGNATURE)
    }

    pub fn get_proof_of_possession(&self) -> Signature<E> {
        let message = self.get_public_key().compress();
        self.sign_domain(message.as_ref(), DOMAIN_PROOF_OF_POSSESSION)
    }

    fn sign_domain(&self, message: &[u8], domain: &[u8]) -> Signature<E> {
        // TODO: it would be really nice if CurveProjective::hash took a pair of arguments instead
        // of just one.  The copy here is silly and avoidable.  It's here because we require domain
        // separation for the proof-of-possession.  Simply signing your own public key is not
        // sufficient.  See https://rist.tech.cornell.edu/papers/pkreg.pdf
        let padded_message = [message, domain].concat();
        self.sign_internal(padded_message.as_ref())
    }

    fn sign_internal(&self, message: &[u8]) -> Signature<E> {
        let h = E::G2::hash(message).into_affine();
        Signature { point: h.mul(self.scalar).into_affine() }
    }
}

impl<E: Engine> PublicKey<E> {
    pub fn empty() -> Self {
        PublicKey { point: E::G1Affine::zero() }
    }

    pub fn is_empty(&self) -> bool {
        self.point == E::G1Affine::zero()
    }

    pub fn compress(&self) -> CompressedPublicKey<E> {
        CompressedPublicKey(self.point.into_compressed())
    }

    pub fn to_readable(&self) -> ReadableBlsPublicKey {
        ReadableBlsPublicKey(self.to_string())
    }

    pub fn verify(&self, message: &[u8], signature: &Signature<E>) -> bool {
        self.verify_domain(message, DOMAIN_SIGNATURE, signature)
    }

    pub fn verify_proof_of_possession(&self, signature: &Signature<E>) -> bool {
        let message = self.compress();
        self.verify_domain(message.as_ref(), DOMAIN_PROOF_OF_POSSESSION, signature)
    }

    fn verify_domain(&self, message: &[u8], domain: &[u8], signature: &Signature<E>) -> bool {
        let padded_message = [message, domain].concat();
        self.verify_internal(padded_message.as_ref(), signature)
    }

    fn verify_internal(&self, message: &[u8], signature: &Signature<E>) -> bool {
        if !signature.point.is_in_correct_subgroup_assuming_on_curve() {
            return false;
        }
        let h = E::G2::hash(message).into_affine();
        let lhs = E::pairing(E::G1Affine::one(), signature.point);
        let rhs = E::pairing(self.point, h);
        lhs == rhs
    }
}

impl<E: Engine> fmt::Debug for PublicKey<E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", to_base(self.compress().as_ref()))
    }
}

impl<E: Engine> fmt::Display for PublicKey<E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", to_base(self.compress().as_ref()))
    }
}

// Note: deriving PartialEq and Eq doesn't work
impl<E: Engine> PartialEq for PublicKey<E> {
    fn eq(&self, other: &PublicKey<E>) -> bool {
        self.point == other.point
    }
}

impl<E: Engine> Eq for PublicKey<E> {}

impl<E: Engine> std::hash::Hash for PublicKey<E> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let bytes: Vec<u8> = self.into();
        state.write(&bytes);
    }
}

impl<E: Engine> Signature<E> {
    pub fn compress(&self) -> CompressedSignature<E> {
        CompressedSignature(self.point.into_compressed())
    }

    pub fn encode_uncompressed(&self) -> UncompressedSignature<E> {
        UncompressedSignature(self.point.into_uncompressed())
    }

    pub fn empty() -> Self {
        Signature { point: E::G2Affine::zero() }
    }
}

// Note: deriving PartialEq and Eq doesn't work
impl<E: Engine> PartialEq for Signature<E> {
    fn eq(&self, other: &Signature<E>) -> bool {
        self.point == other.point
    }
}

impl<E: Engine> Eq for Signature<E> {}

impl<E: Engine> std::hash::Hash for Signature<E> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.compress().as_ref().hash(state);
    }
}

impl<E: Engine> Default for Signature<E> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<E: Engine> From<SecretKey<E>> for Vec<u8> {
    fn from(secret_key: SecretKey<E>) -> Vec<u8> {
        let repr = secret_key.scalar.into_repr();
        let mut res = Vec::new();
        res.resize(repr.num_bits() as usize / 8, 0);
        let buf = Cursor::new(&mut res);
        repr.write_be(buf).unwrap();
        res
    }
}

impl<E: Engine> TryFrom<&[u8]> for SecretKey<E> {
    type Error = Box<dyn std::error::Error>;

    fn try_from(v: &[u8]) -> Result<Self, Self::Error> {
        let mut repr: <E::Fr as PrimeField>::Repr = Default::default();
        let buf = Cursor::new(v);
        repr.read_be(buf).map_err(|err| err.to_string())?;
        let scalar = <E::Fr as PrimeField>::from_repr(repr).map_err(|err| err.to_string())?;
        Ok(Self { scalar })
    }
}

impl<E: Engine> TryFrom<Vec<u8>> for SecretKey<E> {
    type Error = Box<dyn std::error::Error>;

    fn try_from(v: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(v.as_ref())
    }
}

// `Eq`, `PartialEq`, and `Hash` traits allow us to use `SecretKey<E>` in standard std containers
// and macros.
impl<E: Engine> Eq for SecretKey<E> {}

impl<E: Engine> PartialEq for SecretKey<E> {
    fn eq(&self, other: &Self) -> bool {
        self.scalar == other.scalar
    }
}

impl<E: Engine> std::hash::Hash for SecretKey<E> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let bytes: Vec<u8> = self.clone().into();
        state.write(&bytes);
    }
}

impl<E: Engine> From<&PublicKey<E>> for Vec<u8> {
    fn from(public_key: &PublicKey<E>) -> Vec<u8> {
        public_key.compress().as_ref().to_vec()
    }
}

#[derive(Debug)]
pub struct LengthError(usize, usize);

impl Error for LengthError {
    fn description(&self) -> &str {
        "encoding has incorrect length"
    }
}

impl fmt::Display for LengthError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}, expected {}, got {}", self.description(), self.0, self.1)
    }
}

impl<E: Engine> TryFrom<&[u8]> for PublicKey<E> {
    type Error = Box<dyn std::error::Error>;

    fn try_from(v: &[u8]) -> Result<Self, Self::Error> {
        Ok(CompressedPublicKey::try_from(v)?.decompress().map_err(|err| err.to_string())?)
    }
}

impl<E: Engine> From<&Signature<E>> for Vec<u8> {
    fn from(signature: &Signature<E>) -> Vec<u8> {
        signature.compress().into()
    }
}

impl<E: Engine> TryFrom<&[u8]> for Signature<E> {
    type Error = Box<dyn std::error::Error>;

    fn try_from(v: &[u8]) -> Result<Self, Self::Error> {
        Ok(CompressedSignature::try_from(v)?.decode().map_err(|err| err.to_string())?)
    }
}

impl<E: Engine> TryFrom<Vec<u8>> for Signature<E> {
    type Error = Box<dyn std::error::Error>;

    fn try_from(v: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(v.as_ref())
    }
}

impl<E: Engine> CompressedPublicKey<E> {
    pub fn decompress(&self) -> Result<PublicKey<E>, GroupDecodingError> {
        Ok(PublicKey { point: self.0.into_affine()? })
    }

    /// Decompress a pubkey, without verifying that the resulting point is actually on the curve.
    /// Verifying is very slow, so if we know we've already done it (for example, if we're reading
    /// from disk a previously validated block), we can skip point verification.  Use with caution.
    pub fn decompress_unchecked(&self) -> PublicKey<E> {
        PublicKey { point: self.0.into_affine_unchecked().unwrap() }
    }
}

impl<E: Engine> From<&CompressedPublicKey<E>> for Vec<u8> {
    fn from(public_key: &CompressedPublicKey<E>) -> Vec<u8> {
        public_key.as_ref().to_vec()
    }
}

impl<E: Engine> AsRef<[u8]> for CompressedPublicKey<E> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<E: Engine> AsMut<[u8]> for CompressedPublicKey<E> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.0.as_mut()
    }
}

impl<E: Engine> From<CompressedPublicKey<E>> for Vec<u8> {
    fn from(public_key: CompressedPublicKey<E>) -> Vec<u8> {
        public_key.as_ref().to_vec()
    }
}

impl<E: Engine> TryFrom<&[u8]> for CompressedPublicKey<E> {
    type Error = Box<dyn std::error::Error>;

    fn try_from(v: &[u8]) -> Result<Self, Self::Error> {
        let expected = <<E::G1Affine as CurveAffine>::Compressed as EncodedPoint>::size();
        if v.len() != expected {
            return Err(LengthError(expected, v.len()).into());
        }
        let mut encoded = <E::G1Affine as CurveAffine>::Compressed::empty();
        encoded.as_mut().copy_from_slice(v);
        Ok(Self(encoded))
    }
}

impl<E: Engine> CompressedSignature<E> {
    pub fn decode(&self) -> Result<Signature<E>, GroupDecodingError> {
        // Subgroup check is postponed until signature verification
        Ok(Signature { point: self.0.into_affine_semi_checked()? })
    }
}

impl<E: Engine> From<&CompressedSignature<E>> for Vec<u8> {
    fn from(signature: &CompressedSignature<E>) -> Vec<u8> {
        signature.as_ref().to_vec()
    }
}

impl<E: Engine> AsRef<[u8]> for CompressedSignature<E> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<E: Engine> AsMut<[u8]> for CompressedSignature<E> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.0.as_mut()
    }
}

impl<E: Engine> From<CompressedSignature<E>> for Vec<u8> {
    fn from(signature: CompressedSignature<E>) -> Vec<u8> {
        signature.as_ref().to_vec()
    }
}

impl<E: Engine> TryFrom<&[u8]> for CompressedSignature<E> {
    type Error = Box<dyn std::error::Error>;

    fn try_from(v: &[u8]) -> Result<Self, Self::Error> {
        let expected = <<E::G2Affine as CurveAffine>::Compressed as EncodedPoint>::size();
        if v.len() != expected {
            return Err(LengthError(expected, v.len()).into());
        }
        let mut encoded = <E::G2Affine as CurveAffine>::Compressed::empty();
        encoded.as_mut().copy_from_slice(v);
        Ok(Self(encoded))
    }
}

impl<E: Engine> UncompressedSignature<E> {
    pub fn decode(&self) -> Result<Signature<E>, GroupDecodingError> {
        // Subgroup check is postponed until signature verification
        Ok(Signature { point: self.0.into_affine_semi_checked()? })
    }
}

impl<E: Engine> From<&UncompressedSignature<E>> for Vec<u8> {
    fn from(signature: &UncompressedSignature<E>) -> Vec<u8> {
        signature.as_ref().to_vec()
    }
}

impl<E: Engine> AsRef<[u8]> for UncompressedSignature<E> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<E: Engine> AsMut<[u8]> for UncompressedSignature<E> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.0.as_mut()
    }
}

impl<E: Engine> From<UncompressedSignature<E>> for Vec<u8> {
    fn from(signature: UncompressedSignature<E>) -> Vec<u8> {
        signature.as_ref().to_vec()
    }
}

impl<E: Engine> TryFrom<&[u8]> for UncompressedSignature<E> {
    type Error = Box<dyn std::error::Error>;

    fn try_from(v: &[u8]) -> Result<Self, Self::Error> {
        let expected = <<E::G2Affine as CurveAffine>::Uncompressed as EncodedPoint>::size();
        if v.len() != expected {
            return Err(LengthError(expected, v.len()).into());
        }
        let mut encoded = <E::G2Affine as CurveAffine>::Uncompressed::empty();
        encoded.as_mut().copy_from_slice(v);
        Ok(Self(encoded))
    }
}

#[derive(Debug, Clone)]
pub struct AggregatePublicKey<E: Engine> {
    // This is the same as a public key, but stored in projective coordinates instead of affine.
    point: E::G1,
}

#[derive(Debug, Clone)]
pub struct AggregateSignature<E: Engine> {
    // This is the same as a signature, but stored in projective coordinates instead of affine.
    point: E::G2,
}

impl<E: Engine> AggregatePublicKey<E> {
    pub fn new() -> Self {
        AggregatePublicKey { point: E::G1::zero() }
    }

    // Very important: you must verify a proof-of-possession for each public key!
    pub fn aggregate(&mut self, pubkey: &PublicKey<E>) {
        self.point.add_assign_mixed(&pubkey.point);
    }

    pub fn get_key(&self) -> PublicKey<E> {
        PublicKey { point: self.point.into_affine() }
    }
}

impl<E: Engine> Default for AggregatePublicKey<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: Engine> AggregateSignature<E> {
    pub fn new() -> Self {
        AggregateSignature { point: E::G2::zero() }
    }

    pub fn aggregate(&mut self, sig: &Signature<E>) {
        self.point.add_assign_mixed(&sig.point);
    }

    pub fn get_signature(&self) -> Signature<E> {
        Signature { point: self.point.into_affine() }
    }
}

impl<E: Engine> Default for AggregateSignature<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: Engine> BaseDecode for PublicKey<E> {}
impl<E: Engine> BaseDecode for SecretKey<E> {}
impl<E: Engine> BaseDecode for UncompressedSignature<E> {}
impl<E: Engine> BaseDecode for Signature<E> {}

pub type BlsSecretKey = SecretKey<Bls12>;
pub type BlsPublicKey = PublicKey<Bls12>;
pub type BlsSignature = Signature<Bls12>;
pub type BlsAggregatePublicKey = AggregatePublicKey<Bls12>;
pub type BlsAggregateSignature = AggregateSignature<Bls12>;

pub mod uncompressed_bs64_signature_serializer {
    use serde::{Deserialize, Deserializer, Serializer};

    use crate::crypto::aggregate_signature::{Bls12, BlsSignature, UncompressedSignature};
    use crate::serialize::{BaseDecode, BaseEncode};

    pub fn serialize<S>(sig: &BlsSignature, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&sig.encode_uncompressed().to_base())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<BlsSignature, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let uncompressed = UncompressedSignature::<Bls12>::from_base(&s).unwrap();
        Ok(uncompressed.decode().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand_xorshift::XorShiftRng;

    use super::*;

    #[test]
    fn sign_verify() {
        let mut rng = XorShiftRng::seed_from_u64(1);

        let secret = (0..2).map(|_| BlsSecretKey::generate_from_rng(&mut rng)).collect::<Vec<_>>();
        let pubkey = (0..2).map(|i| secret[i].get_public_key()).collect::<Vec<_>>();
        let message = (0..2).map(|i| format!("message {}", i)).collect::<Vec<_>>();
        let signature = (0..2).map(|i| secret[i].sign(message[i].as_bytes())).collect::<Vec<_>>();

        for i in 0..2 {
            for j in 0..2 {
                for k in 0..2 {
                    assert_eq!(
                        pubkey[i].verify(message[j].as_bytes(), &signature[k]),
                        (i == j) && (j == k)
                    );
                }
            }
        }
    }

    #[test]
    fn proof_verify() {
        let mut rng = XorShiftRng::seed_from_u64(2);

        let secret = (0..2).map(|_| BlsSecretKey::generate_from_rng(&mut rng)).collect::<Vec<_>>();
        let pubkey = (0..2).map(|i| secret[i].get_public_key()).collect::<Vec<_>>();
        let proof = (0..2).map(|i| secret[i].get_proof_of_possession()).collect::<Vec<_>>();

        for i in 0..2 {
            for j in 0..2 {
                assert_eq!(pubkey[i].verify_proof_of_possession(&proof[j]), i == j);
            }
        }

        // make sure domain-separation is working
        let fake_proof = secret[0].sign(pubkey[0].compress().as_ref());
        assert!(!pubkey[0].verify_proof_of_possession(&fake_proof));
    }

    #[test]
    fn aggregate_signature() {
        let mut rng = XorShiftRng::seed_from_u64(3);

        let secret = (0..10).map(|_| BlsSecretKey::generate_from_rng(&mut rng)).collect::<Vec<_>>();

        let mut signature = BlsAggregateSignature::new();
        let mut pubkey = BlsAggregatePublicKey::new();

        let message = "Hello, world!";

        for i in 0..10 {
            signature.aggregate(&secret[i].sign(message.as_bytes()));
            pubkey.aggregate(&secret[i].get_public_key());
        }

        assert!(pubkey.get_key().verify(message.as_bytes(), &signature.get_signature()));

        // Signature should not validate on empty pubkey set
        let blank_pk = BlsAggregatePublicKey::new().get_key();
        assert!(!blank_pk.verify(message.as_bytes(), &signature.get_signature()));

        // Blank signature should not validate on non-empty pubkey set
        let blank_signature = BlsAggregateSignature::new().get_signature();
        assert!(!pubkey.get_key().verify(message.as_bytes(), &blank_signature));

        // Blank signature does validate on empty pubkey set for any message.  It does seem a little
        // odd, but it's consistent.
        assert!(blank_pk.verify(message.as_bytes(), &blank_signature));
    }

    #[test]
    fn encoding() {
        let mut rng = XorShiftRng::seed_from_u64(4);

        let secret = BlsSecretKey::generate_from_rng(&mut rng);
        let _pubkey = secret.get_public_key();
        let message = "Hello, world!";
        let signature = secret.sign(message.as_bytes());

        let compressed = signature.compress();
        let uncompressed = signature.encode_uncompressed();

        assert_eq!(
            CompressedSignature::try_from(compressed.as_ref()).unwrap().decode().unwrap(),
            signature
        );
        assert_eq!(
            UncompressedSignature::try_from(uncompressed.as_ref()).unwrap().decode().unwrap(),
            signature
        );
        assert!(CompressedSignature::<Bls12>::try_from(uncompressed.as_ref()).is_err());
        assert!(UncompressedSignature::<Bls12>::try_from(compressed.as_ref()).is_err());
    }
}

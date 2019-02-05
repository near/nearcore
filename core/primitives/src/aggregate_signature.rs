use std::fmt;
use std::io::Cursor;
use bs58;
use pairing::{CurveAffine, CurveProjective, EncodedPoint, Engine, Field, GroupDecodingError, PrimeField, PrimeFieldRepr};
use rand::{OsRng, Rand, Rng};

const DOMAIN_SIGNATURE: &[u8] = b"_s";
const DOMAIN_PROOF_OF_POSSESSION: &[u8] = b"_p";

#[derive(Clone, Debug)]
pub struct SecretKey<E: Engine> {
    scalar: E::Fr,
}

#[derive(Clone, Debug)]
pub struct PublicKey<E: Engine> {
    // G1 is the small-and-fast group.  G2 is the big-and-slow group.  Either one can be used for
    // public keys, and the other for signatures.  Since signature aggregation only needs to be
    // performed by provers, but pubkey aggregation needs to be done by verifiers, we choose the
    // small-and-fast group for public keys.
    point: E::G1Affine,
}

#[derive(Clone, Debug)]
pub struct Signature<E: Engine> {
    point: E::G2Affine,
}

// TODO: it will usually be desirable to store pubkeys and signatures in compressed form, even in
// memory.  The compressed representations are half the size.
#[derive(Clone)]
pub struct CompressedPublicKey<E: Engine>(<E::G1Affine as CurveAffine>::Compressed);

#[derive(Clone)]
pub struct CompressedSignature<E: Engine>(<E::G2Affine as CurveAffine>::Compressed);

impl<E: Engine> SecretKey<E> {
    /// Generate a new secret key from the OS rng.  Panics if OS is unable to provide randomness
    pub fn generate() -> Self {
        let mut rng = OsRng::new().expect("Unable to generate random numbers");
        Self::generate_from_rng(&mut rng)
    }

    pub fn generate_from_rng<R: Rng>(csprng: &mut R) -> Self {
        SecretKey {
            scalar: E::Fr::rand(csprng),
        }
    }

    pub fn empty() -> Self {
        SecretKey {
            scalar: E::Fr::zero(),
        }
    }

    pub fn get_public_key(&self) -> PublicKey<E> {
        PublicKey {
            point: E::G1Affine::one().mul(self.scalar).into_affine(),
        }
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
        let padded_message= [message, domain].concat();
        self.sign_internal(padded_message.as_ref())
    }

    fn sign_internal(&self, message: &[u8]) -> Signature<E> {
        let h = E::G2::hash(message).into_affine();
        Signature { point: h.mul(self.scalar).into_affine() }
    }
}

impl<E: Engine> PublicKey<E> {
    pub fn compress(&self) -> CompressedPublicKey<E> {
        CompressedPublicKey( self.point.into_compressed() )
    }

    pub fn verify(&self, message: &[u8], signature: &Signature<E>) -> bool {
        self.verify_domain(message, DOMAIN_SIGNATURE, signature)
    }

    pub fn verify_proof_of_possession(&self, signature: &Signature<E>) -> bool {
        let message = self.compress();
        self.verify_domain(message.as_ref(), DOMAIN_PROOF_OF_POSSESSION, signature)
    }

    fn verify_domain(&self, message: &[u8], domain: &[u8], signature: &Signature<E>) -> bool {
        let padded_message= [message, domain].concat();
        self.verify_internal(padded_message.as_ref(), signature)
    }

    fn verify_internal(&self, message: &[u8], signature: &Signature<E>) -> bool {
        let h = E::G2::hash(message).into_affine();
        let lhs = E::pairing(E::G1Affine::one(), signature.point);
        let rhs = E::pairing(self.point, h);
        lhs == rhs
    }
}

impl<E: Engine> fmt::Display for PublicKey<E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result { write!(f, "{}", bs58::encode(self.compress().as_ref()).into_string()) }
}

impl<E: Engine> Signature<E> {
    pub fn compress(&self) -> CompressedSignature<E> {
        CompressedSignature( self.point.into_compressed() )
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
    fn default() -> Self { Self::empty() }
}

impl<E: Engine> From<&SecretKey<E>> for Vec<u8> {
    fn from(key: &SecretKey<E>) -> Self {
        let repr = key.scalar.into_repr();
        let mut res = Vec::new();
        res.resize(repr.num_bits() as usize / 8, 0);
        let buf = Cursor::new(&mut res);
        repr.write_be(buf).unwrap();
        res
    }
}

// TODO(#502): TryFrom instead of From since conversion can fail
impl<E: Engine> From<Vec<u8>> for SecretKey<E> {
    fn from(v: Vec<u8>) -> Self {
        let mut repr : <E::Fr as PrimeField>::Repr = Default::default();
        let buf = Cursor::new(v);
        repr.read_be(buf).unwrap();
        let scalar = <E::Fr as PrimeField>::from_repr(repr).unwrap();
        Self{ scalar }
    }
}

impl<E: Engine> From<&PublicKey<E>> for Vec<u8> {
    fn from(key: &PublicKey<E>) -> Self {
        Self::from(key.compress().as_ref())
    }
}

// TODO(#502): TryFrom instead of From since conversion can fail
impl<E: Engine> From<Vec<u8>> for PublicKey<E> {
    fn from(v: Vec<u8>) -> Self {
        let expected = <<E::G1Affine as CurveAffine>::Compressed as EncodedPoint>::size();
        if v.len() != expected {
            panic!("invalid signature");
        }
        let mut compressed = CompressedPublicKey::empty();
        compressed.as_mut().copy_from_slice(v.as_ref());
        compressed.decompress().unwrap()
    }
}

impl<E: Engine> From<&Signature<E>> for Vec<u8> {
    fn from(sig: &Signature<E>) -> Self {
        Self::from(sig.compress().as_ref())
    }
}

// TODO(#502): TryFrom instead of From since conversion can fail
impl<E: Engine> From<Vec<u8>> for Signature<E> {
    fn from(v: Vec<u8>) -> Self {
        let expected = <<E::G2Affine as CurveAffine>::Compressed as EncodedPoint>::size();
        if v.len() != expected {
            panic!("invalid signature");
        }
        let mut compressed = CompressedSignature::empty();
        compressed.as_mut().copy_from_slice(v.as_ref());
        compressed.decompress().unwrap()
    }
}

impl<E: Engine> CompressedPublicKey<E> {
    fn empty() -> Self {
        CompressedPublicKey( <E::G1Affine as CurveAffine>::Compressed::empty() )
    }

    fn decompress(&self) -> Result<PublicKey<E>, GroupDecodingError> {
        Ok(PublicKey{ point: self.0.into_affine()? })
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

impl<E: Engine> CompressedSignature<E> {
    fn empty() -> Self {
        CompressedSignature( <E::G2Affine as CurveAffine>::Compressed::empty() )
    }

    fn decompress(&self) -> Result<Signature<E>, GroupDecodingError> {
        Ok(Signature{ point: self.0.into_affine()? })
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

#[derive(Debug)]
pub struct AggregatePublicKey<E: Engine> {
    // This is the same as a public key, but stored in projective coordinates instead of affine.
    point: E::G1,
}

#[derive(Debug)]
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
        PublicKey {
            point: self.point.into_affine()
        }
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
        Signature {
            point: self.point.into_affine()
        }
    }
}

impl<E: Engine> Default for AggregateSignature<E> {
    fn default() -> Self {
        Self::new()
    }
}

use pairing::bls12_381::Bls12;

pub type BlsSecretKey = SecretKey<Bls12>;
pub type BlsPublicKey = PublicKey<Bls12>;
pub type BlsSignature = Signature<Bls12>;
pub type BlsAggregatePublicKey = AggregatePublicKey<Bls12>;
pub type BlsAggregateSignature = AggregateSignature<Bls12>;

#[cfg(test)]
mod tests {
    use super::*;

    use rand::{SeedableRng, XorShiftRng};

    #[test]
    fn sign_verify() {
        let mut rng = XorShiftRng::from_seed([11111111, 22222222, 33333333, 44444444]);

        let secret = (0..2).map(|_| BlsSecretKey::generate_from_rng(&mut rng)).collect::<Vec<_>>();
        let pubkey = (0..2).map(|i| secret[i].get_public_key()).collect::<Vec<_>>();
        let message = (0..2).map(|i| format!("message {}", i)).collect::<Vec<_>>();
        let signature = (0..2).map(|i| secret[i].sign(message[i].as_bytes())).collect::<Vec<_>>();

        for i in 0..2 {
            for j in 0..2 {
                for k in 0..2 {
                    assert_eq!(pubkey[i].verify(message[j].as_bytes(), &signature[k]), (i == j) && (j == k));
                }
            }
        }
    }

    #[test]
    fn proof_verify() {
        let mut rng = XorShiftRng::from_seed([22222222, 33333333, 44444444, 55555555]);

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
        let mut rng = XorShiftRng::from_seed([33333333, 44444444, 55555555, 66666666]);

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
}

use blake2::{Blake2b, VarBlake2b};
use bs58;
use curve25519_dalek::constants::{
    RISTRETTO_BASEPOINT_POINT as G, RISTRETTO_BASEPOINT_TABLE as GT,
};
use curve25519_dalek::ristretto::{CompressedRistretto, RistrettoPoint};
use curve25519_dalek::scalar::Scalar;
use curve25519_dalek::traits::VartimeMultiscalarMul;
use digest::{Input, VariableOutput};
use rand::{CryptoRng, RngCore};
use std::borrow::Borrow;
use std::convert::TryFrom;
use subtle::{ConditionallySelectable, ConstantTimeEq};

#[derive(Copy, Clone)]
pub struct PublicKey([u8; 32], RistrettoPoint);
#[derive(Copy, Clone)]
pub struct SecretKey(Scalar, PublicKey);
value_type!(pub, Value, 32, "value");
value_type!(pub, Proof, 64, "proof");

struct Hash(VarBlake2b);

impl Hash {
    fn new() -> Self {
        Hash(VarBlake2b::new(32).unwrap())
    }
    fn chain(self, data: &[u8]) -> Self {
        Hash(self.0.chain(data))
    }
    fn result(self) -> [u8; 32] {
        let mut r = [0; 32];
        self.0.variable_result(|s| {
            r = *array_ref!(s, 0, 32);
        });
        r
    }
    fn result_scalar(self) -> Scalar {
        Scalar::from_bytes_mod_order(self.result())
    }
}

fn bvmul2(s1: Scalar, p1: &RistrettoPoint, s2: Scalar, p2: &RistrettoPoint) -> [u8; 32] {
    RistrettoPoint::vartime_multiscalar_mul(&[s1, s2], [p1, p2].iter().copied())
        .compress()
        .to_bytes()
}

impl PublicKey {
    fn from_bytes(bytes: &[u8; 32]) -> Option<Self> {
        CompressedRistretto(*bytes).decompress().map(|p| PublicKey(*bytes, p))
    }

    fn offset(&self, input: &[u8]) -> Scalar {
        Hash::new().chain(&self.0).chain(input).result_scalar()
    }

    pub fn check_vrf(&self, input: &impl Borrow<[u8]>, value: &Value, proof: &Proof) -> bool {
        self.check(input.borrow(), value, proof)
    }

    fn check(&self, input: &[u8], value: &Value, proof: &Proof) -> bool {
        let p = match CompressedRistretto(value.0).decompress() {
            Some(p) => p,
            None => return false,
        };
        let (&pr, &pc) = array_refs!(&proof.0, 32, 32);
        let r = match Scalar::from_canonical_bytes(pr) {
            Some(r) => r,
            None => return false,
        };
        let c = match Scalar::from_canonical_bytes(pc) {
            Some(c) => c,
            None => return false,
        };
        Hash::new()
            .chain(&self.0)
            .chain(&value.0)
            .chain(&bvmul2(r + c * self.offset(input), &G, c, &self.1))
            .chain(&bvmul2(r, &p, c, &G))
            .result_scalar()
            == c
    }
}

fn basemul(s: Scalar) -> RistrettoPoint {
    &s * &GT
}

fn bbmul(s: Scalar) -> [u8; 32] {
    basemul(s).compress().to_bytes()
}

fn safe_invert(s: Scalar) -> Scalar {
    Scalar::conditional_select(&s, &Scalar::one(), s.ct_eq(&Scalar::zero())).invert()
}

impl SecretKey {
    fn from_scalar(sk: Scalar) -> Self {
        let pk = basemul(sk);
        SecretKey(sk, PublicKey(pk.compress().to_bytes(), pk))
    }

    fn from_bytes(bytes: &[u8; 32]) -> Option<Self> {
        Scalar::from_canonical_bytes(*bytes).map(Self::from_scalar)
    }

    pub fn random(rng: &mut (impl RngCore + CryptoRng)) -> Self {
        Self::from_scalar(Scalar::random(rng))
    }

    pub fn public_key(&self) -> PublicKey {
        self.1
    }

    pub fn compute_vrf(&self, input: &impl Borrow<[u8]>) -> Value {
        self.compute(input.borrow())
    }

    fn compute(&self, input: &[u8]) -> Value {
        Value(bbmul(safe_invert(self.0 + self.1.offset(input))))
    }

    pub fn compute_vrf_with_proof(&self, input: &impl Borrow<[u8]>) -> (Value, Proof) {
        self.compute_with_proof(input.borrow())
    }

    fn compute_with_proof(&self, input: &[u8]) -> (Value, Proof) {
        let x = self.0 + self.1.offset(input);
        let inv = safe_invert(x);
        let val = bbmul(inv);
        let k = Scalar::from_hash(Blake2b::default().chain(x.as_bytes()));
        let c = Hash::new()
            .chain(&(self.1).0)
            .chain(&val)
            .chain(&bbmul(k))
            .chain(&bbmul(inv * k))
            .result_scalar();
        let r = k - c * x;
        let mut proof = [0; 64];
        let (pr, pc) = mut_array_refs!(&mut proof, 32, 32);
        *pr = r.to_bytes();
        *pc = c.to_bytes();
        (Value(val), Proof(proof))
    }

    pub fn check_vrf(&self, input: &impl Borrow<[u8]>, value: &Value, proof: &Proof) -> bool {
        self.1.check(input.borrow(), value, proof)
    }
}

macro_rules! traits {
    ($ty:ident, $l:literal, $bytes:expr, $what:literal) => {
        eq!($ty, |a, b| a.0 == b.0);
        common_conversions_fixed!($ty, 32, $bytes, $what);

        impl TryFrom<&[u8; $l]> for $ty {
            type Error = ();
            fn try_from(value: &[u8; $l]) -> Result<Self, ()> {
                Self::from_bytes(value).ok_or(())
            }
        }
    };
}

traits!(PublicKey, 32, |s| &s.0, "public key");
traits!(SecretKey, 32, |s| s.0.as_bytes(), "secret key");

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::OsRng;
    use serde::{Deserialize, Serialize};
    use serde_json::{from_str, to_string};

    #[test]
    fn test_conversion() {
        let sk = SecretKey::random(&mut OsRng::default());
        let sk2 = SecretKey::from_bytes(&sk.into()).unwrap();
        assert_eq!(sk, sk2);
        let pk = sk.public_key();
        let pk2 = sk2.public_key();
        let pk3 = PublicKey::from_bytes(&pk2.into()).unwrap();
        assert_eq!(pk, pk2);
        assert_eq!(pk, pk3);
    }

    #[test]
    fn test_verify() {
        let sk = SecretKey::random(&mut OsRng::default());
        let (val, proof) = sk.compute_vrf_with_proof(b"Test");
        let val2 = sk.compute_vrf(b"Test");
        assert_eq!(val, val2);
        assert!(sk.public_key().check_vrf(b"Test", &val, &proof));
        assert!(!sk.public_key().check_vrf(b"Tent", &val, &proof));
    }

    #[test]
    fn test_different_keys() {
        let sk = SecretKey::random(&mut OsRng::default());
        let sk2 = SecretKey::random(&mut OsRng::default());
        assert_ne!(sk, sk2);
        assert_ne!(Into::<[u8; 32]>::into(sk), Into::<[u8; 32]>::into(sk2));
        let pk = sk.public_key();
        let pk2 = sk2.public_key();
        assert_ne!(pk, pk2);
        assert_ne!(Into::<[u8; 32]>::into(pk), Into::<[u8; 32]>::into(pk2));
        let (val, proof) = sk.compute_vrf_with_proof(b"Test");
        let (val2, proof2) = sk2.compute_vrf_with_proof(b"Test");
        assert_ne!(val, val2);
        assert_ne!(proof, proof2);
        assert!(!pk2.check_vrf(b"Test", &val, &proof));
        assert!(!pk2.check_vrf(b"Test", &val2, &proof));
        assert!(!pk2.check_vrf(b"Test", &val, &proof2));
    }

    fn round_trip<T: Serialize + for<'de> Deserialize<'de>>(value: &T) -> T {
        from_str(to_string(value).unwrap().as_str()).unwrap()
    }

    #[test]
    fn test_serialize() {
        let sk = SecretKey::random(&mut OsRng::default());
        let sk2 = round_trip(&sk);
        assert_eq!(sk, sk2);
        let (val, proof) = sk.compute_vrf_with_proof(b"Test");
        let (val2, proof2) = sk2.compute_vrf_with_proof(b"Test");
        let (val3, proof3) = (round_trip(&val), round_trip(&proof));
        assert_eq!((val, proof), (val2, proof2));
        assert_eq!((val, proof), (val3, proof3));
        let pk = sk.public_key();
        let pk2 = sk2.public_key();
        let pk3 = round_trip(&pk);
        assert!(pk.check_vrf(b"Test", &val, &proof));
        assert!(pk2.check_vrf(b"Test", &val, &proof));
        assert!(pk3.check_vrf(b"Test", &val, &proof));
    }
}

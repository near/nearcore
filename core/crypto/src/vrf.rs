use crate::util::*;
use blake2::{Blake2b, VarBlake2b};
use bs58;
use curve25519_dalek::constants::{
    RISTRETTO_BASEPOINT_POINT as G, RISTRETTO_BASEPOINT_TABLE as GT,
};
use digest::{Input, VariableOutput};
use rand_core::OsRng;
use std::borrow::Borrow;
use std::convert::TryFrom;
use subtle::{ConditionallySelectable, ConstantTimeEq};

#[derive(Copy, Clone)]
pub struct PublicKey(pub(crate) [u8; 32], pub(crate) Point);
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

impl PublicKey {
    fn from_bytes(bytes: &[u8; 32]) -> Option<Self> {
        Some(PublicKey(*bytes, unpack(bytes)?))
    }

    fn offset(&self, input: &[u8]) -> Scalar {
        Hash::new().chain(&self.0).chain(input).result_scalar()
    }

    pub fn is_vrf_valid(&self, input: &impl Borrow<[u8]>, value: &Value, proof: &Proof) -> bool {
        self.is_valid(input.borrow(), value, proof)
    }

    fn is_valid(&self, input: &[u8], value: &Value, proof: &Proof) -> bool {
        let p = unwrap_or_return_false!(unpack(&value.0));
        let (r, c) = unwrap_or_return_false!(unpack(&proof.0));
        Hash::new()
            .chain(&self.0)
            .chain(&value.0)
            .chain(&vmul2(r + c * self.offset(input), &G, c, &self.1).pack())
            .chain(&vmul2(r, &p, c, &G).pack())
            .result_scalar()
            == c
    }
}

fn basemul(s: Scalar) -> Point {
    &s * &GT
}

fn bbmul(s: Scalar) -> [u8; 32] {
    basemul(s).pack()
}

fn safe_invert(s: Scalar) -> Scalar {
    Scalar::conditional_select(&s, &Scalar::one(), s.ct_eq(&Scalar::zero())).invert()
}

impl SecretKey {
    pub(crate) fn from_scalar(sk: Scalar) -> Self {
        let pk = basemul(sk);
        SecretKey(sk, PublicKey(pk.pack(), pk))
    }

    fn from_bytes(bytes: &[u8; 32]) -> Option<Self> {
        Some(Self::from_scalar(unpack(bytes)?))
    }

    pub fn random() -> Self {
        Self::from_scalar(Scalar::random(&mut OsRng))
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
        (Value(val), Proof((r, c).pack()))
    }

    pub fn is_vrf_valid(&self, input: &impl Borrow<[u8]>, value: &Value, proof: &Proof) -> bool {
        self.1.is_valid(input.borrow(), value, proof)
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
    use serde::{Deserialize, Serialize};
    use serde_json::{from_str, to_string};

    #[test]
    fn test_conversion() {
        let sk = SecretKey::random();
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
        let sk = SecretKey::random();
        let (val, proof) = sk.compute_vrf_with_proof(b"Test");
        let val2 = sk.compute_vrf(b"Test");
        assert_eq!(val, val2);
        assert!(sk.public_key().is_vrf_valid(b"Test", &val, &proof));
        assert!(!sk.public_key().is_vrf_valid(b"Tent", &val, &proof));
    }

    #[test]
    fn test_different_keys() {
        let sk = SecretKey::random();
        let sk2 = SecretKey::random();
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
        assert!(!pk2.is_vrf_valid(b"Test", &val, &proof));
        assert!(!pk2.is_vrf_valid(b"Test", &val2, &proof));
        assert!(!pk2.is_vrf_valid(b"Test", &val, &proof2));
    }

    fn round_trip<T: Serialize + for<'de> Deserialize<'de>>(value: &T) -> T {
        from_str(to_string(value).unwrap().as_str()).unwrap()
    }

    #[test]
    fn test_serialize() {
        let sk = SecretKey::random();
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
        assert!(pk.is_vrf_valid(b"Test", &val, &proof));
        assert!(pk2.is_vrf_valid(b"Test", &val, &proof));
        assert!(pk3.is_vrf_valid(b"Test", &val, &proof));
    }
}

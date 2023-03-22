use crate::errors::ImplicitPublicKeyError;
use crate::{KeyType, PublicKey};
use borsh::BorshDeserialize;
use curve25519_dalek::ristretto::CompressedRistretto;
use curve25519_dalek::traits::VartimeMultiscalarMul;

pub use curve25519_dalek::ristretto::RistrettoPoint as Point;
pub use curve25519_dalek::scalar::Scalar;

pub fn vmul2(s1: Scalar, p1: &Point, s2: Scalar, p2: &Point) -> Point {
    Point::vartime_multiscalar_mul(&[s1, s2], [p1, p2].iter().copied())
}

pub trait Packable: Sized {
    type Packed;
    fn unpack(data: &Self::Packed) -> Option<Self>;
    fn pack(&self) -> Self::Packed;
}

pub fn unpack<T: Packable>(data: &T::Packed) -> Option<T> {
    Packable::unpack(data)
}

impl Packable for [u8; 32] {
    type Packed = [u8; 32];

    fn unpack(data: &[u8; 32]) -> Option<Self> {
        Some(*data)
    }

    fn pack(&self) -> [u8; 32] {
        *self
    }
}

impl Packable for Point {
    type Packed = [u8; 32];

    fn unpack(data: &[u8; 32]) -> Option<Self> {
        CompressedRistretto(*data).decompress()
    }

    fn pack(&self) -> [u8; 32] {
        self.compress().to_bytes()
    }
}

impl Packable for Scalar {
    type Packed = [u8; 32];

    fn unpack(data: &[u8; 32]) -> Option<Self> {
        Scalar::from_canonical_bytes(*data)
    }

    fn pack(&self) -> [u8; 32] {
        self.to_bytes()
    }
}

impl<T1: Packable<Packed = [u8; 32]>, T2: Packable<Packed = [u8; 32]>> Packable for (T1, T2) {
    type Packed = [u8; 64];

    fn unpack(data: &[u8; 64]) -> Option<Self> {
        let (d1, d2) = stdx::split_array::<64, 32, 32>(data);
        Some((unpack(d1)?, unpack(d2)?))
    }

    fn pack(&self) -> [u8; 64] {
        stdx::join_array(self.0.pack(), self.1.pack())
    }
}

impl<
        T1: Packable<Packed = [u8; 32]>,
        T2: Packable<Packed = [u8; 32]>,
        T3: Packable<Packed = [u8; 32]>,
    > Packable for (T1, T2, T3)
{
    type Packed = [u8; 96];

    fn unpack(data: &[u8; 96]) -> Option<Self> {
        let (d1, d2) = stdx::split_array::<96, 32, 64>(data);
        let (d2, d3) = stdx::split_array::<64, 32, 32>(d2);
        Some((unpack(d1)?, unpack(d2)?, unpack(d3)?))
    }

    fn pack(&self) -> [u8; 96] {
        let mut res = [0; 96];
        let (d1, d2) = stdx::split_array_mut::<96, 32, 64>(&mut res);
        let (d2, d3) = stdx::split_array_mut::<64, 32, 32>(d2);
        *d1 = self.0.pack();
        *d2 = self.1.pack();
        *d3 = self.2.pack();
        res
    }
}

impl PublicKey {
    /// Create the implicit public key from an implicit account ID.
    ///
    /// Returns `ImplicitPublicKeyError::AccountIsNotImplicit` if the given
    /// account id is not a valid implicit account ID.
    /// See [`near_account_id::AccountId#is_implicit`] for the definition.
    pub fn from_implicit_account(
        account_id: &near_account_id::AccountId,
    ) -> Result<Self, ImplicitPublicKeyError> {
        if !account_id.is_implicit() {
            return Err(ImplicitPublicKeyError::AccountIsNotImplicit {
                account_id: account_id.clone(),
            });
        }
        let mut public_key_data = Vec::with_capacity(33);
        public_key_data.push(KeyType::ED25519 as u8);
        public_key_data.extend(
            hex::decode(account_id.as_ref().as_bytes())
                .expect("account id was a valid hex of length 64 resulting in 32 bytes"),
        );
        debug_assert_eq!(public_key_data.len(), 33);
        let public_key = PublicKey::try_from_slice(&public_key_data)
            .expect("we should be able to deserialize ED25519 public key");
        Ok(public_key)
    }
}

macro_rules! unwrap_or_return_false {
    ($e:expr) => {
        match $e {
            ::std::option::Option::Some(v) => v,
            ::std::option::Option::None => return false,
        }
    };
}

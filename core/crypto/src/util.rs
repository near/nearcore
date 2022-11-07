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
        // TODO(mina86): Use split_array_ref once stabilised.
        let d1 = unpack((&data[..32]).try_into().unwrap())?;
        let d2 = unpack((&data[32..]).try_into().unwrap())?;
        Some((d1, d2))
    }

    fn pack(&self) -> [u8; 64] {
        let mut res = [0; 64];
        // TODO(mina86): Use split_array_mut once stabilised.
        *<&mut [u8; 32]>::try_from(&mut res[..32]).unwrap() = self.0.pack();
        *<&mut [u8; 32]>::try_from(&mut res[32..]).unwrap() = self.1.pack();
        res
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
        // TODO(mina86): Use split_array_ref once stabilised.
        let d1 = unpack((&data[..32]).try_into().unwrap())?;
        let d2 = unpack((&data[32..64]).try_into().unwrap())?;
        let d3 = unpack((&data[64..]).try_into().unwrap())?;
        Some((d1, d2, d3))
    }

    fn pack(&self) -> [u8; 96] {
        let mut res = [0; 96];
        // TODO(mina86): Use split_array_mut once stabilised.
        *<&mut [u8; 32]>::try_from(&mut res[..32]).unwrap() = self.0.pack();
        *<&mut [u8; 32]>::try_from(&mut res[32..64]).unwrap() = self.1.pack();
        *<&mut [u8; 32]>::try_from(&mut res[64..]).unwrap() = self.2.pack();
        res
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

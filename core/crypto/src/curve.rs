use curve25519_dalek::ristretto::CompressedRistretto;
use curve25519_dalek::traits::VartimeMultiscalarMul;

pub use curve25519_dalek::ristretto::RistrettoPoint as Point;
pub use curve25519_dalek::scalar::Scalar;

pub trait Packable: Sized {
    type Packed;
    fn unpack(data: &Self::Packed) -> Option<Self>;
    fn pack(&self) -> Self::Packed;
}

macro_rules! try_unpack {
    ($data:expr) => {
        try_unpack!($data, ::std::default::Default::default())
    };
    ($data:expr, $r:expr) => {
        match $crate::curve::Packable::unpack($data) {
            Some(val) => val,
            None => return $r,
        }
    };
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
        let (d1, d2) = array_refs!(data, 32, 32);
        Some((try_unpack!(d1), try_unpack!(d2)))
    }

    fn pack(&self) -> [u8; 64] {
        let mut res = [0; 64];
        let (d1, d2) = mut_array_refs!(&mut res, 32, 32);
        *d1 = self.0.pack();
        *d2 = self.1.pack();
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
        let (d1, d2, d3) = array_refs!(data, 32, 32, 32);
        Some((try_unpack!(d1), try_unpack!(d2), try_unpack!(d3)))
    }

    fn pack(&self) -> [u8; 96] {
        let mut res = [0; 96];
        let (d1, d2, d3) = mut_array_refs!(&mut res, 32, 32, 32);
        *d1 = self.0.pack();
        *d2 = self.1.pack();
        *d3 = self.2.pack();
        res
    }
}

pub fn vmul2(s1: Scalar, p1: &Point, s2: Scalar, p2: &Point) -> Point {
    Point::vartime_multiscalar_mul(&[s1, s2], [p1, p2].iter().copied())
}

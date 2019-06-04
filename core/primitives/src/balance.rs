use std::convert::TryFrom;
use std::fmt;
use std::io::Cursor;
use std::ops::{Add, AddAssign, Div, Mul, Sub, SubAssign};

use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use protobuf::SingularPtrField;

use near_protos::uint128 as uint128_proto;

/// Monetary balance of an account or an amount for transfer.
#[derive(Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Hash, Clone, Debug, Copy)]
pub struct Balance(pub u128);

impl AddAssign for Balance {
    fn add_assign(&mut self, rhs: Balance) {
        self.0 += rhs.0;
    }
}

impl SubAssign for Balance {
    fn sub_assign(&mut self, rhs: Balance) {
        self.0 -= rhs.0;
    }
}

impl Add for Balance {
    type Output = Self;

    fn add(self, rhs: Balance) -> Self::Output {
        Balance(self.0 + rhs.0)
    }
}

impl Sub for Balance {
    type Output = Self;

    fn sub(self, rhs: Balance) -> Self::Output {
        Balance(self.0 - rhs.0)
    }
}

impl Div for Balance {
    type Output = Self;

    fn div(self, rhs: Balance) -> Self::Output {
        Balance(self.0 / rhs.0)
    }
}

impl Mul for Balance {
    type Output = Self;

    fn mul(self, rhs: Balance) -> Self::Output {
        Balance(self.0 * rhs.0)
    }
}

impl Mul<Balance> for u128 {
    type Output = Balance;

    fn mul(self, rhs: Balance) -> Self::Output {
        Balance(self * rhs.0)
    }
}

impl<'a> std::iter::Sum<&'a Balance> for Balance {
    fn sum<I: Iterator<Item = &'a Balance>>(iter: I) -> Self {
        Balance(iter.map(|x| x.0).sum())
    }
}

impl From<Balance> for u128 {
    fn from(value: Balance) -> Self {
        value.0
    }
}

impl From<u128> for Balance {
    fn from(value: u128) -> Self {
        Balance(value)
    }
}

impl From<u64> for Balance {
    fn from(value: u64) -> Self {
        Balance(value as u128)
    }
}

impl From<u32> for Balance {
    fn from(value: u32) -> Self {
        Balance(value as u128)
    }
}

impl TryFrom<uint128_proto::Uint128> for Balance {
    type Error = Box<std::error::Error>;

    fn try_from(value: uint128_proto::Uint128) -> Result<Self, Self::Error> {
        if value.number.len() != 16 {
            return Err(format!(
                "uint128 proto has {} bytes, but expected 16.",
                value.number.len()
            )
            .into());
        }
        let mut rdr = Cursor::new(value.number);
        Ok(Balance(rdr.read_uint128::<LittleEndian>(16)?))
    }
}

impl TryFrom<SingularPtrField<uint128_proto::Uint128>> for Balance {
    type Error = Box<std::error::Error>;

    fn try_from(value: SingularPtrField<uint128_proto::Uint128>) -> Result<Self, Self::Error> {
        let t: Result<uint128_proto::Uint128, std::io::Error> = value
            .into_option()
            .ok_or(std::io::Error::new(std::io::ErrorKind::Other, "Missing bytes for uint128"));
        Balance::try_from(t?)
    }
}

impl From<Balance> for uint128_proto::Uint128 {
    fn from(value: Balance) -> Self {
        uint128_proto::Uint128 {
            number: value.0.to_le_bytes().to_vec(),
            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

impl From<Balance> for SingularPtrField<uint128_proto::Uint128> {
    fn from(value: Balance) -> Self {
        SingularPtrField::some(value.into())
    }
}

impl fmt::Display for Balance {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

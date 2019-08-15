use std::io::{Cursor, Error, Read};
use std::mem::size_of;

/// A data-structure that can be de-serialized from binary format by NBOR.
pub trait Deserializable: Sized {
    fn read<R: Read>(reader: &mut R) -> Result<Self, Error>;

    /// Deserialize this instance from a slice of bytes.
    fn from_slice(v: &[u8]) -> Result<Self, Error> {
        let mut c = Cursor::new(v);
        Self::read(&mut c)
    }
}

macro_rules! impl_for_integer {
    ($type: ident) => {
        impl Deserializable for $type {
            fn read<R: Read>(reader: &mut R) -> Result<Self, Error> {
                let mut data = [0u8; size_of::<$type>()];
                reader.read_exact(&mut data)?;
                Ok($type::from_le_bytes(data))
            }
        }
    };
}

impl_for_integer!(i8);
impl_for_integer!(i16);
impl_for_integer!(i32);
impl_for_integer!(i64);
impl_for_integer!(i128);
impl_for_integer!(isize);
impl_for_integer!(u8);
impl_for_integer!(u16);
impl_for_integer!(u32);
impl_for_integer!(u64);
impl_for_integer!(u128);
impl_for_integer!(usize);

// Note NaNs have a portability issue. Specifically, signalling NaNs on MIPS are quiet NaNs on x86,
// and vice-versa. We disallow NaNs to avoid this issue.
macro_rules! impl_for_float {
    ($type: ident, $int_type: ident) => {
        impl Deserializable for $type {
            fn read<R: Read>(reader: &mut R) -> Result<Self, Error> {
                let mut data = [0u8; size_of::<$type>()];
                reader.read_exact(&mut data)?;
                let res = $type::from_bits($int_type::from_le_bytes(data));
                assert!(
                    !res.is_nan(),
                    "For portability reasons we do not allow to deserialize NaNs."
                );
                Ok(res)
            }
        }
    };
}

impl_for_float!(f32, u32);
impl_for_float!(f64, u64);

impl<T> Deserializable for Option<T>
where
    T: Deserializable,
{
    fn read<R: Read>(reader: &mut R) -> Result<Self, Error> {
        let mut flag = [0u8];
        reader.read_exact(&mut flag)?;
        if flag[0] == 0 {
            Ok(None)
        } else {
            Ok(Some(T::read(reader)?))
        }
    }
}

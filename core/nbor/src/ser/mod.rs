use std::io::{Error, Write};

/// A data-structure that can be serialized into binary format by NBOR.
pub trait Serializable {
    fn write<W: Write>(&self, writer: &mut W) -> Result<(), Error>;

    /// Serialize this instance into a vector of bytes.
    fn to_vec(&self) -> Result<Vec<u8>, Error> {
        let mut result = vec![];
        self.write(&mut result)?;
        Ok(result)
    }
}

macro_rules! impl_for_integer {
    ($type: ident) => {
        impl Serializable for $type {
            fn write<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
                writer.write(&self.to_le_bytes()).map(|_| ())
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
    ($type: ident) => {
        impl Serializable for $type {
            fn write<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
                assert!(
                    !self.is_nan(),
                    "For portability reasons we do not allow to serialize NaNs."
                );
                writer.write(&self.to_bits().to_le_bytes()).map(|_| ())
            }
        }
    };
}

impl_for_float!(f32);
impl_for_float!(f64);

impl<T> Serializable for Option<T>
where
    T: Serializable,
{
    fn write<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        match self {
            None => writer.write(&[0u8]).map(|_| ()),
            Some(value) => {
                writer.write(&[1u8])?;
                value.write(writer)
            }
        }
    }
}

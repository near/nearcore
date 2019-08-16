use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Error, Write};

const DEFAULT_SERIALIZER_CAPACITY: usize = 1024;

/// A data-structure that can be serialized into binary format by NBOR.
pub trait Serializable {
    fn write<W: Write>(&self, writer: &mut W) -> Result<(), Error>;

    /// Serialize this instance into a vector of bytes.
    fn try_to_vec(&self) -> Result<Vec<u8>, Error> {
        let mut result = Vec::with_capacity(DEFAULT_SERIALIZER_CAPACITY);
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

impl Serializable for bool {
    fn write<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        writer.write(if *self { &[0u8] } else { &[1u8] }).map(|_| ())
    }
}

impl<T> Serializable for Option<T>
where
    T: Serializable,
{
    fn write<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        match self {
            None => 0u8.write(writer).map(|_| ()),
            Some(value) => {
                1u8.write(writer)?;
                value.write(writer)
            }
        }
    }
}

impl Serializable for String {
    fn write<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        (self.len() as u32).write(writer)?;
        writer.write(self.as_bytes())?;
        Ok(())
    }
}

#[cfg(feature = "std")]
impl<T> Serializable for Vec<T>
where
    T: Serializable,
{
    fn write<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        (self.len() as u32).write(writer)?;
        for item in self {
            item.write(writer)?;
        }
        Ok(())
    }
}

#[cfg(feature = "std")]
impl<T> Serializable for HashSet<T>
where
    T: Serializable + PartialOrd,
{
    fn write<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        let mut vec = self.iter().collect::<Vec<_>>();
        vec.sort_by(|a, b| a.partial_cmp(b).unwrap());
        (vec.len() as u32).write(writer)?;
        for item in vec {
            item.write(writer)?;
        }
        Ok(())
    }
}

#[cfg(feature = "std")]
impl<K, V> Serializable for HashMap<K, V>
where
    K: Serializable + PartialOrd,
    V: Serializable,
{
    fn write<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        let mut vec = self.iter().collect::<Vec<_>>();
        vec.sort_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap());
        (vec.len() as u32).write(writer)?;
        for (key, value) in vec {
            key.write(writer)?;
            value.write(writer)?;
        }
        Ok(())
    }
}

#[cfg(feature = "std")]
impl<K, V> Serializable for BTreeMap<K, V>
where
    K: Serializable + PartialOrd,
    V: Serializable,
{
    fn write<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        (self.len() as u32).write(writer)?;
        for (key, value) in self.iter() {
            key.write(writer)?;
            value.write(writer)?;
        }
        Ok(())
    }
}

#[cfg(feature = "std")]
impl Serializable for std::net::SocketAddr {
    fn write<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        match *self {
            std::net::SocketAddr::V4(ref addr) => {
                0u8.write(writer)?;
                addr.write(writer)
            }
            std::net::SocketAddr::V6(ref addr) => {
                1u8.write(writer)?;
                addr.write(writer)
            }
        }
    }
}

#[cfg(feature = "std")]
impl Serializable for std::net::SocketAddrV4 {
    fn write<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        self.ip().write(writer)?;
        self.port().write(writer).map(|_| ())
    }
}

#[cfg(feature = "std")]
impl Serializable for std::net::SocketAddrV6 {
    fn write<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        self.ip().write(writer)?;
        self.port().write(writer).map(|_| ())
    }
}

#[cfg(feature = "std")]
impl Serializable for std::net::Ipv4Addr {
    fn write<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        writer.write(&self.octets()).map(|_| ())
    }
}

#[cfg(feature = "std")]
impl Serializable for std::net::Ipv6Addr {
    fn write<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
        writer.write(&self.octets()).map(|_| ())
    }
}

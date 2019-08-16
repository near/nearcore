use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Cursor, Error, Read};
use std::mem::size_of;

/// A data-structure that can be de-serialized from binary format by NBOR.
pub trait Deserializable: Sized {
    fn read<R: Read>(reader: &mut R) -> Result<Self, Error>;

    /// Deserialize this instance from a slice of bytes.
    fn try_from_slice(v: &[u8]) -> Result<Self, Error> {
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

impl Deserializable for bool {
    fn read<R: Read>(reader: &mut R) -> Result<Self, Error> {
        let mut buf = [0u8];
        reader.read(&mut buf)?;
        Ok(buf[0] == 1)
    }
}

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

impl Deserializable for String {
    fn read<R: Read>(reader: &mut R) -> Result<Self, Error> {
        let len = u32::read(reader)?;
        let mut result = vec![0; len as usize];
        reader.read(&mut result)?;
        String::from_utf8(result)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err.to_string()))
    }
}

#[cfg(feature = "std")]
impl<T> Deserializable for Vec<T>
where
    T: Deserializable,
{
    fn read<R: Read>(reader: &mut R) -> Result<Self, Error> {
        let len = u32::read(reader)?;
        let mut result = Vec::with_capacity(len as usize);
        for _ in 0..len {
            result.push(T::read(reader)?);
        }
        Ok(result)
    }
}

#[cfg(feature = "std")]
impl<T> Deserializable for HashSet<T>
where
    T: Deserializable + Eq + std::hash::Hash,
{
    fn read<R: Read>(reader: &mut R) -> Result<Self, Error> {
        let vec = <Vec<T>>::read(reader)?;
        Ok(vec.into_iter().collect::<HashSet<T>>())
    }
}

#[cfg(feature = "std")]
impl<K, V> Deserializable for HashMap<K, V>
where
    K: Deserializable + Eq + std::hash::Hash,
    V: Deserializable,
{
    fn read<R: Read>(reader: &mut R) -> Result<Self, Error> {
        let len = u32::read(reader)?;
        let mut result = HashMap::with_capacity(len as usize);
        for _ in 0..len {
            let key = K::read(reader)?;
            let value = V::read(reader)?;
            result.insert(key, value);
        }
        Ok(result)
    }
}

#[cfg(feature = "std")]
impl<K, V> Deserializable for BTreeMap<K, V>
where
    K: Deserializable + Ord + std::hash::Hash,
    V: Deserializable,
{
    fn read<R: Read>(reader: &mut R) -> Result<Self, Error> {
        let len = u32::read(reader)?;
        let mut result = BTreeMap::new();
        for _ in 0..len {
            let key = K::read(reader)?;
            let value = V::read(reader)?;
            result.insert(key, value);
        }
        Ok(result)
    }
}

#[cfg(feature = "std")]
impl Deserializable for std::net::SocketAddr {
    fn read<R: Read>(reader: &mut R) -> Result<Self, Error> {
        let kind = u8::read(reader)?;
        match kind {
            0 => std::net::SocketAddrV4::read(reader).map(|addr| std::net::SocketAddr::V4(addr)),
            1 => std::net::SocketAddrV6::read(reader).map(|addr| std::net::SocketAddr::V6(addr)),
            value => panic!(format!("Invalid SocketAddr variant: {}", value)),
        }
    }
}

#[cfg(feature = "std")]
impl Deserializable for std::net::SocketAddrV4 {
    fn read<R: Read>(reader: &mut R) -> Result<Self, Error> {
        let ip = std::net::Ipv4Addr::read(reader)?;
        let port = u16::read(reader)?;
        Ok(std::net::SocketAddrV4::new(ip, port))
    }
}

#[cfg(feature = "std")]
impl Deserializable for std::net::SocketAddrV6 {
    fn read<R: Read>(reader: &mut R) -> Result<Self, Error> {
        let ip = std::net::Ipv6Addr::read(reader)?;
        let port = u16::read(reader)?;
        Ok(std::net::SocketAddrV6::new(ip, port, 0, 0))
    }
}

#[cfg(feature = "std")]
impl Deserializable for std::net::Ipv4Addr {
    fn read<R: Read>(reader: &mut R) -> Result<Self, Error> {
        let mut buf = [0u8; 4];
        reader.read(&mut buf)?;
        Ok(std::net::Ipv4Addr::from(buf))
    }
}

#[cfg(feature = "std")]
impl Deserializable for std::net::Ipv6Addr {
    fn read<R: Read>(reader: &mut R) -> Result<Self, Error> {
        let mut buf = [0u8; 16];
        reader.read(&mut buf)?;
        Ok(std::net::Ipv6Addr::from(buf))
    }
}

use std::io;

use serde::{de::DeserializeOwned, Serialize};

pub type EncodeResult = Result<Vec<u8>, io::Error>;
pub type DecodeResult<T> = Result<T, io::Error>;

// encode a type to byte array
pub trait Encode {
    fn encode(&self) -> EncodeResult;
}

// decode from byte array
pub trait Decode: Sized {
    fn decode(data: &[u8]) -> DecodeResult<Self>;
}

impl<T: Serialize> Encode for T {
    fn encode(&self) -> EncodeResult {
        bincode::serialize(&self)
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Failed to serialize"))
    }
}

impl<T> Decode for T
where
    T: DeserializeOwned,
{
    fn decode(data: &[u8]) -> DecodeResult<Self> {
        bincode::deserialize(data)
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Failed to deserialize"))
    }
}

pub fn to_base64<T: ?Sized + AsRef<[u8]>>(input: &T) -> String {
    base64::encode(input)
}

pub fn from_base64(s: &str) -> Result<Vec<u8>, Box<std::error::Error>> {
    base64::decode(s).map_err(|err| err.into())
}

pub fn from_base64_buf(s: &str, buffer: &mut Vec<u8>) -> Result<(), Box<std::error::Error>> {
    base64::decode_config_buf(s, base64::STANDARD, buffer).map_err(|err| err.into())
}

pub mod base64_format {
    use std::convert::TryFrom;

    use serde::de;
    use serde::{Deserialize, Deserializer, Serializer};

    use super::{from_base64_buf, to_base64};

    pub fn serialize<T, S>(data: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: AsRef<[u8]>,
        S: Serializer,
    {
        serializer.serialize_str(&to_base64(data))
    }

    pub fn deserialize<'de, T, D, E>(deserializer: D) -> Result<T, D::Error>
    where
        T: TryFrom<Vec<u8>, Error=E>,
        E: ToString,
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let mut array = Vec::with_capacity(32);
        match from_base64_buf(&s, &mut array) {
            Ok(_) => T::try_from(array).map_err(|err| de::Error::custom(err.to_string())),
            Err(err) => Err(de::Error::custom(err.to_string())),
        }
    }
}

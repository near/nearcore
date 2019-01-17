use serde::{de::DeserializeOwned, Serialize};

pub type EncodeType = Result<Vec<u8>, String>;
pub type DecodeType<T> = Result<T, String>;

// encode a type to byte array
pub trait Encode {
    fn encode(&self) -> EncodeType;
}

// decode from byte array
pub trait Decode: Sized {
    fn decode(data: &[u8]) -> DecodeType<Self>;
}

impl<T> Encode for T
    where
        T: Serialize,
{
    fn encode(&self) -> EncodeType {
        bincode::serialize(&self).map_err(|_| "Failed to serialize".to_string())
    }
}

impl<T> Decode for T
    where
        T: DeserializeOwned,
{
    fn decode(data: &[u8]) -> DecodeType<Self> {
        bincode::deserialize(data).map_err(|_| "Failed to deserialize".to_string())
    }
}

use serde::{de::DeserializeOwned, Serialize};

pub type EncodeResult = Result<Vec<u8>, String>;
pub type DecodeResult<T> = Result<T, String>;

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
        bincode::serialize(&self).map_err(|_| "Failed to serialize".to_string())
    }
}

impl<T> Decode for T
    where
        T: DeserializeOwned,
{
    fn decode(data: &[u8]) -> DecodeResult<Self> {
        bincode::deserialize(data).map_err(|_| "Failed to deserialize".to_string())
    }
}

impl Encode for protobuf::Message {
    fn encode(&self) -> EncodeResult {
        let mut bytes = Vec::new();
        self.write_to_writer(&mut bytes).map_err(|_| "Protobuf write failed")?;
        Ok(bytes)
    }
}

//impl<T> Decode for T where T: protobuf::Message {
//    fn decode(bytes: &[u8]) -> DecodeResult<Self> {
//        let m = Self::new();
//        m.merge_from_bytes(&bytes)?;
//        Ok(m)
//    }
//}

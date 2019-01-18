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

pub mod proto_format {
    use super::{Encode, Decode};
    use base64;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S, T>(s: T, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer, T: Encode {
        let bytes = s.encode().map_err(|_| serde::ser::Error::custom("Encoding proto failed".to_string()))?;
        serializer.serialize_str(&base64::encode(&bytes))
    }

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
        where D: Deserializer<'de>, T: Decode {
        let s = String::deserialize(deserializer)?;
        let bytes = base64::decode(&s).map_err(|_| serde::de::Error::custom("Decoding base64 failed".to_string()))?;
        Ok(T::decode(&bytes).map_err(|_| serde::de::Error::custom("Decoding proto failed"))?)
    }
}

//impl<T> Decode for T where T: protobuf::Message {
//    fn decode(bytes: &[u8]) -> DecodeResult<Self> {
//        let m = Self::new();
//        m.merge_from_bytes(&bytes)?;
//        Ok(m)
//    }
//}

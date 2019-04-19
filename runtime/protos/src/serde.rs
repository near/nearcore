pub mod b64_format {
    use base64;
    use serde::{Deserialize, Deserializer, Serializer};
    use protobuf::Message;

    pub fn serialize<S, T>(s: &T, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer,
              T: Message,
    {
        let bytes = s.write_to_bytes().map_err(serde::ser::Error::custom)?;
        let encoded = base64::encode(&bytes);
        serializer.serialize_str(&encoded)
    }

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
        where D: Deserializer<'de>,
              T: Message,
    {
        let s = String::deserialize(deserializer)?;
        let bytes = base64::decode(&s).map_err(serde::de::Error::custom)?;
        Ok(protobuf::parse_from_bytes(&bytes).map_err(serde::de::Error::custom)?)
    }
}

/// Serialize `Vec<u8>` as `String`.
pub mod bytes_as_str {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(arr: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&String::from_utf8(arr.clone()).unwrap())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(s.into_bytes())
    }
}

/// Serialize `Vec<u8>` as base58 encoding.
pub mod bytes_as_base58 {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(arr: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&bs58::encode(arr).into_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(bs58::decode(s).into_vec().expect("Failed to deserialize base58 string"))
    }
}

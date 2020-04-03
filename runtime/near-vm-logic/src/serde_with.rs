/// Serialize `Vec<u8>` as base64 encoding.
pub mod bytes_as_base64 {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(arr: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&base64::encode(arr))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(base64::decode(&s).expect("Failed to deserialize base64 string"))
    }
}

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

/// Serialize `Vec<Vec<u8>>` as `Vec<String>`.
pub mod vec_bytes_as_str {
    use std::fmt;

    use serde::de::{SeqAccess, Visitor};
    use serde::ser::SerializeSeq;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(data: &Vec<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(data.len()))?;
        for v in data {
            seq.serialize_element(&String::from_utf8(v.clone()).unwrap())?;
        }
        seq.end()
    }

    struct VecBytesVisitor;

    impl<'de> Visitor<'de> for VecBytesVisitor {
        type Value = Vec<Vec<u8>>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("an array with string in the first element")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Vec<Vec<u8>>, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut vec = Vec::new();
            while let Some(s) = seq.next_element::<String>()? {
                vec.push(s.into_bytes());
            }
            Ok(vec)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(VecBytesVisitor {})
    }
}

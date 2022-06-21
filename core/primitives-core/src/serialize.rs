pub fn to_base<T: AsRef<[u8]>>(input: T) -> String {
    bs58::encode(input).into_string()
}

pub fn from_base(s: &str) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    bs58::decode(s).into_vec().map_err(|err| err.into())
}

pub fn to_base64<T: AsRef<[u8]>>(input: T) -> String {
    base64::encode(&input)
}

pub fn from_base64(s: &str) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    base64::decode(s).map_err(|err| err.into())
}

pub fn from_base_buf(
    s: &str,
    buffer: &mut Vec<u8>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match bs58::decode(s).into(buffer) {
        Ok(_) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

pub trait BaseEncode {
    fn to_base(&self) -> String;
}

impl<T> BaseEncode for T
where
    for<'a> &'a T: Into<Vec<u8>>,
{
    fn to_base(&self) -> String {
        to_base(&self.into())
    }
}

pub mod base64_format {
    use serde::de;
    use serde::{Deserialize, Deserializer, Serializer};

    use super::{from_base64, to_base64};

    pub fn serialize<S, T>(data: T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: AsRef<[u8]>,
    {
        serializer.serialize_str(&to_base64(data))
    }

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
        T: From<Vec<u8>>,
    {
        let s = String::deserialize(deserializer)?;
        from_base64(&s).map_err(|err| de::Error::custom(err.to_string())).map(Into::into)
    }
}

pub mod option_base64_format {
    use serde::de;
    use serde::{Deserialize, Deserializer, Serializer};

    use super::{from_base64, to_base64};

    pub fn serialize<S>(data: &Option<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Some(ref bytes) = data {
            serializer.serialize_str(&to_base64(bytes))
        } else {
            serializer.serialize_none()
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: Option<String> = Option::deserialize(deserializer)?;
        if let Some(s) = s {
            Ok(Some(from_base64(&s).map_err(|err| de::Error::custom(err.to_string()))?))
        } else {
            Ok(None)
        }
    }
}

pub mod base_bytes_format {
    use serde::de;
    use serde::{Deserialize, Deserializer, Serializer};

    use super::{from_base, to_base};

    pub fn serialize<S>(data: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&to_base(data))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        from_base(&s).map_err(|err| de::Error::custom(err.to_string()))
    }
}

pub mod u64_dec_format {
    use serde::de;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(num: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}", num))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        u64::from_str_radix(&s, 10).map_err(de::Error::custom)
    }
}

pub mod u128_dec_format {
    use serde::de;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(num: &u128, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}", num))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u128, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        u128::from_str_radix(&s, 10).map_err(de::Error::custom)
    }
}

pub mod u128_dec_format_compatible {
    //! This in an extension to `u128_dec_format` that serves a compatibility layer role to
    //! deserialize u128 from a "small" JSON number (u64).
    //!
    //! It is unfortunate that we cannot enable "arbitrary_precision" feature in
    //! serde_json due to a bug: <https://github.com/serde-rs/json/issues/505>.
    use serde::{de, Deserialize, Deserializer};

    pub use super::u128_dec_format::serialize;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum U128 {
        Number(u64),
        String(String),
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u128, D::Error>
    where
        D: Deserializer<'de>,
    {
        match U128::deserialize(deserializer)? {
            U128::Number(value) => Ok(u128::from(value)),
            U128::String(value) => u128::from_str_radix(&value, 10).map_err(de::Error::custom),
        }
    }
}

pub mod option_u128_dec_format {
    use serde::de;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(data: &Option<u128>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Some(ref num) = data {
            serializer.serialize_str(&format!("{}", num))
        } else {
            serializer.serialize_none()
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<u128>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: Option<String> = Option::deserialize(deserializer)?;
        if let Some(s) = s {
            Ok(Some(u128::from_str_radix(&s, 10).map_err(de::Error::custom)?))
        } else {
            Ok(None)
        }
    }
}

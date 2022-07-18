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

pub trait BaseDecode:
    for<'a> TryFrom<&'a [u8], Error = Box<dyn std::error::Error + Send + Sync>>
{
    fn from_base(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let bytes = from_base(s)?;
        Self::try_from(&bytes)
    }
}

/// Serialize `Vec<u8>` as `String`.
///
/// Fails during serialisation if the buffer does not contain valid UTF-8
/// string.
pub mod bytes_as_str {
    use serde::{ser, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(arr: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(std::str::from_utf8(arr).map_err(ser::Error::custom)?)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(s.into_bytes())
    }
}

#[test]
fn test_bytes_as_str() {
    #[derive(PartialEq, Debug, serde::Deserialize, serde::Serialize)]
    struct Test {
        #[serde(with = "bytes_as_str")]
        field: Vec<u8>,
    }

    assert_round_trip("{\"field\":\"\"}", Test { field: b"".to_vec() });
    assert_round_trip("{\"field\":\"foo\"}", Test { field: b"foo".to_vec() });
    assert_ser_error(Test { field: b"\xff".to_vec() });
}

/// Serialize `Vec<Vec<u8>>` as `Vec<String>`.
///
/// Fails during serialisation if the buffer does not contain valid UTF-8
/// string.
pub mod vec_bytes_as_str {
    use std::fmt;

    use serde::de::{SeqAccess, Visitor};
    use serde::ser::{self, SerializeSeq};
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(data: &Vec<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(data.len()))?;
        for v in data {
            seq.serialize_element(&std::str::from_utf8(v.as_slice()).map_err(ser::Error::custom)?)?;
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

#[test]
fn test_vec_bytes_as_str() {
    #[derive(PartialEq, Debug, serde::Deserialize, serde::Serialize)]
    struct Test {
        #[serde(with = "vec_bytes_as_str")]
        field: Vec<Vec<u8>>,
    }

    assert_round_trip("{\"field\":[]}", Test { field: vec![] });
    let field = vec![b"foo".to_vec(), b"bar".to_vec(), b"baz".to_vec()];
    assert_round_trip("{\"field\":[\"foo\",\"bar\",\"baz\"]}", Test { field });
    assert_ser_error(Test { field: vec![b"foo".to_vec(), b"\xff".to_vec()] });
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

#[test]
fn test_base64_format() {
    #[derive(PartialEq, Debug, serde::Deserialize, serde::Serialize)]
    struct Test {
        #[serde(with = "base64_format")]
        field: Vec<u8>,
    }

    assert_round_trip("{\"field\":\"Zm9v\"}", Test { field: b"foo".to_vec() });
    assert_de_error::<Test>("{\"field\":null}");
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

#[test]
fn test_option_base64_format() {
    #[derive(PartialEq, Debug, serde::Deserialize, serde::Serialize)]
    struct Test {
        #[serde(with = "option_base64_format")]
        field: Option<Vec<u8>>,
    }

    assert_round_trip("{\"field\":\"Zm9v\"}", Test { field: Some(b"foo".to_vec()) });
    assert_round_trip("{\"field\":null}", Test { field: None });
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

#[test]
fn test_base_bytes_format() {
    #[derive(PartialEq, Debug, serde::Deserialize, serde::Serialize)]
    struct Test {
        #[serde(with = "base_bytes_format")]
        field: Vec<u8>,
    }

    assert_round_trip("{\"field\":\"bQbp\"}", Test { field: b"foo".to_vec() });
    assert_de_error::<Test>("{\"field\":null}");
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

#[test]
fn test_u64_dec_format() {
    #[derive(PartialEq, Debug, serde::Deserialize, serde::Serialize)]
    struct Test {
        #[serde(with = "u64_dec_format")]
        field: u64,
    }

    assert_round_trip("{\"field\":\"42\"}", Test { field: 42 });
    assert_round_trip("{\"field\":\"18446744073709551615\"}", Test { field: u64::MAX });
    assert_de_error::<Test>("{\"field\":18446744073709551616}");
    assert_de_error::<Test>("{\"field\":\"18446744073709551616\"}");
    assert_de_error::<Test>("{\"field\":42}");
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

#[test]
fn test_u128_dec_format() {
    #[derive(PartialEq, Debug, serde::Deserialize, serde::Serialize)]
    struct Test {
        #[serde(with = "u128_dec_format")]
        field: u128,
    }

    assert_round_trip("{\"field\":\"42\"}", Test { field: 42 });
    assert_round_trip("{\"field\":\"18446744073709551615\"}", Test { field: u64::MAX as u128 });
    assert_round_trip("{\"field\":\"18446744073709551616\"}", Test { field: 18446744073709551616 });
    assert_de_error::<Test>("{\"field\":42}");
    assert_de_error::<Test>("{\"field\":null}");
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

#[test]
fn test_u128_dec_format_compatible() {
    #[derive(PartialEq, Debug, serde::Deserialize, serde::Serialize)]
    struct Test {
        #[serde(with = "u128_dec_format_compatible")]
        field: u128,
    }

    assert_round_trip("{\"field\":\"42\"}", Test { field: 42 });
    assert_round_trip("{\"field\":\"18446744073709551615\"}", Test { field: u64::MAX as u128 });
    assert_round_trip("{\"field\":\"18446744073709551616\"}", Test { field: 18446744073709551616 });
    assert_deserialise("{\"field\":42}", Test { field: 42 });
    assert_de_error::<Test>("{\"field\":null}");
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

#[test]
fn test_option_u128_dec_format() {
    #[derive(PartialEq, Debug, serde::Deserialize, serde::Serialize)]
    struct Test {
        #[serde(with = "option_u128_dec_format")]
        field: Option<u128>,
    }

    assert_round_trip("{\"field\":null}", Test { field: None });
    assert_round_trip("{\"field\":\"42\"}", Test { field: Some(42) });
    assert_round_trip(
        "{\"field\":\"18446744073709551615\"}",
        Test { field: Some(u64::MAX as u128) },
    );
    assert_round_trip(
        "{\"field\":\"18446744073709551616\"}",
        Test { field: Some(18446744073709551616) },
    );
    assert_de_error::<Test>("{\"field\":42}");
}

#[cfg(test)]
#[track_caller]
fn assert_round_trip<'a, T>(serialised: &'a str, obj: T)
where
    T: serde::Deserialize<'a> + serde::Serialize + std::fmt::Debug + std::cmp::PartialEq,
{
    assert_eq!(serialised, serde_json::to_string(&obj).unwrap());
    assert_eq!(obj, serde_json::from_str(serialised).unwrap());
}

#[cfg(test)]
#[track_caller]
fn assert_deserialise<'a, T>(serialised: &'a str, obj: T)
where
    T: serde::Deserialize<'a> + std::fmt::Debug + std::cmp::PartialEq,
{
    assert_eq!(obj, serde_json::from_str(serialised).unwrap());
}

#[cfg(test)]
#[track_caller]
fn assert_ser_error<T: serde::Serialize + std::fmt::Debug>(obj: T) {
    serde_json::to_string(&obj).unwrap_err();
}

#[cfg(test)]
#[track_caller]
fn assert_de_error<'a, T: serde::Deserialize<'a> + std::fmt::Debug>(serialised: &'a str) {
    serde_json::from_str::<T>(serialised).unwrap_err();
}

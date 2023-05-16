pub fn to_base64(input: &[u8]) -> String {
    base64::encode(input)
}

pub fn base64_display(input: &[u8]) -> base64::display::Base64Display<'_> {
    base64::display::Base64Display::with_config(input, base64::STANDARD)
}

pub fn from_base64(encoded: &str) -> Result<Vec<u8>, base64::DecodeError> {
    base64::decode(encoded)
}

pub mod base64_format {
    use serde::de::Error;

    pub fn serialize<S, T>(data: T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
        T: AsRef<[u8]>,
    {
        serializer.collect_str(&super::base64_display(data.as_ref()))
    }

    struct Visitor;

    impl<'de> serde::de::Visitor<'de> for Visitor {
        type Value = Vec<u8>;

        fn expecting(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
            fmt.write_str("base64-encoded string")
        }

        fn visit_str<E: Error>(self, value: &str) -> Result<Vec<u8>, E> {
            super::from_base64(value).map_err(Error::custom)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(Visitor).map(Into::into)
    }
}

#[derive(PartialEq, Eq, Debug, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct Base64Bytes(#[serde(with = "base64_format")] pub Vec<u8>);

#[test]
fn test_base64_format() {
    assert_round_trip("\"Zm9v\"", Base64Bytes(b"foo".to_vec()));
    assert_de_error::<Base64Bytes>("null");
}

pub mod option_base64_format {
    use serde::de::Error;

    pub fn serialize<S, T>(data: &Option<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
        T: AsRef<[u8]>,
    {
        if let Some(ref bytes) = data {
            serializer.collect_str(&super::base64_display(bytes.as_ref()))
        } else {
            serializer.serialize_none()
        }
    }

    struct Visitor;

    impl<'de> serde::de::Visitor<'de> for Visitor {
        type Value = Option<Vec<u8>>;

        fn expecting(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
            fmt.write_str("base64-encoded string or null")
        }

        fn visit_str<E: Error>(self, value: &str) -> Result<Self::Value, E> {
            super::from_base64(value).map(Option::Some).map_err(Error::custom)
        }

        fn visit_unit<E: Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
    }

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
    where
        D: serde::Deserializer<'de>,
        T: From<Vec<u8>>,
    {
        Ok(deserializer.deserialize_any(Visitor)?.map(T::from))
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

/// Serialises number as a string; deserialises either as a string or number.
///
/// This format works for `u64`, `u128`, `Option<u64>` and `Option<u128>` types.
/// When serialising, numbers are serialised as decimal strings.  When
/// deserialising, strings are parsed as decimal numbers while numbers are
/// interpreted as is.
pub mod dec_format {
    use serde::de;
    use serde::{Deserializer, Serializer};

    #[derive(thiserror::Error, Debug)]
    #[error("cannot parse from unit")]
    pub struct ParseUnitError;

    /// Abstraction between integers that we serialise.
    pub trait DecType: Sized {
        /// Formats number as a decimal string; passes `None` as is.
        fn serialize(&self) -> Option<String>;

        /// Constructs Self from a `null` value.  Returns error if this type
        /// does not accept `null` values.
        fn try_from_unit() -> Result<Self, ParseUnitError> {
            Err(ParseUnitError)
        }

        /// Tries to parse decimal string as an integer.
        fn try_from_str(value: &str) -> Result<Self, std::num::ParseIntError>;

        /// Constructs Self from a 64-bit unsigned integer.
        fn from_u64(value: u64) -> Self;
    }

    impl DecType for u64 {
        fn serialize(&self) -> Option<String> {
            Some(self.to_string())
        }
        fn try_from_str(value: &str) -> Result<Self, std::num::ParseIntError> {
            Self::from_str_radix(value, 10)
        }
        fn from_u64(value: u64) -> Self {
            value
        }
    }

    impl DecType for u128 {
        fn serialize(&self) -> Option<String> {
            Some(self.to_string())
        }
        fn try_from_str(value: &str) -> Result<Self, std::num::ParseIntError> {
            Self::from_str_radix(value, 10)
        }
        fn from_u64(value: u64) -> Self {
            value.into()
        }
    }

    impl<T: DecType> DecType for Option<T> {
        fn serialize(&self) -> Option<String> {
            self.as_ref().and_then(DecType::serialize)
        }
        fn try_from_unit() -> Result<Self, ParseUnitError> {
            Ok(None)
        }
        fn try_from_str(value: &str) -> Result<Self, std::num::ParseIntError> {
            Some(T::try_from_str(value)).transpose()
        }
        fn from_u64(value: u64) -> Self {
            Some(T::from_u64(value))
        }
    }

    struct Visitor<T>(core::marker::PhantomData<T>);

    impl<'de, T: DecType> de::Visitor<'de> for Visitor<T> {
        type Value = T;

        fn expecting(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
            fmt.write_str("a non-negative integer as a string")
        }

        fn visit_unit<E: de::Error>(self) -> Result<T, E> {
            T::try_from_unit().map_err(|_| de::Error::invalid_type(de::Unexpected::Option, &self))
        }

        fn visit_u64<E: de::Error>(self, value: u64) -> Result<T, E> {
            Ok(T::from_u64(value))
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<T, E> {
            T::try_from_str(value).map_err(de::Error::custom)
        }
    }

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
        T: DecType,
    {
        deserializer.deserialize_any(Visitor(Default::default()))
    }

    pub fn serialize<S, T>(num: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: DecType,
    {
        match num.serialize() {
            Some(value) => serializer.serialize_str(&value),
            None => serializer.serialize_none(),
        }
    }
}

#[test]
fn test_u64_dec_format() {
    #[derive(PartialEq, Debug, serde::Deserialize, serde::Serialize)]
    struct Test {
        #[serde(with = "dec_format")]
        field: u64,
    }

    assert_round_trip("{\"field\":\"42\"}", Test { field: 42 });
    assert_round_trip("{\"field\":\"18446744073709551615\"}", Test { field: u64::MAX });
    assert_deserialise("{\"field\":42}", Test { field: 42 });
    assert_de_error::<Test>("{\"field\":18446744073709551616}");
    assert_de_error::<Test>("{\"field\":\"18446744073709551616\"}");
    assert_de_error::<Test>("{\"field\":42.0}");
}

#[test]
fn test_u128_dec_format() {
    #[derive(PartialEq, Debug, serde::Deserialize, serde::Serialize)]
    struct Test {
        #[serde(with = "dec_format")]
        field: u128,
    }

    assert_round_trip("{\"field\":\"42\"}", Test { field: 42 });
    assert_round_trip("{\"field\":\"18446744073709551615\"}", Test { field: u64::MAX as u128 });
    assert_round_trip("{\"field\":\"18446744073709551616\"}", Test { field: 18446744073709551616 });
    assert_deserialise("{\"field\":42}", Test { field: 42 });
    assert_de_error::<Test>("{\"field\":null}");
    assert_de_error::<Test>("{\"field\":42.0}");
}

#[test]
fn test_option_u128_dec_format() {
    #[derive(PartialEq, Debug, serde::Deserialize, serde::Serialize)]
    struct Test {
        #[serde(with = "dec_format")]
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
    assert_deserialise("{\"field\":42}", Test { field: Some(42) });
    assert_de_error::<Test>("{\"field\":42.0}");
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
fn assert_de_error<'a, T: serde::Deserialize<'a> + std::fmt::Debug>(serialised: &'a str) {
    serde_json::from_str::<T>(serialised).unwrap_err();
}

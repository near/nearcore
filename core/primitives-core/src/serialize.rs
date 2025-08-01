use base64::Engine;
use base64::display::Base64Display;
use base64::engine::general_purpose::GeneralPurpose;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde::ser::SerializeStruct;

pub fn to_base64(input: &[u8]) -> String {
    BASE64_STANDARD.encode(input)
}

pub fn base64_display(input: &[u8]) -> Base64Display<'_, 'static, GeneralPurpose> {
    Base64Display::new(input, &BASE64_STANDARD)
}

pub fn from_base64(encoded: &str) -> Result<Vec<u8>, base64::DecodeError> {
    BASE64_STANDARD.decode(encoded)
}

#[cfg(feature = "schemars")]
pub fn base64_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let mut schema = <String as schemars::JsonSchema>::json_schema(generator);
    schema.ensure_object().insert("format".into(), "bytes".into());
    schema
}

/// Serializes number as a string; deserializes either as a string or number.
///
/// This format works for `u64`, `u128`, `Option<u64>` and `Option<u128>` types.
/// When serializing, numbers are serialized as decimal strings.  When
/// deserializing, strings are parsed as decimal numbers while numbers are
/// interpreted as is.
pub mod dec_format {
    use serde::de;
    use serde::{Deserializer, Serializer};

    #[derive(thiserror::Error, Debug)]
    #[error("cannot parse from unit")]
    pub struct ParseUnitError;

    /// Abstraction between integers that we serialize.
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

// pub mod gas_number_serialization {
//     pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
//     where
//         D: Deserializer<'de>,
//         T: DecType,
//     {
//         deserializer.deserialize_any(Visitor(Default::default()))
//     }

//     pub fn serialize<S, T>(num: &T, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//         T: DecType,
//     {

//     }
// }

pub trait NameTrait {
    fn name() -> &'static str;
    fn name_lossless() -> &'static str;
}

pub struct GasNumberSerialization<T: NameTrait> {
    inner: u64,
    name: T,
}

#[macro_export]
macro_rules! gas_field_name {
    ($field:ident) => {
        paste::paste! {
            struct [<$field GasFieldName>];

            impl NameTrait for [<$field GasFieldName>] {
                fn name() -> &'static str {
                    stringify!($field)
                }
                fn name_lossless() -> &'static str {
                    concat!(stringify!($field), "_lossless")
                }
            }
        }
    };
}

impl<T: NameTrait> serde_with::SerializeAs<near_gas::NearGas> for GasNumberSerialization<T> {
    fn serialize_as<S>(source: &near_gas::NearGas, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("GasNumberSerialization", 2)?;
        state.serialize_field(T::name(), &source.as_gas())?;
        state.serialize_field(T::name_lossless(), &source.as_gas().to_string())?;
        state.end()
    }
}

impl<'de, T: NameTrait> serde_with::DeserializeAs<'de, near_gas::NearGas> for GasNumberSerialization<T> {
    fn deserialize_as<D>(deserializer: D) -> Result<near_gas::NearGas, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Helper<S>(core::marker::PhantomData<S>);
        impl<S> serde::de::Visitor<'_> for Helper<S>
        where
            S: std::str::FromStr,
            <S as std::str::FromStr>::Err: std::fmt::Display,
        {
            type Value = S;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                value.parse::<Self::Value>().map_err(serde::de::Error::custom)
            }
        }

        deserializer.deserialize_str(Helper(core::marker::PhantomData))
    }
}

impl<T: NameTrait> serde_with::schemars_1::JsonSchemaAs<near_gas::NearGas> for GasNumberSerialization<T> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        format!("GasNumberSerialization{}", T::name()).to_string().into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "object",
            "properties": {
                T::name(): {
                    "allOf": [
                        {
                            "$ref": "#/components/schemas/NearGas"
                        }
                    ],
                },
                T::name_lossless(): {
                        "allOf": [
                            {
                                "$ref": "#/components/schemas/NearGas"
                            }
                        ],
                }
            },
            "required": [T::name(), T::name_lossless()],
        })
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
    assert_deserialize("{\"field\":42}", Test { field: 42 });
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
    assert_deserialize("{\"field\":42}", Test { field: 42 });
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
    assert_deserialize("{\"field\":42}", Test { field: Some(42) });
    assert_de_error::<Test>("{\"field\":42.0}");
}

#[cfg(test)]
#[track_caller]
fn assert_round_trip<'a, T>(serialized: &'a str, obj: T)
where
    T: serde::Deserialize<'a> + serde::Serialize + std::fmt::Debug + std::cmp::PartialEq,
{
    assert_eq!(serialized, serde_json::to_string(&obj).unwrap());
    assert_eq!(obj, serde_json::from_str(serialized).unwrap());
}

#[cfg(test)]
#[track_caller]
fn assert_deserialize<'a, T>(serialized: &'a str, obj: T)
where
    T: serde::Deserialize<'a> + std::fmt::Debug + std::cmp::PartialEq,
{
    assert_eq!(obj, serde_json::from_str(serialized).unwrap());
}

#[cfg(test)]
#[track_caller]
fn assert_de_error<'a, T: serde::Deserialize<'a> + std::fmt::Debug>(serialized: &'a str) {
    serde_json::from_str::<T>(serialized).unwrap_err();
}

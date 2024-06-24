/// Provides serialization of Duration as std::time::Duration.
pub mod serde_duration_as_std {
    use crate::Duration;
    use serde::Deserialize;
    use serde::Serialize;

    pub fn serialize<S>(dur: &Duration, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let std: std::time::Duration = (*dur)
            .try_into()
            .map_err(|_| serde::ser::Error::custom("Duration conversion failed"))?;
        std.serialize(s)
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Duration, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let std: std::time::Duration = Deserialize::deserialize(d)?;
        Ok(std.try_into().map_err(|_| serde::de::Error::custom("Duration conversion failed"))?)
    }
}

/// Provides serialization of Duration as std::time::Duration.
pub mod serde_opt_duration_as_std {
    use crate::Duration;
    use serde::Deserialize;
    use serde::Serialize;

    pub fn serialize<S>(dur: &Option<Duration>, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match dur {
            Some(dur) => {
                let std: std::time::Duration = (*dur)
                    .try_into()
                    .map_err(|_| serde::ser::Error::custom("Duration conversion failed"))?;
                std.serialize(s)
            }
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Option<Duration>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let std: Option<std::time::Duration> = Deserialize::deserialize(d)?;
        Ok(std
            .map(|std| {
                std.try_into().map_err(|_| serde::de::Error::custom("Duration conversion failed"))
            })
            .transpose()?)
    }
}

pub mod serde_utc_as_iso {
    use crate::Utc;
    use serde::{Deserialize, Serialize};
    use time::format_description::well_known::Iso8601;

    pub fn serialize<S>(utc: &Utc, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        utc.format(&Iso8601::DEFAULT).map_err(<S::Error as serde::ser::Error>::custom)?.serialize(s)
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Utc, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let str: String = Deserialize::deserialize(d)?;
        Utc::parse(&str, &Iso8601::DEFAULT).map_err(<D::Error as serde::de::Error>::custom)
    }
}

pub mod serde_opt_utc_as_iso {
    use crate::Utc;
    use serde::{Deserialize, Serialize};
    use time::format_description::well_known::Iso8601;

    pub fn serialize<S>(utc: &Option<Utc>, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match utc {
            Some(utc) => utc
                .format(&Iso8601::DEFAULT)
                .map_err(<S::Error as serde::ser::Error>::custom)?
                .serialize(s),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Option<Utc>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let str: Option<String> = Deserialize::deserialize(d)?;
        Ok(str
            .map(|str| {
                Utc::parse(&str, &Iso8601::DEFAULT).map_err(<D::Error as serde::de::Error>::custom)
            })
            .transpose()?)
    }
}

#[cfg(test)]
mod tests {
    use crate::Duration;
    use serde_json;

    #[test]
    fn test_serde_duration() {
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug)]
        struct Test(#[serde(with = "super::serde_duration_as_std")] Duration);

        let expected = Test(Duration::milliseconds(1234));
        let str = serde_json::to_string(&expected).unwrap();
        assert_eq!(str, r#"{"secs":1,"nanos":234000000}"#);
        let actual: Test = serde_json::from_str(&str).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_serde_opt_duration() {
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug)]
        struct Test(#[serde(with = "super::serde_opt_duration_as_std")] Option<Duration>);

        let expected = Test(Some(Duration::milliseconds(1234)));
        let str = serde_json::to_string(&expected).unwrap();
        assert_eq!(str, r#"{"secs":1,"nanos":234000000}"#);
        let actual: Test = serde_json::from_str(&str).unwrap();
        assert_eq!(actual, expected);

        let expected = Test(None);
        let str = serde_json::to_string(&expected).unwrap();
        assert_eq!(str, r#"null"#);
        let actual: Test = serde_json::from_str(&str).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_serde_utc() {
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug)]
        struct Test(#[serde(with = "super::serde_utc_as_iso")] crate::Utc);

        let expected =
            Test(crate::Utc::from_unix_timestamp_nanos(1709582343123456789i128).unwrap());
        let str = serde_json::to_string(&expected).unwrap();
        assert_eq!(str, r#""2024-03-04T19:59:03.123456789Z""#);
        let actual: Test = serde_json::from_str(&str).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_serde_opt_utc() {
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug)]
        struct Test(#[serde(with = "super::serde_opt_utc_as_iso")] Option<crate::Utc>);

        let expected =
            Test(Some(crate::Utc::from_unix_timestamp_nanos(1709582343123456789i128).unwrap()));
        let str = serde_json::to_string(&expected).unwrap();
        assert_eq!(str, r#""2024-03-04T19:59:03.123456789Z""#);
        let actual: Test = serde_json::from_str(&str).unwrap();
        assert_eq!(actual, expected);

        let expected = Test(None);
        let str = serde_json::to_string(&expected).unwrap();
        assert_eq!(str, r#"null"#);
        let actual: Test = serde_json::from_str(&str).unwrap();
        assert_eq!(actual, expected);
    }
}

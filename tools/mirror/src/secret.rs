use rand_core::{OsRng, RngCore};
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::str::FromStr;

pub const SECRET_LEN: usize = 64;
struct KeyMapSecret([u8; SECRET_LEN]);

#[derive(serde::Serialize, serde::Deserialize)]
struct MirrorSecretConfig {
    pub key_map_secret: Option<KeyMapSecret>,
}

impl serde::Serialize for KeyMapSecret {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> Result<<S as serde::Serializer>::Ok, <S as serde::Serializer>::Error>
    where
        S: serde::Serializer,
    {
        let data = bs58::encode(&self.0[..]).into_string();
        serializer.serialize_str(&data)
    }
}

impl<'de> serde::Deserialize<'de> for KeyMapSecret {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as serde::Deserializer<'de>>::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        Self::from_str(&s).map_err(|err| serde::de::Error::custom(format!("{:?}", err)))
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum ParseSecretError {
    #[error("Base58 decode failure: `{1}`")]
    BS58(#[source] bs58::decode::Error, String),
    #[error("invalid decoded length (expected: 64, got: {0}: input: `{1}`)")]
    BadLength(usize, String),
}

impl FromStr for KeyMapSecret {
    type Err = ParseSecretError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut array = [0; SECRET_LEN];
        let length = bs58::decode(s)
            .into(&mut array[..])
            .map_err(|err| Self::Err::BS58(err, s.to_owned()))?;
        if length != SECRET_LEN {
            return Err(Self::Err::BadLength(length, s.to_owned()));
        }
        Ok(Self(array))
    }
}

pub(crate) fn generate<P: AsRef<Path>>(secret_file_out: P) -> anyhow::Result<[u8; SECRET_LEN]> {
    let mut secret = [0; SECRET_LEN];
    let mut out = File::create(secret_file_out)?;

    OsRng.fill_bytes(&mut secret);
    let config = MirrorSecretConfig { key_map_secret: Some(KeyMapSecret(secret)) };
    let str = serde_json::to_string_pretty(&config)?;
    out.write_all(str.as_bytes())?;
    Ok(secret)
}

pub(crate) fn write_empty<P: AsRef<Path>>(secret_file_out: P) -> anyhow::Result<()> {
    let mut out = File::create(secret_file_out)?;
    let config = MirrorSecretConfig { key_map_secret: None };
    let str = serde_json::to_string_pretty(&config)?;
    out.write_all(str.as_bytes())?;
    Ok(())
}

pub fn load<P: AsRef<Path>>(secret_file: P) -> anyhow::Result<Option<[u8; SECRET_LEN]>> {
    let s = std::fs::read_to_string(secret_file)?;
    let config: MirrorSecretConfig = serde_json::from_str(&s)?;
    Ok(config.key_map_secret.map(|s| s.0))
}

use crate::{PublicKey, SecretKey};
use near_account_id::AccountId;
use std::fs::File;
use std::io;
use std::io::{Read, Write};
use std::path::Path;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct KeyFile {
    pub account_id: AccountId,
    pub public_key: PublicKey,
    // Credential files generated which near cli works with have private_key
    // rather than secret_key field.  To make it possible to read those from
    // neard add private_key as an alias to this field so either will work.
    #[serde(alias = "private_key")]
    pub secret_key: SecretKey,
}

impl KeyFile {
    pub fn write_to_file(&self, path: &Path) -> io::Result<()> {
        let data = serde_json::to_string_pretty(self)?;
        let mut file = Self::create(path)?;
        file.write_all(data.as_bytes())
    }

    #[cfg(unix)]
    fn create(path: &Path) -> io::Result<File> {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::File::options().mode(0o600).write(true).create(true).open(path)
    }

    #[cfg(not(unix))]
    fn create(path: &Path) -> io::Result<File> {
        std::fs::File::create(path)
    }

    pub fn from_file(path: &Path) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let mut json_config_str = String::new();
        file.read_to_string(&mut json_config_str)?;
        let json_str_without_comments: String =
            near_config_utils::strip_comments_from_json_str(&json_config_str)?;

        Ok(serde_json::from_str(&json_str_without_comments)?)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const ACCOUNT_ID: &str = "example";
    const SECRET_KEY: &str = "ed25519:3D4YudUahN1nawWogh8pAKSj92sUNMdbZGjn7kERKzYoTy8tnFQuwoGUC51DowKqorvkr2pytJSnwuSbsNVfqygr";
    const KEY_FILE_CONTENTS: &str = r#"{
  "account_id": "example",
  "public_key": "ed25519:6DSjZ8mvsRZDvFqFxo8tCKePG96omXW7eVYVSySmDk8e",
  "secret_key": "ed25519:3D4YudUahN1nawWogh8pAKSj92sUNMdbZGjn7kERKzYoTy8tnFQuwoGUC51DowKqorvkr2pytJSnwuSbsNVfqygr"
}"#;

    #[test]
    fn test_to_file() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("key-file");

        let account_id = ACCOUNT_ID.parse().unwrap();
        let secret_key: SecretKey = SECRET_KEY.parse().unwrap();
        let public_key = secret_key.public_key();
        let key = KeyFile { account_id, public_key, secret_key };
        key.write_to_file(&path).unwrap();

        assert_eq!(KEY_FILE_CONTENTS, std::fs::read_to_string(&path).unwrap());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let got = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(0o600, got & 0o777);
        }
    }

    #[test]
    fn test_from_file() {
        fn load(contents: &[u8]) -> io::Result<()> {
            let tmp = tempfile::NamedTempFile::new().unwrap();
            tmp.as_file().write_all(contents).unwrap();
            let result = KeyFile::from_file(tmp.path());
            tmp.close().unwrap();

            result.map(|key| {
                assert_eq!(ACCOUNT_ID, key.account_id.to_string());
                let secret_key: SecretKey = SECRET_KEY.parse().unwrap();
                assert_eq!(secret_key, key.secret_key);
                assert_eq!(secret_key.public_key(), key.public_key);
            })
        }

        load(KEY_FILE_CONTENTS.as_bytes()).unwrap();

        // Test private_key alias for secret_key works.
        let contents = KEY_FILE_CONTENTS.replace("secret_key", "private_key");
        load(contents.as_bytes()).unwrap();

        // Test private_key is mutually exclusive with secret_key.
        let err = load(br#"{
            "account_id": "example",
            "public_key": "ed25519:6DSjZ8mvsRZDvFqFxo8tCKePG96omXW7eVYVSySmDk8e",
            "secret_key": "ed25519:3D4YudUahN1nawWogh8pAKSj92sUNMdbZGjn7kERKzYoTy8tnFQuwoGUC51DowKqorvkr2pytJSnwuSbsNVfqygr",
            "private_key": "ed25519:3D4YudUahN1nawWogh8pAKSj92sUNMdbZGjn7kERKzYoTy8tnFQuwoGUC51DowKqorvkr2pytJSnwuSbsNVfqygr"
        }"#).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let inner_msg = err.into_inner().unwrap().to_string();
        assert!(inner_msg.contains("duplicate field"));
    }
}

use std::fs::File;
use std::io;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{PublicKey, SecretKey};

use near_account_id::AccountId;

#[derive(Serialize, Deserialize)]
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
        let mut file = File::create(path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            // Only the owner can read or write the file.
            let perm = std::fs::Permissions::from_mode(0o600);
            file.set_permissions(perm)?;
        }
        let str = serde_json::to_string_pretty(self)?;
        file.write_all(str.as_bytes())
    }

    pub fn from_file(path: &Path) -> io::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Ok(serde_json::from_str(&content)?)
    }
}

#[test]
fn test_from_file() {
    fn load(contents: &[u8]) -> io::Result<KeyFile> {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.as_file().write_all(contents).unwrap();
        let result = KeyFile::from_file(tmp.path());
        tmp.close().unwrap();
        result
    }

    load(br#"{
        "account_id": "example",
        "public_key": "ed25519:6DSjZ8mvsRZDvFqFxo8tCKePG96omXW7eVYVSySmDk8e",
        "secret_key": "ed25519:3D4YudUahN1nawWogh8pAKSj92sUNMdbZGjn7kERKzYoTy8tnFQuwoGUC51DowKqorvkr2pytJSnwuSbsNVfqygr"
    }"#).unwrap();

    load(br#"{
        "account_id": "example",
        "public_key": "ed25519:6DSjZ8mvsRZDvFqFxo8tCKePG96omXW7eVYVSySmDk8e",
        "private_key": "ed25519:3D4YudUahN1nawWogh8pAKSj92sUNMdbZGjn7kERKzYoTy8tnFQuwoGUC51DowKqorvkr2pytJSnwuSbsNVfqygr"
    }"#).unwrap();

    let err = load(br#"{
        "account_id": "example",
        "public_key": "ed25519:6DSjZ8mvsRZDvFqFxo8tCKePG96omXW7eVYVSySmDk8e",
        "secret_key": "ed25519:3D4YudUahN1nawWogh8pAKSj92sUNMdbZGjn7kERKzYoTy8tnFQuwoGUC51DowKqorvkr2pytJSnwuSbsNVfqygr",
        "private_key": "ed25519:3D4YudUahN1nawWogh8pAKSj92sUNMdbZGjn7kERKzYoTy8tnFQuwoGUC51DowKqorvkr2pytJSnwuSbsNVfqygr"
    }"#).map(|key| key.account_id).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    let inner_msg = err.into_inner().unwrap().to_string();
    assert!(inner_msg.contains("duplicate field"));
}

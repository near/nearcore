use std::fs::File;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use serde_derive::{Deserialize, Serialize};

use crate::bls::{BlsPublicKey, BlsSecretKey};
use crate::{PublicKey, SecretKey};

#[derive(Serialize, Deserialize)]
pub struct KeyFile {
    pub account_id: String,
    pub public_key: PublicKey,
    pub secret_key: SecretKey,
}

impl KeyFile {
    pub fn write_to_file(&self, path: &Path) {
        let mut file = File::create(path).expect("Failed to create / write a key file.");
        let mut perm =
            file.metadata().expect("Failed to retrieve key file metadata.").permissions();
        perm.set_mode((libc::S_IWUSR | libc::S_IRUSR) as u32);
        file.set_permissions(perm).expect("Failed to set permissions for a key file.");
        let str = serde_json::to_string_pretty(self).expect("Error serializing the key file.");
        if let Err(err) = file.write_all(str.as_bytes()) {
            panic!("Failed to write a key file {}", err);
        }
    }

    pub fn from_file(path: &Path) -> Self {
        let mut file = File::open(path).expect("Could not open key file.");
        let mut content = String::new();
        file.read_to_string(&mut content).expect("Could not read from key file.");
        serde_json::from_str(&content).expect("Failed to deserialize KeyFile")
    }
}

#[derive(Serialize, Deserialize)]
pub struct BlsKeyFile {
    pub account_id: String,
    pub public_key: BlsPublicKey,
    pub secret_key: BlsSecretKey,
}

impl BlsKeyFile {
    pub fn write_to_file(&self, path: &Path) {
        let mut file = File::create(path).expect("Failed to create / write a key file.");
        let str = serde_json::to_string_pretty(self).expect("Error serializing the key file.");
        if let Err(err) = file.write_all(str.as_bytes()) {
            panic!("Failed to write a key file {}", err);
        }
    }

    pub fn from_file(path: &Path) -> Self {
        let mut file = File::open(path).expect("Could not open key file.");
        let mut content = String::new();
        file.read_to_string(&mut content).expect("Could not read from key file.");
        serde_json::from_str(&content).expect("Failed to deserialize KeyFile")
    }
}

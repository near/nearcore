use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use serde_derive::{Deserialize, Serialize};

use crate::bls::{BlsPublicKey, BlsSecretKey};

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

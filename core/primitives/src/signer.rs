use std::fs;
use std::path::Path;
use std::process;

use crate::hash;
use crate::signature::{self, PublicKey, SecretKey, get_key_pair};
use crate::traits;
use crate::types;

#[derive(Serialize, Deserialize)]
pub struct KeyFile {
    #[serde(with = "signature::bs58_pub_key_format")]
    pub public_key: signature::PublicKey,
    #[serde(with = "signature::bs58_secret_key_format")]
    pub secret_key: signature::SecretKey,
}

pub fn write_key_file(
    key_store_path: &Path,
    public_key: PublicKey,
    secret_key: SecretKey,
) -> String {
    if !key_store_path.exists() {
        fs::create_dir_all(key_store_path).unwrap();
    }

    let key_file = KeyFile { public_key, secret_key };
    let key_file_path = key_store_path.join(Path::new(&public_key.to_string()));
    let serialized = serde_json::to_string(&key_file).unwrap();
    fs::write(key_file_path, serialized).unwrap();
    public_key.to_string()
}

pub fn get_key_file(key_store_path: &Path, public_key: Option<String>) -> KeyFile {
    if !key_store_path.exists() {
        println!("Key store path does not exist: {:?}", &key_store_path);
        process::exit(3);
    }

    let mut key_files = fs::read_dir(key_store_path).unwrap();
    let key_file = key_files.next();
    let key_file_string = if key_files.count() != 0 {
        if let Some(p) = public_key {
            let key_file_path = key_store_path.join(Path::new(&p));
            fs::read_to_string(key_file_path).unwrap()
        } else {
            println!("Public key must be specified when there is more than one \
            file in the keystore");
            process::exit(4);
        }
    } else {
        fs::read_to_string(key_file.unwrap().unwrap().path()).unwrap()
    };

    serde_json::from_str(&key_file_string).unwrap()
}

pub fn get_or_create_key_file(key_store_path: &Path, public_key: Option<String>) -> KeyFile {
    if !key_store_path.exists() {
        let (public_key, secret_key) = get_key_pair();
        let new_public_key = write_key_file(key_store_path, public_key, secret_key);
        get_key_file(key_store_path, Some(new_public_key))
    } else {
        get_key_file(key_store_path, public_key)
    }
}


pub struct InMemorySigner {
    pub account_id: types::AccountId,
    pub public_key: signature::PublicKey,
    pub secret_key: signature::SecretKey,
}

impl InMemorySigner {
    pub fn from_key_file(account_id: types::AccountId, key_store_path: &Path, public_key: Option<String>) -> Self {
        let key_file = get_or_create_key_file(key_store_path, public_key);
        InMemorySigner {
            account_id,
            public_key: key_file.public_key,
            secret_key: key_file.secret_key }
    }
}

impl Default for InMemorySigner {
    fn default() -> Self {
        let (public_key, secret_key) = signature::get_key_pair();
        InMemorySigner { account_id: "alice.near".to_string(), public_key, secret_key }
    }
}

impl traits::Signer for InMemorySigner {
    #[inline]
    fn public_key(&self) -> signature::PublicKey {
        self.public_key
    }

    fn sign(&self, hash: &hash::CryptoHash) -> types::PartialSignature {
        signature::sign(hash.as_ref(), &self.secret_key)
    }

    #[inline]
    fn account_id(&self) -> types::AccountId {
        self.account_id.clone()
    }
}

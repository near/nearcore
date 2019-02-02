use std::fs;
use std::path::Path;
use std::process;

use crate::aggregate_signature::{BlsSecretKey, BlsPublicKey};
use crate::hash;
use crate::signature::{self, PublicKey, SecretKey};
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

#[derive(Serialize, Deserialize)]
pub struct BlsKeyFile {
    #[serde(with = "signature::bs58_serializer")]
    pub public_key: BlsPublicKey,
    #[serde(with = "signature::bs58_serializer")]
    pub secret_key: BlsSecretKey,
}

pub fn write_bls_key_file(
    key_store_path: &Path,
    public_key: BlsPublicKey,
    secret_key: BlsSecretKey,
) -> String {
    if !key_store_path.exists() {
        fs::create_dir_all(key_store_path).unwrap();
    }

    let key_file = BlsKeyFile { public_key, secret_key };
    let key_file_path = key_store_path.join(Path::new(&key_file.public_key.to_string()));
    let serialized = serde_json::to_string(&key_file).unwrap();
    fs::write(key_file_path, serialized).unwrap();
    key_file.public_key.to_string()
}

pub fn get_bls_key_file(key_store_path: &Path, public_key: Option<String>) -> BlsKeyFile {
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

pub fn get_or_create_key_file(key_store_path: &Path, public_key: Option<String>) -> BlsKeyFile {
    if !key_store_path.exists() {
        let secret_key = BlsSecretKey::generate();
        let public_key = secret_key.get_public_key();
        let new_public_key = write_bls_key_file(key_store_path, public_key, secret_key);
        get_bls_key_file(key_store_path, Some(new_public_key))
    } else {
        get_bls_key_file(key_store_path, public_key)
    }
}

pub struct InMemorySigner {
    pub account_id: types::AccountId,
    pub public_key: BlsPublicKey,
    pub secret_key: BlsSecretKey,
}

impl InMemorySigner {
    pub fn from_key_file(account_id: types::AccountId, key_store_path: &Path, public_key: Option<String>) -> Self {
        let key_file = get_or_create_key_file(key_store_path, public_key);
        InMemorySigner {
            account_id,
            public_key: key_file.public_key,
            secret_key: key_file.secret_key,
        }
    }
}

impl Default for InMemorySigner {
    fn default() -> Self {
        let secret_key = BlsSecretKey::generate();
        let public_key = secret_key.get_public_key();
        InMemorySigner { account_id: "alice.near".to_string(), public_key, secret_key }
    }
}

impl traits::Signer for InMemorySigner {
    #[inline]
    fn public_key(&self) -> BlsPublicKey {
        self.public_key.clone()
    }

    fn sign(&self, hash: &hash::CryptoHash) -> types::PartialSignature {
        self.secret_key.sign(hash.as_ref())
    }

    #[inline]
    fn account_id(&self) -> types::AccountId {
        self.account_id.clone()
    }
}

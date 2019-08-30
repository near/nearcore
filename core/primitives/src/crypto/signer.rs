use std::fs;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::process;
use std::sync::Arc;

use rand::distributions::Alphanumeric;
use rand::rngs::OsRng;
use rand::Rng;

use crate::crypto::aggregate_signature::BlsPublicKey;
use crate::crypto::signature::{get_key_pair, sign, verify, PublicKey, SecretKey, Signature};
use crate::types::{AccountId, PartialSignature};
use crate::views::{PublicKeyView, SecretKeyView};

/// Trait to abstract the signer account.
pub trait AccountSigner: Sync + Send {
    fn account_id(&self) -> AccountId;
}

/// Trait to abstract the way transaction signing with ed25519.
/// Can be used to not keep private key in the given binary via cross-process communication.
pub trait EDSigner: Sync + Send {
    fn public_key(&self) -> PublicKey;
    fn sign(&self, data: &[u8]) -> Signature;
    fn verify(&self, data: &[u8], signature: &Signature) -> bool;

    fn write_to_file(&self, _path: &Path) {
        unimplemented!();
    }
}

/// Trait to abstract the way signing with bls.
/// Can be used to not keep private key in the given binary via cross-process communication.
pub trait BLSSigner: Sync + Send {
    fn bls_public_key(&self) -> BlsPublicKey;
    fn bls_sign(&self, data: &[u8]) -> PartialSignature;
}

#[derive(Serialize, Deserialize)]
pub struct KeyFile {
    pub account_id: AccountId,
    pub public_key: PublicKeyView,
    pub secret_key: SecretKeyView,
}

impl KeyFile {
    fn write_to_file(&self, path: &Path) {
        let mut file = File::create(path).expect("Failed to create / write a key file.");
        let str = serde_json::to_string_pretty(self).expect("Error serializing the key file.");
        if let Err(err) = file.write_all(str.as_bytes()) {
            panic!("Failed to write a key file {}", err);
        }
    }

    fn from_file(path: &Path) -> Self {
        let mut file = File::open(path).expect("Could not open key file.");
        let mut content = String::new();
        file.read_to_string(&mut content).expect("Could not read from key file.");
        serde_json::from_str(&content).expect("Failed to deserialize KeyFile")
    }
}

pub fn write_key_file(
    key_store_path: &Path,
    public_key: PublicKey,
    secret_key: SecretKey,
) -> String {
    if !key_store_path.exists() {
        fs::create_dir_all(key_store_path).unwrap();
    }

    let key_file = KeyFile {
        account_id: "".to_string(),
        public_key: public_key.into(),
        secret_key: secret_key.into(),
    };
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
            println!(
                "Public key must be specified when there is more than one \
                 file in the keystore"
            );
            process::exit(4);
        }
    } else {
        fs::read_to_string(key_file.unwrap().unwrap().path()).unwrap()
    };

    serde_json::from_str(&key_file_string).unwrap()
}

#[derive(Clone)]
pub struct InMemorySigner {
    pub account_id: AccountId,
    pub public_key: PublicKey,
    pub secret_key: SecretKey,
}

impl InMemorySigner {
    pub fn new(account_id: String) -> Self {
        let (public_key, secret_key) = get_key_pair();
        Self { account_id, public_key, secret_key }
    }

    pub fn from_secret_key(
        account_id: String,
        public_key: PublicKey,
        secret_key: SecretKey,
    ) -> Self {
        Self { account_id, public_key, secret_key }
    }

    /// Read key file into signer.
    pub fn from_file(path: &Path) -> Self {
        KeyFile::from_file(path).into()
    }

    /// Initialize `InMemorySigner` with a random ED25519 and BLS keys, and random account id. Used
    /// for testing only.
    pub fn from_random() -> Self {
        let mut rng = OsRng::new().expect("Unable to generate random numbers");
        let account_id: String =
            rng.sample_iter(&Alphanumeric).filter(char::is_ascii_alphabetic).take(10).collect();
        let (public_key, secret_key) = get_key_pair();
        Self { account_id, public_key, secret_key }
    }
}

impl From<KeyFile> for InMemorySigner {
    fn from(key_file: KeyFile) -> Self {
        Self {
            account_id: key_file.account_id,
            public_key: key_file.public_key.into(),
            secret_key: key_file.secret_key.into(),
        }
    }
}

impl From<&InMemorySigner> for KeyFile {
    fn from(signer: &InMemorySigner) -> KeyFile {
        KeyFile {
            account_id: signer.account_id.clone(),
            public_key: signer.public_key.into(),
            secret_key: signer.secret_key.clone().into(),
        }
    }
}

impl From<Arc<InMemorySigner>> for KeyFile {
    fn from(signer: Arc<InMemorySigner>) -> KeyFile {
        KeyFile {
            account_id: signer.account_id.clone(),
            public_key: signer.public_key.into(),
            secret_key: signer.secret_key.clone().into(),
        }
    }
}

impl AccountSigner for InMemorySigner {
    #[inline]
    fn account_id(&self) -> AccountId {
        self.account_id.clone()
    }
}

impl EDSigner for InMemorySigner {
    #[inline]
    fn public_key(&self) -> PublicKey {
        self.public_key
    }

    fn sign(&self, data: &[u8]) -> Signature {
        sign(data, &self.secret_key)
    }

    fn verify(&self, data: &[u8], signature: &Signature) -> bool {
        verify(data, signature, &self.public_key)
    }

    /// Save signer into key file.
    fn write_to_file(&self, path: &Path) {
        let key_file: KeyFile = self.into();
        key_file.write_to_file(path);
    }
}

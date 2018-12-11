extern crate clap;
extern crate primitives;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

use primitives::signature::{
    bs58_pub_key_format, bs58_secret_key_format,
    get_keypair, PublicKey, SecretKey,
};
use std::fs;
use std::path::Path;
use std::process;

#[derive(Serialize, Deserialize)]
pub struct KeyFile {
    #[serde(with = "bs58_pub_key_format")]
    pub public_key: PublicKey,
    #[serde(with = "bs58_secret_key_format")]
    pub secret_key: SecretKey,
}

pub fn write_key_file(key_store_path: &Path) -> String {
    if !key_store_path.exists() {
        fs::create_dir_all(key_store_path).unwrap();
    }

    let (public_key, secret_key) = get_keypair();
    let key_file = KeyFile { public_key, secret_key };
    let key_file_path = key_store_path.join(Path::new(&public_key.to_string()));
    let serialized = serde_json::to_string(&key_file).unwrap();
    fs::write(key_file_path, serialized).unwrap();
    public_key.to_string()
}

pub fn get_key_file(key_store_path: &Path, public_key: Option<&str>) -> KeyFile {
    if !key_store_path.exists() {
        eprintln!("Key store path does not exist: {:?}", &key_store_path);
        process::exit(3);
    }

    let mut key_files = fs::read_dir(key_store_path).unwrap();
    let key_file = key_files.next();
    let key_file_string = if key_files.count() != 0 {
        if let Some(p) = public_key {
            let key_file_path = key_store_path.join(Path::new(&p));
            fs::read_to_string(key_file_path).unwrap()
        } else {
            eprintln!("Public key must be specified when there is more than one \
            file in the keystore");
            process::exit(4);
        }
    } else {
        fs::read_to_string(key_file.unwrap().unwrap().path()).unwrap()
    };

    serde_json::from_str(&key_file_string).unwrap()
}

extern crate sodiumoxide;
extern crate tiny_keccak;

use std::mem;
use std::fs::File;
use std::io::{Read, Write, Error, ErrorKind};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use serde_derive::{Deserialize, Serialize};
use sodiumoxide::crypto::secretbox;
use sodiumoxide::crypto::secretbox::xsalsa20poly1305::Key;
use tiny_keccak::keccak256;

use crate::{PublicKey, SecretKey, KeyType};

#[derive(Default, PartialEq, Clone, Deserialize, Serialize)]
pub struct KeyHash([u8; 32]);
 
impl KeyHash {
    fn new(byte_slice : &[u8])-> Self { 
        let bytes = keccak256(byte_slice); //[u8,32]
        KeyHash(bytes)
    }
}

#[derive(Serialize, Deserialize, Clone)]
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
        perm.set_mode(u32::from(libc::S_IWUSR | libc::S_IRUSR));
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

#[derive(Serialize, Deserialize, Clone)]
pub struct EncryptedKeyFile {
    pub account_id: String,
    pub public_key: PublicKey,
    pub encrypted_secret_key: EncryptedSecretKey,
    pub key_hash: KeyHash,
    pub ssb: SodiumoxideSecretBox,
}

impl EncryptedKeyFile {
    // TODO implement trait for Keyfile and Encrypted keyFile
    pub fn write_to_file(&self, path: &Path) {
        let mut file = File::create(path).expect("Failed to create / write a key file.");
        let mut perm =
            file.metadata().expect("Failed to retrieve key file metadata.").permissions();
        perm.set_mode(u32::from(libc::S_IWUSR | libc::S_IRUSR));
        file.set_permissions(perm).expect("Failed to set permissions for a key file.");
        let str = serde_json::to_string_pretty(self).expect("Error serializing the key file.");
        if let Err(err) = file.write_all(str.as_bytes()) {
            panic!("Failed to write a key file {}", err);
        }
    }
    // TODO implement trait for Keyfile and Encrypted keyFile
    pub fn from_file(path: &Path) -> Self {
        let mut file = File::open(path).expect("Could not open key file.");
        let mut content = String::new();
        file.read_to_string(&mut content).expect("Could not read from key file.");
        serde_json::from_str(&content).expect("Failed to deserialize KeyFile")
    }
     //decrypts a file and return an instance of the keyfile
    // pub fn from_encrypted_file(path: &Path, key : &Key, encrypted_key_file: EncryptedKeyFile) -> Self {
    //     let ssb = encrypted_key_file.ssb;
    //     let decrypted_secretkey = ssb.decrypt(&secretKeyToVec(encrypted_key_file.secret_key), &key);
    //     EncryptedKeyFile {
    //         account_id: key_file.account_id,
    //         public_key: key_file.public_key,
    //         encrypted_secret_key: key_file.encrypted_secret_key,
    //         key_hash: KeyHash::default(),
    //         ssb: SodiumoxideSecretBox::default(),
    //     }
    // }

    pub fn is_encrypted(&self) -> bool{
        !self.key_hash.eq(&KeyHash::default())
    }
}



// SecretKeyView is used to store 64 byte ciphertext produced by salsa cipher
#[derive(Serialize, Deserialize, Default, Debug, PartialEq, Eq, Clone)]
pub struct EncryptedSecretKey(Vec<u8>);
 
impl EncryptedSecretKey {
    fn new(vec: Vec<u8>) -> Self {
       EncryptedSecretKey(vec)
    }
}

fn secretkey_to_vec(secret_key: SecretKey) -> Vec<u8> {
        match secret_key {
            SecretKey::ED25519(sk) => { sk.0.to_vec() },
            SecretKey::SECP256K1(sk) => unsafe {
                mem::transmute_copy::<secp256k1::key::SecretKey, [u8; 32] > (&sk).to_vec()
            },
        }
}
 
//  //store in SecretKeyView instead
// fn secretkey_from_vec(vec: Vec<u8>) -> SecretKey {
//     if vec.len() == 64 {
//         SecretKey::ED25519(
//             unsafe {
//                 mem::transmute_copy::<Vec<u8>, sodiumoxide::crypto::sign::ed25519::SecretKey>(&vec)
//             }
//         )
//     }
//     else {
//         // need to be 64 byt4es
//         let mut array = [0u8; 32];
//         array[..vec.len()].copy_from_slice(&vec[..]); 
//         SecretKey::SECP256K1(
//             secp256k1::key::SecretKey::from(array)
//         )
//     }
// }

#[derive(PartialEq)]
struct PasswordInput(String);
 
impl PasswordInput {
    // should support that user might not want to encrypt the key
    pub fn enter_from_console() -> String {
        println!("Enter a password for a secret key: ");
        let mut input = String::new();
        let _ = std::io::stdin().read_line(&mut input).expect("Could not read from stdin");
        input = input.trim().to_string(); //&str copy type
        input
    }
 
    pub fn process_input(content: &str) -> Result<Self, Error>  {
         const MAX_LENGTH: usize = 32;
        if content.len() > MAX_LENGTH {
           return Err(Error::new(ErrorKind::InvalidInput, "Input is too long"));
        }
        if content.is_empty() {
            let content_str = String::from_utf8(content.as_bytes().to_vec()).unwrap();
            return Ok(PasswordInput(content_str));
        }
        let mut res = vec![0u8; 32];
        res[..content.len()].copy_from_slice(content.as_bytes());
        let buf_str = String::from_utf8(res).unwrap();
        Ok(PasswordInput(buf_str))
    }
}

// SodiumoxideSecretBox stores nonce for encryption and decruption of secret key
#[derive(Serialize, Deserialize, Clone)]
pub struct SodiumoxideSecretBox(secretbox::Nonce);
 
//https://docs.rs/sodiumoxide/0.2.0/src/sodiumoxide/newtype_macros.rs.html#257-285
impl Default for SodiumoxideSecretBox {
    fn default() -> Self {
        SodiumoxideSecretBox::new()
    }
}
 
impl SodiumoxideSecretBox {
    pub fn new() -> Self {
        let nonce = secretbox::gen_nonce();
        SodiumoxideSecretBox(nonce)
    }
 
    //need to be tested
    pub fn gen_key(bytes: &[u8]) -> Result<Key, Error> {
        Key::from_slice(bytes).map(Ok).unwrap_or_else(|| {
            Err(Error::new(
                ErrorKind::InvalidInput,
                "Failed to create key from slice".to_string(),
            ))
        })
    }
}
 
trait Operation {
    fn encrypt(&self, m: &[u8], k: &Key) -> Vec<u8>;
    fn decrypt(&self, c: &[u8], k: &Key) -> Vec<u8>;
}
 
impl Operation for SodiumoxideSecretBox {
    fn encrypt(&self, m: &[u8], k: &Key) -> Vec<u8> {
        secretbox::seal(m, &self.0, &k)
    }
 
    fn decrypt(&self, c: &[u8], k: &Key) -> Vec<u8>{
        secretbox::open(c, &self.0, &k).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] 
    fn test_encrypt_decrypt() {
        //encryption
        for key_type in vec![KeyType::ED25519, KeyType::SECP256K1] {
            let file_name = match key_type {
                KeyType::ED25519 => "ED25519.txt".to_string(),
                KeyType::SECP256K1 => "SECP256K1.txt".to_string(),
            };
            let secret_key = SecretKey::from_seed(key_type, "test");
            let public_key  = secret_key.public_key();
            let account_id = String::from("test");
            let mut encrypted_key_file = EncryptedKeyFile {
                account_id: account_id,
                public_key: public_key,
                encrypted_secret_key: EncryptedSecretKey::default(),
                key_hash: KeyHash::default(),
                ssb: SodiumoxideSecretBox::default(),
            };


            let path = Path::new(&file_name);
            let input = "qwerty".to_string();
            //encryption
            let mut password_input = PasswordInput::process_input(&input).unwrap();
            let mut  password_slice = password_input.0.as_bytes();
            assert_eq!(password_slice.len(), 32 as usize);
            assert!(!password_input.0.is_empty());
            let secretkey_vec = secretkey_to_vec(secret_key.clone());
            //assert_eq!(secretkey_vec.len(), 64 as usize);
            let encryption_key = SodiumoxideSecretBox::gen_key(password_slice).unwrap();
            let key_hash = KeyHash::new(password_slice);
            let encrypted_secretkey_vec = encrypted_key_file.ssb.encrypt(&secretkey_vec, &encryption_key);
             // TODO make a test to match encrypted secret key length depending on key type
            //assert_eq!(encrypted_secretkey_vec.len(), 80 as usize);
            //key_file.secret_key = secretKeyFromVec(encrypted_secretkey);
            let encrypted_secret_key = EncryptedSecretKey::new(encrypted_secretkey_vec);
            encrypted_key_file.encrypted_secret_key = encrypted_secret_key.clone();
            encrypted_key_file.key_hash = key_hash;
            encrypted_key_file.write_to_file(&path);
            assert!(encrypted_key_file.is_encrypted());
 
            //decryption
            encrypted_key_file = EncryptedKeyFile::from_file(&path);
            //password is encrypted proceed further, if not Ok(key_file)
            assert!(encrypted_key_file.is_encrypted());
            //ask for a password in console
            password_input = PasswordInput::process_input(&input).unwrap(); 
            password_slice = password_input.0.as_bytes();
            // password is not empty decode a file with a passwordm if password empty do nothing
            assert!(!password_input.0.is_empty()); 
            let decryption_keyhash = KeyHash::new(password_slice);
            assert!(decryption_keyhash.eq(&encrypted_key_file.key_hash)); // if hashes of passwords match then decrypt EncryptedKeyfile.encrypted_secret_key
            let decryption_key = SodiumoxideSecretBox::gen_key(password_slice).unwrap();
            let ciphertext_vec = encrypted_key_file.encrypted_secret_key.0; //encrypted secret key vec
            let ssb = encrypted_key_file.ssb;
            let decrypted_secretkey_vec = ssb.decrypt(&ciphertext_vec, &decryption_key);//Vec<u8>
            let decrypted_secretkey = EncryptedSecretKey::new(decrypted_secretkey_vec);
            assert_eq!(encrypted_secret_key, decrypted_secretkey);
            //TODO check the lengtb of decrypted secret key depending on key type
            //assert_eq!(decrypted_secretkey_vec.len(), 32 as usize );
            //let decrypted_sk = secretkey_from_vec(decrypted_secretkey_vec);
            // assert_eq!(decrypted_sk , secret_key);
        }
    }
    #[test] 
    fn test_process_input() {
        for input in &vec!["qwerty".to_string(), "".to_string() ] {
            let password_input = PasswordInput::process_input(input).unwrap();
            let password_input_str = password_input.0;
            if input.is_empty() {
                assert!(password_input_str.is_empty());
            } else {
                let mut res = vec![0u8; 32];
                res[..input.len()].copy_from_slice(input.as_bytes());
                let expected_str = String::from_utf8(res).unwrap();
                assert_eq!(&password_input_str, &expected_str);
            }
        }
        let zero_vec = vec![0u8; 33];
        let s = String::from_utf8(zero_vec).unwrap();
        assert!( PasswordInput::process_input(&s).is_err() );
    }  
}












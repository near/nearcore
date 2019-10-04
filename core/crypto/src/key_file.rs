extern crate sodiumoxide;
extern crate tiny_keccak;

use std::mem;
use std::fs::File;
use std::result::Result;
use std::io::{Read, Write, Error, ErrorKind};
use std::path::Path;

use serde_derive::{Deserialize, Serialize};
use sodiumoxide::crypto::secretbox;
use sodiumoxide::crypto::secretbox::xsalsa20poly1305::Key;
use tiny_keccak::keccak256;

use crate::signature::{PublicKey, SecretKey, KeyType};

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
    pub key_hash: KeyHash,
    pub ssb: SodiumoxideSecretBox,
}


impl KeyFile {
    pub fn write_to_file(&self, path: &Path) {
        let mut file = File::create(path).expect("Failed to create / write a key file.");
        let str = serde_json::to_string_pretty(self).expect("Error serializing the key file.");
        if let Err(err) = file.write_all(str.as_bytes()) {
            panic!("Failed to write a key file {}", err);
        }
    }

    //asks for a password in console and encrypt a  private key ( secret_key field from keyFile structure) with an entered password
    // if password is empty just write to the file
    pub fn write_to_encrypted_file(&mut self, path: &Path, input: &str) -> bool {   
        let password_input = PasswordInput::process_input(input).unwrap();
        let password_slice = password_input.0.as_bytes();
        if !password_input.0.is_empty() {
            let secretkey_vec = secretKeyToVec(self.secret_key.clone());
            let encryption_key = SodiumoxideSecretBox::gen_key(password_slice).unwrap();
            let key_hash = KeyHash::new(password_slice);
            let ssb = SodiumoxideSecretBox::new();
            let encrypted_secretkey = ssb.encrypt(&secretkey_vec, &encryption_key);
            self.secret_key = secretKeyFromVec(encrypted_secretkey);
            self.key_hash = key_hash;
            self.ssb = ssb;
        } else {
            self.key_hash = KeyHash::default();
            self.ssb = SodiumoxideSecretBox::default();
        }
        self.write_to_file(path);
        self.is_encrypted()
    }

    pub fn from_file(path: &Path) -> Self {  
        let mut file = File::open(path).expect("Could not open key file.");
        let mut content = String::new();
        file.read_to_string(&mut content).expect("Could not read from key file.");
        serde_json::from_str(&content).expect("Failed to deserialize KeyFile")
    }
    
    // decrypts a file and return an instance of the file
    pub fn from_encrypted_file(path: &Path, key : &Key, key_file: KeyFile) -> Self {
        let ssb = key_file.ssb;
        let decrypted_secretkey = ssb.decrypt(&secretKeyToVec(key_file.secret_key), &key);
        KeyFile {
            account_id: key_file.account_id,
            public_key: key_file.public_key,
            secret_key: secretKeyFromVec(decrypted_secretkey),
            key_hash: KeyHash::default(),
            ssb: SodiumoxideSecretBox::default(),
        }
    }

    pub fn is_encrypted(&self) -> bool{
        !self.key_hash.eq(&KeyHash::default())
    }

    // if a private key is encrypted , ask for a password in console
    // if an entered password matches a password entered in step 3, decode an encrypted password otherwise keep asking for a correct password
    pub fn from_encrypted_file_with_password(path : &Path, input: &str) -> Self  {  
        //reading file
        let mut key_file = KeyFile::from_file(path);
        //password is encrypted proceed further, if not Ok(key_file)
        if key_file.is_encrypted() {
        //ask for a password in console
        //infinite loop
            let password_input = PasswordInput::process_input(input).unwrap(); 
            let password_slice = password_input.0.as_bytes();
            // password is not empty decode a file with a passwordm if password empty do nothing
            if !password_input.0.is_empty() { 
                let decryption_keyhash = KeyHash::new(password_slice);
                if decryption_keyhash.eq(&key_file.key_hash) { // if hashes of passwords match then decrypt Keyfile.secret_key
                        let decryption_key = SodiumoxideSecretBox::gen_key(password_slice).unwrap();
                        let cloned_keyfile = key_file.clone();
                        key_file = KeyFile::from_encrypted_file(path, &decryption_key, cloned_keyfile);
                    }
                }
            }
        key_file
    }
}

fn secretKeyToVec(secret_key: SecretKey) -> Vec<u8> {
    match secret_key {
        SecretKey::ED25519(sk) => { sk.0.to_vec() },
        SecretKey::SECP256K1(sk) => unsafe {
            mem::transmute_copy::<secp256k1::key::SecretKey, [u8; 32]>(&sk).to_vec()
        },
    }
}

fn secretKeyFromVec(vec: Vec<u8>) -> SecretKey {
    if vec.len() == 64 {
        SecretKey::ED25519(
            unsafe {
                mem::transmute_copy::<Vec<u8>, sodiumoxide::crypto::sign::ed25519::SecretKey>(&vec)
            }
        )
    }
    else {
        let mut array = [0u8; 32];
        array.copy_from_slice(&vec[..]); 
        SecretKey::SECP256K1(
            secp256k1::key::SecretKey::from(array)
        )
    }
}

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
        res.copy_from_slice(content.as_bytes());
        let buf_str = String::from_utf8(res).unwrap();
        Ok(PasswordInput(buf_str))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

// pub fn perform_encryption(key_file : &mut KeyFile,
//     key_store_path: &Path, home_dir: &Path,
//     ) -> Result<(), Error> {
//         key_file.write_to_file(&key_store_path);
    
//     if home_dir.join("config.json").exists() {
//         //add password corrrectness validation to from_encrypted_file_with_password
//         let input = PasswordInput::enter_from_console();
//         KeyFile::from_encrypted_file_with_password(&key_store_path, input)?;
//     } else {
//         let input = PasswordInput::enter_from_console();//ask for input of password 
//         key_file = key_file.write_to_encrypted_file(&key_store_path, input)?;
//     }

//     Ok(())
// }

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
            let mut key_file = KeyFile {
                account_id: account_id,
                public_key: public_key,
                secret_key: secret_key.clone(),
                key_hash: KeyHash::default(),
                ssb: SodiumoxideSecretBox::default(),
            };
            let key_store_path = Path::new(&file_name);
            let input = "qwerty".to_string();
            //encryption
            assert!(key_file.write_to_encrypted_file(&key_store_path, &input));
            assert!(key_store_path.exists());

            //decryption
            let key_file = KeyFile::from_encrypted_file_with_password(&key_store_path, &input);
            let sk = key_file.secret_key;
            assert_eq!(secret_key, sk);
        }
    }

    #[test] 
    fn test_process() {
        fn process_test_input(input: &str)  {
            assert!(PasswordInput::process_input(input).is_ok());      
            let password_input = PasswordInput::process_input(input).unwrap();
            let password_input_str = password_input.0;
            if input.is_empty() {
                assert_eq!(&password_input_str, input);
            } else {
                let mut res = vec![0u8; 32];
                res.copy_from_slice(input.as_bytes());
                let expected_str = String::from_utf8(res).unwrap();
                assert_eq!(&password_input_str, &expected_str);
            }
        }
        //testing normal input
        let mut input = "qwerty".to_string();
        process_test_input(&input);


        input = "".to_string();
        process_test_input(&input);

        fn larger_max_length_input(len: usize){
            let zero_vec = vec![0u8; len];
            let s = String::from_utf8(zero_vec).expect("Found invalid UTF-8");
            let err_result = PasswordInput::process_input(&s);
            assert!( err_result.is_err() );
        }
         //testing erroneous input
        larger_max_length_input(33);
    }

   
}



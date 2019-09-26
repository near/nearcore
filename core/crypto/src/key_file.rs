extern crate sodiumoxide;
extern crate tiny_keccak;

use std::mem;
use std::fs::File;
use std::io::{Read, Write, Error,  ErrorKind};
use std::path::Path;

use serde_derive::{Deserialize, Serialize};
use sodiumoxide::crypto::secretbox;
use sodiumoxide::crypto::secretbox::xsalsa20poly1305::Key;
use tiny_keccak::keccak256;

use crate::signature::{PublicKey, SecretKey};

#[derive(Serialize, Deserialize)]
pub struct KeyFile {
    pub account_id: String,
    pub public_key: PublicKey,
    pub secret_key: SecretKey,
    #[serde(default)]
    pub key_hash: KeyHash
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
    pub fn write_to_encrypted_file(&mut self, path: &Path) -> Result<(), Error> {   
        let password_input = PasswordInput::process()?;//ask for input of password
        if password_input.not_empty() {
            let secretkey_vec = secretKeyToVec(self.secret_key.clone());
            let encryption_key = password_input.gen_key_from_password_input()?;
            let key_hash = KeyHash::new(&password_input.content);
            let ssb = SodiumoxideSecretBox::new();
            let encrypted_secretkey = ssb.encrypt(&secretkey_vec, &encryption_key);
            self.secret_key = secretKeyFromVec(encrypted_secretkey);
            self.key_hash = key_hash;
        }

        self.write_to_file(path);
        Ok(())
    }

    pub fn from_file(path: &Path) -> Self {  
        let mut file = File::open(path).expect("Could not open key file.");
        let mut content = String::new();
        file.read_to_string(&mut content).expect("Could not read from key file.");
        serde_json::from_str(&content).expect("Failed to deserialize KeyFile")
    }
    
    // decrypts a file and return an instance of the file
    pub fn from_encrypted_file(path: &Path, key : &Key) -> Self {
        let key_file = KeyFile::from_file(path);
        let ssb = SodiumoxideSecretBox::new();
        let decrypted_secretkey = ssb.decrypt(&secretKeyToVec(key_file.secret_key), &key);
        KeyFile {
            account_id: key_file.account_id,
            public_key: key_file.public_key,
            secret_key: secretKeyFromVec(decrypted_secretkey),
            key_hash: KeyHash::default(),
        }
    }

    // if a private key is encrypted , ask for a password in console
    // if an entered password matches a password entered in step 3, decode an encrypted password otherwise keep asking for a correct password
    pub fn from_encrypted_file_with_password(path : &Path) -> Result<Self, Error>  {  
        let mut key_file = KeyFile::from_file(path);
        if key_file.key_hash.eq(&KeyHash::default()) {
            let password_input = PasswordInput::process()?; // ask for a password in command line
            if  password_input.not_empty() { // password is not empty decode a file with a passwordm if password empty do nothing
                let decryption_keyhash = KeyHash::new(&password_input.content);
                if decryption_keyhash.eq(&key_file.key_hash) { // if hashes of passwords match then decrypt Keyfile.secret_key
                    let decryption_key = password_input.gen_key_from_password_input()?;
                    key_file = KeyFile::from_encrypted_file(path, &decryption_key);
                }
            }
        }

        Ok(key_file)
    }
}

pub fn perform_encryption(
    key_file : &mut KeyFile,
    key_store_path: &Path,
    home_dir: &Path,
) -> Result<(), Error> {
    key_file.write_to_file(&key_store_path);
    
    if home_dir.join("config.json").exists() {
        //add password corrrectness validation to from_encrypted_file_with_password
        KeyFile::from_encrypted_file_with_password(&key_store_path)?;
    } else {
        key_file.write_to_encrypted_file(&key_store_path)?;
    }

    Ok(())
}

pub struct PasswordInput {
    pub content: Vec<u8>,
    pub length: usize
}

impl PasswordInput {
    // should support that user might not want to encrypt the key
    pub fn enter_from_console() -> Self {
        println!("Enter a password for a secret key: ");
        let mut input = String::new();
        let _ = std::io::stdin().read_line(&mut input).expect("Could not read from stdin");
        let content = input.trim().as_bytes().to_vec();
        let length = input.len();
        PasswordInput { content, length }
    }

    pub fn exceed_max_length(&self) -> bool {
        const MAX_LENGTH: usize = 32;
        self.length > MAX_LENGTH
    }

    // xsalsa20poly1305 restricts key size to 32
    // https://github.com/sodiumoxide/sodiumoxide/blob/master/src/crypto/secretbox/xsalsa20poly1305.rs#L50
    pub fn pad_content(&self) -> Vec<u8> {
        let mut buf = [0u8; 32];
        buf.copy_from_slice(&self.content[..]);
        buf.to_vec()
    }

    pub fn not_empty(&self) -> bool {
        self.length > 0
    }

    pub fn process() -> Result<PasswordInput, Error> {  
        let mut password_input = PasswordInput::enter_from_console();
        if password_input.exceed_max_length() {
            return Err(Error::new(ErrorKind::InvalidInput, "Input is too long"))
        }
       
        if password_input.not_empty() {
            let content = password_input.pad_content();
            let length = content.len();
            password_input = PasswordInput{content, length};
        }

        Ok(password_input)
    }

    pub fn gen_key_from_password_input(&self) -> Result<Key, Error>  {
        let key = SodiumoxideSecretBox::gen_key(&self.content)?;
        Ok(key)
    }
}

trait Operation {
    fn encrypt(&self, m: &[u8], k: &Key) -> Vec<u8>;
    fn decrypt(&self, c: &[u8], k: &Key) -> Vec<u8>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SodiumoxideSecretBox {
    nonce: secretbox::Nonce,
}

impl SodiumoxideSecretBox {
    pub fn new() -> Self {
        let nonce = secretbox::gen_nonce();
        SodiumoxideSecretBox { nonce }
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

impl Operation for SodiumoxideSecretBox {
    fn encrypt(&self, m: &[u8], k: &Key) -> Vec<u8> {
        secretbox::seal(m, &self.nonce, &k)
    }

    fn decrypt(&self, c: &[u8], k: &Key) -> Vec<u8>{
        secretbox::open(c, &self.nonce, &k).unwrap()
    }
}

#[derive(Deserialize, Serialize)]
pub struct KeyHash {
    pub bytes: [u8; 32]
}

impl Default for KeyHash {
    fn default() -> Self {
        let zero_vec = vec![0u8, 32];
        KeyHash::new(&zero_vec)
    }
}

impl PartialEq for KeyHash {
    fn eq(&self, other: &KeyHash) -> bool {
        self.bytes.iter().zip(other.bytes.iter()).all(|(a,b)| a == b) 
    }
}

impl KeyHash {
    fn new(byte_slice : &[u8])-> Self { 
        let bytes = keccak256(byte_slice); //[u8,32]
        KeyHash{bytes}
    }
}


#[cfg(test)]
mod tests {
    use super::*;

   #[test] 
    fn test_process() {
        //helper
        fn password_input_helper(input : &str) -> Result<PasswordInput, Error> {
            let mut content = input.trim().as_bytes().to_vec();
            let mut length = content.len();
            let mut password_input = PasswordInput { content, length };
            if password_input.exceed_max_length() {
                return Err(Error::new(ErrorKind::InvalidInput, "Input is too long"))
            }
            if password_input.not_empty() {
                content = password_input.pad_content();
                length = content.len();
                password_input = PasswordInput{content, length};
            }
            Ok(password_input)
        }
        //helper
        fn process_input(input: &str, expected_len: usize)  {
            let password_input = password_input_helper(input).unwrap();
            let content_slice = password_input.content.as_slice();
            let expected = input.as_bytes();
            let len = password_input.length;
            assert_eq!(content_slice, expected);
            assert_eq!(len, expected_len);
        }
        //testing normal input
        process_input("qwerty", 32);

         //testing empty input
        process_input("", 0);

        fn larger_max_length_input(len: usize){
            let zero_vec = vec![0u8; len];
            let s = String::from_utf8(zero_vec).expect("Found invalid UTF-8");
            let password_input = password_input_helper(&s);
            assert!(password_input.is_err());
        }
        //error_input(33);
    }
}



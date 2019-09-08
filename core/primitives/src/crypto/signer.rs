extern crate sodiumoxide;
extern crate tiny_keccak;

use sodiumoxide::crypto::secret_box;
use sodiumoxide::crypto::secretbox::xsalsa20poly1305::Key;
use tiny_keccak::keccak256;

use std::fs;
use std::fs::File;
use std::io::{Error, Read, Write};
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
    #[serde(default)]
    pub key_hash: KeyHash
}

impl KeyFile {
    fn write_to_file(&self, path: &Path) {
        let mut file = File::create(path).expect("Failed to create / write a key file.");
        let str = serde_json::to_string_pretty(self).expect("Error serializing the key file.");
        if let Err(err) = file.write_all(str.as_bytes()) {
            panic!("Failed to write a key file {}", err);
        }
    }
    // is_encrypted checks if current keyhash struct matches default one
    fn is_encrypted(&self)-> bool {
        let keyhash = self.key_hash; 
        let default_keyhash = KeyHash::default(); //?
        keyhash.eq(default_keyhash)
    }
    //asks for a password in console and encrypt a  private key ( secret_key field from keyFile structure) with an entered password
    // if password is empty just write to the file
    fn write_to_encrypted_file(&self, path: &Path) - > Result<(), Error> {   
        let password_input = PasswordInput::process()?;//ask for input of password
        if password_input.not_empty() {
            let secretkey_vec = Vec::<u8>::from(self.secret_key);
            let encryption_key = password_input.gen_key_from_password_input()?;
            let key_hash = KeyHash::new(&password_input.content);
            let encrypted_secretkey = ssb.encrypt(&secretkey_vec, &encryption_key);
            //ToDO convert encrypted_secretkey from Vec<u8>to SecretKeyView
            let key_file = KeyFile { self.account_id, self.public_key, encrypted_secretkey, key_hash } //?
            key_file.write_to_file(path);
            Ok(())
        }
            // console input is empty do not encryot, just write to the file
            self.write_to_file(path); //self.key_hash will be default
            Ok(())
    }

    fn read_from_file(path: &Path) -> Self {  
        let mut file = File::open(path).expect("Could not open key file.");
        let mut content = String::new();
        file.read_to_string(&mut content).expect("Could not read from key file.");
        serde_json::from_str(&content).expect("Failed to deserialize KeyFile")
    }
     //decrypts a file and return an instance of the file
    fn read_from_encrypted_file(&self, path: &Path, key : &Key) -> Self {  // reads the file and the decodes it with the password
        let ssb = SodiumoxideSecretBox::new();
        let decrypted_secretkey = ssb.decrypt(&self.secret_key, &key); //Vec<u8>

        // TODO to implement this method
        let secret_keyview = SecretKeyView::From(decrypted_secretkey);
        //return decrypted Keyfile.secret_key
        KeyFile{secret_key: secret_keyview, ..self} //?  i am not sure if it is correct
    }

    
    //if a private key is encrypted , ask for a password in console
    //if an entered password matches a password entered in step 3, decode an encrypted password otherwise keep asking for a correct password
    
    fn read_from_encrypted_file_with_password(path :  &Path) -> Result<KeyFile, Error>  {  
        let keyfile = KeyFile::read_from_file(path);
        if keyfile.is_encrypted()   {
            let password_input = PasswordInput::process()?;//ask for a password in command line
            if  password_input.not_empty() {   //password is not empty decode a file with a passwordm if password empty do nothing
                    let decryption_keyhash = KeyHash::new(&password_input.content);
                    let encryption_keyhash = keyfile.key_hash; 
                    if decryption_keyhash.eq(encryption_keyhash) { //if hashes of passwords match then decrypt Keyfile.secret_key
                        let decryption_key  = password_input.gen_key_from_password_input()?;
                        let key_file = keyfile.read_from_encrypted_file(path, &decryption_key);
                        Ok(key_file)
                    }  
            } 
        }
         //keyfile is not encrypted or imported password is empty
        Ok(keyfile)
    }

}

pub fn perform_encryption(
    key_file : &KeyFile,
    key_store_path: &Path,
    home_dir: &Path,
    ) -> Result<(), Error> {
        key_file.write_to_file(&key_store_path);
        
        if home_dir.join("config.json").exists() {
            //add password corrrectness validation to read_from_encrypted_file_with_password
            let new_key_file = KeyFile::read_from_encrypted_file_with_password(&key_store_path)?;
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
        let byte_input = input.trim().as_bytes().to_vec();
        let input_len = input.len();
        PasswordInput { byte_input, input_len }
    }

    pub fn exceed_max_length(&self) -> bool {
        const MAX_LENGTH: usize = 32;
        self.length > MAX_LENGTH
    }

    // xsalsa20poly1305 restricts key size to 32
    // https://github.com/sodiumoxide/sodiumoxide/blob/master/src/crypto/secretbox/xsalsa20poly1305.rs#L50
    pub fn pad_content(&self) -> Vec<u8> {
        let mut buf = [0u8; 32];
        buf.copy_from_slice(self.content[..]);
        buf.to_vec()
    }

    pub fn not_empty(&self) -> bool {
        self.length > 0
    }

    pub fn process() -> Result<Self, Error> {  
        let mut password_input = PasswordInput::enter_from_console();
        if password_input.exceed_max_length() {
            Err((Error::new(ErrorKind::InvalidInput, "Input is too long")))
        }
        //  if user didn't enter password - don't encrypt or decrypt
        if password_input.not_empty() {
            let pad_vec = password_input.pad_content();
            password_input = PasswordInput{pad_vec, pad_vec.len()};
            Ok(password_input)
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
            Err((Error::new(
                ErrorKind::InvalidInput,
                "Failed to create key from slice".to_string(),
            )))
        })
    }

}

#[derive(Deserialize, Serialize)]
pub struct KeyHash {
    pub bytes: [u8, 32]
}

impl Default for KeyHash {
    fn default() -> Self {
        let zero_vec = vec![0u8, 32];
        KeyHash::new(&zero_vec)
    }
}

impl Eq for KeyHash {
    fn eq(&self, other: &KeyHash) -> bool {
        self.bytes.iter().zip(other.bytes.iter()).all(|(a,b)| a == b) 
    }
}

impl KeyHash {
    fn new(bytes : &[u8])-> Self { 
        let hash_array = keccak256(bytes); //[u8,32]
        KeyHash{hash_array}
    }
}

impl Operation for SodiumoxideSecretBox {
    fn encrypt(&self, m: &[u8], k: &Key) -> Vec<u8> {
        secretbox::seal(m, &self.nonce, &k)
    }

    fn decrypt(&self, c: &[u8], k: &Key) -> Vec<u8> {
        secretbox::open(c, &self.nonce, &k)
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


#[cfg(test)]
mod tests {
    use super::*;

   #[test] 
    fn test_process() {
        //helper
        fn password_input_helper(input : &str) -> Result<PasswordInput, Error> {
            let byte_input = input.trim().as_bytes().to_vec();
            let input_len = input.len();
            let password_input = PasswordInput { byte_input, input_len };
            if password_input.exceed_max_length() {
                Err((Error::new(ErrorKind::InvalidInput, "Input is too long"))
             }
            if password_input.not_empty() {
                let pad_vec = password_input.pad_content();
                password_input = PasswordInput{pad_vec, pad_vec.len()};
                Ok(password_input)
            }
            Ok(password_input)
        }
        //helper
        fn process_input(input: &str, expected_len: usize)  {
            let password_input = password_input_helper(input).unwrap();
            let content_slice = &password_input.content;
            let expected = input.to_string().as_bytes;
            let len = password_input.length ;
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
        error_input(33);
    }
}


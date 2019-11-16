// The keys used to store the persistent state of the contract.
// Prefixes 0, 1, 2 are reserved for the keys.
/// The amount of locked tokens, `u128`, which should be at all times less or equal to the current
/// balance of the account.
const KEY_LOCK_AMOUNT: &[u8] = &[3u8];
/// The block timestamp, `u64`, after which all locked tokens become unlocked.
const KEY_LOCK_TIMESTAMP: &[u8] = &[4u8];

/// Stakes the provided amount of tokens.
///
/// Can be called with: regular keys, privileged keys, foundation keys.
#[no_mangle]
pub fn stake() {}

/// Transfer the given amount.
///
/// Can be called with: regular keys, privileged keys, foundation keys.
#[no_mangle]
pub fn transfer() {}

/// Different types of keys can add/remove other types of keys. Here is the alignment matrix:
/// <vertical> can be added/removed by <horizontal>:
///              regular     privileged      foundation
/// regular      -           +               +
/// privileged   -           +               +
/// foundation   -           -               +
pub enum KeyType {
    Regular,
    Privileged,
    Foundation,
}

/// The maxium key length, including prefixes like `ed25519` and `secp256k1`.
const MAX_KEY_LEN: u64 = 100;

const REGULAR_KEY_METHODS: &[u8] = b"stake,transfer";
const PRIVILEGED_KEY_METHODS: &[u8] = b"add_key,remove_key";

/// The key type provided by the user is incorrect.
const ERR_INCORRECT_KEY_TYPE: &[u8] = b"Incorrect key type";
/// The key that was used to sign the transaction is not known.
const ERR_KEY_WAS_NOT_FOUND: &[u8] = b"Key was not found";
/// The key that was used to sign the transaction is not powerful enough to add or remove another key.
const ERR_KEY_RIGHTS: &[u8] = b"Key does not have rights to add or remove another key";

impl KeyType {
    pub fn to_u8(&self) -> u8 {
        match self {
            KeyType::Regular => 0,
            KeyType::Privileged => 1,
            KeyType::Foundation => 2,
        }
    }

    pub fn from_u8(value: u8) -> Self {
        match value {
            0 => KeyType::Regular,
            1 => KeyType::Privileged,
            2 => KeyType::Foundation,
            _ => unsafe {
                panic_utf8(
                    ERR_INCORRECT_KEY_TYPE.len() as u64,
                    ERR_INCORRECT_KEY_TYPE.as_ptr() as u64,
                );
                unreachable!()
            },
        }
    }
    pub fn signer_key_type() -> Self {
        unsafe {
            signer_account_pk(0);
            let mut key = [0u8; MAX_KEY_LEN as usize];
            read_register(0, key.as_ptr() as u64 + 1);
            for prefix in 0u8..=2u8 {
                key[0] = prefix;
                if storage_read(key.len() as u64, key.as_ptr() as u64, 1) == 1 {
                    return Self::from_u8(prefix);
                }
            }
            unsafe {
                panic_utf8(
                    ERR_KEY_WAS_NOT_FOUND.len() as u64,
                    ERR_KEY_WAS_NOT_FOUND.as_ptr() as u64,
                );
                unreachable!()
            }
        }
    }

    /// Check whether this key can add or remove the other key.
    pub fn check_can_add_remove(&self, other: &Self) {
        let ok = match (self, other) {
            (KeyType::Regular, _) => false,
            (KeyType::Privileged, KeyType::Regular) => true,
            (KeyType::Privileged, KeyType::Privileged) => true,
            (KeyType::Privileged, KeyType::Foundation) => false,
            (KeyType::Foundation, _) => true,
        };
        if !ok {
            unsafe {
                panic_utf8(ERR_KEY_RIGHTS.len() as u64, ERR_KEY_RIGHTS.as_ptr() as u64);
            }
        }
    }
}

/// Input interpreted as key and key type.
struct InputKeyKeyType {
    /// Key with key type.
    pub data: [u8; MAX_KEY_LEN as usize],
    /// Length of the key with key type.
    pub key_len: u64,
    key_type: KeyType,
}

impl InputKeyKeyType {
    /// Read key type and key from the input.
    pub fn from_input() -> Self {
        unsafe {
            let data = [0u8; MAX_KEY_LEN];
            input(0);
            let key_len = register_len(0);
            read_register(0, data.as_ptr() as u64);
            let key_type = KeyType::from_u8(data[0]);
            Self { data, key_len, key_type }
        }
    }

    pub fn check_signer(&self) {
        let signer_key_type = KeyType::signer_key_type();
        signer_key_type.check_can_add_remove(&self.key_type);
    }
}

/// Represents empty value.
const EMPTY_VALUE: &[u8] = &[];

/// Adds this key to storage and issues a key creation transaction.
#[no_mangle]
pub fn add_key() {
    let inp = InputKeyKeyType::from_input();
    inp.check_signer();
    unsafe {
        storage_write(
            inp.key_len,
            inp.data.as_ptr() as u64,
            EMPTY_VALUE.len() as u64,
            EMPTY_VALUE.as_ptr() as u64,
        );
    }
}

/// Removes a key from this account.
#[no_mangle]
pub fn remove_key() {}

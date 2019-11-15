#![no_std]
//#![feature(core_intrinsics)]

//#[panic_handler]
//#[no_mangle]
//pub fn panic(_info: &::core::panic::PanicInfo) -> ! {
//    unsafe {
//        ::core::intrinsics::abort();
//    }
//}

use core::mem::size_of;

#[allow(unused)]
extern "C" {
    // #############
    // # Registers #
    // #############
    fn read_register(register_id: u64, ptr: u64);
    fn register_len(register_id: u64) -> u64;
    // ###############
    // # Context API #
    // ###############
    fn current_account_id(register_id: u64);
    fn signer_account_id(register_id: u64);
    fn signer_account_pk(register_id: u64);
    fn predecessor_account_id(register_id: u64);
    fn input(register_id: u64);
    fn block_index() -> u64;
    fn storage_usage() -> u64;
    // #################
    // # Economics API #
    // #################
    fn account_balance(balance_ptr: u64);
    fn attached_deposit(balance_ptr: u64);
    fn prepaid_gas() -> u64;
    fn used_gas() -> u64;
    // ############
    // # Math API #
    // ############
    fn random_seed(register_id: u64);
    fn sha256(value_len: u64, value_ptr: u64, register_id: u64);
    // #####################
    // # Miscellaneous API #
    // #####################
    fn value_return(value_len: u64, value_ptr: u64);
    fn log_utf8(len: u64, ptr: u64);
    fn log_utf16(len: u64, ptr: u64);
    fn abort(msg_ptr: u32, filename_ptr: u32, line: u32, col: u32);
    // ################
    // # Promises API #
    // ################
    fn promise_create(
        account_id_len: u64,
        account_id_ptr: u64,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        amount_ptr: u64,
        gas: u64,
    ) -> u64;
    fn promise_then(
        promise_index: u64,
        account_id_len: u64,
        account_id_ptr: u64,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        amount_ptr: u64,
        gas: u64,
    ) -> u64;
    fn promise_and(promise_idx_ptr: u64, promise_idx_count: u64) -> u64;
    fn promise_batch_create(account_id_len: u64, account_id_ptr: u64) -> u64;
    fn promise_batch_then(promise_index: u64, account_id_len: u64, account_id_ptr: u64) -> u64;
    // #######################
    // # Promise API actions #
    // #######################
    fn promise_batch_action_create_account(promise_index: u64);
    fn promise_batch_action_deploy_contract(promise_index: u64, code_len: u64, code_ptr: u64);
    fn promise_batch_action_function_call(
        promise_index: u64,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        amount_ptr: u64,
        gas: u64,
    );
    fn promise_batch_action_transfer(promise_index: u64, amount_ptr: u64);
    fn promise_batch_action_stake(
        promise_index: u64,
        amount_ptr: u64,
        public_key_len: u64,
        public_key_ptr: u64,
    );
    fn promise_batch_action_add_key_with_full_access(
        promise_index: u64,
        public_key_len: u64,
        public_key_ptr: u64,
        nonce: u64,
    );
    fn promise_batch_action_add_key_with_function_call(
        promise_index: u64,
        public_key_len: u64,
        public_key_ptr: u64,
        nonce: u64,
        allowance_ptr: u64,
        receiver_id_len: u64,
        receiver_id_ptr: u64,
        method_names_len: u64,
        method_names_ptr: u64,
    );
    fn promise_batch_action_delete_key(
        promise_index: u64,
        public_key_len: u64,
        public_key_ptr: u64,
    );
    fn promise_batch_action_delete_account(
        promise_index: u64,
        beneficiary_id_len: u64,
        beneficiary_id_ptr: u64,
    );
    // #######################
    // # Promise API results #
    // #######################
    fn promise_results_count() -> u64;
    fn promise_result(result_idx: u64, register_id: u64) -> u64;
    fn promise_return(promise_id: u64);
    // ###############
    // # Storage API #
    // ###############
    fn storage_write(
        key_len: u64,
        key_ptr: u64,
        value_len: u64,
        value_ptr: u64,
        register_id: u64,
    ) -> u64;
    fn storage_read(key_len: u64, key_ptr: u64, register_id: u64) -> u64;
    fn storage_remove(key_len: u64, key_ptr: u64, register_id: u64) -> u64;
    fn storage_has_key(key_len: u64, key_ptr: u64) -> u64;
    fn storage_iter_prefix(prefix_len: u64, prefix_ptr: u64) -> u64;
    fn storage_iter_range(start_len: u64, start_ptr: u64, end_len: u64, end_ptr: u64) -> u64;
    fn storage_iter_next(iterator_id: u64, key_register_id: u64, value_register_id: u64) -> u64;

    fn panic_utf8(len: u64, ptr: u64);
}

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

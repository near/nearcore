//! This is test contract, using features also used by sharded contracts. It
//! resembles how a sharded contract would roughly use these feature but
//! disregards all security and stability concerns.
//!
//! This contract only exists for testing the features in nearcore. It is not
//! meant to be used for anything real and should not be used to create real
//! contracts.
//!
//! There are 4 public methods in this contract.
//!
//! `owner_only`: Can only be called by one specific owner account.
//! `peer_only`: Can only be called by other sharded contracts.
//! `ping`: Calls peer_only of another account.
//! `spread`: Deploys the sharded contract on another account.

#![allow(clippy::missing_safety_doc)]

use borsh::BorshDeserialize;
use near_primitives_core::account::id::ParseAccountError;
use near_primitives_core::deterministic_account_id::{
    DeterministicAccountStateInit, DeterministicAccountStateInitV1,
};
use near_primitives_core::global_contract::GlobalContractIdentifier;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{AccountId, Balance};
use std::collections::BTreeMap;

#[allow(unused)]
extern "C" {
    fn read_register(register_id: u64, ptr: u64);
    fn register_len(register_id: u64) -> u64;
    fn predecessor_account_id(register_id: u64);
    fn input(register_id: u64);
    fn account_balance(balance_ptr: u64);
    fn attached_deposit(balance_ptr: u64);
    fn keccak256(value_len: u64, value_ptr: u64, register_id: u64);
    fn panic_utf8(len: u64, ptr: u64) -> !;
    fn log_utf8(len: u64, ptr: u64);
    fn promise_return(promise_id: u64);
    fn promise_batch_create(account_id_len: u64, account_id_ptr: u64) -> u64;
    fn promise_batch_action_function_call_weight(
        promise_index: u64,
        method_name_len: u64,
        method_name_ptr: u64,
        arguments_len: u64,
        arguments_ptr: u64,
        amount_ptr: u64,
        gas: u64,
        gas_weight: u64,
    );
    fn storage_read(key_len: u64, key_ptr: u64, register_id: u64) -> u64;

    fn promise_set_refund_to(promise_index: u64, account_id_len: u64, account_id_ptr: u64);
    fn promise_batch_action_state_init(
        promise_idx: u64,
        code_len: u64,
        code_ptr: u64,
        amount_ptr: u64,
    ) -> u64;
    fn promise_batch_action_state_init_by_account_id(
        promise_idx: u64,
        account_id_len: u64,
        code_hash_ptr: u64,
        amount_ptr: u64,
    ) -> u64;
    fn set_state_init_data_entry(
        promise_idx: u64,
        action_index: u64,
        key_len: u64,
        key_ptr: u64,
        value_len: u64,
        value_ptr: u64,
    );
    fn promise_result_length(result_idx: u64) -> u64;
    fn current_contract_code(register_id: u64) -> u64;
    fn refund_to_account_id(register_id: u64);
}

const OWNER_KEY: &[u8] = b"root";

const REG_INPUT: u64 = 0;
const REG_A: u64 = 1;
const REG_B: u64 = 2;

/// Print "root ok" if predecessor owns this contract, panic otherwise.
#[unsafe(no_mangle)]
pub unsafe fn owner_only() {
    root_check();
    log("root ok");
}

/// Print "peer ok" if predecessor is a peer of this contract, panic otherwise.
#[unsafe(no_mangle)]
pub unsafe fn peer_only() {
    peer_check();
    log("peer ok");
}

/// Call peer_only on a peer.
#[unsafe(no_mangle)]
pub unsafe fn ping() {
    // Receiver must be set as input arg
    input(REG_INPUT);

    let promise_idx = promise_batch_create(u64::MAX, REG_INPUT);

    let method = "peer_only";
    let balance = 0u128.to_le_bytes();

    // Owner must be set as arg
    storage_read(OWNER_KEY.len() as u64, OWNER_KEY.as_ptr() as u64, REG_A);

    promise_batch_action_function_call_weight(
        promise_idx,
        method.len() as u64,
        method.as_ptr() as u64,
        u64::MAX,
        REG_A,
        balance.as_ptr() as u64,
        0,
        1,
    );
}

/// Deploy a new instance of the sharded contract on a different account.
///
/// expect input to be a single string that's the receiver
#[unsafe(no_mangle)]
pub unsafe fn spread() {
    // Owner must be set as input arg
    input(REG_INPUT);
    let owner = register_to_memory(REG_INPUT);

    // derive id to be created
    let state_init = construct_state_init(owner);
    let receiver_id = derive_near_deterministic_account_id(&state_init);

    // Create a new receipt
    let promise_idx = promise_batch_create(receiver_id.len() as u64, receiver_id.as_ptr() as u64);

    // If the predecessor has sent balance, pass it on to fund the new account.
    let attached = {
        let mut buf = [0u8; 16];
        attached_deposit(buf.as_mut_ptr() as u64);
        u128::from_le_bytes(buf)
    };
    let send_balance = if attached > 0 {
        // Pass on refund_to, ensuring the predecessor gets the balance back in case of refund.
        refund_to_account_id(REG_A);
        promise_set_refund_to(promise_idx, u64::MAX, REG_A);
        attached
    } else {
        // Use our own balance to send as much balance as needed to the receiver.
        // Keep the refund to ourself.
        std::cmp::min(free_balance(), new_account_storage_cost(&state_init))
    };
    let send_balance_bytes = send_balance.to_le_bytes();

    // Add state init action
    let action_idx = match sharded_contract_id() {
        GlobalContractIdentifier::CodeHash(hash) => promise_batch_action_state_init(
            promise_idx,
            hash.as_bytes().len() as u64,
            hash.as_bytes().as_ptr() as u64,
            send_balance_bytes.as_ptr() as u64,
        ),
        GlobalContractIdentifier::AccountId(account_id) => {
            promise_batch_action_state_init_by_account_id(
                promise_idx,
                account_id.as_bytes().len() as u64,
                account_id.as_bytes().as_ptr() as u64,
                send_balance_bytes.as_ptr() as u64,
            )
        }
    };

    // Add data record to state init action
    set_state_init_data_entry(
        promise_idx,
        action_idx,
        OWNER_KEY.len() as u64,
        OWNER_KEY.as_ptr() as u64,
        // read the value from register rather than memory
        u64::MAX,
        REG_INPUT,
    );

    log(&format!("spreading to {receiver_id}"));
    promise_return(promise_idx);
}

// ***********************
// Access Check Functions
// ***********************

/// Check that the predecessor is the owner of this sharded contract instance.
unsafe fn root_check() {
    storage_read(OWNER_KEY.len() as u64, OWNER_KEY.as_ptr() as u64, REG_A);
    predecessor_account_id(REG_B);

    if !registers_eq(REG_A, REG_B) {
        let expected_predecessor = bytes_to_string(register_to_memory(REG_A));
        let predecessor = bytes_to_string(register_to_memory(REG_B));

        panic(&format!("not root: {expected_predecessor} != {predecessor}"));
    }
}

/// Check that the predecessor is of the same sharded contract type as us.
///
/// Assumes the input is a single string naming the owner of the peer instance.
unsafe fn peer_check() {
    // Read argument to a Vec<u8>
    input(REG_A);
    let owner = register_to_memory(REG_A);

    // derive what the predecessor id should be
    let state_init = construct_state_init(owner);
    let expected_predecessor = derive_near_deterministic_account_id(&state_init);

    // read actual predecessor id
    predecessor_account_id(REG_A);
    let predecessor = register_to_memory(REG_A);

    if expected_predecessor.as_bytes() != predecessor {
        let predecessor = bytes_to_string(predecessor);
        panic(&format!("not peer: {expected_predecessor} != {predecessor}"));
    }
}

// ******************************************
// Deterministic Account Id Helper Functions
// ******************************************

unsafe fn construct_state_init(owner: Vec<u8>) -> DeterministicAccountStateInit {
    // Read current code, which the caller must also be using
    let code = sharded_contract_id();
    // Create full state init from it
    DeterministicAccountStateInit::V1(DeterministicAccountStateInitV1 {
        code,
        data: BTreeMap::from_iter([(OWNER_KEY.to_vec(), owner)]),
    })
}

unsafe fn sharded_contract_id() -> GlobalContractIdentifier {
    match current_contract_code(REG_A) {
        0 => panic("contract can't be None"),
        1 => panic("contract mustn't be Local"),
        2 => {
            let code_hash = register_to_memory(REG_A);
            GlobalContractIdentifier::CodeHash(
                CryptoHash::try_from_slice(&code_hash).unwrap_or_else(|e| panic(&e.to_string())),
            )
        }
        3 => {
            let account_id_bytes = register_to_memory(REG_A);
            let account_id_string = bytes_to_string(account_id_bytes);
            let account_id: AccountId = account_id_string
                .parse()
                .unwrap_or_else(|e: ParseAccountError| panic(&e.to_string()));
            GlobalContractIdentifier::AccountId(account_id)
        }
        _ => panic("unknown contract type"),
    }
}

unsafe fn derive_near_deterministic_account_id(
    state_init: &DeterministicAccountStateInit,
) -> String {
    let data = borsh::to_vec(&state_init).expect("borsh must not fail");
    keccak256(data.len() as u64, data.as_ptr() as u64, REG_A);
    let hash = register_to_memory(REG_A);

    let hex_string: String = hash[12..32].iter().map(|b| format!("{:02x}", b)).collect();
    format!("0s{hex_string}")
}

unsafe fn new_account_storage_cost(state_init: &DeterministicAccountStateInit) -> u128 {
    // using hard-coded constant values until protocol support for looking up
    // these from protocol config is given
    let byte_cost = 10000000000000000000;
    let bytes_account = 100;
    let extra_bytes_record = 40;

    let total_bytes = state_init.len_bytes() as u64
        + state_init.code().len() as u64
        + state_init.data().len() as u64 * extra_bytes_record
        + bytes_account;

    total_bytes as u128 * byte_cost
}

unsafe fn free_balance() -> u128 {
    let mut buf = [0u8; 16];
    account_balance(buf.as_mut_ptr() as u64);
    let total_balance = u128::from_le_bytes(buf);

    // keep 1 Near for us
    total_balance.saturating_sub(Balance::from_near(1).as_yoctonear())
}

// ****************************
// General Helper Functions
// ****************************

unsafe fn panic(msg: &str) -> ! {
    panic_utf8(msg.len() as u64, msg.as_ptr() as u64);
}

unsafe fn log(msg: &str) {
    log_utf8(msg.len() as u64, msg.as_ptr() as u64);
}

unsafe fn register_to_memory(register: u64) -> Vec<u8> {
    let mut buf = vec![0; register_len(register) as usize];
    read_register(register, buf.as_mut_ptr() as u64);
    buf
}

unsafe fn registers_eq(a: u64, b: u64) -> bool {
    register_to_memory(a) == register_to_memory(b)
}

unsafe fn bytes_to_string(bytes: Vec<u8>) -> String {
    String::from_utf8(bytes).unwrap_or_else(|e| panic(&e.to_string()))
}

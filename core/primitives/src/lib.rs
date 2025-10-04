#![cfg_attr(enable_const_type_id, feature(const_type_id))]

pub use near_primitives_core::account;
pub use near_primitives_core::apply;
pub use near_primitives_core::borsh;
pub use near_primitives_core::config;
pub use near_primitives_core::deterministic_account_id;
pub use near_primitives_core::gas;
pub use near_primitives_core::global_contract;
pub use near_primitives_core::hash;
pub use near_primitives_core::num_rational;
pub use near_primitives_core::serialize;

pub mod action;
pub mod bandwidth_scheduler;
pub mod block;
pub mod block_body;
pub mod block_header;
pub mod challenge;
pub mod chunk_apply_stats;
pub mod congestion_info;
pub mod epoch_block_info;
pub mod epoch_info;
pub mod epoch_manager;
pub mod epoch_sync;
pub mod errors;
pub mod genesis;
pub mod merkle;
pub mod network;
pub mod optimistic_block;
pub mod profile_data_v2;
pub mod profile_data_v3;
pub mod rand;
pub mod receipt;
#[cfg(feature = "solomon")]
pub mod reed_solomon;
pub mod sandbox;
pub mod shard_layout;
pub mod sharding;
pub mod signable_message;
pub mod state;
pub mod state_part;
pub mod state_record;
pub mod state_sync;
pub mod stateless_validation;
pub mod telemetry;
#[cfg(feature = "test_utils")]
pub mod test_utils;
pub mod transaction;
pub mod trie_key;
pub mod types;
pub mod upgrade_schedule;
pub mod utils;
pub mod validator_mandates;
pub mod validator_signer;
pub mod version;
pub mod views;

pub use near_primitives_core::chains;

#[test]
#[should_panic = "attempt to add with overflow"]
fn test_overflow() {
    let a = u64::MAX;
    let b = 5u64;
    let c = u128::from(a + b);
    println!("{} + {} = {}", a, b, c);
}

#[test]
#[should_panic = "attempt to subtract with overflow"]
fn test_underflow() {
    let a = 10u64;
    let b = 5u64;
    let c = u128::from(b - a);
    println!("{} - {} = {}", b, a, c);
}

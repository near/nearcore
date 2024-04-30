pub use near_primitives_core::account;
pub use near_primitives_core::borsh;
pub use near_primitives_core::config;
pub use near_primitives_core::hash;
pub use near_primitives_core::num_rational;
pub use near_primitives_core::serialize;

pub mod action;
pub mod block;
pub mod block_body;
pub mod block_header;
pub mod challenge;
pub mod congestion_info;
pub mod epoch_manager;
pub mod epoch_sync;
pub mod errors;
pub mod merkle;
pub mod network;
pub mod profile_data_v2;
pub mod rand;
pub mod receipt;
pub mod reed_solomon;
pub mod runtime;
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
pub mod test_utils;
pub mod transaction;
pub mod trie_key;
pub mod types;
mod upgrade_schedule;
pub mod utils;
pub mod validator_mandates;
pub mod validator_signer;
pub mod version;
pub mod views;

pub use crate::version::checked_feature;
pub use near_primitives_core::chains;

#[cfg(fuzz)]
#[test]
fn failing_fuzzer() {
    // This fuzzer always fails. It is used as a sanity-check that our clusterfuzz instance
    // is working properly, as it has silently stopped working quite a few times already.
    bolero::check!().for_each(|_| panic!("The expected-to-fail fuzzer actually failed"))
}

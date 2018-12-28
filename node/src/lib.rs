extern crate log;
extern crate node_cli;
extern crate primitives;
extern crate txflow;

use std::ffi::{CStr};
use std::os::raw::{c_char};
use std::path::PathBuf;
use std::str::FromStr;
use node_cli::service;
use txflow::txflow_task::beacon_witness_selector::BeaconWitnessSelector;
use primitives::types::ChainPayload;


#[no_mangle]
pub extern fn run_with_config(
    base_path_ptr: *const c_char,
    account_id_ptr: *const c_char,
    public_key_ptr: *const c_char,
    chain_spec_path_ptr: *const c_char,
    log_level_ptr: *const c_char,
    p2p_port: u16,
    rpc_port: u16,
    boot_nodes_ptr: *const c_char,
    test_network_key_seed_val: u32) {

    unsafe {
        let base_path = CStr::from_ptr(base_path_ptr)
            .to_str()
            .map(PathBuf::from)
            .unwrap();

        let account_id = CStr::from_ptr(account_id_ptr)
            .to_str()
            .map(String::from)
            .unwrap();

        let public_key = CStr::from_ptr(public_key_ptr)
            .to_str()
            .map(String::from)
            .ok();

        let chain_spec_path = CStr::from_ptr(chain_spec_path_ptr)
            .to_str()
            .map(PathBuf::from)
            .ok();

        let log_level = CStr::from_ptr(log_level_ptr)
            .to_str()
            .map(log::LevelFilter::from_str)
            .unwrap()
            .unwrap();

        let boot_nodes = CStr::from_ptr(boot_nodes_ptr)
            .to_str()
            .unwrap()
            .split(",")
            .map(String::from)
            .collect();

        let test_network_key_seed = Some(test_network_key_seed_val); 

        let config = service::ServiceConfig {
            base_path,
            account_id,
            public_key,
            chain_spec_path,
            log_level,
            p2p_port,
            rpc_port,
            boot_nodes,
            test_network_key_seed,
        };

        service::start_service(
            config,
            txflow::txflow_task::spawn_task::<ChainPayload, BeaconWitnessSelector>
        );
    }
}

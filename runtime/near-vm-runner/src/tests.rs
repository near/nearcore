mod cache;
mod compile_errors;
mod fuzzers;
mod rs_contract;
mod runtime_errors;
pub(crate) mod test_builder;
mod ts_contract;
mod wasm_validation;

use crate::vm_kind::VMKind;
use near_primitives::version::ProtocolVersion;
use near_vm_logic::VMContext;

const CURRENT_ACCOUNT_ID: &str = "alice";
const SIGNER_ACCOUNT_ID: &str = "bob";
const SIGNER_ACCOUNT_PK: [u8; 3] = [0, 1, 2];
const PREDECESSOR_ACCOUNT_ID: &str = "carol";

const LATEST_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::MAX;

fn with_vm_variants(runner: fn(VMKind) -> ()) {
    #[cfg(all(feature = "wasmer0_vm", target_arch = "x86_64"))]
    runner(VMKind::Wasmer0);

    #[cfg(feature = "wasmtime_vm")]
    runner(VMKind::Wasmtime);

    #[cfg(all(feature = "wasmer2_vm", target_arch = "x86_64"))]
    runner(VMKind::Wasmer2);
}

fn create_context(input: Vec<u8>) -> VMContext {
    VMContext {
        current_account_id: CURRENT_ACCOUNT_ID.parse().unwrap(),
        signer_account_id: SIGNER_ACCOUNT_ID.parse().unwrap(),
        signer_account_pk: Vec::from(&SIGNER_ACCOUNT_PK[..]),
        predecessor_account_id: PREDECESSOR_ACCOUNT_ID.parse().unwrap(),
        input,
        block_index: 10,
        block_timestamp: 42,
        epoch_height: 1,
        account_balance: 2u128,
        account_locked_balance: 0,
        storage_usage: 12,
        attached_deposit: 2u128,
        prepaid_gas: 10_u64.pow(14),
        random_seed: vec![0, 1, 2],
        view_config: None,
        output_data_receivers: vec![],
    }
}

/// Small helper to compute expected loading gas cost charged before loading.
///
/// Includes hard-coded value for runtime parameter values
/// `wasm_contract_loading_base` and `wasm_contract_loading_bytes` which would
/// have to be updated if they change in the future.
#[allow(unused)]
fn prepaid_loading_gas(bytes: usize) -> u64 {
    if cfg!(feature = "protocol_feature_fix_contract_loading_cost") {
        35_445_963 + bytes as u64 * 21_6750
    } else {
        0
    }
}

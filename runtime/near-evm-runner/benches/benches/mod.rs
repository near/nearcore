use near_evm_runner::run_evm;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_vm_errors::VMError;
use near_vm_logic::{External, VMConfig, VMOutcome};

pub(crate) fn evm_call(
    ext: &mut dyn External,
    signer_id: &str,
    method_name: &str,
    args: Vec<u8>,
    is_view: bool,
) -> (Option<VMOutcome>, Option<VMError>) {
    let config = VMConfig::default();
    let fees_config = RuntimeFeesConfig::default();

    run_evm(
        ext,
        0x99,
        &config,
        &fees_config,
        &"evm".to_string(),
        &signer_id.to_string(),
        &signer_id.to_string(),
        10u128.pow(26),
        0,
        182,
        method_name.to_string(),
        args,
        300 * 10u64.pow(12),
        is_view,
    )
}

pub mod cryptozombies;

use near_parameters::RuntimeConfigStore;
use near_parameters::vm::VMKind;
use near_primitives_core::version::PROTOCOL_VERSION;
use near_vm_runner::{ContractCode, MockContractRuntimeCache, precompile_contract};
use std::sync::Arc;

fn main() -> anyhow::Result<()> {
    let env_filter = near_o11y::EnvFilterBuilder::from_env().verbose(Some("vm")).finish()?;
    let _subscriber = near_o11y::default_subscriber(env_filter, &Default::default()).global();

    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: bench-contracts-compilation <path>...");
        eprintln!("  example: bench-contracts-compilation contracts/*.wasm");
        std::process::exit(1);
    }

    let store = RuntimeConfigStore::new(None);
    let config = store.get_config(PROTOCOL_VERSION);
    let mut wasm_config = near_parameters::vm::Config::clone(&config.wasm_config);
    wasm_config.vm_kind = VMKind::Wasmtime;
    let wasm_config = Arc::new(wasm_config);

    let cache = MockContractRuntimeCache::default();

    for path in &args {
        let wasm = std::fs::read(path)?;
        let code = ContractCode::new(wasm, None);
        let name = std::path::Path::new(path).file_name().unwrap_or_default().to_string_lossy();
        match precompile_contract(&code, Arc::clone(&wasm_config), Some(&cache)) {
            Ok(Ok(_)) => {
                eprintln!("{name}: ok");
            }
            Ok(Err(err)) => {
                eprintln!("{name}: compilation error: {err:?}");
            }
            Err(err) => {
                eprintln!("{name}: cache error: {err:?}");
            }
        }
    }

    Ok(())
}

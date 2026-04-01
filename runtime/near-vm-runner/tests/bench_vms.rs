//! Benchmark comparing NearVM, Wasmtime (Cranelift), and Winch compilation times.
//!
//! Usage:
//!   # Wasmtime only:
//!   cargo build --release -p near-vm-runner --test bench_vms --features wasmtime_vm
//!
//!   # Wasmtime + Winch:
//!   cargo build --release -p near-vm-runner --test bench_vms --features 'wasmtime_vm,winch'
//!
//!   # Wasmtime + NearVM + Winch:
//!   cargo build --release -p near-vm-runner --test bench_vms --features 'near_vm,wasmtime_vm,winch'
//!
//!   # Run (use the actual binary, not the glob, to avoid the .d file):
//!   target/release/deps/bench_vms-<hash> <wasm_file> [wasm_file...]
//!
//!   # Parse results:
//!   ... | python3 runtime/near-vm-runner/tests/parse_bench_vms.py
//!
//! If no arguments are given, compiles a small default contract.

use near_parameters::vm::VMKind;
use near_vm_runner::{ContractCode, MockContractRuntimeCache, precompile_contract};
use std::sync::Arc;
use std::time::Instant;

fn bench_contract(path: &str) {
    let store = near_parameters::RuntimeConfigStore::new(None);
    let runtime_config = store.get_config(near_primitives_core::version::PROTOCOL_VERSION);
    let code_bytes = std::fs::read(path).unwrap();
    let name = std::path::Path::new(path).file_name().unwrap().to_string_lossy();

    let mut vm_kinds = vec![VMKind::Wasmtime];
    #[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
    vm_kinds.insert(0, VMKind::NearVm);
    for vm_kind in vm_kinds {
        let config = near_parameters::vm::Config {
            vm_kind,
            ..near_parameters::vm::Config::clone(&runtime_config.wasm_config)
        };
        let cache = MockContractRuntimeCache::default();
        let code = ContractCode::new(code_bytes.clone(), None);
        let start = Instant::now();
        let result = precompile_contract(&code, Arc::new(config), Some(&cache));
        let elapsed = start.elapsed();
        let status = match result {
            Ok(Ok(_)) => "ok".to_string(),
            Ok(Err(e)) => format!("{e:?}"),
            Err(e) => format!("{e:?}"),
        };
        println!("{vm_kind:?} {name:<55} {:>8.1}ms  {status}", elapsed.as_secs_f64() * 1000.0);
    }

    #[cfg(feature = "winch")]
    bench_winch(&code_bytes, &name);
}

#[cfg(feature = "winch")]
fn bench_winch(code_bytes: &[u8], name: &str) {
    use near_vm_runner::prepare;

    let store = near_parameters::RuntimeConfigStore::new(None);
    let runtime_config = store.get_config(near_primitives_core::version::PROTOCOL_VERSION);
    let config = near_parameters::vm::Config {
        vm_kind: VMKind::Wasmtime,
        ..near_parameters::vm::Config::clone(&runtime_config.wasm_config)
    };

    let prepared = prepare::prepare_contract(code_bytes, &config, VMKind::Wasmtime);
    match prepared {
        Ok(prepared_code) => {
            let mut engine_config = wasmtime::Config::default();
            engine_config
                .strategy(wasmtime::Strategy::Winch)
                // Match nearcore's settings that are compatible with Winch.
                .native_unwind_info(false)
                .wasm_backtrace(false)
                .wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Disable)
                .wasm_tail_call(false)
                .wasm_simd(false)
                .wasm_relaxed_simd(false)
                .wasm_wide_arithmetic(true);
            match wasmtime::Engine::new(&engine_config) {
                Ok(engine) => {
                    let start = Instant::now();
                    let result = engine.precompile_module(&prepared_code);
                    let elapsed = start.elapsed();
                    let status = match &result {
                        Ok(_) => "ok".to_string(),
                        Err(e) => format!("FAILED: {e:#}"),
                    };
                    println!(
                        "Winch    {name:<55} {:>8.1}ms  {status}",
                        elapsed.as_secs_f64() * 1000.0
                    );
                }
                Err(e) => println!("Winch    {name:<55} engine error: {e}"),
            }
        }
        Err(e) => println!("Winch    {name:<55} prepare error: {e:?}"),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        // Default: compile a small test contract.
        let wasm = wat::parse_str(r#"(module (func (export "main")))"#).unwrap();
        let tmp = std::env::temp_dir().join("bench_vms_default.wasm");
        std::fs::write(&tmp, &wasm).unwrap();
        bench_contract(tmp.to_str().unwrap());
    } else {
        for path in &args {
            bench_contract(path);
        }
    }
}

use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;

use threadpool::ThreadPool;

use near_primitives::hash::CryptoHash;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::CompiledContractCache;
use near_vm_errors::VMError;
use near_vm_logic::profile::ProfileData;
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{External, ProtocolVersion, VMConfig, VMContext, VMKind, VMOutcome};

use crate::cache;
use crate::preload::VMModule::Wasmer0;
use crate::wasmer_runner::run_wasmer_module;

enum VMModule {
    Wasmer0(wasmer_runtime::Module),
}

struct VMCallData {
    result: Result<VMModule, VMError>,
}

struct CallInner {
    rx: Receiver<VMCallData>,
}

// TODO: consider using https://crates.io/crates/scoped_threadpool and not clone the data.
#[derive(Clone)]
pub struct ContractCallPrepareRequest {
    pub code_hash: CryptoHash,
    pub code: Vec<u8>,
    pub vm_config: VMConfig,
    pub cache: Option<Arc<dyn CompiledContractCache>>,
}

#[derive(Clone)]
pub struct ContractCallPrepareResult {
    pub handle: Option<usize>,
    pub error: Option<VMError>,
}

pub struct ContractCaller {
    pool: ThreadPool,
    prepared: Vec<CallInner>,
}

impl ContractCaller {
    pub fn new(num_threads: usize) -> ContractCaller {
        ContractCaller { pool: ThreadPool::new(num_threads), prepared: Vec::new() }
    }

    pub fn preload<'a>(
        self: &mut ContractCaller,
        requests: Vec<ContractCallPrepareRequest>,
        vm_kind: VMKind,
    ) -> Vec<ContractCallPrepareResult> {
        let mut result: Vec<ContractCallPrepareResult> = Vec::new();
        for request in requests {
            let index = self.prepared.len();
            let (tx, rx) = channel();
            self.prepared.push(CallInner { rx });
            let copy_request = request.clone();
            let tx = tx.clone();
            self.pool.execute(move || {
                prepare_in_thread(copy_request, vm_kind, tx);
            });
            result.push(ContractCallPrepareResult { handle: Some(index), error: None });
        }
        result
    }

    pub fn run_preloaded<'a>(
        self: &mut ContractCaller,
        prepared: &ContractCallPrepareResult,
        method_name: &str,
        ext: &mut dyn External,
        context: VMContext,
        vm_config: &'a VMConfig,
        fees_config: &'a RuntimeFeesConfig,
        promise_results: &'a [PromiseResult],
        current_protocol_version: ProtocolVersion,
        profile: ProfileData,
    ) -> (Option<VMOutcome>, Option<VMError>) {
        match &prepared.error {
            Some(error) => return (None, Some(error.clone())),
            _ => {}
        }
        match prepared.handle {
            Some(handle) => {
                let call = self.prepared.get(handle).unwrap();
                let call_data = call.rx.recv().unwrap();
                return match call_data.result {
                    Err(err) => (None, Some(err)),
                    Ok(module) => match module {
                        Wasmer0(module) => run_wasmer_module(
                            module,
                            method_name,
                            ext,
                            context,
                            vm_config,
                            fees_config,
                            promise_results,
                            profile,
                            current_protocol_version,
                        ),
                    },
                };
            }
            None => panic!("Must be valid"),
        }
    }
}

impl Drop for ContractCaller {
    fn drop(&mut self) {
        self.pool.join();
    }
}

fn prepare_in_thread(request: ContractCallPrepareRequest, vm_kind: VMKind, tx: Sender<VMCallData>) {
    let cache = request.cache.as_deref();
    let result = match vm_kind {
        VMKind::Wasmer0 => cache::wasmer0_cache::compile_module_cached_wasmer(
            &(request.code_hash.0).0,
            request.code.as_slice(),
            &request.vm_config,
            cache,
        )
        .map(|module| VMModule::Wasmer0(module)),
        _ => panic!("Unsupported VM"),
    };
    tx.send(VMCallData { result }).unwrap();
}

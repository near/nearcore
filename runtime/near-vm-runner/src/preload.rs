use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;

use near_primitives::contract::ContractCode;
use threadpool::ThreadPool;

use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::CompiledContractCache;
use near_vm_errors::VMError;
use near_vm_logic::profile::ProfileData;
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{External, ProtocolVersion, VMConfig, VMContext, VMKind, VMOutcome};

use crate::cache;
use crate::preload::VMModule::{Wasmer0, Wasmer1};
use crate::wasmer1_runner::{default_wasmer1_store, run_wasmer1_module};
use crate::wasmer_runner::run_wasmer0_module;

enum VMModule {
    Wasmer0(wasmer_runtime::Module),
    Wasmer1(wasmer::Module, wasmer::Store),
}

struct VMCallData {
    result: Result<VMModule, VMError>,
}

struct CallInner {
    rx: Receiver<VMCallData>,
}
pub struct ContractCallPrepareRequest {
    pub code: Arc<ContractCode>,
    pub vm_config: VMConfig,
    pub cache: Option<Arc<dyn CompiledContractCache>>,
}

#[derive(Clone)]
pub struct ContractCallPrepareResult {
    handle: usize,
}

pub struct ContractCaller {
    pool: ThreadPool,
    prepared: Vec<CallInner>,
}

impl ContractCaller {
    pub fn new(num_threads: usize) -> ContractCaller {
        ContractCaller { pool: ThreadPool::new(num_threads), prepared: Vec::new() }
    }

    pub fn preload(
        &mut self,
        requests: Vec<ContractCallPrepareRequest>,
        vm_kind: VMKind,
    ) -> Vec<ContractCallPrepareResult> {
        let mut result: Vec<ContractCallPrepareResult> = Vec::new();
        for request in requests {
            let index = self.prepared.len();
            let (tx, rx) = channel();
            self.prepared.push(CallInner { rx });
            self.pool.execute({
                let tx = tx.clone();
                move || prepare_in_thread(request, vm_kind, tx)
            });
            result.push(ContractCallPrepareResult { handle: index });
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
        match self.prepared.get(prepared.handle) {
            Some(call) => {
                let call_data = call.rx.recv().unwrap();
                match call_data.result {
                    Err(err) => (None, Some(err)),
                    Ok(module) => match module {
                        Wasmer0(module) => run_wasmer0_module(
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
                        Wasmer1(module, store) => run_wasmer1_module(
                            &module,
                            &store,
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
                }
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
        VMKind::Wasmer0 => cache::wasmer0_cache::compile_module_cached_wasmer0(
            &request.code,
            &request.vm_config,
            cache,
        )
        .map(VMModule::Wasmer0),
        VMKind::Wasmer1 => {
            let store = default_wasmer1_store();
            cache::wasmer1_cache::compile_module_cached_wasmer1(
                &request.code,
                &request.vm_config,
                cache,
                &store,
            )
            .map(|m| VMModule::Wasmer1(m, store))
        }

        _ => panic!("Unsupported VM"),
    };
    tx.send(VMCallData { result }).unwrap();
}

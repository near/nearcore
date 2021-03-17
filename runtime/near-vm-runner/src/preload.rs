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
use crate::memory::WasmerMemory;
use crate::preload::VMModule::{Wasmer0, Wasmer1};
use crate::wasmer1_runner::{default_wasmer1_store, run_wasmer1_module, Wasmer1Memory};
use crate::wasmer_runner::run_wasmer0_module;

enum VMModule {
    Wasmer0(wasmer_runtime::Module),
    Wasmer1(wasmer::Module),
}

enum VMDataPrivate {
    Wasmer0(WasmerMemory),
    Wasmer1(Wasmer1Memory),
}

#[derive(Clone)]
enum VMDataShared {
    Wasmer0,
    Wasmer1(wasmer::Store),
}

struct VMCallData {
    result: Result<VMModule, VMError>,
}

struct CallInner {
    rx: Receiver<VMCallData>,
}
pub struct ContractCallPrepareRequest {
    pub code: Arc<ContractCode>,
    pub cache: Option<Arc<dyn CompiledContractCache>>,
}

#[derive(Clone)]
pub struct ContractCallPrepareResult {
    handle: usize,
}

pub struct ContractCaller {
    pool: ThreadPool,
    vm_kind: VMKind,
    vm_config: VMConfig,
    vm_data_private: VMDataPrivate,
    vm_data_shared: VMDataShared,
    prepared: Vec<CallInner>,
}

impl ContractCaller {
    pub fn new(num_threads: usize, vm_kind: VMKind, vm_config: VMConfig) -> ContractCaller {
        let (shared, private) = match vm_kind {
            VMKind::Wasmer0 => (
                VMDataShared::Wasmer0,
                VMDataPrivate::Wasmer0(
                    WasmerMemory::new(
                        vm_config.limit_config.initial_memory_pages,
                        vm_config.limit_config.max_memory_pages,
                    )
                    .unwrap(),
                ),
            ),
            VMKind::Wasmer1 => {
                let store = default_wasmer1_store();
                let store_clone = store.clone();
                (
                    VMDataShared::Wasmer1(store),
                    VMDataPrivate::Wasmer1(
                        Wasmer1Memory::new(
                            &store_clone,
                            vm_config.limit_config.initial_memory_pages,
                            vm_config.limit_config.max_memory_pages,
                        )
                        .unwrap(),
                    ),
                )
            }
            _ => panic!("Not currently supported"),
        };
        ContractCaller {
            pool: ThreadPool::new(num_threads),
            vm_kind,
            vm_config,
            vm_data_private: private,
            vm_data_shared: shared,
            prepared: Vec::new(),
        }
    }

    pub fn preload(
        &mut self,
        requests: Vec<ContractCallPrepareRequest>,
    ) -> Vec<ContractCallPrepareResult> {
        let mut result: Vec<ContractCallPrepareResult> = Vec::new();
        for request in requests {
            let index = self.prepared.len();
            let (tx, rx) = channel();
            self.prepared.push(CallInner { rx });
            self.pool.execute({
                let tx = tx.clone();
                let vm_config = self.vm_config.clone();
                let vm_data_shared = self.vm_data_shared.clone();
                let vm_kind = self.vm_kind.clone();
                move || prepare_in_thread(request, vm_kind, vm_config, vm_data_shared, tx)
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
                    Ok(module) => {
                        match (&module, &mut self.vm_data_private, &self.vm_data_shared) {
                            (
                                Wasmer0(module),
                                VMDataPrivate::Wasmer0(memory),
                                VMDataShared::Wasmer0,
                            ) => run_wasmer0_module(
                                module.clone(),
                                memory,
                                method_name,
                                ext,
                                context,
                                &self.vm_config,
                                fees_config,
                                promise_results,
                                profile,
                                current_protocol_version,
                            ),
                            (
                                Wasmer1(module),
                                VMDataPrivate::Wasmer1(memory),
                                VMDataShared::Wasmer1(store),
                            ) => run_wasmer1_module(
                                &module,
                                store,
                                memory,
                                method_name,
                                ext,
                                context,
                                &self.vm_config,
                                fees_config,
                                promise_results,
                                profile,
                                current_protocol_version,
                            ),
                            _ => panic!("Incorrect logic"),
                        }
                    }
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

fn prepare_in_thread(
    request: ContractCallPrepareRequest,
    vm_kind: VMKind,
    vm_config: VMConfig,
    vm_data_shared: VMDataShared,
    tx: Sender<VMCallData>,
) {
    let cache = request.cache.as_deref();
    let result = match (vm_kind, vm_data_shared) {
        (VMKind::Wasmer0, VMDataShared::Wasmer0) => {
            cache::wasmer0_cache::compile_module_cached_wasmer0(&request.code, &vm_config, cache)
                .map(VMModule::Wasmer0)
        }
        (VMKind::Wasmer1, VMDataShared::Wasmer1(store)) => {
            cache::wasmer1_cache::compile_module_cached_wasmer1(
                &request.code,
                &vm_config,
                cache,
                &store,
            )
            .map(|m| VMModule::Wasmer1(m))
        }
        _ => panic!("Incorrect logic"),
    };
    tx.send(VMCallData { result }).unwrap();
}

use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;

use near_primitives::contract::ContractCode;
use threadpool::ThreadPool;

use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::CompiledContractCache;
use near_vm_errors::VMError;
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{External, ProtocolVersion, VMConfig, VMContext, VMOutcome};

use crate::cache::{self, into_vm_result};
use crate::memory::WasmerMemory;
use crate::preload::VMModule::{Wasmer0, Wasmer2};
use crate::vm_kind::VMKind;
use crate::wasmer2_runner::{default_wasmer2_store, run_wasmer2_module, Wasmer2Memory};
use crate::wasmer_runner::run_wasmer0_module;

const SHARE_MEMORY_INSTANCE: bool = false;

enum VMModule {
    Wasmer0(wasmer_runtime::Module),
    Wasmer2(wasmer::Module),
}

enum VMDataPrivate {
    Wasmer0(Option<WasmerMemory>),
    Wasmer2(Option<Wasmer2Memory>),
}

#[derive(Clone)]
enum VMDataShared {
    Wasmer0,
    Wasmer2(wasmer::Store),
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
    preloaded: Vec<CallInner>,
}

impl ContractCaller {
    pub fn new(num_threads: usize, vm_kind: VMKind, vm_config: VMConfig) -> ContractCaller {
        let (shared, private) = match vm_kind {
            VMKind::Wasmer0 => (
                VMDataShared::Wasmer0,
                VMDataPrivate::Wasmer0(if SHARE_MEMORY_INSTANCE {
                    Some(
                        WasmerMemory::new(
                            vm_config.limit_config.initial_memory_pages,
                            vm_config.limit_config.max_memory_pages,
                        )
                        .unwrap(),
                    )
                } else {
                    None
                }),
            ),
            VMKind::Wasmer2 => {
                let store = default_wasmer2_store();
                let store_clone = store.clone();
                (
                    VMDataShared::Wasmer2(store),
                    VMDataPrivate::Wasmer2(if SHARE_MEMORY_INSTANCE {
                        Some(
                            Wasmer2Memory::new(
                                &store_clone,
                                vm_config.limit_config.initial_memory_pages,
                                vm_config.limit_config.max_memory_pages,
                            )
                            .unwrap(),
                        )
                    } else {
                        None
                    }),
                )
            }
            VMKind::Wasmtime => panic!("Not currently supported"),
        };
        ContractCaller {
            pool: ThreadPool::new(num_threads),
            vm_kind,
            vm_config,
            vm_data_private: private,
            vm_data_shared: shared,
            preloaded: Vec::new(),
        }
    }

    pub fn preload(
        &mut self,
        requests: Vec<ContractCallPrepareRequest>,
    ) -> Vec<ContractCallPrepareResult> {
        let mut result: Vec<ContractCallPrepareResult> = Vec::new();
        for request in requests {
            let index = self.preloaded.len();
            let (tx, rx) = channel();
            self.preloaded.push(CallInner { rx });
            self.pool.execute({
                let tx = tx.clone();
                let vm_config = self.vm_config.clone();
                let vm_data_shared = self.vm_data_shared.clone();
                let vm_kind = self.vm_kind.clone();
                move || preload_in_thread(request, vm_kind, vm_config, vm_data_shared, tx)
            });
            result.push(ContractCallPrepareResult { handle: index });
        }
        result
    }

    pub fn run_preloaded<'a>(
        self: &mut ContractCaller,
        preloaded: &ContractCallPrepareResult,
        method_name: &str,
        ext: &mut dyn External,
        context: VMContext,
        fees_config: &'a RuntimeFeesConfig,
        promise_results: &'a [PromiseResult],
        current_protocol_version: ProtocolVersion,
    ) -> (Option<VMOutcome>, Option<VMError>) {
        match self.preloaded.get(preloaded.handle) {
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
                            ) => {
                                let mut new_memory;
                                run_wasmer0_module(
                                    module.clone(),
                                    if memory.is_some() {
                                        memory.as_mut().unwrap()
                                    } else {
                                        new_memory = WasmerMemory::new(
                                            self.vm_config.limit_config.initial_memory_pages,
                                            self.vm_config.limit_config.max_memory_pages,
                                        )
                                        .unwrap();
                                        &mut new_memory
                                    },
                                    method_name,
                                    ext,
                                    context,
                                    &self.vm_config,
                                    fees_config,
                                    promise_results,
                                    current_protocol_version,
                                )
                            }
                            (
                                Wasmer2(module),
                                VMDataPrivate::Wasmer2(memory),
                                VMDataShared::Wasmer2(store),
                            ) => {
                                let mut new_memory;
                                run_wasmer2_module(
                                    &module,
                                    store,
                                    if memory.is_some() {
                                        memory.as_mut().unwrap()
                                    } else {
                                        new_memory = Wasmer2Memory::new(
                                            store,
                                            self.vm_config.limit_config.initial_memory_pages,
                                            self.vm_config.limit_config.max_memory_pages,
                                        )
                                        .unwrap();
                                        &mut new_memory
                                    },
                                    method_name,
                                    ext,
                                    context,
                                    &self.vm_config,
                                    fees_config,
                                    promise_results,
                                    current_protocol_version,
                                )
                            }
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

fn preload_in_thread(
    request: ContractCallPrepareRequest,
    vm_kind: VMKind,
    vm_config: VMConfig,
    vm_data_shared: VMDataShared,
    tx: Sender<VMCallData>,
) {
    let cache = request.cache.as_deref();
    let result = match (vm_kind, vm_data_shared) {
        (VMKind::Wasmer0, VMDataShared::Wasmer0) => {
            let module = cache::wasmer0_cache::compile_module_cached_wasmer0(
                &request.code,
                &vm_config,
                cache,
            );
            into_vm_result(module).map(VMModule::Wasmer0)
        }
        (VMKind::Wasmer2, VMDataShared::Wasmer2(store)) => {
            let module = cache::wasmer2_cache::compile_module_cached_wasmer2(
                &request.code,
                &vm_config,
                cache,
                &store,
            );
            into_vm_result(module).map(VMModule::Wasmer2)
        }
        _ => panic!("Incorrect logic"),
    };
    tx.send(VMCallData { result }).unwrap();
}

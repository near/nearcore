use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;

use near_primitives::contract::ContractCode;
use near_vm_logic::gas_counter::GasCounter;
use threadpool::ThreadPool;

use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::CompiledContractCache;
use near_vm_errors::VMError;
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{External, ProtocolVersion, VMConfig, VMContext};

use crate::cache::{self, into_vm_result};
use crate::vm_kind::VMKind;
use crate::VMResult;

const SHARE_MEMORY_INSTANCE: bool = false;

enum VMModule {
    #[cfg(feature = "wasmer0_vm")]
    Wasmer0(wasmer_runtime::Module),
    #[cfg(feature = "wasmer2_vm")]
    Wasmer2(crate::wasmer2_runner::VMArtifact),
}

enum VMDataPrivate {
    #[cfg(feature = "wasmer0_vm")]
    Wasmer0(Option<crate::memory::WasmerMemory>),
    #[cfg(feature = "wasmer2_vm")]
    Wasmer2(Option<crate::wasmer2_runner::Wasmer2Memory>),
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
    preloaded: Vec<CallInner>,
}

impl ContractCaller {
    pub fn new(num_threads: usize, vm_kind: VMKind, vm_config: VMConfig) -> ContractCaller {
        let private = match vm_kind {
            #[cfg(not(feature = "wasmer0_vm"))]
            VMKind::Wasmer0 => panic!("Wasmer0 is not enabled"),
            #[cfg(feature = "wasmer0_vm")]
            VMKind::Wasmer0 => VMDataPrivate::Wasmer0(if SHARE_MEMORY_INSTANCE {
                Some(
                    crate::memory::WasmerMemory::new(
                        vm_config.limit_config.initial_memory_pages,
                        vm_config.limit_config.max_memory_pages,
                    )
                    .unwrap(),
                )
            } else {
                None
            }),
            #[cfg(not(feature = "wasmer2_vm"))]
            VMKind::Wasmer2 => panic!("Wasmer2 is not enabled"),
            #[cfg(feature = "wasmer2_vm")]
            VMKind::Wasmer2 => VMDataPrivate::Wasmer2(if SHARE_MEMORY_INSTANCE {
                Some(
                    crate::wasmer2_runner::Wasmer2Memory::new(
                        vm_config.limit_config.initial_memory_pages,
                        vm_config.limit_config.max_memory_pages,
                    )
                    .unwrap(),
                )
            } else {
                None
            }),
            VMKind::Wasmtime => panic!("Not currently supported"),
        };
        ContractCaller {
            pool: ThreadPool::new(num_threads),
            vm_kind,
            vm_config,
            vm_data_private: private,
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
                let vm_kind = self.vm_kind.clone();
                move || preload_in_thread(request, vm_kind, vm_config, tx)
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
        gas_counter: GasCounter,
        promise_results: &'a [PromiseResult],
        current_protocol_version: ProtocolVersion,
    ) -> VMResult {
        match self.preloaded.get(preloaded.handle) {
            Some(call) => {
                let call_data = call.rx.recv().unwrap();
                match call_data.result {
                    Err(err) => VMResult::nop_outcome(err),
                    Ok(module) => match (&module, &mut self.vm_data_private) {
                        #[cfg(feature = "wasmer0_vm")]
                        (VMModule::Wasmer0(module), VMDataPrivate::Wasmer0(memory)) => {
                            let mut new_memory;
                            crate::wasmer_runner::run_wasmer0_module(
                                module.clone(),
                                if memory.is_some() {
                                    memory.as_mut().unwrap()
                                } else {
                                    new_memory = crate::memory::WasmerMemory::new(
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
                                gas_counter,
                                promise_results,
                                current_protocol_version,
                            )
                        }
                        #[cfg(feature = "wasmer2_vm")]
                        (VMModule::Wasmer2(module), VMDataPrivate::Wasmer2(memory)) => {
                            let mut new_memory;
                            crate::wasmer2_runner::Wasmer2VM::new(self.vm_config.clone())
                                .run_module(
                                    &module,
                                    if memory.is_some() {
                                        memory.as_mut().unwrap()
                                    } else {
                                        new_memory = crate::wasmer2_runner::Wasmer2Memory::new(
                                            self.vm_config.limit_config.initial_memory_pages,
                                            self.vm_config.limit_config.max_memory_pages,
                                        )
                                        .unwrap();
                                        &mut new_memory
                                    },
                                    method_name,
                                    ext,
                                    context,
                                    fees_config,
                                    gas_counter,
                                    promise_results,
                                    current_protocol_version,
                                )
                        }
                        _ => panic!("Incorrect logic"),
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

fn preload_in_thread(
    request: ContractCallPrepareRequest,
    vm_kind: VMKind,
    vm_config: VMConfig,
    tx: Sender<VMCallData>,
) {
    let cache = request.cache.as_deref();
    let result = match vm_kind {
        #[cfg(feature = "wasmer0_vm")]
        VMKind::Wasmer0 => {
            let module = cache::wasmer0_cache::compile_module_cached_wasmer0(
                &request.code,
                &vm_config,
                cache,
            );
            into_vm_result(module).map(VMModule::Wasmer0)
        }
        #[cfg(feature = "wasmer2_vm")]
        VMKind::Wasmer2 => {
            let module = cache::wasmer2_cache::compile_module_cached_wasmer2(
                &request.code,
                &vm_config,
                cache,
            );
            into_vm_result(module).map(VMModule::Wasmer2)
        }
        _ => panic!("Incorrect logic"),
    };
    tx.send(VMCallData { result }).unwrap();
}

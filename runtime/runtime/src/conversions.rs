/// Value-to-value conversions defined in this crate as an integrator between multiple other
/// independent crates.
///
/// Implementations here cannot be implementations of the `From` trait for both of the types are
/// expected to be foreign, thus violating the orphan/coherence rules.
pub trait Convert<T>: Sized {
    fn convert(other: T) -> Self;
    fn convert_back(self) -> T;
}

mod method_resolve_error {
    use near_vm_runner::logic::errors::MethodResolveError as From;
    impl super::Convert<From> for near_primitives::errors::MethodResolveError {
        fn convert(outer_err: From) -> Self {
            match outer_err {
                From::MethodEmptyName => Self::MethodEmptyName,
                From::MethodNotFound => Self::MethodNotFound,
                From::MethodInvalidSignature => Self::MethodInvalidSignature,
            }
        }

        fn convert_back(self) -> From {
            match self {
                Self::MethodEmptyName => From::MethodEmptyName,
                Self::MethodNotFound => From::MethodNotFound,
                Self::MethodInvalidSignature => From::MethodInvalidSignature,
            }
        }
    }
}

mod prepare_error {
    use near_vm_runner::logic::errors::PrepareError as From;
    impl super::Convert<From> for near_primitives::errors::PrepareError {
        fn convert(outer_err: From) -> Self {
            match outer_err {
                From::Serialization => Self::Serialization,
                From::Deserialization => Self::Deserialization,
                From::InternalMemoryDeclared => Self::InternalMemoryDeclared,
                From::GasInstrumentation => Self::GasInstrumentation,
                From::StackHeightInstrumentation => Self::StackHeightInstrumentation,
                From::Instantiate => Self::Instantiate,
                From::Memory => Self::Memory,
                From::TooManyFunctions => Self::TooManyFunctions,
                From::TooManyLocals => Self::TooManyLocals,
            }
        }

        fn convert_back(self) -> From {
            match self {
                Self::Serialization => From::Serialization,
                Self::Deserialization => From::Deserialization,
                Self::InternalMemoryDeclared => From::InternalMemoryDeclared,
                Self::GasInstrumentation => From::GasInstrumentation,
                Self::StackHeightInstrumentation => From::StackHeightInstrumentation,
                Self::Instantiate => From::Instantiate,
                Self::Memory => From::Memory,
                Self::TooManyFunctions => From::TooManyFunctions,
                Self::TooManyLocals => From::TooManyLocals,
            }
        }
    }
}

mod compilation_error {
    use near_vm_runner::logic::errors::CompilationError as From;
    impl super::Convert<From> for near_primitives::errors::CompilationError {
        fn convert(outer_err: From) -> Self {
            match outer_err {
                From::CodeDoesNotExist { account_id } => Self::CodeDoesNotExist {
                    account_id: account_id.parse().expect("account_id in error must be valid"),
                },
                From::PrepareError(pe) => Self::PrepareError(super::Convert::convert(pe)),
                From::WasmerCompileError { msg } => Self::WasmerCompileError { msg },
            }
        }

        fn convert_back(self) -> From {
            match self {
                Self::CodeDoesNotExist { account_id } => {
                    From::CodeDoesNotExist { account_id: account_id.into() }
                }
                Self::PrepareError(pe) => From::PrepareError(super::Convert::convert_back(pe)),
                Self::WasmerCompileError { msg } => From::WasmerCompileError { msg },
            }
        }
    }
}

mod wasm_trap_error {
    use near_vm_runner::logic::errors::WasmTrap as From;
    impl super::Convert<From> for near_primitives::errors::WasmTrap {
        fn convert(outer_err: From) -> Self {
            match outer_err {
                From::Unreachable => Self::Unreachable,
                From::IncorrectCallIndirectSignature => Self::IncorrectCallIndirectSignature,
                From::MemoryOutOfBounds => Self::MemoryOutOfBounds,
                From::CallIndirectOOB => Self::CallIndirectOOB,
                From::IllegalArithmetic => Self::IllegalArithmetic,
                From::MisalignedAtomicAccess => Self::MisalignedAtomicAccess,
                From::IndirectCallToNull => Self::IndirectCallToNull,
                From::StackOverflow => Self::StackOverflow,
                From::GenericTrap => Self::GenericTrap,
            }
        }

        fn convert_back(self) -> From {
            match self {
                Self::Unreachable => From::Unreachable,
                Self::IncorrectCallIndirectSignature => From::IncorrectCallIndirectSignature,
                Self::MemoryOutOfBounds => From::MemoryOutOfBounds,
                Self::CallIndirectOOB => From::CallIndirectOOB,
                Self::IllegalArithmetic => From::IllegalArithmetic,
                Self::MisalignedAtomicAccess => From::MisalignedAtomicAccess,
                Self::IndirectCallToNull => From::IndirectCallToNull,
                Self::StackOverflow => From::StackOverflow,
                Self::GenericTrap => From::GenericTrap,
            }
        }
    }
}

mod function_call_error {
    use near_vm_runner::logic::errors::FunctionCallError as From;

    impl super::Convert<From> for near_primitives::errors::FunctionCallError {
        fn convert(outer_err: From) -> Self {
            match outer_err {
                From::CompilationError(e) => Self::CompilationError(super::Convert::convert(e)),
                From::MethodResolveError(e) => Self::MethodResolveError(super::Convert::convert(e)),
                // Note: We deliberately collapse all execution errors for
                // serialization to make the DB representation less dependent
                // on specific types in Rust code.
                From::HostError(ref _e) => Self::ExecutionError(outer_err.to_string()),
                From::LinkError { msg } => Self::ExecutionError(format!("Link Error: {}", msg)),
                From::WasmTrap(e) => Self::WasmTrap(super::Convert::convert(e)),
            }
        }

        fn convert_back(self) -> From {
            match self {
                Self::CompilationError(e) => {
                    From::CompilationError(super::Convert::convert_back(e))
                }
                Self::MethodResolveError(e) => {
                    From::MethodResolveError(super::Convert::convert_back(e))
                }
                Self::LinkError { msg } => From::LinkError { msg },
                Self::WasmTrap(e) => From::WasmTrap(super::Convert::convert_back(e)),
                Self::WasmUnknownError => todo!(),
                Self::HostError(_) => todo!(),
                Self::_EVMError => todo!(),
                Self::ExecutionError(msg) => todo!(),
            }
        }
    }
}

impl Convert<near_store::trie::TrieNodesCount> for near_vm_runner::logic::TrieNodesCount {
    fn convert(other: near_store::trie::TrieNodesCount) -> Self {
        Self { db_reads: other.db_reads, mem_reads: other.mem_reads }
    }

    fn convert_back(self) -> near_store::trie::TrieNodesCount {
        near_store::trie::TrieNodesCount { db_reads: self.db_reads, mem_reads: self.mem_reads }
    }
}

mod compiled_contract {
    use near_vm_runner::CompiledContract as From;

    use super::Convert;
    impl Convert<From> for near_primitives::compiled_contract::CompiledContract {
        fn convert(other: From) -> Self {
            match other {
                From::Code(code) => Self::Code(code),
                From::CompileModuleError(error) => {
                    Self::CompileModuleError(Convert::convert(error))
                }
            }
        }

        fn convert_back(self) -> From {
            match self {
                Self::Code(code) => From::Code(code),
                Self::CompileModuleError(error) => {
                    From::CompileModuleError(Convert::convert_back(error))
                }
            }
        }
    }
}

mod compiled_contact_info {
    use near_vm_runner::CompiledContractInfo as From;

    use super::Convert;
    impl Convert<From> for near_primitives::compiled_contract::CompiledContractInfo {
        fn convert(other: From) -> Self {
            Self { wasm_bytes: other.wasm_bytes, compiled: Convert::convert(other.compiled) }
        }

        fn convert_back(self) -> From {
            From { wasm_bytes: self.wasm_bytes, compiled: Convert::convert_back(self.compiled) }
        }
    }
}

mod profile_data_v3 {
    use near_vm_runner::ProfileDataV3 as From;
    impl super::Convert<From> for near_primitives::profile_data_v3::ProfileDataV3 {
        fn convert(other: From) -> Self {
            Self {
                actions_profile: other.actions_profile,
                wasm_ext_profile: other.wasm_ext_profile,
                wasm_gas: other.wasm_gas,
            }
        }

        fn convert_back(self) -> From {
            From {
                actions_profile: self.actions_profile,
                wasm_ext_profile: self.wasm_ext_profile,
                wasm_gas: self.wasm_gas,
            }
        }
    }
}

mod contract_runtime_cache {
    use near_primitives::{
        compiled_contract::CompiledContractInfo as CompiledContractInfoA,
        compiled_contract::ContractRuntimeCache as ContractRuntimeCacheA, hash::CryptoHash,
    };
    use near_vm_runner::{
        CompiledContractInfo as CompiledContractInfoB,
        ContractRuntimeCache as ContractRuntimeCacheB,
    };
    use std::sync::Arc;

    use crate::conversions::Convert;

    pub struct WrapperA(pub Box<dyn ContractRuntimeCacheA + 'static>);
    pub struct WrapperB(pub Box<dyn ContractRuntimeCacheB + 'static>);

    impl WrapperA {
        pub fn to_contract_runtime_cache_b(self) -> WrapperB {
            struct ImplB(Arc<dyn ContractRuntimeCacheA + 'static>);

            impl ContractRuntimeCacheB for ImplB {
                fn handle(&self) -> Box<dyn ContractRuntimeCacheB + 'static> {
                    Box::new(ImplB(Arc::clone(&self.0)))
                }

                fn put(
                    &self,
                    key: &CryptoHash,
                    value: CompiledContractInfoB,
                ) -> std::io::Result<()> {
                    let value_a = Convert::convert(value);
                    self.0.put(key, value_a)
                }

                fn get(&self, key: &CryptoHash) -> std::io::Result<Option<CompiledContractInfoB>> {
                    self.0.get(key).map(|opt| opt.map(Convert::convert_back))
                }
            }

            WrapperB(Box::new(ImplB(self.0.into())))
        }
    }

    impl WrapperB {
        pub fn to_contract_runtime_cache_a(self) -> WrapperA {
            struct ImplA(Arc<dyn ContractRuntimeCacheB + 'static>);

            impl ContractRuntimeCacheA for ImplA {
                fn handle(&self) -> Box<dyn ContractRuntimeCacheA + 'static> {
                    Box::new(ImplA(Arc::clone(&self.0)))
                }

                fn put(
                    &self,
                    key: &CryptoHash,
                    value: CompiledContractInfoA,
                ) -> std::io::Result<()> {
                    let value_b = Convert::convert_back(value);
                    self.0.put(key, value_b)
                }

                fn get(&self, key: &CryptoHash) -> std::io::Result<Option<CompiledContractInfoA>> {
                    self.0.get(key).map(|opt| opt.map(Convert::convert))
                }
            }

            WrapperA(Box::new(ImplA(self.0.into())))
        }
    }

    impl Convert<Box<dyn ContractRuntimeCacheA>> for Box<dyn ContractRuntimeCacheB> {
        fn convert(other: Box<dyn ContractRuntimeCacheA>) -> Box<dyn ContractRuntimeCacheB> {
            Box::new(WrapperA(other.into()).to_contract_runtime_cache_b().0)
        }

        fn convert_back(self) -> Box<dyn ContractRuntimeCacheA> {
            Box::new(WrapperB(self.into()).to_contract_runtime_cache_a().0)
        }
    }
}

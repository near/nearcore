/// Value-to-value conversions defined in this crate as an integrator between multiple other
/// independent crates.
///
/// Implementations here cannot be implementations of the `From` trait for both of the types are
/// expected to be foreign, thus violating the orphan/coherence rules.
pub(crate) trait Convert<T>: Sized {
    fn convert(other: T) -> Self;
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
                From::WasmTrap(ref _e) => Self::ExecutionError(outer_err.to_string()),
            }
        }
    }
}

impl Convert<near_store::trie::TrieNodesCount> for near_vm_runner::logic::TrieNodesCount {
    fn convert(other: near_store::trie::TrieNodesCount) -> Self {
        Self { db_reads: other.db_reads, mem_reads: other.mem_reads }
    }
}

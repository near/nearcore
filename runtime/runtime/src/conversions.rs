pub(crate) mod method_resolve_error {
    pub(crate) use near_primitives::errors::MethodResolveError as Into;
    pub(crate) use near_vm_runner::logic::errors::MethodResolveError as From;
    pub(crate) fn cvt(outer_err: From) -> Into {
        match outer_err {
            From::MethodEmptyName => Into::MethodEmptyName,
            From::MethodNotFound => Into::MethodNotFound,
            From::MethodInvalidSignature => Into::MethodInvalidSignature,
        }
    }
}

pub(crate) mod prepare_error {
    pub(crate) use near_primitives::errors::PrepareError as Into;
    pub(crate) use near_vm_runner::logic::errors::PrepareError as From;
    pub(crate) fn cvt(outer_err: From) -> Into {
        match outer_err {
            From::Serialization => Into::Serialization,
            From::Deserialization => Into::Deserialization,
            From::InternalMemoryDeclared => Into::InternalMemoryDeclared,
            From::GasInstrumentation => Into::GasInstrumentation,
            From::StackHeightInstrumentation => Into::StackHeightInstrumentation,
            From::Instantiate => Into::Instantiate,
            From::Memory => Into::Memory,
            From::TooManyFunctions => Into::TooManyFunctions,
            From::TooManyLocals => Into::TooManyLocals,
        }
    }
}

pub(crate) mod compilation_error {
    pub(crate) use near_primitives::errors::CompilationError as Into;
    pub(crate) use near_vm_runner::logic::errors::CompilationError as From;
    pub(crate) fn cvt(outer_err: From) -> Into {
        match outer_err {
            From::CodeDoesNotExist { account_id } => Into::CodeDoesNotExist {
                account_id: account_id.parse().expect("account_id in error must be valid"),
            },
            From::PrepareError(pe) => Into::PrepareError(super::prepare_error::cvt(pe)),
            From::WasmerCompileError { msg } => Into::WasmerCompileError { msg },
        }
    }
}

pub(crate) mod function_call_error {
    pub(crate) use near_primitives::errors::FunctionCallError as Into;
    pub(crate) use near_vm_runner::logic::errors::FunctionCallError as From;
    pub(crate) fn cvt(outer_err: From) -> Into {
        match outer_err {
            From::CompilationError(e) => Into::CompilationError(super::compilation_error::cvt(e)),
            From::MethodResolveError(e) => {
                Into::MethodResolveError(super::method_resolve_error::cvt(e))
            }
            // Note: We deliberately collapse all execution errors for
            // serialization to make the DB representation less dependent
            // on specific types in Rust code.
            From::HostError(ref _e) => Into::ExecutionError(outer_err.to_string()),
            From::LinkError { msg } => Into::ExecutionError(format!("Link Error: {}", msg)),
            From::WasmTrap(ref _e) => Into::ExecutionError(outer_err.to_string()),
        }
    }
}

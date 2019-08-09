use std::ffi::c_void;

use crate::errors::VMError;
use crate::memory::WasmerMemory;
use crate::{cache, imports};
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{Config, External, VMContext, VMLogic, VMOutcome};

pub fn run<'a>(
    code_hash: Vec<u8>,
    code: &[u8],
    method_name: &[u8],
    ext: &mut dyn External,
    context: VMContext,
    config: &'a Config,
    promise_results: &'a [PromiseResult],
) -> Result<VMOutcome, VMError> {
    if method_name.is_empty() {
        return Err(VMError::MethodEmptyName);
    }

    let module = cache::compile_cached_module(code_hash, code, config)?;
    let mut memory = WasmerMemory::new(config)?;
    let memory_copy = memory.clone();

    let mut logic = VMLogic::new(ext, context, config, promise_results, &mut memory);

    let raw_ptr = &mut logic as *mut _ as *mut c_void;
    let import_object = imports::build(memory_copy, raw_ptr);

    let method_name = std::str::from_utf8(method_name).map_err(|_| VMError::MethodUTF8Error)?;

    module
        .instantiate(&import_object)
        .map_err(|err| VMError::WasmerInstantiateError(format!("{}", err)))?
        .call(&method_name, &[])?;
    Ok(logic.outcome())
}

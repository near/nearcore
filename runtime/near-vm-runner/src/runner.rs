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
) -> (Option<VMOutcome>, Option<VMError>) {
    if method_name.is_empty() {
        return (None, Some(VMError::MethodEmptyName));
    }

    let module = match cache::compile_cached_module(code_hash, code, config) {
        Ok(x) => x,
        Err(err) => return (None, Some(err.into())),
    };
    let mut memory = match WasmerMemory::new(config) {
        Ok(x) => x,
        Err(err) => return (None, Some(err)),
    };
    let memory_copy = memory.clone();

    let mut logic = VMLogic::new(ext, context, config, promise_results, &mut memory);

    let raw_ptr = &mut logic as *mut _ as *mut c_void;
    let import_object = imports::build(memory_copy, raw_ptr);

    let method_name = match std::str::from_utf8(method_name) {
        Ok(x) => x,
        Err(_) => return (None, Some(VMError::MethodUTF8Error)),
    };

    match module.instantiate(&import_object) {
        Ok(instance) => match instance.call(&method_name, &[]) {
            Ok(_) => (Some(logic.outcome()), None),
            Err(err) => (Some(logic.outcome()), Some(err.into())),
        },
        Err(err) => (None, Some(VMError::WasmerInstantiateError(format!("{}", err)))),
    }
}

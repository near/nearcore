use cached::{cached_key, SizedCache};
use wasmer_runtime;

use crate::errors::IntoVMError;
use crate::prepare;
use near_vm_errors::VMError;
use near_vm_logic::Config;

/// Cache size in number of cached modules to hold.
const CACHE_SIZE: usize = 1024;
// TODO: store a larger on-disk cache

cached_key! {
    MODULES: SizedCache<(Vec<u8>, u64), Result<wasmer_runtime::Module, VMError>>
        = SizedCache::with_size(CACHE_SIZE);
    Key = {
        (code_hash, config.non_crypto_hash())
    };

    fn compile_cached_module(code_hash: Vec<u8>, code: &[u8], config: &Config
        ) -> Result<wasmer_runtime::Module, VMError> = {
        let prepared_code = prepare::prepare_contract(code, config)?;
        wasmer_runtime::compile(&prepared_code).map_err(|err| err.into_vm_error())
    }
}

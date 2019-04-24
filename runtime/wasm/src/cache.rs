use cached::SizedCache;
use wasmer_runtime;

use crate::prepare;
use crate::types::{Config, ContractCode, Error};
use primitives::hash::hash;
use primitives::serialize::Encode;

/// Cache size in number of cached modules to hold.
const CACHE_SIZE: usize = 1024;
// TODO: store a larger on-disk cache

cached_key! {
    MODULES: SizedCache<String, Result<wasmer_runtime::Module, Error>> = SizedCache::with_size(CACHE_SIZE);
    Key = {
        format!("{}:{}",
            code.get_hash(),
            hash(&config.encode().expect("encoding of config shouldn't fail")),
        )
    };

    fn compile_cached_module(code: &ContractCode, config: &Config) -> Result<wasmer_runtime::Module, Error> = {
        let prepared_code = prepare::prepare_contract(code, config).map_err(Error::Prepare)?;

        wasmer_runtime::compile(&prepared_code)
            .map_err(|e| Error::Wasmer(format!("{}", e)))
    }
}

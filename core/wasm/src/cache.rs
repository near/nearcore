use cached::SizedCache;
use wasmer_runtime;

use primitives::serialize::Encode;
use primitives::hash::hash;
use crate::prepare;
use crate::types::{Config, Error};

/// Cache size in number of cached modules to hold.
const CACHE_SIZE: usize = 1024;

cached_key! {
    MODULES: SizedCache<String, Result<wasmer_runtime::Cache, Error>> = SizedCache::with_size(CACHE_SIZE);
    Key = {
        format!("{}:{}",
            hash(code),
            hash(&config.encode().expect("encoding of config shouldn't fail")),
        )
    };

    fn compile_cached_module(code: &[u8], config: &Config) -> Result<wasmer_runtime::Cache, Error> = {
        let prepared_code = prepare::prepare_contract(code, config).map_err(Error::Prepare)?;

        wasmer_runtime::compile_cache(&prepared_code).map_err(|e| Error::Wasmer(e.into()))
    }
}
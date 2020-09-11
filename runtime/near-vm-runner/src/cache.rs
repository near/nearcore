#[cfg(not(feature = "no_cache"))]
use cached::{cached_key, SizedCache};

use crate::errors::IntoVMError;
use crate::prepare;
use bs58;
use near_vm_errors::VMError;
use near_vm_logic::VMConfig;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::{env, fs};
use wasmer_runtime::{compiler_for_backend, Backend};
use wasmer_runtime_core::cache::Artifact;
use wasmer_runtime_core::load_cache_with;

/// Cache size in number of cached modules to hold.
#[cfg(not(feature = "no_cache"))]
const CACHE_SIZE: usize = 128;
// TODO: store a larger on-disk cache

#[cfg(not(feature = "no_cache"))]
cached_key! {
    MODULES: SizedCache<(Vec<u8>, u64), Result<wasmer_runtime::Module, VMError>>
        = SizedCache::with_size(CACHE_SIZE);
    Key = {
        (code_hash, config.non_crypto_hash())
    };

    fn compile_module(code_hash: Vec<u8>, code: &[u8], config: &VMConfig
        ) -> Result<wasmer_runtime::Module, VMError> = {
        let prepared_code = prepare::prepare_contract(code, config)?;
        wasmer_runtime::compile(&prepared_code).map_err(|err| err.into_vm_error())
    }
}

#[cfg(feature = "no_cache")]
pub(crate) fn compile_module(
    _code_hash: Vec<u8>,
    code: &[u8],
    config: &VMConfig,
) -> Result<wasmer_runtime::Module, VMError> {
    let prepared_code = prepare::prepare_contract(code, config)?;
    wasmer_runtime::compile(&prepared_code).map_err(|err| err.into_vm_error())
}

fn get_cached_name(code_hash: &Vec<u8>) -> String {
    // For a prototype we store compiled contracts in file, later on we'll use database.
    let dir = match env::var_os("HOME") {
        Some(val) => format!("{}/.near/cache", val.into_string().unwrap()),
        None => panic!("HOME must be defined!"),
    };
    match fs::create_dir_all(&dir) {
        Ok(_) => {}
        Err(e) => panic!("Cannot create {}: {:?}", dir, e),
    };
    format!("{}/{}", dir, bs58::encode(code_hash).into_string())
}

pub(crate) fn put_module(code: &[u8], config: &VMConfig) -> Result<Vec<u8>, VMError> {
    use sha2::Digest;
    let code_hash = sha2::Sha256::digest(&code).to_vec();
    let path = get_cached_name(&code_hash);
    if Path::new(&path).exists() {
        return Ok(code_hash);
    }
    let compiled: wasmer_runtime::Module = match compile_module(code_hash.clone(), code, config) {
        Ok(module) => module,
        Err(err) => return Err(err),
    };
    let cached = match compiled.cache() {
        Ok(cached) => cached,
        Err(_e) => return Err(VMError::ExternalError(b"cannot make cached artifact".to_vec())),
    };
    let buffer = match cached.serialize() {
        Ok(buffer) => buffer,
        Err(_e) => {
            return Err(VMError::ExternalError(b"cannot serialize cached artifact".to_vec()))
        }
    };

    let mut file = match File::create(path) {
        Ok(file) => file,
        Err(_e) => return Err(VMError::ExternalError(b"Cannot create cache file".to_vec())),
    };
    match file.write_all(&buffer) {
        Ok(_ok) => {}
        Err(_e) => return Err(VMError::ExternalError(b"Cannot write cache file".to_vec())),
    };
    Ok(code_hash.clone())
}

pub(crate) fn get_module(
    code_hash: Vec<u8>,
    _config: &VMConfig,
) -> Result<wasmer_runtime::Module, VMError> {
    let path = get_cached_name(&code_hash);
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(_e) => return Err(VMError::ExternalError(b"cannot read from file".to_vec())),
    };
    let cache = match Artifact::deserialize(bytes.as_slice()) {
        Ok(cache) => cache,
        Err(_e) => return Err(VMError::ExternalError(b"cannot deserialize from file".to_vec())),
    };
    unsafe {
        return match load_cache_with(
            cache,
            compiler_for_backend(Backend::Singlepass).unwrap().as_ref(),
        ) {
            Ok(module) => Ok(module),
            Err(_e) => Err(VMError::ExternalError(b"cannot read from cache".to_vec())),
        };
    }
}

pub(crate) fn compile_module_cached(
    code: &[u8],
    config: &VMConfig,
) -> Result<wasmer_runtime::Module, VMError> {
    let code_hash = put_module(code, config)?;
    get_module(code_hash, config)
}

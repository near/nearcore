use core::fmt;
use std::{
    any::Any,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::hash::CryptoHash;

#[derive(Debug, Clone, PartialEq, BorshDeserialize, BorshSerialize)]
pub enum CompiledContract {
    CompileModuleError(crate::errors::CompilationError),
    Code(Vec<u8>),
}

impl CompiledContract {
    /// Return the length of the compiled contract data.
    ///
    /// If the `CompiledContract` represents a compilation failure, returns `0`.
    pub fn debug_len(&self) -> usize {
        match self {
            CompiledContract::CompileModuleError(_) => 0,
            CompiledContract::Code(c) => c.len(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, BorshDeserialize, BorshSerialize)]
pub struct CompiledContractInfo {
    pub wasm_bytes: u64,
    pub compiled: CompiledContract,
}

/// Cache for compiled modules
pub trait ContractRuntimeCache: Send + Sync {
    fn handle(&self) -> Box<dyn ContractRuntimeCache>;
    fn memory_cache(&self) -> &AnyCache {
        // This method returns a reference, so we need to store an instance somewhere.
        static ZERO_ANY_CACHE: once_cell::sync::Lazy<AnyCache> =
            once_cell::sync::Lazy::new(|| AnyCache::new(0));
        &ZERO_ANY_CACHE
    }
    fn put(&self, key: &CryptoHash, value: CompiledContractInfo) -> std::io::Result<()>;
    fn get(&self, key: &CryptoHash) -> std::io::Result<Option<CompiledContractInfo>>;
    fn has(&self, key: &CryptoHash) -> std::io::Result<bool> {
        self.get(key).map(|entry| entry.is_some())
    }
}

impl fmt::Debug for dyn ContractRuntimeCache {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Compiled contracts cache")
    }
}

impl ContractRuntimeCache for Box<dyn ContractRuntimeCache> {
    fn handle(&self) -> Box<dyn ContractRuntimeCache> {
        <dyn ContractRuntimeCache>::handle(&**self)
    }

    fn put(&self, key: &CryptoHash, value: CompiledContractInfo) -> std::io::Result<()> {
        <dyn ContractRuntimeCache>::put(&**self, key, value)
    }

    fn get(&self, key: &CryptoHash) -> std::io::Result<Option<CompiledContractInfo>> {
        <dyn ContractRuntimeCache>::get(&**self, key)
    }

    fn has(&self, key: &CryptoHash) -> std::io::Result<bool> {
        <dyn ContractRuntimeCache>::has(&**self, key)
    }
}

impl ContractRuntimeCache for Arc<dyn ContractRuntimeCache> {
    fn handle(&self) -> Box<dyn ContractRuntimeCache> {
        <dyn ContractRuntimeCache>::handle(&**self)
    }

    fn put(&self, key: &CryptoHash, value: CompiledContractInfo) -> std::io::Result<()> {
        <dyn ContractRuntimeCache>::put(&**self, key, value)
    }

    fn get(&self, key: &CryptoHash) -> std::io::Result<Option<CompiledContractInfo>> {
        <dyn ContractRuntimeCache>::get(&**self, key)
    }

    fn has(&self, key: &CryptoHash) -> std::io::Result<bool> {
        <dyn ContractRuntimeCache>::has(&**self, key)
    }
}

impl<C: ContractRuntimeCache> ContractRuntimeCache for &C {
    fn handle(&self) -> Box<dyn ContractRuntimeCache> {
        <C as ContractRuntimeCache>::handle(self)
    }

    fn put(&self, key: &CryptoHash, value: CompiledContractInfo) -> std::io::Result<()> {
        <C as ContractRuntimeCache>::put(self, key, value)
    }

    fn get(&self, key: &CryptoHash) -> std::io::Result<Option<CompiledContractInfo>> {
        <C as ContractRuntimeCache>::get(self, key)
    }

    fn has(&self, key: &CryptoHash) -> std::io::Result<bool> {
        <C as ContractRuntimeCache>::has(self, key)
    }
}

#[derive(Default, Clone)]
pub struct NoContractRuntimeCache;

impl ContractRuntimeCache for NoContractRuntimeCache {
    fn handle(&self) -> Box<dyn ContractRuntimeCache> {
        Box::new(self.clone())
    }

    fn put(&self, _: &CryptoHash, _: CompiledContractInfo) -> std::io::Result<()> {
        Ok(())
    }

    fn get(&self, _: &CryptoHash) -> std::io::Result<Option<CompiledContractInfo>> {
        Ok(None)
    }
}

type AnyCacheValue = dyn Any + Send;

/// Cache that can store instances of any type, keyed by a CryptoHash.
///
/// Used primarily for storage of artifacts on a per-VM basis.
pub struct AnyCache {
    cache: Option<Mutex<lru::LruCache<CryptoHash, Box<AnyCacheValue>>>>,
}

impl AnyCache {
    pub fn new(size: usize) -> Self {
        Self {
            cache: if let Some(size) = NonZeroUsize::new(size) {
                Some(Mutex::new(lru::LruCache::new(size.into())))
            } else {
                None
            },
        }
    }

    /// Lookup the key in the cache, generating a new element if absent.
    ///
    /// This function accepts two callbacks as an argument: first is a fallible generation
    /// function which may generate a new value for the cache if there isn't one at the specified
    /// key; and the second to act on the value that has been found (or generated and placed in the
    /// cache.)
    ///
    /// If the `generate` fails to generate a value, the failure will not be cached, but rather
    /// returned to the caller of `try_lookup`. The second callback is not called either, as there
    /// is no value to call it with.
    ///
    /// # Examples
    ///
    /// ```
    /// use near_primitives_core::hash::CryptoHash;
    /// use near_primitives::compiled_contract::{ContractRuntimeCache, NoContractRuntimeCache};
    /// use std::path::Path;
    ///
    /// let cache = NoContractRuntimeCache;
    /// let result = cache.memory_cache().try_lookup(CryptoHash::hash_bytes(b"my_key"), || {
    ///     // The value is not in the cache, (re-)generate a new one by reading from the file
    ///     // system.
    ///     match std::fs::read("/this/path/does/not/exist/") {
    ///         Err(e) => Err(e),
    ///         Ok(bytes) => Ok(Box::new(bytes)) // : Result<Box<dyn Any...>, std::io::Error>
    ///     }
    ///     // If the function above succeeds (returns `Ok`), `Vec<u8>` will end up being stored in
    ///     // the cache.
    /// }, |value| {
    ///     // The value was found in the cache or been just generated successfully. It may not
    ///     // necessarily be a `Vec` however, since there could've been another call to the cache
    ///     // that populated this key with a value of a different type.
    ///     let value: &Vec<u8> = value.downcast_ref()?;
    ///     // If it turned out to be a Vec after all, clone and return it.
    ///     Some(Vec::clone(value))
    /// });
    /// // Since we were reading a path that does not exist, most likely outcome is for the
    /// // generation function to fail...
    /// assert!(result.is_err());
    /// // However if it was to succeed, the 2nd time this is called, the value would potentially
    /// // come from the cache.
    /// ```
    pub fn try_lookup<E, R>(
        &self,
        key: CryptoHash,
        generate: impl FnOnce() -> Result<Box<AnyCacheValue>, E>,
        with: impl FnOnce(&AnyCacheValue) -> R,
    ) -> Result<R, E> {
        let Some(cache) = &self.cache else {
            let v = generate()?;
            // NB: The stars and ampersands here are semantics-affecting. e.g. if the star is
            // missing, we end up making an object out of `Box<dyn ...>` rather than using `dyn
            // Any` within the box which is obviously quite wrong.
            return Ok(with(&*v));
        };
        {
            let mut guard = cache.lock().unwrap();
            if let Some(cached_value) = guard.get(&key) {
                // Same here.
                return Ok(with(&**cached_value));
            }
        }
        let generated = generate()?;
        let result = with(&*generated);
        {
            let mut guard = cache.lock().unwrap();
            guard.put(key, generated);
        }
        Ok(result)
    }
}

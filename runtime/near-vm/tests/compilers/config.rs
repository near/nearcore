use near_vm_compiler::{CompilerConfig, Features};
use near_vm_engine::universal::UniversalEngine;
use near_vm_test_api::Store;

#[derive(Clone, Debug, PartialEq)]
pub enum Compiler {
    Singlepass,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Engine {
    Universal,
}

#[derive(Clone)]
pub struct Config {
    pub compiler: Compiler,
    pub engine: Engine,
    pub features: Option<Features>,
    pub canonicalize_nans: bool,
}

impl Config {
    pub fn new(engine: Engine, compiler: Compiler) -> Self {
        Self { compiler, engine, features: None, canonicalize_nans: false }
    }

    pub fn set_features(&mut self, features: Features) {
        self.features = Some(features);
    }

    pub fn set_nan_canonicalization(&mut self, canonicalize_nans: bool) {
        self.canonicalize_nans = canonicalize_nans;
    }

    pub fn store(&self) -> Store {
        let compiler_config = self.compiler_config(self.canonicalize_nans);
        let engine = self.engine(compiler_config);
        Store::new(engine.into())
    }

    pub fn headless_store(&self) -> Store {
        let engine = self.engine_headless();
        Store::new(engine.into())
    }

    pub fn engine(&self, compiler_config: Box<dyn CompilerConfig>) -> UniversalEngine {
        let mut engine = near_vm_engine::universal::Universal::new(compiler_config)
            .code_memory_pool(
                near_vm_engine::universal::LimitedMemoryPool::new(128, 16 * 4096).unwrap(),
            );
        if let Some(ref features) = self.features {
            engine = engine.features(features.clone())
        }
        engine.engine()
    }

    pub fn engine_headless(&self) -> UniversalEngine {
        near_vm_engine::universal::Universal::headless().engine()
    }

    pub fn compiler_config(&self, canonicalize_nans: bool) -> Box<dyn CompilerConfig> {
        match &self.compiler {
            Compiler::Singlepass => {
                let mut compiler = near_vm_compiler_singlepass::Singlepass::new();
                compiler.canonicalize_nans(canonicalize_nans);
                compiler.enable_verifier();
                Box::new(compiler)
            }
        }
    }
}

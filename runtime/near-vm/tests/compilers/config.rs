use near_vm::{CompilerConfig, Engine as WasmerEngine, Features, Store};

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
        Store::new(&*engine)
    }

    pub fn headless_store(&self) -> Store {
        let engine = self.engine_headless();
        Store::new(&*engine)
    }

    pub fn engine(&self, compiler_config: Box<dyn CompilerConfig>) -> Box<dyn WasmerEngine> {
        match &self.engine {
            Engine::Universal => {
                let mut engine = near_vm_engine_universal::Universal::new(compiler_config);
                if let Some(ref features) = self.features {
                    engine = engine.features(features.clone())
                }
                Box::new(engine.engine())
            }
        }
    }

    pub fn engine_headless(&self) -> Box<dyn WasmerEngine> {
        match &self.engine {
            Engine::Universal => Box::new(near_vm_engine_universal::Universal::headless().engine()),
        }
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

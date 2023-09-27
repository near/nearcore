use crate::universal::UniversalEngine;
use near_vm_compiler::{CompilerConfig, Features, Target};

/// The Universal builder
pub struct Universal {
    #[allow(dead_code)]
    compiler_config: Option<Box<dyn CompilerConfig>>,
    target: Option<Target>,
    features: Option<Features>,
    pool: Option<super::LimitedMemoryPool>,
}

impl Universal {
    /// Create a new Universal
    pub fn new<T>(compiler_config: T) -> Self
    where
        T: Into<Box<dyn CompilerConfig>>,
    {
        Self {
            compiler_config: Some(compiler_config.into()),
            target: None,
            features: None,
            pool: None,
        }
    }

    /// Create a new headless Universal
    pub fn headless() -> Self {
        Self { compiler_config: None, target: None, features: None, pool: None }
    }

    /// Set the target
    pub fn target(mut self, target: Target) -> Self {
        self.target = Some(target);
        self
    }

    /// Set the features
    pub fn features(mut self, features: Features) -> Self {
        self.features = Some(features);
        self
    }

    /// Set the pool of reusable code memory
    pub fn code_memory_pool(mut self, pool: super::LimitedMemoryPool) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Build the `UniversalEngine` for this configuration
    pub fn engine(self) -> UniversalEngine {
        let target = self.target.unwrap_or_default();
        let pool =
            self.pool.unwrap_or_else(|| panic!("Universal::code_memory_pool was not set up!"));
        if let Some(compiler_config) = self.compiler_config {
            let features = self
                .features
                .unwrap_or_else(|| compiler_config.default_features_for_target(&target));
            let compiler = compiler_config.compiler();
            UniversalEngine::new(compiler, target, features, pool)
        } else {
            UniversalEngine::headless(pool)
        }
    }
}

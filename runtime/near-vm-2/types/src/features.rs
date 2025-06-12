/// Controls which experimental features will be enabled.
/// Features usually have a corresponding [WebAssembly proposal].
///
/// [WebAssembly proposal]: https://github.com/WebAssembly/proposals
#[derive(Clone, Debug, Eq, PartialEq, rkyv::Serialize, rkyv::Deserialize, rkyv::Archive)]
pub struct Features {
    /// Threads proposal should be enabled
    pub threads: bool,
    /// Reference Types proposal should be enabled
    pub reference_types: bool,
    /// SIMD proposal should be enabled
    pub simd: bool,
    /// Bulk Memory proposal should be enabled
    pub bulk_memory: bool,
    /// Multi Value proposal should be enabled
    pub multi_value: bool,
    /// Tail call proposal should be enabled
    pub tail_call: bool,
    /// Multi Memory proposal should be enabled
    pub multi_memory: bool,
    /// 64-bit Memory proposal should be enabled
    pub memory64: bool,
    /// Wasm exceptions proposal should be enabled
    pub exceptions: bool,
    /// Mutable global proposal should be enabled
    pub mutable_global: bool,
    /// Non-trapping float-to-int proposal should be enabled
    pub saturating_float_to_int: bool,
    /// Sign-extension operators should be enabled
    pub sign_extension: bool,
}

impl Features {
    /// Create a new feature
    pub fn new() -> Self {
        Self {
            threads: false,
            // Reference types should be on by default
            reference_types: true,
            // SIMD should be on by default
            simd: true,
            // Bulk Memory should be on by default
            bulk_memory: true,
            // Multivalue should be on by default
            multi_value: true,
            tail_call: false,
            multi_memory: false,
            memory64: false,
            exceptions: false,
            // these were once defaulting to true in wasmparser, we now set them to true here
            mutable_global: true,
            saturating_float_to_int: true,
            sign_extension: true,
        }
    }

    /// Configures whether the WebAssembly threads proposal will be enabled.
    ///
    /// The [WebAssembly threads proposal][threads] is not currently fully
    /// standardized and is undergoing development. Support for this feature can
    /// be enabled through this method for appropriate WebAssembly modules.
    ///
    /// This feature gates items such as shared memories and atomic
    /// instructions.
    ///
    /// This is `false` by default.
    ///
    /// [threads]: https://github.com/webassembly/threads
    pub fn threads(&mut self, enable: bool) -> &mut Self {
        self.threads = enable;
        self
    }

    /// Configures whether the WebAssembly reference types proposal will be
    /// enabled.
    ///
    /// The [WebAssembly reference types proposal][proposal] is now
    /// fully standardized and enabled by default.
    ///
    /// This feature gates items such as the `externref` type and multiple tables
    /// being in a module. Note that enabling the reference types feature will
    /// also enable the bulk memory feature.
    ///
    /// This is `true` by default.
    ///
    /// [proposal]: https://github.com/webassembly/reference-types
    pub fn reference_types(&mut self, enable: bool) -> &mut Self {
        self.reference_types = enable;
        // The reference types proposal depends on the bulk memory proposal
        if enable {
            self.bulk_memory(true);
        }
        self
    }

    /// Configures whether the WebAssembly SIMD proposal will be
    /// enabled.
    ///
    /// The [WebAssembly SIMD proposal][proposal] is not currently
    /// fully standardized and is undergoing development. Support for this
    /// feature can be enabled through this method for appropriate WebAssembly
    /// modules.
    ///
    /// This feature gates items such as the `v128` type and all of its
    /// operators being in a module.
    ///
    /// This is `false` by default.
    ///
    /// [proposal]: https://github.com/webassembly/simd
    pub fn simd(&mut self, enable: bool) -> &mut Self {
        self.simd = enable;
        self
    }

    /// Configures whether the WebAssembly bulk memory operations proposal will
    /// be enabled.
    ///
    /// The [WebAssembly bulk memory operations proposal][proposal] is now
    /// fully standardized and enabled by default.
    ///
    /// This feature gates items such as the `memory.copy` instruction, passive
    /// data/table segments, etc, being in a module.
    ///
    /// This is `true` by default.
    ///
    /// [proposal]: https://github.com/webassembly/bulk-memory-operations
    pub fn bulk_memory(&mut self, enable: bool) -> &mut Self {
        self.bulk_memory = enable;
        // In case is false, we disable both threads and reference types
        // since they both depend on bulk memory
        if !enable {
            self.reference_types(false);
        }
        self
    }

    /// Configures whether the WebAssembly multi-value proposal will
    /// be enabled.
    ///
    /// The [WebAssembly multi-value proposal][proposal] is now fully
    /// standardized and enabled by default, except with the singlepass
    /// compiler which does not support it.
    ///
    /// This feature gates functions and blocks returning multiple values in a
    /// module, for example.
    ///
    /// This is `true` by default.
    ///
    /// [proposal]: https://github.com/webassembly/multi-value
    pub fn multi_value(&mut self, enable: bool) -> &mut Self {
        self.multi_value = enable;
        self
    }

    /// Configures whether the WebAssembly tail-call proposal will
    /// be enabled.
    ///
    /// The [WebAssembly tail-call proposal][proposal] is not
    /// currently fully standardized and is undergoing development.
    /// Support for this feature can be enabled through this method for
    /// appropriate WebAssembly modules.
    ///
    /// This feature gates tail-call functions in WebAssembly.
    ///
    /// This is `false` by default.
    ///
    /// [proposal]: https://github.com/webassembly/tail-call
    pub fn tail_call(&mut self, enable: bool) -> &mut Self {
        self.tail_call = enable;
        self
    }

    /// Configures whether the WebAssembly multi-memory proposal will
    /// be enabled.
    ///
    /// The [WebAssembly multi-memory proposal][proposal] is not
    /// currently fully standardized and is undergoing development.
    /// Support for this feature can be enabled through this method for
    /// appropriate WebAssembly modules.
    ///
    /// This feature adds the ability to use multiple memories within a
    /// single Wasm module.
    ///
    /// This is `false` by default.
    ///
    /// [proposal]: https://github.com/WebAssembly/multi-memory
    pub fn multi_memory(&mut self, enable: bool) -> &mut Self {
        self.multi_memory = enable;
        self
    }

    /// Configures whether the WebAssembly 64-bit memory proposal will
    /// be enabled.
    ///
    /// The [WebAssembly 64-bit memory proposal][proposal] is not
    /// currently fully standardized and is undergoing development.
    /// Support for this feature can be enabled through this method for
    /// appropriate WebAssembly modules.
    ///
    /// This feature gates support for linear memory of sizes larger than
    /// 2^32 bits.
    ///
    /// This is `false` by default.
    ///
    /// [proposal]: https://github.com/WebAssembly/memory64
    pub fn memory64(&mut self, enable: bool) -> &mut Self {
        self.memory64 = enable;
        self
    }
}

impl Default for Features {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test_features {
    use super::*;
    #[test]
    fn default_features() {
        let default = Features::default();
        assert_eq!(
            default,
            Features {
                threads: false,
                reference_types: true,
                simd: true,
                bulk_memory: true,
                multi_value: true,
                tail_call: false,
                multi_memory: false,
                memory64: false,
                exceptions: false,
                mutable_global: true,
                saturating_float_to_int: true,
                sign_extension: true,
            }
        );
    }

    #[test]
    fn enable_threads() {
        let mut features = Features::new();
        features.bulk_memory(false).threads(true);

        assert!(features.threads);
    }

    #[test]
    fn enable_reference_types() {
        let mut features = Features::new();
        features.bulk_memory(false).reference_types(true);
        assert!(features.reference_types);
        assert!(features.bulk_memory);
    }

    #[test]
    fn enable_simd() {
        let mut features = Features::new();
        features.simd(true);
        assert!(features.simd);
    }

    #[test]
    fn enable_multi_value() {
        let mut features = Features::new();
        features.multi_value(true);
        assert!(features.multi_value);
    }

    #[test]
    fn enable_bulk_memory() {
        let mut features = Features::new();
        features.bulk_memory(true);
        assert!(features.bulk_memory);
    }

    #[test]
    fn disable_bulk_memory() {
        let mut features = Features::new();
        features.threads(true).reference_types(true).bulk_memory(false);
        assert!(!features.bulk_memory);
        assert!(!features.reference_types);
    }

    #[test]
    fn enable_tail_call() {
        let mut features = Features::new();
        features.tail_call(true);
        assert!(features.tail_call);
    }

    #[test]
    fn enable_multi_memory() {
        let mut features = Features::new();
        features.multi_memory(true);
        assert!(features.multi_memory);
    }

    #[test]
    fn enable_memory64() {
        let mut features = Features::new();
        features.memory64(true);
        assert!(features.memory64);
    }
}

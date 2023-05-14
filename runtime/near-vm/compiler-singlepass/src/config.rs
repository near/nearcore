// Allow unused imports while developing
#![allow(unused_imports, dead_code)]

use crate::compiler::SinglepassCompiler;
use crate::emitter_x64::Location;
use near_vm_compiler::{Compiler, CompilerConfig, CpuFeature, Target};
use near_vm_types::{Features, FunctionType, Type};
use smallvec::SmallVec;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) enum IntrinsicKind {
    Gas,
}

#[derive(Debug, Clone)]
pub(crate) struct Intrinsic {
    pub(crate) kind: IntrinsicKind,
    pub(crate) name: String,
    pub(crate) signature: FunctionType,
}

#[derive(Debug, Clone)]
pub struct Singlepass {
    pub(crate) enable_nan_canonicalization: bool,
    pub(crate) enable_stack_check: bool,
    /// Compiler intrinsics.
    pub(crate) intrinsics: Vec<Intrinsic>,
}

impl Singlepass {
    /// Creates a new configuration object with the default configuration
    /// specified.
    pub fn new() -> Self {
        Self {
            enable_nan_canonicalization: true,
            enable_stack_check: false,
            intrinsics: vec![Intrinsic {
                kind: IntrinsicKind::Gas,
                name: "gas".to_string(),
                signature: ([Type::I32], []).into(),
            }],
        }
    }

    /// Enable stack check.
    ///
    /// When enabled, an explicit stack depth check will be performed on entry
    /// to each function to prevent stack overflow.
    ///
    /// Note that this doesn't guarantee deterministic execution across
    /// different platforms.
    pub fn enable_stack_check(&mut self, enable: bool) -> &mut Self {
        self.enable_stack_check = enable;
        self
    }

    fn enable_nan_canonicalization(&mut self) {
        self.enable_nan_canonicalization = true;
    }

    pub fn canonicalize_nans(&mut self, enable: bool) -> &mut Self {
        self.enable_nan_canonicalization = enable;
        self
    }
}

impl CompilerConfig for Singlepass {
    fn enable_pic(&mut self) {
        // Do nothing, since singlepass already emits
        // PIC code.
    }

    /// Transform it into the compiler
    fn compiler(self: Box<Self>) -> Box<dyn Compiler> {
        Box::new(SinglepassCompiler::new(*self))
    }

    /// Gets the default features for this compiler in the given target
    fn default_features_for_target(&self, _target: &Target) -> Features {
        let mut features = Features::default();
        features.multi_value(false);
        features
    }
}

impl Default for Singlepass {
    fn default() -> Singlepass {
        Self::new()
    }
}

impl Intrinsic {
    pub(crate) fn is_params_ok(&self, params: &SmallVec<[Location; 8]>) -> bool {
        match self.kind {
            IntrinsicKind::Gas => match params[0] {
                Location::Imm32(value) => value < i32::MAX as u32,
                _ => false,
            },
        }
    }
}

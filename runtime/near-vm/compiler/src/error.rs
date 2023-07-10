use crate::lib::std::string::String;

// Compilation Errors

/// The WebAssembly.CompileError object indicates an error during
/// WebAssembly decoding or validation.
///
/// This is based on the [Wasm Compile Error][compile-error] API.
///
/// [compiler-error]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/WebAssembly/CompileError
#[derive(Debug, thiserror::Error)]
pub enum CompileError {
    /// A Wasm translation error occured.
    #[error("WebAssembly translation error: {0}")]
    Wasm(WasmError),

    /// A compilation error occured.
    #[error("Compilation error: {0}")]
    Codegen(String),

    /// The module did not pass validation.
    #[error("Validation error: {0}")]
    Validate(String),

    /// Finite-wasm failed to handle the module.
    #[error("Finite-wasm analysis error: {0}")]
    Analyze(finite_wasm::Error),

    /// The compiler doesn't support a Wasm feature
    #[error("Feature {0} is not yet supported")]
    UnsupportedFeature(String),

    /// The compiler cannot compile for the given target.
    /// This can refer to the OS, the chipset or any other aspect of the target system.
    #[error("The target {0} is not yet supported (see https://docs.wasmer.io/ecosystem/wasmer/wasmer-features)")]
    UnsupportedTarget(String),

    /// Insufficient resources available for execution.
    #[error("Insufficient resources: {0}")]
    Resource(String),

    /// Cannot downcast the engine to a specific type.
    #[error("data offset is out of bounds")]
    InvalidOffset,
}

impl From<WasmError> for CompileError {
    fn from(original: WasmError) -> Self {
        Self::Wasm(original)
    }
}

/// A error in the middleware.
#[derive(Debug, thiserror::Error)]
#[error("Error in middleware {name}: {message}")]
pub struct MiddlewareError {
    /// The name of the middleware where the error was created
    pub name: String,
    /// The error message
    pub message: String,
}

impl MiddlewareError {
    /// Create a new `MiddlewareError`
    pub fn new<A: Into<String>, B: Into<String>>(name: A, message: B) -> Self {
        Self { name: name.into(), message: message.into() }
    }
}

/// A WebAssembly translation error.
///
/// When a WebAssembly function can't be translated, one of these error codes will be returned
/// to describe the failure.
#[derive(Debug, thiserror::Error)]
pub enum WasmError {
    /// The input WebAssembly code is invalid.
    ///
    /// This error code is used by a WebAssembly translator when it encounters invalid WebAssembly
    /// code. This should never happen for validated WebAssembly code.
    #[error("Invalid input WebAssembly code at offset {offset}: {message}")]
    InvalidWebAssembly {
        /// A string describing the validation error.
        message: String,
        /// The bytecode offset where the error occurred.
        offset: usize,
    },

    /// A feature used by the WebAssembly code is not supported by the embedding environment.
    ///
    /// Embedding environments may have their own limitations and feature restrictions.
    #[error("Unsupported feature: {0}")]
    Unsupported(String),

    /// An implementation limit was exceeded.
    #[error("Implementation limit exceeded")]
    ImplLimitExceeded,

    /// An error from the middleware error.
    #[error("{0}")]
    Middleware(MiddlewareError),

    /// A generic error.
    #[error("{0}")]
    Generic(String),
}

impl From<MiddlewareError> for WasmError {
    fn from(original: MiddlewareError) -> Self {
        Self::Middleware(original)
    }
}

/// The error that can happen while parsing a `str`
/// to retrieve a [`CpuFeature`](crate::target::CpuFeature).
#[derive(Debug, thiserror::Error)]
pub enum ParseCpuFeatureError {
    /// The provided string feature doesn't exist
    #[error("CpuFeature {0} not recognized")]
    Missing(String),
}

/// A convenient alias for a `Result` that uses `WasmError` as the error type.
pub type WasmResult<T> = Result<T, WasmError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn middleware_error_can_be_created() {
        let msg = String::from("Something went wrong");
        let error = MiddlewareError::new("manipulator3000", msg);
        assert_eq!(error.name, "manipulator3000");
        assert_eq!(error.message, "Something went wrong");
    }

    #[test]
    fn middleware_error_be_converted_to_wasm_error() {
        let error = WasmError::from(MiddlewareError::new("manipulator3000", "foo"));
        match error {
            WasmError::Middleware(MiddlewareError { name, message }) => {
                assert_eq!(name, "manipulator3000");
                assert_eq!(message, "foo");
            }
            err => panic!("Unexpected error: {:?}", err),
        }
    }
}

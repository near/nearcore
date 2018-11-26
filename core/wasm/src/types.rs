use wasmi::{Error as WasmiError, Trap};

#[derive(Debug, PartialEq, Eq)]
/// Error that can occur while preparing or executing wasm smart-contract.
pub enum PrepareError {
    /// Error happened while serializing the module.
    Serialization,

    /// Error happened while deserializing the module.
    Deserialization,

    /// Internal memory declaration has been found in the module.
    InternalMemoryDeclared,

    /// Gas instrumentation failed.
    ///
    /// This most likely indicates the module isn't valid.
    GasInstrumentation,

    /// Stack instrumentation failed.
    ///
    /// This  most likely indicates the module isn't valid.
    StackHeightInstrumentation,

    /// Error happened during invocation of the contract's entrypoint.
    ///
    /// Most likely because of trap.
    Invoke,

    /// Error happened during instantiation.
    ///
    /// This might indicate that `start` function trapped, or module isn't
    /// instantiable and/or unlinkable.
    Instantiate,

    /// Memory creation error.
    ///
    /// This might happen when the memory import has invalid descriptor or
    /// requested too much resources.
    Memory,
}

/// Wrapped error
#[derive(Debug)]
pub enum Error {
    /// Method name can't be decoded to UTF8.
    BadUtf8,

    /// Method name is empty.
    EmptyMethodName,

    /// Method is private, because it starts with '_'.
    PrivateMethod,

    Prepare(PrepareError),

    Interpreter(WasmiError),

    Trap(Trap),
}

impl From<WasmiError> for Error {
    fn from(e: WasmiError) -> Self {
        Error::Interpreter(e)
    }
}

impl From<Trap> for Error {
    fn from(e: Trap) -> Self {
        Error::Trap(e)
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionParams {
    pub config: Config,
}

// TODO: Extract it to the root of the crate
#[derive(Clone, Debug)]
pub struct Config {
    /// Gas cost of a growing memory by single page.
    pub grow_mem_cost: u32,

    /// Gas cost of a regular operation.
    pub regular_op_cost: u32,

    /// Gas cost per one byte returned.
    pub return_data_per_byte_cost: u32,

    /// How tall the stack is allowed to grow?
    ///
    /// See https://wiki.parity.io/WebAssembly-StackHeight to find out
    /// how the stack frame cost is calculated.
    pub max_stack_height: u32,

    /// What is the maximal memory pages amount is allowed to have for
    /// a contract.
    pub max_memory_pages: u32,

    /// Gas limit of the one contract call
    pub gas_limit: u64,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            grow_mem_cost: 1,
            regular_op_cost: 1,
            return_data_per_byte_cost: 1,
            max_stack_height: 64 * 1024,
            max_memory_pages: 16,
            gas_limit: 128 * 1024,
        }
    }
}

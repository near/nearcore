use ethereum_types::{Address, U256};

#[derive(Debug)]
pub enum EvmError {
    /// Unknown error, catch all for unexpected things.
    UnknownError,
    /// Fatal failure due conflicting addresses on contract deployment.  
    DuplicateContract(Address),
    /// Contract deployment failure.
    DeployFail(Vec<u8>),
    /// Contract execution failed, revert the state.
    Revert(Vec<u8>),
}

impl std::fmt::Display for EvmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str(&format!("{:?}", self))
    }
}

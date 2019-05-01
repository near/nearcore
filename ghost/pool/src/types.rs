use failure::Fail;

use near_chain::ValidTransaction;

/// Possible errors whe interacting with transaction pool.
#[derive(Debug, Fail)]
pub enum Error {
    /// An invalid pool entry caused by underlying tx validation error
    #[fail(display = "Invalid Tx {}", _0)]
    InvalidTx(String),
    /// Other kinds of error (not yet pulled out into meaningful errors).
    #[fail(display = "General pool error {}", _0)]
    Other(String),
}

pub type ValidateTxCallback = fn(&[u8]) -> Result<ValidTransaction, Error>;

// Interface that the transaction pool requires from a blockchain implementation.
//pub trait ChainAdapter {
//    /// Verify transaction validity.
//    // TODO: possible return values: deserialized transaction, it's expected "weight", nonce and account id for grouping.
//    fn validate_tx(&self, tx: &[u8]) -> Result<ValidTransaction, Error>;
//}

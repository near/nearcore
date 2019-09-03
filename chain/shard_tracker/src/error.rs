use near_epoch_manager::EpochError;
use std::fmt;

pub enum Error {
    EpochManagerError(EpochError),
    Other(String),
}

impl std::error::Error for Error {}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::EpochManagerError(e) => write!(f, "EpochManagerError {}", e),
            Error::Other(e) => write!(f, "Other {}", e),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::EpochManagerError(e) => write!(f, "EpochManagerError({})", e),
            Error::Other(e) => write!(f, "Other({})", e),
        }
    }
}

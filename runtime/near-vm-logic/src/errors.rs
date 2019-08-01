use crate::dependencies::ExternalError;

#[derive(Debug, PartialEq)]
pub enum HostError {
    MemoryAccessViolation,
    External(ExternalError),
    InvalidRegisterId,
    InvalidIteratorIndex,
    IteratorWasInvalidated,
    EmptyMethodName,
    GuestPanic,
    BadUTF8,
    BadUTF16,
    IntegerOverflow,
    UsageLimit,
    BalanceExceeded,
    InvalidPromiseResultIndex,
}

impl From<ExternalError> for HostError {
    fn from(err: ExternalError) -> Self {
        HostError::External(err)
    }
}

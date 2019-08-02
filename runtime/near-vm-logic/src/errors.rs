use crate::dependencies::ExternalError;

#[derive(Debug, PartialEq)]
pub enum HostError {
    BadUTF16,
    BadUTF8,
    BalanceExceeded,
    EmptyMethodName,
    External(ExternalError),
    GuestPanic,
    IntegerOverflow,
    InvalidIteratorIndex,
    InvalidPromiseIndex,
    CannotReturnJointPromise,
    InvalidPromiseResultIndex,
    InvalidRegisterId,
    IteratorWasInvalidated,
    MemoryAccessViolation,
    UsageLimit,
}

impl From<ExternalError> for HostError {
    fn from(err: ExternalError) -> Self {
        HostError::External(err)
    }
}

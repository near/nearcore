use crate::dependencies::ExternalError;

pub enum HostError {
    MemoryAccessViolation,
    External(ExternalError),
    InvalidRegisterId,
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

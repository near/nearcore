use crate::dependencies::ExternalError;

#[derive(Debug, Clone, PartialEq)]
pub enum HostError {
    BadUTF16,
    BadUTF8,
    GasExceeded,
    GasLimitExceeded,
    BalanceExceeded,
    EmptyMethodName,
    External(ExternalError),
    GuestPanic,
    IntegerOverflow,
    InvalidIteratorIndex,
    InvalidPromiseIndex,
    CannotAppendActionToJointPromise,
    CannotReturnJointPromise,
    InvalidPromiseResultIndex,
    InvalidRegisterId,
    IteratorWasInvalidated,
    MemoryAccessViolation,
    ProhibitedInView(String),
}

impl From<ExternalError> for HostError {
    fn from(err: ExternalError) -> Self {
        HostError::External(err)
    }
}

impl std::fmt::Display for HostError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        use HostError::*;
        match self {
            BadUTF8 => write!(f, "String encoding is bad UTF-8 sequence."),
            BadUTF16 => write!(f, "String encoding is bad UTF-16 sequence."),
            GasExceeded => write!(f, "Exceeded the prepaid gas."),
            GasLimitExceeded => write!(f, "Exceeded the maximum amount of gas allowed to burn per contract."),
            BalanceExceeded => write!(f, "Exceeded the account balance."),
            EmptyMethodName => write!(f, "Tried to call an empty method name."),
            External(ext) => {
                write!(f, "External error: ")?;
                ext.fmt(f)
            },
            GuestPanic => write!(f, "Smart contract has explicitly invoked `panic`."),
            IntegerOverflow => write!(f, "Integer overflow."),
            InvalidIteratorIndex => write!(f, "Invalid iterator index"),
            InvalidPromiseIndex => write!(f, "Invalid promise index"),
            CannotAppendActionToJointPromise => write!(f, "Actions can only be appended to non-joint promise."),
            CannotReturnJointPromise => write!(f, "Returning joint promise is currently prohibited."),
            InvalidPromiseResultIndex => write!(f, "Accessed invalid promise result index."),
            InvalidRegisterId => write!(f, "Accessed invalid register id"),
            IteratorWasInvalidated => write!(f, "Iterator was invalidated after its creation by performing a mutable operation on trie"),
            MemoryAccessViolation => write!(f, "Accessed memory outside the bounds."),
            ProhibitedInView(method_name) => write!(f, "{} is not allowed in view calls", method_name)
        }
    }
}

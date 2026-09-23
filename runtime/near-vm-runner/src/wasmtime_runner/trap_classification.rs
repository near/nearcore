use wasmtime::Trap;

/// Mirror of [`wasmtime::Trap`] that we own, so we can match exhaustively.
///
/// When a Wasmtime upgrade adds a new trap variant, `From<Trap>` maps it to
/// `Unknown` and the `test_all_wasmtime_traps_classified` test fails, forcing
/// explicit classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrapClassification {
    StackOverflow,
    MemoryOutOfBounds,
    HeapMisaligned,
    TableOutOfBounds,
    IndirectCallToNull,
    BadSignature,
    IntegerOverflow,
    IntegerDivisionByZero,
    BadConversionToInteger,
    UnreachableCodeReached,
    Interrupt,
    /// Trap codes that exist in Wasmtime but should never be produced by a core
    /// Wasm module running under NEAR's engine configuration (GC, component
    /// model, threads, fuel, etc.).
    Unreachable {
        trap: Trap,
    },
    Unknown {
        trap: Trap,
    },
}

impl From<Trap> for TrapClassification {
    fn from(trap: Trap) -> Self {
        match trap {
            Trap::StackOverflow => Self::StackOverflow,
            Trap::MemoryOutOfBounds => Self::MemoryOutOfBounds,
            Trap::HeapMisaligned => Self::HeapMisaligned,
            Trap::TableOutOfBounds => Self::TableOutOfBounds,
            Trap::IndirectCallToNull => Self::IndirectCallToNull,
            Trap::BadSignature => Self::BadSignature,
            Trap::IntegerOverflow => Self::IntegerOverflow,
            Trap::IntegerDivisionByZero => Self::IntegerDivisionByZero,
            Trap::BadConversionToInteger => Self::BadConversionToInteger,
            Trap::UnreachableCodeReached => Self::UnreachableCodeReached,
            Trap::Interrupt => Self::Interrupt,

            // fuel (not used; NEAR meters gas via instrumentation)
            t @ (Trap::OutOfFuel
            // threads proposal
            | Trap::AtomicWaitNonSharedMemory
            // GC / function-references proposals
            | Trap::NullReference
            | Trap::ArrayOutOfBounds
            | Trap::AllocationTooLarge
            | Trap::CastFailure
            // component model
            | Trap::CannotEnterComponent
            | Trap::CannotLeaveComponent
            | Trap::CannotBlockSyncTask
            | Trap::InvalidChar
            | Trap::StringOutOfBounds
            | Trap::ListOutOfBounds
            | Trap::InvalidDiscriminant
            | Trap::UnalignedPointer
            | Trap::DebugAssertStringEncodingFinished
            | Trap::DebugAssertEqualCodeUnits
            | Trap::DebugAssertPointerAligned
            | Trap::DebugAssertUpperBitsUnset
            // component-model async
            | Trap::NoAsyncResult
            | Trap::AsyncDeadlock
            | Trap::TaskCancelNotCancelled
            | Trap::TaskCancelOrReturnTwice
            | Trap::SubtaskCancelAfterTerminal
            | Trap::TaskReturnInvalid
            | Trap::WaitableSetDropHasWaiters
            | Trap::SubtaskDropNotResolved
            | Trap::BackpressureOverflow
            | Trap::UnsupportedCallbackCode
            | Trap::ConcurrentFutureStreamOp
            | Trap::ReferenceCountOverflow
            | Trap::StreamOpTooBig
            // stack switching / continuations
            | Trap::UnhandledTag
            | Trap::ContinuationAlreadyConsumed
            | Trap::CannotResumeThread
            | Trap::ThreadNewIndirectInvalidType
            | Trap::ThreadNewIndirectUninitialized
            // pulley interpreter
            | Trap::DisabledOpcode) => Self::Unreachable { trap: t },

            t => Self::Unknown { trap: t },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_wasmtime_traps_classified() {
        for byte in 0..=u8::MAX {
            let Some(trap) = Trap::from_u8(byte) else {
                continue;
            };
            let classification = TrapClassification::from(trap);
            assert!(
                !matches!(classification, TrapClassification::Unknown { .. }),
                "unclassified wasmtime trap variant: {trap:?} (discriminant {byte})",
            );
        }
    }
}

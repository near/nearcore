//! Priority hint for contract compilation.
//!
//! When the out-of-process compiler daemon is enabled, a limited pool of worker
//! subprocesses serves all compilations. Without a hint, a burst of background
//! work (state sync, cache pre-warming, witness validation) can occupy every
//! worker and delay latency-critical compilations (chunk application, view
//! calls). The priority is carried on the per-call VM handle (set via
//! [`crate::VM::set_compile_priority`]); the daemon's worker checkout then
//! serves the most urgent waiter first.
//!
//! The default is [`CompilePriority::Critical`]: callers on the block-production
//! hot path need no annotation (a forgotten one is never starved), and only the
//! known background/interactive entry points opt down.

/// Relative urgency of a contract compilation. Lower discriminant = more urgent.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum CompilePriority {
    /// On the critical path of chunk application or contract deployment.
    /// The default: anything not explicitly lowered is treated as critical.
    #[default]
    Critical = 0,
    /// User-facing but off the block-production path (e.g. RPC view calls).
    Interactive = 1,
    /// Best-effort background work: cache pre-warming, state sync, witness
    /// validation precompiles.
    Background = 2,
}

impl CompilePriority {
    /// Number of distinct priority classes; used to size per-class structures.
    // Only read by the compiler-daemon pool, which is gated on `wasmtime_vm`.
    #[cfg_attr(not(feature = "wasmtime_vm"), allow(dead_code))]
    pub(crate) const COUNT: usize = 3;

    #[cfg_attr(not(feature = "wasmtime_vm"), allow(dead_code))]
    pub(crate) fn index(self) -> usize {
        self as usize
    }
}

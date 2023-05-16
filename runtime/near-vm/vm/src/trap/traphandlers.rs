// This file contains code from external sources.
// Attributions: https://github.com/wasmerio/wasmer/blob/master/ATTRIBUTIONS.md

//! WebAssembly trap handling, which is built on top of the lower-level
//! signalhandling mechanisms.

use super::trapcode::TrapCode;
use crate::vmcontext::{VMFunctionBody, VMFunctionEnvironment, VMTrampoline};
use backtrace::Backtrace;
use std::any::Any;
use std::cell::{Cell, UnsafeCell};
use std::error::Error;
use std::mem::{self, MaybeUninit};
use std::ptr;
pub use tls::TlsRestore;

extern "C" {
    fn near_vm_register_setjmp(
        jmp_buf: *mut *const u8,
        callback: extern "C" fn(*mut u8),
        payload: *mut u8,
    ) -> i32;
    fn near_vm_unwind(jmp_buf: *const u8) -> !;
}

/// Raises a user-defined trap immediately.
///
/// This function performs as-if a wasm trap was just executed, only the trap
/// has a dynamic payload associated with it which is user-provided. This trap
/// payload is then returned from `catch_traps` below.
///
/// # Safety
///
/// Only safe to call when wasm code is on the stack, aka `catch_traps` must
/// have been previous called and not yet returned.
/// Additionally no Rust destructors may be on the stack.
/// They will be skipped and not executed.
pub unsafe fn raise_user_trap(data: Box<dyn Error + Send + Sync>) -> ! {
    tls::with(|info| info.unwrap().unwind_with(UnwindReason::UserTrap(data)))
}

/// Raises a trap from inside library code immediately.
///
/// This function performs as-if a wasm trap was just executed. This trap
/// payload is then returned from `catch_traps` below.
///
/// # Safety
///
/// Only safe to call when wasm code is on the stack, aka `catch_traps` must
/// have been previous called and not yet returned.
/// Additionally no Rust destructors may be on the stack.
/// They will be skipped and not executed.
pub unsafe fn raise_lib_trap(trap: Trap) -> ! {
    tls::with(|info| info.unwrap().unwind_with(UnwindReason::LibTrap(trap)))
}

/// Carries a Rust panic across wasm code and resumes the panic on the other
/// side.
///
/// # Safety
///
/// Only safe to call when wasm code is on the stack, aka `catch_traps` must
/// have been previously called and not returned. Additionally no Rust destructors may be on the
/// stack. They will be skipped and not executed.
pub unsafe fn resume_panic(payload: Box<dyn Any + Send>) -> ! {
    tls::with(|info| info.unwrap().unwind_with(UnwindReason::Panic(payload)))
}

/// Stores trace message with backtrace.
#[derive(Debug)]
pub enum Trap {
    /// A user-raised trap through `raise_user_trap`.
    User(Box<dyn Error + Send + Sync>),

    /// A trap raised from the Wasm generated code
    ///
    /// Note: this trap is deterministic (assuming a deterministic host implementation)
    Wasm {
        /// The program counter in generated code where this trap happened.
        pc: usize,
        /// Native stack backtrace at the time the trap occurred
        backtrace: Backtrace,
        /// Optional trapcode associated to the signal that caused the trap
        signal_trap: Option<TrapCode>,
    },

    /// A trap raised from a wasm libcall
    ///
    /// Note: this trap is deterministic (assuming a deterministic host implementation)
    Lib {
        /// Code of the trap.
        trap_code: TrapCode,
        /// Native stack backtrace at the time the trap occurred
        backtrace: Backtrace,
    },

    /// A trap indicating that the runtime was unable to allocate sufficient memory.
    ///
    /// Note: this trap is nondeterministic, since it depends on the host system.
    OOM {
        /// Native stack backtrace at the time the OOM occurred
        backtrace: Backtrace,
    },
}

impl Trap {
    /// Construct a new Wasm trap with the given source location and backtrace.
    ///
    /// Internally saves a backtrace when constructed.
    pub fn wasm(pc: usize, backtrace: Backtrace, signal_trap: Option<TrapCode>) -> Self {
        Self::Wasm { pc, backtrace, signal_trap }
    }

    /// Construct a new Wasm trap with the given trap code.
    ///
    /// Internally saves a backtrace when constructed.
    pub fn lib(trap_code: TrapCode) -> Self {
        let backtrace = Backtrace::new_unresolved();
        Self::Lib { trap_code, backtrace }
    }

    /// Construct a new OOM trap with the given source location and trap code.
    ///
    /// Internally saves a backtrace when constructed.
    pub fn oom() -> Self {
        let backtrace = Backtrace::new_unresolved();
        Self::OOM { backtrace }
    }
}

/// Call the VM function pointed to by `callee`.
///
/// * `callee_env` - the function environment
/// * `trampoline` - the jit-generated trampoline whose ABI takes 3 values, the
///    callee funcenv, the `callee` argument below, and then the `values_vec` argument.
/// * `callee` - the 2nd argument to the `trampoline` function
/// * `values_vec` - points to a buffer which holds the incoming arguments, and to
///   which the outgoing return values will be written.
///
/// Prefer invoking this via `Instance::invoke_trampoline`.
///
/// # Safety
///
/// Wildly unsafe because it calls raw function pointers and reads/writes raw
/// function pointers.
pub unsafe fn near_vm_call_trampoline(
    callee_env: VMFunctionEnvironment,
    trampoline: VMTrampoline,
    callee: *const VMFunctionBody,
    values_vec: *mut u8,
) -> Result<(), Trap> {
    catch_traps(|| {
        mem::transmute::<_, extern "C" fn(VMFunctionEnvironment, *const VMFunctionBody, *mut u8)>(
            trampoline,
        )(callee_env, callee, values_vec);
    })
}

/// Catches any wasm traps that happen within the execution of `closure`,
/// returning them as a `Result`.
///
/// # Safety
///
/// Soundness must not depend on `closure` destructors being run.
pub unsafe fn catch_traps<F>(mut closure: F) -> Result<(), Trap>
where
    F: FnMut(),
{
    return CallThreadState::new().with(|cx| {
        near_vm_register_setjmp(
            cx.jmp_buf.as_ptr(),
            call_closure::<F>,
            &mut closure as *mut F as *mut u8,
        )
    });

    extern "C" fn call_closure<F>(payload: *mut u8)
    where
        F: FnMut(),
    {
        unsafe { (*(payload as *mut F))() }
    }
}

/// Catches any wasm traps that happen within the execution of `closure`,
/// returning them as a `Result`, with the closure contents.
///
/// The main difference from this method and `catch_traps`, is that is able
/// to return the results from the closure.
///
/// # Safety
///
/// Check [`catch_traps`].
pub unsafe fn catch_traps_with_result<F, R>(mut closure: F) -> Result<R, Trap>
where
    F: FnMut() -> R,
{
    let mut global_results = MaybeUninit::<R>::uninit();
    catch_traps(|| {
        global_results.as_mut_ptr().write(closure());
    })?;
    Ok(global_results.assume_init())
}

/// Temporary state stored on the stack which is registered in the `tls` module
/// below for calls into wasm.
pub struct CallThreadState {
    unwind: UnsafeCell<MaybeUninit<UnwindReason>>,
    jmp_buf: Cell<*const u8>,
    prev: Cell<tls::Ptr>,
}

enum UnwindReason {
    /// A panic caused by the host
    Panic(Box<dyn Any + Send>),
    /// A custom error triggered by the user
    UserTrap(Box<dyn Error + Send + Sync>),
    /// A Trap triggered by a wasm libcall
    LibTrap(Trap),
    /// A trap caused by the Wasm generated code
    WasmTrap { backtrace: Backtrace, pc: usize, signal_trap: Option<TrapCode> },
}

impl<'a> CallThreadState {
    #[inline]
    fn new() -> Self {
        Self {
            unwind: UnsafeCell::new(MaybeUninit::uninit()),
            jmp_buf: Cell::new(ptr::null()),
            prev: Cell::new(ptr::null()),
        }
    }

    fn with(self, closure: impl FnOnce(&Self) -> i32) -> Result<(), Trap> {
        let ret = tls::set(&self, || closure(&self))?;
        if ret != 0 {
            return Ok(());
        }
        // We will only reach this path if ret == 0. And that will
        // only happen if a trap did happen. As such, it's safe to
        // assume that the `unwind` field is already initialized
        // at this moment.
        match unsafe { (*self.unwind.get()).as_ptr().read() } {
            UnwindReason::UserTrap(data) => Err(Trap::User(data)),
            UnwindReason::LibTrap(trap) => Err(trap),
            UnwindReason::WasmTrap { backtrace, pc, signal_trap } => {
                Err(Trap::wasm(pc, backtrace, signal_trap))
            }
            UnwindReason::Panic(panic) => std::panic::resume_unwind(panic),
        }
    }

    fn unwind_with(&self, reason: UnwindReason) -> ! {
        unsafe {
            (*self.unwind.get()).as_mut_ptr().write(reason);
            near_vm_unwind(self.jmp_buf.get());
        }
    }
}

// A private inner module for managing the TLS state that we require across
// calls in wasm. The WebAssembly code is called from C++ and then a trap may
// happen which requires us to read some contextual state to figure out what to
// do with the trap. This `tls` module is used to persist that information from
// the caller to the trap site.
mod tls {
    use super::CallThreadState;
    use crate::Trap;
    use std::mem;
    use std::ptr;

    pub use raw::Ptr;

    // An even *more* inner module for dealing with TLS. This actually has the
    // thread local variable and has functions to access the variable.
    //
    // Note that this is specially done to fully encapsulate that the accessors
    // for tls must not be inlined. Wasmer's async support will employ stack
    // switching which can resume execution on different OS threads. This means
    // that borrows of our TLS pointer must never live across accesses because
    // otherwise the access may be split across two threads and cause unsafety.
    //
    // This also means that extra care is taken by the runtime to save/restore
    // these TLS values when the runtime may have crossed threads.
    mod raw {
        use super::CallThreadState;
        use crate::Trap;
        use std::cell::Cell;
        use std::ptr;

        pub type Ptr = *const CallThreadState;

        // The first entry here is the `Ptr` which is what's used as part of the
        // public interface of this module. The second entry is a boolean which
        // allows the runtime to perform per-thread initialization if necessary
        // for handling traps (e.g. setting up ports on macOS and sigaltstack on
        // Unix).
        thread_local!(static PTR: Cell<Ptr> = Cell::new(ptr::null()));

        #[inline(never)] // see module docs for why this is here
        pub fn replace(val: Ptr) -> Result<Ptr, Trap> {
            PTR.with(|p| {
                // When a new value is configured that means that we may be
                // entering WebAssembly so check to see if this thread has
                // performed per-thread initialization for traps.
                let prev = p.get();
                p.set(val);
                Ok(prev)
            })
        }

        #[inline(never)] // see module docs for why this is here
        pub fn get() -> Ptr {
            PTR.with(|p| p.get())
        }
    }

    /// Opaque state used to help control TLS state across stack switches for
    /// async support.
    pub struct TlsRestore(raw::Ptr);

    impl TlsRestore {
        /// Takes the TLS state that is currently configured and returns a
        /// token that is used to replace it later.
        ///
        /// # Safety
        ///
        /// This is not a safe operation since it's intended to only be used
        /// with stack switching found with fibers and async near_vm.
        pub unsafe fn take() -> Result<Self, Trap> {
            // Our tls pointer must be set at this time, and it must not be
            // null. We need to restore the previous pointer since we're
            // removing ourselves from the call-stack, and in the process we
            // null out our own previous field for safety in case it's
            // accidentally used later.
            let raw = raw::get();
            assert!(!raw.is_null());
            let prev = (*raw).prev.replace(ptr::null());
            raw::replace(prev)?;
            Ok(Self(raw))
        }

        /// Restores a previous tls state back into this thread's TLS.
        ///
        /// # Safety
        ///
        /// This is unsafe because it's intended to only be used within the
        /// context of stack switching within near_vm.
        pub unsafe fn replace(self) -> Result<(), super::Trap> {
            // We need to configure our previous TLS pointer to whatever is in
            // TLS at this time, and then we set the current state to ourselves.
            let prev = raw::get();
            assert!((*self.0).prev.get().is_null());
            (*self.0).prev.set(prev);
            raw::replace(self.0)?;
            Ok(())
        }
    }

    /// Configures thread local state such that for the duration of the
    /// execution of `closure` any call to `with` will yield `ptr`, unless this
    /// is recursively called again.
    pub fn set<R>(state: &CallThreadState, closure: impl FnOnce() -> R) -> Result<R, Trap> {
        struct Reset<'a>(&'a CallThreadState);

        impl Drop for Reset<'_> {
            #[inline]
            fn drop(&mut self) {
                raw::replace(self.0.prev.replace(ptr::null()))
                    .expect("tls should be previously initialized");
            }
        }

        // Note that this extension of the lifetime to `'static` should be
        // safe because we only ever access it below with an anonymous
        // lifetime, meaning `'static` never leaks out of this module.
        let ptr = unsafe { mem::transmute::<*const CallThreadState, _>(state) };
        let prev = raw::replace(ptr)?;
        state.prev.set(prev);
        let _reset = Reset(state);
        Ok(closure())
    }

    /// Returns the last pointer configured with `set` above. Panics if `set`
    /// has not been previously called and not returned.
    pub fn with<R>(closure: impl FnOnce(Option<&CallThreadState>) -> R) -> R {
        let p = raw::get();
        unsafe { closure(if p.is_null() { None } else { Some(&*p) }) }
    }
}

extern "C" fn signal_less_trap_handler(pc: *const u8, trap: TrapCode) {
    let jmp_buf = tls::with(|info| {
        let backtrace = Backtrace::new_unresolved();
        let info = info.unwrap();
        unsafe {
            (*info.unwind.get()).as_mut_ptr().write(UnwindReason::WasmTrap {
                backtrace,
                signal_trap: Some(trap),
                pc: pc as usize,
            });
            info.jmp_buf.get()
        }
    });
    unsafe {
        near_vm_unwind(jmp_buf);
    }
}

/// Returns pointer to the trap handler used in VMContext.
pub fn get_trap_handler() -> *const u8 {
    signal_less_trap_handler as *const u8
}

//! Test-loop breakpoint and yield-point mechanism.
//!
//! Lets tests pause a task partway through (at a named yield point) and inspect or manipulate
//! state before resuming. Implemented with stackful coroutines (`corosensei`) so the pause can
//! happen from inside sync code at any call depth, without converting the call chain to
//! `async fn`. Single-threaded throughout: corosensei swaps stack pointers on the same OS
//! thread.
//!
//! Production sites use the macro:
//! ```ignore
//! test_loop_yield!("after_chunk_apply", node = self.id, height = h);
//! ```
//!
//! Tests arm a breakpoint after enabling yield points on the builder:
//! ```ignore
//! let bp = test_loop.breakpoint("after_chunk_apply")
//!     .when(|ctx| ctx.get("node") == Some("node1"))
//!     .arm();
//! test_loop.run_until(|_| bp.hit_count() >= 1, Duration::seconds(5));
//! let hit = bp.take_hit().unwrap();
//! hit.resume();
//! ```
//! Name collisions are caller-managed: `arm()` panics if the same name is already armed.

use corosensei::{Coroutine, CoroutineResult, Yielder};
use parking_lot::Mutex;
use std::cell::Cell;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::task::{Context as TaskContext, Poll, Waker};

/// Type alias for the corosensei yielder used by our coroutines.
type BpYielder = Yielder<(), YieldRequest>;

/// What a yield point sends to its driver. The name identifies the breakpoint;
/// the tags carry caller-supplied context for predicate matching.
pub struct YieldRequest {
    pub name: &'static str,
    pub context: Context,
}

/// Tag map carried by a yield request. Keys are static strings (the macro argument names);
/// values are `Display`-formatted strings produced by the caller.
#[derive(Clone, Default)]
pub struct Context {
    tags: Vec<(&'static str, String)>,
}

impl Context {
    pub(crate) fn new(tags: Vec<(&'static str, String)>) -> Self {
        Self { tags }
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.tags.iter().find(|(k, _)| *k == key).map(|(_, v)| v.as_str())
    }
}

thread_local! {
    /// Pointer to the yielder of the currently-running coroutine, or null when not on a
    /// yieldable coroutine's stack. Saved/restored by `YieldableTask::poll` around each
    /// `resume()` call.
    static ACTIVE_YIELDER: Cell<*const BpYielder> = const { Cell::new(std::ptr::null()) };

    /// Pointer to the breakpoint registry for the currently-active `TestLoopV2`. Set by
    /// `RegistryGuard` around event dispatch; null elsewhere.
    static ACTIVE_REGISTRY: Cell<Option<NonNull<BreakpointRegistry>>> = const { Cell::new(None) };
}

/// RAII guard installed around test-loop event dispatch. While alive, makes the registry
/// visible to any yield point that fires on this thread.
pub(crate) struct RegistryGuard {
    prev: Option<NonNull<BreakpointRegistry>>,
}

impl RegistryGuard {
    pub(crate) fn install(registry: &BreakpointRegistry) -> Self {
        let ptr = NonNull::from(registry);
        let prev = ACTIVE_REGISTRY.with(|cell| {
            let prev = cell.get();
            cell.set(Some(ptr));
            prev
        });
        Self { prev }
    }
}

impl Drop for RegistryGuard {
    fn drop(&mut self) {
        ACTIVE_REGISTRY.with(|cell| cell.set(self.prev));
    }
}

fn with_registry<R>(f: impl FnOnce(&BreakpointRegistry) -> R) -> Option<R> {
    ACTIVE_REGISTRY.with(|cell| {
        let ptr = cell.get()?;
        // Safety: the registry referenced by this pointer was installed by a `RegistryGuard`
        // that is still alive (it is dropped at the end of `process_event`, which is the only
        // place that calls user code on this thread).
        let registry = unsafe { ptr.as_ref() };
        Some(f(registry))
    })
}

/// Cheap predicate the macro uses to short-circuit allocation when nothing could happen here.
/// Inlined so the `is_null` branch is hot-path-friendly.
#[doc(hidden)]
#[inline]
pub fn is_on_coroutine() -> bool {
    !ACTIVE_YIELDER.with(Cell::get).is_null()
}

/// Called by the macro at every yield point that survives `is_on_coroutine`. Suspends the
/// current coroutine; on resume, the wrapper checks the registry and parks the task or
/// continues, then returns control here.
#[doc(hidden)]
pub fn dispatch(name: &'static str, tags: Vec<(&'static str, String)>) {
    let yielder_ptr = ACTIVE_YIELDER.with(Cell::get);
    if yielder_ptr.is_null() {
        return;
    }
    let request = YieldRequest { name, context: Context::new(tags) };
    // Safety: non-null means we are on the coroutine stack whose yielder this is. The
    // reference is valid until the body returns; `coroutine.resume()` on the wrapper's stack
    // frame outlives this `suspend` call.
    let yielder = unsafe { &*yielder_ptr };
    yielder.suspend(request);
}

type Predicate = dyn Fn(&Context) -> bool + Send + Sync;

struct ArmedBreakpoint {
    predicate: Arc<Predicate>,
    hits: VecDeque<HitInternal>,
}

/// A registry of armed breakpoints, owned by `TestLoopV2`.
pub struct BreakpointRegistry {
    armed: Mutex<HashMap<&'static str, ArmedBreakpoint>>,
}

impl BreakpointRegistry {
    pub(crate) fn new() -> Self {
        Self { armed: Mutex::new(HashMap::new()) }
    }

    /// Arms a breakpoint. Panics if the same name is already armed (one matcher per name).
    fn arm(&self, name: &'static str, predicate: Arc<Predicate>) {
        let mut armed = self.armed.lock();
        if armed.contains_key(name) {
            panic!("breakpoint {name:?} is already armed; only one matcher per name");
        }
        armed.insert(name, ArmedBreakpoint { predicate, hits: VecDeque::new() });
    }

    /// Disarms a breakpoint. Auto-resumes any queued hits so their tasks don't hang.
    fn disarm(&self, name: &'static str) {
        let removed = self.armed.lock().remove(name);
        if let Some(entry) = removed {
            for hit in entry.hits {
                hit.flag.store(true, Ordering::Release);
                hit.waker.wake();
            }
        }
    }

    fn hit_count(&self, name: &'static str) -> usize {
        self.armed.lock().get(name).map_or(0, |b| b.hits.len())
    }

    fn take_hit(&self, name: &'static str) -> Option<Hit> {
        let mut armed = self.armed.lock();
        let entry = armed.get_mut(name)?;
        entry.hits.pop_front().map(Hit::from_internal)
    }

    fn drain_hits(&self, name: &'static str) -> Vec<Hit> {
        let mut armed = self.armed.lock();
        let Some(entry) = armed.get_mut(name) else {
            return Vec::new();
        };
        entry.hits.drain(..).map(Hit::from_internal).collect()
    }

    /// If an armed breakpoint matches the request, enqueue a hit and return true (the caller
    /// should suspend the task). If no match, return false (the caller continues immediately).
    fn try_match(
        &self,
        request: YieldRequest,
        waker: Waker,
        resumed_flag: Arc<AtomicBool>,
    ) -> bool {
        let mut armed = self.armed.lock();
        let Some(entry) = armed.get_mut(request.name) else {
            return false;
        };
        if !(entry.predicate)(&request.context) {
            return false;
        }
        entry.hits.push_back(HitInternal { context: request.context, waker, flag: resumed_flag });
        true
    }
}

struct HitInternal {
    context: Context,
    waker: Waker,
    flag: Arc<AtomicBool>,
}

/// A single hit on an armed breakpoint. Holds the parked task's waker.
pub struct Hit {
    context: Context,
    waker: Waker,
    flag: Arc<AtomicBool>,
}

impl Hit {
    fn from_internal(internal: HitInternal) -> Self {
        Self { context: internal.context, waker: internal.waker, flag: internal.flag }
    }

    pub fn context(&self) -> &Context {
        &self.context
    }

    /// Unblocks the parked task. The task will be polled again on the next event-loop tick.
    pub fn resume(self) {
        self.flag.store(true, Ordering::Release);
        self.waker.wake();
    }
}

/// Handle returned by `TestLoopV2::breakpoint(name)`. Builds the matcher, arms it, and lets
/// the test poll for and drain hits. Dropping the handle disarms (and auto-resumes queued hits).
pub struct BreakpointHandle {
    name: &'static str,
    registry: Arc<BreakpointRegistry>,
    predicate: Arc<Predicate>,
    armed: bool,
}

impl BreakpointHandle {
    pub(crate) fn new(name: &'static str, registry: Arc<BreakpointRegistry>) -> Self {
        Self { name, registry, predicate: Arc::new(|_: &Context| true), armed: false }
    }

    pub fn when(mut self, f: impl Fn(&Context) -> bool + Send + Sync + 'static) -> Self {
        assert!(!self.armed, "set .when() before calling .arm()");
        self.predicate = Arc::new(f);
        self
    }

    pub fn arm(mut self) -> Self {
        assert!(!self.armed, "breakpoint already armed");
        self.registry.arm(self.name, self.predicate.clone());
        self.armed = true;
        self
    }

    pub fn hit_count(&self) -> usize {
        self.registry.hit_count(self.name)
    }

    pub fn take_hit(&self) -> Option<Hit> {
        self.registry.take_hit(self.name)
    }

    pub fn drain_hits(&self) -> Vec<Hit> {
        self.registry.drain_hits(self.name)
    }
}

impl Drop for BreakpointHandle {
    fn drop(&mut self) {
        if self.armed {
            self.registry.disarm(self.name);
        }
    }
}

/// A future that drives a sync closure running inside a corosensei coroutine. Each call to
/// `test_loop_yield!` inside the closure suspends the coroutine; this future then either
/// re-resumes immediately (no armed breakpoint matched) or returns `Pending` until the
/// matched `Hit::resume()` flips the resume flag.
pub struct YieldableTask {
    coroutine: Option<Coroutine<(), YieldRequest, ()>>,
    /// Set by the coroutine body on its first run; lets the wrapper install the yielder thread-local storage
    /// on subsequent resumes.
    yielder_slot: Arc<AtomicPtr<BpYielder>>,
    /// Set by `Hit::resume()` to signal "you can continue now."
    resumed: Arc<AtomicBool>,
    /// True when we returned `Pending` after a breakpoint match; we must observe `resumed` to
    /// flip before polling further.
    waiting_for_resume: bool,
}

// Safety: corosensei's `Coroutine` is `!Send` because moving a coroutine to a different OS
// thread mid-flight is unsound (its stack pointer would refer to the original thread). The
// test-loop framework runs every event on the same thread and panics at runtime if anything
// else is observed (`InFlightEvents::add` in core/async/src/test_loop/mod.rs). The future is
// never actually moved between threads, only sent through `BoxFuture` to satisfy the type
// system of `FutureSpawner::spawn_boxed`.
unsafe impl Send for YieldableTask {}

impl YieldableTask {
    pub fn new(work: Box<dyn FnOnce() + Send>) -> Self {
        let yielder_slot = Arc::new(AtomicPtr::<BpYielder>::new(std::ptr::null_mut()));
        let slot_for_body = yielder_slot.clone();
        let coroutine = Coroutine::new(move |yielder: &BpYielder, _: ()| {
            // The wrapper can't access `yielder` directly (corosensei only hands it to the
            // body), so stash a pointer for subsequent resumes. The first resume still needs
            // its own install since the wrapper doesn't yet have the pointer.
            slot_for_body.store(yielder as *const _ as *mut _, Ordering::Release);
            let prev = ACTIVE_YIELDER.with(|cell| {
                let prev = cell.get();
                cell.set(yielder as *const _);
                prev
            });
            work();
            ACTIVE_YIELDER.with(|cell| cell.set(prev));
        });
        Self {
            coroutine: Some(coroutine),
            yielder_slot,
            resumed: Arc::new(AtomicBool::new(false)),
            waiting_for_resume: false,
        }
    }
}

impl Future for YieldableTask {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<()> {
        let this = self.get_mut();
        loop {
            if this.waiting_for_resume {
                if !this.resumed.swap(false, Ordering::Acquire) {
                    return Poll::Pending;
                }
                this.waiting_for_resume = false;
            }
            let Some(coroutine) = this.coroutine.as_mut() else {
                return Poll::Ready(());
            };
            // First resume: slot is still null (body sets it on entry and does the first
            // install itself). Subsequent resumes: install here so user code reads the right
            // yielder regardless of what happened on the test-loop thread in between.
            let yielder_ptr = this.yielder_slot.load(Ordering::Acquire);
            let prev_tls = ACTIVE_YIELDER.with(|cell| {
                let prev = cell.get();
                if !yielder_ptr.is_null() {
                    cell.set(yielder_ptr);
                }
                prev
            });
            let result = coroutine.resume(());
            ACTIVE_YIELDER.with(|cell| cell.set(prev_tls));

            match result {
                CoroutineResult::Return(()) => {
                    this.coroutine = None;
                    return Poll::Ready(());
                }
                CoroutineResult::Yield(request) => {
                    let parked = with_registry(|registry| {
                        registry.try_match(request, cx.waker().clone(), this.resumed.clone())
                    })
                    .unwrap_or(false);
                    if parked {
                        this.waiting_for_resume = true;
                        return Poll::Pending;
                    }
                    continue;
                }
            }
        }
    }
}

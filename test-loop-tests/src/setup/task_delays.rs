//! Delays on the background tasks a node runs: from the moment a delay is added until it is
//! lifted, every task of the node that it matches takes that much longer in test-loop time,
//! in place of the spawner's default. Delays are keyed by account, so a node that is killed
//! and restarted keeps them, and a node added later is covered by the delays added for its
//! account before.
//!
//! A task is named by the first argument of the `spawn` call that starts it, see
//! [`task_names`] for the ones tests reach for. When several delays match a task, the one
//! added last decides and is the only one counted, so a later, narrower delay overrides an
//! earlier, broader one.

use near_async::time::Duration;
use near_primitives::types::AccountId;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// The names of the spawner tasks tests slow down, as the code that spawns them names
/// them. A name that matches nothing is not an error: the delay's handle reports zero
/// matches, which is what a test asserts on.
// A catalog: not every name has a test using it at every feature set.
#[allow(dead_code)]
pub mod task_names {
    /// A block's chunks applied by the client, `Chain::apply_chunks_preprocessing`.
    pub const APPLY_CHUNKS_NORMAL: &str = "apply_chunks_normal";
    /// An optimistic block's chunks applied by the client.
    pub const APPLY_CHUNKS_OPTIMISTIC: &str = "apply_chunks_optimistic";
    /// A chunk applied by the chunk executor under spice; also the name the sync jobs actor
    /// gives the chunks it applies while catching up.
    pub const SPICE_APPLY_CHUNKS: &str = "apply_chunks";
    /// A chunk-only validator's witness validation under spice; the only task such a
    /// validator runs per block, since it applies no chunk.
    pub const SPICE_STATELESS_VALIDATION: &str = "spice_stateless_validation";
    /// A chunk validator's witness validation before spice.
    pub const STATELESS_VALIDATION: &str = "stateless_validation";
    /// A chunk producer's transaction preparation.
    pub const PREPARE_TRANSACTIONS: &str = "prepare_transactions";
}

/// The part of a delay the test keeps: switches it off and says how often it applied.
#[derive(Clone)]
pub struct TaskDelayHandle(Arc<DelayState>);

#[derive(Default)]
struct DelayState {
    lifted: AtomicBool,
    matched: AtomicU64,
}

impl TaskDelayHandle {
    /// Switches the delay off: no task is slowed by it from now on. Tasks already queued
    /// with the delay run when they were going to.
    pub fn lift(&self) {
        self.0.lifted.store(true, Ordering::Relaxed);
    }

    /// How many tasks the delay applied to so far, so a test can check that it fired.
    pub fn matched(&self) -> u64 {
        self.0.matched.load(Ordering::Relaxed)
    }
}

struct TaskDelay {
    /// The node the delay is on; `None` for every node, the ones added later included.
    account_id: Option<AccountId>,
    matches: Box<dyn Fn(&str) -> bool + Send + Sync>,
    delay: Duration,
    state: Arc<DelayState>,
}

/// Every task delay of the test, shared by the spawners of all nodes.
#[derive(Clone, Default)]
pub struct TaskDelays(Arc<Mutex<Vec<Arc<TaskDelay>>>>);

impl TaskDelays {
    /// Adds a delay on the tasks of `account_id` whose name `matches`, after the delays added
    /// before, which it overrides where they both match.
    pub fn add(
        &self,
        account_id: AccountId,
        matches: impl Fn(&str) -> bool + Send + Sync + 'static,
        delay: Duration,
    ) -> TaskDelayHandle {
        self.add_delay(Some(account_id), Box::new(matches), delay)
    }

    /// [`Self::add`] on every node, the ones added later included: one delay with one count,
    /// lifted once.
    pub fn add_for_all(
        &self,
        matches: impl Fn(&str) -> bool + Send + Sync + 'static,
        delay: Duration,
    ) -> TaskDelayHandle {
        self.add_delay(None, Box::new(matches), delay)
    }

    fn add_delay(
        &self,
        account_id: Option<AccountId>,
        matches: Box<dyn Fn(&str) -> bool + Send + Sync>,
        delay: Duration,
    ) -> TaskDelayHandle {
        let state = Arc::new(DelayState::default());
        let task_delay = TaskDelay { account_id, matches, delay, state: state.clone() };
        self.0.lock().push(Arc::new(task_delay));
        TaskDelayHandle(state)
    }

    /// The delay the task `task_name` of `account_id` takes now, if a delay matches it: the
    /// last one added that does, which is counted as applied.
    pub(crate) fn delay_for(&self, account_id: &AccountId, task_name: &str) -> Option<Duration> {
        let delays = self.0.lock();
        let task_delay = delays.iter().rev().find(|task_delay| {
            task_delay.account_id.as_ref().is_none_or(|on| on == account_id)
                && !task_delay.state.lifted.load(Ordering::Relaxed)
                && (task_delay.matches)(task_name)
        })?;
        task_delay.state.matched.fetch_add(1, Ordering::Relaxed);
        Some(task_delay.delay)
    }
}

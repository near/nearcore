//! `slow_node`: one node's processing is slowed for a window of heights.

use super::super::scenario::{Fault, FaultEvent, Progress, Timeline};
use super::endorsement_delay::{WindowEnd, WindowStart};
use super::render;
use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use near_async::time::Duration;
use near_primitives::types::AccountId;
use parking_lot::Mutex;
use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const DEFAULT_NODE: usize = 4;
const DEFAULT_TASK: &str = "spice_stateless_validation";
const DEFAULT_DELAY_MS: u64 = 2000;
const DEFAULT_START: WindowStart = WindowStart::Offset(-3);
const DEFAULT_END: WindowEnd = WindowEnd::Offset(3);

/// The test-loop spawner's `task` on `node` takes `delay` longer between `start` and
/// `end`, through the builder's `task_delay_fn`. The hook can only be set at build time,
/// so the closure reads the delay from a shared cell the fault owns, zero outside the
/// window, and counts what it delayed, which is what the fired check reads: a chunk-only
/// validator applies no chunk, so its head never lags, and what the default task slows is
/// its witness validation. The target is a chunk-only validator until milestone 6: a
/// slowed producer skips its pre-spice heights and moves the boundary.
pub struct SlowNode {
    node: usize,
    task: String,
    delay_ms: u64,
    start: WindowStart,
    end: WindowEnd,
    /// The delay the spawner applies right now, in milliseconds; zero outside the window.
    current_delay_ms: Arc<AtomicU64>,
    /// The slowed account, set once the env exists.
    account_id: Arc<Mutex<Option<AccountId>>>,
    opened: bool,
    closed: bool,
    /// How many tasks the spawner delayed, counted by the hook itself.
    delayed: Arc<AtomicU64>,
}

impl Default for SlowNode {
    fn default() -> Self {
        Self::new()
    }
}

impl SlowNode {
    /// The first chunk-only validator of profile A, with its chunk application slowed by
    /// two seconds from three heights below the boundary to three above it.
    pub fn new() -> Self {
        Self {
            node: DEFAULT_NODE,
            task: DEFAULT_TASK.to_string(),
            delay_ms: DEFAULT_DELAY_MS,
            start: DEFAULT_START,
            end: DEFAULT_END,
            current_delay_ms: Arc::new(AtomicU64::new(0)),
            account_id: Arc::new(Mutex::new(None)),
            opened: false,
            closed: false,
            delayed: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn node(mut self, node: usize) -> Self {
        self.node = node;
        self
    }

    /// Surface for the string form of milestone 4; no preset needs it.
    #[allow(dead_code)]
    pub fn task(mut self, task: &str) -> Self {
        self.task = task.to_string();
        self
    }

    /// Surface for the string form of milestone 4; no preset needs it.
    #[allow(dead_code)]
    pub fn delay_ms(mut self, delay_ms: u64) -> Self {
        self.delay_ms = delay_ms;
        self
    }

    pub fn start(mut self, start: WindowStart) -> Self {
        self.start = start;
        self
    }

    pub fn end(mut self, end: WindowEnd) -> Self {
        self.end = end;
        self
    }

    pub fn target(&self) -> usize {
        self.node
    }

    fn open(&mut self) {
        self.current_delay_ms.store(self.delay_ms, Ordering::Relaxed);
        self.opened = true;
    }

    fn close(&mut self) {
        self.current_delay_ms.store(0, Ordering::Relaxed);
        self.closed = true;
    }

    fn is_open(&self) -> bool {
        self.opened && !self.closed
    }
}

impl Debug for SlowNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let start = match self.start {
            WindowStart::WarmedUp => Some("warmed_up".to_string()),
            WindowStart::Offset(start) => (self.start != DEFAULT_START).then(|| start.to_string()),
        };
        let end = match self.end {
            WindowEnd::Done => Some("done".to_string()),
            WindowEnd::Offset(end) => (self.end != DEFAULT_END).then(|| end.to_string()),
        };
        render(
            f,
            self.tag(),
            &[
                ("node", (self.node != DEFAULT_NODE).then(|| self.node.to_string())),
                ("task", (self.task != DEFAULT_TASK).then(|| self.task.clone())),
                (
                    "delay_ms",
                    (self.delay_ms != DEFAULT_DELAY_MS).then(|| self.delay_ms.to_string()),
                ),
                ("start", start),
                ("end", end),
            ],
        )
    }
}

impl Fault for SlowNode {
    fn tag(&self) -> &'static str {
        "slow_node"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn validate(&self) -> Result<(), String> {
        if self.delay_ms == 0 {
            return Err("delay_ms must be positive".to_string());
        }
        if let (WindowStart::Offset(start), WindowEnd::Offset(end)) = (self.start, self.end) {
            if start >= end {
                return Err(format!("the window start {start} must be below its end {end}"));
            }
        }
        Ok(())
    }

    fn setup(&self, builder: TestLoopBuilder) -> TestLoopBuilder {
        let account_id = self.account_id.clone();
        let current_delay_ms = self.current_delay_ms.clone();
        let delayed = self.delayed.clone();
        let task = self.task.clone();
        builder.task_delay_fn(move |account, task_name| {
            if task_name != task || account_id.lock().as_ref() != Some(account) {
                return None;
            }
            let delay_ms = current_delay_ms.load(Ordering::Relaxed);
            if delay_ms == 0 {
                return None;
            }
            delayed.fetch_add(1, Ordering::Relaxed);
            Some(Duration::milliseconds(delay_ms as i64))
        })
    }

    fn on_progress(
        &mut self,
        env: &mut TestLoopEnv,
        _timeline: &Timeline,
        progress: &Progress,
    ) -> Vec<FaultEvent> {
        match progress {
            Progress::Built => {
                let data = env
                    .node_datas
                    .get(self.node)
                    .unwrap_or_else(|| panic!("node {} does not exist", self.node));
                *self.account_id.lock() = Some(data.account_id.clone());
            }
            Progress::WarmedUp if self.start == WindowStart::WarmedUp => self.open(),
            Progress::BoundaryKnown { estimate } => {
                let offset_now = env.rpc_node().head().height as i64 - *estimate as i64;
                if let WindowStart::Offset(start) = self.start {
                    if start < offset_now && !self.opened {
                        self.open();
                    }
                }
            }
            Progress::Height { offset, .. } => {
                if self.start == WindowStart::Offset(*offset) && !self.opened {
                    self.open();
                }
                if self.end == WindowEnd::Offset(*offset) && self.is_open() {
                    self.close();
                }
            }
            _ => {}
        }
        Vec::new()
    }

    fn assert_fired(&self, _env: &TestLoopEnv, _timeline: &Timeline) {
        assert!(self.opened, "the window never opened");
        let delayed = self.delayed.load(Ordering::Relaxed);
        assert!(
            delayed > 0,
            "node {} ran no {:?} task inside the window, so nothing was slowed",
            self.node,
            self.task,
        );
    }
}

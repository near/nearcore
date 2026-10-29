//! `slow_node`: one node's processing is slowed for a window of heights.

use super::super::scenario::{Fault, FaultEvent, Progress, Timeline};
use super::endorsement_delay::{WindowEnd, WindowStart};
use super::render;
use crate::setup::env::TestLoopEnv;
use crate::setup::task_delays::{TaskDelayHandle, task_names};
use near_async::time::Duration;
use std::any::Any;
use std::fmt::{self, Debug, Formatter};

const DEFAULT_NODE: usize = 4;
const DEFAULT_TASK: &str = task_names::SPICE_STATELESS_VALIDATION;
const DEFAULT_DELAY_MS: u64 = 2000;
const DEFAULT_START: WindowStart = WindowStart::Offset(-3);
const DEFAULT_END: WindowEnd = WindowEnd::Offset(3);

/// The spawner task `task` on `node` takes `delay` between `start` and `end`, through the
/// env's task delays: added at `start`, lifted at `end`, and the fired check reads how many
/// tasks it applied to. A chunk-only validator applies no chunk, so its head never lags, and
/// what the default task slows is its witness validation. The target is a chunk-only
/// validator until milestone 6: a slowed producer skips its pre-spice heights and moves the
/// boundary.
pub struct SlowNode {
    node: usize,
    task: String,
    delay_ms: u64,
    start: WindowStart,
    end: WindowEnd,
    /// The delay in force, from the window's start.
    handle: Option<TaskDelayHandle>,
    closed: bool,
}

impl Default for SlowNode {
    fn default() -> Self {
        Self::new()
    }
}

impl SlowNode {
    /// The first chunk-only validator of profile A, with its witness validation slowed by two
    /// seconds from three heights below the boundary to three above it.
    pub fn new() -> Self {
        Self {
            node: DEFAULT_NODE,
            task: DEFAULT_TASK.to_string(),
            delay_ms: DEFAULT_DELAY_MS,
            start: DEFAULT_START,
            end: DEFAULT_END,
            handle: None,
            closed: false,
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

    fn open(&mut self, env: &TestLoopEnv) {
        assert!(self.handle.is_none(), "the window opens once");
        let delay = Duration::milliseconds(self.delay_ms as i64);
        self.handle = Some(env.delay_task(self.node, &self.task, delay));
    }

    fn close(&mut self) {
        assert!(self.is_open(), "the window closes once, after it opened");
        self.handle.as_ref().unwrap().lift();
        self.closed = true;
    }

    fn is_open(&self) -> bool {
        self.handle.is_some() && !self.closed
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

    fn on_progress(
        &mut self,
        env: &mut TestLoopEnv,
        _timeline: &Timeline,
        progress: &Progress,
    ) -> Vec<FaultEvent> {
        match progress {
            Progress::Built => {
                assert!(self.node < env.node_datas.len(), "node {} does not exist", self.node);
            }
            Progress::WarmedUp if self.start == WindowStart::WarmedUp => self.open(env),
            Progress::BoundaryKnown { estimate } => {
                let offset_now = env.rpc_node().head().height as i64 - *estimate as i64;
                if let WindowStart::Offset(start) = self.start {
                    if start < offset_now && self.handle.is_none() {
                        self.open(env);
                    }
                }
            }
            Progress::Height { offset, .. } => {
                if self.start == WindowStart::Offset(*offset) && self.handle.is_none() {
                    self.open(env);
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
        let handle = self.handle.as_ref().expect("the window never opened");
        assert!(
            handle.matched() > 0,
            "node {} ran no {:?} task inside the window, so nothing was slowed",
            self.node,
            self.task,
        );
    }
}

//! `gc_tight`: garbage collection runs past the boundary during the test.

use super::super::loads::LoadKind;
use super::super::resolve::Profile;
use super::super::scenario::{Fault, Timeline};
use super::render;
use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use near_async::time::Duration;
use std::any::Any;
use std::fmt::{self, Debug, Formatter};

/// The GC stop height is the start of the epoch `keep - 1` epochs below the head's. At
/// `done` the head is in the third spice epoch, so `keep=4` puts the stop height at the
/// start of the last pre-spice epoch and every block the oracles read is still there. The
/// fault then adds a post-`done` phase: the driver runs on until the stop height passes
/// the last pre-spice height, about one more epoch, the tail follows it, and the node
/// must still answer.
const DEFAULT_KEEP: u64 = 4;

pub struct GcTight {
    keep: u64,
}

impl Default for GcTight {
    fn default() -> Self {
        Self::new()
    }
}

impl GcTight {
    pub fn new() -> Self {
        Self { keep: DEFAULT_KEEP }
    }

    /// Surface for the string form of milestone 4; no preset needs it.
    #[allow(dead_code)]
    pub fn keep(mut self, keep: u64) -> Self {
        self.keep = keep;
        self
    }
}

impl Debug for GcTight {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        render(
            f,
            self.tag(),
            &[("keep", (self.keep != DEFAULT_KEEP).then(|| self.keep.to_string()))],
        )
    }
}

impl Fault for GcTight {
    fn tag(&self) -> &'static str {
        "gc_tight"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn validate(&self) -> Result<(), String> {
        if self.keep < DEFAULT_KEEP {
            return Err(format!(
                "keep={} would let the tail pass the last pre-spice block before done, where \
                 the oracles still read it; the least is {DEFAULT_KEEP}",
                self.keep,
            ));
        }
        Ok(())
    }

    fn setup(&self, builder: TestLoopBuilder) -> TestLoopBuilder {
        builder.gc_num_epochs_to_keep(self.keep)
    }

    /// The tail passes the boundary and the node still answers.
    fn post_done(&mut self, env: &mut TestLoopEnv, timeline: &Timeline) -> Result<(), String> {
        let boundary_height = timeline.last_pre_spice_height().unwrap();
        let epoch_length = Profile::A.epoch_length();
        // The stop height moves one epoch at a time, so one more epoch takes it past the
        // boundary; the tail follows it a few blocks later.
        let target = env.rpc_node().head().height + epoch_length + 5;
        env.rpc_runner().run_until_head_height(target);
        let stop_height = {
            let node = env.rpc_node();
            node.client().runtime_adapter.get_gc_stop_height(&node.head().last_block_hash)
        };
        if stop_height <= boundary_height {
            return Err(format!(
                "the GC stop height is {stop_height} after one more epoch, not past the last \
                 pre-spice block at {boundary_height}"
            ));
        }
        env.rpc_runner().run_until(|node| node.tail() > boundary_height, Duration::seconds(60));
        let node = env.rpc_node();
        let tail = node.tail();
        if tail <= boundary_height {
            return Err(format!(
                "the tail is at {tail}, not past the last pre-spice block at {boundary_height}"
            ));
        }
        node.view_account_query(&LoadKind::TransferLowToUpper.receiver())
            .map_err(|err| format!("the node no longer answers a view query: {err:?}"))?;
        let _ = node.head_block();
        Ok(())
    }
}

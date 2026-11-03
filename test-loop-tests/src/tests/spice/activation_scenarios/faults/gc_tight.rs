//! `gc_tight`: garbage collection runs past the boundary during the test.

use super::super::loads::LoadKind;
use super::super::resolve::Resolved;
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
/// the last pre-spice height, `keep - 3` more epochs, the tail follows it, and the node
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

    /// The epochs of blocks the node keeps, at least the default.
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

    fn validate(&self, _resolved: Resolved) -> Result<(), String> {
        if self.keep < DEFAULT_KEEP {
            return Err(format!(
                "keep={} would let the tail pass the last pre-spice block before done, where \
                 the oracles still read it; the least is {DEFAULT_KEEP}",
                self.keep,
            ));
        }
        Ok(())
    }

    fn setup(&self, _resolved: Resolved, builder: TestLoopBuilder) -> TestLoopBuilder {
        builder.gc_num_epochs_to_keep(self.keep)
    }

    /// The tail passes the boundary and the node still answers.
    fn post_done(&mut self, env: &mut TestLoopEnv, timeline: &Timeline) -> Result<(), String> {
        let boundary_height = timeline.last_pre_spice_height().unwrap();
        // The stop height moves one epoch at a time and trails the head by `keep - 1`
        // epochs. The head is in the third spice epoch at `done`, and the stop height is past
        // the boundary once the head is in the spice epoch numbered `keep`, `keep - 3` epochs
        // on; the tail follows it a few blocks later.
        let target = {
            let node = env.rpc_node();
            let epoch_length = node
                .client()
                .epoch_manager
                .get_epoch_config(&node.head().epoch_id)
                .unwrap()
                .epoch_length;
            node.head().height + (self.keep - 3) * epoch_length + 5
        };
        env.rpc_runner().run_until_head_height(target);
        let stop_height = {
            let node = env.rpc_node();
            node.client().runtime_adapter.get_gc_stop_height(&node.head().last_block_hash)
        };
        if stop_height <= boundary_height {
            return Err(format!(
                "the GC stop height is {stop_height} after {} more epochs, not past the last \
                 pre-spice block at {boundary_height}",
                self.keep - 3,
            ));
        }
        env.rpc_runner()
            .try_run_until(|node| node.tail() > boundary_height, Duration::seconds(60))
            .map_err(|timed_out| {
                format!("the tail did not pass the last pre-spice block at {boundary_height}: {timed_out}")
            })?;
        let node = env.rpc_node();
        node.view_account_query(&LoadKind::TransferLowToUpper.receiver())
            .map_err(|err| format!("the node no longer answers a view query: {err:?}"))?;
        let _ = node.head_block();
        Ok(())
    }
}

//! `archival`: an archival node follows the chain across the boundary.

use super::super::driver::RunRecord;
use super::super::oracles::{Oracle, Relaxations};
use super::super::resolve::Resolved;
use super::super::scenario::{Fault, OracleId};
use super::render;
use crate::setup::builder::{ArchivalKind, TestLoopBuilder};
use crate::setup::env::TestLoopEnv;
use near_chain::ChainStoreAccess;
use near_primitives::block::Tip;
use near_store::{COLD_HEAD_KEY, DBCol};
use std::any::Any;
use std::fmt::{self, Debug, Formatter};

/// An archival node with split storage added to the profile. It tracks every shard and
/// is never a validator, so it crosses the boundary the way an rpc node does, and its cold
/// head must keep following the final head across it.
#[derive(Default)]
pub struct Archival;

impl Archival {
    pub fn new() -> Self {
        Self
    }
}

impl Debug for Archival {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        render(f, self.tag(), &[])
    }
}

impl Fault for Archival {
    fn tag(&self) -> &'static str {
        "archival"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn setup(&self, _resolved: Resolved, builder: TestLoopBuilder) -> TestLoopBuilder {
        builder.enable_archival_node(ArchivalKind::Cold)
    }

    fn extra_oracles(&self) -> Vec<Box<dyn Oracle>> {
        vec![Box::new(ArchivalCrosses)]
    }
}

/// The archival node's head is spice, its execution crossed the boundary and its cold
/// head passed the last pre-spice block.
struct ArchivalCrosses;

impl Oracle for ArchivalCrosses {
    fn id(&self) -> OracleId {
        OracleId::ArchivalCrosses
    }

    fn check(
        &self,
        env: &TestLoopEnv,
        run: &RunRecord,
        _relaxations: &Relaxations,
    ) -> Result<(), String> {
        let node = env.archival_node();
        let boundary_height = run.last_pre_spice().header().height();
        if !node.head_block().is_spice_block() {
            return Err(format!(
                "the archival node's head at height {} is not a spice block",
                node.head().height
            ));
        }
        let chain_store = &node.client().chain.chain_store;
        let final_execution_head = chain_store
            .spice_final_execution_head()
            .map_err(|err| format!("the archival node has no spice final execution head: {err}"))?;
        if final_execution_head.height <= boundary_height {
            return Err(format!(
                "the archival node's spice final execution head is at height {}, not past the \
                 last pre-spice block at {boundary_height}",
                final_execution_head.height,
            ));
        }
        let cold_head = chain_store
            .store()
            .get_ser::<Tip>(DBCol::BlockMisc, COLD_HEAD_KEY)
            .ok_or_else(|| "the archival node has no cold head".to_string())?;
        if cold_head.height <= boundary_height {
            return Err(format!(
                "the archival node's cold head is at height {}, not past the last pre-spice \
                 block at {boundary_height}",
                cold_head.height,
            ));
        }
        Ok(())
    }
}

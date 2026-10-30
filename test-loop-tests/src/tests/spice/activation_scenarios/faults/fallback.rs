//! `fallback`: the boundary chunks' designated endorsements dropped, so only the all-stake
//! fallback certifies them.

use super::super::driver::RunRecord;
use super::super::oracles::{
    Oracle, Relaxations, check_certified_via_fallback, check_fallback_has_enough_stake,
};
use super::super::scenario::{
    Fault, FaultEvent, Narrowing, OracleId, Progress, Relaxation, Timeline,
};
use super::render;
use crate::setup::env::TestLoopEnv;
use crate::setup::network_rules::{Action, RuleHandle};
use near_network::types::NetworkRequests;
use std::any::Any;
use std::fmt::{self, Debug, Formatter};

/// A network rule on every node from `boundary_known` that drops the endorsements of the
/// last pre-spice block's chunks by their designated validators, so those chunks certify
/// only once their fallback window opens, through the non-designated remainder; lifted at
/// `boundary_certified`. Selects profile B, whose designated sets hold under a third of the
/// stake; that they do is checked on the genesis epoch at `built`, which is representative.
///
/// The drop is limited to the boundary chunks on purpose. A chunk becomes fallback-eligible
/// `SPICE_FALLBACK_CERTIFICATION_DELAY` blocks after it became certifiable, which is when
/// the chunk before it certified, so under a total designated outage the certified frontier
/// advances one block per window and a run that must leave two spice epochs never ends;
/// the all-stake fallback tests cover that outage on their own. Here certification trails
/// the head by a window at the boundary and catches up behind it, which stretches the first
/// spice epoch, so the fault asks for more time.
#[derive(Default)]
pub struct Fallback {
    rules: Vec<RuleHandle>,
}

impl Fallback {
    pub fn new() -> Self {
        Self::default()
    }

    fn dropped(&self) -> u64 {
        self.rules.iter().map(RuleHandle::matched).sum()
    }
}

impl Debug for Fallback {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        render(f, self.tag(), &[])
    }
}

impl Fault for Fallback {
    fn tag(&self) -> &'static str {
        "fallback"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn on_progress(
        &mut self,
        env: &mut TestLoopEnv,
        _timeline: &Timeline,
        progress: &Progress,
    ) -> Vec<FaultEvent> {
        match progress {
            Progress::Built => {
                check_fallback_has_enough_stake(&env.rpc_node())
                    .unwrap_or_else(|error| panic!("precondition: {error}"));
            }
            Progress::BoundaryKnown { estimate } => {
                // Nothing moves the boundary until milestone 6, so the last pre-spice block
                // sits right below the estimate; a node endorses a block it has processed,
                // so its own epoch manager knows the block's height and epoch.
                let boundary_height = estimate - 1;
                for index in 0..env.node_datas.len() {
                    let epoch_manager = env.node(index).client().epoch_manager.clone();
                    let matches = move |request: &NetworkRequests| {
                        let NetworkRequests::SpiceChunkEndorsement(_, endorsement) = request else {
                            return false;
                        };
                        let Ok(block_info) = epoch_manager.get_block_info(endorsement.block_hash())
                        else {
                            return false;
                        };
                        if block_info.height() != boundary_height {
                            return false;
                        }
                        epoch_manager
                            .get_chunk_validator_assignments(
                                block_info.epoch_id(),
                                endorsement.shard_id(),
                                block_info.height(),
                            )
                            .is_ok_and(|designated| designated.contains(endorsement.account_id()))
                    };
                    self.rules.push(env.network_rule(index, matches, Action::Drop));
                }
            }
            Progress::BoundaryCertified => {
                for rule in &self.rules {
                    rule.lift();
                }
            }
            _ => {}
        }
        Vec::new()
    }

    fn relaxes(&self) -> Vec<Relaxation> {
        vec![Relaxation { oracle: OracleId::WithinWindow, narrowing: Narrowing::Whole }]
    }

    fn extra_oracles(&self) -> Vec<Box<dyn Oracle>> {
        vec![Box::new(CertifiedViaFallback)]
    }

    fn extra_epochs(&self) -> u64 {
        2
    }

    /// Designated endorsements of the boundary chunks were dropped, and the last pre-spice
    /// block certified through the fallback.
    fn assert_fired(&self, env: &TestLoopEnv, timeline: &Timeline) {
        assert!(self.dropped() > 0, "no designated endorsement of a boundary chunk was dropped");
        let last_pre_spice = timeline.last_pre_spice.as_ref().unwrap();
        check_certified_via_fallback(&env.rpc_node(), last_pre_spice.header())
            .unwrap_or_else(|error| panic!("the last pre-spice block: {error}"));
    }
}

/// The boundary certified through the fallback and certification carried on past it: the
/// certified frontier at `done` is beyond the last pre-spice block.
struct CertifiedViaFallback;

impl Oracle for CertifiedViaFallback {
    fn id(&self) -> OracleId {
        OracleId::CertifiedViaFallback
    }

    fn check(&self, env: &TestLoopEnv, run: &RunRecord, _: &Relaxations) -> Result<(), String> {
        let node = env.rpc_node();
        let last_pre_spice = run.last_pre_spice();
        check_certified_via_fallback(&node, last_pre_spice.header())
            .map_err(|error| format!("the last pre-spice block: {error}"))?;
        let frontier = node.last_certified_block_header();
        let boundary_height = last_pre_spice.header().height();
        if frontier.height() <= boundary_height {
            return Err(format!(
                "the certified frontier is at height {}, not past the last pre-spice block at \
                 {boundary_height}: certification did not recover behind the fallback",
                frontier.height(),
            ));
        }
        Ok(())
    }
}

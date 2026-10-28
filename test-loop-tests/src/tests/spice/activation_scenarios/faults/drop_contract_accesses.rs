//! `drop_contract_accesses`: contract-accesses messages from producers never arrive.

use super::super::scenario::{Fault, FaultEvent, Progress, Timeline};
use super::render;
use crate::setup::env::TestLoopEnv;
use crate::setup::spice_partial_data_faults::SpicePartialDataObserved;
use parking_lot::Mutex;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

/// Every contract-accesses message dropped, so validators endorse only from what the
/// witness lists and fetch the code they lack with a code request. The contract-call load
/// always runs, so there are accesses to drop at every block around the boundary.
#[derive(Default)]
pub struct DropContractAccesses {
    observed: Option<Arc<Mutex<SpicePartialDataObserved>>>,
}

impl DropContractAccesses {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Debug for DropContractAccesses {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        render(f, self.tag(), &[])
    }
}

impl Fault for DropContractAccesses {
    fn tag(&self) -> &'static str {
        "drop_contract_accesses"
    }

    fn on_progress(
        &mut self,
        env: &mut TestLoopEnv,
        _timeline: &Timeline,
        progress: &Progress,
    ) -> Vec<FaultEvent> {
        if *progress == Progress::Built {
            let (faults, observed) = env.install_spice_partial_data_faults();
            faults
                .lock()
                .drop_contract_accesses_from
                .extend(env.node_datas.iter().map(|data| data.account_id.clone()));
            self.observed = Some(observed);
        }
        Vec::new()
    }

    fn on_node_restarted(&mut self, env: &mut TestLoopEnv, _index: usize) {
        env.install_spice_partial_data_faults();
    }

    fn assert_fired(&self, _env: &TestLoopEnv, _timeline: &Timeline) {
        let observed = self.observed.as_ref().expect("the faults are installed at built").lock();
        assert!(observed.dropped_contract_accesses > 0, "no contract-accesses message was dropped");
    }
}

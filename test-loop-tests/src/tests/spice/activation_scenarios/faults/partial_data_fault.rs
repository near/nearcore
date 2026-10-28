//! `partial_data_fault`: faults on the partial data a set of producers send.

use super::super::scenario::{
    Fault, FaultEvent, Narrowing, OracleId, Progress, Relaxation, Timeline,
};
use super::render;
use crate::setup::env::TestLoopEnv;
use crate::setup::spice_partial_data_faults::{
    SpiceDataKind, SpicePartialDataFaults, SpicePartialDataObserved,
};
use near_async::time::Duration;
use near_primitives::types::AccountId;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

/// What happens to the partial data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Drop,
    Delay,
    Corrupt,
    Equivocate,
}

/// Which producers' data is faulted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Producers {
    /// Every producer of each shard except one, so recovery has somewhere to pull from.
    AllButOne,
    /// The validator with this index in `node_datas`, which must produce chunks.
    Producer(usize),
    All,
}

/// Which kind of partial data the fault applies to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Only {
    Any,
    Witness,
    Receipts,
}

const DEFAULT_DELAY_MS: u64 = 2000;

/// Partial data dropped, delayed, corrupted or sent again under a conflicting commitment,
/// through `install_spice_partial_data_faults`. The faulted producers are the first spice
/// epoch's, read off the epoch info at `boundary_known`.
pub struct PartialDataFault {
    kind: Kind,
    from: Producers,
    only: Only,
    delay: Duration,
    faults: Option<Arc<Mutex<SpicePartialDataFaults>>>,
    observed: Option<Arc<Mutex<SpicePartialDataObserved>>>,
}

impl Default for PartialDataFault {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialDataFault {
    /// Partial data from all but one producer of each shard delayed past the request
    /// interval, so the recipients request while the push is in flight.
    pub fn new() -> Self {
        Self {
            kind: Kind::Delay,
            from: Producers::AllButOne,
            only: Only::Any,
            delay: Duration::milliseconds(DEFAULT_DELAY_MS as i64),
            faults: None,
            observed: None,
        }
    }

    pub fn kind(mut self, kind: Kind) -> Self {
        self.kind = kind;
        self
    }

    pub fn from(mut self, from: Producers) -> Self {
        self.from = from;
        self
    }

    pub fn only(mut self, only: Only) -> Self {
        self.only = only;
        self
    }

    /// Surface for the string form of milestone 4; no preset needs it.
    #[allow(dead_code)]
    pub fn delay_ms(mut self, delay_ms: u64) -> Self {
        self.delay = Duration::milliseconds(delay_ms as i64);
        self
    }

    /// The accounts whose partial data is faulted.
    fn faulted_producers(&self, env: &TestLoopEnv) -> HashSet<AccountId> {
        let node = env.rpc_node();
        let epoch_manager = node.client().epoch_manager.as_ref();
        let spice_epoch_id =
            epoch_manager.get_next_epoch_id_from_prev_block(&node.head().last_block_hash).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&spice_epoch_id).unwrap();
        let mut faulted = HashSet::new();
        for shard_id in shard_layout.shard_ids() {
            let producers = epoch_manager
                .get_epoch_chunk_producers_for_shard(&spice_epoch_id, shard_id)
                .unwrap();
            match self.from {
                Producers::AllButOne => faulted.extend(producers.into_iter().skip(1)),
                Producers::All => faulted.extend(producers),
                Producers::Producer(index) => {
                    let account_id = &env.node_datas[index].account_id;
                    if producers.contains(account_id) {
                        faulted.insert(account_id.clone());
                    }
                }
            }
        }
        assert!(
            !faulted.is_empty(),
            "{:?} names no chunk producer of the first spice epoch",
            self.from,
        );
        faulted
    }
}

impl Debug for PartialDataFault {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let kind = match self.kind {
            Kind::Delay => None,
            Kind::Drop => Some("drop"),
            Kind::Corrupt => Some("corrupt"),
            Kind::Equivocate => Some("equivocate"),
        };
        let from = match self.from {
            Producers::AllButOne => None,
            Producers::Producer(index) => Some(format!("producer({index})")),
            Producers::All => Some("all".to_string()),
        };
        let only = match self.only {
            Only::Any => None,
            Only::Witness => Some("witness"),
            Only::Receipts => Some("receipts"),
        };
        let delay_ms = self.delay.whole_milliseconds() as u64;
        render(
            f,
            self.tag(),
            &[
                ("kind", kind.map(str::to_string)),
                ("from", from),
                ("only", only.map(str::to_string)),
                ("delay_ms", (delay_ms != DEFAULT_DELAY_MS).then(|| delay_ms.to_string())),
            ],
        )
    }
}

impl Fault for PartialDataFault {
    fn tag(&self) -> &'static str {
        "partial_data_fault"
    }

    fn validate(&self) -> Result<(), String> {
        if self.from == Producers::All && matches!(self.kind, Kind::Drop | Kind::Corrupt) {
            return Err(format!(
                "{:?} from every producer leaves no copy of the data anywhere, so nothing \
                 certifies",
                self.kind,
            ));
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
                let (faults, observed) = env.install_spice_partial_data_faults();
                faults.lock().only_kind = match self.only {
                    Only::Any => None,
                    Only::Witness => Some(SpiceDataKind::Witness),
                    Only::Receipts => Some(SpiceDataKind::ReceiptProof),
                };
                self.faults = Some(faults);
                self.observed = Some(observed);
            }
            Progress::BoundaryKnown { .. } => {
                let producers = self.faulted_producers(env);
                let faults = self.faults.as_ref().expect("the faults are installed at built");
                let mut faults = faults.lock();
                match self.kind {
                    Kind::Drop => faults.drop_from.extend(producers),
                    Kind::Delay => {
                        faults.delay_from.extend(producers.into_iter().map(|p| (p, self.delay)))
                    }
                    Kind::Corrupt => faults.corrupt_from.extend(producers),
                    Kind::Equivocate => faults.equivocate_from.extend(producers),
                }
            }
            _ => {}
        }
        Vec::new()
    }

    fn on_node_restarted(&mut self, env: &mut TestLoopEnv, _index: usize) {
        // Re-running instruments the new node; the knobs are shared.
        env.install_spice_partial_data_faults();
    }

    fn relaxes(&self) -> Vec<Relaxation> {
        if self.kind == Kind::Drop {
            return vec![Relaxation {
                oracle: OracleId::WithinWindow,
                narrowing: Narrowing::Whole,
            }];
        }
        Vec::new()
    }

    fn assert_fired(&self, _env: &TestLoopEnv, _timeline: &Timeline) {
        let observed = self.observed.as_ref().expect("the faults are installed at built").lock();
        let (count, what) = match self.kind {
            Kind::Drop => (observed.dropped, "dropped"),
            Kind::Delay => (observed.delayed, "delayed"),
            Kind::Corrupt => (observed.corrupted, "corrupted"),
            Kind::Equivocate => (observed.equivocated, "sent again under a conflicting commitment"),
        };
        assert!(count > 0, "no partial data message was {what}");
    }
}

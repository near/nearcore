//! `bad_boundary_witness`: a chunk producer sends a tampered boundary witness.

use super::super::resolve::Resolved;
use super::super::scenario::{
    Fault, FaultEvent, Narrowing, OracleId, Progress, Relaxation, Timeline,
};
use super::render;
use crate::setup::env::TestLoopEnv;
use crate::setup::network_rules::RuleHandle;
use near_chain::spice::boundary::is_last_pre_spice_block;
use near_client::spice::chunk_executor_actor::AdvBoundaryWitnessMode;
use near_network::types::NetworkRequests;
use near_primitives::spice::partial_data::SpiceDataIdentifier;
use near_primitives::types::AccountId;
use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Producer 2: the first producer whose shard's other producer has a lower index, so
/// the honest witness is the one the validators decode first. Validators that decode a
/// tampered witness first never recover the honest one, see the design's findings, so
/// producers 0 and 1 are under investigation.
const DEFAULT_PRODUCER: usize = 2;
/// The receipts hash: the one mode that changes every boundary witness. The post state
/// and implicit transition modes need an implicit transition, which a witness carries
/// only when the boundary block lacks the shard's chunk.
const DEFAULT_MODE: AdvBoundaryWitnessMode = AdvBoundaryWitnessMode::WrongReceiptsHash;

/// The `test_features` knob of the chunk executor's boundary bootstrap, sent to
/// `producer`'s executor at `boundary_known`: every boundary witness the producer
/// distributes from then on is tampered with as `mode` says, in the spots the unit tests
/// of `boundary_chunk_validation.rs` tamper, before it is erasure-coded and pushed. The
/// producer still sends its receipts and endorses its own honest result, so the honest
/// result has an endorser, and the shard's other producer, where there is one, sends the
/// honest witness under a commitment of its own.
pub struct BadBoundaryWitness {
    producer: usize,
    mode: AdvBoundaryWitnessMode,
    account_id: Option<AccountId>,
    /// The producer's pushes of boundary witness parts, observed on its rule, and the
    /// recipients they were addressed to.
    observer: Option<RuleHandle>,
    recipients: Arc<AtomicU64>,
    /// Whether the producer is the only chunk producer of a shard it distributes the
    /// boundary witness for, read at `last_pre_spice`.
    only_producer: bool,
}

impl Default for BadBoundaryWitness {
    fn default() -> Self {
        Self::new()
    }
}

impl BadBoundaryWitness {
    /// Validator 2 with the receipts hash of its boundary witnesses replaced.
    pub fn new() -> Self {
        Self {
            producer: DEFAULT_PRODUCER,
            mode: DEFAULT_MODE,
            account_id: None,
            observer: None,
            recipients: Arc::default(),
            only_producer: false,
        }
    }

    /// The index among the validators; must be a chunk producer.
    pub fn producer(mut self, producer: usize) -> Self {
        self.producer = producer;
        self
    }

    pub fn mode(mut self, mode: AdvBoundaryWitnessMode) -> Self {
        self.mode = mode;
        self
    }

    /// Whether the mode changes a witness only when it carries an implicit transition,
    /// that is when the boundary block lacks the shard's chunk.
    pub fn needs_missing_boundary_chunk(&self) -> bool {
        matches!(
            self.mode,
            AdvBoundaryWitnessMode::WrongPostState
                | AdvBoundaryWitnessMode::MissingImplicitTransition
        )
    }

    fn account_id(&self) -> &AccountId {
        self.account_id.as_ref().expect("the producer is looked up at built")
    }
}

impl Debug for BadBoundaryWitness {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mode = match self.mode {
            AdvBoundaryWitnessMode::WrongReceiptsHash => None,
            AdvBoundaryWitnessMode::WrongPostState => Some("wrong_post_state"),
            AdvBoundaryWitnessMode::DropReceiptProof => Some("drop_receipt_proof"),
            AdvBoundaryWitnessMode::MissingImplicitTransition => {
                Some("missing_implicit_transition")
            }
        };
        render(
            f,
            self.tag(),
            &[
                (
                    "producer",
                    (self.producer != DEFAULT_PRODUCER).then(|| self.producer.to_string()),
                ),
                ("mode", mode.map(str::to_string)),
            ],
        )
    }
}

impl Fault for BadBoundaryWitness {
    fn tag(&self) -> &'static str {
        "bad_boundary_witness"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn validate(&self, resolved: Resolved) -> Result<(), String> {
        let profile = resolved.profile;
        if !profile.is_producer(self.producer) {
            return Err(format!(
                "node {} is not a chunk producer on profile {profile:?}, so it distributes no \
                 boundary witness; the producers are {:?}",
                self.producer,
                profile.producer_indices(),
            ));
        }
        Ok(())
    }

    fn on_progress(
        &mut self,
        env: &mut TestLoopEnv,
        timeline: &Timeline,
        progress: &Progress,
    ) -> Vec<FaultEvent> {
        match progress {
            Progress::Built => {
                self.account_id = Some(env.node_datas[self.producer].account_id.clone());
            }
            Progress::BoundaryKnown { .. } => {
                env.node_runner(self.producer).send_adversarial_boundary_witness(self.mode);
                // The producer's pushes of parts of a boundary witness: the witness it
                // distributes is the tampered one, so every recipient of a part got some
                // of it.
                let epoch_manager = env.node(self.producer).client().epoch_manager.clone();
                let matches = move |request: &NetworkRequests| {
                    let NetworkRequests::SpicePartialData { partial_data, .. } = request else {
                        return false;
                    };
                    let SpiceDataIdentifier::Witness { block_hash, .. } = partial_data.id() else {
                        return false;
                    };
                    is_last_pre_spice_block(epoch_manager.as_ref(), block_hash).unwrap_or(false)
                };
                let recipients = self.recipients.clone();
                let observe = move |request: &NetworkRequests| {
                    if let NetworkRequests::SpicePartialData { recipients: to, .. } = request {
                        recipients.fetch_add(to.len() as u64, Ordering::Relaxed);
                    }
                };
                // Ahead of the producer's other rules: a partial data fault delaying its
                // pushes would otherwise let them go out past this one (measured).
                self.observer = Some(env.observe_messages_from(self.producer, matches, observe));
            }
            Progress::LastPreSpice => {
                let last_pre_spice = timeline.last_pre_spice.as_ref().unwrap();
                let epoch_id = last_pre_spice.header().epoch_id();
                let epoch_manager = env.rpc_node().client().epoch_manager.clone();
                let shard_layout = epoch_manager.get_shard_layout(epoch_id).unwrap();
                self.only_producer = shard_layout.shard_ids().any(|shard_id| {
                    epoch_manager
                        .get_epoch_chunk_producers_for_shard(epoch_id, shard_id)
                        .is_ok_and(|producers| producers == vec![self.account_id().clone()])
                });
            }
            _ => {}
        }
        Vec::new()
    }

    /// When the producer is the only chunk producer of a shard, no honest witness of
    /// that shard's boundary chunk goes out and its validators wait for the fallback.
    fn relaxes(&self) -> Vec<Relaxation> {
        if !self.only_producer {
            return Vec::new();
        }
        vec![Relaxation { oracle: OracleId::WithinWindow, narrowing: Narrowing::Whole }]
    }

    /// The executor acknowledged the mode and changed at least one boundary witness, and
    /// parts of it were pushed to at least one validator.
    fn assert_fired(&self, env: &TestLoopEnv, _timeline: &Timeline) {
        let tampered =
            env.node(self.producer).chunk_executor().adversarial_boundary_witnesses_tampered();
        assert!(
            tampered > 0,
            "the chunk executor of node {} changed no boundary witness under {:?}{}",
            self.producer,
            self.mode,
            if self.needs_missing_boundary_chunk() {
                "; the mode needs an implicit transition, which only a boundary block lacking \
                 the shard's chunk gives"
            } else {
                ""
            },
        );
        let observer = self.observer.as_ref().expect("the observer is installed at boundary_known");
        let recipients = self.recipients.load(Ordering::Relaxed);
        assert!(
            observer.matched() > 0 && recipients > 0,
            "node {} pushed no part of a boundary witness to anyone",
            self.producer,
        );
        tracing::info!(
            target: "test",
            producer = self.producer,
            mode = ?self.mode,
            tampered,
            pushes = observer.matched(),
            recipients,
            only_producer = self.only_producer,
            "bad_boundary_witness tampered with the producer's boundary witnesses",
        );
    }
}

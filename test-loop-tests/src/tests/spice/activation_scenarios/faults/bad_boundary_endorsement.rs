//! `bad_boundary_endorsement`: a validator endorses a wrong result for a boundary chunk.

use super::super::resolve::Resolved;
use super::super::scenario::{
    Fault, FaultEvent, Narrowing, OracleId, Progress, Relaxation, Timeline,
};
use super::{render, shard_layout_at_head};
use crate::setup::env::TestLoopEnv;
use crate::setup::network_rules::{Action, RuleHandle};
use near_chain::spice::boundary::is_last_pre_spice_block;
use near_network::types::NetworkRequests;
use near_primitives::hash::CryptoHash;
use near_primitives::spice::chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{AccountId, ChunkExecutionResult, ShardId};
use std::any::Any;
use std::fmt::{self, Debug, Formatter};

const DEFAULT_VALIDATOR: usize = 4;
const DEFAULT_SHARD: usize = 0;

/// A rule on `validator`'s account from `boundary_known` rewrites its endorsement of
/// `shard`'s chunk of the last pre-spice block, found by the predicate on the endorsed
/// block, which the validator has processed: the execution result's outgoing receipts root
/// is replaced and the endorsement re-signed with the validator's test signer, so the
/// signature holds and the fault is attributable to the validator. The validator's own
/// store keeps the honest endorsement. Honest nodes count the rewritten one toward a result
/// nobody else endorsed, and may carry it on chain, which is what an equivocating
/// validator's endorsement does.
pub struct BadBoundaryEndorsement {
    validator: usize,
    shard: usize,
    account_id: Option<AccountId>,
    shard_id: Option<ShardId>,
    rule: Option<RuleHandle>,
    /// Whether the validator is designated for the chunk, read at `last_pre_spice`.
    designated: bool,
}

impl Default for BadBoundaryEndorsement {
    fn default() -> Self {
        Self::new()
    }
}

impl BadBoundaryEndorsement {
    /// The first chunk-only validator of profile A, on shard 0's boundary chunk.
    pub fn new() -> Self {
        Self {
            validator: DEFAULT_VALIDATOR,
            shard: DEFAULT_SHARD,
            account_id: None,
            shard_id: None,
            rule: None,
            designated: false,
        }
    }

    /// The index among the validators.
    pub fn validator(mut self, validator: usize) -> Self {
        self.validator = validator;
        self
    }

    pub fn shard(mut self, shard: usize) -> Self {
        self.shard = shard;
        self
    }
}

impl Debug for BadBoundaryEndorsement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        render(
            f,
            self.tag(),
            &[
                (
                    "validator",
                    (self.validator != DEFAULT_VALIDATOR).then(|| self.validator.to_string()),
                ),
                ("shard", (self.shard != DEFAULT_SHARD).then(|| self.shard.to_string())),
            ],
        )
    }
}

/// `result` with another outgoing receipts root: the same chunk, a different claim.
fn with_wrong_receipts_root(result: &ChunkExecutionResult) -> ChunkExecutionResult {
    ChunkExecutionResult {
        chunk_extra: result.chunk_extra.clone(),
        outgoing_receipts_root: CryptoHash::hash_borsh(&result.outgoing_receipts_root),
    }
}

impl Fault for BadBoundaryEndorsement {
    fn tag(&self) -> &'static str {
        "bad_boundary_endorsement"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn validate(&self, resolved: Resolved) -> Result<(), String> {
        if self.validator >= resolved.profile.num_validators() {
            return Err(format!(
                "node {} is not a validator; the driver and the clients the faults add endorse \
                 nothing",
                self.validator
            ));
        }
        if self.shard >= resolved.profile.num_shards() {
            return Err(format!(
                "shard index {} is out of range: {} shards",
                self.shard,
                resolved.profile.num_shards()
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
                self.account_id = Some(env.node_datas[self.validator].account_id.clone());
                self.shard_id = Some(shard_layout_at_head(env).get_shard_id(self.shard).unwrap());
            }
            Progress::BoundaryKnown { .. } => {
                let shard_id = self.shard_id.unwrap();
                let epoch_manager = env.node(self.validator).client().epoch_manager.clone();
                let matches = move |request: &NetworkRequests| {
                    let NetworkRequests::SpiceChunkEndorsement(_, endorsement) = request else {
                        return false;
                    };
                    endorsement.shard_id() == shard_id
                        && is_last_pre_spice_block(epoch_manager.as_ref(), endorsement.block_hash())
                            .unwrap_or(false)
                };
                let signer = create_test_signer(self.account_id.as_ref().unwrap().as_str());
                let rewrite = move |request: NetworkRequests| {
                    let NetworkRequests::SpiceChunkEndorsement(target, endorsement) = request
                    else {
                        unreachable!("the rule matches spice endorsements only");
                    };
                    let verified = endorsement
                        .into_verified(&[signer.public_key()])
                        .expect("test-loop nodes sign with create_test_signer keys");
                    let rewritten = SpiceChunkEndorsement::new(
                        verified.chunk_id().clone(),
                        with_wrong_receipts_root(verified.execution_result()),
                        &signer,
                    );
                    vec![NetworkRequests::SpiceChunkEndorsement(target, rewritten)]
                };
                self.rule = Some(env.network_rule(
                    self.validator,
                    matches,
                    Action::Replace(Box::new(rewrite)),
                ));
            }
            Progress::LastPreSpice => {
                let last_pre_spice = timeline.last_pre_spice.as_ref().unwrap();
                let header = last_pre_spice.header();
                let node = env.rpc_node();
                self.designated = node
                    .client()
                    .epoch_manager
                    .get_chunk_validator_assignments(
                        header.epoch_id(),
                        self.shard_id.unwrap(),
                        header.height(),
                    )
                    .is_ok_and(|assignments| {
                        assignments.contains(self.account_id.as_ref().unwrap())
                    });
            }
            _ => {}
        }
        Vec::new()
    }

    /// The validator's endorsement of the chunk is wrong by construction, so it is no
    /// stateless endorser of the certified result; and the chunk loses one designated
    /// endorsement when the validator is designated for it.
    fn relaxes(&self) -> Vec<Relaxation> {
        let mut relaxations = vec![Relaxation {
            oracle: OracleId::StatelessEndorser,
            narrowing: Narrowing::Node(self.validator),
        }];
        if self.designated {
            relaxations
                .push(Relaxation { oracle: OracleId::WithinWindow, narrowing: Narrowing::Whole });
        }
        relaxations
    }

    /// The rewritten endorsement was sent.
    fn assert_fired(&self, _env: &TestLoopEnv, _timeline: &Timeline) {
        let rule = self.rule.as_ref().expect("the rule was never installed");
        assert!(
            rule.matched() > 0,
            "node {} sent no endorsement of shard {}'s boundary chunk to rewrite",
            self.validator,
            self.shard,
        );
        tracing::info!(
            target: "test",
            validator = self.validator,
            shard = self.shard,
            rewritten = rule.matched(),
            designated = self.designated,
            "bad_boundary_endorsement rewrote the validator's endorsements",
        );
    }
}

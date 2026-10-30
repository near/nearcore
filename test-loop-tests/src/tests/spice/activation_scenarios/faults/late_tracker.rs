//! `late_tracker`: validators start tracking a shard exactly at activation.

use super::super::driver::RunRecord;
use super::super::oracles::{Oracle, Relaxations};
use super::super::resolve::Profile;
use super::super::scenario::{Fault, FaultEvent, OracleId, Progress, Timeline};
use super::render;
use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain::spice::all_stake_fallback::SPICE_FALLBACK_CERTIFICATION_DELAY;
use near_chain_configs::TrackedShardsConfig;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::types::{AccountId, NumShards, ShardId, SpiceChunkId};
use std::any::Any;
use std::collections::HashSet;
use std::fmt::{self, Debug, Formatter};

const DEFAULT_SHARD: usize = 1;
const DEFAULT_CATCHUP_PERIOD_S: i64 = 5;

/// Every chunk-only validator of the profile starts tracking `shard` at the activation
/// epoch, through a tracking schedule indexed by epoch height, with catch-up too slow to
/// land the last pre-spice block before its boundary bootstrap runs. The producers hold
/// 59.5% of the stake on profile A, so the boundary chunk cannot certify without the late
/// trackers' endorsements, which they can only give by validating the boundary witness.
pub struct LateTracker {
    shard: usize,
    catchup_period: Duration,
    shard_id: Option<ShardId>,
    late: Vec<usize>,
    /// Per late tracker, whether it processed the last pre-spice block without a chunk
    /// extra for the shard; `None` until its head got there.
    processed_without_apply: Vec<Option<bool>>,
    /// Whether every late tracker's boundary endorsement was on chain while none of them
    /// had applied the shard yet; `None` until the endorsements all landed.
    endorsed_before_catchup: Option<bool>,
}

impl Default for LateTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl LateTracker {
    pub fn new() -> Self {
        Self {
            shard: DEFAULT_SHARD,
            catchup_period: Duration::seconds(DEFAULT_CATCHUP_PERIOD_S),
            shard_id: None,
            late: Vec::new(),
            processed_without_apply: Vec::new(),
            endorsed_before_catchup: None,
        }
    }

    /// No preset needs it yet.
    #[allow(dead_code)]
    pub fn shard(mut self, shard_index: usize) -> Self {
        self.shard = shard_index;
        self
    }

    /// No preset needs it yet.
    #[allow(dead_code)]
    pub fn catchup_period_s(mut self, seconds: i64) -> Self {
        self.catchup_period = Duration::seconds(seconds);
        self
    }

    fn shard_id(&self) -> ShardId {
        self.shard_id.expect("the shard id is known from the layout")
    }

    fn late_accounts(&self, env: &TestLoopEnv) -> HashSet<AccountId> {
        self.late.iter().map(|index| env.node_datas[*index].account_id.clone()).collect()
    }

    fn late_shard_uid(&self, env: &TestLoopEnv, timeline: &Timeline) -> ShardUId {
        let last_pre_spice = timeline.last_pre_spice.as_ref().unwrap();
        let shard_layout = env
            .rpc_node()
            .client()
            .epoch_manager
            .get_shard_layout(last_pre_spice.header().epoch_id())
            .unwrap();
        ShardUId::from_shard_id_and_layout(self.shard_id(), &shard_layout)
    }
}

impl Debug for LateTracker {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let seconds = self.catchup_period.whole_seconds();
        render(
            f,
            self.tag(),
            &[
                ("shard", (self.shard != DEFAULT_SHARD).then(|| self.shard.to_string())),
                (
                    "catchup_period_s",
                    (seconds != DEFAULT_CATCHUP_PERIOD_S).then(|| seconds.to_string()),
                ),
            ],
        )
    }
}

impl Fault for LateTracker {
    fn tag(&self) -> &'static str {
        "late_tracker"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    /// On profile B the designated sets are sampled, so the late trackers are usually not
    /// designated for the boundary chunk and the precondition cannot hold.
    fn validate(&self, profile: Profile) -> Result<(), String> {
        if profile != Profile::A {
            return Err(format!(
                "runs on profile A only: on profile {profile:?} the late trackers are usually \
                 not designated for the boundary chunk, so the precondition cannot hold"
            ));
        }
        Ok(())
    }

    fn setup(&self, profile: Profile, builder: TestLoopBuilder) -> TestLoopBuilder {
        // The builder's layout for the profile's shard count, see `num_shards`.
        let shard_layout = ShardLayout::multi_shard(profile.num_shards() as NumShards, 1);
        let shard_id = shard_layout.get_shard_id(self.shard).unwrap();
        let activation_epoch_height = profile.activation_epoch_height();
        let late =
            profile.num_producers()..profile.num_producers() + profile.num_chunk_validators();
        // Indexed by epoch height: nothing tracked before activation, the shard from then on.
        let schedule: Vec<Vec<ShardId>> = (0..20)
            .map(
                |epoch_height| {
                    if epoch_height < activation_epoch_height { vec![] } else { vec![shard_id] }
                },
            )
            .collect();
        let catchup_period = self.catchup_period;
        builder.config_modifier(move |config, index| {
            // Catch-up advances one step per period: state sync takes a few steps and then
            // each block of the epoch takes one. Steps this far apart keep the late trackers
            // catching up well past the epoch's last block.
            config.catchup_step_period = catchup_period;
            if late.contains(&index) {
                config.tracked_shards_config = TrackedShardsConfig::Schedule(schedule.clone());
            }
        })
    }

    fn on_progress(
        &mut self,
        env: &mut TestLoopEnv,
        timeline: &Timeline,
        progress: &Progress,
    ) -> Vec<FaultEvent> {
        match progress {
            Progress::Built => {
                let profile = Profile::A;
                self.late = (profile.num_producers()
                    ..profile.num_producers() + profile.num_chunk_validators())
                    .collect();
                self.processed_without_apply = vec![None; self.late.len()];
                let node = env.rpc_node();
                let shard_layout =
                    node.client().epoch_manager.get_shard_layout(&node.head().epoch_id).unwrap();
                self.shard_id = Some(shard_layout.get_shard_id(self.shard).unwrap());
            }
            Progress::BoundaryKnown { estimate } => {
                // The late trackers' endorsements of the boundary chunk must be needed for
                // it to certify through its designated validators, or their loss goes
                // unnoticed.
                let node = env.rpc_node();
                let designated = node
                    .client()
                    .epoch_manager
                    .get_chunk_validator_assignments(
                        &node.head().epoch_id,
                        self.shard_id(),
                        estimate - 1,
                    )
                    .unwrap();
                let late = self.late_accounts(env);
                for account_id in &late {
                    assert!(
                        designated.contains(account_id),
                        "precondition: {account_id} must validate the boundary chunk of shard \
                         {}",
                        self.shard_id(),
                    );
                }
                let others: HashSet<AccountId> = designated
                    .assignments()
                    .iter()
                    .map(|(account_id, _)| account_id.clone())
                    .filter(|account_id| !late.contains(account_id))
                    .collect();
                assert!(
                    !designated.is_endorsed(&others),
                    "precondition: the boundary chunk of shard {} certifies without the late \
                     trackers' endorsements, so the run would exercise nothing",
                    self.shard_id(),
                );
            }
            Progress::Height { .. } if timeline.last_pre_spice.is_some() => {
                let last_pre_spice = timeline.last_pre_spice.as_ref().unwrap();
                let boundary_height = last_pre_spice.header().height();
                let shard_uid = self.late_shard_uid(env, timeline);
                let applied = |index: usize| {
                    env.node(index)
                        .client()
                        .chain
                        .chain_store
                        .get_chunk_extra(last_pre_spice.hash(), &shard_uid)
                        .is_ok()
                };
                // Each late tracker processes the last pre-spice block without applying the
                // shard it starts tracking; that apply is still ahead of it, in catch-up.
                for (position, index) in self.late.iter().enumerate() {
                    if self.processed_without_apply[position].is_none()
                        && env.node(*index).head().height >= boundary_height
                    {
                        self.processed_without_apply[position] = Some(!applied(*index));
                    }
                }
                // Their endorsements land while their catch-up has still not applied it.
                if self.endorsed_before_catchup.is_none() {
                    let core_reader = &env.rpc_node().client().chain.spice_core_reader;
                    let all_endorsed = self.late_accounts(env).iter().all(|account_id| {
                        core_reader
                            .get_endorsement(last_pre_spice.hash(), self.shard_id(), account_id)
                            .is_some()
                    });
                    if all_endorsed {
                        self.endorsed_before_catchup =
                            Some(self.late.iter().all(|index| !applied(*index)));
                    }
                }
            }
            _ => {}
        }
        Vec::new()
    }

    /// The late trackers apply the spice chunks of the shard they now track themselves,
    /// one slow catch-up step per block, and certification waits on their endorsements,
    /// so the spice epochs stretch far beyond their length.
    fn extra_epochs(&self) -> u64 {
        12
    }

    fn extra_oracles(&self) -> Vec<Box<dyn Oracle>> {
        vec![
            Box::new(EndorsedBeforeCatchup { observed: self.endorsed_before_catchup }),
            Box::new(CertifiedThroughDesignated {
                shard_id: self.shard_id(),
                late: self.late.clone(),
            }),
        ]
    }

    fn assert_fired(&self, env: &TestLoopEnv, _timeline: &Timeline) {
        for (position, index) in self.late.iter().enumerate() {
            let account_id = &env.node_datas[*index].account_id;
            match self.processed_without_apply[position] {
                None => panic!("{account_id}'s head never reached the last pre-spice block"),
                Some(false) => panic!(
                    "{account_id} had applied shard {} of the last pre-spice block on processing \
                     it; the catch-up is not slow enough for the fault to show anything",
                    self.shard_id(),
                ),
                Some(true) => {}
            }
        }
    }
}

/// Every late tracker's boundary endorsement was on chain while none of them had applied
/// the shard yet, so the endorsements came from validating the witness.
struct EndorsedBeforeCatchup {
    observed: Option<bool>,
}

impl Oracle for EndorsedBeforeCatchup {
    fn id(&self) -> OracleId {
        OracleId::EndorsedBeforeCatchup
    }

    fn check(&self, _: &TestLoopEnv, _: &RunRecord, _: &Relaxations) -> Result<(), String> {
        match self.observed {
            Some(true) => Ok(()),
            Some(false) => Err("a late tracker caught up on the shard before every late \
                               tracker's boundary endorsement landed; the endorsements did \
                               not all come from validating the witness"
                .to_string()),
            None => Err("the late trackers' boundary endorsements never all landed".to_string()),
        }
    }
}

/// The boundary chunk certified through its designated validators, inside the fallback
/// window, with a late tracker's endorsement on chain.
struct CertifiedThroughDesignated {
    shard_id: ShardId,
    late: Vec<usize>,
}

impl Oracle for CertifiedThroughDesignated {
    fn id(&self) -> OracleId {
        OracleId::CertifiedThroughDesignated
    }

    fn check(
        &self,
        env: &TestLoopEnv,
        run: &RunRecord,
        _relaxations: &Relaxations,
    ) -> Result<(), String> {
        let last_pre_spice = run.last_pre_spice();
        let boundary_height = last_pre_spice.header().height();
        let chunk_id = SpiceChunkId { block_hash: *last_pre_spice.hash(), shard_id: self.shard_id };
        let (certifying_height, endorsers) =
            env.rpc_node().certifying_height_and_endorsers(&chunk_id).ok_or_else(|| {
                format!("the boundary chunk of shard {} never certified", self.shard_id)
            })?;
        if certifying_height >= boundary_height + SPICE_FALLBACK_CERTIFICATION_DELAY {
            return Err(format!(
                "the boundary chunk of shard {} certified at height {certifying_height}, with \
                 the fallback open",
                self.shard_id,
            ));
        }
        let late: HashSet<AccountId> =
            self.late.iter().map(|index| env.node_datas[*index].account_id.clone()).collect();
        if late.is_disjoint(&endorsers) {
            return Err(format!(
                "the boundary chunk of shard {} certified with no late tracker's endorsement on \
                 chain: {endorsers:?}",
                self.shard_id,
            ));
        }
        Ok(())
    }
}

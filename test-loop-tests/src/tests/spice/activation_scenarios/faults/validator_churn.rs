//! `validator_churn`: the validator set changes exactly at activation.

use super::super::driver::RunRecord;
use super::super::oracles::{Oracle, Relaxations};
use super::super::resolve::Resolved;
use super::super::scenario::{Fault, FaultEvent, OracleId, Progress, Timeline};
use super::render;
use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::{create_account_id, create_validator_id};
use near_chain::ChainStoreAccess;
use near_chain::spice::boundary::shards_applied_itself;
use near_chain_configs::TrackedShardsConfig;
use near_primitives::action::{Action, StakeAction};
use near_primitives::shard_layout::ShardUId;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{AccountId, Balance, EpochId, ShardId};
use std::any::Any;
use std::fmt::{self, Debug, Formatter};

/// Who changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Join,
    Leave,
    Both,
}

/// One validator joins in the first spice epoch and one leaves after the last pre-spice
/// epoch, through stake transactions in the genesis epoch, which decide the validator set
/// of the epoch after next. The joiner is a non-validator client the fault adds, tracking
/// no shard until it produces; it catches up on its spice shards through state sync in
/// the last pre-spice epoch and runs no boundary step for them, since the bootstrap treats
/// a shard it only started tracking this epoch as not caught up.
pub struct ValidatorChurn {
    kind: Kind,
    joiner: AccountId,
    leaver: AccountId,
    /// The joiner's check at the boundary, once it ran.
    joiner_check: Option<Result<(), String>>,
}

impl Default for ValidatorChurn {
    fn default() -> Self {
        Self::new()
    }
}

impl ValidatorChurn {
    pub fn new() -> Self {
        Self {
            kind: Kind::Both,
            joiner: create_account_id("joiner"),
            // A block producer on profile A and a chunk-only validator on B; any validator
            // can leave.
            leaver: create_validator_id(3),
            joiner_check: None,
        }
    }

    pub fn kind(mut self, kind: Kind) -> Self {
        self.kind = kind;
        self
    }

    fn joins(&self) -> bool {
        self.kind != Kind::Leave
    }

    fn leaves(&self) -> bool {
        self.kind != Kind::Join
    }

    /// The joiner must hold the last pre-spice block's state for the shards it produces for
    /// under spice, caught up through state sync, and still run no boundary step for them.
    fn check_joiner_at_boundary(
        &self,
        env: &mut TestLoopEnv,
        timeline: &Timeline,
    ) -> Result<(), String> {
        let last_pre_spice = timeline.last_pre_spice.as_ref().unwrap();
        let boundary_height = last_pre_spice.header().height();
        env.runner_for_account(&self.joiner).run_until_head_height(boundary_height);
        let node = env.node_for_account(&self.joiner);
        let client = node.client();
        let epoch_manager = client.epoch_manager.as_ref();
        let spice_epoch_id =
            epoch_manager.get_epoch_id_from_prev_block(last_pre_spice.hash()).unwrap();
        let shard_layout =
            epoch_manager.get_shard_layout(last_pre_spice.header().epoch_id()).unwrap();
        let joiner_shards: Vec<ShardId> = shard_layout
            .shard_ids()
            .filter(|shard_id| {
                epoch_manager
                    .get_epoch_chunk_producers_for_shard(&spice_epoch_id, *shard_id)
                    .unwrap()
                    .contains(&self.joiner)
            })
            .collect();
        if joiner_shards.is_empty() {
            return Err("the joiner produces no chunk under spice".to_string());
        }
        for shard_id in joiner_shards {
            let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
            if client.chain.chain_store.get_chunk_extra(last_pre_spice.hash(), &shard_uid).is_err()
            {
                return Err(format!(
                    "the joiner has not caught up on {shard_uid} by the last pre-spice block"
                ));
            }
        }
        let applied_itself =
            shards_applied_itself(&client.shard_tracker, epoch_manager, last_pre_spice.header())
                .map_err(|err| format!("{err}"))?;
        if !applied_itself.is_empty() {
            return Err(format!(
                "the joiner runs the boundary step for {applied_itself:?} instead of skipping it"
            ));
        }
        Ok(())
    }
}

impl Debug for ValidatorChurn {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let kind = match self.kind {
            Kind::Both => None,
            Kind::Join => Some("join".to_string()),
            Kind::Leave => Some("leave".to_string()),
        };
        render(f, self.tag(), &[("kind", kind)])
    }
}

impl Fault for ValidatorChurn {
    fn tag(&self) -> &'static str {
        "validator_churn"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn setup(&self, resolved: Resolved, mut builder: TestLoopBuilder) -> TestLoopBuilder {
        let profile = resolved.profile;
        if self.joins() {
            // The joiner is the client right after the validators, or after the archival node
            // that the builder puts there.
            let joiner_index = profile.num_validators() + usize::from(resolved.archival_node);
            builder = builder
                .add_non_validator_client(&self.joiner)
                .add_user_account(&self.joiner, Balance::from_near(20_000))
                .config_modifier(move |config, index| {
                    if index == joiner_index {
                        config.tracked_shards_config = TrackedShardsConfig::NoShards;
                    }
                });
        }
        if self.leaves() {
            // The leaver's account needs a user access key to sign its unstake.
            builder = builder.add_user_account(&self.leaver, Balance::from_near(1_000));
        }
        builder
    }

    fn on_progress(
        &mut self,
        env: &mut TestLoopEnv,
        timeline: &Timeline,
        progress: &Progress,
    ) -> Vec<FaultEvent> {
        match progress {
            Progress::Built if self.joins() => {
                assert!(
                    env.node_datas.iter().any(|data| data.account_id == self.joiner),
                    "the joiner must be a client of the environment",
                );
            }
            Progress::WarmedUp => {
                // Stake changes in the genesis epoch define the validator set of the epoch
                // after next, which under the immediate vote is the first spice epoch.
                let node = env.rpc_node();
                let leaver_stake = node
                    .client()
                    .epoch_manager
                    .get_epoch_all_validators(&node.head().epoch_id)
                    .unwrap()
                    .into_iter()
                    .find(|stake| stake.account_id() == &self.leaver)
                    .expect("the leaver validates at genesis")
                    .stake();
                let stake_tx = |account_id: &AccountId, stake: Balance| {
                    let public_key = create_test_signer(account_id.as_str()).public_key();
                    node.tx_from_actions(
                        account_id,
                        account_id,
                        vec![Action::Stake(Box::new(StakeAction { stake, public_key }))],
                    )
                };
                let mut txs = Vec::new();
                if self.joins() {
                    txs.push(stake_tx(&self.joiner, leaver_stake));
                }
                if self.leaves() {
                    txs.push(stake_tx(&self.leaver, Balance::ZERO));
                }
                for tx in txs {
                    node.submit_tx(tx);
                }
            }
            Progress::LastPreSpice if self.joins() => {
                self.joiner_check = Some(self.check_joiner_at_boundary(env, timeline));
            }
            _ => {}
        }
        Vec::new()
    }

    fn extra_oracles(&self) -> Vec<Box<dyn Oracle>> {
        if !self.joins() {
            return Vec::new();
        }
        vec![Box::new(JoinerSkipsBoundaryStep { observed: self.joiner_check.clone() })]
    }

    /// The change must land exactly at activation, or the fault shows nothing.
    fn assert_fired(&self, env: &TestLoopEnv, timeline: &Timeline) {
        let last_pre_spice = timeline.last_pre_spice.as_ref().unwrap();
        let node = env.rpc_node();
        let epoch_manager = node.client().epoch_manager.as_ref();
        let pre_spice_epoch_id = last_pre_spice.header().epoch_id();
        let spice_epoch_id =
            epoch_manager.get_epoch_id_from_prev_block(last_pre_spice.hash()).unwrap();
        let validates_in = |epoch_id: &EpochId, account_id: &AccountId| {
            epoch_manager
                .get_epoch_all_validators(epoch_id)
                .unwrap()
                .iter()
                .any(|stake| stake.account_id() == account_id)
        };
        if self.joins() {
            assert!(
                !validates_in(pre_spice_epoch_id, &self.joiner),
                "the joiner validates in the last pre-spice epoch already"
            );
            assert!(
                validates_in(&spice_epoch_id, &self.joiner),
                "the joiner does not validate under spice"
            );
        }
        if self.leaves() {
            assert!(
                validates_in(pre_spice_epoch_id, &self.leaver),
                "the leaver does not validate in the last pre-spice epoch"
            );
            assert!(
                !validates_in(&spice_epoch_id, &self.leaver),
                "the leaver still validates under spice"
            );
        }
    }
}

/// The joiner held the boundary state of its spice shards and ran no boundary step.
struct JoinerSkipsBoundaryStep {
    observed: Option<Result<(), String>>,
}

impl Oracle for JoinerSkipsBoundaryStep {
    fn id(&self) -> OracleId {
        OracleId::JoinerSkipsBoundaryStep
    }

    fn check(&self, _: &TestLoopEnv, _: &RunRecord, _: &Relaxations) -> Result<(), String> {
        match &self.observed {
            Some(result) => result.clone(),
            None => Err("the joiner was never checked at the boundary".to_string()),
        }
    }
}

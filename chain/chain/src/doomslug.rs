use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::{Duration, Instant};

use near_primitives::block::Approval;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, Balance, BlockHeight, BlockHeightDelta, ValidatorStake};
use near_primitives::validator_signer::ValidatorSigner;

/// Have that many iterations in the timer instead of `loop` to prevent potential bugs from blocking
/// the node
const MAX_TIMER_ITERS: usize = 20;

/// How many heights ahead to track approvals. This needs to be sufficiently large so that we can
/// recover after rather long network interruption, but not too large to consume too much memory if
/// someone in the network spams with invalid approvals. Note that we will only store approvals for
/// heights that are targeting us, which is once per as many heights as there are block producers,
/// thus 10_000 heights in practice will mean on the order of one hundred entries.
const MAX_HEIGHTS_AHEAD_TO_STORE_APPROVALS: BlockHeight = 10_000;

/// The threshold for doomslug to create a block.
/// `HalfStake` means the block can only be produced if at least half of the stake is approvign it,
///             and is what should be used in production (and what guarantees doomslug finality)
/// `NoApprovals` means the block production is not blocked on approvals. This is used
///             in many tests (e.g. `cross_shard_tx`) to create lots of forkfulness.
#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum DoomslugThresholdMode {
    NoApprovals,
    HalfStake,
}

/// The result of processing an approval.
#[derive(PartialEq, Eq, Debug)]
pub enum DoomslugBlockProductionReadiness {
    None,
    /// after processing this approval the block has passed the threshold set by
    /// `threshold_mode` (either one half of the total stake, or a single approval).
    /// Once the threshold is hit, we wait for `T(h - h_final) / 6` before producing
    /// a block
    PassedThreshold(Instant),
    /// after processing this approval the block can be produced without waiting.
    /// We produce a block without further waiting if it has 2/3 of approvals AND has
    /// doomslug finality on the previous block. We still return the time when the
    /// height crossed the threshold -- the caller might still want to sleep, e.g.
    /// if they don't have enough chunks
    ReadyToProduce(Instant),
}

struct DoomslugTimer {
    started: Instant,
    height: BlockHeight,
    endorsement_delay: Duration,
    min_delay: Duration,
    delay_step: Duration,
    max_delay: Duration,
}

struct DoomslugTip {
    block_hash: CryptoHash,
    reference_hash: Option<CryptoHash>,
    height: BlockHeight,
}

struct DoomslugApprovalsTracker {
    witness: HashMap<AccountId, Approval>,
    account_id_to_stake: HashMap<AccountId, Balance>,
    total_stake: Balance,
    approved_stake: Balance,
    endorsed_stake: Balance,
    time_passed_threshold: Option<Instant>,
    threshold_mode: DoomslugThresholdMode,
}

/// Approvals can arrive before the corresponding blocks, and we need a meaningful way to keep as
/// many approvals as possible that can be useful in the future, while not allowing an adversary
/// to spam us with invalid approvals.
/// To that extent, for each `account_id` and each `target_height` we keep exactly one approval,
/// whichever came last. We only maintain those for
///  a) `account_id`s that match the corresponding epoch (and for which we can validate a signature)
///  b) `target_height`s for which we produce blocks
///  c) `target_height`s within a meaningful horizon from the current tip.
/// This class is responsible for maintaining witnesses for the blocks, while also ensuring that
/// only one approval per (`account_id`) is kept. We instantiate one such class per height, thus
/// ensuring that only one approval is kept per (`target_height`, `account_id`). `Doomslug` below
/// ensures that only instances within the horizon are kept, and the user of the `Doomslug` is
/// responsible for ensuring that only approvals for proper account_ids with valid signatures are
/// provided.
struct DoomslugApprovalsTrackersAtHeight {
    approval_trackers: HashMap<CryptoHash, DoomslugApprovalsTracker>,
    last_approval_per_account: HashMap<AccountId, CryptoHash>,
}

/// Contains all the logic for Doomslug, but no integration with chain or storage. The integration
/// happens via `PersistentDoomslug` struct. The split is to simplify testing of the logic separate
/// from the chain.
pub struct Doomslug {
    approval_tracking: HashMap<BlockHeight, DoomslugApprovalsTrackersAtHeight>,
    /// Largest height that we promised to skip
    largest_promised_skip_height: BlockHeight,
    /// Largest height that we endorsed
    largest_endorsed_height: BlockHeight,
    /// Largest height for which we saw a block containing 1/2 endorsements in it
    largest_ds_final_height: BlockHeight,
    /// Largest height for which we saw threshold approvals (and thus can potentially create a block)
    largest_threshold_height: BlockHeight,
    /// Information Doomslug tracks about the chain tip
    tip: DoomslugTip,
    /// Whether an endorsement (or in general an approval) was sent since updating the tip
    endorsement_pending: bool,
    /// Information to track the timer (see `start_timer` routine in the paper)
    timer: DoomslugTimer,
    signer: Option<Arc<dyn ValidatorSigner>>,
    /// How many approvals to have before producing a block. In production should be always `HalfStake`,
    ///    but for many tests we use `NoApprovals` to invoke more forkfulness
    threshold_mode: DoomslugThresholdMode,
}

impl DoomslugTimer {
    /// Computes the delay to sleep given the number of heights from the last block with doomslug
    /// finality. This is what `T` represents in the paper.
    ///
    /// # Arguments
    /// * `n` - number of heights since the last block with doomslug finality
    ///
    /// # Returns
    /// Duration to sleep
    pub fn get_delay(&self, n: BlockHeightDelta) -> Duration {
        let n32 = u32::try_from(n).unwrap_or(std::u32::MAX);
        std::cmp::min(self.max_delay, self.min_delay + self.delay_step * n32.saturating_sub(1))
    }
}

impl DoomslugApprovalsTracker {
    fn new(stakes: &Vec<ValidatorStake>, threshold_mode: DoomslugThresholdMode) -> Self {
        let account_id_to_stake =
            stakes.iter().map(|x| (x.account_id.clone(), x.stake)).collect::<HashMap<_, _>>();
        assert!(account_id_to_stake.len() == stakes.len());
        let total_stake = account_id_to_stake.values().sum::<Balance>();

        DoomslugApprovalsTracker {
            witness: Default::default(),
            account_id_to_stake,
            total_stake,
            approved_stake: 0,
            endorsed_stake: 0,
            time_passed_threshold: None,
            threshold_mode,
        }
    }

    /// Given a single approval (either an endorsement or a skip-message) updates the approved
    /// stake on the block that is being approved, and returns the largest threshold that is crossed
    /// This method only returns `ReadyToProduce` if the block has 2/3 approvals and 1/2 endorsements.
    /// The production readiness due to enough time passing since crossing the threshold should be
    /// handled by the caller.
    ///
    /// # Arguments
    /// * now      - the current timestamp
    /// * approval - the approval to process
    ///
    /// # Returns
    /// `None` if the block doesn't have enough approvals yet to cross the doomslug threshold
    /// `PassedThreshold` if the block has enough approvals to pass the threshold, but can't be
    ///     produced bypassing the timeout
    /// `ReadyToProduce` if the block can be produced bypassing the timeout (i.e. has 2/3+ of
    ///     approvals and 1/2+ of endorsements)
    fn process_approval(
        &mut self,
        now: Instant,
        approval: &Approval,
    ) -> DoomslugBlockProductionReadiness {
        let mut increment_approved_stake = false;
        let mut increment_endorsed_stake = false;
        self.witness.entry(approval.account_id.clone()).or_insert_with(|| {
            increment_approved_stake = true;
            if approval.is_endorsement {
                increment_endorsed_stake = true;
            }
            approval.clone()
        });

        if increment_approved_stake {
            self.approved_stake +=
                self.account_id_to_stake.get(&approval.account_id).map_or(0, |x| *x);
        }

        if increment_endorsed_stake {
            self.endorsed_stake +=
                self.account_id_to_stake.get(&approval.account_id).map_or(0, |x| *x);
        }

        // We call to `get_block_production_readiness` here so that if the number of approvals crossed
        // the threshold, the timer for block production starts.
        self.get_block_production_readiness(now)
    }

    /// Withdraws an approval. This happens if a newer approval for the same `target_height` comes
    /// from the same account. Removes the approval from the `witness` and updates approved and
    /// endorsed stakes.
    fn withdraw_approval(&mut self, account_id: &AccountId) {
        let approval = match self.witness.remove(account_id) {
            None => return,
            Some(approval) => approval,
        };

        self.approved_stake -= self.account_id_to_stake.get(&approval.account_id).map_or(0, |x| *x);

        if approval.is_endorsement {
            self.endorsed_stake -=
                self.account_id_to_stake.get(&approval.account_id).map_or(0, |x| *x);
        }
    }

    /// Returns whether the block is ready to be produced without waiting for any extra time,
    /// has crossed the doomslug threshold (and thus can be produced given enough time passed),
    /// or is not ready to be produced at all.
    /// This method only returns `ReadyToProduce` if the block has 2/3 approvals and 1/2 endorsements.
    /// The production readiness due to enough time passing since crossing the threshold should be
    /// handled by the caller.
    ///
    /// # Arguments
    /// * now - the current timestamp
    ///
    /// # Returns
    /// `None` if the block doesn't have enough approvals yet to cross the doomslug threshold
    /// `PassedThreshold` if the block has enough approvals to pass the threshold, but can't be
    ///     produced bypassing the timeout
    /// `ReadyToProduce` if the block can be produced bypassing the timeout (i.e. has 2/3+ of
    ///     approvals and 1/2+ of endorsements)
    fn get_block_production_readiness(&mut self, now: Instant) -> DoomslugBlockProductionReadiness {
        if self.approved_stake > self.total_stake * 2 / 3
            && self.endorsed_stake > self.total_stake / 2
        {
            if self.time_passed_threshold == None {
                self.time_passed_threshold = Some(now);
            }
            DoomslugBlockProductionReadiness::ReadyToProduce(self.time_passed_threshold.unwrap())
        } else if self.approved_stake > self.total_stake / 2
            || self.threshold_mode == DoomslugThresholdMode::NoApprovals
        {
            if self.time_passed_threshold == None {
                self.time_passed_threshold = Some(now);
            }
            DoomslugBlockProductionReadiness::PassedThreshold(self.time_passed_threshold.unwrap())
        } else {
            DoomslugBlockProductionReadiness::None
        }
    }
}

impl DoomslugApprovalsTrackersAtHeight {
    fn new() -> Self {
        Self { approval_trackers: HashMap::new(), last_approval_per_account: HashMap::new() }
    }

    /// This method is a wrapper around `DoomslugApprovalsTracker::process_approval`, see comment
    /// above it for more details.
    /// This method has an extra logic that ensures that we only track one approval per `account_id`,
    /// if we already know some other approval for this account, we first withdraw it from the
    /// corresponding tracker, and associate the new approval with the account.
    ///
    /// # Arguments
    /// * `now`      - the current timestamp
    /// * `approval` - the approval to be processed
    /// * `stakes`   - all the stakes of all the block producers in the current epoch
    /// * `threshold_mode` - how many approvals are needed to produce a block. Is used to compute
    ///                the return value
    ///
    /// # Returns
    /// Same as `DoomslugApprovalsTracker::process_approval`
    fn process_approval(
        &mut self,
        now: Instant,
        approval: &Approval,
        stakes: &Vec<ValidatorStake>,
        threshold_mode: DoomslugThresholdMode,
    ) -> DoomslugBlockProductionReadiness {
        if let Some(last_parent_hash) = self.last_approval_per_account.get(&approval.account_id) {
            let should_remove = self
                .approval_trackers
                .get_mut(&last_parent_hash)
                .map(|x| {
                    x.withdraw_approval(&approval.account_id);
                    x.witness.is_empty()
                })
                .unwrap_or(false);

            if should_remove {
                self.approval_trackers.remove(&last_parent_hash);
            }
        }
        self.last_approval_per_account.insert(approval.account_id.clone(), approval.parent_hash);
        self.approval_trackers
            .entry(approval.parent_hash)
            .or_insert_with(|| DoomslugApprovalsTracker::new(stakes, threshold_mode))
            .process_approval(now, approval)
    }
}

impl Doomslug {
    pub fn new(
        largest_previously_skipped_height: BlockHeight,
        largest_previously_endorsed_height: BlockHeight,
        endorsement_delay: Duration,
        min_delay: Duration,
        delay_step: Duration,
        max_delay: Duration,
        signer: Option<Arc<dyn ValidatorSigner>>,
        threshold_mode: DoomslugThresholdMode,
    ) -> Self {
        Doomslug {
            approval_tracking: HashMap::new(),
            largest_promised_skip_height: largest_previously_skipped_height,
            largest_endorsed_height: largest_previously_endorsed_height,
            largest_ds_final_height: 0,
            largest_threshold_height: 0,
            tip: DoomslugTip { block_hash: CryptoHash::default(), reference_hash: None, height: 0 },
            endorsement_pending: false,
            timer: DoomslugTimer {
                started: Instant::now(),
                height: 0,
                endorsement_delay,
                min_delay,
                delay_step,
                max_delay,
            },
            signer,
            threshold_mode,
        }
    }

    /// Returns the `(hash, height)` of the current tip. Currently is only used by tests.
    pub fn get_tip(&self) -> (CryptoHash, BlockHeight) {
        (self.tip.block_hash, self.tip.height)
    }

    /// Returns the largest height for which we have enough approvals to be theoretically able to
    ///     produce a block (in practice a blocks might not be produceable yet if not enough time
    ///     passed since it accumulated enough approvals)
    pub fn get_largest_height_crossing_threshold(&self) -> BlockHeight {
        self.largest_threshold_height
    }

    pub fn get_largest_height_with_doomslug_finality(&self) -> BlockHeight {
        self.largest_ds_final_height
    }

    pub fn get_largest_skipped_height(&self) -> BlockHeight {
        self.largest_promised_skip_height
    }

    pub fn get_largest_endorsed_height(&self) -> BlockHeight {
        self.largest_endorsed_height
    }

    pub fn get_timer_height(&self) -> BlockHeight {
        self.timer.height
    }

    pub fn get_timer_start(&self) -> Instant {
        self.timer.started
    }

    /// Is expected to be called periodically and processed the timer (`start_timer` in the paper)
    /// If the `cur_time` way ahead of last time the `process_timer` was called, will only process
    /// a bounded number of steps, to avoid an infinite loop in case of some bugs.
    /// Processes sending delayed approvals or skip messages
    /// A major difference with the paper is that we process endorsement from the `process_timer`,
    /// not at the time of receiving a block. It is done to stagger blocks if the network is way
    /// too fast (e.g. during tests, or if a large set of validators have connection significantly
    /// better between themselves than with the rest of the validators)
    ///
    /// # Arguments
    /// * `cur_time` - is expected to receive `now`. Doesn't directly use `now` to simplify testing
    ///
    /// # Returns
    /// A vector of approvals that need to be sent to other block producers as a result of processing
    /// the timers
    #[must_use]
    pub fn process_timer(&mut self, cur_time: Instant) -> Vec<Approval> {
        let mut ret = vec![];
        for _ in 0..MAX_TIMER_ITERS {
            let skip_delay = self
                .timer
                .get_delay(self.timer.height.saturating_sub(self.largest_ds_final_height));

            // The `endorsement_delay` is time to send approval to the block producer at `timer.height`,
            // while the `skip_delay` is the time before sending the approval to BP of `timer_height + 1`,
            // so it makes sense for them to be at least 2x apart
            debug_assert!(skip_delay >= 2 * self.timer.endorsement_delay);

            if self.endorsement_pending
                && cur_time >= self.timer.started + self.timer.endorsement_delay
            {
                let tip_height = self.tip.height;

                let is_endorsement = tip_height > self.largest_promised_skip_height
                    && tip_height > self.largest_endorsed_height;

                if is_endorsement {
                    self.largest_endorsed_height = tip_height;
                }

                if let Some(approval) = self.create_approval(tip_height + 1, is_endorsement) {
                    ret.push(approval);
                }

                self.endorsement_pending = false;
            }

            if cur_time >= self.timer.started + skip_delay {
                debug_assert!(!self.endorsement_pending);

                if self.timer.height > self.largest_endorsed_height
                    && self.timer.height > self.tip.height
                {
                    self.largest_promised_skip_height =
                        std::cmp::max(self.timer.height, self.largest_promised_skip_height);

                    if let Some(approval) = self.create_approval(self.timer.height + 1, false) {
                        ret.push(approval);
                    }
                }

                // Restart the timer
                self.timer.started += skip_delay;
                self.timer.height += 1;
            } else {
                break;
            }
        }

        ret
    }

    pub fn create_approval(
        &self,
        target_height: BlockHeight,
        is_endorsement: bool,
    ) -> Option<Approval> {
        self.signer.as_ref().map(|signer| {
            Approval::new(
                self.tip.block_hash,
                self.tip.reference_hash,
                target_height,
                is_endorsement,
                &**signer,
            )
        })
    }

    /// Determines whether a block that precedes the block containing approvals has doomslug
    /// finality, i.e. if the sum of stakes of block producers who produced endorsements (approvals
    /// with `is_endorsed = true`) exceeds half of the total stake
    ///
    /// # Arguments
    /// * `approvals` - the set of approvals in the current block
    /// * `stakes`    - the vector of validator stakes in the current epoch
    pub fn is_approved_block_ds_final(
        approvals: &Vec<Approval>,
        account_id_to_stake: &HashMap<AccountId, Balance>,
    ) -> bool {
        let threshold = account_id_to_stake.values().sum::<Balance>() / 2;

        let endorsed_stake = approvals
            .iter()
            .map(|approval| {
                { account_id_to_stake.get(&approval.account_id) }.map_or(0, |stake| {
                    if approval.is_endorsement {
                        *stake
                    } else {
                        0
                    }
                })
            })
            .sum::<Balance>();

        endorsed_stake > threshold
    }

    /// Determines whether the block `prev_hash` has doomslug finality from perspective of a block
    /// that has it as its previous, and is at height `target_height`.
    /// Internally just pulls approvals from the `approval_tracking` and calls to
    /// `is_approved_block_ds_final`
    /// This method is presently only used by tests, and is not efficient (specifically, it
    /// recomputes `endorsed_state` and `total_stake`, both of which are/can be maintained)
    ///
    /// # Arguments
    /// * `prev_hash`     - the hash of the previous block (for which the ds finality is tested)
    /// * `target_height` - the height of the current block
    pub fn is_prev_block_ds_final(
        &self,
        prev_hash: CryptoHash,
        target_height: BlockHeight,
    ) -> bool {
        if let Some(approval_trackers_at_height) = self.approval_tracking.get(&target_height) {
            if let Some(approvals_tracker) =
                approval_trackers_at_height.approval_trackers.get(&prev_hash)
            {
                Doomslug::is_approved_block_ds_final(
                    &approvals_tracker.witness.values().cloned().collect::<Vec<_>>(),
                    &approvals_tracker.account_id_to_stake,
                )
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Determines whether a block has enough approvals to be produced.
    /// In production (with `mode == HalfStake`) we require the total stake of all the approvals to
    /// be strictly more than half of the total stake. For many non-doomslug specific tests
    /// (with `mode == NoApprovals`) no approvals are needed.
    ///
    /// # Arguments
    /// * `mode`      - whether we want half of the total stake or just a single approval
    /// * `approvals` - the set of approvals in the current block
    /// * `stakes`    - the vector of validator stakes in the current epoch
    pub fn can_approved_block_be_produced(
        mode: DoomslugThresholdMode,
        approvals: &Vec<Approval>,
        account_id_to_stake: &HashMap<AccountId, Balance>,
    ) -> bool {
        if mode == DoomslugThresholdMode::NoApprovals {
            return true;
        }

        let threshold = account_id_to_stake.values().sum::<Balance>() / 2;

        let approved_stake = approvals
            .iter()
            .map(|approval| {
                { account_id_to_stake.get(&approval.account_id) }.map_or(0, |stake| *stake)
            })
            .sum::<Balance>();

        approved_stake > threshold
    }

    pub fn remove_witness(
        &mut self,
        prev_hash: &CryptoHash,
        target_height: BlockHeight,
    ) -> Vec<Approval> {
        if let Some(approval_trackers_at_height) = self.approval_tracking.get_mut(&target_height) {
            let approvals_tracker = approval_trackers_at_height.approval_trackers.remove(prev_hash);
            match approvals_tracker {
                None => vec![],
                Some(approvals_tracker) => {
                    approvals_tracker.witness.into_iter().map(|(_, v)| v).collect::<Vec<_>>()
                }
            }
        } else {
            vec![]
        }
    }

    /// Updates the current tip of the chain. Restarts the timer accordingly.
    ///
    /// # Arguments
    /// * `now`            - current time. Doesn't call to `Utc::now()` directly to simplify testing
    /// * `block_hash`     - the hash of the new tip
    /// * `reference_hash` - is expected to come from the finality gadget and represents the
    ///                      reference hash (if any) to be included in the approvals being sent for
    ///                      this tip (whether endorsements or skip messages)
    /// * `height`         - the height of the tip
    /// * `last_ds_final_height` - last height at which a block in this chain has doomslug finality
    pub fn set_tip(
        &mut self,
        now: Instant,
        block_hash: CryptoHash,
        reference_hash: Option<CryptoHash>,
        height: BlockHeight,
        last_ds_final_height: BlockHeight,
    ) {
        self.tip = DoomslugTip { block_hash, reference_hash, height };

        self.largest_ds_final_height = last_ds_final_height;
        self.timer.height = height + 1;
        self.timer.started = now;

        self.approval_tracking
            .retain(|h, _| *h > height && *h <= height + MAX_HEIGHTS_AHEAD_TO_STORE_APPROVALS);

        self.endorsement_pending = true;
    }

    /// Records an approval message, and return whether the block has passed the threshold / ready
    /// to be produced without waiting any further. See the comment for `DoomslugApprovalTracker::process_approval`
    /// for details
    #[must_use]
    fn on_approval_message_internal(
        &mut self,
        now: Instant,
        approval: &Approval,
        stakes: &Vec<ValidatorStake>,
    ) -> DoomslugBlockProductionReadiness {
        let threshold_mode = self.threshold_mode;
        let ret = self
            .approval_tracking
            .entry(approval.target_height)
            .or_insert_with(|| DoomslugApprovalsTrackersAtHeight::new())
            .process_approval(now, approval, stakes, threshold_mode);

        if ret != DoomslugBlockProductionReadiness::None {
            if approval.target_height > self.largest_threshold_height {
                self.largest_threshold_height = approval.target_height;
            }
        }

        ret
    }

    /// Processes single approval
    pub fn on_approval_message(
        &mut self,
        now: Instant,
        approval: &Approval,
        stakes: &Vec<ValidatorStake>,
    ) {
        if approval.target_height < self.tip.height
            || approval.target_height > self.tip.height + MAX_HEIGHTS_AHEAD_TO_STORE_APPROVALS
        {
            return;
        }

        let _ = self.on_approval_message_internal(now, approval, stakes);
    }

    /// Returns whether we can produce a block for this height. The check for whether `me` is the
    /// block producer for the height needs to be done by the caller.
    /// We can produce a block if:
    ///  - The block has 2/3 of approvals, doomslug-finalizing the previous block, and we have
    ///    enough chunks, or
    ///  - The block has 1/2 of approvals, and T(h' / 6) has passed since the block has had 1/2 of
    ///    approvals for the first time, where h' is time since the last ds-final block.
    /// Only the height is passed into the function, we use the tip known to `Doomslug` as the
    /// parent hash.
    ///
    /// # Arguments:
    /// * `now`               - current timestamp
    /// * `target_height`     - the height for which the readiness is checked
    /// * `has_enough_chunks` - if not, we will wait for T(h' / 6) even if we have 2/3 approvals &
    ///                         have the previous block ds-final.
    #[must_use]
    pub fn ready_to_produce_block(
        &mut self,
        now: Instant,
        target_height: BlockHeight,
        has_enough_chunks: bool,
    ) -> bool {
        if let Some(approval_trackers_at_height) = self.approval_tracking.get_mut(&target_height) {
            if let Some(approval_tracker) =
                approval_trackers_at_height.approval_trackers.get_mut(&self.tip.block_hash)
            {
                let block_production_readiness =
                    approval_tracker.get_block_production_readiness(now);
                match block_production_readiness {
                    DoomslugBlockProductionReadiness::None => false,
                    DoomslugBlockProductionReadiness::PassedThreshold(when) => {
                        let delay = self.timer.get_delay(
                            self.timer.height.saturating_sub(self.largest_ds_final_height),
                        ) / 6;

                        now > when + delay
                    }
                    DoomslugBlockProductionReadiness::ReadyToProduce(when) => {
                        if has_enough_chunks {
                            true
                        } else {
                            let delay = self.timer.get_delay(
                                self.timer.height.saturating_sub(self.largest_ds_final_height),
                            ) / 6;

                            now > when + delay
                        }
                    }
                }
            } else {
                false
            }
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use near_crypto::{KeyType, SecretKey};
    use near_primitives::block::Approval;
    use near_primitives::hash::hash;
    use near_primitives::types::ValidatorStake;
    use near_primitives::validator_signer::InMemoryValidatorSigner;

    use crate::doomslug::{
        DoomslugApprovalsTrackersAtHeight, DoomslugBlockProductionReadiness, DoomslugThresholdMode,
    };
    use crate::Doomslug;

    #[test]
    fn test_endorsements_and_skips_basic() {
        let mut now = Instant::now(); // For the test purposes the absolute value of the initial instant doesn't matter

        let mut ds = Doomslug::new(
            0,
            0,
            Duration::from_millis(400),
            Duration::from_millis(1000),
            Duration::from_millis(100),
            Duration::from_millis(3000),
            Some(Arc::new(InMemoryValidatorSigner::from_seed("test", KeyType::ED25519, "test"))),
            DoomslugThresholdMode::HalfStake,
        );

        // Set a new tip, must produce an endorsement
        ds.set_tip(now, hash(&[1]), None, 1, 1);
        assert_eq!(ds.process_timer(now + Duration::from_millis(399)).len(), 0);
        let approval =
            ds.process_timer(now + Duration::from_millis(400)).into_iter().nth(0).unwrap();
        assert_eq!(approval.parent_hash, hash(&[1]));
        assert_eq!(approval.target_height, 2);
        assert!(approval.is_endorsement);

        // Same tip => no endorsement, but still expect an approval (it is for the cases when a block
        // at lower height is received after a block at a higher height, e.g. due to finality gadget)
        ds.set_tip(now, hash(&[1]), None, 1, 1);
        let approval =
            ds.process_timer(now + Duration::from_millis(400)).into_iter().nth(0).unwrap();
        assert_eq!(approval.parent_hash, hash(&[1]));
        assert_eq!(approval.target_height, 2);
        assert!(!approval.is_endorsement);

        // The block was `ds_final` and therefore started the timer. Try checking before one second expires
        assert_eq!(ds.process_timer(now + Duration::from_millis(999)), vec![]);

        // But one second should trigger the skip
        match ds.process_timer(now + Duration::from_millis(1000)) {
            approvals if approvals.len() == 0 => assert!(false),
            approvals => {
                assert_eq!(approvals[0].parent_hash, hash(&[1]));
                assert_eq!(approvals[0].target_height, 3);
                assert!(!approvals[0].is_endorsement);
            }
        }

        // Shift now 1 second forward
        now += Duration::from_millis(1000);

        // Not processing a block at height 2 should not produce an endorsement (but still an approval)
        ds.set_tip(now, hash(&[2]), None, 2, 1);
        let approval =
            ds.process_timer(now + Duration::from_millis(400)).into_iter().nth(0).unwrap();
        assert_eq!(approval.parent_hash, hash(&[2]));
        assert_eq!(approval.target_height, 3);
        assert!(!approval.is_endorsement);

        // Shift now 1 second forward
        now += Duration::from_millis(1000);

        // But at height 3 should (also neither block has ds_finality set, keep last ds_final at 1 for now)
        ds.set_tip(now, hash(&[3]), None, 3, 1);
        let approval =
            ds.process_timer(now + Duration::from_millis(400)).into_iter().nth(0).unwrap();
        assert_eq!(approval.parent_hash, hash(&[3]));
        assert_eq!(approval.target_height, 4);
        assert!(approval.is_endorsement);

        // Move 1 second further
        now += Duration::from_millis(1000);

        assert_eq!(ds.process_timer(now + Duration::from_millis(199)), vec![]);

        match ds.process_timer(now + Duration::from_millis(200)) {
            approvals if approvals.len() == 0 => assert!(false),
            approvals if approvals.len() == 1 => {
                assert_eq!(approvals[0].parent_hash, hash(&[3]));
                assert_eq!(approvals[0].target_height, 5);
                assert!(!approvals[0].is_endorsement);
            }
            _ => assert!(false),
        }

        // Move 1 second further
        now += Duration::from_millis(1000);

        // Now skip 5 (the extra delay is 200+300 = 500)
        assert_eq!(ds.process_timer(now + Duration::from_millis(499)), vec![]);

        match ds.process_timer(now + Duration::from_millis(500)) {
            approvals if approvals.len() == 0 => assert!(false),
            approvals => {
                assert_eq!(approvals[0].parent_hash, hash(&[3]));
                assert_eq!(approvals[0].target_height, 6);
                assert!(!approvals[0].is_endorsement);
            }
        }

        // Move 1 second further
        now += Duration::from_millis(1000);

        // Skip 6 (the extra delay is 0+200+300+400 = 900)
        assert_eq!(ds.process_timer(now + Duration::from_millis(899)), vec![]);

        match ds.process_timer(now + Duration::from_millis(900)) {
            approvals if approvals.len() == 0 => assert!(false),
            approvals => {
                assert_eq!(approvals[0].parent_hash, hash(&[3]));
                assert_eq!(approvals[0].target_height, 7);
                assert!(!approvals[0].is_endorsement);
            }
        }

        // Move 1 second further
        now += Duration::from_millis(1000);

        // Accept block at 5 with ds finality, expect it to produce an approval, but not an endorsement
        ds.set_tip(now, hash(&[5]), None, 5, 5);
        let approval =
            ds.process_timer(now + Duration::from_millis(400)).into_iter().nth(0).unwrap();
        assert_eq!(approval.parent_hash, hash(&[5]));
        assert_eq!(approval.target_height, 6);
        assert!(!approval.is_endorsement);

        // Skip a whole bunch of heights by moving 100 seconds ahead
        now += Duration::from_millis(100_000);
        assert!(ds.process_timer(now).len() > 10);

        // Add some random small number of milliseconds to test that when the next block is added, the
        // timer is reset
        now += Duration::from_millis(17);

        // That approval should not be an endorsement, since we skipped 6
        ds.set_tip(now, hash(&[6]), None, 6, 5);
        let approval =
            ds.process_timer(now + Duration::from_millis(400)).into_iter().nth(0).unwrap();
        assert_eq!(approval.parent_hash, hash(&[6]));
        assert_eq!(approval.target_height, 7);
        assert!(!approval.is_endorsement);

        // The block height was less than the timer height, and thus the timer was reset.
        // The wait time for height 7 with last ds final block at 5 is 1100
        assert_eq!(ds.process_timer(now + Duration::from_millis(1099)), vec![]);

        match ds.process_timer(now + Duration::from_millis(1100)) {
            approvals if approvals.len() == 0 => assert!(false),
            approvals => {
                assert_eq!(approvals[0].parent_hash, hash(&[6]));
                assert_eq!(approvals[0].target_height, 8);
                assert!(!approvals[0].is_endorsement);
            }
        }
    }

    #[test]
    fn test_doomslug_approvals() {
        let accounts: Vec<(&str, u128)> =
            vec![("test1", 2), ("test2", 1), ("test3", 3), ("test4", 2)];
        let stakes = accounts
            .iter()
            .map(|(account_id, stake)| ValidatorStake {
                account_id: account_id.to_string(),
                stake: *stake,
                public_key: SecretKey::from_seed(KeyType::ED25519, account_id).public_key(),
            })
            .collect::<Vec<_>>();
        let signers = accounts
            .iter()
            .map(|(account_id, _)| {
                InMemoryValidatorSigner::from_seed(account_id, KeyType::ED25519, account_id)
            })
            .collect::<Vec<_>>();

        let signer = Arc::new(InMemoryValidatorSigner::from_seed("test", KeyType::ED25519, "test"));
        let mut ds = Doomslug::new(
            0,
            0,
            Duration::from_millis(400),
            Duration::from_millis(1000),
            Duration::from_millis(100),
            Duration::from_millis(3000),
            Some(signer.clone()),
            DoomslugThresholdMode::HalfStake,
        );

        let mut now = Instant::now();

        // In the comments below the format is
        // account, height -> approved stake
        // The total stake is 8, so the thresholds are 5 and 7

        // "test1", 2 -> 2
        assert_eq!(
            ds.on_approval_message_internal(
                now,
                &Approval::new(hash(&[1]), None, 2, true, &signers[0]),
                &stakes,
            ),
            DoomslugBlockProductionReadiness::None,
        );

        // "test3", 4 -> 3
        assert_eq!(
            ds.on_approval_message_internal(
                now,
                &Approval::new(hash(&[1]), None, 4, true, &signers[2]),
                &stakes,
            ),
            DoomslugBlockProductionReadiness::None,
        );

        // "test1", 4 -> 5
        assert_eq!(
            ds.on_approval_message_internal(
                now,
                &Approval::new(hash(&[1]), None, 4, true, &signers[0]),
                &stakes,
            ),
            DoomslugBlockProductionReadiness::PassedThreshold(now),
        );

        // "test1", 4 -> same account, still 5
        assert_eq!(
            ds.on_approval_message_internal(
                now + Duration::from_millis(100),
                &Approval::new(hash(&[1]), None, 4, true, &signers[0]),
                &stakes,
            ),
            DoomslugBlockProductionReadiness::PassedThreshold(now),
        );

        // "test4", 4 -> 7
        assert_eq!(
            ds.on_approval_message_internal(
                now,
                &Approval::new(hash(&[1]), None, 4, true, &signers[3]),
                &stakes,
            ),
            DoomslugBlockProductionReadiness::ReadyToProduce(now),
        );

        // "test4", 2 -> 4
        assert_eq!(
            ds.on_approval_message_internal(
                now,
                &Approval::new(hash(&[1]), None, 2, true, &signers[3]),
                &stakes,
            ),
            DoomslugBlockProductionReadiness::None,
        );

        now += Duration::from_millis(200);

        // "test2", 2 -> 5
        assert_eq!(
            ds.on_approval_message_internal(
                now,
                &Approval::new(hash(&[1]), None, 2, true, &signers[1]),
                &stakes,
            ),
            DoomslugBlockProductionReadiness::PassedThreshold(now),
        );

        // A different parent hash
        assert_eq!(
            ds.on_approval_message_internal(
                now,
                &Approval::new(hash(&[2]), None, 4, true, &signers[1]),
                &stakes,
            ),
            DoomslugBlockProductionReadiness::None,
        );
    }

    #[test]
    fn test_doomslug_one_approval_per_target_height() {
        let accounts = vec![("test1", 2), ("test2", 1), ("test3", 3), ("test4", 2)];
        let signers = accounts
            .iter()
            .map(|(account_id, _)| {
                InMemoryValidatorSigner::from_seed(account_id, KeyType::ED25519, account_id)
            })
            .collect::<Vec<_>>();
        let stakes = accounts
            .into_iter()
            .map(|(account_id, stake)| ValidatorStake {
                account_id: account_id.to_string(),
                stake,
                public_key: SecretKey::from_seed(KeyType::ED25519, account_id).public_key(),
            })
            .collect::<Vec<_>>();
        let mut tracker = DoomslugApprovalsTrackersAtHeight::new();

        let a1_1 = Approval::new(hash(&[1]), None, 4, true, &signers[0]);
        let a1_2 = Approval::new(hash(&[1]), None, 4, false, &signers[1]);
        let a1_3 = Approval::new(hash(&[1]), None, 4, true, &signers[2]);

        let a2_1 = Approval::new(hash(&[2]), None, 4, true, &signers[0]);
        let a2_2 = Approval::new(hash(&[2]), None, 4, false, &signers[1]);
        let a2_3 = Approval::new(hash(&[2]), None, 4, true, &signers[2]);

        // Process first approval, and then process it again and make sure it works
        tracker.process_approval(Instant::now(), &a1_1, &stakes, DoomslugThresholdMode::HalfStake);

        assert_eq!(tracker.approval_trackers.get(&hash(&[1])).unwrap().approved_stake, 2);
        assert_eq!(tracker.approval_trackers.get(&hash(&[1])).unwrap().endorsed_stake, 2);

        tracker.process_approval(Instant::now(), &a1_1, &stakes, DoomslugThresholdMode::HalfStake);

        assert_eq!(tracker.approval_trackers.get(&hash(&[1])).unwrap().approved_stake, 2);
        assert_eq!(tracker.approval_trackers.get(&hash(&[1])).unwrap().endorsed_stake, 2);

        // Process the remaining two approvals on the first block
        tracker.process_approval(Instant::now(), &a1_2, &stakes, DoomslugThresholdMode::HalfStake);
        tracker.process_approval(Instant::now(), &a1_3, &stakes, DoomslugThresholdMode::HalfStake);

        assert_eq!(tracker.approval_trackers.get(&hash(&[1])).unwrap().approved_stake, 6);
        assert_eq!(tracker.approval_trackers.get(&hash(&[1])).unwrap().endorsed_stake, 5);

        // Process new approvals one by one, expect the approved and endorsed stake to slowly decrease
        tracker.process_approval(Instant::now(), &a2_1, &stakes, DoomslugThresholdMode::HalfStake);

        assert_eq!(tracker.approval_trackers.get(&hash(&[1])).unwrap().approved_stake, 4);
        assert_eq!(tracker.approval_trackers.get(&hash(&[1])).unwrap().endorsed_stake, 3);

        tracker.process_approval(Instant::now(), &a2_2, &stakes, DoomslugThresholdMode::HalfStake);

        assert_eq!(tracker.approval_trackers.get(&hash(&[1])).unwrap().approved_stake, 3);
        assert_eq!(tracker.approval_trackers.get(&hash(&[1])).unwrap().endorsed_stake, 3);

        // As we update the last of the three approvals, the tracker for the first block should be completely removed
        tracker.process_approval(Instant::now(), &a2_3, &stakes, DoomslugThresholdMode::HalfStake);

        assert!(tracker.approval_trackers.get(&hash(&[1])).is_none());

        // Check the approved and endorsed stake for the new block, and also ensure that processing one of the same approvals
        // again works fine

        assert_eq!(tracker.approval_trackers.get(&hash(&[2])).unwrap().approved_stake, 6);
        assert_eq!(tracker.approval_trackers.get(&hash(&[2])).unwrap().endorsed_stake, 5);

        tracker.process_approval(Instant::now(), &a2_3, &stakes, DoomslugThresholdMode::HalfStake);

        assert_eq!(tracker.approval_trackers.get(&hash(&[2])).unwrap().approved_stake, 6);
        assert_eq!(tracker.approval_trackers.get(&hash(&[2])).unwrap().endorsed_stake, 5);
    }
}

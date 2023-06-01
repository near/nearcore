//! Estimation functions for action costs, separated by send and exec.
//!
//! Estimations in this module are more tailored towards specific gas parameters
//! compared to those in the parent module. But the estimations here potential
//! miss some overhead that is outside action verification and outside action
//! application. But in combination with the wholistic action cost estimation,
//! the picture should be fairly complete.

use crate::estimator_context::{EstimatorContext, Testbed};
use crate::gas_cost::{GasCost, NonNegativeTolerance};
use crate::transaction_builder::AccountRequirement;
use crate::utils::{average_cost, percentiles};
use near_crypto::{KeyType, PublicKey};
use near_primitives::account::{AccessKey, AccessKeyPermission, FunctionCallPermission};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{ActionReceipt, Receipt};
use near_primitives::transaction::Action;
use near_primitives::types::{AccountId, Gas};
use std::iter;

const GAS_1_MICROSECOND: Gas = 1_000_000_000;
const GAS_1_NANOSECOND: Gas = 1_000_000;
const GAS_100_PICOSECONDS: Gas = 100_000;

/// A builder object for constructing action cost estimations.
///
/// This module uses `ActionEstimation` as a builder object to specify the
/// details of each action estimation. For example, creating an account has the
/// requirement that the account does not exist, yet. But for a staking action,
/// it must exist. The builder object makes it easy to specify these
/// requirements separately for each estimation, with only a small amount of
/// boiler-plate code repeated.
///
/// Besides account id requirements, the builder also accepts a few other
/// settings. See the available methods and their doc comments.
///
/// Once `ActionEstimation` is complete, call either `verify_cost` or
/// `apply_cost` to receive just the execution cost or just the sender cost.
/// This will run a loop internally that spawns a bunch of actions using
/// different accounts. This allows to average the cost of a number of runs to
/// make the result more stable.
///
/// By default, the inner actions are also multiplied within a receipt. This is
/// to reduce the overhead noise of the receipt cost, which can often dominate
/// compared to the cost of a single action inside. The only problem is that all
/// actions inside a receipt must share the receiver and the sender account ids.
/// This makes action duplication unsuitable for actions that cannot be
/// repeated, such as creating or deleting an account. In those cases, set inner
/// iterations to 1.
struct ActionEstimation {
    /// generate account ids from the transaction builder with requirements
    signer: AccountRequirement,
    predecessor: AccountRequirement,
    receiver: AccountRequirement,
    /// the actions to estimate
    actions: Vec<Action>,
    /// how often actions are repeated in a receipt
    inner_iters: usize,
    /// how many receipts to measure
    outer_iters: usize,
    /// how many iterations to ignore for measurements
    warmup: usize,
    /// What statistic to report when combining outer iterations.
    ///
    /// By default, the average is reported.
    reported_statistic: Statistic,
    /// the gas metric to measure
    metric: crate::config::GasMetric,
    /// subtract the cost of an empty receipt from the measured cost
    /// (`false` is only really useful for action receipt creation cost)
    subtract_base: bool,
    /// Constant to which the estimation is rounded up in case the estimation
    /// turns out smaller, to avoid unstable estimations.
    ///
    /// The costs computed here are often very small. The noise can be larger
    /// than the measurement in many cases. However, we also don't care too much
    /// about the exact value as long as it is small enough. "Small enough" has
    /// to be defined per parameter, as it is usually higher for base costs
    /// compared to per-byte costs. This field is used to set the threshold,
    /// everything below it will be rounded up.
    ///
    /// This is also used to set the tolerance towards the negative. Results in
    /// the range [-min_gas,+min_gas] are set to min_gas and marked as good
    /// results. Anything below -min_gas is set to zero and marked as underflow.
    min_gas: Gas,
}

enum Statistic {
    /// Arithmetic mean, high variance is marked as uncertain.
    Average,
    /// Use a specific value from the distribution.
    ///
    /// The (potential) uncertainty of this measurement is inherited. High
    /// variance is ignored, this is usually the reason why this statistic is
    /// used over the mean in the first place.
    ///
    /// Usage: `Percentile(0.99)` gives the 99th percentile.
    Percentile(f32),
}

impl ActionEstimation {
    /// Create a new action estimation that can be modified using builder-style
    /// methods.
    ///
    /// The object returned by this constructor uses random and unused accounts
    /// for signer, predecessor, and receiver. This means the sender is not the
    /// receiver. Further, the default returned here uses 100 inner iterations,
    /// thereby duplicating all given actions 100 fold inside each receipt.
    ///
    /// Note that the object returned here does not contain any actions, yet. It
    /// will operate on an action receipt with no actions inside, unless actions
    /// are added.
    fn new(ctx: &mut EstimatorContext) -> Self {
        Self {
            signer: AccountRequirement::RandomUnused,
            predecessor: AccountRequirement::RandomUnused,
            receiver: AccountRequirement::RandomUnused,
            actions: vec![],
            inner_iters: 100,
            outer_iters: ctx.config.iter_per_block,
            warmup: ctx.config.warmup_iters_per_block,
            reported_statistic: Statistic::Average,
            metric: ctx.config.metric,
            subtract_base: true,
            // This is a reasonable limit because even the cheapest action
            // (transfer) is 115 us per component. Only for the per-byte costs
            // we need a smaller limit, because these costs can go below 1 ns.
            // They are changed on a case-by-case basis.
            min_gas: GAS_1_MICROSECOND,
        }
    }

    /// Create a new action estimation that can be modified using builder-style
    /// methods and sets the accounts ids such that the signer, sender, and
    /// receiver are all the same account id.
    ///
    /// This constructor is also used for execution estimations because:
    /// (1) Some actions require sender = receiver to execute without an error.
    /// (2) It does not matter for execution performance.
    fn new_sir(ctx: &mut EstimatorContext) -> Self {
        Self {
            signer: AccountRequirement::RandomUnused,
            predecessor: AccountRequirement::SameAsSigner,
            receiver: AccountRequirement::SameAsSigner,
            ..Self::new(ctx)
        }
    }

    /// Set how to generate the predecessor, also known as sender, for each
    /// transaction or action receipt.
    fn predecessor(mut self, predecessor: AccountRequirement) -> Self {
        self.predecessor = predecessor;
        self
    }

    /// Set how to generate the receiver account id for each transaction or
    /// action receipt.
    fn receiver(mut self, receiver: AccountRequirement) -> Self {
        self.receiver = receiver;
        self
    }

    /// Add an action that will be duplicated for every inner iteration.
    ///
    /// Calling this multiple times is allowed and inner iterations will
    /// duplicate the full group as a block, rather than individual actions.
    /// (3 * AB = ABABAB, not AAABBB)
    fn add_action(mut self, action: Action) -> Self {
        self.actions.push(action);
        self
    }

    /// Set how many times the actions are duplicated per receipt or transaction.
    fn inner_iters(mut self, inner_iters: usize) -> Self {
        self.inner_iters = inner_iters;
        self
    }

    /// Set how many receipts or transactions are measured at least.
    ///
    /// This overrides the CLI argument that is usually used for number of
    /// iterations. Use this when a certain estimation requires a minimum amount
    /// of iterations.
    fn min_outer_iters(mut self, outer_iters: usize) -> Self {
        self.outer_iters = self.outer_iters.max(outer_iters);
        self
    }

    /// If enabled, the estimation will automatically subtract the cost of an
    /// empty action receipt from the measurement. (enabled by default)
    fn subtract_base(mut self, yes: bool) -> Self {
        self.subtract_base = yes;
        self
    }

    /// Set the smallest gas value for which we need accurate estimations,
    /// values below will be clamped.
    fn min_gas(mut self, gas: Gas) -> Self {
        self.min_gas = gas;
        self
    }

    /// If enabled, the outer iterations are aggregated to a percentile value
    /// instead of to the average for the reported cost.
    ///
    /// Ideally, an estimation produces a stable output. In that case the
    /// average is a good statistic. But some estimations keep having outliers.
    /// If we cannot get rid of those, we can fall back to using a percentile of
    /// the sample distribution as a more meaningful statistic.
    fn report_percentile(mut self, rank: f32) -> Self {
        self.reported_statistic = Statistic::Percentile(rank);
        self
    }

    /// Estimate the gas cost for converting an action in a transaction to one in an
    /// action receipt, without network costs.
    ///
    /// To convert a transaction into a receipt, each action has to be verified.
    /// This happens on a different shard than the action execution and should
    /// therefore be estimated and charged separately.
    ///
    /// Network costs should also be taken into account here but we don't do that,
    /// yet.
    #[track_caller]
    fn verify_cost(&self, testbed: &mut Testbed) -> GasCost {
        self.estimate_average_cost(testbed, Self::verify_actions_cost)
    }

    /// Estimate the cost for executing the actions in the builder.
    ///
    /// This is the "apply" cost only, without validation, without sending and
    /// without overhead that does not scale with the number of actions.
    #[track_caller]
    fn apply_cost(&self, testbed: &mut Testbed) -> GasCost {
        self.estimate_average_cost(testbed, Self::apply_actions_cost)
    }

    /// Estimate the cost of verifying a set of actions once.
    #[track_caller]
    fn verify_actions_cost(&self, testbed: &mut Testbed, actions: Vec<Action>) -> GasCost {
        let tb = testbed.transaction_builder();
        let signer_id = tb.account_by_requirement(self.signer, None);
        let predecessor_id = tb.account_by_requirement(self.predecessor, Some(&signer_id));
        let receiver_id = tb.account_by_requirement(self.receiver, Some(&signer_id));
        let tx = tb.transaction_from_actions(predecessor_id, receiver_id, actions);
        testbed.verify_transaction(&tx, self.metric)
    }

    /// Estimate the cost of applying a set of actions once.
    #[track_caller]
    fn apply_actions_cost(&self, testbed: &mut Testbed, actions: Vec<Action>) -> GasCost {
        let tb = testbed.transaction_builder();

        let signer_id = tb.account_by_requirement(self.signer, None);
        let predecessor_id = tb.account_by_requirement(self.predecessor, Some(&signer_id));
        let receiver_id = tb.account_by_requirement(self.receiver, Some(&signer_id));
        let signer_public_key = PublicKey::from_seed(KeyType::ED25519, &signer_id);

        let action_receipt = ActionReceipt {
            signer_id,
            signer_public_key,
            gas_price: 100_000_000,
            output_data_receivers: vec![],
            input_data_ids: vec![],
            actions,
        };
        let receipt = Receipt {
            predecessor_id,
            receiver_id,
            receipt_id: CryptoHash::new(),
            receipt: near_primitives::receipt::ReceiptEnum::Action(action_receipt),
        };
        testbed.apply_action_receipt(&receipt, self.metric)
    }

    /// Take a function that executes a list of actions on a testbed, execute
    /// and measure it multiple times and return the average cost.
    #[track_caller]
    fn estimate_average_cost(
        &self,
        testbed: &mut Testbed,
        estimated_fn: fn(&Self, &mut Testbed, Vec<Action>) -> GasCost,
    ) -> GasCost {
        assert!(
            !self.actions.is_empty() || self.inner_iters == 1,
            "inner iterations don't work if there are no actions to multiply"
        );
        let num_total_actions = self.actions.len() * self.inner_iters;
        let actions: Vec<Action> =
            self.actions.iter().cloned().cycle().take(num_total_actions).collect();

        let gas_results = iter::repeat_with(|| estimated_fn(self, testbed, actions.clone()))
            .skip(self.warmup)
            .take(self.outer_iters)
            .collect();

        // This could be cached for efficiency. But experience so far shows that
        // reusing caches values for many future estimations leads to the
        // problem that a single "HIGH-VARIANCE" uncertain estimation can spoil
        // all following estimations. In this case, rerunning is cheap and it
        // ensures the base is computed in a very similar state of the machine as
        // the measurement it is subtracted from.
        let base =
            if self.subtract_base { estimated_fn(self, testbed, vec![]) } else { GasCost::zero() };

        let cost_per_tx = match self.reported_statistic {
            Statistic::Average => average_cost(gas_results),
            Statistic::Percentile(rank) => percentiles(gas_results, &[rank]).next().unwrap(),
        };
        let gas_tolerance = self.inner_iters as u64 * self.min_gas;
        let gas_per_action = cost_per_tx
            .saturating_sub(&base, &NonNegativeTolerance::AbsoluteTolerance(gas_tolerance))
            / self.inner_iters as u64;

        // Set small (but not underflowed) results to the minimum value.
        // This avoids flaky estimation results.
        if !gas_per_action.is_uncertain() {
            gas_per_action.min_gas(self.min_gas)
        } else {
            gas_per_action
        }
    }
}

pub(crate) fn create_account_send_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx)
        .add_action(create_account_action())
        .receiver(AccountRequirement::SubOfSigner)
        .verify_cost(&mut ctx.testbed())
}

pub(crate) fn create_account_send_not_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new(ctx)
        .add_action(create_account_action())
        .receiver(AccountRequirement::SubOfSigner)
        .verify_cost(&mut ctx.testbed())
}

pub(crate) fn create_account_exec(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx)
        .add_action(create_account_action())
        .receiver(AccountRequirement::SubOfSigner)
        .inner_iters(1) // creating account works only once in a receipt
        .add_action(create_transfer_action()) // must have balance for storage
        .apply_cost(&mut ctx.testbed())
}

pub(crate) fn delete_account_send_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx)
        .add_action(delete_account_action())
        .inner_iters(1) // only one account deletion possible
        .verify_cost(&mut ctx.testbed())
}

pub(crate) fn delete_account_send_not_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new(ctx)
        .add_action(delete_account_action())
        .inner_iters(1) // only one account deletion possible
        .verify_cost(&mut ctx.testbed())
}

pub(crate) fn delete_account_exec(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx)
        .add_action(delete_account_action())
        .inner_iters(1) // only one account deletion possible
        .apply_cost(&mut ctx.testbed())
}

pub(crate) fn deploy_contract_base_send_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx)
        .add_action(deploy_action(ActionSize::Min))
        .verify_cost(&mut ctx.testbed())
}

pub(crate) fn deploy_contract_base_send_not_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new(ctx)
        .add_action(deploy_action(ActionSize::Min))
        .verify_cost(&mut ctx.testbed())
}

/// Note: This is not the best estimation because a dummy contract is clearly
/// not the worst-case scenario for gas costs.
pub(crate) fn deploy_contract_base_exec(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx)
        .add_action(deploy_action(ActionSize::Min))
        .apply_cost(&mut ctx.testbed())
}

pub(crate) fn deploy_contract_byte_send_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx)
        .add_action(deploy_action(ActionSize::Max))
        .inner_iters(1) // circumvent TX size limit
        // parameter today: 6_812_999
        // typical estimation: < 100_000 (<100ps)
        // we expect this to stay near 0
        .min_gas(GAS_100_PICOSECONDS)
        .verify_cost(&mut ctx.testbed())
        / ActionSize::Max.deploy_contract()
}

pub(crate) fn deploy_contract_byte_send_not_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new(ctx)
        .add_action(deploy_action(ActionSize::Max))
        .inner_iters(1) // circumvent TX size limit
        // parameter today: 6_812_999
        // typical estimation: < 100_000 (<100ps)
        // we expect this to stay near 0
        .min_gas(GAS_100_PICOSECONDS)
        .verify_cost(&mut ctx.testbed())
        / ActionSize::Max.deploy_contract()
}

pub(crate) fn deploy_contract_byte_exec(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx)
        .add_action(deploy_action(ActionSize::Max))
        .inner_iters(1) // circumvent TX size limit
        // parameter today: 64_572_944
        // typical estimation: 8_000_000 (8ns)
        // setting limit to 1ns because 1us would be too high
        .min_gas(GAS_1_NANOSECOND)
        .apply_cost(&mut ctx.testbed())
        / ActionSize::Max.deploy_contract()
}

pub(crate) fn function_call_base_send_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx)
        .add_action(function_call_action(ActionSize::Min))
        .verify_cost(&mut ctx.testbed())
}

pub(crate) fn function_call_base_send_not_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new(ctx)
        .add_action(function_call_action(ActionSize::Min))
        .verify_cost(&mut ctx.testbed())
}

pub(crate) fn function_call_base_exec(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx)
        .add_action(function_call_action(ActionSize::Min))
        .apply_cost(&mut ctx.testbed())
}

pub(crate) fn function_call_byte_send_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx)
        .add_action(function_call_action(ActionSize::Max))
        // parameter today: 2_235_934
        // typical estimation: 1_000 - 2_000 (1-2ns)
        // setting limit to 1ns because lower values are mostly noisy
        .min_gas(GAS_1_NANOSECOND)
        .verify_cost(&mut ctx.testbed())
        / ActionSize::Max.function_call_payload()
}

pub(crate) fn function_call_byte_send_not_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new(ctx)
        .add_action(function_call_action(ActionSize::Max))
        // parameter today: 2_235_934
        // typical estimation: 1_000 - 2_000 (1-2ns)
        // setting limit to 1ns because lower values are mostly noisy
        .min_gas(GAS_1_NANOSECOND)
        .verify_cost(&mut ctx.testbed())
        / ActionSize::Max.function_call_payload()
}

pub(crate) fn function_call_byte_exec(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx)
        .add_action(function_call_action(ActionSize::Max))
        // parameter today: 2_235_934
        // typical estimation: 2_000_000 (2ns)
        // setting limit to 1ns because lower values are mostly noisy
        .min_gas(GAS_1_NANOSECOND)
        .apply_cost(&mut ctx.testbed())
        / ActionSize::Max.function_call_payload()
}

pub(crate) fn transfer_send_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx).add_action(transfer_action()).verify_cost(&mut ctx.testbed())
}

pub(crate) fn transfer_send_not_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new(ctx).add_action(transfer_action()).verify_cost(&mut ctx.testbed())
}

pub(crate) fn transfer_exec(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx).add_action(transfer_action()).apply_cost(&mut ctx.testbed())
}

pub(crate) fn stake_send_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx).add_action(stake_action()).verify_cost(&mut ctx.testbed())
}

/// This is not a useful action, as staking only works with sender = receiver.
/// But since this fails only in the exec step, we must still charge a fitting
/// amount of gas in the send step.
pub(crate) fn stake_send_not_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new(ctx)
        .add_action(stake_action())
        .predecessor(AccountRequirement::SameAsSigner)
        .verify_cost(&mut ctx.testbed())
}

pub(crate) fn stake_exec(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx)
        .add_action(stake_action())
        .predecessor(AccountRequirement::SameAsSigner)
        .receiver(AccountRequirement::SameAsSigner) // staking must be local
        .apply_cost(&mut ctx.testbed())
}

pub(crate) fn add_full_access_key_send_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx)
        .add_action(add_full_access_key_action())
        .verify_cost(&mut ctx.testbed())
}

pub(crate) fn add_full_access_key_send_not_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new(ctx)
        .add_action(add_full_access_key_action())
        .verify_cost(&mut ctx.testbed())
}

pub(crate) fn add_full_access_key_exec(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx)
        .add_action(add_full_access_key_action())
        .inner_iters(1) // adding the same key a second time would fail
        .apply_cost(&mut ctx.testbed())
}

pub(crate) fn add_function_call_key_base_send_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx)
        .add_action(add_fn_access_key_action(ActionSize::Min))
        .verify_cost(&mut ctx.testbed())
}

pub(crate) fn add_function_call_key_base_send_not_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new(ctx)
        .add_action(add_fn_access_key_action(ActionSize::Min))
        .verify_cost(&mut ctx.testbed())
}

pub(crate) fn add_function_call_key_base_exec(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx)
        .add_action(add_fn_access_key_action(ActionSize::Min))
        .inner_iters(1) // adding the same key a second time would fail
        .apply_cost(&mut ctx.testbed())
}

pub(crate) fn add_function_call_key_byte_send_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx)
        .add_action(add_fn_access_key_action(ActionSize::Max))
        // parameter today: 1_925_331
        // typical estimation: < 200_000 (<200ps)
        // we expect this to stay near 0
        .min_gas(GAS_100_PICOSECONDS)
        .verify_cost(&mut ctx.testbed())
        / ActionSize::Max.key_methods_list()
}

pub(crate) fn add_function_call_key_byte_send_not_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new(ctx)
        .add_action(add_fn_access_key_action(ActionSize::Max))
        // parameter today: 1_925_331
        // typical estimation: < 200_000 (<200ps)
        // we expect this to stay near 0
        .min_gas(GAS_100_PICOSECONDS)
        .verify_cost(&mut ctx.testbed())
        / ActionSize::Max.key_methods_list()
}

pub(crate) fn add_function_call_key_byte_exec(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx)
        .add_action(add_fn_access_key_action(ActionSize::Max))
        .inner_iters(1) // adding the same key a second time would fail
        // parameter today: 1_925_331
        // typical estimation: 18_000_000 (18ns)
        // (we know this is undercharged but it's not a concern as described in #6716)
        // setting limit to 1ns to keep it lower than the parameter
        .min_gas(GAS_1_NANOSECOND)
        .apply_cost(&mut ctx.testbed())
        / ActionSize::Max.key_methods_list()
}

pub(crate) fn delete_key_send_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx).add_action(delete_key_action()).verify_cost(&mut ctx.testbed())
}

pub(crate) fn delete_key_send_not_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new(ctx).add_action(delete_key_action()).verify_cost(&mut ctx.testbed())
}

pub(crate) fn delete_key_exec(ctx: &mut EstimatorContext) -> GasCost {
    // Cannot delete a key without creating it first. Therefore, compute cost of
    // (create) and of (create + delete) and return the difference.
    let base_builder = ActionEstimation::new_sir(ctx)
        .inner_iters(1)
        .add_action(add_fn_access_key_action(ActionSize::Max));
    let base = base_builder.apply_cost(&mut ctx.testbed());
    let total = base_builder
        .add_action(delete_key_action())
        .inner_iters(100)
        .apply_cost(&mut ctx.testbed());

    total - base
}

pub(crate) fn new_action_receipt_send_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx)
        .inner_iters(1) // inner iterations don't work with empty action lists
        .subtract_base(false)
        .verify_cost(&mut ctx.testbed())
}

pub(crate) fn new_action_receipt_send_not_sir(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new(ctx)
        .inner_iters(1) // inner iterations don't work with empty action lists
        .subtract_base(false)
        .verify_cost(&mut ctx.testbed())
}

pub(crate) fn new_action_receipt_exec(ctx: &mut EstimatorContext) -> GasCost {
    ActionEstimation::new_sir(ctx)
        .inner_iters(1) // inner iterations don't work with empty action lists
        .subtract_base(false)
        .apply_cost(&mut ctx.testbed())
}

pub(crate) fn delegate_send_sir(ctx: &mut EstimatorContext) -> GasCost {
    let receiver_id: AccountId = "a".repeat(AccountId::MAX_LEN).parse().unwrap();
    let sender_id: AccountId = genesis_populate::get_account_id(0);

    ActionEstimation::new_sir(ctx)
        .add_action(empty_delegate_action(0, receiver_id, sender_id))
        // only single delegate action is allowed
        .inner_iters(1)
        // but then the variance is too high to report the average
        .report_percentile(0.75)
        // If the percentile of a distribution is reported, the typical
        // number of outer iterations (3-5) is just too small. Let's do at
        // least 60 iterations, this way the 15th largest ~ 75th percentile.
        // (This estimations is super fast, 60 iterations are quick)
        .min_outer_iters(60)
        .verify_cost(&mut ctx.testbed())
}

pub(crate) fn delegate_send_not_sir(ctx: &mut EstimatorContext) -> GasCost {
    let receiver_id: AccountId = "a".repeat(AccountId::MAX_LEN).parse().unwrap();
    let sender_id: AccountId = genesis_populate::get_account_id(0);
    ActionEstimation::new(ctx)
        .add_action(empty_delegate_action(0, receiver_id, sender_id))
        // only single delegate action is allowed
        .inner_iters(1)
        // but then the variance is too high to report the average
        .report_percentile(0.75)
        // If the percentile of a distribution is reported, the typical
        // number of outer iterations (3-5) is just too small. Let's do at
        // least 60 iterations, this way the 15th largest ~ 75th percentile.
        // (This estimations is super fast, 60 iterations are quick)
        .min_outer_iters(60)
        .verify_cost(&mut ctx.testbed())
}

pub(crate) fn delegate_exec(ctx: &mut EstimatorContext) -> GasCost {
    let receiver_id: AccountId = "a".repeat(AccountId::MAX_LEN).parse().unwrap();
    let sender_id: AccountId = genesis_populate::get_account_id(0);
    let mut builder = ActionEstimation::new_sir(ctx)
        // nonce check would fail with cloned actions, therefore inner iterations don't work
        .inner_iters(1)
        // tx receiver must be the same as the meta tx signer, we can't just
        // pick one at random or it will fail validation
        .receiver(AccountRequirement::ConstantAccount0);
    // manually make inner iterations by creating 100 actions with increasing nonces
    let manual_inner_iters = 100;
    for i in 0..manual_inner_iters {
        builder =
            builder.add_action(empty_delegate_action(i + 1, receiver_id.clone(), sender_id.clone()))
    }
    builder.apply_cost(&mut ctx.testbed()) / manual_inner_iters
}

fn create_account_action() -> Action {
    Action::CreateAccount(near_primitives::transaction::CreateAccountAction {})
}

fn create_transfer_action() -> Action {
    Action::Transfer(near_primitives::transaction::TransferAction { deposit: 10u128.pow(24) })
}

fn stake_action() -> Action {
    Action::Stake(near_primitives::transaction::StakeAction {
        stake: 5u128.pow(28), // some arbitrary positive number
        public_key: PublicKey::from_seed(KeyType::ED25519, "seed"),
    })
}

fn delete_account_action() -> Action {
    Action::DeleteAccount(near_primitives::transaction::DeleteAccountAction {
        beneficiary_id: "bob.near".parse().unwrap(),
    })
}

fn deploy_action(size: ActionSize) -> Action {
    Action::DeployContract(near_primitives::transaction::DeployContractAction {
        code: near_test_contracts::sized_contract(size.deploy_contract() as usize),
    })
}

fn add_full_access_key_action() -> Action {
    Action::AddKey(near_primitives::transaction::AddKeyAction {
        public_key: PublicKey::from_seed(KeyType::ED25519, "full-access-key-seed"),
        access_key: AccessKey { nonce: 0, permission: AccessKeyPermission::FullAccess },
    })
}

fn add_fn_access_key_action(size: ActionSize) -> Action {
    // 3 bytes for "foo" and one for an implicit separator
    let method_names = vec!["foo".to_owned(); size.key_methods_list() as usize / 4];
    // This is charged flat, therefore it should always be max len.
    let receiver_id = "a".repeat(AccountId::MAX_LEN).parse().unwrap();
    Action::AddKey(near_primitives::transaction::AddKeyAction {
        public_key: PublicKey::from_seed(KeyType::ED25519, "seed"),
        access_key: AccessKey {
            nonce: 0,
            permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                allowance: Some(1),
                receiver_id,
                method_names,
            }),
        },
    })
}

fn delete_key_action() -> Action {
    Action::DeleteKey(near_primitives::transaction::DeleteKeyAction {
        public_key: PublicKey::from_seed(KeyType::ED25519, "seed"),
    })
}

fn transfer_action() -> Action {
    Action::Transfer(near_primitives::transaction::TransferAction { deposit: 77 })
}

fn function_call_action(size: ActionSize) -> Action {
    let total_size = size.function_call_payload();
    let method_len = 4.min(total_size) as usize;
    let method_name: String = "noop".chars().take(method_len).collect();
    let arg_len = total_size as usize - method_len;
    Action::FunctionCall(near_primitives::transaction::FunctionCallAction {
        method_name,
        args: vec![1u8; arg_len],
        gas: 3 * 10u64.pow(12), // 3 Tgas, to allow 100 copies in the same receipt
        deposit: 10u128.pow(24),
    })
}

pub(crate) fn empty_delegate_action(
    nonce: u64,
    receiver_id: AccountId,
    sender_id: AccountId,
) -> Action {
    use near_primitives::delegate_action::DelegateAction;
    use near_primitives::signable_message::{SignableMessage, SignableMessageType};
    use near_primitives::test_utils::create_user_test_signer;

    let signer = create_user_test_signer(&sender_id);
    let delegate_action = DelegateAction {
        sender_id,
        receiver_id,
        actions: vec![],
        nonce,
        max_block_height: 1000,
        public_key: signer.public_key.clone(),
    };
    let signature =
        SignableMessage::new(&delegate_action, SignableMessageType::DelegateAction).sign(&signer);
    Action::Delegate(near_primitives::delegate_action::SignedDelegateAction {
        delegate_action,
        signature,
    })
}

/// Helper enum to select how large an action should be generated.
#[derive(Clone, Copy)]
enum ActionSize {
    Min,
    Max,
}

impl ActionSize {
    fn function_call_payload(self) -> u64 {
        match self {
            // calling "noop" requires 4 bytes
            ActionSize::Min => 4,
            // max_arguments_length: 4_194_304
            // max_transaction_size: 4_194_304
            ActionSize::Max => (4_194_304 / 100) - 35,
        }
    }

    fn key_methods_list(self) -> u64 {
        match self {
            ActionSize::Min => 0,
            // max_number_bytes_method_names: 2000
            ActionSize::Max => 2000,
        }
    }

    fn deploy_contract(self) -> u64 {
        match self {
            // small number that still allows to generate a valid contract
            ActionSize::Min => 120,
            // max_number_bytes_method_names: 2000
            // This size exactly touches tx limit with 1 deploy action. If this suddenly
            // fails with `InvalidTxError(TransactionSizeExceeded`, it could be a
            // protocol change due to the TX limit computation changing.
            // The test `test_deploy_contract_tx_max_size` checks this.
            ActionSize::Max => 4 * 1024 * 1024 - 182,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{deploy_action, ActionSize};
    use genesis_populate::get_account_id;

    #[test]
    fn test_deploy_contract_tx_max_size() {
        // The size of a transaction constructed from this must be exactly at the limit.
        let deploy_action = deploy_action(ActionSize::Max);
        let limit = 4_194_304;

        // We also need some account IDs constructed the same way as in the estimator.
        // Let's try multiple index sizes to ensure this does not affect the length.
        let sender_0 = get_account_id(0);
        let receiver_0 = get_account_id(1);
        let sender_1 = get_account_id(1000);
        let receiver_1 = get_account_id(20001);
        let test_accounts =
            vec![sender_0.clone(), sender_1.clone(), receiver_0.clone(), receiver_1.clone()];
        let mut tb = crate::TransactionBuilder::new(test_accounts);

        let tx_0 = tb.transaction_from_actions(sender_0, receiver_0, vec![deploy_action.clone()]);
        assert_eq!(tx_0.get_size(), limit, "TX size changed");

        let tx_1 = tb.transaction_from_actions(sender_1, receiver_1, vec![deploy_action]);
        assert_eq!(tx_1.get_size(), limit, "TX size changed");
    }
}

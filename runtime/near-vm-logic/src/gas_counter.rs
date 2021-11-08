use crate::{HostError, VMLogicError};
use near_primitives_core::runtime::fees::Fee;
use near_primitives_core::{
    config::{ActionCosts, ExtCosts, ExtCostsConfig},
    profile::ProfileData,
    types::Gas,
};
use std::collections::HashMap;
use std::fmt;

#[inline]
pub fn with_ext_cost_counter(f: impl FnOnce(&mut HashMap<ExtCosts, u64>)) {
    #[cfg(feature = "costs_counting")]
    {
        thread_local! {
            static EXT_COSTS_COUNTER: std::cell::RefCell<HashMap<ExtCosts, u64>> =
                Default::default();
        }
        EXT_COSTS_COUNTER.with(|rc| f(&mut *rc.borrow_mut()));
    }
    #[cfg(not(feature = "costs_counting"))]
    let _ = f;
}

type Result<T> = ::std::result::Result<T, VMLogicError>;

/// Fast gas counter with very simple structure, could be exposed to compiled code in the VM.
#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FastGasCounter {
    /// The following three fields must be put next to another to make sure
    /// generated gas counting code can use and adjust them.
    /// We will share counter to ensure we never miss synchronization.
    /// This could change and in such a case synchronization required between compiled WASM code
    /// and the host code.

    /// The amount of gas that was irreversibly used for contract execution.
    burnt_gas: u64,
    /// Hard gas limit for execution
    gas_limit: u64,
    /// Single WASM opcode cost
    opcode_cost: u64,
}

/// Gas counter (a part of VMlogic)
pub struct GasCounter {
    /// Shared gas counter data.
    fast_counter: FastGasCounter,
    /// Gas that was attached to the promises.
    promises_gas: Gas,
    /// Hard gas limit for execution
    max_gas_burnt: Gas,
    /// Amount of prepaid gas, we can never burn more than prepaid amount
    prepaid_gas: Gas,
    /// If this is a view-only call.
    is_view: bool,
    ext_costs_config: ExtCostsConfig,
    /// Where to store profile data, if needed.
    profile: ProfileData,
}

impl fmt::Debug for GasCounter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("").finish()
    }
}

impl GasCounter {
    pub fn new(
        ext_costs_config: ExtCostsConfig,
        max_gas_burnt: Gas,
        opcode_cost: u32,
        prepaid_gas: Gas,
        is_view: bool,
    ) -> Self {
        use std::cmp::min;
        Self {
            ext_costs_config,
            fast_counter: FastGasCounter {
                burnt_gas: 0,
                gas_limit: if is_view {
                    // Ignore prepaid gas limit and promises.
                    max_gas_burnt
                } else {
                    min(max_gas_burnt, prepaid_gas)
                },
                opcode_cost: Gas::from(opcode_cost),
            },
            max_gas_burnt: max_gas_burnt,
            promises_gas: 0,
            prepaid_gas,
            is_view,
            profile: Default::default(),
        }
    }

    fn deduct_gas(&mut self, burn_gas: Gas, use_gas: Gas) -> Result<()> {
        assert!(burn_gas <= use_gas);
        let promise_gas = use_gas - burn_gas;
        let new_promises_gas =
            self.promises_gas.checked_add(promise_gas).ok_or(HostError::IntegerOverflow)?;
        let new_burnt_gas =
            self.fast_counter.burnt_gas.checked_add(burn_gas).ok_or(HostError::IntegerOverflow)?;
        let new_used_gas =
            new_burnt_gas.checked_add(new_promises_gas).ok_or(HostError::IntegerOverflow)?;
        if new_burnt_gas <= self.max_gas_burnt && (self.is_view || new_used_gas <= self.prepaid_gas)
        {
            use std::cmp::min;
            if promise_gas != 0 && !self.is_view {
                self.fast_counter.gas_limit =
                    min(self.max_gas_burnt, self.prepaid_gas - new_promises_gas);
            }
            self.fast_counter.burnt_gas = new_burnt_gas;
            self.promises_gas = new_promises_gas;
            Ok(())
        } else {
            Err(self.process_gas_limit(new_burnt_gas, new_used_gas))
        }
    }

    // Optimized version of above function for cases where no promises involved.
    pub fn burn_gas(&mut self, value: Gas) -> Result<()> {
        let new_burnt_gas =
            self.fast_counter.burnt_gas.checked_add(value).ok_or(HostError::IntegerOverflow)?;
        if new_burnt_gas <= self.fast_counter.gas_limit {
            self.fast_counter.burnt_gas = new_burnt_gas;
            Ok(())
        } else {
            Err(self.process_gas_limit(new_burnt_gas, new_burnt_gas + self.promises_gas))
        }
    }

    fn process_gas_limit(&mut self, new_burnt_gas: Gas, new_used_gas: Gas) -> VMLogicError {
        use std::cmp::min;
        if new_used_gas > self.prepaid_gas {
            // Ensure that contract never burn more than `max_gas_burnt`, even if paid for more.
            let hard_burnt_limit = min(self.prepaid_gas, self.max_gas_burnt);
            self.fast_counter.burnt_gas = min(new_burnt_gas, hard_burnt_limit);
            // Technically we shall do `self.promises_gas = 0;` or error paths, as in this case
            // no promises will be kept, but that would mean protocol change.
            // See https://github.com/near/nearcore/issues/5148.
            // TODO: consider making this change!
            assert!(hard_burnt_limit >= self.fast_counter.burnt_gas);
            let used_gas_limit = min(self.prepaid_gas, new_used_gas);
            assert!(used_gas_limit >= self.fast_counter.burnt_gas);
            self.promises_gas = used_gas_limit - self.fast_counter.burnt_gas;
            HostError::GasExceeded.into()
        } else {
            self.fast_counter.burnt_gas = min(new_burnt_gas, self.max_gas_burnt);
            self.promises_gas = new_used_gas - self.fast_counter.burnt_gas;
            HostError::GasLimitExceeded.into()
        }
    }

    pub fn pay_wasm_gas(&mut self, opcodes: u32) -> Result<()> {
        let value = Gas::from(opcodes) * self.fast_counter.opcode_cost;
        self.burn_gas(value)
    }

    #[inline]
    fn inc_ext_costs_counter(&mut self, cost: ExtCosts, value: u64) {
        with_ext_cost_counter(|cc| *cc.entry(cost).or_default() += value)
    }

    #[inline]
    fn update_profile_host(&mut self, cost: ExtCosts, value: u64) {
        self.profile.add_ext_cost(cost, value)
    }

    #[inline]
    fn update_profile_action(&mut self, action: ActionCosts, value: u64) {
        self.profile.add_action_cost(action, value)
    }

    /// A helper function to pay a multiple of a cost.
    pub fn pay_per(&mut self, cost: ExtCosts, num: u64) -> Result<()> {
        let use_gas = num
            .checked_mul(cost.value(&self.ext_costs_config))
            .ok_or(HostError::IntegerOverflow)?;

        self.inc_ext_costs_counter(cost, num);
        self.update_profile_host(cost, use_gas);
        self.burn_gas(use_gas)
    }

    /// A helper function to pay base cost gas.
    pub fn pay_base(&mut self, cost: ExtCosts) -> Result<()> {
        let base_fee = cost.value(&self.ext_costs_config);
        self.inc_ext_costs_counter(cost, 1);
        self.update_profile_host(cost, base_fee);
        self.burn_gas(base_fee)
    }

    /// A helper function to pay per byte gas fee for batching an action.
    /// # Args:
    /// * `per_byte_fee`: the fee per byte;
    /// * `num_bytes`: the number of bytes;
    /// * `sir`: whether the receiver_id is same as the current account ID;
    /// * `action`: what kind of action is charged for;
    pub fn pay_action_per_byte(
        &mut self,
        per_byte_fee: &Fee,
        num_bytes: u64,
        sir: bool,
        action: ActionCosts,
    ) -> Result<()> {
        let burn_gas =
            num_bytes.checked_mul(per_byte_fee.send_fee(sir)).ok_or(HostError::IntegerOverflow)?;
        let use_gas = burn_gas
            .checked_add(
                num_bytes.checked_mul(per_byte_fee.exec_fee()).ok_or(HostError::IntegerOverflow)?,
            )
            .ok_or(HostError::IntegerOverflow)?;
        self.update_profile_action(action, burn_gas);
        self.deduct_gas(burn_gas, use_gas)
    }

    /// A helper function to pay base cost gas fee for batching an action.
    /// # Args:
    /// * `base_fee`: base fee for the action;
    /// * `sir`: whether the receiver_id is same as the current account ID;
    /// * `action`: what kind of action is charged for;
    pub fn pay_action_base(
        &mut self,
        base_fee: &Fee,
        sir: bool,
        action: ActionCosts,
    ) -> Result<()> {
        let burn_gas = base_fee.send_fee(sir);
        let use_gas =
            burn_gas.checked_add(base_fee.exec_fee()).ok_or(HostError::IntegerOverflow)?;
        self.update_profile_action(action, burn_gas);
        self.deduct_gas(burn_gas, use_gas)
    }

    /// A helper function to pay base cost gas fee for batching an action.
    /// # Args:
    /// * `burn_gas`: amount of gas to burn;
    /// * `use_gas`: amount of gas to reserve;
    /// * `action`: what kind of action is charged for;
    pub fn pay_action_accumulated(
        &mut self,
        burn_gas: Gas,
        use_gas: Gas,
        action: ActionCosts,
    ) -> Result<()> {
        self.update_profile_action(action, burn_gas);
        self.deduct_gas(burn_gas, use_gas)
    }

    pub fn prepay_gas(&mut self, use_gas: Gas) -> Result<()> {
        self.deduct_gas(0, use_gas)
    }

    pub fn burnt_gas(&self) -> Gas {
        self.fast_counter.burnt_gas
    }
    pub fn used_gas(&self) -> Gas {
        self.promises_gas + self.fast_counter.burnt_gas
    }

    pub fn profile_data(&self) -> ProfileData {
        self.profile.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_primitives_core::config::ExtCostsConfig;

    #[test]
    fn test_deduct_gas() {
        let mut counter = GasCounter::new(ExtCostsConfig::default(), 10, 1, 10, false);
        counter.deduct_gas(5, 10).expect("deduct_gas should work");
        assert_eq!(counter.burnt_gas(), 5);
        assert_eq!(counter.used_gas(), 10);
    }

    #[test]
    #[should_panic]
    fn test_prepaid_gas_min() {
        let mut counter = GasCounter::new(ExtCostsConfig::default(), 100, 1, 10, false);
        counter.deduct_gas(10, 5).unwrap();
    }
}

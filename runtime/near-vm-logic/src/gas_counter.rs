use crate::config::{ActionCosts, ExtCosts, ExtCostsConfig};
use crate::types::{Gas, ProfileData};
use crate::{HostError, VMLogicError};
use near_runtime_fees::Fee;

#[cfg(feature = "costs_counting")]
thread_local! {
    pub static EXT_COSTS_COUNTER: std::cell::RefCell<std::collections::HashMap<ExtCosts, u64>> =
        Default::default();
}

type Result<T> = ::std::result::Result<T, VMLogicError>;

/// Gas counter (a part of VMlogic)
pub struct GasCounter {
    /// The amount of gas that was irreversibly used for contract execution.
    burnt_gas: Gas,
    /// `burnt_gas` + gas that was attached to the promises.
    used_gas: Gas,
    /// Gas limit for execution
    max_gas_burnt: Gas,
    prepaid_gas: Gas,
    is_view: bool,
    ext_costs_config: ExtCostsConfig,
    /// Where to store profile data, if needed.
    profile: Option<ProfileData>,
}

impl GasCounter {
    pub fn new(
        ext_costs_config: ExtCostsConfig,
        max_gas_burnt: Gas,
        prepaid_gas: Gas,
        is_view: bool,
        profile: Option<ProfileData>,
    ) -> Self {
        Self {
            ext_costs_config,
            burnt_gas: 0,
            used_gas: 0,
            max_gas_burnt,
            prepaid_gas,
            is_view,
            profile,
        }
    }

    fn deduct_gas(&mut self, burn_gas: Gas, use_gas: Gas) -> Result<()> {
        assert!(burn_gas <= use_gas);
        let new_burnt_gas =
            self.burnt_gas.checked_add(burn_gas).ok_or(HostError::IntegerOverflow)?;
        let new_used_gas = self.used_gas.checked_add(use_gas).ok_or(HostError::IntegerOverflow)?;
        if new_burnt_gas <= self.max_gas_burnt && (self.is_view || new_used_gas <= self.prepaid_gas)
        {
            self.burnt_gas = new_burnt_gas;
            self.used_gas = new_used_gas;
            Ok(())
        } else {
            use std::cmp::min;
            let res = if new_burnt_gas > self.max_gas_burnt {
                Err(HostError::GasLimitExceeded.into())
            } else if new_used_gas > self.prepaid_gas {
                Err(HostError::GasExceeded.into())
            } else {
                unreachable!()
            };

            let max_burnt_gas = min(self.max_gas_burnt, self.prepaid_gas);
            self.burnt_gas = min(new_burnt_gas, max_burnt_gas);
            self.used_gas = min(new_used_gas, self.prepaid_gas);

            res
        }
    }

    #[cfg(feature = "costs_counting")]
    #[inline]
    fn inc_ext_costs_counter(&mut self, cost: ExtCosts, value: u64) {
        EXT_COSTS_COUNTER.with(|f| {
            *f.borrow_mut().entry(cost).or_default() += value;
        });
    }

    #[cfg(not(feature = "costs_counting"))]
    #[inline]
    fn inc_ext_costs_counter(&mut self, _cost: ExtCosts, _value: u64) {}

    #[cfg(feature = "costs_counting")]
    #[inline]
    fn update_profile_host(&mut self, cost: ExtCosts, value: u64) {
        match &self.profile {
            Some(profile) => *profile.borrow_mut().get_mut(cost as usize).unwrap() += value,
            None => {}
        };
    }

    #[cfg(not(feature = "costs_counting"))]
    #[inline]
    fn update_profile_host(&mut self, cost: ExtCosts, _value: u64) {}

    #[cfg(feature = "costs_counting")]
    #[inline]
    fn update_profile_action(&mut self, action: ActionCosts, value: u64) {
        match &self.profile {
            Some(profile) => {
                *profile.borrow_mut().get_mut(action as usize + ExtCosts::count()).unwrap() += value
            }
            None => {}
        };
    }

    #[cfg(not(feature = "costs_counting"))]
    #[inline]
    fn update_profile_action(&mut self, action: ActionCosts, _value: u64) {}

    #[cfg(feature = "costs_counting")]
    #[inline]
    fn update_profile_wasm(&mut self, value: u64) {
        match &self.profile {
            Some(profile) => {
                *profile.borrow_mut().get_mut(ActionCosts::count() + ExtCosts::count()).unwrap() +=
                    value
            }
            None => {}
        };
    }

    #[cfg(not(feature = "costs_counting"))]
    #[inline]
    fn update_profile_wasm(&mut self, _value: u64) {}

    pub fn pay_wasm_gas(&mut self, value: u64) -> Result<()> {
        self.update_profile_wasm(value);
        self.deduct_gas(value, value)
    }

    /// A helper function to pay per byte gas
    pub fn pay_per_byte(&mut self, cost: ExtCosts, num_bytes: u64) -> Result<()> {
        let use_gas = num_bytes
            .checked_mul(cost.value(&self.ext_costs_config))
            .ok_or(HostError::IntegerOverflow)?;

        self.inc_ext_costs_counter(cost, num_bytes);
        self.update_profile_host(cost, use_gas);
        self.deduct_gas(use_gas, use_gas)
    }

    /// A helper function to pay base cost gas
    pub fn pay_base(&mut self, cost: ExtCosts) -> Result<()> {
        let base_fee = cost.value(&self.ext_costs_config);
        self.inc_ext_costs_counter(cost, 1);
        self.update_profile_host(cost, base_fee);
        self.deduct_gas(base_fee, base_fee)
    }

    /// A helper function to pay per byte gas fee for batching an action.
    /// # Args:
    /// * `per_byte_fee`: the fee per byte;
    /// * `num_bytes`: the number of bytes;
    /// * `sir`: whether the receiver_id is same as the current account ID;
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
        self.burnt_gas
    }
    pub fn used_gas(&self) -> Gas {
        self.used_gas
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_deduct_gas() {
        let mut counter = GasCounter::new(ExtCostsConfig::default(), 10, 10, false, None);
        counter.deduct_gas(5, 10).expect("deduct_gas should work");
        assert_eq!(counter.burnt_gas(), 5);
        assert_eq!(counter.used_gas(), 10);
    }

    #[test]
    #[should_panic]
    fn test_prepaid_gas_min() {
        let mut counter = GasCounter::new(ExtCostsConfig::default(), 100, 10, false, None);
        counter.deduct_gas(10, 5).unwrap();
    }
}

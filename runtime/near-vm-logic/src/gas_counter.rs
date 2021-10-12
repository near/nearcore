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
                gas_limit: min(max_gas_burnt, prepaid_gas),
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
        let new_promises_gas =
            self.promises_gas.checked_add(use_gas - burn_gas).ok_or(HostError::IntegerOverflow)?;
        let new_burnt_gas =
            self.fast_counter.burnt_gas.checked_add(burn_gas).ok_or(HostError::IntegerOverflow)?;
        let new_used_gas =
            new_burnt_gas.checked_add(new_promises_gas).ok_or(HostError::IntegerOverflow)?;
        if new_burnt_gas <= self.max_gas_burnt && (self.is_view || new_used_gas <= self.prepaid_gas)
        {
            self.fast_counter.burnt_gas = new_burnt_gas;
            self.promises_gas = new_promises_gas;
            Ok(())
        } else {
            use std::cmp::min;
            let res = if new_burnt_gas > self.max_gas_burnt {
                self.fast_counter.burnt_gas = self.max_gas_burnt;
                Err(HostError::GasLimitExceeded.into())
            } else if new_used_gas > self.prepaid_gas {
                self.fast_counter.burnt_gas = min(self.prepaid_gas, self.fast_counter.gas_limit);
                Err(HostError::GasExceeded.into())
            } else {
                unreachable!()
            };

            self.promises_gas = min(new_used_gas, self.prepaid_gas) - self.fast_counter.burnt_gas;

            res
        }
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

    pub fn pay_wasm_gas(&mut self, opcodes: u32) -> Result<()> {
        let value = Gas::from(opcodes) * self.fast_counter.opcode_cost;
        let new_burnt_gas =
            self.fast_counter.burnt_gas.checked_add(value).ok_or(HostError::IntegerOverflow)?;
        if new_burnt_gas <= self.fast_counter.gas_limit {
            self.fast_counter.burnt_gas = new_burnt_gas;
            Ok(())
        } else {
            self.fast_counter.burnt_gas = self.fast_counter.gas_limit;
            if self.fast_counter.gas_limit == self.max_gas_burnt {
                Err(HostError::GasLimitExceeded.into())
            } else {
                Err(HostError::GasExceeded.into())
            }
        }
    }

    /// Very special function to get the gas counter pointer for generated machine code.
    /// Please do not use, unless fully understand Rust aliasing and other consequences.
    /// Can be used to emit inlined code like `pay_wasm_gas()`, i.e.
    ///    mov base, gas_counter_raw_ptr
    ///    mov rax, [base] ; current burnt gas
    ///    mov rcx, [base + 16] ; opcode cost
    ///    imul rcx, block_ops_count ; block cost
    ///    add rax, rcx ; new burnt gas
    ///    jo emit_integer_overflow
    ///    cmp rax, [base + 8] ; unsigned compare with burnt limit
    ///    ja emit_gas_exceeded
    ///    mov [base], rax
    pub fn gas_counter_raw_ptr(&mut self) -> *mut FastGasCounter {
        use std::ptr;
        ptr::addr_of_mut!(self.fast_counter)
    }

    /// A helper function to pay a multiple of a cost.
    pub fn pay_per(&mut self, cost: ExtCosts, num: u64) -> Result<()> {
        let use_gas = num
            .checked_mul(cost.value(&self.ext_costs_config))
            .ok_or(HostError::IntegerOverflow)?;

        self.inc_ext_costs_counter(cost, num);
        self.update_profile_host(cost, use_gas);
        self.deduct_gas(use_gas, use_gas)
    }

    /// A helper function to pay base cost gas.
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

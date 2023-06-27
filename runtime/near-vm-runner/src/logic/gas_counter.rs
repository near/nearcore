use super::errors::{HostError, VMLogicError};
use super::TrieNodesCount;
use near_primitives_core::config::ExtCosts::read_cached_trie_node;
use near_primitives_core::config::ExtCosts::touching_trie_node;
use near_primitives_core::{
    config::{ActionCosts, ExtCosts, ExtCostsConfig},
    profile::ProfileDataV3,
    types::Gas,
};
use std::collections::HashMap;

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

/// Fast gas counter with very simple structure, could be exposed to compiled code in the VM. For
/// instance by intrinsifying host functions responsible for gas metering.
#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FastGasCounter {
    /// The following three fields must be put next to another to make sure
    /// generated gas counting code can use and adjust them.
    /// We will share counter to ensure we never miss synchronization.
    /// This could change and in such a case synchronization required between compiled WASM code
    /// and the host code.

    /// The amount of gas that was irreversibly used for contract execution.
    pub burnt_gas: u64,
    /// Hard gas limit for execution
    pub gas_limit: u64,
    /// Cost for one opcode. Used only by VMs preceding near_vm.
    pub opcode_cost: u64,
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
    /// FIXME(nagisa): why do we store a copy both here and in the VMLogic???
    ext_costs_config: ExtCostsConfig,
    /// Where to store profile data, if needed.
    profile: ProfileDataV3,
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
        // Ignore prepaid gas limit when in view.
        let prepaid_gas = if is_view { Gas::MAX } else { prepaid_gas };
        Self {
            ext_costs_config,
            fast_counter: FastGasCounter {
                burnt_gas: 0,
                gas_limit: min(max_gas_burnt, prepaid_gas),
                opcode_cost: Gas::from(opcode_cost),
            },
            max_gas_burnt,
            promises_gas: 0,
            prepaid_gas,
            is_view,
            profile: Default::default(),
        }
    }

    /// Deducts burnt and used gas.
    ///
    /// Returns an error if the `max_gax_burnt` or the `prepaid_gas` limits are
    /// crossed or there are arithmetic overflows.
    ///
    /// Panics
    ///
    /// This function asserts that `gas_burnt <= gas_used`
    fn deduct_gas(&mut self, gas_burnt: Gas, gas_used: Gas) -> Result<()> {
        assert!(gas_burnt <= gas_used);
        let promises_gas = gas_used - gas_burnt;
        let new_promises_gas =
            self.promises_gas.checked_add(promises_gas).ok_or(HostError::IntegerOverflow)?;
        let new_burnt_gas =
            self.fast_counter.burnt_gas.checked_add(gas_burnt).ok_or(HostError::IntegerOverflow)?;
        let new_used_gas =
            new_burnt_gas.checked_add(new_promises_gas).ok_or(HostError::IntegerOverflow)?;
        if new_burnt_gas <= self.max_gas_burnt && new_used_gas <= self.prepaid_gas {
            use std::cmp::min;
            if promises_gas != 0 && !self.is_view {
                self.fast_counter.gas_limit =
                    min(self.max_gas_burnt, self.prepaid_gas - new_promises_gas);
            }
            self.fast_counter.burnt_gas = new_burnt_gas;
            self.promises_gas = new_promises_gas;
            Ok(())
        } else {
            Err(self.process_gas_limit(new_burnt_gas, new_used_gas).into())
        }
    }

    /// Simpler version of `deduct_gas()` for when no promises are involved.
    ///
    /// Return an error if there are arithmetic overflows.
    pub fn burn_gas(&mut self, gas_burnt: Gas) -> Result<()> {
        let new_burnt_gas =
            self.fast_counter.burnt_gas.checked_add(gas_burnt).ok_or(HostError::IntegerOverflow)?;
        if new_burnt_gas <= self.fast_counter.gas_limit {
            self.fast_counter.burnt_gas = new_burnt_gas;
            Ok(())
        } else {
            // In the past `new_used_gas` would be computed using an implicit wrapping addition,
            // which would then give an opportunity for the `assert` (now `debug_assert`) in the
            // callee to fail, leading to a DoS of a node. A wrapping_add in this instance is
            // actually fine, even if it gives the attacker full control of the value passed in
            // here…
            //
            // [CONTINUATION IN THE NEXT COMMENT]
            let new_used_gas = new_burnt_gas.wrapping_add(self.promises_gas);
            Err(self.process_gas_limit(new_burnt_gas, new_used_gas).into())
        }
    }

    pub fn process_gas_limit(&mut self, new_burnt_gas: Gas, new_used_gas: Gas) -> HostError {
        use std::cmp::min;
        // Never burn more gas than what was paid for.
        let hard_burnt_limit = min(self.prepaid_gas, self.max_gas_burnt);
        self.fast_counter.burnt_gas = min(new_burnt_gas, hard_burnt_limit);
        debug_assert!(hard_burnt_limit >= self.fast_counter.burnt_gas);

        // Technically we shall do `self.promises_gas = 0;` or error paths, as in this case
        // no promises will be kept, but that would mean protocol change.
        // See https://github.com/near/nearcore/issues/5148.
        // TODO: consider making this change!
        let used_gas_limit = min(self.prepaid_gas, new_used_gas);
        // [CONTINUATION OF THE PREVIOUS COMMENT]
        //
        // Now, there are two distinct ways an attacker can attempt to exploit this code given
        // their full control of the `new_used_gas` value.
        //
        // 1. `self.prepaid_gas < new_used_gas` This is perfectly fine and would be the happy path,
        //    were the computations performed with arbitrary precision integers all the time.
        // 2. `new_used_gas < new_burnt_gas` means that the `new_used_gas` computation wrapped
        //    and `used_gas_limit` is now set to a lower value than it otherwise should be. In the
        //    past this would have triggered an unconditional assert leading to nodes crashing and
        //    network getting stuck/going down. We don’t actually need to assert, though. By
        //    replacing the wrapping subtraction with a saturating one we make sure that the
        //    resulting value of `self.promises_gas` is well behaved (becomes 0.) All a potential
        //    attacker has achieved in this case is throwing some of their gas away.
        self.promises_gas = used_gas_limit.saturating_sub(self.fast_counter.burnt_gas);

        // If we crossed both limits prefer reporting GasLimitExceeded.
        // Alternative would be to prefer reporting limit that is lower (or
        // perhaps even some other heuristic) but old code preferred
        // GasLimitExceeded and we’re keeping compatibility with that.
        if new_burnt_gas > self.max_gas_burnt {
            HostError::GasLimitExceeded
        } else {
            HostError::GasExceeded
        }
    }

    /// Very special function to get the gas counter pointer for generated machine code.
    /// Please do not use, unless fully understand Rust aliasing and other consequences.
    /// Can be used to emit inlined code like `pay_wasm_gas()`, i.e.
    ///    mov base, gas_counter_raw_ptr
    ///    mov rax, [base + 0] ; current burnt gas
    ///    mov rcx, [base + 16] ; opcode cost
    ///    imul rcx, block_ops_count ; block cost
    ///    add rax, rcx ; new burnt gas
    ///    jo emit_integer_overflow
    ///    cmp rax, [base + 8] ; unsigned compare with burnt limit
    ///    mov [base + 0], rax
    ///    ja emit_gas_exceeded
    pub fn gas_counter_raw_ptr(&mut self) -> *mut FastGasCounter {
        use std::ptr;
        ptr::addr_of_mut!(self.fast_counter)
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
        let use_gas =
            num.checked_mul(cost.gas(&self.ext_costs_config)).ok_or(HostError::IntegerOverflow)?;

        self.inc_ext_costs_counter(cost, num);
        let old_burnt_gas = self.fast_counter.burnt_gas;
        let burn_gas_result = self.burn_gas(use_gas);
        self.update_profile_host(cost, self.fast_counter.burnt_gas.saturating_sub(old_burnt_gas));
        burn_gas_result
    }

    /// A helper function to pay base cost gas.
    pub fn pay_base(&mut self, cost: ExtCosts) -> Result<()> {
        let base_fee = cost.gas(&self.ext_costs_config);
        self.inc_ext_costs_counter(cost, 1);
        let old_burnt_gas = self.fast_counter.burnt_gas;
        let burn_gas_result = self.burn_gas(base_fee);
        self.update_profile_host(cost, self.fast_counter.burnt_gas.saturating_sub(old_burnt_gas));
        burn_gas_result
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
        let old_burnt_gas = self.fast_counter.burnt_gas;
        let deduct_gas_result = self.deduct_gas(burn_gas, use_gas);
        self.update_profile_action(
            action,
            self.fast_counter.burnt_gas.saturating_sub(old_burnt_gas),
        );
        deduct_gas_result
    }

    pub fn add_trie_fees(&mut self, count: &TrieNodesCount) -> Result<()> {
        self.pay_per(touching_trie_node, count.db_reads)?;
        self.pay_per(read_cached_trie_node, count.mem_reads)?;
        Ok(())
    }

    pub fn prepay_gas(&mut self, use_gas: Gas) -> Result<()> {
        self.deduct_gas(0, use_gas)
    }

    pub fn burnt_gas(&self) -> Gas {
        self.fast_counter.burnt_gas
    }

    /// Amount of gas used through promises and amount burned.
    pub fn used_gas(&self) -> Gas {
        self.promises_gas + self.fast_counter.burnt_gas
    }

    /// Remaining gas based on the amount of prepaid gas not yet used.
    pub fn unused_gas(&self) -> Gas {
        self.prepaid_gas - self.used_gas()
    }

    pub fn profile_data(&self) -> ProfileDataV3 {
        self.profile.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::{ExtCostsConfig, HostError};
    use near_primitives_core::types::Gas;

    /// Max prepaid amount of gas.
    const MAX_GAS: u64 = 300_000_000_000_000;

    fn make_test_counter(max_burnt: Gas, prepaid: Gas, is_view: bool) -> super::GasCounter {
        super::GasCounter::new(ExtCostsConfig::test(), max_burnt, 1, prepaid, is_view)
    }

    #[test]
    fn test_deduct_gas() {
        let mut counter = make_test_counter(10, 10, false);
        counter.deduct_gas(5, 10).expect("deduct_gas should work");
        assert_eq!(counter.burnt_gas(), 5);
        assert_eq!(counter.used_gas(), 10);
    }

    #[test]
    #[should_panic]
    fn test_burn_gas_must_be_lt_use_gas() {
        let _ = make_test_counter(10, 10, false).deduct_gas(5, 2);
    }

    #[test]
    #[should_panic]
    fn test_burn_gas_must_be_lt_use_gas_view() {
        let _ = make_test_counter(10, 10, true).deduct_gas(5, 2);
    }

    #[test]
    fn test_burn_too_much() {
        fn test(burn: Gas, prepaid: Gas, view: bool, want: Result<(), HostError>) {
            let mut counter = make_test_counter(burn, prepaid, view);
            assert_eq!(counter.burn_gas(5), Ok(()));
            assert_eq!(counter.burn_gas(3), want.map_err(Into::into));
        }

        test(5, 7, false, Err(HostError::GasLimitExceeded));
        test(5, 7, true, Err(HostError::GasLimitExceeded));
        test(5, 5, false, Err(HostError::GasLimitExceeded));
        test(5, 5, true, Err(HostError::GasLimitExceeded));
        test(7, 5, false, Err(HostError::GasLimitExceeded));
        test(7, 5, true, Err(HostError::GasLimitExceeded));
    }

    #[test]
    fn test_deduct_too_much() {
        fn test(burn: Gas, prepaid: Gas, view: bool, want: Result<(), HostError>) {
            let mut counter = make_test_counter(burn, prepaid, view);
            assert_eq!(counter.deduct_gas(5, 5), Ok(()));
            assert_eq!(counter.deduct_gas(3, 3), want.map_err(Into::into));
        }

        test(5, 7, false, Err(HostError::GasLimitExceeded));
        test(5, 7, true, Err(HostError::GasLimitExceeded));
        test(5, 5, false, Err(HostError::GasLimitExceeded));
        test(5, 5, true, Err(HostError::GasLimitExceeded));
        test(7, 5, false, Err(HostError::GasLimitExceeded));
        test(7, 5, true, Err(HostError::GasLimitExceeded));

        test(5, 8, false, Err(HostError::GasLimitExceeded));
        test(5, 8, true, Err(HostError::GasLimitExceeded));
        test(8, 5, false, Err(HostError::GasExceeded));
        test(8, 5, true, Ok(()));
    }

    #[test]
    fn test_profile_compute_cost_is_accurate() {
        let mut counter = make_test_counter(MAX_GAS, MAX_GAS, false);
        counter.pay_base(near_primitives_core::config::ExtCosts::storage_write_base).unwrap();
        counter
            .pay_per(near_primitives_core::config::ExtCosts::storage_write_value_byte, 10)
            .unwrap();
        counter
            .pay_action_accumulated(
                100,
                100,
                near_primitives_core::config::ActionCosts::new_data_receipt_byte,
            )
            .unwrap();

        let mut profile = counter.profile_data();
        profile.compute_wasm_instruction_cost(counter.burnt_gas());

        assert_eq!(profile.total_compute_usage(&ExtCostsConfig::test()), counter.burnt_gas());
    }

    #[test]
    fn test_profile_compute_cost_ext_over_limit() {
        fn test(burn: Gas, prepaid: Gas, want: Result<(), HostError>) {
            let mut counter = make_test_counter(burn, prepaid, false);
            assert_eq!(
                counter
                    .pay_per(near_primitives_core::config::ExtCosts::storage_write_value_byte, 100),
                want.map_err(Into::into)
            );
            let mut profile = counter.profile_data();
            profile.compute_wasm_instruction_cost(counter.burnt_gas());

            assert_eq!(profile.total_compute_usage(&ExtCostsConfig::test()), counter.burnt_gas());
        }

        test(MAX_GAS, 1_000_000_000, Err(HostError::GasExceeded));
        test(1_000_000_000, MAX_GAS, Err(HostError::GasLimitExceeded));
        test(1_000_000_000, 1_000_000_000, Err(HostError::GasLimitExceeded));
    }

    #[test]
    fn test_profile_compute_cost_action_over_limit() {
        fn test(burn: Gas, prepaid: Gas, want: Result<(), HostError>) {
            let mut counter = make_test_counter(burn, prepaid, false);
            assert_eq!(
                counter.pay_action_accumulated(
                    10_000_000_000,
                    10_000_000_000,
                    near_primitives_core::config::ActionCosts::new_data_receipt_byte
                ),
                want.map_err(Into::into)
            );
            let mut profile = counter.profile_data();
            profile.compute_wasm_instruction_cost(counter.burnt_gas());

            assert_eq!(profile.total_compute_usage(&ExtCostsConfig::test()), counter.burnt_gas());
        }

        test(MAX_GAS, 1_000_000_000, Err(HostError::GasExceeded));
        test(1_000_000_000, MAX_GAS, Err(HostError::GasLimitExceeded));
        test(1_000_000_000, 1_000_000_000, Err(HostError::GasLimitExceeded));
    }
}

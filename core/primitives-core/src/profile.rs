use std::fmt;
use std::ops::Index;

use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use crate::config::{ActionCosts, ExtCosts};

/// Profile of gas consumption.
/// Vecs are used for forward compatible. should only append new costs and never remove old costs
/// When add new cost, the new cost should also be append to ProfileData::ALL
#[derive(Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ProfileData {
    ext_costs: Vec<u64>,
    action_costs: Vec<u64>,
}

impl Default for ProfileData {
    fn default() -> ProfileData {
        ProfileData::new()
    }
}

impl ProfileData {
    #[inline]
    pub fn new() -> Self {
        let ext_costs = vec![0; ExtCosts::count()];
        let action_costs = vec![0; ActionCosts::count()];
        ProfileData { ext_costs, action_costs }
    }

    #[inline]
    pub fn merge(&mut self, other: &ProfileData) {
        for i in 0..ExtCosts::count() {
            self.ext_costs[i] = self.ext_costs[i].saturating_add(other.ext_costs[i]);
        }
        for i in 0..ActionCosts::count() {
            self.action_costs[i] = self.action_costs[i].saturating_add(other.action_costs[i]);
        }
    }

    #[inline]
    pub fn add_action_cost(&mut self, action: ActionCosts, value: u64) {
        self.action_costs[action as usize] =
            self.action_costs[action as usize].saturating_add(value);
    }

    #[inline]
    pub fn add_ext_cost(&mut self, ext: ExtCosts, value: u64) {
        self.ext_costs[ext as usize] = self.ext_costs[ext as usize].saturating_add(value);
    }

    pub fn get_action_cost(&self, action: usize) -> u64 {
        self.action_costs[action]
    }

    pub fn get_ext_cost(&self, ext: usize) -> u64 {
        self.ext_costs[ext]
    }

    pub fn host_gas(&self) -> u64 {
        let mut host_gas = 0u64;
        for e in 0..ExtCosts::count() {
            host_gas += self.get_ext_cost(e);
        }
        host_gas
    }

    pub fn action_gas(&self) -> u64 {
        let mut action_gas = 0u64;
        for e in 0..ActionCosts::count() {
            action_gas += self.get_action_cost(e)
        }
        action_gas
    }
}

impl fmt::Debug for ProfileData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use num_rational::Ratio;
        let host_gas = self.host_gas();
        let action_gas = self.action_gas();

        writeln!(f, "------------------------------")?;
        writeln!(f, "Action gas: {}", action_gas)?;
        writeln!(f, "------ Host functions --------")?;
        for e in 0..ExtCosts::count() {
            let d = self.get_ext_cost(e);
            if d != 0 {
                writeln!(
                    f,
                    "{} -> {} [{}% host]",
                    ExtCosts::name_of(e),
                    d,
                    Ratio::new(d * 100, host_gas).to_integer(),
                )?;
            }
        }
        writeln!(f, "------ Actions --------")?;
        for e in 0..ActionCosts::count() {
            let d = self.get_action_cost(e);
            if d != 0 {
                writeln!(f, "{} -> {}", ActionCosts::name_of(e), d,)?;
            }
        }
        writeln!(f, "------------------------------")?;
        Ok(())
    }
}

#[derive(
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Clone,
    BorshSerialize,
    BorshDeserialize,
    Debug,
)]
pub enum Cost {
    ExtCost(ExtCosts),
    ActionCost(ActionCosts),
}

impl Cost {
    const ALL: &'static [Cost] = &[
        Cost::ExtCost(ExtCosts::base),
        Cost::ExtCost(ExtCosts::contract_compile_base),
        Cost::ExtCost(ExtCosts::contract_compile_bytes),
        Cost::ExtCost(ExtCosts::read_memory_base),
        Cost::ExtCost(ExtCosts::read_memory_byte),
        Cost::ExtCost(ExtCosts::write_memory_base),
        Cost::ExtCost(ExtCosts::write_memory_byte),
        Cost::ExtCost(ExtCosts::read_register_base),
        Cost::ExtCost(ExtCosts::read_register_byte),
        Cost::ExtCost(ExtCosts::write_register_base),
        Cost::ExtCost(ExtCosts::write_register_byte),
        Cost::ExtCost(ExtCosts::utf8_decoding_base),
        Cost::ExtCost(ExtCosts::utf8_decoding_byte),
        Cost::ExtCost(ExtCosts::utf16_decoding_base),
        Cost::ExtCost(ExtCosts::utf16_decoding_byte),
        Cost::ExtCost(ExtCosts::sha256_base),
        Cost::ExtCost(ExtCosts::sha256_byte),
        Cost::ExtCost(ExtCosts::keccak256_base),
        Cost::ExtCost(ExtCosts::keccak256_byte),
        Cost::ExtCost(ExtCosts::keccak512_base),
        Cost::ExtCost(ExtCosts::keccak512_byte),
        Cost::ExtCost(ExtCosts::ripemd160_base),
        Cost::ExtCost(ExtCosts::ripemd160_block),
        Cost::ExtCost(ExtCosts::ecrecover_base),
        Cost::ExtCost(ExtCosts::log_base),
        Cost::ExtCost(ExtCosts::log_byte),
        Cost::ExtCost(ExtCosts::storage_write_base),
        Cost::ExtCost(ExtCosts::storage_write_key_byte),
        Cost::ExtCost(ExtCosts::storage_write_value_byte),
        Cost::ExtCost(ExtCosts::storage_write_evicted_byte),
        Cost::ExtCost(ExtCosts::storage_read_base),
        Cost::ExtCost(ExtCosts::storage_read_key_byte),
        Cost::ExtCost(ExtCosts::storage_read_value_byte),
        Cost::ExtCost(ExtCosts::storage_remove_base),
        Cost::ExtCost(ExtCosts::storage_remove_key_byte),
        Cost::ExtCost(ExtCosts::storage_remove_ret_value_byte),
        Cost::ExtCost(ExtCosts::storage_has_key_base),
        Cost::ExtCost(ExtCosts::storage_has_key_byte),
        Cost::ExtCost(ExtCosts::storage_iter_create_prefix_base),
        Cost::ExtCost(ExtCosts::storage_iter_create_prefix_byte),
        Cost::ExtCost(ExtCosts::storage_iter_create_range_base),
        Cost::ExtCost(ExtCosts::storage_iter_create_from_byte),
        Cost::ExtCost(ExtCosts::storage_iter_create_to_byte),
        Cost::ExtCost(ExtCosts::storage_iter_next_base),
        Cost::ExtCost(ExtCosts::storage_iter_next_key_byte),
        Cost::ExtCost(ExtCosts::storage_iter_next_value_byte),
        Cost::ExtCost(ExtCosts::touching_trie_node),
        Cost::ExtCost(ExtCosts::promise_and_base),
        Cost::ExtCost(ExtCosts::promise_and_per_promise),
        Cost::ExtCost(ExtCosts::promise_return),
        Cost::ExtCost(ExtCosts::validator_stake_base),
        Cost::ExtCost(ExtCosts::validator_total_stake_base),
        #[cfg(feature = "protocol_feature_alt_bn128")]
        Cost::ExtCost(ExtCosts::alt_bn128_g1_multiexp_base),
        #[cfg(feature = "protocol_feature_alt_bn128")]
        Cost::ExtCost(ExtCosts::alt_bn128_g1_multiexp_byte),
        #[cfg(feature = "protocol_feature_alt_bn128")]
        Cost::ExtCost(ExtCosts::alt_bn128_g1_multiexp_sublinear),
        #[cfg(feature = "protocol_feature_alt_bn128")]
        Cost::ExtCost(ExtCosts::alt_bn128_pairing_check_base),
        #[cfg(feature = "protocol_feature_alt_bn128")]
        Cost::ExtCost(ExtCosts::alt_bn128_pairing_check_byte),
        #[cfg(feature = "protocol_feature_alt_bn128")]
        Cost::ExtCost(ExtCosts::alt_bn128_g1_sum_base),
        #[cfg(feature = "protocol_feature_alt_bn128")]
        Cost::ExtCost(ExtCosts::alt_bn128_g1_sum_byte),
        Cost::ActionCost(ActionCosts::create_account),
        Cost::ActionCost(ActionCosts::delete_account),
        Cost::ActionCost(ActionCosts::deploy_contract),
        Cost::ActionCost(ActionCosts::function_call),
        Cost::ActionCost(ActionCosts::transfer),
        Cost::ActionCost(ActionCosts::stake),
        Cost::ActionCost(ActionCosts::add_key),
        Cost::ActionCost(ActionCosts::delete_key),
        Cost::ActionCost(ActionCosts::value_return),
        Cost::ActionCost(ActionCosts::new_receipt),
    ];
}

impl Index<&Cost> for ProfileData {
    type Output = u64;

    fn index(&self, index: &Cost) -> &Self::Output {
        match index {
            Cost::ExtCost(ext) => &self.ext_costs[*ext as usize],
            Cost::ActionCost(action) => &self.action_costs[*action as usize],
        }
    }
}

impl ProfileData {
    pub fn nonzero_costs(&self) -> Vec<(Cost, u64)> {
        let mut data = Vec::new();
        for i in Cost::ALL {
            if self[i] > 0 {
                data.push((i.clone(), self[i]));
            }
        }
        data
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_profile_data_debug() {
        let profile_data = ProfileData::new();
        println!("{:#?}", &profile_data);
    }

    #[test]
    fn test_profile_data_debug_no_data() {
        let profile_data = ProfileData::new();
        println!("{:#?}", &profile_data);
    }

    #[test]
    fn test_no_panic_on_overflow() {
        let mut profile_data = ProfileData::new();
        profile_data.add_action_cost(ActionCosts::function_call, u64::MAX);
        profile_data.add_action_cost(ActionCosts::function_call, u64::MAX);

        let res = profile_data.get_action_cost(ActionCosts::function_call as usize);
        assert_eq!(res, u64::MAX);
    }

    #[test]
    fn test_merge() {
        let mut profile_data = ProfileData::new();
        profile_data.add_action_cost(ActionCosts::function_call, 111);
        profile_data.add_ext_cost(ExtCosts::storage_read_base, 11);

        let mut profile_data2 = ProfileData::new();
        profile_data2.add_action_cost(ActionCosts::function_call, 222);
        profile_data2.add_ext_cost(ExtCosts::storage_read_base, 22);

        profile_data.merge(&profile_data2);
        assert_eq!(profile_data.get_action_cost(ActionCosts::function_call as usize), 333);
        assert_eq!(profile_data.get_ext_cost(ExtCosts::storage_read_base as usize), 33);
    }
}

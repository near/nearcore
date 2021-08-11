use std::collections::BTreeMap;
use std::fmt;
use std::ops::Index;

use borsh::{BorshDeserialize, BorshSerialize};
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};

use crate::config::{ActionCosts, ExtCosts};
use crate::types::Gas;

/// Profile of gas consumption.
/// Vecs are used for forward compatible. should only append new costs and never remove old costs
#[derive(Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ProfileData {
    all_gas: u64,
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
        let all_gas = 0;
        let ext_costs = vec![0; ExtCosts::count()];
        let action_costs = vec![0; ActionCosts::count()];
        ProfileData { all_gas, ext_costs, action_costs }
    }

    #[inline]
    pub fn merge(&mut self, other: &ProfileData) {
        self.all_gas = self.all_gas.saturating_add(other.all_gas);
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

    pub fn all_gas(&self) -> Gas {
        self.all_gas
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

    pub fn wasm_gas(&self) -> u64 {
        self.all_gas() - self.host_gas() - self.action_gas()
    }

    pub fn set_burnt_gas(&mut self, burnt_gas: u64) {
        self.all_gas = burnt_gas;
    }
}

impl fmt::Debug for ProfileData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use num_rational::Ratio;
        let all_gas = self.all_gas();
        if all_gas == 0 {
            writeln!(f, "ERROR: No gas profiled")?;
            return Ok(());
        }
        let host_gas = self.host_gas();
        let action_gas = self.action_gas();
        let wasm_gas = self.wasm_gas();

        writeln!(f, "------------------------------")?;
        writeln!(f, "Total gas: {}", all_gas)?;
        writeln!(
            f,
            "Host gas: {} [{}% total]",
            host_gas,
            Ratio::new(host_gas * 100, all_gas).to_integer()
        )?;
        writeln!(
            f,
            "Action gas: {} [{}% total]",
            action_gas,
            Ratio::new(action_gas * 100, all_gas).to_integer()
        )?;
        writeln!(
            f,
            "Wasm execution: {} [{}% total]",
            wasm_gas,
            Ratio::new(wasm_gas * 100, all_gas).to_integer()
        )?;
        writeln!(f, "------ Host functions --------")?;
        for e in 0..ExtCosts::count() {
            let d = self.get_ext_cost(e);
            if d != 0 {
                writeln!(
                    f,
                    "{} -> {} [{}% total, {}% host]",
                    ExtCosts::name_of(e),
                    d,
                    Ratio::new(d * 100, all_gas).to_integer(),
                    Ratio::new(d * 100, host_gas).to_integer(),
                )?;
            }
        }
        writeln!(f, "------ Actions --------")?;
        for e in 0..ActionCosts::count() {
            let d = self.get_action_cost(e);
            if d != 0 {
                writeln!(
                    f,
                    "{} -> {} [{}% total]",
                    ActionCosts::name_of(e),
                    d,
                    Ratio::new(d * 100, all_gas).to_integer()
                )?;
            }
        }
        writeln!(f, "------------------------------")?;
        Ok(())
    }
}

#[derive(PartialOrd, Ord, PartialEq, Eq)]
pub enum Cost {
    ExtCost(usize),    // need to be usize to iterate, it's actually a ExtCosts variant
    ActionCost(usize), // ActionCosts variant
}

impl Index<Cost> for ProfileData {
    type Output = u64;

    fn index(&self, index: Cost) -> &Self::Output {
        match index {
            Cost::ExtCost(ext) => &self.ext_costs[ext as usize],
            Cost::ActionCost(action) => &self.action_costs[action as usize],
        }
    }
}

impl ProfileData {
    pub fn nonzero_costs(&self) -> BTreeMap<Cost, u64> {
        let mut data = BTreeMap::new();
        for i in 0..ExtCosts::count() {
            if self.ext_costs[i] > 0 {
                data.insert(Cost::ExtCost(i), self.ext_costs[i]);
            }
        }
        for i in 0..ActionCosts::count() {
            if self.action_costs[i] > 0 {
                data.insert(Cost::ActionCost(i), self.action_costs[i]);
            }
        }
        data
    }
}

impl Serialize for Cost {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Cost::ExtCost(ext) => serializer.serialize_str(ExtCosts::name_of(*ext)),
            Cost::ActionCost(action) => serializer.serialize_str(ActionCosts::name_of(*action)),
        }
    }
}

impl Serialize for ProfileData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let cost_data = self.nonzero_costs();
        let mut map = serializer.serialize_map(Some(cost_data.len()))?;
        for (k, v) in cost_data {
            map.serialize_entry(&k, &v)?;
        }
        map.end()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_profile_all_gas() {
        let mut profile_data = ProfileData::new();
        profile_data.set_burnt_gas(42);
        assert_eq!(profile_data.all_gas(), 42);
    }

    #[test]
    fn test_profile_data_debug() {
        let mut profile_data = ProfileData::new();
        profile_data.set_burnt_gas(42);
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
        profile_data.set_burnt_gas(1111);

        let mut profile_data2 = ProfileData::new();
        profile_data2.add_action_cost(ActionCosts::function_call, 222);
        profile_data2.add_ext_cost(ExtCosts::storage_read_base, 22);
        profile_data2.set_burnt_gas(2222);

        profile_data.merge(&profile_data2);
        assert_eq!(profile_data.get_action_cost(ActionCosts::function_call as usize), 333);
        assert_eq!(profile_data.get_ext_cost(ExtCosts::storage_read_base as usize), 33);
        assert_eq!(profile_data.all_gas(), 3333);
    }
}

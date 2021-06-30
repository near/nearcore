use std::rc::Rc;
use std::{cell::Cell, fmt};

use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use crate::config::{ActionCosts, ExtCosts};
use crate::types::Gas;

/// Data for total gas burnt (index 0), and then each external cost and action
type DataArray = [Cell<u64>; ProfileData::LEN];

/// Profile of gas consumption.
#[derive(Clone, PartialEq, Eq)]
pub struct ProfileData {
    data: Rc<DataArray>,
}

/// Result of a ProfileData stat
#[derive(Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct ProfileDataResult(pub Vec<u64>);

impl Default for ProfileData {
    fn default() -> ProfileData {
        ProfileData::new()
    }
}

impl ProfileData {
    const EXT_START: usize = 1;
    const ACTION_START: usize = ProfileData::EXT_START + ExtCosts::count();
    const LEN: usize = 1 + ActionCosts::count() + ExtCosts::count();

    #[inline]
    pub fn new() -> Self {
        // We must manually promote to a constant for array literal to work.
        const ZERO: Cell<u64> = Cell::new(0);

        let data = Rc::new([ZERO; ProfileData::LEN]);
        ProfileData { data }
    }

    #[inline]
    pub fn add_action_cost(&self, action: ActionCosts, value: u64) {
        self.add_val(ProfileData::ACTION_START + action as usize, value);
    }
    #[inline]
    pub fn add_ext_cost(&self, ext: ExtCosts, value: u64) {
        self.add_val(ProfileData::EXT_START + ext as usize, value);
    }
    #[inline]
    fn add_val(&self, index: usize, value: u64) {
        let slot = &self.data[index];
        let old = slot.get();
        slot.set(old.saturating_add(value));
    }

    fn read(&self, index: usize) -> u64 {
        let slot = &self.data[index];
        slot.get()
    }

    pub fn all_gas(&self) -> Gas {
        self.read(0)
    }

    pub fn get_action_cost(&self, action: usize) -> u64 {
        self.read(ProfileData::ACTION_START + action)
    }

    pub fn get_ext_cost(&self, ext: usize) -> u64 {
        self.read(ProfileData::EXT_START + ext)
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

    pub fn set_burnt_gas(&self, burnt_gas: u64) {
        let slot = &self.data[0];
        slot.set(burnt_gas)
    }

    /// Cut up counter values up to now
    /// get and reset all counter to zero
    pub fn cut(&self) -> ProfileDataResult {
        let mut ret = vec![];
        for i in 0..ProfileData::LEN {
            let slot = &self.data[i];
            let old = slot.get();
            ret.push(old);
            slot.set(0);
        }
        ProfileDataResult(ret)
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

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_profile_all_gas() {
        let profile_data = ProfileData::new();
        profile_data.set_burnt_gas(42);
        assert_eq!(profile_data.all_gas(), 42);
    }

    #[test]
    fn test_profile_data_debug() {
        let profile_data = ProfileData::new();
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
        let profile_data = ProfileData::new();
        profile_data.add_action_cost(ActionCosts::function_call, u64::MAX);
        profile_data.add_action_cost(ActionCosts::function_call, u64::MAX);

        let res = profile_data.get_action_cost(ActionCosts::function_call as usize);
        assert_eq!(res, u64::MAX);
    }
}

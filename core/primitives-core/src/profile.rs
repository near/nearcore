use std::fmt;

use crate::config::{ActionCosts, ExtCosts};
use crate::types::Gas;

/// Data for total gas burnt (index 0), and then each external cost and action
type DataArray = [u64; ProfileData::LEN];

/// Profile of gas consumption.
#[derive(Clone, PartialEq, Eq)]
pub struct ProfileData {
    data: DataArray,
}

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
        let data = [0; ProfileData::LEN];
        ProfileData { data }
    }

    #[inline]
    pub fn merge(&mut self, other: &ProfileData) {
        for i in 0..ProfileData::LEN {
            self.data[i] = self.data[i].saturating_add(other.data[i]);
        }
    }

    #[inline]
    pub fn add_action_cost(&mut self, action: ActionCosts, value: u64) {
        self.add_val(ProfileData::ACTION_START + action as usize, value);
    }
    #[inline]
    pub fn add_ext_cost(&mut self, ext: ExtCosts, value: u64) {
        self.add_val(ProfileData::EXT_START + ext as usize, value);
    }
    #[inline]
    fn add_val(&mut self, index: usize, value: u64) {
        self.data[index] = self.data[index].saturating_add(value);
    }

    fn read(&self, index: usize) -> u64 {
        self.data[index]
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

    pub fn set_burnt_gas(&mut self, burnt_gas: u64) {
        self.data[0] = burnt_gas;
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

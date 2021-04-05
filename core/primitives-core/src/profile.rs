use std::{cell::Cell, fmt};

use crate::config::{ActionCosts, ExtCosts};
use crate::types::Gas;

/// Data for total gas burnt (index 0), and then each external cost and action
#[allow(dead_code)]
type DataArray = [Cell<u64>; ProfileData::LEN];

/// Profile of gas consumption.
#[derive(Clone)]
pub struct ProfileData {
    repr: Repr,
}

#[derive(Clone)]
enum Repr {
    #[cfg(feature = "costs_counting")]
    Enabled {
        data: std::rc::Rc<DataArray>,
    },
    Disabled,
}

#[cfg(not(feature = "costs_counting"))]
const _ASSERT_NO_OP_IF_COUNTING_DISABLED: [(); 0] = [(); std::mem::size_of::<ProfileData>()];

impl Default for ProfileData {
    fn default() -> ProfileData {
        ProfileData::new_disabled()
    }
}

impl ProfileData {
    const EXT_START: usize = 1;
    const ACTION_START: usize = ProfileData::EXT_START + ExtCosts::count();
    const LEN: usize = 1 + ActionCosts::count() + ExtCosts::count();

    #[inline]
    #[cfg(feature = "costs_counting")]
    pub fn new_enabled() -> Self {
        // We must manually promote to a constant for array literal to work.
        const ZERO: Cell<u64> = Cell::new(0);

        let data = std::rc::Rc::new([ZERO; ProfileData::LEN]);
        let repr = Repr::Enabled { data };
        ProfileData { repr }
    }

    #[inline]
    #[cfg(not(feature = "costs_counting"))]
    pub fn new_enabled() -> Self {
        Self::new_disabled()
    }

    #[inline]
    pub fn new_disabled() -> Self {
        let repr = Repr::Disabled;
        ProfileData { repr }
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
        self.with_slot(index, |slot| {
            let old = slot.get();
            slot.set(old + value);
        })
    }

    #[inline]
    fn with_slot(&self, index: usize, f: impl FnOnce(&Cell<u64>)) {
        match &self.repr {
            #[cfg(feature = "costs_counting")]
            Repr::Enabled { data } => f(&data[index]),
            Repr::Disabled => drop((index, f)),
        }
    }
    fn read(&self, index: usize) -> u64 {
        let mut res = 0;
        self.with_slot(index, |slot| res = slot.get());
        res
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

    pub fn unaccounted_gas(&self) -> u64 {
        self.all_gas() - self.wasm_gas() - self.action_gas() - self.host_gas()
    }

    pub fn set_burnt_gas(&self, burnt_gas: u64) {
        self.with_slot(0, |slot| slot.set(burnt_gas));
    }
}

impl fmt::Debug for ProfileData {
    #[cfg(not(feature = "costs_counting"))]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "ERROR: cost_counting feature is not enabled in near-primitives-core, cannot print profile data")?;
        Ok(())
    }

    #[cfg(feature = "costs_counting")]
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
        let unaccounted_gas = self.unaccounted_gas();
        if unaccounted_gas > 0 {
            writeln!(
                f,
                "Unaccounted: {} [{}% total]",
                unaccounted_gas,
                Ratio::new(unaccounted_gas * 100, all_gas).to_integer()
            )?;
        }
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
        let profile_data = ProfileData::new_enabled();
        profile_data.set_burnt_gas(42);
        #[cfg(not(feature = "costs_counting"))]
        assert_eq!(profile_data.all_gas(), 0);
        #[cfg(feature = "costs_counting")]
        assert_eq!(profile_data.all_gas(), 42);
    }

    #[test]
    fn test_profile_data_debug() {
        let profile_data = ProfileData::new_enabled();
        profile_data.set_burnt_gas(42);
        println!("{:#?}", &profile_data);
    }

    #[test]
    fn test_profile_data_debug_no_data() {
        let profile_data = ProfileData::new_enabled();
        println!("{:#?}", &profile_data);
    }
}

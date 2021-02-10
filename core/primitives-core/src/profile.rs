use crate::config::{ActionCosts, ExtCosts};
use crate::types::Gas;
use num_rational::Ratio;
use std::{cell::RefCell, fmt, rc::Rc};

const PROFILE_DATA_LEN: usize = 1 + ActionCosts::count() + ExtCosts::count();

/// Data for total gas burnt (index 0), and then each action and external cost
pub type InternalProfileData<T> = [T; PROFILE_DATA_LEN];

pub struct FixedArray<T> {
    pub data: Rc<RefCell<InternalProfileData<T>>>,
}

impl<T> Clone for FixedArray<T> {
    fn clone(&self) -> Self {
        Self { data: Rc::clone(&self.data) }
    }
}

/// Profile of gas consumption.
pub type ProfileData = FixedArray<u64>;

pub fn clone_profile(profile: &ProfileData) -> ProfileData {
    profile.clone()
}

impl ProfileData {
    pub fn new() -> Self {
        Self { data: Rc::new(RefCell::new([0u64; 1 + ExtCosts::count() + ActionCosts::count()])) }
    }

    const EXT_START: usize = 1;
    const ACTION_START: usize = ProfileData::EXT_START + ExtCosts::count();
    // const LENGTH: usize = PROFILE_DATA_LEN;
    pub fn all_gas(&self) -> Gas {
        (*self.data.borrow())[0]
    }

    fn borrow(&self) -> InternalProfileData<u64> {
        *self.data.borrow()
    }

    fn add_val(&self, index: usize, value: u64) {
        *self.data.borrow_mut().get_mut(index).unwrap() += value;
    }

    pub fn add_action_cost(&self, action: ActionCosts, value: u64) {
        self.add_val(ProfileData::ACTION_START + action as usize, value);
    }

    pub fn get_action_cost(&self, action: usize) -> u64 {
        self.borrow()[ProfileData::ACTION_START + action]
    }

    pub fn get_ext_cost(&self, ext: usize) -> u64 {
        self.borrow()[ProfileData::EXT_START + ext]
    }

    pub fn add_ext_cost(&self, ext: ExtCosts, value: u64) {
        self.add_val(ProfileData::EXT_START + ext as usize, value);
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
        *self.data.borrow_mut().get_mut(0 as usize).unwrap() = burnt_gas;
    }
}

impl fmt::Debug for ProfileData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let all_gas = self.all_gas();
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

unsafe impl<T: Send> Send for FixedArray<T> {}
unsafe impl<T: Sync> Sync for FixedArray<T> {}

pub use profile_v2::ProfileDataV2;

use crate::config::{ActionCosts, ExtCosts, ExtCostsConfig};
use crate::types::{Compute, Gas};
use borsh::{BorshDeserialize, BorshSerialize};
use enum_map::{enum_map, Enum, EnumMap};
use std::fmt;
use strum::IntoEnumIterator;

mod profile_v2;

/// Profile of gas consumption.
#[derive(Clone, PartialEq, Eq)]
pub struct ProfileDataV3 {
    /// Gas spent on sending or executing actions.
    actions_profile: EnumMap<ActionCosts, Gas>,
    /// Non-action gas spent outside the WASM VM while executing a contract.
    wasm_ext_profile: EnumMap<ExtCosts, Gas>,
    /// Gas spent on execution inside the WASM VM.
    wasm_gas: Gas,
}

impl Default for ProfileDataV3 {
    fn default() -> ProfileDataV3 {
        ProfileDataV3::new()
    }
}

impl ProfileDataV3 {
    #[inline]
    pub fn new() -> Self {
        Self {
            actions_profile: enum_map! { _ => 0 },
            wasm_ext_profile: enum_map! { _ => 0 },
            wasm_gas: 0,
        }
    }

    /// Test instance with unique numbers in each field.
    pub fn test() -> Self {
        let mut profile_data = ProfileDataV3::default();
        for (i, cost) in ExtCosts::iter().enumerate() {
            profile_data.add_ext_cost(cost, i as Gas);
        }
        for (i, cost) in ActionCosts::iter().enumerate() {
            profile_data.add_action_cost(cost, i as Gas + 1000);
        }
        profile_data
    }

    #[inline]
    pub fn merge(&mut self, other: &ProfileDataV3) {
        for ((_, gas), (_, other_gas)) in
            self.actions_profile.iter_mut().zip(other.actions_profile.iter())
        {
            *gas = gas.saturating_add(*other_gas);
        }
        for ((_, gas), (_, other_gas)) in
            self.wasm_ext_profile.iter_mut().zip(other.wasm_ext_profile.iter())
        {
            *gas = gas.saturating_add(*other_gas);
        }
        self.wasm_gas = self.wasm_gas.saturating_add(other.wasm_gas);
    }

    #[inline]
    pub fn add_action_cost(&mut self, action: ActionCosts, value: Gas) {
        self.actions_profile[action] = self.actions_profile[action].saturating_add(value);
    }

    #[inline]
    pub fn add_ext_cost(&mut self, ext: ExtCosts, value: Gas) {
        self.wasm_ext_profile[ext] = self.wasm_ext_profile[ext].saturating_add(value);
    }

    /// WasmInstruction is the only cost we don't explicitly account for.
    /// Instead, we compute it at the end of contract call as the difference
    /// between total gas burnt and what we've explicitly accounted for in the
    /// profile.
    ///
    /// This is because WasmInstruction is the hottest cost and is implemented
    /// with the help on the VM side, so we don't want to have profiling logic
    /// there both for simplicity and efficiency reasons.
    pub fn compute_wasm_instruction_cost(&mut self, total_gas_burnt: Gas) {
        self.wasm_gas =
            total_gas_burnt.saturating_sub(self.action_gas()).saturating_sub(self.host_gas());
    }

    pub fn get_action_cost(&self, action: ActionCosts) -> Gas {
        self.actions_profile[action]
    }

    pub fn get_ext_cost(&self, ext: ExtCosts) -> Gas {
        self.wasm_ext_profile[ext]
    }

    pub fn get_wasm_cost(&self) -> Gas {
        self.wasm_gas
    }

    fn host_gas(&self) -> Gas {
        self.wasm_ext_profile.as_slice().iter().copied().fold(0, Gas::saturating_add)
    }

    pub fn action_gas(&self) -> Gas {
        self.actions_profile.as_slice().iter().copied().fold(0, Gas::saturating_add)
    }

    /// Returns total compute usage of host calls.
    pub fn total_compute_usage(&self, ext_costs_config: &ExtCostsConfig) -> Compute {
        let ext_compute_cost = self
            .wasm_ext_profile
            .iter()
            .map(|(key, value)| {
                // Technically, gas cost might be zero while the compute cost is non-zero. To
                // handle this case, we would need to explicitly count number of calls, not just
                // the total gas usage.
                // We don't have such costs at the moment, so this case is not implemented.
                debug_assert!(key.gas(ext_costs_config) > 0 || key.compute(ext_costs_config) == 0);

                if *value == 0 {
                    return *value;
                }
                // If the `value` is non-zero, the gas cost also must be non-zero.
                debug_assert!(key.gas(ext_costs_config) != 0);
                ((*value as u128).saturating_mul(key.compute(ext_costs_config) as u128)
                    / (key.gas(ext_costs_config) as u128)) as u64
            })
            .fold(0, Compute::saturating_add);

        // We currently only support compute costs for host calls. In the future we might add
        // them for actions as well.
        ext_compute_cost.saturating_add(self.action_gas()).saturating_add(self.get_wasm_cost())
    }
}

impl BorshDeserialize for ProfileDataV3 {
    fn deserialize_reader<R: std::io::Read>(rd: &mut R) -> std::io::Result<Self> {
        let actions_array: Vec<u64> = BorshDeserialize::deserialize_reader(rd)?;
        let ext_array: Vec<u64> = BorshDeserialize::deserialize_reader(rd)?;
        let wasm_gas: u64 = BorshDeserialize::deserialize_reader(rd)?;

        // Mapping raw arrays to enum maps.
        // The enum map could be smaller or larger than the raw array.
        // Extra values in the array that are unknown to the current binary will
        // be ignored. Missing values are filled with 0.
        let actions_profile = enum_map! {
            cost => actions_array.get(borsh_action_index(cost)).copied().unwrap_or(0)
        };
        let wasm_ext_profile = enum_map! {
            cost => ext_array.get(borsh_ext_index(cost)).copied().unwrap_or(0)
        };

        Ok(Self { actions_profile, wasm_ext_profile, wasm_gas })
    }
}

impl BorshSerialize for ProfileDataV3 {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> Result<(), std::io::Error> {
        let mut actions_costs: Vec<u64> = vec![0u64; ActionCosts::LENGTH];
        for (cost, gas) in self.actions_profile.iter() {
            actions_costs[borsh_action_index(cost)] = *gas;
        }
        BorshSerialize::serialize(&actions_costs, writer)?;

        let mut ext_costs: Vec<u64> = vec![0u64; ExtCosts::LENGTH];
        for (cost, gas) in self.wasm_ext_profile.iter() {
            ext_costs[borsh_ext_index(cost)] = *gas;
        }
        BorshSerialize::serialize(&ext_costs, writer)?;

        let wasm_cost: u64 = self.wasm_gas;
        BorshSerialize::serialize(&wasm_cost, writer)
    }
}

/// Fixed index of an action cost for borsh (de)serialization.
///
/// We use borsh to store profiles on the DB and borsh is quite fragile with
/// changes. This mapping is to decouple the Rust enum from the borsh
/// representation. The enum can be changed freely but here in the mapping we
/// can only append more values at the end.
///
/// TODO: Consider changing this to a different format (e.g. protobuf) because
/// canonical representation is not required here.
const fn borsh_action_index(action: ActionCosts) -> usize {
    // actual indices are defined on the enum variants
    action as usize
}

/// Fixed index of an ext cost for borsh (de)serialization.
///
/// We use borsh to store profiles on the DB and borsh is quite fragile with
/// changes. This mapping is to decouple the Rust enum from the borsh
/// representation. The enum can be changed freely but here in the mapping we
/// can only append more values at the end.
///
/// TODO: Consider changing this to a different format (e.g. protobuf) because
/// canonical representation is not required here.
const fn borsh_ext_index(ext: ExtCosts) -> usize {
    // actual indices are defined on the enum variants
    ext as usize
}

impl fmt::Debug for ProfileDataV3 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use num_rational::Ratio;
        let host_gas = self.host_gas();
        let action_gas = self.action_gas();

        writeln!(f, "------------------------------")?;
        writeln!(f, "Action gas: {}", action_gas)?;
        writeln!(f, "------ Host functions --------")?;
        for cost in ExtCosts::iter() {
            let d = self.get_ext_cost(cost);
            if d != 0 {
                writeln!(
                    f,
                    "{} -> {} [{}% host]",
                    cost,
                    d,
                    Ratio::new(d * 100, core::cmp::max(host_gas, 1)).to_integer(),
                )?;
            }
        }
        writeln!(f, "------ Actions --------")?;
        for cost in ActionCosts::iter() {
            let d = self.get_action_cost(cost);
            if d != 0 {
                writeln!(f, "{} -> {}", cost, d)?;
            }
        }
        writeln!(f, "------------------------------")?;
        Ok(())
    }
}

/// Tests for ProfileDataV3
#[cfg(test)]
mod test {
    use super::*;

    #[test]
    #[cfg(not(feature = "nightly"))]
    fn test_profile_data_debug() {
        let profile_data = ProfileDataV3::test();
        // we don't care about exact formatting, but the numbers should not change unexpectedly
        let pretty_debug_str = format!("{profile_data:#?}");
        insta::assert_snapshot!(pretty_debug_str);
    }

    #[test]
    fn test_profile_data_debug_no_data() {
        let profile_data = ProfileDataV3::default();
        // we don't care about exact formatting, but at least it should not panic
        println!("{:#?}", &profile_data);
    }

    #[test]
    fn test_no_panic_on_overflow() {
        let mut profile_data = ProfileDataV3::default();
        profile_data.add_action_cost(ActionCosts::add_full_access_key, u64::MAX);
        profile_data.add_action_cost(ActionCosts::add_full_access_key, u64::MAX);

        let res = profile_data.get_action_cost(ActionCosts::add_full_access_key);
        assert_eq!(res, u64::MAX);
    }

    #[test]
    fn test_merge() {
        let mut profile_data = ProfileDataV3::default();
        profile_data.add_action_cost(ActionCosts::add_full_access_key, 111);
        profile_data.add_ext_cost(ExtCosts::storage_read_base, 11);

        let mut profile_data2 = ProfileDataV3::default();
        profile_data2.add_action_cost(ActionCosts::add_full_access_key, 222);
        profile_data2.add_ext_cost(ExtCosts::storage_read_base, 22);

        profile_data.merge(&profile_data2);
        assert_eq!(profile_data.get_action_cost(ActionCosts::add_full_access_key), 333);
        assert_eq!(profile_data.get_ext_cost(ExtCosts::storage_read_base), 33);
    }

    #[test]
    fn test_total_compute_usage() {
        let ext_costs_config = ExtCostsConfig::test_with_undercharging_factor(3);
        let mut profile_data = ProfileDataV3::default();
        profile_data.add_ext_cost(
            ExtCosts::storage_read_base,
            2 * ExtCosts::storage_read_base.gas(&ext_costs_config),
        );
        profile_data.add_ext_cost(
            ExtCosts::storage_write_base,
            5 * ExtCosts::storage_write_base.gas(&ext_costs_config),
        );
        profile_data.add_action_cost(ActionCosts::function_call_base, 100);

        assert_eq!(
            profile_data.total_compute_usage(&ext_costs_config),
            3 * profile_data.host_gas() + profile_data.action_gas()
        );
    }

    #[test]
    fn test_borsh_ser_deser() {
        let mut profile_data = ProfileDataV3::default();
        for (i, cost) in ExtCosts::iter().enumerate() {
            profile_data.add_ext_cost(cost, i as Gas);
        }
        for (i, cost) in ActionCosts::iter().enumerate() {
            profile_data.add_action_cost(cost, i as Gas + 1000);
        }
        let buf = profile_data.try_to_vec().expect("failed serializing a normal profile");

        let restored: ProfileDataV3 = BorshDeserialize::deserialize(&mut buf.as_slice())
            .expect("failed deserializing a normal profile");

        assert_eq!(profile_data, restored);
    }

    #[test]
    fn test_borsh_incomplete_profile() {
        let action_profile = vec![50u64, 60];
        let ext_profile = vec![100u64, 200];
        let wasm_cost = 99u64;
        let input = manually_encode_profile_v2(action_profile, ext_profile, wasm_cost);

        let profile: ProfileDataV3 = BorshDeserialize::deserialize(&mut input.as_slice())
            .expect("should be able to parse a profile with less entries");

        assert_eq!(50, profile.get_action_cost(ActionCosts::create_account));
        assert_eq!(60, profile.get_action_cost(ActionCosts::delete_account));
        assert_eq!(0, profile.get_action_cost(ActionCosts::deploy_contract_base));

        assert_eq!(100, profile.get_ext_cost(ExtCosts::base));
        assert_eq!(200, profile.get_ext_cost(ExtCosts::contract_loading_base));
        assert_eq!(0, profile.get_ext_cost(ExtCosts::contract_loading_bytes));
    }

    #[test]
    fn test_borsh_larger_profile_than_current() {
        let action_profile = vec![1234u64; ActionCosts::LENGTH + 5];
        let ext_profile = vec![5678u64; ExtCosts::LENGTH + 10];
        let wasm_cost = 90u64;
        let input = manually_encode_profile_v2(action_profile, ext_profile, wasm_cost);

        let profile: ProfileDataV3 = BorshDeserialize::deserialize(&mut input.as_slice()).expect(
            "should be able to parse a profile with more entries than the current version has",
        );

        for action in ActionCosts::iter() {
            assert_eq!(1234, profile.get_action_cost(action), "{action:?}");
        }

        for ext in ExtCosts::iter() {
            assert_eq!(5678, profile.get_ext_cost(ext), "{ext:?}");
        }

        assert_eq!(90, profile.wasm_gas);
    }

    #[track_caller]
    fn manually_encode_profile_v2(
        action_profile: Vec<u64>,
        ext_profile: Vec<u64>,
        wasm_cost: u64,
    ) -> Vec<u8> {
        let mut input = vec![];
        BorshSerialize::serialize(&action_profile, &mut input).unwrap();
        BorshSerialize::serialize(&ext_profile, &mut input).unwrap();
        BorshSerialize::serialize(&wasm_cost, &mut input).unwrap();
        input
    }
}

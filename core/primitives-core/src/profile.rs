pub use profile_v1::ProfileDataV1;

use crate::config::{ActionCosts, ExtCosts};
use crate::types::Gas;
use borsh::{BorshDeserialize, BorshSerialize};
use enum_map::{enum_map, Enum, EnumMap};
use std::fmt;
use std::ops::Index;
use strum::IntoEnumIterator;

/// Profile of gas consumption.
#[derive(Clone, PartialEq, Eq)]
pub struct ProfileDataV2 {
    /// Gas spent on sending or executing actions.
    actions_profile: EnumMap<ActionCosts, Gas>,
    /// Non-action gas spent outside the WASM VM while executing a contract.
    wasm_ext_profile: EnumMap<ExtCosts, Gas>,
    /// Gas spent on execution inside the WASM VM.
    wasm_gas: Gas,
}

impl Default for ProfileDataV2 {
    fn default() -> ProfileDataV2 {
        ProfileDataV2::new()
    }
}

impl ProfileDataV2 {
    #[inline]
    pub fn new() -> Self {
        Self {
            actions_profile: enum_map! { _ => 0 },
            wasm_ext_profile: enum_map! { _ => 0 },
            wasm_gas: 0,
        }
    }

    #[inline]
    pub fn merge(&mut self, other: &ProfileDataV2) {
        for (cost, gas) in self.actions_profile.iter_mut() {
            *gas = gas.saturating_add(other.actions_profile[cost]);
        }
        for (cost, gas) in self.wasm_ext_profile.iter_mut() {
            *gas = gas.saturating_add(other.wasm_ext_profile[cost]);
        }
        self.wasm_gas = self.wasm_gas.saturating_add(other.wasm_gas);
    }

    #[inline]
    pub fn add_action_cost(&mut self, action: ActionCosts, value: u64) {
        self.actions_profile[action] = self.actions_profile[action].saturating_add(value);
    }

    #[inline]
    pub fn add_ext_cost(&mut self, ext: ExtCosts, value: u64) {
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
    pub fn compute_wasm_instruction_cost(&mut self, total_gas_burnt: u64) {
        self.wasm_gas =
            total_gas_burnt.saturating_sub(self.action_gas()).saturating_sub(self.host_gas());
    }

    fn get_action_cost(&self, action: ActionCosts) -> u64 {
        self.actions_profile[action]
    }

    pub fn get_ext_cost(&self, ext: ExtCosts) -> u64 {
        self.wasm_ext_profile[ext]
    }

    fn host_gas(&self) -> u64 {
        self.wasm_ext_profile.as_slice().iter().copied().fold(0, u64::saturating_add)
    }

    pub fn action_gas(&self) -> u64 {
        self.actions_profile.as_slice().iter().copied().fold(0, u64::saturating_add)
    }
}

impl BorshDeserialize for ProfileDataV2 {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, std::io::Error> {
        let actions_array: Vec<u64> = BorshDeserialize::deserialize(buf)?;
        let ext_array: Vec<u64> = BorshDeserialize::deserialize(buf)?;
        let wasm_gas: u64 = BorshDeserialize::deserialize(buf)?;

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

impl BorshSerialize for ProfileDataV2 {
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
    match action {
        ActionCosts::create_account => 0,
        ActionCosts::delete_account => 1,
        ActionCosts::deploy_contract_base => 2,
        ActionCosts::deploy_contract_byte => 3,
        ActionCosts::function_call_base => 4,
        ActionCosts::function_call_byte => 5,
        ActionCosts::transfer => 6,
        ActionCosts::stake => 7,
        ActionCosts::add_full_access_key => 8,
        ActionCosts::add_function_call_key_base => 9,
        ActionCosts::add_function_call_key_byte => 10,
        ActionCosts::delete_key => 11,
        ActionCosts::new_action_receipt => 12,
        ActionCosts::new_data_receipt_base => 13,
        ActionCosts::new_data_receipt_byte => 14,
    }
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
    match ext {
        ExtCosts::base => 0,
        ExtCosts::contract_loading_base => 1,
        ExtCosts::contract_loading_bytes => 2,
        ExtCosts::read_memory_base => 3,
        ExtCosts::read_memory_byte => 4,
        ExtCosts::write_memory_base => 5,
        ExtCosts::write_memory_byte => 6,
        ExtCosts::read_register_base => 7,
        ExtCosts::read_register_byte => 8,
        ExtCosts::write_register_base => 9,
        ExtCosts::write_register_byte => 10,
        ExtCosts::utf8_decoding_base => 11,
        ExtCosts::utf8_decoding_byte => 12,
        ExtCosts::utf16_decoding_base => 13,
        ExtCosts::utf16_decoding_byte => 14,
        ExtCosts::sha256_base => 15,
        ExtCosts::sha256_byte => 16,
        ExtCosts::keccak256_base => 17,
        ExtCosts::keccak256_byte => 18,
        ExtCosts::keccak512_base => 19,
        ExtCosts::keccak512_byte => 20,
        ExtCosts::ripemd160_base => 21,
        ExtCosts::ripemd160_block => 22,
        ExtCosts::ecrecover_base => 23,
        ExtCosts::log_base => 24,
        ExtCosts::log_byte => 25,
        ExtCosts::storage_write_base => 26,
        ExtCosts::storage_write_key_byte => 27,
        ExtCosts::storage_write_value_byte => 28,
        ExtCosts::storage_write_evicted_byte => 29,
        ExtCosts::storage_read_base => 30,
        ExtCosts::storage_read_key_byte => 31,
        ExtCosts::storage_read_value_byte => 32,
        ExtCosts::storage_remove_base => 33,
        ExtCosts::storage_remove_key_byte => 34,
        ExtCosts::storage_remove_ret_value_byte => 35,
        ExtCosts::storage_has_key_base => 36,
        ExtCosts::storage_has_key_byte => 37,
        ExtCosts::storage_iter_create_prefix_base => 38,
        ExtCosts::storage_iter_create_prefix_byte => 39,
        ExtCosts::storage_iter_create_range_base => 40,
        ExtCosts::storage_iter_create_from_byte => 41,
        ExtCosts::storage_iter_create_to_byte => 42,
        ExtCosts::storage_iter_next_base => 43,
        ExtCosts::storage_iter_next_key_byte => 44,
        ExtCosts::storage_iter_next_value_byte => 45,
        ExtCosts::touching_trie_node => 46,
        ExtCosts::read_cached_trie_node => 47,
        ExtCosts::promise_and_base => 48,
        ExtCosts::promise_and_per_promise => 49,
        ExtCosts::promise_return => 50,
        ExtCosts::validator_stake_base => 51,
        ExtCosts::validator_total_stake_base => 52,
        ExtCosts::alt_bn128_g1_multiexp_base => 53,
        ExtCosts::alt_bn128_g1_multiexp_element => 54,
        ExtCosts::alt_bn128_pairing_check_base => 55,
        ExtCosts::alt_bn128_pairing_check_element => 56,
        ExtCosts::alt_bn128_g1_sum_base => 57,
        ExtCosts::alt_bn128_g1_sum_element => 58,
    }
}

impl fmt::Debug for ProfileDataV2 {
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

mod profile_v1 {
    use super::*;
    /// Deprecated serialization format to store profiles in the database.
    ///
    /// This is not part of the protocol but archival nodes still rely on this not
    /// changing to answer old tx-status requests with a gas profile.
    ///
    /// It used to store an array that manually mapped `enum Cost` to gas
    /// numbers. Now `ProfileDataV1` and `Cost` are deprecated. But to lookup
    /// old gas profiles from the DB, we need to keep the code around.
    #[derive(Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
    pub struct ProfileDataV1 {
        data: DataArray,
    }

    // TODO: Remove
    impl Default for ProfileDataV1 {
        fn default() -> Self {
            let costs = DataArray(Box::new([0; DataArray::LEN]));
            ProfileDataV1 { data: costs }
        }
    }

    #[derive(Clone, PartialEq, Eq)]
    struct DataArray(Box<[u64; Self::LEN]>);

    impl DataArray {
        const LEN: usize = if cfg!(feature = "protocol_feature_ed25519_verify") { 72 } else { 70 };
    }

    impl ProfileDataV1 {
        pub fn get_ext_cost(&self, ext: ExtCosts) -> u64 {
            self[ext]
        }

        pub fn get_wasm_cost(&self) -> u64 {
            // ProfileV1Cost::WasmInstruction => 62,
            self.data[62]
        }

        fn host_gas(&self) -> u64 {
            ExtCosts::iter().map(|a| self.get_ext_cost(a)).fold(0, u64::saturating_add)
        }

        /// List action cost in the old way, which conflated several action parameters into one.
        ///
        /// This is used to display old gas profiles on the RPC API and in debug output.
        pub fn legacy_action_costs(&self) -> Vec<(&'static str, Gas)> {
            vec![
                ("CREATE_ACCOUNT", self.data[0]),
                ("DELETE_ACCOUNT", self.data[1]),
                ("DEPLOY_CONTRACT", self.data[2]), // contains per byte and base cost
                ("FUNCTION_CALL", self.data[3]),   // contains per byte and base cost
                ("TRANSFER", self.data[4]),
                ("STAKE", self.data[5]),
                ("ADD_KEY", self.data[6]), // contains base + per byte cost for function call keys and full access keys
                ("DELETE_KEY", self.data[7]),
                ("NEW_DATA_RECEIPT_BYTE", self.data[8]), // contains the per-byte cost for sending back a data dependency
                ("NEW_RECEIPT", self.data[9]), // contains base cost for data receipts and action receipts
            ]
        }

        pub fn action_gas(&self) -> u64 {
            self.legacy_action_costs()
                .iter()
                .map(|(_name, cost)| *cost)
                .fold(0, u64::saturating_add)
        }
    }

    impl fmt::Debug for ProfileDataV1 {
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
            for (cost, gas) in self.legacy_action_costs() {
                if gas != 0 {
                    writeln!(f, "{} -> {}", cost.to_ascii_lowercase(), gas)?;
                }
            }
            writeln!(f, "------------------------------")?;
            Ok(())
        }
    }

    impl Index<usize> for DataArray {
        type Output = u64;

        fn index(&self, index: usize) -> &Self::Output {
            &self.0[index]
        }
    }

    impl Index<ActionCosts> for ProfileDataV1 {
        type Output = u64;

        fn index(&self, cost: ActionCosts) -> &Self::Output {
            let index = match cost {
                ActionCosts::create_account => 0,
                ActionCosts::delete_account => 1,
                ActionCosts::deploy_contract_base => 2,
                ActionCosts::deploy_contract_byte => 2,
                ActionCosts::function_call_base => 3,
                ActionCosts::function_call_byte => 3,
                ActionCosts::transfer => 4,
                ActionCosts::stake => 5,
                ActionCosts::add_full_access_key => 6,
                ActionCosts::add_function_call_key_base => 6,
                ActionCosts::add_function_call_key_byte => 6,
                ActionCosts::delete_key => 7,
                ActionCosts::new_data_receipt_byte => 8,
                ActionCosts::new_action_receipt => 9,
                ActionCosts::new_data_receipt_base => 9,
                // new costs added after profile v1 was deprecated don't have this entry
                #[allow(unreachable_patterns)]
                _ => return &0,
            };
            &self.data[index]
        }
    }

    impl Index<ExtCosts> for ProfileDataV1 {
        type Output = u64;

        fn index(&self, cost: ExtCosts) -> &Self::Output {
            let index = match cost {
                ExtCosts::base => 10,
                ExtCosts::contract_loading_base => 11,
                ExtCosts::contract_loading_bytes => 12,
                ExtCosts::read_memory_base => 13,
                ExtCosts::read_memory_byte => 14,
                ExtCosts::write_memory_base => 15,
                ExtCosts::write_memory_byte => 16,
                ExtCosts::read_register_base => 17,
                ExtCosts::read_register_byte => 18,
                ExtCosts::write_register_base => 19,
                ExtCosts::write_register_byte => 20,
                ExtCosts::utf8_decoding_base => 21,
                ExtCosts::utf8_decoding_byte => 22,
                ExtCosts::utf16_decoding_base => 23,
                ExtCosts::utf16_decoding_byte => 24,
                ExtCosts::sha256_base => 25,
                ExtCosts::sha256_byte => 26,
                ExtCosts::keccak256_base => 27,
                ExtCosts::keccak256_byte => 28,
                ExtCosts::keccak512_base => 29,
                ExtCosts::keccak512_byte => 30,
                ExtCosts::ripemd160_base => 31,
                ExtCosts::ripemd160_block => 32,
                ExtCosts::ecrecover_base => 33,
                ExtCosts::log_base => 34,
                ExtCosts::log_byte => 35,
                ExtCosts::storage_write_base => 36,
                ExtCosts::storage_write_key_byte => 37,
                ExtCosts::storage_write_value_byte => 38,
                ExtCosts::storage_write_evicted_byte => 39,
                ExtCosts::storage_read_base => 40,
                ExtCosts::storage_read_key_byte => 41,
                ExtCosts::storage_read_value_byte => 42,
                ExtCosts::storage_remove_base => 43,
                ExtCosts::storage_remove_key_byte => 44,
                ExtCosts::storage_remove_ret_value_byte => 45,
                ExtCosts::storage_has_key_base => 46,
                ExtCosts::storage_has_key_byte => 47,
                ExtCosts::storage_iter_create_prefix_base => 48,
                ExtCosts::storage_iter_create_prefix_byte => 49,
                ExtCosts::storage_iter_create_range_base => 50,
                ExtCosts::storage_iter_create_from_byte => 51,
                ExtCosts::storage_iter_create_to_byte => 52,
                ExtCosts::storage_iter_next_base => 53,
                ExtCosts::storage_iter_next_key_byte => 54,
                ExtCosts::storage_iter_next_value_byte => 55,
                ExtCosts::touching_trie_node => 56,
                ExtCosts::promise_and_base => 57,
                ExtCosts::promise_and_per_promise => 58,
                ExtCosts::promise_return => 59,
                ExtCosts::validator_stake_base => 60,
                ExtCosts::validator_total_stake_base => 61,
                ExtCosts::read_cached_trie_node => 63,
                ExtCosts::alt_bn128_g1_multiexp_base => 64,
                ExtCosts::alt_bn128_g1_multiexp_element => 65,
                ExtCosts::alt_bn128_pairing_check_base => 66,
                ExtCosts::alt_bn128_pairing_check_element => 67,
                ExtCosts::alt_bn128_g1_sum_base => 68,
                ExtCosts::alt_bn128_g1_sum_element => 69,
                // new costs added after profile v1 was deprecated don't have this entry
                #[allow(unreachable_patterns)]
                _ => return &0,
            };
            &self.data[index]
        }
    }

    impl BorshDeserialize for DataArray {
        fn deserialize(buf: &mut &[u8]) -> Result<Self, std::io::Error> {
            let data_vec: Vec<u64> = BorshDeserialize::deserialize(buf)?;
            let mut data_array = [0; Self::LEN];
            let len = Self::LEN.min(data_vec.len());
            data_array[0..len].copy_from_slice(&data_vec[0..len]);
            Ok(Self(Box::new(data_array)))
        }
    }

    impl BorshSerialize for DataArray {
        fn serialize<W: std::io::Write>(&self, writer: &mut W) -> Result<(), std::io::Error> {
            let v: Vec<_> = self.0.as_ref().to_vec();
            BorshSerialize::serialize(&v, writer)
        }
    }

    /// Tests for ProfileDataV1
    #[cfg(test)]
    mod test {
        use super::*;

        #[test]
        fn test_profile_data_debug() {
            let mut profile_data = ProfileDataV1::default();
            let num_legacy_actions = 10;
            for i in 0..num_legacy_actions {
                profile_data.data.0[i] = i as Gas + 1000;
            }
            for i in num_legacy_actions..DataArray::LEN {
                profile_data.data.0[i] = (i - num_legacy_actions) as Gas;
            }
            // we don't care about exact formatting, but the numbers should not change unexpectedly
            let pretty_debug_str = format!("{profile_data:#?}");
            insta::assert_snapshot!(pretty_debug_str);
        }

        #[test]
        fn test_profile_data_debug_no_data() {
            let profile_data = ProfileDataV1::default();
            // we don't care about exact formatting, but at least it should not panic
            println!("{:#?}", &profile_data);
        }
    }
}

/// Tests for ProfileDataV2
#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_profile_data_debug() {
        let mut profile_data = ProfileDataV2::default();
        for (i, cost) in ExtCosts::iter().enumerate() {
            profile_data.add_ext_cost(cost, i as Gas);
        }
        for (i, cost) in ActionCosts::iter().enumerate() {
            profile_data.add_action_cost(cost, i as Gas + 1000);
        }
        // we don't care about exact formatting, but the numbers should not change unexpectedly
        let pretty_debug_str = format!("{profile_data:#?}");
        insta::assert_snapshot!(pretty_debug_str);
    }

    #[test]
    fn test_profile_data_debug_no_data() {
        let profile_data = ProfileDataV2::default();
        // we don't care about exact formatting, but at least it should not panic
        println!("{:#?}", &profile_data);
    }

    #[test]
    fn test_no_panic_on_overflow() {
        let mut profile_data = ProfileDataV2::default();
        profile_data.add_action_cost(ActionCosts::add_full_access_key, u64::MAX);
        profile_data.add_action_cost(ActionCosts::add_full_access_key, u64::MAX);

        let res = profile_data.get_action_cost(ActionCosts::add_full_access_key);
        assert_eq!(res, u64::MAX);
    }

    #[test]
    fn test_merge() {
        let mut profile_data = ProfileDataV2::default();
        profile_data.add_action_cost(ActionCosts::add_full_access_key, 111);
        profile_data.add_ext_cost(ExtCosts::storage_read_base, 11);

        let mut profile_data2 = ProfileDataV2::default();
        profile_data2.add_action_cost(ActionCosts::add_full_access_key, 222);
        profile_data2.add_ext_cost(ExtCosts::storage_read_base, 22);

        profile_data.merge(&profile_data2);
        assert_eq!(profile_data.get_action_cost(ActionCosts::add_full_access_key), 333);
        assert_eq!(profile_data.get_ext_cost(ExtCosts::storage_read_base), 33);
    }

    #[test]
    fn test_borsh_ser_deser() {
        let mut profile_data = ProfileDataV2::default();
        for (i, cost) in ExtCosts::iter().enumerate() {
            profile_data.add_ext_cost(cost, i as Gas);
        }
        for (i, cost) in ActionCosts::iter().enumerate() {
            profile_data.add_action_cost(cost, i as Gas + 1000);
        }
        let buf = profile_data.try_to_vec().expect("failed serializing a normal profile");

        let restored: ProfileDataV2 = BorshDeserialize::deserialize(&mut buf.as_slice())
            .expect("failed deserializing a normal profile");

        assert_eq!(profile_data, restored);
    }

    #[test]
    fn test_borsh_incomplete_profile() {
        let action_profile = vec![50u64, 60];
        let ext_profile = vec![100u64, 200];
        let wasm_cost = 99u64;
        let input = manually_encode_profile_v2(action_profile, ext_profile, wasm_cost);

        let profile: ProfileDataV2 = BorshDeserialize::deserialize(&mut input.as_slice())
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

        let profile: ProfileDataV2 = BorshDeserialize::deserialize(&mut input.as_slice()).expect(
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

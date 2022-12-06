use crate::config::{ActionCosts, ExtCosts};
use borsh::{BorshDeserialize, BorshSerialize};
use std::fmt;
use std::ops::{Index, IndexMut};
use strum::IntoEnumIterator;

/// Serialization format to store profiles in the database.
///
/// This is not part of the protocol but archival nodes still rely on this not
/// changing to answer old tx-status requests with a gas profile.
#[derive(Clone, PartialEq, Eq)]
pub struct DataArray(Box<[u64; Self::LEN]>);

impl DataArray {
    pub const LEN: usize = if cfg!(feature = "protocol_feature_ed25519_verify") { 75 } else { 73 };
}

impl Index<usize> for DataArray {
    type Output = u64;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl IndexMut<usize> for DataArray {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.0[index]
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

/// Profile of gas consumption.
/// When add new cost, the new cost should also be append to profile_index
#[derive(Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ProfileData {
    data: DataArray,
}

impl Default for ProfileData {
    fn default() -> ProfileData {
        ProfileData::new()
    }
}

impl ProfileData {
    #[inline]
    pub fn new() -> Self {
        let costs = DataArray(Box::new([0; DataArray::LEN]));
        ProfileData { data: costs }
    }

    #[inline]
    pub fn merge(&mut self, other: &ProfileData) {
        for i in 0..DataArray::LEN {
            self.data[i] = self.data[i].saturating_add(other.data[i]);
        }
    }

    #[inline]
    pub fn add_action_cost(&mut self, action: ActionCosts, value: u64) {
        self[Cost::ActionCost { action_cost_kind: action }] =
            self[Cost::ActionCost { action_cost_kind: action }].saturating_add(value);
    }

    #[inline]
    pub fn add_ext_cost(&mut self, ext: ExtCosts, value: u64) {
        self[Cost::ExtCost { ext_cost_kind: ext }] =
            self[Cost::ExtCost { ext_cost_kind: ext }].saturating_add(value);
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
        self[Cost::WasmInstruction] =
            total_gas_burnt.saturating_sub(self.action_gas()).saturating_sub(self.host_gas());
    }

    fn get_action_cost(&self, action: ActionCosts) -> u64 {
        self[Cost::ActionCost { action_cost_kind: action }]
    }

    pub fn get_ext_cost(&self, ext: ExtCosts) -> u64 {
        self[Cost::ExtCost { ext_cost_kind: ext }]
    }

    fn host_gas(&self) -> u64 {
        ExtCosts::iter().map(|a| self.get_ext_cost(a)).fold(0, u64::saturating_add)
    }

    pub fn action_gas(&self) -> u64 {
        // TODO(#8033): Temporary hack to work around multiple action costs
        // mapping to the same profile entry. Can be removed as soon as inner
        // tracking of profile is tracked by parameter or when the profile is
        // more detail, which ever happens first.
        let mut indices: Vec<_> = ActionCosts::iter()
            .map(|action_cost_kind| Cost::ActionCost { action_cost_kind }.profile_index())
            .collect();
        indices.sort_unstable();
        indices.dedup();
        indices.into_iter().map(|i| self.data[i]).fold(0, u64::saturating_add)
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

#[derive(Clone, Copy, Debug)]
pub enum Cost {
    ActionCost { action_cost_kind: ActionCosts },
    ExtCost { ext_cost_kind: ExtCosts },
    WasmInstruction,
}

impl Cost {
    pub fn iter() -> impl Iterator<Item = Self> {
        let actions =
            ActionCosts::iter().map(|action_cost_kind| Cost::ActionCost { action_cost_kind });
        let ext = ExtCosts::iter().map(|ext_cost_kind| Cost::ExtCost { ext_cost_kind });
        let op = std::iter::once(Cost::WasmInstruction);
        actions.chain(ext).chain(op)
    }

    /// Entry in DataArray inside the gas profile.
    ///
    /// Entries can be shared by multiple costs.
    ///
    /// TODO: Remove after completing all of #8033.
    pub fn profile_index(self) -> usize {
        // make sure to increase `DataArray::LEN` when adding a new index
        match self {
            Cost::ActionCost { action_cost_kind: ActionCosts::create_account } => 0,
            Cost::ActionCost { action_cost_kind: ActionCosts::delete_account } => 1,
            Cost::ActionCost { action_cost_kind: ActionCosts::deploy_contract_base } => 2,
            Cost::ActionCost { action_cost_kind: ActionCosts::deploy_contract_byte } => 2,
            Cost::ActionCost { action_cost_kind: ActionCosts::function_call_base } => 3,
            Cost::ActionCost { action_cost_kind: ActionCosts::function_call_byte } => 3,
            Cost::ActionCost { action_cost_kind: ActionCosts::transfer } => 4,
            Cost::ActionCost { action_cost_kind: ActionCosts::stake } => 5,
            Cost::ActionCost { action_cost_kind: ActionCosts::add_full_access_key } => 6,
            Cost::ActionCost { action_cost_kind: ActionCosts::add_function_call_key_base } => 6,
            Cost::ActionCost { action_cost_kind: ActionCosts::add_function_call_key_byte } => 6,
            Cost::ActionCost { action_cost_kind: ActionCosts::delete_key } => 7,
            Cost::ActionCost { action_cost_kind: ActionCosts::new_data_receipt_byte } => 8,
            Cost::ActionCost { action_cost_kind: ActionCosts::new_action_receipt } => 9,
            Cost::ActionCost { action_cost_kind: ActionCosts::new_data_receipt_base } => 9,
            Cost::ExtCost { ext_cost_kind: ExtCosts::base } => 10,
            Cost::ExtCost { ext_cost_kind: ExtCosts::contract_loading_base } => 11,
            Cost::ExtCost { ext_cost_kind: ExtCosts::contract_loading_bytes } => 12,
            Cost::ExtCost { ext_cost_kind: ExtCosts::read_memory_base } => 13,
            Cost::ExtCost { ext_cost_kind: ExtCosts::read_memory_byte } => 14,
            Cost::ExtCost { ext_cost_kind: ExtCosts::write_memory_base } => 15,
            Cost::ExtCost { ext_cost_kind: ExtCosts::write_memory_byte } => 16,
            Cost::ExtCost { ext_cost_kind: ExtCosts::read_register_base } => 17,
            Cost::ExtCost { ext_cost_kind: ExtCosts::read_register_byte } => 18,
            Cost::ExtCost { ext_cost_kind: ExtCosts::write_register_base } => 19,
            Cost::ExtCost { ext_cost_kind: ExtCosts::write_register_byte } => 20,
            Cost::ExtCost { ext_cost_kind: ExtCosts::utf8_decoding_base } => 21,
            Cost::ExtCost { ext_cost_kind: ExtCosts::utf8_decoding_byte } => 22,
            Cost::ExtCost { ext_cost_kind: ExtCosts::utf16_decoding_base } => 23,
            Cost::ExtCost { ext_cost_kind: ExtCosts::utf16_decoding_byte } => 24,
            Cost::ExtCost { ext_cost_kind: ExtCosts::sha256_base } => 25,
            Cost::ExtCost { ext_cost_kind: ExtCosts::sha256_byte } => 26,
            Cost::ExtCost { ext_cost_kind: ExtCosts::keccak256_base } => 27,
            Cost::ExtCost { ext_cost_kind: ExtCosts::keccak256_byte } => 28,
            Cost::ExtCost { ext_cost_kind: ExtCosts::keccak512_base } => 29,
            Cost::ExtCost { ext_cost_kind: ExtCosts::keccak512_byte } => 30,
            Cost::ExtCost { ext_cost_kind: ExtCosts::ripemd160_base } => 31,
            Cost::ExtCost { ext_cost_kind: ExtCosts::ripemd160_block } => 32,
            Cost::ExtCost { ext_cost_kind: ExtCosts::ecrecover_base } => 33,
            Cost::ExtCost { ext_cost_kind: ExtCosts::log_base } => 34,
            Cost::ExtCost { ext_cost_kind: ExtCosts::log_byte } => 35,
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_write_base } => 36,
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_write_key_byte } => 37,
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_write_value_byte } => 38,
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_write_evicted_byte } => 39,
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_read_base } => 40,
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_read_key_byte } => 41,
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_read_value_byte } => 42,
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_remove_base } => 43,
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_remove_key_byte } => 44,
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_remove_ret_value_byte } => 45,
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_has_key_base } => 46,
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_has_key_byte } => 47,
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_prefix_base } => 48,
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_prefix_byte } => 49,
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_range_base } => 50,
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_from_byte } => 51,
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_to_byte } => 52,
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_next_base } => 53,
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_next_key_byte } => 54,
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_next_value_byte } => 55,
            Cost::ExtCost { ext_cost_kind: ExtCosts::touching_trie_node } => 56,
            Cost::ExtCost { ext_cost_kind: ExtCosts::promise_and_base } => 57,
            Cost::ExtCost { ext_cost_kind: ExtCosts::promise_and_per_promise } => 58,
            Cost::ExtCost { ext_cost_kind: ExtCosts::promise_return } => 59,
            Cost::ExtCost { ext_cost_kind: ExtCosts::validator_stake_base } => 60,
            Cost::ExtCost { ext_cost_kind: ExtCosts::validator_total_stake_base } => 61,
            Cost::WasmInstruction => 62,
            Cost::ExtCost { ext_cost_kind: ExtCosts::read_cached_trie_node } => 63,
            Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_g1_multiexp_base } => 64,
            Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_g1_multiexp_element } => 65,
            Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_pairing_check_base } => 66,
            Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_pairing_check_element } => 67,
            Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_g1_sum_base } => 68,
            Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_g1_sum_element } => 69,
            Cost::ExtCost { ext_cost_kind: ExtCosts::bls12381_verify_base } => 70,
            Cost::ExtCost { ext_cost_kind: ExtCosts::bls12381_verify_byte } => 71,
            Cost::ExtCost { ext_cost_kind: ExtCosts::bls12381_verify_elements } => 72,
            #[cfg(feature = "protocol_feature_ed25519_verify")]
            Cost::ExtCost { ext_cost_kind: ExtCosts::ed25519_verify_base } => 73,
            #[cfg(feature = "protocol_feature_ed25519_verify")]
            Cost::ExtCost { ext_cost_kind: ExtCosts::ed25519_verify_byte } => 74,
        }
    }
}

impl Index<Cost> for ProfileData {
    type Output = u64;

    fn index(&self, index: Cost) -> &Self::Output {
        &self.data[index.profile_index()]
    }
}

impl IndexMut<Cost> for ProfileData {
    fn index_mut(&mut self, index: Cost) -> &mut Self::Output {
        &mut self.data[index.profile_index()]
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
        profile_data.add_action_cost(ActionCosts::add_full_access_key, u64::MAX);
        profile_data.add_action_cost(ActionCosts::add_full_access_key, u64::MAX);

        let res = profile_data.get_action_cost(ActionCosts::add_full_access_key);
        assert_eq!(res, u64::MAX);
    }

    #[test]
    fn test_merge() {
        let mut profile_data = ProfileData::new();
        profile_data.add_action_cost(ActionCosts::add_full_access_key, 111);
        profile_data.add_ext_cost(ExtCosts::storage_read_base, 11);

        let mut profile_data2 = ProfileData::new();
        profile_data2.add_action_cost(ActionCosts::add_full_access_key, 222);
        profile_data2.add_ext_cost(ExtCosts::storage_read_base, 22);

        profile_data.merge(&profile_data2);
        assert_eq!(profile_data.get_action_cost(ActionCosts::add_full_access_key), 333);
        assert_eq!(profile_data.get_ext_cost(ExtCosts::storage_read_base), 33);
    }

    #[test]
    fn test_profile_len() {
        let mut indices: Vec<_> = Cost::iter().map(|i| i.profile_index()).collect();
        indices.sort();
        indices.dedup();
        assert_eq!(indices.len(), DataArray::LEN);
    }
}

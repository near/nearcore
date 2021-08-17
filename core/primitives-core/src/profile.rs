use std::fmt;
use std::ops::{Index, IndexMut};

use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use crate::config::{ActionCosts, ExtCosts};
use crate::types::Gas;

/// Profile of gas consumption.
/// Vecs are used for forward compatible. should only append new costs and never remove old costs
/// When add new cost, the new cost should also be append to ProfileData::ALL
#[derive(Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ProfileData {
    costs: Vec<u64>,
}

impl Default for ProfileData {
    fn default() -> ProfileData {
        ProfileData::new()
    }
}

impl ProfileData {
    #[inline]
    pub fn new() -> Self {
        let costs = vec![0; Cost::ALL.len()];
        ProfileData { costs }
    }

    #[inline]
    pub fn merge(&mut self, other: &ProfileData) {
        for i in 0..self.costs.len() {
            self.costs[i] = self.costs[i].saturating_add(other.costs[i]);
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

    pub fn get_action_cost(&self, action: ActionCosts) -> u64 {
        self[Cost::ActionCost { action_cost_kind: action }]
    }

    pub fn get_ext_cost(&self, ext: ExtCosts) -> u64 {
        self[Cost::ExtCost { ext_cost_kind: ext }]
    }

    pub fn host_gas(&self) -> u64 {
        let mut host_gas = 0u64;
        for cost in Cost::ALL {
            match cost {
                Cost::ExtCost { ext_cost_kind: e } => host_gas += self.get_ext_cost(*e),
                _ => {}
            }
        }
        host_gas
    }

    pub fn action_gas(&self) -> u64 {
        let mut action_gas = 0u64;
        for cost in Cost::ALL {
            match cost {
                Cost::ActionCost { action_cost_kind: a } => action_gas += self.get_action_cost(*a),
                _ => {}
            }
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
        for cost in Cost::ALL {
            match cost {
                Cost::ExtCost { ext_cost_kind: e } => {
                    let d = self.get_ext_cost(*e);
                    if d != 0 {
                        writeln!(
                            f,
                            "{} -> {} [{}% host]",
                            ExtCosts::name_of(*e as usize),
                            d,
                            Ratio::new(d * 100, host_gas).to_integer(),
                        )?;
                    }
                }
                _ => {}
            }
        }
        writeln!(f, "------ Actions --------")?;
        for cost in Cost::ALL {
            match cost {
                Cost::ActionCost { action_cost_kind: a } => {
                    let d = self.get_action_cost(*a);
                    if d != 0 {
                        writeln!(f, "{} -> {}", ActionCosts::name_of(*a as usize), d)?;
                    }
                }
                _ => {}
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
    Copy,
    BorshSerialize,
    BorshDeserialize,
    Debug,
)]
#[serde(tag = "cost_kind")]
pub enum Cost {
    ActionCost { action_cost_kind: ActionCosts },
    ExtCost { ext_cost_kind: ExtCosts },
}

impl Cost {
    const ALL: &'static [Cost] = &[
        // ActionCost is unlikely to have new ones, so have it at first
        Cost::ActionCost { action_cost_kind: ActionCosts::create_account },
        Cost::ActionCost { action_cost_kind: ActionCosts::delete_account },
        Cost::ActionCost { action_cost_kind: ActionCosts::deploy_contract },
        Cost::ActionCost { action_cost_kind: ActionCosts::function_call },
        Cost::ActionCost { action_cost_kind: ActionCosts::transfer },
        Cost::ActionCost { action_cost_kind: ActionCosts::stake },
        Cost::ActionCost { action_cost_kind: ActionCosts::add_key },
        Cost::ActionCost { action_cost_kind: ActionCosts::delete_key },
        Cost::ActionCost { action_cost_kind: ActionCosts::value_return },
        Cost::ActionCost { action_cost_kind: ActionCosts::new_receipt },
        Cost::ExtCost { ext_cost_kind: ExtCosts::base },
        Cost::ExtCost { ext_cost_kind: ExtCosts::contract_compile_base },
        Cost::ExtCost { ext_cost_kind: ExtCosts::contract_compile_bytes },
        Cost::ExtCost { ext_cost_kind: ExtCosts::read_memory_base },
        Cost::ExtCost { ext_cost_kind: ExtCosts::read_memory_byte },
        Cost::ExtCost { ext_cost_kind: ExtCosts::write_memory_base },
        Cost::ExtCost { ext_cost_kind: ExtCosts::write_memory_byte },
        Cost::ExtCost { ext_cost_kind: ExtCosts::read_register_base },
        Cost::ExtCost { ext_cost_kind: ExtCosts::read_register_byte },
        Cost::ExtCost { ext_cost_kind: ExtCosts::write_register_base },
        Cost::ExtCost { ext_cost_kind: ExtCosts::write_register_byte },
        Cost::ExtCost { ext_cost_kind: ExtCosts::utf8_decoding_base },
        Cost::ExtCost { ext_cost_kind: ExtCosts::utf8_decoding_byte },
        Cost::ExtCost { ext_cost_kind: ExtCosts::utf16_decoding_base },
        Cost::ExtCost { ext_cost_kind: ExtCosts::utf16_decoding_byte },
        Cost::ExtCost { ext_cost_kind: ExtCosts::sha256_base },
        Cost::ExtCost { ext_cost_kind: ExtCosts::sha256_byte },
        Cost::ExtCost { ext_cost_kind: ExtCosts::keccak256_base },
        Cost::ExtCost { ext_cost_kind: ExtCosts::keccak256_byte },
        Cost::ExtCost { ext_cost_kind: ExtCosts::keccak512_base },
        Cost::ExtCost { ext_cost_kind: ExtCosts::keccak512_byte },
        Cost::ExtCost { ext_cost_kind: ExtCosts::ripemd160_base },
        Cost::ExtCost { ext_cost_kind: ExtCosts::ripemd160_block },
        Cost::ExtCost { ext_cost_kind: ExtCosts::ecrecover_base },
        Cost::ExtCost { ext_cost_kind: ExtCosts::log_base },
        Cost::ExtCost { ext_cost_kind: ExtCosts::log_byte },
        Cost::ExtCost { ext_cost_kind: ExtCosts::storage_write_base },
        Cost::ExtCost { ext_cost_kind: ExtCosts::storage_write_key_byte },
        Cost::ExtCost { ext_cost_kind: ExtCosts::storage_write_value_byte },
        Cost::ExtCost { ext_cost_kind: ExtCosts::storage_write_evicted_byte },
        Cost::ExtCost { ext_cost_kind: ExtCosts::storage_read_base },
        Cost::ExtCost { ext_cost_kind: ExtCosts::storage_read_key_byte },
        Cost::ExtCost { ext_cost_kind: ExtCosts::storage_read_value_byte },
        Cost::ExtCost { ext_cost_kind: ExtCosts::storage_remove_base },
        Cost::ExtCost { ext_cost_kind: ExtCosts::storage_remove_key_byte },
        Cost::ExtCost { ext_cost_kind: ExtCosts::storage_remove_ret_value_byte },
        Cost::ExtCost { ext_cost_kind: ExtCosts::storage_has_key_base },
        Cost::ExtCost { ext_cost_kind: ExtCosts::storage_has_key_byte },
        Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_prefix_base },
        Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_prefix_byte },
        Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_range_base },
        Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_from_byte },
        Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_to_byte },
        Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_next_base },
        Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_next_key_byte },
        Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_next_value_byte },
        Cost::ExtCost { ext_cost_kind: ExtCosts::touching_trie_node },
        Cost::ExtCost { ext_cost_kind: ExtCosts::promise_and_base },
        Cost::ExtCost { ext_cost_kind: ExtCosts::promise_and_per_promise },
        Cost::ExtCost { ext_cost_kind: ExtCosts::promise_return },
        Cost::ExtCost { ext_cost_kind: ExtCosts::validator_stake_base },
        Cost::ExtCost { ext_cost_kind: ExtCosts::validator_total_stake_base },
        #[cfg(feature = "protocol_feature_alt_bn128")]
        Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_g1_multiexp_base },
        #[cfg(feature = "protocol_feature_alt_bn128")]
        Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_g1_multiexp_byte },
        #[cfg(feature = "protocol_feature_alt_bn128")]
        Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_g1_multiexp_sublinear },
        #[cfg(feature = "protocol_feature_alt_bn128")]
        Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_pairing_check_base },
        #[cfg(feature = "protocol_feature_alt_bn128")]
        Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_pairing_check_byte },
        #[cfg(feature = "protocol_feature_alt_bn128")]
        Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_g1_sum_base },
        #[cfg(feature = "protocol_feature_alt_bn128")]
        Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_g1_sum_byte },
    ];
}

impl Index<Cost> for ProfileData {
    type Output = u64;

    fn index(&self, index: Cost) -> &Self::Output {
        match index {
            Cost::ActionCost { action_cost_kind: ActionCosts::create_account } => &self.costs[0],
            Cost::ActionCost { action_cost_kind: ActionCosts::delete_account } => &self.costs[1],
            Cost::ActionCost { action_cost_kind: ActionCosts::deploy_contract } => &self.costs[2],
            Cost::ActionCost { action_cost_kind: ActionCosts::function_call } => &self.costs[3],
            Cost::ActionCost { action_cost_kind: ActionCosts::transfer } => &self.costs[4],
            Cost::ActionCost { action_cost_kind: ActionCosts::stake } => &self.costs[5],
            Cost::ActionCost { action_cost_kind: ActionCosts::add_key } => &self.costs[6],
            Cost::ActionCost { action_cost_kind: ActionCosts::delete_key } => &self.costs[7],
            Cost::ActionCost { action_cost_kind: ActionCosts::value_return } => &self.costs[8],
            Cost::ActionCost { action_cost_kind: ActionCosts::new_receipt } => &self.costs[9],
            Cost::ActionCost { action_cost_kind: ActionCosts::__count } => unreachable!(),
            Cost::ExtCost { ext_cost_kind: ExtCosts::base } => &self.costs[10],
            Cost::ExtCost { ext_cost_kind: ExtCosts::contract_compile_base } => &self.costs[11],
            Cost::ExtCost { ext_cost_kind: ExtCosts::contract_compile_bytes } => &self.costs[12],
            Cost::ExtCost { ext_cost_kind: ExtCosts::read_memory_base } => &self.costs[13],
            Cost::ExtCost { ext_cost_kind: ExtCosts::read_memory_byte } => &self.costs[14],
            Cost::ExtCost { ext_cost_kind: ExtCosts::write_memory_base } => &self.costs[15],
            Cost::ExtCost { ext_cost_kind: ExtCosts::write_memory_byte } => &self.costs[16],
            Cost::ExtCost { ext_cost_kind: ExtCosts::read_register_base } => &self.costs[17],
            Cost::ExtCost { ext_cost_kind: ExtCosts::read_register_byte } => &self.costs[18],
            Cost::ExtCost { ext_cost_kind: ExtCosts::write_register_base } => &self.costs[19],
            Cost::ExtCost { ext_cost_kind: ExtCosts::write_register_byte } => &self.costs[20],
            Cost::ExtCost { ext_cost_kind: ExtCosts::utf8_decoding_base } => &self.costs[21],
            Cost::ExtCost { ext_cost_kind: ExtCosts::utf8_decoding_byte } => &self.costs[22],
            Cost::ExtCost { ext_cost_kind: ExtCosts::utf16_decoding_base } => &self.costs[23],
            Cost::ExtCost { ext_cost_kind: ExtCosts::utf16_decoding_byte } => &self.costs[24],
            Cost::ExtCost { ext_cost_kind: ExtCosts::sha256_base } => &self.costs[25],
            Cost::ExtCost { ext_cost_kind: ExtCosts::sha256_byte } => &self.costs[26],
            Cost::ExtCost { ext_cost_kind: ExtCosts::keccak256_base } => &self.costs[27],
            Cost::ExtCost { ext_cost_kind: ExtCosts::keccak256_byte } => &self.costs[28],
            Cost::ExtCost { ext_cost_kind: ExtCosts::keccak512_base } => &self.costs[29],
            Cost::ExtCost { ext_cost_kind: ExtCosts::keccak512_byte } => &self.costs[30],
            Cost::ExtCost { ext_cost_kind: ExtCosts::ripemd160_base } => &self.costs[31],
            Cost::ExtCost { ext_cost_kind: ExtCosts::ripemd160_block } => &self.costs[32],
            Cost::ExtCost { ext_cost_kind: ExtCosts::ecrecover_base } => &self.costs[33],
            Cost::ExtCost { ext_cost_kind: ExtCosts::log_base } => &self.costs[34],
            Cost::ExtCost { ext_cost_kind: ExtCosts::log_byte } => &self.costs[35],
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_write_base } => &self.costs[36],
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_write_key_byte } => &self.costs[37],
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_write_value_byte } => &self.costs[38],
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_write_evicted_byte } => {
                &self.costs[39]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_read_base } => &self.costs[40],
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_read_key_byte } => &self.costs[41],
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_read_value_byte } => &self.costs[42],
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_remove_base } => &self.costs[43],
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_remove_key_byte } => &self.costs[44],
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_remove_ret_value_byte } => {
                &self.costs[45]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_has_key_base } => &self.costs[46],
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_has_key_byte } => &self.costs[47],
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_prefix_base } => {
                &self.costs[48]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_prefix_byte } => {
                &self.costs[49]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_range_base } => {
                &self.costs[50]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_from_byte } => {
                &self.costs[51]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_to_byte } => {
                &self.costs[52]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_next_base } => &self.costs[53],
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_next_key_byte } => {
                &self.costs[54]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_next_value_byte } => {
                &self.costs[55]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::touching_trie_node } => &self.costs[56],
            Cost::ExtCost { ext_cost_kind: ExtCosts::promise_and_base } => &self.costs[57],
            Cost::ExtCost { ext_cost_kind: ExtCosts::promise_and_per_promise } => &self.costs[58],
            Cost::ExtCost { ext_cost_kind: ExtCosts::promise_return } => &self.costs[59],
            Cost::ExtCost { ext_cost_kind: ExtCosts::validator_stake_base } => &self.costs[60],
            Cost::ExtCost { ext_cost_kind: ExtCosts::validator_total_stake_base } => {
                &self.costs[61]
            }
            #[cfg(feature = "protocol_feature_alt_bn128")]
            Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_g1_multiexp_base } => {
                &self.costs[62]
            }
            #[cfg(feature = "protocol_feature_alt_bn128")]
            Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_g1_multiexp_byte } => {
                &self.costs[63]
            }
            #[cfg(feature = "protocol_feature_alt_bn128")]
            Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_g1_multiexp_sublinear } => {
                &self.costs[64]
            }
            #[cfg(feature = "protocol_feature_alt_bn128")]
            Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_pairing_check_base } => {
                &self.costs[65]
            }
            #[cfg(feature = "protocol_feature_alt_bn128")]
            Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_pairing_check_byte } => {
                &self.costs[66]
            }
            #[cfg(feature = "protocol_feature_alt_bn128")]
            Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_g1_sum_base } => &self.costs[67],
            #[cfg(feature = "protocol_feature_alt_bn128")]
            Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_g1_sum_byte } => &self.costs[68],
            Cost::ExtCost { ext_cost_kind: ExtCosts::__count } => unreachable!(),
        }
    }
}

impl IndexMut<Cost> for ProfileData {
    fn index_mut(&mut self, index: Cost) -> &mut Self::Output {
        match index {
            Cost::ActionCost { action_cost_kind: ActionCosts::create_account } => {
                &mut self.costs[0]
            }
            Cost::ActionCost { action_cost_kind: ActionCosts::delete_account } => {
                &mut self.costs[1]
            }
            Cost::ActionCost { action_cost_kind: ActionCosts::deploy_contract } => {
                &mut self.costs[2]
            }
            Cost::ActionCost { action_cost_kind: ActionCosts::function_call } => &mut self.costs[3],
            Cost::ActionCost { action_cost_kind: ActionCosts::transfer } => &mut self.costs[4],
            Cost::ActionCost { action_cost_kind: ActionCosts::stake } => &mut self.costs[5],
            Cost::ActionCost { action_cost_kind: ActionCosts::add_key } => &mut self.costs[6],
            Cost::ActionCost { action_cost_kind: ActionCosts::delete_key } => &mut self.costs[7],
            Cost::ActionCost { action_cost_kind: ActionCosts::value_return } => &mut self.costs[8],
            Cost::ActionCost { action_cost_kind: ActionCosts::new_receipt } => &mut self.costs[9],
            Cost::ActionCost { action_cost_kind: ActionCosts::__count } => unreachable!(),
            Cost::ExtCost { ext_cost_kind: ExtCosts::base } => &mut self.costs[10],
            Cost::ExtCost { ext_cost_kind: ExtCosts::contract_compile_base } => &mut self.costs[11],
            Cost::ExtCost { ext_cost_kind: ExtCosts::contract_compile_bytes } => {
                &mut self.costs[12]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::read_memory_base } => &mut self.costs[13],
            Cost::ExtCost { ext_cost_kind: ExtCosts::read_memory_byte } => &mut self.costs[14],
            Cost::ExtCost { ext_cost_kind: ExtCosts::write_memory_base } => &mut self.costs[15],
            Cost::ExtCost { ext_cost_kind: ExtCosts::write_memory_byte } => &mut self.costs[16],
            Cost::ExtCost { ext_cost_kind: ExtCosts::read_register_base } => &mut self.costs[17],
            Cost::ExtCost { ext_cost_kind: ExtCosts::read_register_byte } => &mut self.costs[18],
            Cost::ExtCost { ext_cost_kind: ExtCosts::write_register_base } => &mut self.costs[19],
            Cost::ExtCost { ext_cost_kind: ExtCosts::write_register_byte } => &mut self.costs[20],
            Cost::ExtCost { ext_cost_kind: ExtCosts::utf8_decoding_base } => &mut self.costs[21],
            Cost::ExtCost { ext_cost_kind: ExtCosts::utf8_decoding_byte } => &mut self.costs[22],
            Cost::ExtCost { ext_cost_kind: ExtCosts::utf16_decoding_base } => &mut self.costs[23],
            Cost::ExtCost { ext_cost_kind: ExtCosts::utf16_decoding_byte } => &mut self.costs[24],
            Cost::ExtCost { ext_cost_kind: ExtCosts::sha256_base } => &mut self.costs[25],
            Cost::ExtCost { ext_cost_kind: ExtCosts::sha256_byte } => &mut self.costs[26],
            Cost::ExtCost { ext_cost_kind: ExtCosts::keccak256_base } => &mut self.costs[27],
            Cost::ExtCost { ext_cost_kind: ExtCosts::keccak256_byte } => &mut self.costs[28],
            Cost::ExtCost { ext_cost_kind: ExtCosts::keccak512_base } => &mut self.costs[29],
            Cost::ExtCost { ext_cost_kind: ExtCosts::keccak512_byte } => &mut self.costs[30],
            Cost::ExtCost { ext_cost_kind: ExtCosts::ripemd160_base } => &mut self.costs[31],
            Cost::ExtCost { ext_cost_kind: ExtCosts::ripemd160_block } => &mut self.costs[32],
            Cost::ExtCost { ext_cost_kind: ExtCosts::ecrecover_base } => &mut self.costs[33],
            Cost::ExtCost { ext_cost_kind: ExtCosts::log_base } => &mut self.costs[34],
            Cost::ExtCost { ext_cost_kind: ExtCosts::log_byte } => &mut self.costs[35],
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_write_base } => &mut self.costs[36],
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_write_key_byte } => {
                &mut self.costs[37]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_write_value_byte } => {
                &mut self.costs[38]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_write_evicted_byte } => {
                &mut self.costs[39]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_read_base } => &mut self.costs[40],
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_read_key_byte } => &mut self.costs[41],
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_read_value_byte } => {
                &mut self.costs[42]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_remove_base } => &mut self.costs[43],
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_remove_key_byte } => {
                &mut self.costs[44]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_remove_ret_value_byte } => {
                &mut self.costs[45]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_has_key_base } => &mut self.costs[46],
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_has_key_byte } => &mut self.costs[47],
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_prefix_base } => {
                &mut self.costs[48]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_prefix_byte } => {
                &mut self.costs[49]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_range_base } => {
                &mut self.costs[50]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_from_byte } => {
                &mut self.costs[51]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_create_to_byte } => {
                &mut self.costs[52]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_next_base } => {
                &mut self.costs[53]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_next_key_byte } => {
                &mut self.costs[54]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::storage_iter_next_value_byte } => {
                &mut self.costs[55]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::touching_trie_node } => &mut self.costs[56],
            Cost::ExtCost { ext_cost_kind: ExtCosts::promise_and_base } => &mut self.costs[57],
            Cost::ExtCost { ext_cost_kind: ExtCosts::promise_and_per_promise } => {
                &mut self.costs[58]
            }
            Cost::ExtCost { ext_cost_kind: ExtCosts::promise_return } => &mut self.costs[59],
            Cost::ExtCost { ext_cost_kind: ExtCosts::validator_stake_base } => &mut self.costs[60],
            Cost::ExtCost { ext_cost_kind: ExtCosts::validator_total_stake_base } => {
                &mut self.costs[61]
            }
            #[cfg(feature = "protocol_feature_alt_bn128")]
            Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_g1_multiexp_base } => {
                &mut self.costs[62]
            }
            #[cfg(feature = "protocol_feature_alt_bn128")]
            Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_g1_multiexp_byte } => {
                &mut self.costs[63]
            }
            #[cfg(feature = "protocol_feature_alt_bn128")]
            Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_g1_multiexp_sublinear } => {
                &mut self.costs[64]
            }
            #[cfg(feature = "protocol_feature_alt_bn128")]
            Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_pairing_check_base } => {
                &mut self.costs[65]
            }
            #[cfg(feature = "protocol_feature_alt_bn128")]
            Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_pairing_check_byte } => {
                &mut self.costs[66]
            }
            #[cfg(feature = "protocol_feature_alt_bn128")]
            Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_g1_sum_base } => &mut self.costs[67],
            #[cfg(feature = "protocol_feature_alt_bn128")]
            Cost::ExtCost { ext_cost_kind: ExtCosts::alt_bn128_g1_sum_byte } => &mut self.costs[68],
            Cost::ExtCost { ext_cost_kind: ExtCosts::__count } => unreachable!(),
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, PartialEq, Clone, Eq, Debug)]
pub struct CostGasUsed {
    #[serde(flatten)]
    pub cost: Cost,
    pub gas_used: Gas,
}

impl ProfileData {
    pub fn nonzero_costs(&self) -> Vec<CostGasUsed> {
        let mut data = Vec::new();
        for i in Cost::ALL {
            if self[*i] > 0 {
                data.push(CostGasUsed { cost: *i, gas_used: self[*i] });
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

        let res = profile_data.get_action_cost(ActionCosts::function_call);
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
        assert_eq!(profile_data.get_action_cost(ActionCosts::function_call), 333);
        assert_eq!(profile_data.get_ext_cost(ExtCosts::storage_read_base), 33);
    }
}

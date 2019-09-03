use crate::types::Gas;
use near_runtime_fees::RuntimeFeesConfig;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug, Hash, Serialize, Deserialize)]
pub struct Config {
    /// Fees for creating actions on runtime.
    pub runtime_fees: RuntimeFeesConfig,

    /// Gas cost of a growing memory by single page.
    pub grow_mem_cost: u32,
    /// Gas cost of a regular operation.
    pub regular_op_cost: u32,
    /// Max amount of gas that can be used, excluding gas attached to promises.
    pub max_gas_burnt: Gas,

    /// How tall the stack is allowed to grow?
    ///
    /// See https://wiki.parity.io/WebAssembly-StackHeight to find out
    /// how the stack frame cost is calculated.
    pub max_stack_height: u32,
    /// The initial number of memory pages.
    pub initial_memory_pages: u32,
    /// What is the maximal memory pages amount is allowed to have for
    /// a contract.
    pub max_memory_pages: u32,

    /// Limit of memory used by registers.
    pub registers_memory_limit: u64,
    /// Maximum number of bytes that can be stored in a single register.
    pub max_register_size: u64,
    /// Maximum number of registers that can be used simultaneously.
    pub max_number_registers: u64,

    /// Maximum number of log entries.
    pub max_number_logs: u64,
    /// Maximum length of a single log, in bytes.
    pub max_log_len: u64,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            grow_mem_cost: 1,
            regular_op_cost: 1,
            max_gas_burnt: 10u64.pow(9),
            max_stack_height: 64 * 1024,
            initial_memory_pages: 17,
            max_memory_pages: 32,
            // By default registers are limited by 1GiB of memory.
            registers_memory_limit: 2u64.pow(30),
            // By default each register is limited by 100MiB of memory.
            max_register_size: 2u64.pow(20) * 100,
            // By default there is at most 100 registers.
            max_number_registers: 100,
            max_number_logs: 100,
            max_log_len: 500,
            runtime_fees: Default::default(),
        }
    }
}

impl Config {
    /// Computes non-cryptographically-proof hash. The computation is fast but not cryptographically
    /// secure.
    pub fn non_crypto_hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.hash(&mut s);
        s.finish()
    }
}

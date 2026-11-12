//! Settings of the parameters of the runtime.
use super::parameter_table::InvalidConfigError;
use crate::cost::RuntimeFeesConfig;
use crate::parameter_table::ParameterTable;
use near_account_id::AccountId;
use near_primitives_core::types::{Balance, Compute, Gas, ProtocolVersion};
use near_primitives_core::version::PROTOCOL_VERSION;
use num_rational::Rational32;
use std::sync::Arc;

// Lowered promise yield timeout length used in integration tests.
// The resharding tests for yield timeouts take too long to run otherwise.
pub const TEST_CONFIG_YIELD_TIMEOUT_LENGTH: u64 = 10;

/// The structure that holds the parameters of the runtime, mostly economics.
#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeConfig {
    /// Action gas costs, storage fees, and economic constants around them.
    ///
    /// This contains parameters that are required by the WASM runtime and the
    /// transaction runtime.
    pub fees: Arc<RuntimeFeesConfig>,
    /// Config of wasm operations, also includes wasm gas costs.
    ///
    /// This contains all the configuration parameters that are only required by
    /// the WASM runtime.
    pub wasm_config: Arc<crate::vm::Config>,
    /// Config that defines rules for account creation.
    pub account_creation_config: AccountCreationConfig,
    /// The configuration for congestion control.
    pub congestion_control_config: CongestionControlConfig,
    /// Configuration specific to ChunkStateWitness.
    pub witness_config: WitnessConfig,
    /// Limits on external contract calls, per call and per chunk.
    pub external_calls_config: ExternalCallsConfig,
    /// Configuration specific to BandwidthScheduler.
    pub bandwidth_scheduler_config: BandwidthSchedulerConfig,

    /// Minimum price at which the gas attached to a receipt is purchased. The price at which it is
    /// burned might be lower, in which case the difference is refunded after execution.
    pub min_gas_purchase_price: Balance,

    /// How much creating an account should cost in NEAR. Taken into account when burning gas for
    /// account creation.
    pub account_creation_charge: Balance,
}

impl RuntimeConfig {
    pub(crate) fn new(params: &ParameterTable) -> Result<Self, InvalidConfigError> {
        RuntimeConfig::try_from(params)
    }

    pub fn test() -> Self {
        Self::test_protocol_version(PROTOCOL_VERSION)
    }

    pub fn test_protocol_version(protocol_version: ProtocolVersion) -> Self {
        let config_store = super::config_store::RuntimeConfigStore::new();
        let runtime_config = config_store.get_config(protocol_version);

        let mut wasm_config = crate::vm::Config::clone(&runtime_config.wasm_config);
        // Lower the yield timeout length so that we can observe timeouts in integration tests.
        wasm_config.limit_config.yield_timeout_length_in_blocks = TEST_CONFIG_YIELD_TIMEOUT_LENGTH;

        RuntimeConfig {
            fees: Arc::new(RuntimeFeesConfig::test()),
            wasm_config: Arc::new(wasm_config),
            account_creation_config: AccountCreationConfig::default(),
            congestion_control_config: runtime_config.congestion_control_config,
            witness_config: runtime_config.witness_config,
            external_calls_config: runtime_config.external_calls_config,
            bandwidth_scheduler_config: runtime_config.bandwidth_scheduler_config,
            min_gas_purchase_price: runtime_config.min_gas_purchase_price,
            account_creation_charge: runtime_config.account_creation_charge,
        }
    }

    pub fn free() -> Self {
        let config_store = super::config_store::RuntimeConfigStore::new();
        let runtime_config = config_store.get_config(PROTOCOL_VERSION);

        let mut wasm_config = crate::vm::Config::clone(&runtime_config.wasm_config);
        wasm_config.make_free();

        Self {
            fees: Arc::new(RuntimeFeesConfig::free()),
            wasm_config: Arc::new(wasm_config),
            account_creation_config: AccountCreationConfig::default(),
            congestion_control_config: runtime_config.congestion_control_config,
            witness_config: runtime_config.witness_config,
            external_calls_config: runtime_config.external_calls_config,
            bandwidth_scheduler_config: runtime_config.bandwidth_scheduler_config,
            min_gas_purchase_price: Balance::ZERO,
            // The free config disables all gas costs; keep account_creation_charge at
            // zero as well so the invariant min_gas_purchase_price * create_account_gas_cost >=
            // account_creation_charge holds trivially (0 >= 0).
            account_creation_charge: Balance::ZERO,
        }
    }

    pub fn storage_amount_per_byte(&self) -> Balance {
        self.fees.storage_usage_config.storage_amount_per_byte
    }
}

/// The structure describes configuration for creation of new accounts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountCreationConfig {
    /// The minimum length of the top-level account ID that is allowed to be created by any account.
    pub min_allowed_top_level_account_length: u8,
    /// The account ID of the account registrar. This account ID allowed to create top-level
    /// accounts of any valid length.
    pub registrar_account_id: AccountId,
}

impl Default for AccountCreationConfig {
    fn default() -> Self {
        Self {
            min_allowed_top_level_account_length: 0,
            registrar_account_id: "registrar".parse().unwrap(),
        }
    }
}

/// The configuration for congestion control.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct CongestionControlConfig {
    /// How much gas in delayed receipts of a shard is 100% incoming congestion.
    ///
    /// Based on incoming congestion levels, a shard reduces the gas it spends on
    /// accepting new transactions instead of working on incoming receipts. Plus,
    /// incoming congestion contributes to overall congestion, which reduces how
    /// much other shards are allowed to forward to this shard.
    pub max_congestion_incoming_gas: Gas,

    /// How much gas in outgoing buffered receipts of a shard is 100% congested.
    ///
    /// Outgoing congestion contributes to overall congestion, which reduces how
    /// much other shards are allowed to forward to this shard.
    pub max_congestion_outgoing_gas: Gas,

    /// How much memory space of all delayed and buffered receipts in a shard is
    /// considered 100% congested.
    ///
    /// Memory congestion contributes to overall congestion, which reduces how much
    /// other shards are allowed to forward to this shard.
    ///
    /// This threshold limits memory requirements of validators to a degree but it
    /// is not a hard guarantee.
    pub max_congestion_memory_consumption: u64,

    /// How many missed chunks in a row in a shard is considered 100% congested.
    pub max_congestion_missed_chunks: u64,

    /// The maximum amount of gas attached to receipts a shard can forward to
    /// another shard per chunk.
    ///
    /// The actual gas forwarding allowance is a linear interpolation between
    /// [MIN_OUTGOING_GAS](CongestionControlConfig::min_outgoing_gas) and
    /// [MAX_OUTGOING_GAS](CongestionControlConfig::max_outgoing_gas), or 0 if the receiver is
    /// fully congested.
    pub max_outgoing_gas: Gas,

    /// The minimum gas each shard can send to a shard that is not fully congested.
    ///
    /// The actual gas forwarding allowance is a linear interpolation between
    /// [MIN_OUTGOING_GAS](CongestionControlConfig::min_outgoing_gas) and
    /// [MAX_OUTGOING_GAS](CongestionControlConfig::max_outgoing_gas), or 0 if the receiver is
    /// fully congested.
    pub min_outgoing_gas: Gas,

    /// How much gas the chosen allowed shard can send to a 100% congested shard.
    ///
    /// This amount is the absolute minimum of new workload a congested shard has to
    /// accept every round. It ensures deadlocks are provably impossible. But in
    /// ideal conditions, the gradual reduction of new workload entering the system
    /// combined with gradually limited forwarding to congested shards should
    /// prevent shards from becoming 100% congested in the first place.
    pub allowed_shard_outgoing_gas: Gas,

    /// The maximum amount of gas in a chunk spent on converting new transactions to
    /// receipts.
    ///
    /// The actual gas forwarding allowance is a linear interpolation between
    /// [MIN_OUTGOING_GAS](CongestionControlConfig::min_outgoing_gas) and
    /// [MAX_OUTGOING_GAS](CongestionControlConfig::max_outgoing_gas),
    /// based on the incoming congestion of the local shard.
    /// Additionally, transactions can be rejected if the receiving
    /// remote shard is congested more than
    /// [REJECT_TX_CONGESTION_THRESHOLD](CongestionControlConfig::reject_tx_congestion_threshold)
    /// based on their general congestion level.
    pub max_tx_gas: Gas,

    /// The minimum amount of gas in a chunk spent on converting new transactions
    /// to receipts, as long as the receiving shard is not congested.
    ///
    /// The actual gas forwarding allowance is a linear interpolation between
    /// [MIN_OUTGOING_GAS](CongestionControlConfig::min_outgoing_gas) and
    /// [MAX_OUTGOING_GAS](CongestionControlConfig::max_outgoing_gas),
    /// based on the incoming congestion of the local shard.
    /// Additionally, transactions can be rejected if the receiving
    /// remote shard is congested more than
    /// [REJECT_TX_CONGESTION_THRESHOLD](CongestionControlConfig::reject_tx_congestion_threshold)
    /// based on their general congestion level.
    pub min_tx_gas: Gas,

    /// How much congestion a shard can tolerate before it stops all shards from
    /// accepting new transactions with the receiver set to the congested shard.
    pub reject_tx_congestion_threshold: f64,
}

// The Eq cannot be automatically derived for this class because it contains a
// f64 field. The f64 type does not implement the Eq trait because it's possible
// that NaN != NaN. In the CongestionControlConfig the field should never be NaN
// so it is okay for us to add the Eq trait to it manually.
impl Eq for CongestionControlConfig {}

impl CongestionControlConfig {
    /// Creates a config where congestion control is disabled. This config can
    /// be used for tests. It can be useful e.g. in tests with missing chunks
    /// where we still want to process all transactions.
    pub fn test_disabled() -> Self {
        let max_value = u64::MAX;
        Self {
            max_congestion_incoming_gas: Gas::from_gas(max_value),
            max_congestion_outgoing_gas: Gas::from_gas(max_value),
            max_congestion_memory_consumption: max_value,
            max_congestion_missed_chunks: max_value,
            max_outgoing_gas: Gas::from_gas(max_value),
            min_outgoing_gas: Gas::from_gas(max_value),
            allowed_shard_outgoing_gas: Gas::from_gas(max_value),
            max_tx_gas: Gas::from_gas(max_value),
            min_tx_gas: Gas::from_gas(max_value),
            reject_tx_congestion_threshold: 2.0,
        }
    }

    /// The congestion control parameters as originally stabilized at protocol
    /// version 68 (NEP-539). The live parameters have been adjusted since, and
    /// tests calibrated to the original values pin them here so they don't need
    /// adjusting every time the live parameters change.
    // TODO(limited_replayability): Move tests to use config from latest protocol version.
    pub fn test_original() -> Self {
        Self {
            // 20 PGas
            max_congestion_incoming_gas: Gas::from_gas(20_000_000_000_000_000),
            // 10 PGas
            max_congestion_outgoing_gas: Gas::from_gas(10_000_000_000_000_000),
            // 1000 MB
            max_congestion_memory_consumption: 1_000_000_000,
            max_congestion_missed_chunks: 5,
            // 300 PGas
            max_outgoing_gas: Gas::from_gas(300_000_000_000_000_000),
            // 1 PGas
            min_outgoing_gas: Gas::from_gas(1_000_000_000_000_000),
            // 1 PGas
            allowed_shard_outgoing_gas: Gas::from_gas(1_000_000_000_000_000),
            // 500 TGas
            max_tx_gas: Gas::from_gas(500_000_000_000_000),
            // 20 TGas
            min_tx_gas: Gas::from_gas(20_000_000_000_000),
            reject_tx_congestion_threshold: 0.5,
        }
    }
}

/// Configuration specific to ChunkStateWitness.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct WitnessConfig {
    /// Size limit for storage proof generated while executing receipts in a chunk.
    /// After this limit is reached we defer execution of any new receipts.
    pub main_storage_proof_size_soft_limit: u64,
    /// Maximum size of transactions contained inside ChunkStateWitness.
    ///
    /// A witness contains transactions from both the previous chunk and the current one.
    /// This parameter limits the sum of sizes of transactions from both of those chunks.
    pub combined_transactions_size_limit: usize,
    /// Size limit of storage proof used to validate new transactions in ChunkStateWitness.
    pub new_transactions_validation_state_size_soft_limit: u64,
}

impl WitnessConfig {
    /// Creates a config that effectively disables ChunkStateWitness related limits by setting them
    /// to max values. This can be useful for tests and benchmarks.
    pub fn test_disabled() -> Self {
        Self {
            main_storage_proof_size_soft_limit: u64::MAX,
            combined_transactions_size_limit: usize::MAX,
            new_transactions_validation_state_size_soft_limit: u64::MAX,
        }
    }
}

/// Limits on external contract calls, per call and per chunk. The gas limit of a call,
/// `max_gas_burnt_external`, is in `LimitConfig`, since the VM enforces it.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct ExternalCallsConfig {
    /// Max size of an external contract call, in bytes. A call over it is invalid.
    pub max_call_size: u64,
    /// Max storage proof an external call's run may record, in bytes. A run is stopped at the
    /// trie node that crosses it, and a value that would cross it is not fetched.
    pub max_call_recorded_size: usize,
    /// The share of the chunk's compute limit that its external calls may use.
    pub chunk_compute_share: Rational32,
    /// The share of `main_storage_proof_size_soft_limit` that a chunk's external calls may
    /// record, the reads of their contract accounts included.
    pub chunk_storage_proof_share: Rational32,
    /// The share of `combined_transactions_size_limit` that a chunk's external calls may take.
    /// A chunk whose external calls take more is invalid.
    pub chunk_transactions_size_share: Rational32,
    /// Max number of external calls in a chunk. A chunk with more is invalid.
    pub max_calls_per_chunk: u64,
}

impl ExternalCallsConfig {
    /// The compute that a chunk's external calls may use, out of the chunk's compute limit,
    /// which is its gas limit.
    pub fn compute_share(&self, chunk_gas_limit: Gas) -> Compute {
        share_of(self.chunk_compute_share, chunk_gas_limit.as_gas())
    }

    /// The storage proof that a chunk's external calls may record, in bytes.
    pub fn storage_proof_share(&self, witness_config: &WitnessConfig) -> u64 {
        share_of(self.chunk_storage_proof_share, witness_config.main_storage_proof_size_soft_limit)
    }

    /// The bytes that a chunk's external calls may take together.
    pub fn transactions_size_share(&self, witness_config: &WitnessConfig) -> u64 {
        let limit =
            u64::try_from(witness_config.combined_transactions_size_limit).unwrap_or(u64::MAX);
        share_of(self.chunk_transactions_size_share, limit)
    }
}

/// `share` of `amount`, rounded down. A share that is not positive is zero, and the result
/// saturates at `u64::MAX`.
fn share_of(share: Rational32, amount: u64) -> u64 {
    let (Ok(numerator), Ok(denominator)) =
        (u128::try_from(*share.numer()), u128::try_from(*share.denom()))
    else {
        return 0;
    };
    let share = u128::from(amount) * numerator / denominator;
    u64::try_from(share).unwrap_or(u64::MAX)
}

/// Configuration specific to BandwidthScheduler
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct BandwidthSchedulerConfig {
    /// The maximum amount of data that a shard can send or receive at a single height.
    pub max_shard_bandwidth: u64,
    /// The maximum amount of bandwidth that can be granted on a single link.
    /// Should be at least as big as `max_receipt_size`.
    pub max_single_grant: u64,
    /// Maximum bandwidth allowance that a link can accumulate.
    pub max_allowance: u64,
    /// Max value of `base_bandwidth` that is granted on all links by default.
    pub max_base_bandwidth: u64,
}

impl BandwidthSchedulerConfig {
    /// Creates a config that effectively disables BandwidthScheduler related limits by setting them
    /// to max values. This can be useful for tests and benchmarks.
    pub fn test_disabled() -> Self {
        // Using `u64::MAX` would lead to overflows as these numbers are used in additions and
        // multiplications. The values below are still high enough to effectively disable limits.
        let one_tb = 1_000_000_000_000;
        Self {
            max_shard_bandwidth: 100 * one_tb,
            max_single_grant: one_tb,
            max_allowance: one_tb,
            max_base_bandwidth: one_tb,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost::{ActionCosts, ExtCosts};
    use crate::{ParameterCost, RuntimeConfigStore};
    use near_primitives_core::chains::MAINNET;
    use near_primitives_core::version::ProtocolFeature;

    #[test]
    fn test_share_of() {
        assert_eq!(share_of(Rational32::new(2, 5), 1000), 400);
        assert_eq!(share_of(Rational32::new(1, 40), 4_194_304), 104_857);
        assert_eq!(share_of(Rational32::new(0, 1), 1000), 0);
        assert_eq!(share_of(Rational32::new(-1, 2), 1000), 0);
        assert_eq!(share_of(Rational32::new(1, 20), u64::MAX), u64::MAX / 20);
        assert_eq!(share_of(Rational32::new(3, 1), u64::MAX), u64::MAX);
    }

    /// Each share of a chunk's limits that its external calls get fits five calls that use as much
    /// of it as a call can, and so does the limit on their number, at every protocol version with
    /// external calls. Five worst-case calls
    /// are about as many full runs as a chunk producer makes in its time for external calls.
    #[test]
    fn test_external_calls_shares_fit_five_worst_case_calls() {
        const NUM_CALLS: u64 = 5;
        // The gas limit of a chunk at mainnet genesis, which the compute limit equals.
        let chunk_gas_limit = Gas::from_teragas(1000);
        let store = RuntimeConfigStore::for_chain_id(MAINNET);
        let feature_version = ProtocolFeature::ExternalContractCalls.protocol_version();
        for version in feature_version..=feature_version.max(PROTOCOL_VERSION) {
            let config = store.get_config(version);
            let calls = &config.external_calls_config;
            let witness = &config.witness_config;

            let compute = worst_case_compute(config);
            let compute_share = calls.compute_share(chunk_gas_limit);
            assert!(
                NUM_CALLS * compute <= compute_share,
                "{NUM_CALLS} calls of {compute} compute don't fit in {compute_share} at protocol version {version}"
            );

            // A run stops at the trie node that crosses its cap. The share leaves room for that
            // node and for each call's reads before the run, which the cap doesn't cover.
            let recorded = u64::try_from(calls.max_call_recorded_size).unwrap();
            let storage_proof_share = calls.storage_proof_share(witness);
            assert!(
                NUM_CALLS * recorded <= storage_proof_share,
                "{NUM_CALLS} calls recording {recorded} bytes don't fit in {storage_proof_share} at protocol version {version}"
            );

            let size = calls.max_call_size;
            let size_share = calls.transactions_size_share(witness);
            assert!(
                NUM_CALLS * size <= size_share,
                "{NUM_CALLS} calls of {size} bytes don't fit in {size_share} at protocol version {version}"
            );

            let max_calls = calls.max_calls_per_chunk;
            assert!(
                NUM_CALLS <= max_calls,
                "{NUM_CALLS} calls don't fit in the limit of {max_calls} at protocol version {version}"
            );
        }
    }

    /// The most compute an external call can use: its whole gas budget, fees included, spent on
    /// whichever host operation or fee has the highest compute for its gas.
    fn worst_case_compute(config: &RuntimeConfig) -> Compute {
        let ext_costs = &config.wasm_config.ext_costs.costs;
        // The large read overheads cost 1 gas, and are only charged along with a storage read.
        // So they count as part of the read, or their ratio would swamp all the others.
        let with_overhead = |cost: ExtCosts, overhead: ExtCosts| ParameterCost {
            gas: ext_costs[cost].gas.checked_add(ext_costs[overhead].gas).unwrap(),
            compute: ext_costs[cost].compute + ext_costs[overhead].compute,
        };
        let host_operations = ext_costs.iter().filter_map(|(cost, parameter_cost)| match cost {
            ExtCosts::storage_large_read_overhead_base
            | ExtCosts::storage_large_read_overhead_byte => None,
            ExtCosts::storage_read_base => {
                Some(with_overhead(cost, ExtCosts::storage_large_read_overhead_base))
            }
            ExtCosts::storage_read_value_byte => {
                Some(with_overhead(cost, ExtCosts::storage_large_read_overhead_byte))
            }
            _ => Some(*parameter_cost),
        });
        // The fees of a call: a self-addressed function call, sent and executed at once.
        let fees = [
            ActionCosts::new_action_receipt,
            ActionCosts::function_call_base,
            ActionCosts::function_call_byte,
        ]
        .into_iter()
        .flat_map(|cost| {
            let fee = config.fees.fee(cost);
            [fee.send_fee(true), fee.exec_fee()]
        });
        // Compared as `compute / gas` without dividing: a / b > c / d when a * d > c * b.
        let (compute, gas) = host_operations
            .chain(fees)
            .filter(|cost| cost.gas > Gas::ZERO)
            .map(|cost| (u128::from(cost.compute), u128::from(cost.gas.as_gas())))
            .max_by(|(compute_a, gas_a), (compute_b, gas_b)| {
                (compute_a * gas_b).cmp(&(compute_b * gas_a))
            })
            .unwrap();
        let budget = u128::from(config.wasm_config.limit_config.max_gas_burnt_external.as_gas());
        Compute::try_from(budget * compute / gas).unwrap()
    }
}

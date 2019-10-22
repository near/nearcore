use crate::cases::Metric;
use crate::stats::{DataStats, Measurements};
use near_runtime_fees::{
    AccessKeyCreationConfig, ActionCreationConfig, DataReceiptCreationConfig, ExtCostsConfig, Fee,
    Fraction, RuntimeFeesConfig, StorageUsageConfig,
};
use std::collections::{BTreeMap, HashMap};

/// We need to convert float fees to gas values, so we need to multiply it by some value and
/// then round it up. We can either: a) figure out what value to multiply by dynamically, based on
/// on some criteria, e.g. making the lowest fee equal to 1 gas; b) or we can multiply by a
/// fixed value. The disadvantage of (a) is that if suddenly our code becomes 2X slower the
/// fees expressed in gas will not change. So we go with (b).
/// Since some operations take on the order of 1-10 picoseconds per byte we multiply by 10^9.
const GAS_PER_MICROSEC: u64 = 1_000_000_000;

/// Given the time it takes per receipt/action in microseconds, returns the fee in gas.
fn receipt_base_fee(micros: f64) -> Fee {}

/// Given the time it takes per byte of receipt/action in microseconds, returns the fee in gas.
fn receipt_per_byte_fee(micros: f64) -> Fee {}

pub struct ConfigGenerator {
    measurement: Measurements,
    aggregated: BTreeMap<Metric, DataStats>,
}

/// Fees for receipts and actions measured in floats.
#[derive(Clone, Copy, Hash, PartialEq, Eq)]
enum ReceiptFeesFloat {
    ActionReceiptCreation,
    DataReceiptCreationBase,
    DataReceiptCreationPerByte,
    ActionCreateAccount,
    ActionDeployContractBase,
    ActionDeployContractPerByte,
    ActionFunctionCallBase,
    ActionFunctionCallPerByte,
    ActionTransfer,
    ActionStake,
    ActionAddFullAccessKey,
    ActionAddFunctionAccessKeyBase,
    ActionAddFunctionAccessKeyPerByte,
    ActionDeleteKey,
    ActionDeleteAccount,
}

impl ConfigGenerator {
    pub fn new(measurement: Measurements) -> Self {
        let aggregated = measurement.aggregate();
        Self { measurement, aggregated }
    }

    /// Compute fees for receipts and actions in microseconds as floats.
    fn receipt_fees_in_micros(&self) -> HashMap<ReceiptFeesFloat, f64> {
        let mut res = HashMap::new();
        res.insert(
            ReceiptFeesFloat::ActionReceiptCreation,
            self.aggregated[Metric::Receipt].sigma_micros() as f64,
        );
        res.insert(
            ReceiptFeesFloat::DataReceiptCreation,
            self.aggregated[Metric::CallPromiseBatchCreateThen10K].sigma_micros()
                - self.aggregated[Metric::CallPromiseBatchCreate10K].sigma_micros() as f64,
        );
        res.insert(
            ReceiptFeesFloat::DataReceiptPerByte,
            // TODO: Currently we set it to 0, because we need to figure out how to compute it.
            0.0,
        );
        res.insert(
            ReceiptFeesFloat::ActionCreateAccount,
            self.aggregated[Metric::ActionCreateAccount].sigma_micros()
                - self.aggregated[Metric::Receipt].sigma_micros() as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionDeployContractBase,
            // TODO: This is a base cost, so we should not be charging for bytes here.
            self.aggregated[Metric::ActionDeploy10K].sigma_micros()
                - self.aggregated[Metric::Receipt].sigma_micros() as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionDeployContractPerByte,
            (self.aggregated[Metric::ActionDeploy1M].sigma_micros()
                - self.aggregated[Metric::ActionDeploy100K].sigma_micros() as f64)
                / (1024 * 1024 - 100 * 1024),
        );
        res.insert(
            ReceiptFeesFloat::ActionFunctionCallBase,
            self.aggregated[Metric::CallNoop].sigma_micros()
                - self.aggregated[Metric::Receipt].sigma_micros() as f64,
        );
        res.insert(
            ReceiptFeesFloat::ActionFunctionCallPerByte,
            /// TODO: Currently we set it to 0, because we need to figure out how to compute it.
            0.0,
        );
        res.insert(
            ReceiptFeesFloat::ActionTransfer,
            self.aggregated[Metric::ActionTransfer].sigma_micros()
                - self.aggregated[Metric::ActionReceiptCreation].sigma_micros() as f64,
        );

        //                transfer_cost: Fee { send_sir: 10, send_not_sir: 10, execution: 10 },
        //                stake_cost: Fee { send_sir: 10, send_not_sir: 10, execution: 10 },
        //                add_key_cost: AccessKeyCreationConfig {
        //                    full_access_cost: Fee { send_sir: 10, send_not_sir: 10, execution: 10 },
        //                    function_call_cost: Fee { send_sir: 10, send_not_sir: 10, execution: 10 },
        //                    function_call_cost_per_byte: Fee {
        //                        send_sir: 10,
        //                        send_not_sir: 10,
        //                        execution: 10,
        //                    },
        //                },
        //                delete_key_cost: Fee { send_sir: 10, send_not_sir: 10, execution: 10 },
        //                delete_account_cost: Fee { send_sir: 10, send_not_sir: 10, execution: 10 },
        //            },
        //            storage_usage_config: StorageUsageConfig {
        //                account_cost: 100,
        //                data_record_cost: 40,
        //                key_cost_per_byte: 1,
        //                value_cost_per_byte: 1,
        //                code_cost_per_byte: 1,
        //            },
        //            burnt_gas_reward: Fraction { numerator: 3, denominator: 10 },
        //        }
        res
    }
}

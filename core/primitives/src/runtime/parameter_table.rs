use near_primitives_core::parameter::{FeeParameter, Parameter};
use serde_json::json;
use std::collections::BTreeMap;
use thiserror::Error;

pub(crate) struct ParameterTable {
    params: BTreeMap<Parameter, serde_json::Value>,
}

/// Error returned by ParameterTable::from_txt() that parses a runtime
/// configuration TXT file.
#[derive(Error, Debug)]
pub(crate) enum InvalidConfigError {
    #[error("could not parse `{1}` as a parameter")]
    UnknownParameter(#[source] strum::ParseError, String),
    #[error("Unable to parse value: `{1}`")]
    ValueParseError(#[source] serde_json::Error, String),
    #[error("Parameter name and value must be separated by a `:`")]
    NoSeparator,
    #[error("Intermediate JSON created by parser does not match `RuntimeConfig`")]
    WrongStructure(#[source] serde_json::Error),
}

impl ParameterTable {
    pub(crate) fn from_txt(arg: &str) -> Result<ParameterTable, InvalidConfigError> {
        let parameters = arg
            .lines()
            .filter_map(|line| {
                // ignore comments and empty lines
                let trimmed = line.trim().to_owned();
                if trimmed.starts_with("#") || trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed)
                }
            })
            .map(|trimmed| {
                let (key, value) =
                    trimmed.split_once(":").ok_or(InvalidConfigError::NoSeparator)?;
                let typed_key: Parameter = key
                    .trim()
                    .parse()
                    .map_err(|err| InvalidConfigError::UnknownParameter(err, key.to_owned()))?;
                Ok((typed_key, parse_parameter_txt_value(value.trim())?))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ParameterTable { params: BTreeMap::from_iter(parameters) })
    }

    /// Transforms parameters stored in the table into a JSON representation of `RuntimeConfig`.
    pub(crate) fn runtime_config_json(&self) -> serde_json::Value {
        let storage_amount_per_byte = self.get(Parameter::StorageAmountPerByte);
        let transaction_costs = self.transaction_costs_json();
        json!({
            "storage_amount_per_byte": storage_amount_per_byte,
            "transaction_costs": transaction_costs,
            "wasm_config": {
                "ext_costs": self.json_map(Parameter::ext_costs()),
                "grow_mem_cost": self.get(Parameter::WasmGrowMemCost),
                "regular_op_cost": self.get(Parameter::WasmRegularOpCost),
                "limit_config": self.json_map(Parameter::vm_limits()),
            },
            "account_creation_config": {
                "min_allowed_top_level_account_length": self.get(Parameter::MinAllowedTopLevelAccountLength),
                "registrar_account_id": self.get(Parameter::RegistrarAccountId),
            }
        })
    }

    pub(crate) fn apply_diff(&mut self, diff: ParameterTable) {
        self.params.extend(diff.params)
    }

    fn transaction_costs_json(&self) -> serde_json::Value {
        json!( {
            "action_receipt_creation_config": self.fee_json(FeeParameter::ActionReceiptCreation),
            "data_receipt_creation_config": {
                "base_cost": self.fee_json(FeeParameter::DataReceiptCreationBase),
                "cost_per_byte": self.fee_json(FeeParameter::DataReceiptCreationPerByte),
            },
            "action_creation_config": {
                "create_account_cost": self.fee_json(FeeParameter::ActionCreateAccount),
                "deploy_contract_cost": self.fee_json(FeeParameter::ActionDeployContract),
                "deploy_contract_cost_per_byte": self.fee_json(FeeParameter::ActionDeployContractPerByte),
                "function_call_cost": self.fee_json(FeeParameter::ActionFunctionCall),
                "function_call_cost_per_byte": self.fee_json(FeeParameter::ActionFunctionCallPerByte),
                "transfer_cost": self.fee_json(FeeParameter::ActionTransfer),
                "stake_cost": self.fee_json(FeeParameter::ActionStake),
                "add_key_cost": {
                    "full_access_cost": self.fee_json(FeeParameter::ActionAddFullAccessKey),
                    "function_call_cost": self.fee_json(FeeParameter::ActionAddFunctionCallKey),
                    "function_call_cost_per_byte": self.fee_json(FeeParameter::ActionAddFunctionCallKeyPerByte),
                },
                "delete_key_cost": self.fee_json(FeeParameter::ActionDeleteKey),
                "delete_account_cost": self.fee_json(FeeParameter::ActionDeleteAccount),
            },
            "storage_usage_config": {
                "num_bytes_account": self.get(Parameter::StorageNumBytesAccount),
                "num_extra_bytes_record": self.get(Parameter::StorageNumExtraBytesRecord),
            },
            "burnt_gas_reward": [
                self.get(Parameter::BurntGasRewardNumerator),
                self.get(Parameter::BurntGasRewardDenominator)
            ],
            "pessimistic_gas_price_inflation_ratio": [
                self.get(Parameter::PessimisticGasPriceInflationNumerator),
                self.get(Parameter::PessimisticGasPriceInflationDenominator)
            ]
        })
    }

    fn json_map(&self, params: impl Iterator<Item = &'static Parameter>) -> serde_json::Value {
        let mut json = serde_json::Map::new();
        for param in params {
            json.insert(param.to_string(), self.get(*param).clone());
        }
        json.into()
    }

    fn get(&self, key: Parameter) -> &serde_json::Value {
        self.params.get(&key).unwrap_or(&serde_json::Value::Null)
    }

    fn fee_json(&self, key: FeeParameter) -> serde_json::Value {
        json!( {
            "send_sir": self.get(format!("{key}_send_sir").parse().unwrap()),
            "send_not_sir": self.get(format!("{key}_send_not_sir").parse().unwrap()),
            "execution": self.get(format!("{key}_execution").parse().unwrap()),
        })
    }
}

/// Parses a value from the custom format for runtime parameter definitions.
///
/// A value can be a positive integer or a string, both written without quotes.
/// Integers can use underlines as separators (for readability).
fn parse_parameter_txt_value(value: &str) -> Result<serde_json::Value, InvalidConfigError> {
    if value.is_empty() {
        return Ok(serde_json::Value::Null);
    }
    if value.chars().all(|c| c.is_numeric() || c == '_') {
        Ok(serde_json::Value::Number(
            value
                .chars()
                .filter(|c| c.is_numeric())
                .collect::<String>()
                .parse()
                .map_err(|err| InvalidConfigError::ValueParseError(err, value.to_owned()))?,
        ))
    } else {
        Ok(serde_json::Value::String(value.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::{InvalidConfigError, ParameterTable};
    use assert_matches::assert_matches;
    use near_primitives_core::parameter::Parameter;
    use std::collections::BTreeMap;

    #[track_caller]
    fn check_parameter_table(
        base_config: &str,
        diffs: &[&str],
        expected: impl IntoIterator<Item = (Parameter, &'static str)>,
    ) {
        let mut params = ParameterTable::from_txt(base_config).unwrap();
        for diff in diffs {
            let diff = ParameterTable::from_txt(diff).unwrap();
            params.apply_diff(diff);
        }

        let expected_map = BTreeMap::from_iter(expected.into_iter().map(|(param, value)| {
            (
                param,
                if value.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::from_str(value).expect("Test data has invalid JSON")
                },
            )
        }));

        assert_eq!(params.params, expected_map);
    }

    #[track_caller]
    fn check_invalid_parameter_table(base_config: &str, diffs: &[&str]) -> InvalidConfigError {
        let params = ParameterTable::from_txt(base_config);

        let result = params.and_then(|params| {
            diffs.iter().try_fold(params, |mut params, diff| {
                params.apply_diff(ParameterTable::from_txt(diff)?);
                Ok(params)
            })
        });

        match result {
            Ok(_) => panic!("Input should have parser error"),
            Err(err) => err,
        }
    }

    static BASE_0: &str = r#"
# Comment line
registrar_account_id: registrar
min_allowed_top_level_account_length: 32
storage_amount_per_byte: 100_000_000_000_000_000_000
storage_num_bytes_account: 100
storage_num_extra_bytes_record: 40
"#;

    static BASE_1: &str = r#"
registrar_account_id: registrar
# Comment line
min_allowed_top_level_account_length: 32

# Comment line with trailing whitespace # 

storage_amount_per_byte: 100000000000000000000
storage_num_bytes_account: 100
storage_num_extra_bytes_record   :   40  

"#;

    static DIFF_0: &str = r#"
# Comment line
registrar_account_id: near
min_allowed_top_level_account_length: 32000
wasm_regular_op_cost: 3856371
"#;

    static DIFF_1: &str = r#"
# Comment line
registrar_account_id: near
storage_num_extra_bytes_record: 77
wasm_regular_op_cost: 0
max_memory_pages: 512
"#;

    // Tests synthetic small example configurations. For tests with "real"
    // input data, we already have
    // `test_old_and_new_runtime_config_format_match` in `configs_store.rs`.

    /// Check empty input
    #[test]
    fn test_empty_parameter_table() {
        check_parameter_table("", &[], []);
    }

    /// Reading reading a normally formatted base parameter file with no diffs
    #[test]
    fn test_basic_parameter_table() {
        check_parameter_table(
            BASE_0,
            &[],
            [
                (Parameter::RegistrarAccountId, "\"registrar\""),
                (Parameter::MinAllowedTopLevelAccountLength, "32"),
                (Parameter::StorageAmountPerByte, "100000000000000000000"),
                (Parameter::StorageNumBytesAccount, "100"),
                (Parameter::StorageNumExtraBytesRecord, "40"),
            ],
        );
    }

    /// Reading reading a slightly funky formatted base parameter file with no diffs
    #[test]
    fn test_basic_parameter_table_weird_syntax() {
        check_parameter_table(
            BASE_1,
            &[],
            [
                (Parameter::RegistrarAccountId, "\"registrar\""),
                (Parameter::MinAllowedTopLevelAccountLength, "32"),
                (Parameter::StorageAmountPerByte, "100000000000000000000"),
                (Parameter::StorageNumBytesAccount, "100"),
                (Parameter::StorageNumExtraBytesRecord, "40"),
            ],
        );
    }

    /// Apply one diff
    #[test]
    fn test_parameter_table_with_diff() {
        check_parameter_table(
            BASE_0,
            &[DIFF_0],
            [
                (Parameter::RegistrarAccountId, "\"near\""),
                (Parameter::MinAllowedTopLevelAccountLength, "32000"),
                (Parameter::StorageAmountPerByte, "100000000000000000000"),
                (Parameter::StorageNumBytesAccount, "100"),
                (Parameter::StorageNumExtraBytesRecord, "40"),
                (Parameter::WasmRegularOpCost, "3856371"),
            ],
        );
    }

    /// Apply two diffs
    #[test]
    fn test_parameter_table_with_diffs() {
        check_parameter_table(
            BASE_0,
            &[DIFF_0, DIFF_1],
            [
                (Parameter::RegistrarAccountId, "\"near\""),
                (Parameter::MinAllowedTopLevelAccountLength, "32000"),
                (Parameter::StorageAmountPerByte, "100000000000000000000"),
                (Parameter::StorageNumBytesAccount, "100"),
                (Parameter::StorageNumExtraBytesRecord, "77"),
                (Parameter::WasmRegularOpCost, "0"),
                (Parameter::MaxMemoryPages, "512"),
            ],
        );
    }

    /// Providing no value is also legal, the code that uses the parameter to
    /// figure out how to interpret it. For example, it could mean this
    /// parameter no longer exists.
    #[test]
    fn test_parameter_table_with_empty_value() {
        let diff_with_empty_value = "min_allowed_top_level_account_length:";
        check_parameter_table(
            BASE_0,
            &[diff_with_empty_value],
            [
                (Parameter::RegistrarAccountId, "\"registrar\""),
                (Parameter::MinAllowedTopLevelAccountLength, ""),
                (Parameter::StorageAmountPerByte, "100000000000000000000"),
                (Parameter::StorageNumBytesAccount, "100"),
                (Parameter::StorageNumExtraBytesRecord, "40"),
            ],
        );
    }

    #[test]
    fn test_parameter_table_invalid_key() {
        // Key that is not a `Parameter`
        assert_matches!(
            check_invalid_parameter_table("invalid_key: 100", &[]),
            InvalidConfigError::UnknownParameter(_, _)
        );
    }

    #[test]
    fn test_parameter_table_invalid_key_in_diff() {
        assert_matches!(
            check_invalid_parameter_table("wasm_regular_op_cost: 100", &["invalid_key: 100"]),
            InvalidConfigError::UnknownParameter(_, _)
        );
    }

    #[test]
    fn test_parameter_table_no_key() {
        assert_matches!(
            check_invalid_parameter_table(": 100", &[]),
            InvalidConfigError::UnknownParameter(_, _)
        );
    }

    #[test]
    fn test_parameter_table_no_key_in_diff() {
        assert_matches!(
            check_invalid_parameter_table("wasm_regular_op_cost: 100", &[": 100"]),
            InvalidConfigError::UnknownParameter(_, _)
        );
    }

    #[test]
    fn test_parameter_table_wrong_separator() {
        assert_matches!(
            check_invalid_parameter_table("wasm_regular_op_cost=100", &[]),
            InvalidConfigError::NoSeparator
        );
    }

    #[test]
    fn test_parameter_table_wrong_separator_in_diff() {
        assert_matches!(
            check_invalid_parameter_table(
                "wasm_regular_op_cost: 100",
                &["wasm_regular_op_cost=100"]
            ),
            InvalidConfigError::NoSeparator
        );
    }
}

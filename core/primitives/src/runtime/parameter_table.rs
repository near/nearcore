use near_primitives_core::parameter::{FeeParameter, Parameter};
use serde_json::json;
use std::collections::BTreeMap;

pub(crate) struct ParameterTable {
    parameters: BTreeMap<Parameter, serde_json::Value>,
}

/// Changes made to parameters between versions.
pub(crate) struct ParameterTableDiff {
    parameters: BTreeMap<Parameter, (serde_json::Value, serde_json::Value)>,
}

/// Error returned by ParameterTable::from_txt() that parses a runtime
/// configuration TXT file.
#[derive(thiserror::Error, Debug)]
pub(crate) enum InvalidConfigError {
    #[error("could not parse `{1}` as a parameter")]
    UnknownParameter(#[source] strum::ParseError, String),
    #[error("could not parse `{1}` as a value")]
    ValueParseError(#[source] serde_json::Error, String),
    #[error("expected a `:` separator between name and value of a parameter `{1}` on line {0}")]
    NoSeparator(usize, String),
    #[error("intermediate JSON created by parser does not match `RuntimeConfig`")]
    WrongStructure(#[source] serde_json::Error),
    #[error("config diff expected to contain old value `{1}` for parameter `{0}`")]
    OldValueExists(Parameter, String),
    #[error(
        "unexpected old value `{1}` for parameter `{0}` in config diff, previous version does not have such a value"
    )]
    NoOldValueExists(Parameter, String),
    #[error("expected old value `{1}` but found `{2}` for parameter `{0}` in config diff")]
    WrongOldValue(Parameter, String, String),
}

impl std::str::FromStr for ParameterTable {
    type Err = InvalidConfigError;
    fn from_str(arg: &str) -> Result<ParameterTable, InvalidConfigError> {
        let parameters = txt_to_key_values(arg)
            .map(|result| {
                let (typed_key, value) = result?;
                Ok((typed_key, parse_parameter_txt_value(value.trim())?))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        Ok(ParameterTable { parameters })
    }
}

impl ParameterTable {
    /// Transforms parameters stored in the table into a JSON representation of `RuntimeConfig`.
    pub(crate) fn runtime_config_json(&self) -> serde_json::Value {
        let storage_amount_per_byte = self.get(Parameter::StorageAmountPerByte);
        let transaction_costs = self.transaction_costs_json();
        json!({
            "storage_amount_per_byte": storage_amount_per_byte,
            "transaction_costs": transaction_costs,
            "wasm_config": {
                "ext_costs": self.json_map(Parameter::ext_costs(), "wasm_"),
                "grow_mem_cost": self.get(Parameter::WasmGrowMemCost),
                "regular_op_cost": self.get(Parameter::WasmRegularOpCost),
                "limit_config": self.json_map(Parameter::vm_limits(), ""),
            },
            "account_creation_config": {
                "min_allowed_top_level_account_length": self.get(Parameter::MinAllowedTopLevelAccountLength),
                "registrar_account_id": self.get(Parameter::RegistrarAccountId),
            }
        })
    }

    pub(crate) fn apply_diff(
        &mut self,
        diff: ParameterTableDiff,
    ) -> Result<(), InvalidConfigError> {
        for (key, (before, after)) in diff.parameters {
            if before.is_null() {
                match self.parameters.get(&key) {
                    Some(serde_json::Value::Null) | None => {
                        self.parameters.insert(key, after);
                    }
                    Some(old_value) => {
                        return Err(InvalidConfigError::OldValueExists(key, old_value.to_string()))
                    }
                }
            } else {
                match self.parameters.get(&key) {
                    Some(serde_json::Value::Null) | None => {
                        return Err(InvalidConfigError::NoOldValueExists(key, before.to_string()))
                    }
                    Some(old_value) => {
                        if *old_value != before {
                            return Err(InvalidConfigError::WrongOldValue(
                                key,
                                old_value.to_string(),
                                before.to_string(),
                            ));
                        } else {
                            self.parameters.insert(key, after);
                        }
                    }
                }
            }
        }
        Ok(())
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

    fn json_map(
        &self,
        params: impl Iterator<Item = &'static Parameter>,
        remove_prefix: &'static str,
    ) -> serde_json::Value {
        let mut json = serde_json::Map::new();
        for param in params {
            let mut key: &'static str = param.into();
            key = key.strip_prefix(remove_prefix).unwrap_or(key);
            if let Some(value) = self.get(*param) {
                json.insert(key.to_owned(), value.clone());
            }
        }
        json.into()
    }

    fn get(&self, key: Parameter) -> Option<&serde_json::Value> {
        self.parameters.get(&key)
    }

    fn fee_json(&self, key: FeeParameter) -> serde_json::Value {
        json!( {
            "send_sir": self.get(format!("{key}_send_sir").parse().unwrap()),
            "send_not_sir": self.get(format!("{key}_send_not_sir").parse().unwrap()),
            "execution": self.get(format!("{key}_execution").parse().unwrap()),
        })
    }
}

impl std::str::FromStr for ParameterTableDiff {
    type Err = InvalidConfigError;
    fn from_str(arg: &str) -> Result<ParameterTableDiff, InvalidConfigError> {
        let parameters = txt_to_key_values(arg)
            .map(|result| {
                let (typed_key, value) = result?;
                if let Some((before, after)) = value.split_once("->") {
                    Ok((
                        typed_key,
                        (
                            parse_parameter_txt_value(before.trim())?,
                            parse_parameter_txt_value(after.trim())?,
                        ),
                    ))
                } else {
                    Ok((
                        typed_key,
                        (serde_json::Value::Null, parse_parameter_txt_value(value.trim())?),
                    ))
                }
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        Ok(ParameterTableDiff { parameters })
    }
}

fn txt_to_key_values(
    arg: &str,
) -> impl Iterator<Item = Result<(Parameter, &str), InvalidConfigError>> {
    arg.lines()
        .enumerate()
        .filter_map(|(nr, line)| {
            // ignore comments and empty lines
            let trimmed = line.trim();
            if trimmed.starts_with("#") || trimmed.is_empty() {
                None
            } else {
                Some((nr, trimmed))
            }
        })
        .map(|(nr, trimmed)| {
            let (key, value) = trimmed
                .split_once(":")
                .ok_or(InvalidConfigError::NoSeparator(nr + 1, trimmed.to_owned()))?;
            let typed_key: Parameter = key
                .trim()
                .parse()
                .map_err(|err| InvalidConfigError::UnknownParameter(err, key.to_owned()))?;
            Ok((typed_key, value))
        })
}

/// Parses a value from the custom format for runtime parameter definitions.
///
/// A value can be a positive integer or a string, both written without quotes.
/// Integers can use underlines as separators (for readability).
fn parse_parameter_txt_value(value: &str) -> Result<serde_json::Value, InvalidConfigError> {
    if value.is_empty() {
        return Ok(serde_json::Value::Null);
    }
    if value.bytes().all(|c| c.is_ascii_digit() || c == '_' as u8) {
        let mut raw_number = value.to_owned();
        raw_number.retain(char::is_numeric);
        // We do not have "arbitrary_precision" serde feature enabled, thus we
        // can only store up to `u64::MAX`, which is `18446744073709551615` and
        // has 20 characters.
        if raw_number.len() < 20 {
            Ok(serde_json::Value::Number(
                raw_number
                    .parse()
                    .map_err(|err| InvalidConfigError::ValueParseError(err, value.to_owned()))?,
            ))
        } else {
            Ok(serde_json::Value::String(raw_number))
        }
    } else {
        Ok(serde_json::Value::String(value.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::{InvalidConfigError, ParameterTable, ParameterTableDiff};
    use assert_matches::assert_matches;
    use near_primitives_core::parameter::Parameter;
    use std::collections::BTreeMap;

    #[track_caller]
    fn check_parameter_table(
        base_config: &str,
        diffs: &[&str],
        expected: impl IntoIterator<Item = (Parameter, &'static str)>,
    ) {
        let mut params: ParameterTable = base_config.parse().unwrap();
        for diff in diffs {
            let diff: ParameterTableDiff = diff.parse().unwrap();
            params.apply_diff(diff).unwrap();
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

        assert_eq!(params.parameters, expected_map);
    }

    #[track_caller]
    fn check_invalid_parameter_table(base_config: &str, diffs: &[&str]) -> InvalidConfigError {
        let params = base_config.parse();

        let result = params.and_then(|params: ParameterTable| {
            diffs.iter().try_fold(params, |mut params, diff| {
                params.apply_diff(diff.parse()?)?;
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
registrar_account_id: registrar -> near
min_allowed_top_level_account_length: 32 -> 32_000
wasm_regular_op_cost: 3_856_371
"#;

    static DIFF_1: &str = r#"
# Comment line
registrar_account_id: near -> registrar
storage_num_extra_bytes_record: 40 -> 77
wasm_regular_op_cost: 3_856_371 -> 0
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
                (Parameter::StorageAmountPerByte, "\"100000000000000000000\""),
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
                (Parameter::StorageAmountPerByte, "\"100000000000000000000\""),
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
                (Parameter::StorageAmountPerByte, "\"100000000000000000000\""),
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
                (Parameter::RegistrarAccountId, "\"registrar\""),
                (Parameter::MinAllowedTopLevelAccountLength, "32000"),
                (Parameter::StorageAmountPerByte, "\"100000000000000000000\""),
                (Parameter::StorageNumBytesAccount, "100"),
                (Parameter::StorageNumExtraBytesRecord, "77"),
                (Parameter::WasmRegularOpCost, "0"),
                (Parameter::MaxMemoryPages, "512"),
            ],
        );
    }

    #[test]
    fn test_parameter_table_with_empty_value() {
        let diff_with_empty_value = "min_allowed_top_level_account_length: 32 -> ";
        check_parameter_table(
            BASE_0,
            &[diff_with_empty_value],
            [
                (Parameter::RegistrarAccountId, "\"registrar\""),
                (Parameter::MinAllowedTopLevelAccountLength, ""),
                (Parameter::StorageAmountPerByte, "\"100000000000000000000\""),
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
            InvalidConfigError::NoSeparator(1, _)
        );
    }

    #[test]
    fn test_parameter_table_wrong_separator_in_diff() {
        assert_matches!(
            check_invalid_parameter_table(
                "wasm_regular_op_cost: 100",
                &["wasm_regular_op_cost=100"]
            ),
            InvalidConfigError::NoSeparator(1, _)
        );
    }

    #[test]
    fn test_parameter_table_wrong_old_value() {
        assert_matches!(
            check_invalid_parameter_table(
                "min_allowed_top_level_account_length: 3_200_000_000",
                &["min_allowed_top_level_account_length: 3_200_000 -> 1_600_000"]
            ),
            InvalidConfigError::WrongOldValue(
                Parameter::MinAllowedTopLevelAccountLength,
                expected,
                found
            ) => {
                assert_eq!(expected, "3200000000");
                assert_eq!(found, "3200000");
            }
        );
    }

    #[test]
    fn test_parameter_table_no_old_value() {
        assert_matches!(
            check_invalid_parameter_table(
                "min_allowed_top_level_account_length: 3_200_000_000",
                &["min_allowed_top_level_account_length: 1_600_000"]
            ),
            InvalidConfigError::OldValueExists(Parameter::MinAllowedTopLevelAccountLength, expected) => {
                assert_eq!(expected, "3200000000");
            }
        );
    }

    #[test]
    fn test_parameter_table_old_parameter_undefined() {
        assert_matches!(
            check_invalid_parameter_table(
                "min_allowed_top_level_account_length: 3_200_000_000",
                &["wasm_regular_op_cost: 3_200_000 -> 1_600_000"]
            ),
            InvalidConfigError::NoOldValueExists(Parameter::WasmRegularOpCost, found) => {
                assert_eq!(found, "3200000");
            }
        );
    }
}

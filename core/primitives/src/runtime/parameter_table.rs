use super::config::{AccountCreationConfig, RuntimeConfig};
use near_primitives_core::config::{ExtCostsConfig, VMConfig};
use near_primitives_core::parameter::{FeeParameter, Parameter};
use near_primitives_core::runtime::fees::{RuntimeFeesConfig, StorageUsageConfig};
use num_rational::Rational;
use std::collections::BTreeMap;

/// Represents values supported by parameter config.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
#[serde(untagged)]
pub(crate) enum ParameterValue {
    Null,
    U64(u64),
    Rational { numerator: isize, denominator: isize },
    String(String),
}

impl ParameterValue {
    fn is_null(&self) -> bool {
        match self {
            ParameterValue::Null => true,
            _ => false,
        }
    }
}

pub(crate) struct ParameterTable {
    parameters: BTreeMap<Parameter, ParameterValue>,
}

/// Changes made to parameters between versions.
pub(crate) struct ParameterTableDiff {
    parameters: BTreeMap<Parameter, (ParameterValue, ParameterValue)>,
}

/// Error returned by ParameterTable::from_str() that parses a runtime configuration YAML file.
#[derive(thiserror::Error, Debug)]
pub(crate) enum InvalidConfigError {
    #[error("could not parse `{1}` as a parameter")]
    UnknownParameter(#[source] strum::ParseError, String),
    #[error("could not parse `{1}` as a value")]
    ValueParseError(#[source] serde_yaml::Error, String),
    #[error("could not parse YAML that defines the structure of the config")]
    InvalidYaml(#[source] serde_yaml::Error),
    #[error("config diff expected to contain old value `{1:?}` for parameter `{0}`")]
    OldValueExists(Parameter, ParameterValue),
    #[error(
        "unexpected old value `{1:?}` for parameter `{0}` in config diff, previous version does not have such a value"
    )]
    NoOldValueExists(Parameter, ParameterValue),
    #[error("expected old value `{1:?}` but found `{2:?}` for parameter `{0}` in config diff")]
    WrongOldValue(Parameter, ParameterValue, ParameterValue),
    #[error("expected a value for `{0}` but found none")]
    MissingParameter(Parameter),
    #[error("expected a value of type `{1}` for `{0}` but could not parse it from `{2:?}`")]
    WrongValueType(Parameter, &'static str, ParameterValue),
}

impl std::str::FromStr for ParameterTable {
    type Err = InvalidConfigError;
    fn from_str(arg: &str) -> Result<ParameterTable, InvalidConfigError> {
        // TODO(#8320): Remove this after migration to `serde_yaml` 0.9 that supports empty strings.
        if arg.is_empty() {
            return Ok(ParameterTable { parameters: BTreeMap::new() });
        }

        let yaml_map: BTreeMap<String, serde_yaml::Value> =
            serde_yaml::from_str(arg).map_err(|err| InvalidConfigError::InvalidYaml(err))?;

        let parameters = yaml_map
            .iter()
            .map(|(key, value)| {
                let typed_key: Parameter = key
                    .parse()
                    .map_err(|err| InvalidConfigError::UnknownParameter(err, key.to_owned()))?;
                Ok((typed_key, parse_parameter_value(value)?))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;

        Ok(ParameterTable { parameters })
    }
}

impl TryFrom<&ParameterTable> for RuntimeConfig {
    type Error = InvalidConfigError;

    fn try_from(params: &ParameterTable) -> Result<Self, Self::Error> {
        Ok(RuntimeConfig {
            fees: RuntimeFeesConfig {
                action_fees: enum_map::enum_map! {
                    action_cost => params.fee(action_cost)
                },
                burnt_gas_reward: params.get_rational(Parameter::BurntGasReward)?,
                pessimistic_gas_price_inflation_ratio: params
                    .get_rational(Parameter::PessimisticGasPriceInflation)?,
                storage_usage_config: StorageUsageConfig {
                    storage_amount_per_byte: params.get_u128(Parameter::StorageAmountPerByte)?,
                    num_bytes_account: params.get_number(Parameter::StorageNumBytesAccount)?,
                    num_extra_bytes_record: params
                        .get_number(Parameter::StorageNumExtraBytesRecord)?,
                },
            },
            wasm_config: VMConfig {
                ext_costs: ExtCostsConfig {
                    costs: enum_map::enum_map! {
                        cost => params.get_number(cost.param())?
                    },
                },
                grow_mem_cost: params.get_number(Parameter::WasmGrowMemCost)?,
                regular_op_cost: params.get_number(Parameter::WasmRegularOpCost)?,
                limit_config: serde_yaml::from_value(params.yaml_map(Parameter::vm_limits(), ""))
                    .map_err(InvalidConfigError::InvalidYaml)?,
            },
            account_creation_config: AccountCreationConfig {
                min_allowed_top_level_account_length: params
                    .get_number(Parameter::MinAllowedTopLevelAccountLength)?,
                registrar_account_id: params
                    .get_string(Parameter::RegistrarAccountId)?
                    .parse()
                    .map_err(|_| {
                        InvalidConfigError::WrongValueType(
                            Parameter::RegistrarAccountId,
                            std::any::type_name::<near_primitives_core::types::AccountId>(),
                            ParameterValue::Null,
                        )
                    })?,
            },
        })
    }
}

impl ParameterTable {
    pub(crate) fn apply_diff(
        &mut self,
        diff: ParameterTableDiff,
    ) -> Result<(), InvalidConfigError> {
        for (key, (before, after)) in diff.parameters {
            if before.is_null() {
                match self.parameters.get(&key) {
                    Some(ParameterValue::Null) | None => {
                        self.parameters.insert(key, after);
                    }
                    Some(old_value) => {
                        return Err(InvalidConfigError::OldValueExists(key, old_value.clone()))
                    }
                }
            } else {
                match self.parameters.get(&key) {
                    Some(ParameterValue::Null) | None => {
                        return Err(InvalidConfigError::NoOldValueExists(key, before.clone()))
                    }
                    Some(old_value) => {
                        if *old_value != before {
                            return Err(InvalidConfigError::WrongOldValue(
                                key,
                                old_value.clone(),
                                before.clone(),
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

    fn yaml_map(
        &self,
        params: impl Iterator<Item = &'static Parameter>,
        remove_prefix: &'static str,
    ) -> serde_yaml::Value {
        let mut yaml = serde_yaml::Mapping::new();
        for param in params {
            let mut key: &'static str = param.into();
            key = key.strip_prefix(remove_prefix).unwrap_or(key);
            if let Some(value) = self.get(*param) {
                // TODO(akashin): Handle unwrap.
                yaml.insert(key.into(), serde_yaml::to_value(value.clone()).unwrap());
            }
        }
        yaml.into()
    }

    fn get(&self, key: Parameter) -> Option<&ParameterValue> {
        self.parameters.get(&key)
    }

    /// Access action fee by `ActionCosts`.
    fn fee(
        &self,
        cost: near_primitives_core::config::ActionCosts,
    ) -> near_primitives_core::runtime::fees::Fee {
        let yaml = self.fee_yaml(FeeParameter::from(cost));
        serde_yaml::from_value::<near_primitives_core::runtime::fees::Fee>(yaml)
            .expect("just constructed a Fee YAML")
    }

    /// Read and parse a number parameter from the `ParameterTable`.
    fn get_number<T>(&self, key: Parameter) -> Result<T, InvalidConfigError>
    where
        T: TryFrom<u64>,
        <T as std::convert::TryFrom<u64>>::Error: std::fmt::Debug,
    {
        let value = self.parameters.get(&key).ok_or(InvalidConfigError::MissingParameter(key))?;
        match value {
            ParameterValue::U64(v) => Ok(T::try_from(*v).unwrap()),
            _ => Err(InvalidConfigError::WrongValueType(
                key,
                std::any::type_name::<u64>(),
                value.clone(),
            )),
        }
    }

    /// Read and parse a u128 parameter from the `ParameterTable`.
    fn get_u128(&self, key: Parameter) -> Result<u128, InvalidConfigError> {
        let value = self.parameters.get(&key).ok_or(InvalidConfigError::MissingParameter(key))?;
        match value {
            ParameterValue::U64(v) => Ok(u128::from(*v)),
            ParameterValue::String(s) => serde_yaml::from_str(s)
                .map_err(|err| InvalidConfigError::ValueParseError(err, s.to_owned())),
            _ => Err(InvalidConfigError::WrongValueType(
                key,
                std::any::type_name::<u128>(),
                value.clone(),
            )),
        }
    }

    /// Read and parse a string parameter from the `ParameterTable`.
    fn get_string(&self, key: Parameter) -> Result<String, InvalidConfigError> {
        let value = self.parameters.get(&key).ok_or(InvalidConfigError::MissingParameter(key))?;
        match value {
            ParameterValue::String(s) => Ok(s.clone()),
            _ => Err(InvalidConfigError::WrongValueType(
                key,
                std::any::type_name::<String>(),
                value.clone(),
            )),
        }
    }

    /// Read and parse a rational parameter from the `ParameterTable`.
    fn get_rational(&self, key: Parameter) -> Result<Rational, InvalidConfigError> {
        let value = self.parameters.get(&key).ok_or(InvalidConfigError::MissingParameter(key))?;
        match value {
            ParameterValue::Rational { numerator, denominator } => {
                Ok(Rational::new(*numerator, *denominator))
            }
            _ => Err(InvalidConfigError::WrongValueType(
                key,
                std::any::type_name::<String>(),
                value.clone(),
            )),
        }
    }

    fn fee_yaml(&self, key: FeeParameter) -> serde_yaml::Value {
        serde_yaml::to_value(BTreeMap::from([
            ("send_sir", self.get(format!("{key}_send_sir").parse().unwrap())),
            ("send_not_sir", self.get(format!("{key}_send_not_sir").parse().unwrap())),
            ("execution", self.get(format!("{key}_execution").parse().unwrap())),
        ]))
        .expect("failed to construct fee yaml")
    }
}

/// Represents values supported by parameter diff config.
#[derive(serde::Deserialize, Clone, Debug)]
struct ParameterDiffConfigValue {
    old: Option<serde_yaml::Value>,
    new: Option<serde_yaml::Value>,
}

impl std::str::FromStr for ParameterTableDiff {
    type Err = InvalidConfigError;
    fn from_str(arg: &str) -> Result<ParameterTableDiff, InvalidConfigError> {
        let yaml_map: BTreeMap<String, ParameterDiffConfigValue> =
            serde_yaml::from_str(arg).map_err(|err| InvalidConfigError::InvalidYaml(err))?;

        let parameters = yaml_map
            .iter()
            .map(|(key, value)| {
                let typed_key: Parameter = key
                    .parse()
                    .map_err(|err| InvalidConfigError::UnknownParameter(err, key.to_owned()))?;

                let old_value = if let Some(s) = &value.old {
                    parse_parameter_value(s)?
                } else {
                    ParameterValue::Null
                };

                let new_value = if let Some(s) = &value.new {
                    parse_parameter_value(s)?
                } else {
                    ParameterValue::Null
                };

                Ok((typed_key, (old_value, new_value)))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        Ok(ParameterTableDiff { parameters })
    }
}

/// Parses a value from YAML to a more restricted type of parameter values.
fn parse_parameter_value(value: &serde_yaml::Value) -> Result<ParameterValue, InvalidConfigError> {
    Ok(serde_yaml::from_value(canonicalize_yaml_value(value)?)
        .map_err(|err| InvalidConfigError::InvalidYaml(err))?)
}

/// Recursively canonicalizes values inside of the YAML structure.
fn canonicalize_yaml_value(
    value: &serde_yaml::Value,
) -> Result<serde_yaml::Value, InvalidConfigError> {
    Ok(match value {
        serde_yaml::Value::String(s) => canonicalize_yaml_string(s)?,
        serde_yaml::Value::Mapping(m) => serde_yaml::Value::Mapping(
            m.iter()
                .map(|(key, value)| {
                    let canonical_value = canonicalize_yaml_value(value)?;
                    Ok((key.clone(), canonical_value))
                })
                .collect::<Result<_, _>>()?,
        ),
        _ => value.clone(),
    })
}

/// Parses a value from the custom format for runtime parameter definitions.
///
/// A value can be a positive integer or a string, with or without quotes.
/// Integers can use underlines as separators (for readability).
///
/// The main purpose of this function is to add support for integers with underscore digit
/// separators which we use in the config but are not supported in YAML.
fn canonicalize_yaml_string(value: &str) -> Result<serde_yaml::Value, InvalidConfigError> {
    if value.is_empty() {
        return Ok(serde_yaml::Value::Null);
    }
    if value.bytes().all(|c| c.is_ascii_digit() || c == '_' as u8) {
        let mut raw_number = value.to_owned();
        raw_number.retain(char::is_numeric);
        // We do not have "arbitrary_precision" serde feature enabled, thus we
        // can only store up to `u64::MAX`, which is `18446744073709551615` and
        // has 20 characters.
        if raw_number.len() < 20 {
            serde_yaml::from_str(&raw_number)
                .map_err(|err| InvalidConfigError::ValueParseError(err, value.to_owned()))
        } else {
            Ok(serde_yaml::Value::String(raw_number))
        }
    } else {
        Ok(serde_yaml::Value::String(value.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        parse_parameter_value, InvalidConfigError, ParameterTable, ParameterTableDiff,
        ParameterValue,
    };
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
                    ParameterValue::Null
                } else {
                    parse_parameter_value(
                        &serde_yaml::from_str(value).expect("Test data has invalid YAML"),
                    )
                    .unwrap()
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

# Note the quotes here, they are necessary as otherwise the value can't be parsed by `serde_yaml`
# due to not fitting into u64 type.
storage_amount_per_byte: "100000000000000000000"
storage_num_bytes_account: 100
storage_num_extra_bytes_record   :   40  

"#;

    static DIFF_0: &str = r#"
# Comment line
registrar_account_id: { old: "registrar", new: "near" }
min_allowed_top_level_account_length: { old: 32, new: 32_000 }
wasm_regular_op_cost: { new: 3_856_371 }
"#;

    static DIFF_1: &str = r#"
# Comment line
registrar_account_id: { old: "near", new: "registrar" }
storage_num_extra_bytes_record: { old: 40, new: 77 }
wasm_regular_op_cost: { old: 3_856_371, new: 0 }
max_memory_pages: { new: 512 }
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
        let diff_with_empty_value = "min_allowed_top_level_account_length: { old: 32 }";
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
            check_invalid_parameter_table(
                "wasm_regular_op_cost: 100",
                &["invalid_key: { new: 100 }"]
            ),
            InvalidConfigError::UnknownParameter(_, _)
        );
    }

    #[test]
    fn test_parameter_table_no_key() {
        assert_matches!(
            check_invalid_parameter_table(": 100", &[]),
            // TODO(#8320): This must be invalid YAML after migration to `serde_yaml` 0.9.
            InvalidConfigError::UnknownParameter(_, _)
        );
    }

    #[test]
    fn test_parameter_table_no_key_in_diff() {
        assert_matches!(
            check_invalid_parameter_table("wasm_regular_op_cost: 100", &[": 100"]),
            InvalidConfigError::InvalidYaml(_)
        );
    }

    #[test]
    fn test_parameter_table_wrong_separator() {
        assert_matches!(
            check_invalid_parameter_table("wasm_regular_op_cost=100", &[]),
            InvalidConfigError::InvalidYaml(_)
        );
    }

    #[test]
    fn test_parameter_table_wrong_separator_in_diff() {
        assert_matches!(
            check_invalid_parameter_table(
                "wasm_regular_op_cost: 100",
                &["wasm_regular_op_cost=100"]
            ),
            InvalidConfigError::InvalidYaml(_)
        );
    }

    #[test]
    fn test_parameter_table_wrong_old_value() {
        assert_matches!(
            check_invalid_parameter_table(
                "min_allowed_top_level_account_length: 3_200_000_000",
                &["min_allowed_top_level_account_length: { old: 3_200_000, new: 1_600_000 }"]
            ),
            InvalidConfigError::WrongOldValue(
                Parameter::MinAllowedTopLevelAccountLength,
                expected,
                found
            ) => {
                assert_eq!(expected, ParameterValue::U64(3200000000));
                assert_eq!(found, ParameterValue::U64(3200000));
            }
        );
    }

    #[test]
    fn test_parameter_table_no_old_value() {
        assert_matches!(
            check_invalid_parameter_table(
                "min_allowed_top_level_account_length: 3_200_000_000",
                &["min_allowed_top_level_account_length: { new: 1_600_000 }"]
            ),
            InvalidConfigError::OldValueExists(Parameter::MinAllowedTopLevelAccountLength, expected) => {
                assert_eq!(expected, ParameterValue::U64(3200000000));
            }
        );
    }

    #[test]
    fn test_parameter_table_old_parameter_undefined() {
        assert_matches!(
            check_invalid_parameter_table(
                "min_allowed_top_level_account_length: 3_200_000_000",
                &["wasm_regular_op_cost: { old: 3_200_000, new: 1_600_000 }"]
            ),
            InvalidConfigError::NoOldValueExists(Parameter::WasmRegularOpCost, found) => {
                assert_eq!(found, ParameterValue::U64(3200000));
            }
        );
    }
}

use crate::internal::VMKind;
use near_vm_errors::PrepareError;
use near_vm_logic::VMConfig;

pub(crate) fn prepare_contract(
    original_code: &[u8],
    config: &VMConfig,
    kind: VMKind,
) -> Result<Vec<u8>, PrepareError> {
    let original_code = early_prepare(original_code, config)?;
    if kind == VMKind::NearVm {
        // Built-in near-vm code instruments code for itself.
        return Ok(original_code);
    }

    let res = finite_wasm::Analysis::new()
        .with_stack(Box::new(SimpleMaxStackCfg))
        .with_gas(Box::new(SimpleGasCostCfg(u64::from(config.regular_op_cost))))
        .analyze(&original_code)
        .map_err(|err| {
            tracing::error!(?err, ?kind, "Analysis failed");
            PrepareError::Deserialization
        })?
        // Make sure contracts can’t call the instrumentation functions via `env`.
        .instrument("internal", &original_code)
        .map_err(|err| {
            tracing::error!(?err, ?kind, "Instrumentation failed");
            PrepareError::Serialization
        })?;
    Ok(res)
}

/// Early preparation code.
///
/// Must happen before the finite-wasm analysis and is applicable to NearVm just as much as it is
/// applicable to other runtimes.
///
/// This will validate the module, normalize the memories within, apply limits.
fn early_prepare(code: &[u8], config: &VMConfig) -> Result<Vec<u8>, PrepareError> {
    use super::prepare_v1;
    prepare_v1::validate_contract(code, config)?;
    prepare_v1::ContractModule::init(code, config)? // TODO: completely get rid of pwasm-utils
        .scan_imports()?
        .standardize_mem()
        .ensure_no_internal_memory()?
        .into_wasm_code()
    // TODO: enforce our own limits, don't rely on wasmparser for it
}

// TODO: refactor to avoid copy-paste with the ones currently defined in near_vm_runner
struct SimpleMaxStackCfg;

impl finite_wasm::max_stack::SizeConfig for SimpleMaxStackCfg {
    fn size_of_value(&self, ty: finite_wasm::wasmparser::ValType) -> u8 {
        use finite_wasm::wasmparser::ValType;
        match ty {
            ValType::I32 => 4,
            ValType::I64 => 8,
            ValType::F32 => 4,
            ValType::F64 => 8,
            ValType::V128 => 16,
            ValType::FuncRef => 8,
            ValType::ExternRef => 8,
        }
    }
    fn size_of_function_activation(
        &self,
        locals: &prefix_sum_vec::PrefixSumVec<finite_wasm::wasmparser::ValType, u32>,
    ) -> u64 {
        let mut res = 64_u64; // Rough accounting for rip, rbp and some registers spilled. Not exact.
        let mut last_idx_plus_one = 0_u64;
        for (idx, local) in locals {
            let idx = u64::from(*idx);
            res = res.saturating_add(
                idx.checked_sub(last_idx_plus_one)
                    .expect("prefix-sum-vec indices went backwards")
                    .saturating_add(1)
                    .saturating_mul(u64::from(self.size_of_value(*local))),
            );
            last_idx_plus_one = idx.saturating_add(1);
        }
        res
    }
}

struct SimpleGasCostCfg(u64);

macro_rules! gas_cost {
    ($( @$proposal:ident $op:ident $({ $($arg:ident: $argty:ty),* })? => $visit:ident)*) => {
        $(
            fn $visit(&mut self $($(, $arg: $argty)*)?) -> u64 {
                gas_cost!(@@$proposal $op self $({ $($arg: $argty),* })? => $visit)
            }
        )*
    };

    (@@mvp $_op:ident $_self:ident $({ $($_arg:ident: $_argty:ty),* })? => visit_block) => {
        0
    };
    (@@mvp $_op:ident $_self:ident $({ $($_arg:ident: $_argty:ty),* })? => visit_end) => {
        0
    };
    (@@mvp $_op:ident $_self:ident $({ $($_arg:ident: $_argty:ty),* })? => visit_else) => {
        0
    };
    (@@$_proposal:ident $_op:ident $self:ident $({ $($arg:ident: $argty:ty),* })? => $visit:ident) => {
        $self.0
    };
}

impl<'a> finite_wasm::wasmparser::VisitOperator<'a> for SimpleGasCostCfg {
    type Output = u64;
    finite_wasm::wasmparser::for_each_operator!(gas_cost);
}

#[cfg(test)]
mod test {
    use crate::internal::VMKind;
    use near_vm_logic::{ContractPrepareVersion, VMConfig};

    #[test]
    fn v2_preparation_wasmtime_generates_valid_contract() {
        let mut config = VMConfig::test();
        config.limit_config.contract_prepare_version = ContractPrepareVersion::V2;
        bolero::check!().for_each(|input: &[u8]| {
            // DO NOT use ArbitraryModule. We do want modules that may be invalid here, if they pass our validation step!
            if let Ok(_) = crate::prepare::prepare_v1::validate_contract(input, &config) {
                match super::prepare_contract(input, &config, VMKind::Wasmtime) {
                    Err(_e) => (), // TODO: this should be a panic, but for now it’d actually trigger
                    Ok(code) => {
                        let mut validator = wasmparser::Validator::new();
                        validator.wasm_features(crate::prepare::WASM_FEATURES);
                        match validator.validate_all(&code) {
                            Ok(_) => (),
                            Err(e) => panic!(
                                "prepared code failed validation: {e:?}\ncontract: {}",
                                hex::encode(input),
                            ),
                        }
                    }
                }
            }
        });
    }

    #[test]
    fn v2_preparation_near_vm_generates_valid_contract() {
        let mut config = VMConfig::test();
        config.limit_config.contract_prepare_version = ContractPrepareVersion::V2;
        bolero::check!().for_each(|input: &[u8]| {
            // DO NOT use ArbitraryModule. We do want modules that may be invalid here, if they pass our validation step!
            if let Ok(_) = crate::prepare::prepare_v1::validate_contract(input, &config) {
                match super::prepare_contract(input, &config, VMKind::NearVm) {
                    Err(_e) => (), // TODO: this should be a panic, but for now it’d actually trigger
                    Ok(code) => {
                        let mut validator = wasmparser::Validator::new();
                        validator.wasm_features(crate::prepare::WASM_FEATURES);
                        match validator.validate_all(&code) {
                            Ok(_) => (),
                            Err(e) => panic!(
                                "prepared code failed validation: {e:?}\ncontract: {}",
                                hex::encode(input),
                            ),
                        }
                    }
                }
            }
        });
    }
}

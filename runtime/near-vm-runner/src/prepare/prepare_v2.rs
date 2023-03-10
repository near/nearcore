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
        .with_gas(Box::new(SimpleGasCostCfg(config.regular_op_cost.try_into().unwrap())))
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

// TODO: refactor to avoid copy-paste with the ones currently defined in near_vm
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
        let mut res = 0;
        res += locals.max_index().map(|l| u64::from(*l).saturating_add(1)).unwrap_or(0) * 8;
        // TODO: make the above take into account the types of locals by adding an iter on PrefixSumVec that returns (count, type)
        res += 32; // Rough accounting for rip, rbp and some registers spilled. Not exact.
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

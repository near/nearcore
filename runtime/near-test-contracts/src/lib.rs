#![doc = include_str!("../README.md")]

use arbitrary::Arbitrary;
use rand::{Fill, SeedableRng};
use std::borrow::Cow;
use std::sync::OnceLock;
use wasm_encoder::{
    BlockType, CodeSection, ConstExpr, DataSection, ElementSection, Elements, EntityType,
    ExportKind, ExportSection, Function, FunctionSection, GlobalSection, GlobalType, Ieee64,
    ImportSection, Instruction, MemorySection, MemoryType, Module, RefType, TableSection,
    TableType, TypeSection, ValType,
};

/// Parse a WASM contract from WAT representation.
pub fn wat_contract(wat: &str) -> Vec<u8> {
    wat::parse_str(wat).unwrap_or_else(|err| panic!("invalid wat: {err}\n{wat}"))
}

/// Trivial contract with a do-nothing main function.
pub fn trivial_contract() -> &'static [u8] {
    static CONTRACT: OnceLock<Vec<u8>> = OnceLock::new();
    CONTRACT.get_or_init(|| wat_contract(r#"(module (func (export "main")))"#)).as_slice()
}

/// Contract with exact size in bytes.
pub fn sized_contract(size: usize) -> Vec<u8> {
    let payload = "x".repeat(size);
    let base_size = wat_contract(&format!(
        r#"(module
            (memory 1)
            (func (export "main"))
            (data (i32.const 0) "{payload}")
        )"#
    ))
    .len();
    let adjusted_size = size as i64 - (base_size as i64 - size as i64);
    let payload = "x".repeat(adjusted_size as usize);
    let code = format!(
        r#"(module
            (memory 1)
            (func (export "main"))
            (data (i32.const 0) "{payload}")
        )"#
    );
    let contract = wat_contract(&code);
    assert_eq!(contract.len(), size);
    contract
}

/// Standard test contract which can call various host functions.
///
/// Note: the contract relies on the latest stable protocol version, and might
/// not work for tests using an older version. In particular, if a test depends
/// on a specific protocol version, it should use [`backwards_compatible_rs_contract`].
pub fn rs_contract() -> &'static [u8] {
    include_bytes!(env!("CONTRACT_test_contract_rs"))
}

/// Standard test contract which is compatible with the earliest
/// protocol version still supported by current implementation of neard.
///
/// Note, that neard typically supports two or three protocol versions
/// at any given time.
///
/// This is useful for tests that use a specific protocol version rather then
/// just the latest one. In particular, protocol upgrade tests should use this
/// function rather than [`rs_contract`].
pub fn backwards_compatible_rs_contract() -> &'static [u8] {
    include_bytes!(env!("CONTRACT_backwards_compatible_rs_contract"))
}

/// Standard test contract which is compatible any protocol version, including
/// the oldest one.
/// [`backwards_compatible_rs_contract`] should be preferred since it is more
/// up to date and also we no longer have to maintain compatibility with the
/// old protocol versions.
///
/// Note: Unlike other contracts, this is not automatically build from source
/// but instead a WASM in checked into source control. To serve the oldest
/// protocol version, we need a WASM that does not contain instructions beyond
/// the WASM MVP. Rustc >=1.70 uses LLVM with the [sign
/// extension](https://github.com/WebAssembly/spec/blob/main/proposals/sign-extension-ops/Overview.md)
/// enabled. So we have to build it with Rustc <= 1.69. If we need to update the
/// contracts content, we can build it manually with an older compiler and check
/// in the new WASM.
pub fn legacy_backwards_compatible_rs_contract() -> &'static [u8] {
    include_bytes!(env!("CONTRACT_legacy_backwards_compatible_rs_contract"))
}

/// Standard test contract which additionally includes all host functions from
/// the nightly protocol.
pub fn nightly_rs_contract() -> &'static [u8] {
    include_bytes!(env!("CONTRACT_nightly_test_contract_rs"))
}

pub fn ts_contract() -> &'static [u8] {
    include_bytes!(env!("CONTRACT_test_contract_ts"))
}

pub fn fuzzing_contract() -> &'static [u8] {
    include_bytes!(env!("CONTRACT_contract_for_fuzzing_rs"))
}

/// NEP-141 implementation (fungible token contract).
///
/// The code is available here:
/// <https://github.com/near/near-sdk-rs/tree/master/examples/fungible-token>
///
/// We keep a static WASM of this for our integration tests. We don't have to
/// update it with every SDK release, any contract implementing the interface
/// defined by NEP-141 is sufficient. But for future reference, the WASM was
/// compiled with SDK version 4.1.1.
pub fn ft_contract() -> &'static [u8] {
    include_bytes!(env!("CONTRACT_fungible_token"))
}

/// Smallest (reasonable) contract possible to build.
///
/// This contract is guaranteed to have a "sum" function
pub fn smallest_rs_contract() -> &'static [u8] {
    static CONTRACT: OnceLock<Vec<u8>> = OnceLock::new();
    CONTRACT
        .get_or_init(|| {
            wat_contract(
                r#"(module
            (func $input (import "env" "input") (param i64))
            (func $sum (export "sum") (param i32 i32) (result i32)
            (call $input
              (i64.const 0))
            (i32.add
              (local.get 1)
              (local.get 0)))
            (memory (export "memory") 16)
            (global (mut i32) (i32.const 1048576))
            (global (export "__data_end") i32 (i32.const 1048576))
            (global (export "__heap_base") i32 (i32.const 1048576)))"#,
            )
        })
        .as_slice()
}

/// Contract that has all methods required by the gas parameter estimator.
pub fn estimator_contract() -> &'static [u8] {
    include_bytes!(env!("CONTRACT_estimator_contract"))
}

pub fn congestion_control_test_contract() -> &'static [u8] {
    include_bytes!(env!("CONTRACT_congestion_control_test_contract"))
}

pub fn sharded_contract_test_contract() -> &'static [u8] {
    include_bytes!(env!("CONTRACT_sharded_contract"))
}

#[test]
fn smoke_test() {
    assert!(!rs_contract().is_empty());
    assert!(!nightly_rs_contract().is_empty());
    assert!(!ts_contract().is_empty());
    assert!(!trivial_contract().is_empty());
    assert!(!fuzzing_contract().is_empty());
    assert!(!backwards_compatible_rs_contract().is_empty());
    assert!(!ft_contract().is_empty());
    assert!(!congestion_control_test_contract().is_empty());
    assert!(!sharded_contract_test_contract().is_empty());
}

pub struct LargeContract {
    pub functions: u32,
    pub locals_per_function: u32,
    pub panic_imports: u32, // How many times to import `env.panic`
}

impl Default for LargeContract {
    fn default() -> Self {
        Self { functions: 1, locals_per_function: 0, panic_imports: 0 }
    }
}

impl LargeContract {
    /// Construct a contract with many entities.
    ///
    /// Currently supports constructing contracts that contain a specified number of functions with the
    /// specified number of locals each.
    ///
    /// Exports a function called `main` that does nothing.
    pub fn make(&self) -> Vec<u8> {
        // Won't generate a valid WASM without functions.
        assert!(self.functions >= 1, "must specify at least 1 function to be generated");
        let mut module = Module::new();
        let mut type_section = TypeSection::new();
        type_section.ty().function([], []);
        module.section(&type_section);

        if self.panic_imports != 0 {
            let mut import_section = ImportSection::new();
            for _ in 0..self.panic_imports {
                import_section.import("env", "panic", EntityType::Function(0));
            }
            module.section(&import_section);
        }

        let mut functions_section = FunctionSection::new();
        for _ in 0..self.functions {
            functions_section.function(0);
        }
        module.section(&functions_section);

        let mut exports_section = ExportSection::new();
        exports_section.export("main", ExportKind::Func, 0);
        module.section(&exports_section);

        let mut code_section = CodeSection::new();
        for _ in 0..self.functions {
            let mut f = Function::new([(self.locals_per_function, ValType::I64)]);
            f.instruction(&Instruction::End);
            code_section.function(&f);
        }
        module.section(&code_section);

        module.finish()
    }
}

/// Generate contracts with function bodies of large continuous sequences of `nop` instruction.
///
/// This is particularly useful for testing how gas instrumentation works and its corner cases.
pub fn function_with_a_lot_of_nop(nops: u64) -> Vec<u8> {
    let mut module = Module::new();
    let mut type_section = TypeSection::new();
    type_section.ty().function([], []);
    module.section(&type_section);
    let mut functions_section = FunctionSection::new();
    functions_section.function(0);
    module.section(&functions_section);
    let mut exports_section = ExportSection::new();
    exports_section.export("main", ExportKind::Func, 0);
    module.section(&exports_section);
    let mut code_section = CodeSection::new();
    let mut f = Function::new([]);
    for _ in 0..nops {
        f.instruction(&Instruction::Nop);
    }
    f.instruction(&Instruction::End);
    code_section.function(&f);
    module.section(&code_section);
    module.finish()
}

/// Many zero-initialized globals.
pub fn contract_with_num_globals(num_globals: u32) -> Vec<u8> {
    let mut module = Module::new();
    let mut types = TypeSection::new();
    types.ty().function([], []);
    module.section(&types);
    let mut funcs = FunctionSection::new();
    funcs.function(0);
    module.section(&funcs);
    let mut globals = GlobalSection::new();
    for _ in 0..num_globals {
        globals.global(
            GlobalType { val_type: ValType::I32, mutable: false, shared: false },
            &ConstExpr::i32_const(0),
        );
    }
    module.section(&globals);
    let mut exports = ExportSection::new();
    exports.export("main", ExportKind::Func, 0);
    module.section(&exports);
    let mut code = CodeSection::new();
    let mut f = Function::new([]);
    f.instruction(&Instruction::End);
    code.function(&f);
    module.section(&code);
    module.finish()
}

/// Many tiny active data segments, each writing 1 byte to memory offset 0.
pub fn many_data_segments_contract(num_segments: u32) -> Vec<u8> {
    let mut module = Module::new();
    let mut types = TypeSection::new();
    types.ty().function([], []);
    module.section(&types);
    let mut funcs = FunctionSection::new();
    funcs.function(0);
    module.section(&funcs);
    let mut memories = MemorySection::new();
    memories.memory(MemoryType {
        minimum: 1,
        maximum: None,
        memory64: false,
        shared: false,
        page_size_log2: None,
    });
    module.section(&memories);
    let mut exports = ExportSection::new();
    exports.export("main", ExportKind::Func, 0);
    module.section(&exports);
    let mut data = DataSection::new();
    for i in 0..num_segments {
        data.active(0, &ConstExpr::i32_const(0), [i as u8]);
    }
    module.section(&data);
    let mut code = CodeSection::new();
    let mut f = Function::new([]);
    f.instruction(&Instruction::End);
    code.function(&f);
    module.section(&code);
    module.finish()
}

/// Many active element segments, each writing function 0 into table slot 0.
pub fn many_element_segments_contract(num_segments: u32) -> Vec<u8> {
    let mut module = Module::new();
    let mut types = TypeSection::new();
    types.ty().function([], []);
    module.section(&types);
    let mut funcs = FunctionSection::new();
    funcs.function(0);
    module.section(&funcs);
    let mut tables = TableSection::new();
    tables.table(TableType {
        element_type: RefType::FUNCREF,
        minimum: 1,
        maximum: Some(1),
        table64: false,
        shared: false,
    });
    module.section(&tables);
    let mut exports = ExportSection::new();
    exports.export("main", ExportKind::Func, 0);
    module.section(&exports);
    let mut elements = ElementSection::new();
    for _ in 0..num_segments {
        elements.active(
            Some(0),
            &ConstExpr::i32_const(0),
            Elements::Functions(Cow::Borrowed(&[0u32])),
        );
    }
    module.section(&elements);
    let mut code = CodeSection::new();
    let mut f = Function::new([]);
    f.instruction(&Instruction::End);
    code.function(&f);
    module.section(&code);
    module.finish()
}

/// A contract with many nested blocks approaching protocol limits, maximizing
/// gas-instrumentation overhead and Winch compilation work.
pub fn max_blocks_contract(num_functions: u32, blocks_per_function: u32) -> Vec<u8> {
    let mut module = Module::new();
    let mut types = TypeSection::new();
    types.ty().function([], []);
    module.section(&types);
    let mut funcs = FunctionSection::new();
    for _ in 0..num_functions {
        funcs.function(0);
    }
    module.section(&funcs);
    let mut exports = ExportSection::new();
    exports.export("main", ExportKind::Func, 0);
    module.section(&exports);
    let mut code = CodeSection::new();
    for _ in 0..num_functions {
        let mut f = Function::new([]);
        for _ in 0..blocks_per_function {
            f.instruction(&Instruction::Block(BlockType::Empty));
            f.instruction(&Instruction::End);
        }
        f.instruction(&Instruction::End);
        code.function(&f);
    }
    module.section(&code);
    module.finish()
}

/// Tight f64.add infinite loop for NaN-canonicalization measurement.
pub fn float_nan_loop_contract() -> Vec<u8> {
    let mut module = Module::new();
    let mut types = TypeSection::new();
    types.ty().function([], []);
    module.section(&types);
    let mut funcs = FunctionSection::new();
    funcs.function(0);
    module.section(&funcs);
    let mut exports = ExportSection::new();
    exports.export("main", ExportKind::Func, 0);
    module.section(&exports);
    let mut code = CodeSection::new();
    let mut f = Function::new([(1, ValType::F64)]);
    f.instruction(&Instruction::Loop(BlockType::Empty));
    f.instruction(&Instruction::LocalGet(0));
    f.instruction(&Instruction::F64Const(Ieee64::from(1.5_f64)));
    f.instruction(&Instruction::F64Add);
    f.instruction(&Instruction::LocalSet(0));
    f.instruction(&Instruction::Br(0));
    f.instruction(&Instruction::End);
    f.instruction(&Instruction::End);
    code.function(&f);
    module.section(&code);
    module.finish()
}

/// Tight i64.div_u infinite loop for wide-arithmetic calibration measurement.
pub fn wide_arithmetic_loop_contract() -> Vec<u8> {
    let mut module = Module::new();
    let mut types = TypeSection::new();
    types.ty().function([], []);
    module.section(&types);
    let mut funcs = FunctionSection::new();
    funcs.function(0);
    module.section(&funcs);
    let mut exports = ExportSection::new();
    exports.export("main", ExportKind::Func, 0);
    module.section(&exports);
    let mut code = CodeSection::new();
    let mut f = Function::new([(1, ValType::I64)]);
    f.instruction(&Instruction::Loop(BlockType::Empty));
    // local += 7, then divide by 3; the value cycles and stays non-zero (never div-by-zero)
    f.instruction(&Instruction::LocalGet(0));
    f.instruction(&Instruction::I64Const(7));
    f.instruction(&Instruction::I64Add);
    f.instruction(&Instruction::I64Const(3));
    f.instruction(&Instruction::I64DivU);
    f.instruction(&Instruction::LocalSet(0));
    f.instruction(&Instruction::Br(0));
    f.instruction(&Instruction::End);
    f.instruction(&Instruction::End);
    code.function(&f);
    module.section(&code);
    module.finish()
}

/// Tight i64.add infinite loop — baseline for NaN-canonicalization comparison.
pub fn int_baseline_loop_contract() -> Vec<u8> {
    let mut module = Module::new();
    let mut types = TypeSection::new();
    types.ty().function([], []);
    module.section(&types);
    let mut funcs = FunctionSection::new();
    funcs.function(0);
    module.section(&funcs);
    let mut exports = ExportSection::new();
    exports.export("main", ExportKind::Func, 0);
    module.section(&exports);
    let mut code = CodeSection::new();
    let mut f = Function::new([(1, ValType::I64)]);
    f.instruction(&Instruction::Loop(BlockType::Empty));
    f.instruction(&Instruction::LocalGet(0));
    f.instruction(&Instruction::I64Const(1));
    f.instruction(&Instruction::I64Add);
    f.instruction(&Instruction::LocalSet(0));
    f.instruction(&Instruction::Br(0));
    f.instruction(&Instruction::End);
    f.instruction(&Instruction::End);
    code.function(&f);
    module.section(&code);
    module.finish()
}

/// Wrapper to get more useful Debug.
pub struct ArbitraryModule(pub wasm_smith::Module);

impl ArbitraryModule {
    pub fn new(config: wasm_smith::Config, u: &mut arbitrary::Unstructured) -> Self {
        wasm_smith::Module::new(config, u).map(ArbitraryModule).expect("arbitrary won't fail")
    }
}

fn normalize_config(mut config: wasm_smith::Config) -> wasm_smith::Config {
    config.canonicalize_nans = true;
    config.available_imports = Some(rs_contract().into());
    config.max_memories = 1;
    config.max_tables = 1;
    config.bulk_memory_enabled = false;
    config.exceptions_enabled = false;
    config.gc_enabled = false;
    config.memory64_enabled = false;
    config.multi_value_enabled = false;
    config.reference_types_enabled = false;
    config.relaxed_simd_enabled = false;
    config.saturating_float_to_int_enabled = true;
    config.sign_extension_ops_enabled = false;
    config.simd_enabled = false;
    config.tail_call_enabled = false;
    config.threads_enabled = false;
    config.custom_page_sizes_enabled = false;
    config
}

impl<'a> Arbitrary<'a> for ArbitraryModule {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let config = normalize_config(wasm_smith::Config::default());
        Ok(Self::new(config, u))
    }
}

impl std::fmt::Debug for ArbitraryModule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bytes = self.0.to_bytes();
        write!(f, "{:?}", bytes)?;
        if let Ok(wat) = wasmprinter::print_bytes(&bytes) {
            write!(f, "\n{}", wat)?;
        }
        Ok(())
    }
}

/// Generate an arbitrary valid contract.
pub fn arbitrary_contract(seed: u64) -> Vec<u8> {
    let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
    let mut buffer = vec![0u8; 10240];
    buffer.try_fill(&mut rng).expect("fill buffer with random data");
    let mut arbitrary = arbitrary::Unstructured::new(&buffer);
    let mut config =
        normalize_config(wasm_smith::Config::arbitrary(&mut arbitrary).expect("make config"));
    config.available_imports = Some(legacy_backwards_compatible_rs_contract().into());
    ArbitraryModule::new(config, &mut arbitrary).0.to_bytes()
}

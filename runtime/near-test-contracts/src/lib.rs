#![doc = include_str!("../README.md")]

use arbitrary::Arbitrary;
use once_cell::sync::OnceCell;
use rand::{Fill, SeedableRng};
use std::path::Path;

/// Parse a WASM contract from WAT representation.
pub fn wat_contract(wat: &str) -> Vec<u8> {
    wat::parse_str(wat).unwrap_or_else(|err| panic!("invalid wat: {err}\n{wat}"))
}

/// Trivial contract with a do-nothing main function.
pub fn trivial_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
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
    include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/res/", "test_contract_rs.wasm"))
}

/// Standard test contract which is compatible any protocol version, including
/// the oldest one.
///
/// This is useful for tests that use a specific protocol version rather then
/// just the latest one. In particular, protocol upgrade tests should use this
/// function rather than [`rs_contract`].
///
/// Note: Unlike other contracts, this is not automatically build from source
/// but instead a WASM in checked into source control. To serve the oldest
/// protocol version, we need a WASM that does not contain instructions beyond
/// the WASM MVP. Rustc >=1.70 uses LLVM with the [sign
/// extension](https://github.com/WebAssembly/spec/blob/main/proposals/sign-extension-ops/Overview.md)
/// enabled. So we have to build it with Rustc <= 1.69. If we need to update the
/// contracts content, we can build it manually with an older compiler and check
/// in the new WASM.
pub fn backwards_compatible_rs_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("backwards_compatible_rs_contract.wasm")).as_slice()
}

/// Standard test contract which additionally includes all host functions from
/// the nightly protocol.
pub fn nightly_rs_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("nightly_test_contract_rs.wasm")).as_slice()
}

pub fn ts_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("test_contract_ts.wasm")).as_slice()
}

pub fn fuzzing_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("contract_for_fuzzing_rs.wasm")).as_slice()
}

/// NEP-141 implementation (fungible token contract).
///
/// The code is available here:
/// https://github.com/near/near-sdk-rs/tree/master/examples/fungible-token
///
/// We keep a static WASM of this for our integration tests. We don't have to
/// update it with every SDK release, any contract implementing the interface
/// defined by NEP-141 is sufficient. But for future reference, the WASM was
/// compiled with SDK version 4.1.1.
pub fn ft_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("fungible_token.wasm")).as_slice()
}

/// Smallest (reasonable) contract possible to build.
///
/// This contract is guaranteed to have a "sum" function
pub fn smallest_rs_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
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
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    let file_name = if cfg!(feature = "nightly") {
        "nightly_estimator_contract.wasm"
    } else {
        "stable_estimator_contract.wasm"
    };
    CONTRACT.get_or_init(|| read_contract(file_name)).as_slice()
}

/// Read given wasm file or panic if unable to.
fn read_contract(file_name: &str) -> Vec<u8> {
    let base = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = base.join("res").join(file_name);
    match std::fs::read(&path) {
        Ok(data) => data,
        Err(err) => panic!("{}: {}", path.display(), err),
    }
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
    /// Construct a contract with many entitites.
    ///
    /// Currently supports constructing contracts that contain a specified number of functions with the
    /// specified number of locals each.
    ///
    /// Exports a function called `main` that does nothing.
    pub fn make(&self) -> Vec<u8> {
        use wasm_encoder::{
            CodeSection, EntityType, ExportKind, ExportSection, Function, FunctionSection,
            ImportSection, Instruction, Module, TypeSection, ValType,
        };

        // Won't generate a valid WASM without functions.
        assert!(self.functions >= 1, "must specify at least 1 function to be generated");
        let mut module = Module::new();
        let mut type_section = TypeSection::new();
        type_section.function([], []);
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

/// Generate an arbitrary valid contract.
pub fn arbitrary_contract(seed: u64) -> Vec<u8> {
    let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
    let mut buffer = vec![0u8; 10240];
    buffer.try_fill(&mut rng).expect("fill buffer with random data");
    let mut arbitrary = arbitrary::Unstructured::new(&buffer);
    let mut config = wasm_smith::SwarmConfig::arbitrary(&mut arbitrary).expect("make swarm config");
    config.max_memories = 1;
    config.max_tables = 1;
    config.bulk_memory_enabled = false;
    config.reference_types_enabled = false;
    config.memory64_enabled = false;
    config.simd_enabled = false;
    config.multi_value_enabled = false;
    config.relaxed_simd_enabled = false;
    config.exceptions_enabled = false;
    config.saturating_float_to_int_enabled = false;
    config.sign_extension_enabled = false;
    config.available_imports = Some(backwards_compatible_rs_contract().to_vec());
    let module = wasm_smith::Module::new(config, &mut arbitrary).expect("generate module");
    module.to_bytes()
}

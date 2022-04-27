#![doc = include_str!("../README.md")]

use once_cell::sync::OnceCell;
use std::path::Path;

/// Trivial contact with a do-nothing main function.
pub fn trivial_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT
        .get_or_init(|| wat::parse_str(r#"(module (func (export "main")))"#).unwrap())
        .as_slice()
}

/// Contract with exact size in bytes.
pub fn sized_contract(size: usize) -> Vec<u8> {
    let payload = "x".repeat(size);
    let base_size =
        wat::parse_str(format!("(module (data \"{payload}\") (func (export \"main\")))"))
            .unwrap()
            .len();
    let adjusted_size = size as i64 - (base_size as i64 - size as i64);
    let payload = "x".repeat(adjusted_size as usize);
    let code = format!("(module (data \"{payload}\") (func (export \"main\")))");
    let contract = wat::parse_str(code).unwrap();
    assert_eq!(contract.len(), size);
    contract
}

/// Standard test contract which can call various host functions.
///
/// Note: the contract relies on the latest protocol version, and
/// might not work for tests using an older version.
pub fn rs_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("test_contract_rs.wasm")).as_slice()
}

/// Standard test contract which is compatible with the oldest protocol version.
pub fn base_rs_contract() -> &'static [u8] {
    static CONTRACT: OnceCell<Vec<u8>> = OnceCell::new();
    CONTRACT.get_or_init(|| read_contract("base_test_contract_rs.wasm")).as_slice()
}

/// Standard test contract with all latest and nightly protocol features.
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
    assert!(!base_rs_contract().is_empty());
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
            CodeSection, EntityType, Export, ExportSection, Function, FunctionSection,
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
                import_section.import("env", Some("panic"), EntityType::Function(0));
            }
            module.section(&import_section);
        }

        let mut functions_section = FunctionSection::new();
        for _ in 0..self.functions {
            functions_section.function(0);
        }
        module.section(&functions_section);

        let mut exports_section = ExportSection::new();
        exports_section.export("main", Export::Function(0));
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

//! The in-memory contract cache bounds its footprint by the weights of its entries, so the
//! weight of a loaded module must cover the heap the module retains. Wasmtime decodes that heap
//! from the artifact's metadata, so each module here maximizes one kind of metadata.

use super::WasmtimeVM;
use super::load::{SectionLens, loaded_module_weight};
use crate::prepare::prepare_contract;
use near_parameters::RuntimeConfigStore;
use near_parameters::vm::VMKind;
use near_primitives_core::version::PROTOCOL_VERSION;
use std::alloc::{GlobalAlloc, Layout, System};
use std::borrow::Cow;
use std::cell::Cell;
use std::sync::Arc;
use wasm_encoder::{
    CodeSection, ConstExpr, DataSection, ElementSection, Elements, EntityType, ExportKind,
    ExportSection, Function, FunctionSection, GlobalSection, GlobalType, ImportSection,
    Instruction, Module as WasmModule, RefType, TableSection, TableType, TypeSection, ValType,
};

/// Counts the bytes each thread holds allocated, rounded up to the power of two an allocator
/// size class can reach. Only the loading thread's allocations are counted, so the test relies on
/// Wasmtime deserializing and linking a module on the calling thread.
struct CountingAllocator;

thread_local! {
    static LIVE_BYTES: Cell<i64> = const { Cell::new(0) };
}

fn footprint(size: usize) -> i64 {
    size.next_power_of_two().max(16) as i64
}

fn record(delta: i64) {
    let _ = LIVE_BYTES.try_with(|live| live.set(live.get() + delta));
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record(footprint(layout.size()));
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record(footprint(layout.size()));
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        record(-footprint(layout.size()));
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            record(footprint(new_size) - footprint(layout.size()));
        }
        new_ptr
    }
}

/// The allocator of the whole `near-vm-runner` lib test binary, not only of this module.
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[derive(Debug, Clone, Copy)]
enum Shape {
    /// Hundreds of thousands of export names for one function.
    ExportAliases,
    /// Host function imports up to the function limit.
    ImportedFunctions,
    /// Empty functions up to the function limit.
    DefinedFunctions,
    /// Types up to the type limit, each with the most parameters a function type can have.
    ParameterHeavyTypes,
    /// As [`Shape::ParameterHeavyTypes`], with reference-typed parameters.
    RefParameterHeavyTypes,
    /// Globals up to the global limit.
    Globals,
    /// Empty passive data segments up to Wasmtime's segment limit.
    EmptyDataSegments,
    /// A table at the table element limit, filled by one active element segment. Wasmtime keeps
    /// a precomputed image of the table.
    DenseTableInitializer,
    /// Passive element segments up to Wasmtime's segment limit, each with one function index.
    ManyPassiveElementSegments,
    /// Active element segments of `ref.func` expressions, filling the contract size limit.
    /// Wasmtime cannot precompute a table image from expressions, so it keeps the segments.
    FallbackElementInitialization,
}

impl Shape {
    const ALL: [Shape; 10] = [
        Shape::ExportAliases,
        Shape::ImportedFunctions,
        Shape::DefinedFunctions,
        Shape::ParameterHeavyTypes,
        Shape::RefParameterHeavyTypes,
        Shape::Globals,
        Shape::EmptyDataSegments,
        Shape::DenseTableInitializer,
        Shape::ManyPassiveElementSegments,
        Shape::FallbackElementInitialization,
    ];

    fn module(self) -> Vec<u8> {
        let mut module = WasmModule::new();
        let mut types = TypeSection::new();
        types.ty().function([], []);
        types.ty().function([ValType::I64, ValType::I64], []);
        let parameter = match self {
            Shape::ParameterHeavyTypes => Some(ValType::I32),
            Shape::RefParameterHeavyTypes => Some(ValType::Ref(RefType::FUNCREF)),
            _ => None,
        };
        if let Some(parameter) = parameter {
            // Distinct types, so that none of them are deduplicated.
            for i in 0..1_022 {
                let mut params = vec![parameter; 1_000];
                params[i % 1_000] = if i < 1_000 { ValType::I64 } else { ValType::F64 };
                types.ty().function(params, []);
            }
        }
        module.section(&types);
        if let Shape::ImportedFunctions = self {
            let mut imports = ImportSection::new();
            for _ in 0..9_999 {
                imports.import("env", "read_register", EntityType::Function(1));
            }
            module.section(&imports);
        }
        let defined_functions = match self {
            Shape::DefinedFunctions => 10_000,
            _ => 1,
        };
        let mut functions = FunctionSection::new();
        for _ in 0..defined_functions {
            functions.function(0);
        }
        module.section(&functions);
        let table_size = match self {
            Shape::DenseTableInitializer | Shape::FallbackElementInitialization => 10_000,
            _ => 1,
        };
        let mut tables = TableSection::new();
        tables.table(TableType {
            element_type: RefType::FUNCREF,
            table64: false,
            minimum: table_size,
            maximum: None,
            shared: false,
        });
        module.section(&tables);
        if let Shape::Globals = self {
            let mut globals = GlobalSection::new();
            for _ in 0..65_536 {
                globals.global(
                    GlobalType { val_type: ValType::I32, mutable: true, shared: false },
                    &ConstExpr::i32_const(0),
                );
            }
            module.section(&globals);
        }
        let mut exports = ExportSection::new();
        exports.export("main", ExportKind::Func, 0);
        if let Shape::ExportAliases = self {
            for i in 0..400_000 {
                let name: String = (0..3)
                    .map(|digit| char::from(b'!' + (i / 94u32.pow(digit) % 94) as u8))
                    .collect();
                exports.export(&name, ExportKind::Func, 0);
            }
        }
        module.section(&exports);
        let mut elements = ElementSection::new();
        match self {
            Shape::DenseTableInitializer => {
                let functions = vec![0; table_size as usize];
                elements.active(
                    Some(0),
                    &ConstExpr::i32_const(0),
                    Elements::Functions(Cow::Owned(functions)),
                );
            }
            Shape::ManyPassiveElementSegments => {
                for _ in 0..99_999 {
                    elements.passive(Elements::Functions(Cow::Borrowed(&[0])));
                }
            }
            Shape::FallbackElementInitialization => {
                let expressions = vec![ConstExpr::ref_func(0); table_size as usize];
                for _ in 0..139 {
                    elements.active(
                        Some(0),
                        &ConstExpr::i32_const(0),
                        Elements::Expressions(RefType::FUNCREF, Cow::Borrowed(&expressions)),
                    );
                }
            }
            _ => {}
        }
        if !elements.is_empty() {
            module.section(&elements);
        }
        let mut code = CodeSection::new();
        for _ in 0..defined_functions {
            let mut function = Function::new([]);
            function.instruction(&Instruction::End);
            code.function(&function);
        }
        module.section(&code);
        if let Shape::EmptyDataSegments = self {
            let mut data = DataSection::new();
            for _ in 0..99_999 {
                data.passive([]);
            }
            module.section(&data);
        }
        module.finish()
    }
}

#[test]
fn loaded_module_weight_covers_the_retained_heap() {
    let config = Arc::clone(&RuntimeConfigStore::new().get_config(PROTOCOL_VERSION).wasm_config);
    let vm = WasmtimeVM::new(Arc::clone(&config));
    let compile = |wasm: &[u8]| {
        let prepared = prepare_contract(wasm, &config, VMKind::Wasmtime).unwrap();
        vm.engine.precompile_module(&prepared).unwrap()
    };
    // Loading the first module also allocates the engine's lazily built state.
    vm.load_module(&compile(near_test_contracts::trivial_contract())).unwrap().unwrap();

    let contracts = [
        ("rs_contract".to_string(), near_test_contracts::rs_contract().to_vec()),
        ("ts_contract".to_string(), near_test_contracts::ts_contract().to_vec()),
        ("ft_contract".to_string(), near_test_contracts::ft_contract().to_vec()),
        ("estimator_contract".to_string(), near_test_contracts::estimator_contract().to_vec()),
    ];
    let shapes = Shape::ALL.map(|shape| (format!("{shape:?}"), shape.module()));
    for (name, wasm) in contracts.into_iter().chain(shapes) {
        let serialized = compile(&wasm);
        let before = LIVE_BYTES.with(Cell::get);
        let loaded = vm.load_module(&serialized).unwrap();
        let retained = LIVE_BYTES.with(Cell::get) - before;
        loaded.unwrap();
        let heap_weight = loaded_module_weight(&serialized) as i64 - serialized.len() as i64;
        assert!(
            retained <= heap_weight,
            "{name}: retains {retained} heap bytes, its weight covers {heap_weight}",
        );
    }
}

#[test]
fn section_lens_finds_the_sections_wasmtime_writes() {
    let config = Arc::clone(&RuntimeConfigStore::new().get_config(PROTOCOL_VERSION).wasm_config);
    let vm = WasmtimeVM::new(Arc::clone(&config));
    let prepared =
        prepare_contract(near_test_contracts::rs_contract(), &config, VMKind::Wasmtime).unwrap();
    let serialized = vm.engine.precompile_module(&prepared).unwrap();
    let section_lens = SectionLens::parse(&serialized).unwrap();
    assert!(section_lens.info > 0, "the artifact has no `.wasmtime.info` section");
    assert!(section_lens.data > 0, "the artifact has no `.rodata.wasm` section");
}

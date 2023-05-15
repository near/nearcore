// This file contains code from external sources.
// Attributions: https://github.com/wasmerio/wasmer/blob/master/ATTRIBUTIONS.md

//! Helper functions to gather information for each of the non-function sections of a
//! WebAssembly module.
//!
//! The code of these helper functions is straightforward since they only read metadata
//! about linear memories, tables, globals, etc. and store them for later use.
//!
//! The special case of the initialize expressions for table elements offsets or global variables
//! is handled, according to the semantics of WebAssembly, to only specific expressions that are
//! interpreted on the fly.
use super::environ::ModuleEnvironment;
use super::state::ModuleTranslationState;
use crate::wasm_unsupported;
use crate::{WasmError, WasmResult};
use core::convert::TryFrom;
use near_vm_types::entity::packed_option::ReservedValue;
use near_vm_types::entity::EntityRef;
use near_vm_types::{
    DataIndex, ElemIndex, FunctionIndex, FunctionType, GlobalIndex, GlobalInit, GlobalType,
    MemoryIndex, MemoryType, Mutability, Pages, SignatureIndex, TableIndex, TableType, Type, V128,
};
use std::boxed::Box;
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;
use std::vec::Vec;
use wasmparser::{
    self, Data, DataKind, DataSectionReader, Element, ElementItems, ElementKind,
    ElementSectionReader, Export, ExportSectionReader, ExternalKind, FunctionSectionReader,
    GlobalSectionReader, GlobalType as WPGlobalType, ImportSectionReader, MemorySectionReader,
    NameMap, NameSectionReader, Naming, Operator, TableSectionReader, Type as WPType, TypeRef,
    TypeSectionReader, ValType as WPValType,
};

/// Helper function translating wasmparser types to Wasm Type.
pub fn wptype_to_type(ty: WPValType) -> WasmResult<Type> {
    match ty {
        WPValType::I32 => Ok(Type::I32),
        WPValType::I64 => Ok(Type::I64),
        WPValType::F32 => Ok(Type::F32),
        WPValType::F64 => Ok(Type::F64),
        WPValType::V128 => Ok(Type::V128),
        WPValType::ExternRef => Ok(Type::ExternRef),
        WPValType::FuncRef => Ok(Type::FuncRef),
    }
}

/// Parses the Type section of the wasm module.
pub fn parse_type_section(
    types: TypeSectionReader,
    module_translation_state: &mut ModuleTranslationState,
    environ: &mut ModuleEnvironment,
) -> WasmResult<()> {
    let count = types.count();
    environ.reserve_signatures(count)?;

    for entry in types {
        if let Ok(WPType::Func(t)) = entry {
            let params: Box<[WPValType]> = t.params().into();
            let results: Box<[WPValType]> = t.results().into();
            let sig_params: Arc<[Type]> = params
                .iter()
                .map(|ty| {
                    wptype_to_type(*ty)
                        .expect("only numeric types are supported in function signatures")
                })
                .collect();
            let sig_results: Arc<[Type]> = results
                .iter()
                .map(|ty| {
                    wptype_to_type(*ty)
                        .expect("only numeric types are supported in function signatures")
                })
                .collect();
            let sig = FunctionType::new(sig_params, sig_results);
            environ.declare_signature(sig)?;
            module_translation_state.wasm_types.push((params, results));
        } else {
            unimplemented!("module linking not implemented yet")
        }
    }

    Ok(())
}

/// Parses the Import section of the wasm module.
pub fn parse_import_section<'data>(
    imports: ImportSectionReader<'data>,
    environ: &mut ModuleEnvironment<'data>,
) -> WasmResult<()> {
    environ.reserve_imports(imports.count())?;

    for entry in imports {
        let import = entry?;
        let module_name = import.module;
        let field_name = import.name;

        match import.ty {
            TypeRef::Func(sig) => {
                environ.declare_func_import(
                    SignatureIndex::from_u32(sig),
                    module_name,
                    field_name,
                )?;
            }
            TypeRef::Memory(mem) => {
                assert!(!mem.memory64, "64bit memory not implemented yet");
                environ.declare_memory_import(
                    MemoryType {
                        minimum: Pages(mem.initial.try_into().unwrap()),
                        maximum: mem.maximum.map(|m| Pages(m.try_into().unwrap())),
                        shared: mem.shared,
                    },
                    module_name,
                    field_name,
                )?;
            }
            TypeRef::Global(ref ty) => {
                environ.declare_global_import(
                    GlobalType {
                        ty: wptype_to_type(ty.content_type).unwrap(),
                        mutability: if ty.mutable { Mutability::Var } else { Mutability::Const },
                    },
                    module_name,
                    field_name,
                )?;
            }
            TypeRef::Table(ref tab) => {
                environ.declare_table_import(
                    TableType {
                        ty: wptype_to_type(tab.element_type).unwrap(),
                        minimum: tab.initial,
                        maximum: tab.maximum,
                    },
                    module_name,
                    field_name,
                )?;
            }
            TypeRef::Tag(_) => panic!("exception handling proposal is not implemented yet"),
        }
    }

    environ.finish_imports()?;
    Ok(())
}

/// Parses the Function section of the wasm module.
pub fn parse_function_section(
    functions: FunctionSectionReader,
    environ: &mut ModuleEnvironment,
) -> WasmResult<()> {
    let num_functions = functions.count();
    if num_functions == std::u32::MAX {
        // We reserve `u32::MAX` for our own use.
        return Err(WasmError::ImplLimitExceeded);
    }

    environ.reserve_func_types(num_functions)?;

    for entry in functions {
        let sigindex = entry?;
        environ.declare_func_type(SignatureIndex::from_u32(sigindex))?;
    }

    Ok(())
}

/// Parses the Table section of the wasm module.
pub fn parse_table_section(
    tables: TableSectionReader,
    environ: &mut ModuleEnvironment,
) -> WasmResult<()> {
    environ.reserve_tables(tables.count())?;

    for entry in tables {
        let table = entry?;
        environ.declare_table(TableType {
            ty: wptype_to_type(table.element_type).unwrap(),
            minimum: table.initial,
            maximum: table.maximum,
        })?;
    }

    Ok(())
}

/// Parses the Memory section of the wasm module.
pub fn parse_memory_section(
    memories: MemorySectionReader,
    environ: &mut ModuleEnvironment,
) -> WasmResult<()> {
    environ.reserve_memories(memories.count())?;

    for entry in memories {
        let mem = entry?;
        assert!(!mem.memory64, "64bit memory not implemented yet");

        environ.declare_memory(MemoryType {
            minimum: Pages(mem.initial.try_into().unwrap()),
            maximum: mem.maximum.map(|m| Pages(m.try_into().unwrap())),
            shared: mem.shared,
        })?;
    }

    Ok(())
}

/// Parses the Global section of the wasm module.
pub fn parse_global_section(
    globals: GlobalSectionReader,
    environ: &mut ModuleEnvironment,
) -> WasmResult<()> {
    environ.reserve_globals(globals.count())?;

    for entry in globals {
        let wasmparser::Global { ty: WPGlobalType { content_type, mutable }, init_expr } = entry?;
        let mut init_expr_reader = init_expr.get_binary_reader();
        let initializer = match init_expr_reader.read_operator()? {
            Operator::I32Const { value } => GlobalInit::I32Const(value),
            Operator::I64Const { value } => GlobalInit::I64Const(value),
            Operator::F32Const { value } => GlobalInit::F32Const(f32::from_bits(value.bits())),
            Operator::F64Const { value } => GlobalInit::F64Const(f64::from_bits(value.bits())),
            Operator::V128Const { value } => GlobalInit::V128Const(V128::from(*value.bytes())),
            Operator::RefNull { ty: _ } => GlobalInit::RefNullConst,
            Operator::RefFunc { function_index } => {
                GlobalInit::RefFunc(FunctionIndex::from_u32(function_index))
            }
            Operator::GlobalGet { global_index } => {
                GlobalInit::GetGlobal(GlobalIndex::from_u32(global_index))
            }
            ref s => {
                return Err(wasm_unsupported!("unsupported init expr in global section: {:?}", s));
            }
        };
        let global = GlobalType {
            ty: wptype_to_type(content_type).unwrap(),
            mutability: if mutable { Mutability::Var } else { Mutability::Const },
        };
        environ.declare_global(global, initializer)?;
    }

    Ok(())
}

/// Parses the Export section of the wasm module.
pub fn parse_export_section<'data>(
    exports: ExportSectionReader<'data>,
    environ: &mut ModuleEnvironment<'data>,
) -> WasmResult<()> {
    environ.reserve_exports(exports.count())?;

    for entry in exports {
        let Export { name, ref kind, index } = entry?;

        // The input has already been validated, so we should be able to
        // assume valid UTF-8 and use `from_utf8_unchecked` if performance
        // becomes a concern here.
        let index = index as usize;
        match *kind {
            ExternalKind::Func => environ.declare_func_export(FunctionIndex::new(index), name)?,
            ExternalKind::Table => environ.declare_table_export(TableIndex::new(index), name)?,
            ExternalKind::Memory => environ.declare_memory_export(MemoryIndex::new(index), name)?,
            ExternalKind::Global => environ.declare_global_export(GlobalIndex::new(index), name)?,
            ExternalKind::Tag => panic!("exception handling proposal is not implemented yet"),
        }
    }

    environ.finish_exports()?;
    Ok(())
}

/// Parses the Start section of the wasm module.
pub fn parse_start_section(index: u32, environ: &mut ModuleEnvironment) -> WasmResult<()> {
    environ.declare_start_function(FunctionIndex::from_u32(index))?;
    Ok(())
}

fn read_elems(items: &ElementItems) -> WasmResult<Box<[FunctionIndex]>> {
    match items.clone() {
        ElementItems::Functions(items) => items
            .into_iter()
            .map(|v| v.map(FunctionIndex::from_u32).map_err(WasmError::from))
            .collect(),
        ElementItems::Expressions(items) => {
            let mut elems = Vec::with_capacity(usize::try_from(items.count()).unwrap());
            for item in items.into_iter() {
                let mut reader = item?.get_operators_reader();
                let op = reader.read()?;
                let end = reader.read()?;
                reader.ensure_end()?;
                use Operator::*;
                match (op, end) {
                    (RefFunc { function_index }, End) => {
                        elems.push(FunctionIndex::from_u32(function_index))
                    }
                    (RefNull { .. }, End) => elems.push(FunctionIndex::reserved_value()),
                    _ => todo!("unexpected syntax for elems item initializer"),
                }
            }
            Ok(elems.into_boxed_slice())
        }
    }
}

/// Parses the Element section of the wasm module.
pub fn parse_element_section<'data>(
    elements: ElementSectionReader<'data>,
    environ: &mut ModuleEnvironment,
) -> WasmResult<()> {
    environ.reserve_table_initializers(elements.count())?;

    for (index, entry) in elements.into_iter().enumerate() {
        let Element { kind, items, ty, .. } = entry?;
        if ty != WPValType::FuncRef {
            return Err(wasm_unsupported!("unsupported table element type: {:?}", ty));
        }
        let segments = read_elems(&items)?;
        match kind {
            ElementKind::Active { table_index, offset_expr } => {
                let mut offset_expr_reader = offset_expr.get_binary_reader();
                let (base, offset) = match offset_expr_reader.read_operator()? {
                    Operator::I32Const { value } => (None, value as u32 as usize),
                    Operator::GlobalGet { global_index } => {
                        (Some(GlobalIndex::from_u32(global_index)), 0)
                    }
                    ref s => {
                        return Err(wasm_unsupported!(
                            "unsupported init expr in element section: {:?}",
                            s
                        ));
                    }
                };
                environ.declare_table_initializers(
                    TableIndex::from_u32(table_index),
                    base,
                    offset,
                    segments,
                )?
            }
            ElementKind::Passive => {
                let index = ElemIndex::from_u32(index as u32);
                environ.declare_passive_element(index, segments)?;
            }
            ElementKind::Declared => (),
        }
    }
    Ok(())
}

/// Parses the Data section of the wasm module.
pub fn parse_data_section<'data>(
    data: DataSectionReader<'data>,
    environ: &mut ModuleEnvironment<'data>,
) -> WasmResult<()> {
    environ.reserve_data_initializers(data.count())?;

    for (index, entry) in data.into_iter().enumerate() {
        let Data { kind, data, .. } = entry?;
        match kind {
            DataKind::Active { memory_index, offset_expr } => {
                let mut offset_expr_reader = offset_expr.get_binary_reader();
                let (base, offset) = match offset_expr_reader.read_operator()? {
                    Operator::I32Const { value } => (None, value as u32 as usize),
                    Operator::GlobalGet { global_index } => {
                        (Some(GlobalIndex::from_u32(global_index)), 0)
                    }
                    ref s => {
                        return Err(wasm_unsupported!(
                            "unsupported init expr in data section: {:?}",
                            s
                        ))
                    }
                };
                environ.declare_data_initialization(
                    MemoryIndex::from_u32(memory_index),
                    base,
                    offset,
                    data,
                )?;
            }
            DataKind::Passive => {
                let index = DataIndex::from_u32(index as u32);
                environ.declare_passive_data(index, data)?;
            }
        }
    }

    Ok(())
}

/// Parses the Name section of the wasm module.
pub fn parse_name_section<'data>(
    mut names: NameSectionReader<'data>,
    environ: &mut ModuleEnvironment<'data>,
) -> WasmResult<()> {
    use wasmparser::Name;
    while let Some(subsection) = names.next() {
        let subsection = subsection?;
        match subsection {
            Name::Function(function_subsection) => {
                if let Some(function_names) = parse_function_name_subsection(function_subsection) {
                    for (index, name) in function_names {
                        environ.declare_function_name(index, name)?;
                    }
                }
            }
            Name::Module { name, .. } => {
                environ.declare_module_name(name)?;
            }
            Name::Local(_) => {}
            Name::Label(_) => {}
            Name::Type(_) => {}
            Name::Table(_) => {}
            Name::Memory(_) => {}
            Name::Global(_) => {}
            Name::Element(_) => {}
            Name::Data(_) => {}
            Name::Unknown { .. } => {}
        };
    }
    Ok(())
}

fn parse_function_name_subsection(
    naming_reader: NameMap<'_>,
) -> Option<HashMap<FunctionIndex, &str>> {
    let mut function_names = HashMap::new();
    for name in naming_reader.into_iter() {
        let Naming { index, name } = name.ok()?;
        if index == std::u32::MAX {
            // We reserve `u32::MAX` for our own use.
            return None;
        }

        if function_names.insert(FunctionIndex::from_u32(index), name).is_some() {
            // If the function index has been previously seen, then we
            // break out of the loop and early return `None`, because these
            // should be unique.
            return None;
        }
    }
    Some(function_names)
}

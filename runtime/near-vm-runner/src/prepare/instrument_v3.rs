// NOTE: Adapted from https://github.com/near/finite-wasm/commits/164878dedc1220c4d789d5b07baaf6e2cd08ce35
//
// FIXME: Have `InstrumentContext` implement `Reencode` trait fully... rather than have it half way
// manually implemented and half-way reliant on wasm_encoder::reencode...

use crate::{REMAINING_GAS_EXPORT, START_EXPORT};
use core::num::NonZeroU64;
use finite_wasm_6::gas::InstrumentationKind;
use finite_wasm_6::{AnalysisOutcome, Fee};
use wasm_encoder::reencode::{Error as ReencodeError, Reencode};
use wasm_encoder::{self as we, InstructionSink};
use wasmparser_236 as wp;

const PLACEHOLDER_FOR_NAMES: u8 = !0;

const GAS_GLOBAL: u32 = 0;
const STACK_GLOBAL: u32 = GAS_GLOBAL + 1;

/// Total number of injected globals in the instrumented module.
const G: u32 = STACK_GLOBAL + 1;

/// These function indices are known to be constant, as they are added at the beginning of the
/// imports section.
///
/// Doing so makes it much easier to transform references to other functions (basically add F to
/// all function indices)
const GAS_EXHAUSTED_FN: u32 = 0;
const STACK_EXHAUSTED_FN: u32 = GAS_EXHAUSTED_FN + 1;
const GAS_INSTRUMENTATION_FN: u32 = STACK_EXHAUSTED_FN + 1;

/// Total number of injected functions in the instrumented module.
const F: u32 = GAS_INSTRUMENTATION_FN + 1;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("could not reencode the element section")]
    ElementSection(#[source] ReencodeError<ReencodeUserError>),
    #[error("could not reencode a function type")]
    ReencodeFunctionType(#[source] ReencodeError<ReencodeUserError>),
    #[error("could not reencode the globals section")]
    ReencodeGlobals(#[source] ReencodeError<ReencodeUserError>),
    #[error("could not reencode the imports section")]
    ReencodeImports(#[source] ReencodeError<ReencodeUserError>),
    #[error("could not reencode a local type")]
    ReencodeLocal(#[source] ReencodeError<ReencodeUserError>),
    #[error("could not parse the function locals")]
    ParseLocals(#[source] wp::BinaryReaderError),
    #[error("could not parse a function local")]
    ParseLocal(#[source] wp::BinaryReaderError),
    #[error("could not parse the function operators")]
    ParseOperators(#[source] wp::BinaryReaderError),
    #[error("could not parse an operator")]
    ParseOperator(#[source] wp::BinaryReaderError),
    #[error("could not parse an export")]
    ParseExport(#[source] wp::BinaryReaderError),
    #[error("could not parse a global")]
    ParseGlobal(#[source] wp::BinaryReaderError),
    #[error("could not parse a name section entry")]
    ParseName(#[source] wp::BinaryReaderError),
    #[error("could not parse a name map entry")]
    ParseNameMapName(#[source] wp::BinaryReaderError),
    #[error("could not parse an indirect name map entry")]
    ParseIndirectNameMapName(#[source] wp::BinaryReaderError),
    #[error("could not parse a module section header")]
    ParseModuleSection(#[source] wp::BinaryReaderError),
    #[error("could not parse a type section entry")]
    ParseType(#[source] wp::BinaryReaderError),
    #[error("could not parse an import section entry")]
    ParseImport(#[source] wp::BinaryReaderError),
    #[error("could not parse a function section entry")]
    ParseFunctionTypeId(#[source] wp::BinaryReaderError),
    #[error("the analysis outcome missing a {0} entry for code section entry `{1}`")]
    FunctionMissingInAnalysisOutcome(&'static str, usize),
    #[error("module contains fewer function types than definitions")]
    InsufficientFunctionTypes,
    #[error("module contains a reference to an invalid type index")]
    InvalidTypeIndex,
    #[error("size for custom section {0} is out of input bounds")]
    CustomSectionRange(u8, usize),
    #[error("could not remap function index {0}")]
    RemapFunctionIndex(u32),
    #[error("size for table section is out of input bounds")]
    TableSectionRange(usize),
    #[error("size for memory section is out of input bounds")]
    MemorySectionRange(usize),
    #[error("size for data count section is out of input bounds")]
    DataCountSection(usize),
    #[error("module contains too many globals")]
    TooManyGlobals,
    #[error("function contains too many locals")]
    TooManyLocals,
}

pub(crate) struct InstrumentContext<'a> {
    analysis: &'a AnalysisOutcome,
    wasm: &'a [u8],
    import_env: &'a str,
    globals: u32,
    op_cost: u32,
    max_stack_height: u32,

    type_section: we::TypeSection,
    import_section: we::ImportSection,
    function_section: we::FunctionSection,
    table_section: Option<we::RawSection<'a>>,
    memory_section: Option<we::RawSection<'a>>,
    global_section: we::GlobalSection,
    export_section: we::ExportSection,
    start_section: Option<we::StartSection>,
    element_section: we::ElementSection,
    datacount_section: Option<we::RawSection<'a>>,
    code_section: we::CodeSection,
    name_section: we::NameSection,
    raw_sections: Vec<we::RawSection<'a>>,

    types: Vec<we::FuncType>,
    function_types: std::vec::IntoIter<u32>,
}

struct InstrumentationReencoder;

impl InstrumentationReencoder {
    fn namemap(&mut self, p: wp::NameMap, is_function: bool) -> Result<we::NameMap, Error> {
        let mut new_name_map = we::NameMap::new();
        for naming in p {
            let naming = naming.map_err(Error::ParseNameMapName)?;
            let idx = self
                .function_index(naming.index)
                .or(Err(Error::RemapFunctionIndex(naming.index)))?;
            new_name_map.append(if is_function { idx } else { naming.index }, naming.name);
        }
        Ok(new_name_map)
    }

    fn indirectnamemap(&mut self, p: wp::IndirectNameMap) -> Result<we::IndirectNameMap, Error> {
        let mut new_name_map = we::IndirectNameMap::new();
        for naming in p {
            let naming = naming.map_err(Error::ParseIndirectNameMapName)?;
            let idx = self
                .function_index(naming.index)
                .or(Err(Error::RemapFunctionIndex(naming.index)))?;

            new_name_map.append(idx, &self.namemap(naming.names, false)?);
        }
        Ok(new_name_map)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ReencodeUserError {
    #[error("function index remapping error")]
    FunctionIndex,
}

impl<'a> Reencode for InstrumentationReencoder {
    type Error = ReencodeUserError;

    fn function_index(&mut self, func: u32) -> Result<u32, ReencodeError<Self::Error>> {
        func.checked_add(F).ok_or(ReencodeError::UserError(Self::Error::FunctionIndex))
    }
}

trait InstructionSinkExt {
    /// ```wat
    /// i64.add128
    /// i64.eqz
    /// if
    /// else
    ///     call $f
    ///     unreachable
    /// end
    /// ```
    fn checked_add_i64(self, f: u32) -> Self;

    /// ```wat
    /// i64.sub128
    /// i64.eqz
    /// if
    /// else
    ///     call $f
    ///     unreachable
    /// end
    /// ```
    fn checked_sub_i64(self, f: u32) -> Self;

    /// ```wat
    /// i64.mul_wide_u
    /// i64.eqz
    /// if
    /// else
    ///     call $f
    ///     unreachable
    /// end
    /// ```
    fn checked_mul_i64(self, f: u32) -> Self;
}

impl InstructionSinkExt for &mut we::InstructionSink<'_> {
    fn checked_add_i64(self, f: u32) -> Self {
        self.i64_add128().i64_eqz().if_(we::BlockType::Empty).else_().call(f).unreachable().end()
    }

    fn checked_sub_i64(self, f: u32) -> Self {
        self.i64_sub128().i64_eqz().if_(we::BlockType::Empty).else_().call(f).unreachable().end()
    }

    fn checked_mul_i64(self, f: u32) -> Self {
        self.i64_mul_wide_u()
            .i64_eqz()
            .if_(we::BlockType::Empty)
            .else_()
            .call(f)
            .unreachable()
            .end()
    }
}

impl<'a> InstrumentContext<'a> {
    pub(crate) fn new(
        wasm: &'a [u8],
        import_env: &'a str,
        analysis: &'a AnalysisOutcome,
        op_cost: u32,
        max_stack_height: u32,
    ) -> Self {
        Self {
            analysis,
            wasm,
            import_env,
            globals: 0,
            op_cost,
            max_stack_height,

            type_section: we::TypeSection::new(),
            import_section: we::ImportSection::new(),
            function_section: we::FunctionSection::new(),
            table_section: None,
            memory_section: None,
            global_section: we::GlobalSection::new(),
            export_section: we::ExportSection::new(),
            start_section: None,
            element_section: we::ElementSection::new(),
            datacount_section: None,
            code_section: we::CodeSection::new(),
            name_section: we::NameSection::new(),
            raw_sections: vec![],

            types: vec![],
            function_types: vec![].into_iter(),
        }
    }

    pub(crate) fn run(mut self) -> Result<Vec<u8>, Error> {
        let parser = wp::Parser::new(0);
        let mut renc = InstrumentationReencoder;
        for payload in parser.parse_all(self.wasm) {
            let payload = payload.map_err(Error::ParseModuleSection)?;
            match payload {
                // These two payload types are (re-)generated by wasm_encoder.
                wp::Payload::Version { .. } => {}
                wp::Payload::End(_) => {}
                // We must manually reconstruct the type section because we’re appending types to
                // it.
                wp::Payload::TypeSection(types) => {
                    for ty in types.into_iter_err_on_gc_types() {
                        let ty = ty.map_err(Error::ParseType)?;
                        let ty = renc.func_type(ty).map_err(Error::ReencodeFunctionType)?;
                        self.type_section.ty().func_type(&ty);
                        self.types.push(ty);
                    }
                }

                // We must manually reconstruct the imports section because we’re prepending imports
                // to it.
                wp::Payload::ImportSection(imports) => {
                    self.maybe_add_imports();
                    for import in imports {
                        let import = import.map_err(Error::ParseImport)?;
                        match import.ty {
                            wp::TypeRef::Global(..) => {
                                self.globals =
                                    self.globals.checked_add(1).ok_or(Error::TooManyGlobals)?;
                            }
                            wp::TypeRef::Func(..)
                            | wp::TypeRef::Table(..)
                            | wp::TypeRef::Memory(..)
                            | wp::TypeRef::Tag(..) => {}
                        }
                        renc.parse_import(&mut self.import_section, import)
                            .map_err(Error::ReencodeImports)?;
                    }
                }
                wp::Payload::StartSection { func, .. } => {
                    let function_index =
                        renc.function_index(func).or(Err(Error::RemapFunctionIndex(func)))?;
                    // Export the start function as a regular
                    // function under well-known name, such that the runtime could:
                    // 1. instantiate the module
                    // 2. lookup the [`REMAINING_GAS_EXPORT`] global on the instance
                    // 3. set the value of [`REMAINING_GAS_EXPORT`] global
                    // 4. invoke [`START_EXPORT`]
                    self.export_section.export(START_EXPORT, we::ExportKind::Func, function_index);
                }
                wp::Payload::ElementSection(reader) => {
                    renc.parse_element_section(&mut self.element_section, reader)
                        .map_err(Error::ElementSection)?;
                }
                wp::Payload::FunctionSection(reader) => {
                    // We don’t want to modify this, but need to remember function type indices…
                    let fn_types = reader
                        .into_iter()
                        .collect::<Result<Vec<u32>, _>>()
                        .map_err(Error::ParseFunctionTypeId)?;
                    for fnty in &fn_types {
                        self.function_section.function(*fnty);
                    }
                    self.function_types = fn_types.into_iter();
                }
                wp::Payload::TableSection(..) => {
                    let (id, range) = payload.as_section().unwrap();
                    let len = range.len();
                    self.table_section = Some(we::RawSection {
                        id,
                        data: self.wasm.get(range).ok_or(Error::TableSectionRange(len))?,
                    });
                }
                wp::Payload::MemorySection(..) => {
                    let (id, range) = payload.as_section().unwrap();
                    let len = range.len();
                    self.memory_section = Some(we::RawSection {
                        id,
                        data: self.wasm.get(range).ok_or(Error::MemorySectionRange(len))?,
                    });
                }
                wp::Payload::CodeSectionStart { .. } => {}
                wp::Payload::CodeSectionEntry(reader) => {
                    self.maybe_add_imports();
                    if self.global_section.is_empty() {
                        self.add_globals();
                    }
                    let type_index =
                        self.function_types.next().ok_or(Error::InsufficientFunctionTypes)?;
                    self.transform_code_section(&mut renc, reader, type_index)?;
                }
                wp::Payload::ExportSection(reader) => {
                    for export in reader {
                        let export = export.map_err(Error::ParseExport)?;
                        let (kind, index) = match export.kind {
                            wp::ExternalKind::Func => {
                                let idx = renc
                                    .function_index(export.index)
                                    .or(Err(Error::RemapFunctionIndex(export.index)))?;
                                (we::ExportKind::Func, idx)
                            }
                            wp::ExternalKind::Table => (we::ExportKind::Table, export.index),
                            wp::ExternalKind::Memory => (we::ExportKind::Memory, export.index),
                            wp::ExternalKind::Global => (we::ExportKind::Global, export.index),
                            wp::ExternalKind::Tag => (we::ExportKind::Tag, export.index),
                        };
                        self.export_section.export(export.name, kind, index);
                    }
                }
                wp::Payload::GlobalSection(reader) => {
                    for global in reader {
                        let global = global.map_err(Error::ParseGlobal)?;
                        renc.parse_global(&mut self.global_section, global)
                            .map_err(Error::ReencodeGlobals)?;
                        self.globals = self.globals.checked_add(1).ok_or(Error::TooManyGlobals)?;
                    }
                    if self.globals.checked_add(G).is_none() {
                        return Err(Error::TooManyGlobals);
                    }
                    self.add_globals();
                }
                wp::Payload::DataCountSection { .. } => {
                    let (id, range) = payload.as_section().unwrap();
                    let len = range.len();
                    self.datacount_section = Some(we::RawSection {
                        id,
                        data: self.wasm.get(range).ok_or(Error::DataCountSection(len))?,
                    });
                }
                wp::Payload::CustomSection(reader) if reader.name() == "name" => {
                    let wp::KnownCustom::Name(names) = reader.as_known() else {
                        continue;
                    };
                    if let Ok(_) = self.transform_name_section(&mut renc, names) {
                        // Keep valid name sections only. These sections don't have
                        // semantic purposes, so it isn't a big deal if we only keep the
                        // old section, or don't transform at all.
                        //
                        // (This is largely useful for fuzzing only)
                        self.raw_sections
                            .push(we::RawSection { id: PLACEHOLDER_FOR_NAMES, data: &[] });
                    }
                }
                // All the other sections are transparently copied over (they cannot reference a
                // function id, or we don’t know how to handle it anyhow)
                _ => {
                    let (id, range) = payload
                        .as_section()
                        .expect("any non-section payloads should have been handled already");
                    let len = range.len();
                    self.raw_sections.push(wasm_encoder::RawSection {
                        id,
                        data: self.wasm.get(range).ok_or(Error::CustomSectionRange(id, len))?,
                    });
                }
            }
        }
        // The type and import sections always come first in a module. They may potentially be
        // preceded or interspersed by custom sections in the original module, so we’re just hoping
        // that the ordering doesn’t matter for tests…
        let mut output = wasm_encoder::Module::new();
        if !self.type_section.is_empty() {
            output.section(&self.type_section);
        }
        if !self.import_section.is_empty() {
            output.section(&self.import_section);
        }
        if !self.function_section.is_empty() {
            output.section(&self.function_section);
        }
        if let Some(section) = self.table_section {
            output.section(&section);
        }
        if let Some(section) = self.memory_section {
            output.section(&section);
        }
        if !self.global_section.is_empty() {
            output.section(&self.global_section);
        }
        if !self.export_section.is_empty() {
            output.section(&self.export_section);
        }
        if let Some(section) = self.start_section {
            output.section(&section);
        }
        if !self.element_section.is_empty() {
            output.section(&self.element_section);
        }
        if let Some(section) = self.datacount_section {
            output.section(&section);
        }
        if !self.code_section.is_empty() {
            output.section(&self.code_section);
        }
        for section in self.raw_sections {
            match section.id {
                PLACEHOLDER_FOR_NAMES => output.section(&self.name_section),
                _ => output.section(&section),
            };
        }
        Ok(output.finish())
    }

    fn transform_code_section(
        &mut self,
        renc: &mut InstrumentationReencoder,
        reader: wp::FunctionBody,
        func_type_idx: u32,
    ) -> Result<(), Error> {
        let func_type_idx_usize =
            usize::try_from(func_type_idx).or(Err(Error::InvalidTypeIndex))?;
        let func_type = self.types.get(func_type_idx_usize).ok_or(Error::InvalidTypeIndex)?;

        let local_idx: u32 = func_type.params().len().try_into().or(Err(Error::TooManyLocals))?;
        let (mut locals, local_idx) =
            reader.get_locals_reader().map_err(Error::ParseLocals)?.into_iter().try_fold(
                (Vec::default(), local_idx),
                |(mut locals, local_idx), v| -> Result<_, Error> {
                    let (n, ty) = v.map_err(Error::ParseLocal)?;
                    let ty = renc.val_type(ty).map_err(Error::ReencodeLocal)?;
                    locals.push((n, ty));
                    let local_idx = local_idx.checked_add(n).ok_or(Error::TooManyLocals)?;
                    Ok((locals, local_idx))
                },
            )?;
        let code_idx = self.code_section.len() as usize;
        macro_rules! get_idx {
            (analysis . $field: ident) => {{
                let f = self.analysis.$field.get(code_idx);
                const NAME: &str = stringify!($field);
                f.ok_or(Error::FunctionMissingInAnalysisOutcome(NAME, code_idx))
            }};
        }
        let gas_costs = get_idx!(analysis.gas_costs)?;
        let gas_kinds = get_idx!(analysis.gas_kinds)?;
        let gas_offsets = get_idx!(analysis.gas_offsets)?;
        let stack_sz = *get_idx!(analysis.function_operand_stack_sizes)?;
        let frame_sz = *get_idx!(analysis.function_frame_sizes)?;

        let mut instrumentation_points =
            gas_offsets.iter().zip(gas_costs.iter()).zip(gas_kinds.iter()).peekable();
        let mut operators = reader.get_operators_reader().map_err(Error::ParseOperators)?;

        // In order to enable us to insert the code to release the stack allocation, we’ll wrap the
        // function body into a `block` and insert the instrumentation after the block ends… This
        // additional wrapping block allows us to “intercept” various branching instructions with
        // frame depths that would otherwise lead to a return. This is especially important when
        // these branching instructions are conditional: we could replace `br $well_chosen_index`
        // with a `return` and handle it much the same way, but we can’t do anything of the sort
        // for `br_if $well_chosen_index`.
        let (params, results) = (func_type.params(), func_type.results());
        // NOTE: Function parameters become locals, rather than operands, so we don’t need to
        // handle them in any way when inserting the block.
        let block_type = match (params, results) {
            (_, []) => we::BlockType::Empty,
            (_, [result]) => we::BlockType::Result(*result),
            ([], _) => we::BlockType::FunctionType(func_type_idx),
            (_, results) => {
                let new_block_type_idx = self.type_section.len();
                self.type_section.ty().function(std::iter::empty(), results.iter().copied());
                we::BlockType::FunctionType(new_block_type_idx)
            }
        };

        locals.push((1, we::ValType::I64));
        locals.push((1, we::ValType::I32));
        let mut new_function = we::Function::new(locals);
        'outer: {
            let Some(stack_charge) = stack_sz.checked_add(frame_sz).map(NonZeroU64::new) else {
                new_function.instructions().call(STACK_EXHAUSTED_FN).unreachable().end();
                break 'outer;
            };
            if let Some(stack_charge) = stack_charge {
                let mut new_function = new_function.instructions();

                let Some(gas_charge) = frame_sz
                    .checked_add(7)
                    .map(|n| n / 8)
                    .and_then(|n| n.checked_mul(self.op_cost.into()))
                else {
                    new_function.call(GAS_EXHAUSTED_FN).unreachable().end();
                    break 'outer;
                };
                new_function
                    .block(block_type)
                    .global_get(self.globals + STACK_GLOBAL)
                    // $stack
                    .i64_const(0)
                    .i64_const(u64::from(stack_charge) as i64)
                    .i64_const(0)
                    .checked_sub_i64(STACK_EXHAUSTED_FN)
                    // $stack - $stack_size - $frame_size
                    .global_set(self.globals + STACK_GLOBAL);
                call_gas_instrumentation(
                    &mut new_function,
                    None,
                    Fee { constant: gas_charge, linear: 0 },
                    self.globals,
                    local_idx,
                )?;
            }
            while !operators.eof() {
                let (op, offset) = operators.read_with_offset().map_err(Error::ParseOperator)?;
                let end_offset = operators.original_position();
                while instrumentation_points.peek().map(|((o, _), _)| **o) == Some(offset) {
                    let ((_, g), k) = instrumentation_points.next().expect("we just peeked");
                    if !matches!(k, InstrumentationKind::Unreachable) {
                        call_gas_instrumentation(
                            &mut new_function.instructions(),
                            Some(*k),
                            *g,
                            self.globals,
                            local_idx,
                        )?;
                    }
                }
                match op {
                    wp::Operator::RefFunc { function_index } => {
                        let idx = renc
                            .function_index(function_index)
                            .or(Err(Error::RemapFunctionIndex(function_index)))?;
                        new_function.instructions().ref_func(idx);
                    }
                    wp::Operator::Call { function_index } => {
                        let idx = renc
                            .function_index(function_index)
                            .or(Err(Error::RemapFunctionIndex(function_index)))?;
                        new_function.instructions().call(idx);
                    }
                    wp::Operator::ReturnCall { function_index } => {
                        let mut new_function = new_function.instructions();
                        if let Some(charge) = stack_charge {
                            call_unstack_instrumentation(&mut new_function, charge, self.globals);
                        }
                        let idx = renc
                            .function_index(function_index)
                            .or(Err(Error::RemapFunctionIndex(function_index)))?;
                        new_function.return_call(idx);
                    }
                    wp::Operator::ReturnCallIndirect { .. } => {
                        if let Some(charge) = stack_charge {
                            call_unstack_instrumentation(
                                &mut new_function.instructions(),
                                charge,
                                self.globals,
                            );
                        }
                        new_function.raw(self.wasm[offset..end_offset].iter().copied());
                    }
                    wp::Operator::Return => {
                        // FIXME: we could replace these `return`s with `br $well_chosen_index`
                        // targeting the block we inserted around the function body.
                        let mut new_function = new_function.instructions();
                        if let Some(charge) = stack_charge {
                            call_unstack_instrumentation(&mut new_function, charge, self.globals);
                        }
                        new_function.return_();
                    }
                    wp::Operator::End if operators.eof() => {
                        // This is the last function end…
                        let mut new_function = new_function.instructions();
                        if let Some(charge) = stack_charge {
                            new_function.end();
                            call_unstack_instrumentation(&mut new_function, charge, self.globals);
                        }
                        new_function.end();
                    }
                    _ => {
                        new_function.raw(self.wasm[offset..end_offset].iter().copied());
                    }
                };
            }
        }

        self.code_section.function(&new_function);
        Ok(())
    }

    fn maybe_add_imports(&mut self) {
        if self.import_section.is_empty() {
            // By adding the type at the end of the type section we guarantee that any other
            // type references remain valid.
            let exhausted_fnty = self.type_section.len();
            self.type_section.ty().function([], []);
            let gas_fnty = self.type_section.len();
            self.type_section.ty().function([we::ValType::I64], []);

            // By inserting the imports at the beginning of the import section we make the new
            // function index mapping trivial (it is always just an increment by `F`)
            debug_assert_eq!(self.import_section.len(), GAS_EXHAUSTED_FN);
            self.import_section.import(
                self.import_env,
                "finite_wasm_gas_exhausted",
                we::EntityType::Function(exhausted_fnty),
            );
            debug_assert_eq!(self.import_section.len(), STACK_EXHAUSTED_FN);
            self.import_section.import(
                self.import_env,
                "finite_wasm_stack_exhausted",
                we::EntityType::Function(exhausted_fnty),
            );
            debug_assert_eq!(self.import_section.len(), GAS_INSTRUMENTATION_FN);
            self.import_section.import(
                self.import_env,
                "finite_wasm_gas",
                we::EntityType::Function(gas_fnty),
            );
            debug_assert_eq!(self.import_section.len(), F);
        }
    }

    fn add_globals(&mut self) {
        debug_assert!(self.global_section.len() <= self.globals + GAS_GLOBAL);
        self.global_section.global(
            we::GlobalType { val_type: we::ValType::I64, mutable: true, shared: false },
            &we::ConstExpr::i64_const(0),
        );
        debug_assert!(self.global_section.len() <= self.globals + STACK_GLOBAL);
        self.global_section.global(
            we::GlobalType { val_type: we::ValType::I64, mutable: true, shared: false },
            &we::ConstExpr::i64_const(self.max_stack_height.into()),
        );
        debug_assert!(self.global_section.len() <= self.globals + G);

        self.export_section.export(
            REMAINING_GAS_EXPORT,
            we::ExportKind::Global,
            self.globals + GAS_GLOBAL,
        );
    }

    fn transform_name_section(
        &mut self,
        renc: &mut InstrumentationReencoder,
        names: wp::NameSectionReader,
    ) -> Result<(), Error> {
        for name in names {
            let name = name.map_err(Error::ParseName)?;
            match name {
                wp::Name::Module { name, .. } => self.name_section.module(name),
                wp::Name::Function(map) => {
                    let mut new_name_map = we::NameMap::new();
                    new_name_map.append(GAS_EXHAUSTED_FN, "finite_wasm_gas_exhausted");
                    new_name_map.append(STACK_EXHAUSTED_FN, "finite_wasm_stack_exhausted");
                    new_name_map.append(GAS_INSTRUMENTATION_FN, "finite_wasm_gas");
                    for naming in map {
                        let naming = naming.map_err(Error::ParseNameMapName)?;
                        let idx = renc
                            .function_index(naming.index)
                            .or(Err(Error::RemapFunctionIndex(naming.index)))?;

                        new_name_map.append(idx, naming.name);
                    }
                    self.name_section.functions(&new_name_map)
                }
                wp::Name::Local(map) => self.name_section.locals(&renc.indirectnamemap(map)?),
                wp::Name::Label(map) => self.name_section.labels(&renc.indirectnamemap(map)?),
                wp::Name::Type(map) => self.name_section.types(&renc.namemap(map, false)?),
                wp::Name::Table(map) => self.name_section.tables(&renc.namemap(map, false)?),
                wp::Name::Memory(map) => self.name_section.memories(&renc.namemap(map, false)?),
                wp::Name::Global(map) => self.name_section.globals(&renc.namemap(map, false)?),
                wp::Name::Element(map) => self.name_section.elements(&renc.namemap(map, false)?),
                wp::Name::Data(map) => self.name_section.data(&renc.namemap(map, false)?),
                wp::Name::Field(map) => self.name_section.fields(&renc.indirectnamemap(map)?),
                wp::Name::Tag(map) => self.name_section.tag(&renc.namemap(map, false)?),
                wp::Name::Unknown { .. } => {}
            }
        }
        Ok(())
    }
}

fn call_unstack_instrumentation(func: &mut InstructionSink<'_>, charge: NonZeroU64, globals: u32) {
    func.global_get(globals + STACK_GLOBAL)
        .i64_const(0)
        // This cast being able to wrap-around is intentional.
        // The callee must reinterpret this back to unsigned.
        .i64_const(u64::from(charge) as i64)
        .i64_const(0)
        .checked_add_i64(STACK_EXHAUSTED_FN)
        // $stack + $operand_size + $frame_size
        .global_set(globals + STACK_GLOBAL);
}

fn call_gas_instrumentation(
    func: &mut InstructionSink<'_>,
    k: Option<InstrumentationKind>,
    gas: Fee,
    globals: u32,
    local_idx: u32,
) -> Result<(), Error> {
    if matches!(gas, Fee::ZERO) {
        return Ok(());
    } else if gas.linear == 0 {
        // The reinterpreting cast is intentional here. On the other side the host function is
        // expected to reinterpret the argument back to u64.
        func.global_get(globals + GAS_GLOBAL)
            .i64_const(gas.constant as i64)
            // $gas | $constant
            .i64_lt_u()
            // $gas < $constant
            .if_(we::BlockType::Empty)
            .i64_const(gas.constant as i64)
            .call(GAS_INSTRUMENTATION_FN)
            .unreachable()
            .else_()
            .global_get(globals + GAS_GLOBAL)
            .i64_const(gas.constant as i64)
            .i64_sub()
            // $gas - $constant
            .global_set(globals + GAS_GLOBAL)
            .end();
        return Ok(());
    }
    match k {
        Some(
            InstrumentationKind::TableInit
            | InstrumentationKind::TableFill
            | InstrumentationKind::TableCopy
            | InstrumentationKind::MemoryInit
            | InstrumentationKind::MemoryFill
            | InstrumentationKind::MemoryCopy,
        ) => {
            let count_idx = local_idx.checked_add(1).ok_or(Error::TooManyLocals)?;
            func.local_tee(count_idx)
                .i64_extend_i32_u()
                // $count
                .i64_const(gas.linear as i64)
                // $count | $linear
                .checked_mul_i64(GAS_EXHAUSTED_FN)
                // $count * $linear
                .i64_const(0)
                .i64_const(gas.constant as i64)
                .i64_const(0)
                // $count * $linear | 0 | $constant | 0
                .checked_add_i64(GAS_EXHAUSTED_FN)
                // $count * $linear + $constant
                .local_tee(local_idx)
                .global_get(globals + GAS_GLOBAL)
                .i64_gt_u()
                // $count * $linear + $constant > $gas
                .if_(we::BlockType::Empty)
                .local_get(local_idx)
                .call(GAS_INSTRUMENTATION_FN)
                .unreachable()
                .else_()
                .global_get(globals + GAS_GLOBAL)
                .local_get(local_idx)
                .i64_sub()
                // $gas - $count * $linear + $constant
                .global_set(globals + GAS_GLOBAL)
                .end()
                // $count
                .local_get(count_idx);
            Ok(())
        }
        _ => {
            panic!(
                "configuration error, linear gas fees are only applicable to aggregate operations"
            );
        }
    }
}

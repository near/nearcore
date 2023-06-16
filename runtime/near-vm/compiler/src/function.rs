// This file contains code from external sources.
// Attributions: https://github.com/wasmerio/wasmer/blob/master/ATTRIBUTIONS.md

//! A `Compilation` contains the compiled function bodies for a WebAssembly
//! module (`CompiledFunction`).

use crate::lib::std::vec::Vec;
use crate::section::{CustomSection, SectionIndex};
use crate::trap::TrapInformation;
use crate::{FunctionAddressMap, JumpTableOffsets, Relocation};
use near_vm_types::entity::PrimaryMap;
use near_vm_types::{FunctionIndex, LocalFunctionIndex, SignatureIndex};

/// The frame info for a Compiled function.
///
/// This structure is only used for reconstructing
/// the frame information after a `Trap`.
#[derive(
    rkyv::Serialize, rkyv::Deserialize, rkyv::Archive, Debug, Clone, PartialEq, Eq, Default,
)]
pub struct CompiledFunctionFrameInfo {
    /// The traps (in the function body).
    ///
    /// Code offsets of the traps MUST be in ascending order.
    pub traps: Vec<TrapInformation>,

    /// The address map.
    pub address_map: FunctionAddressMap,
}

/// The function body.
#[derive(rkyv::Serialize, rkyv::Deserialize, rkyv::Archive, Debug, Clone, PartialEq, Eq)]
pub struct FunctionBody {
    /// The function body bytes.
    pub body: Vec<u8>,
}

/// See [`FunctionBody`].
#[derive(Clone, Copy)]
pub struct FunctionBodyRef<'a> {
    /// Function body bytes.
    pub body: &'a [u8],
}

impl<'a> From<&'a FunctionBody> for FunctionBodyRef<'a> {
    fn from(body: &'a FunctionBody) -> Self {
        FunctionBodyRef { body: &*body.body }
    }
}

impl<'a> From<&'a ArchivedFunctionBody> for FunctionBodyRef<'a> {
    fn from(body: &'a ArchivedFunctionBody) -> Self {
        FunctionBodyRef { body: &*body.body }
    }
}

/// The result of compiling a WebAssembly function.
///
/// This structure only have the compiled information data
/// (function bytecode body, relocations, traps, jump tables
/// and unwind information).
#[derive(rkyv::Serialize, rkyv::Deserialize, rkyv::Archive, Debug, Clone, PartialEq, Eq)]
pub struct CompiledFunction {
    /// The function body.
    pub body: FunctionBody,

    /// The relocations (in the body)
    pub relocations: Vec<Relocation>,

    /// The jump tables offsets (in the body).
    pub jt_offsets: JumpTableOffsets,

    /// The frame information.
    pub frame_info: CompiledFunctionFrameInfo,
}

/// The compiled functions map (index in the Wasm -> function)
pub type Functions = PrimaryMap<LocalFunctionIndex, CompiledFunction>;

/// The custom sections for a Compilation.
pub type CustomSections = PrimaryMap<SectionIndex, CustomSection>;

/// The DWARF information for this Compilation.
///
/// It is used for retrieving the unwind information once an exception
/// happens.
/// In the future this structure may also hold other information useful
/// for debugging.
#[derive(rkyv::Serialize, rkyv::Deserialize, rkyv::Archive, Debug, PartialEq, Eq, Clone)]
pub struct Dwarf {
    /// The section index in the [`Compilation`] that corresponds to the exception frames.
    /// [Learn
    /// more](https://refspecs.linuxfoundation.org/LSB_3.0.0/LSB-PDA/LSB-PDA/ehframechpt.html).
    pub eh_frame: SectionIndex,
}

impl Dwarf {
    /// Creates a `Dwarf` struct with the corresponding indices for its sections
    pub fn new(eh_frame: SectionIndex) -> Self {
        Self { eh_frame }
    }
}

/// Trampolines section used by ARM short jump (26bits)
#[derive(rkyv::Serialize, rkyv::Deserialize, rkyv::Archive, Debug, PartialEq, Eq, Clone)]
pub struct TrampolinesSection {
    /// SectionIndex for the actual Trampolines code
    pub section_index: SectionIndex,
    /// Number of jump slots in the section
    pub slots: usize,
    /// Slot size
    pub size: usize,
}

impl TrampolinesSection {
    /// Creates a `Trampolines` struct with the indice for its section, and number of slots and size of slot
    pub fn new(section_index: SectionIndex, slots: usize, size: usize) -> Self {
        Self { section_index, slots, size }
    }
}

/// The result of compiling a WebAssembly module's functions.
#[derive(Debug, PartialEq, Eq)]
pub struct Compilation {
    /// Compiled code for the function bodies.
    pub functions: Functions,

    /// Custom sections for the module.
    /// It will hold the data, for example, for constants used in a
    /// function, global variables, rodata_64, hot/cold function partitioning, ...
    pub custom_sections: CustomSections,

    /// Trampolines to call a function defined locally in the wasm via a
    /// provided `Vec` of values.
    ///
    /// This allows us to call easily Wasm functions, such as:
    ///
    /// ```ignore
    /// let func = instance.exports.get_function("my_func");
    /// func.call(&[Value::I32(1)]);
    /// ```
    pub function_call_trampolines: PrimaryMap<SignatureIndex, FunctionBody>,

    /// Trampolines to call a dynamic function defined in
    /// a host, from a Wasm module.
    ///
    /// This allows us to create dynamic Wasm functions, such as:
    ///
    /// ```ignore
    /// fn my_func(values: &[Val]) -> Result<Vec<Val>, RuntimeError> {
    ///     // do something
    /// }
    ///
    /// let my_func_type = FunctionType::new(vec![Type::I32], vec![Type::I32]);
    /// let imports = imports!{
    ///     "namespace" => {
    ///         "my_func" => Function::new(&store, my_func_type, my_func),
    ///     }
    /// }
    /// ```
    ///
    /// Note: Dynamic function trampolines are only compiled for imported function types.
    pub dynamic_function_trampolines: PrimaryMap<FunctionIndex, FunctionBody>,

    /// Section ids corresponding to the Dwarf debug info
    pub debug: Option<Dwarf>,

    /// Trampolines for the arch that needs it
    pub trampolines: Option<TrampolinesSection>,
}

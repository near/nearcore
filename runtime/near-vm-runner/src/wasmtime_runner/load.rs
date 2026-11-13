use super::{IntoVMError, PreparedModule, WasmtimeVM, link};
use crate::logic::errors::{FunctionCallError, VMRunnerError};
use crate::{MEMORY_EXPORT, REMAINING_GAS_EXPORT, START_EXPORT};
use object::Endianness;
use object::elf::FileHeader64;
use object::read::elf::{FileHeader, SectionHeader, SectionTable};
use wasmtime::{Linker, Module, ResourcesRequired};

/// The artifact section holding the module metadata that Wasmtime decodes onto the heap
/// when it loads the module.
const WASMTIME_INFO_SECTION: &str = ".wasmtime.info";

/// The artifact section holding the module's data segments. The first instantiation copies them
/// into a memory image that the module keeps until it is dropped.
const WASM_DATA_SECTION: &str = ".rodata.wasm";

/// Heap bytes a loaded module retains per byte of its [`WASMTIME_INFO_SECTION`], covering the
/// worst of the measured module shapes: 48.4x, from function types at the type limit, each with
/// 1,000 `i32` parameters. The heap includes what Wasmtime decodes from the section and what
/// linking builds from it, such as the definitions of imported functions.
/// `loaded_module_weight_covers_the_retained_heap` checks this for each measured shape.
const HEAP_BYTES_PER_INFO_BYTE: u64 = 49;

/// The memory a loaded module holds while it is in the cache. It is the sum of three parts:
///
/// 1. The whole artifact. Wasmtime copies it into memory when it loads the module.
/// 2. [`HEAP_BYTES_PER_INFO_BYTE`] times the length of [`WASMTIME_INFO_SECTION`]. Wasmtime
///    decodes this section into heap objects such as export names and function types, and
///    linking adds the definitions of the module's imports. Both take up many more bytes than
///    their encoded form.
/// 3. The length of [`WASM_DATA_SECTION`]. The first instantiation copies this section into a
///    memory image, which the module keeps until it is dropped.
pub(super) fn loaded_module_weight(serialized: &[u8]) -> u64 {
    let section_lens = SectionLens::parse(serialized)
        .expect("wasmtime parsed the section table of the module it loaded");
    (serialized.len() as u64)
        .saturating_add(section_lens.info.saturating_mul(HEAP_BYTES_PER_INFO_BYTE))
        .saturating_add(section_lens.data)
}

/// Lengths of the artifact sections a loaded module's memory grows with.
pub(super) struct SectionLens {
    pub(super) info: u64,
    pub(super) data: u64,
}

impl SectionLens {
    /// Parses no more of the artifact than Wasmtime does when it loads the module.
    pub(super) fn parse(serialized: &[u8]) -> object::read::Result<Self> {
        let header = FileHeader64::<Endianness>::parse(serialized)?;
        let endian = header.endian()?;
        let sections = header.sections(endian, serialized)?;
        Ok(Self {
            info: section_len(&sections, endian, serialized, WASMTIME_INFO_SECTION)?,
            data: section_len(&sections, endian, serialized, WASM_DATA_SECTION)?,
        })
    }
}

/// Length of the section called `name`, or 0 if the artifact has no such section.
fn section_len(
    sections: &SectionTable<'_, FileHeader64<Endianness>>,
    endian: Endianness,
    serialized: &[u8],
    name: &str,
) -> object::read::Result<u64> {
    let Some((_, section)) = sections.section_by_name(endian, name.as_bytes()) else {
        return Ok(0);
    };
    Ok(section.data(endian, serialized)?.len() as u64)
}

impl WasmtimeVM {
    /// Loads a serialized artifact and links it against the host functions.
    ///
    /// A failure to deserialize is [`FunctionCallError::LoadingError`].
    pub(super) fn load_module(
        &self,
        serialized: &[u8],
    ) -> Result<Result<PreparedModule, FunctionCallError>, VMRunnerError> {
        // (UN-)SAFETY: the `serialized` must have been produced by
        // a prior call to `serialize`.
        //
        // In practice this is not necessarily true. One could have
        // forgotten to change the cache key when upgrading the version of
        // Wasmtime or the database could have had its data
        // corrupted while at rest.
        //
        // There should definitely be some validation in Wasmtime to ensure
        // we load what we think we load.
        let module = match unsafe { Module::deserialize(&self.engine, serialized) } {
            Ok(module) => module,
            Err(err) => return Ok(Err(FunctionCallError::LoadingError { msg: err.to_string() })),
        };
        let Some(memory) = module.get_export_index(MEMORY_EXPORT) else {
            return Ok(Err(FunctionCallError::LinkError { msg: "memory export missing".into() }));
        };
        let remaining_gas = module.get_export_index(REMAINING_GAS_EXPORT);
        let start = module.get_export_index(START_EXPORT);
        let mut linker = Linker::new(&self.engine);
        link(&mut linker, &self.config);
        match linker.instantiate_pre(&module) {
            Err(err) => Ok(Err(err.into_vm_error()?)),
            Ok(pre) => {
                let ResourcesRequired { num_tables, .. } = module.resources_required();
                Ok(Ok(PreparedModule { pre, memory, remaining_gas, start, num_tables }))
            }
        }
    }
}

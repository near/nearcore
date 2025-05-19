use crate::DeserializeError;
use near_vm_compiler::{
    CompileModuleInfo, CompiledFunctionFrameInfo, CustomSection, Dwarf, FunctionBody,
    JumpTableOffsets, Relocation, SectionIndex, TrampolinesSection,
};
use near_vm_types::entity::{EntityRef as _, PrimaryMap};
use near_vm_types::{
    ExportIndex, FunctionIndex, ImportIndex, LocalFunctionIndex, OwnedDataInitializer,
    SignatureIndex,
};
use rkyv::Archived;
use rkyv::tuple::ArchivedTuple3;

const MAGIC_HEADER: [u8; 32] = {
    let value = *b"\0nearvm-universal\xFE\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF";
    let _length_must_be_multiple_of_16: bool = [true][value.len() % 16];
    value
};

/// A 0-copy view of the encoded `UniversalExecutable` payload.
#[derive(Clone, Copy)]
pub struct UniversalExecutableRef<'a> {
    archive: &'a ArchivedUniversalExecutable,
}

impl<'a> std::ops::Deref for UniversalExecutableRef<'a> {
    type Target = ArchivedUniversalExecutable;
    fn deref(&self) -> &Self::Target {
        self.archive
    }
}

impl<'a> UniversalExecutableRef<'a> {
    /// Verify the buffer for whether it is a valid `UniversalExecutable`.
    pub fn verify_serialized(data: &[u8]) -> Result<(), &'static str> {
        if !data.starts_with(&MAGIC_HEADER) {
            return Err("the provided bytes are not nearvm-universal");
        }
        if data.len() < MAGIC_HEADER.len() + 8 {
            return Err("the data buffer is too small to be valid");
        }
        let (remaining, position) = data.split_at(data.len() - 8);
        let mut position_value = [0u8; 8];
        position_value.copy_from_slice(position);
        if u64::from_le_bytes(position_value) > remaining.len() as u64 {
            return Err("the buffer is malformed");
        }
        // TODO(0-copy): bytecheck too.
        Ok(())
    }

    /// # Safety
    ///
    /// This method is unsafe since it deserializes data directly
    /// from memory.
    /// Right now we are not doing any extra work for validation, but
    /// `rkyv` has an option to do bytecheck on the serialized data before
    /// serializing (via `rkyv::check_archived_value`).
    pub unsafe fn deserialize(data: &'a [u8]) -> Result<Self, DeserializeError> {
        Self::verify_serialized(data).map_err(|e| DeserializeError::Incompatible(e.to_string()))?;
        let (archive, position) = data.split_at(data.len() - 8);
        let mut position_value = [0u8; 8];
        position_value.copy_from_slice(position);
        let (_, data) = archive.split_at(MAGIC_HEADER.len());

        Ok(UniversalExecutableRef {
            archive: rkyv::api::high::access_pos(data, u64::from_le_bytes(position_value) as usize)
                .map_err(|e: rkyv::rancor::Error| {
                    DeserializeError::CorruptedBinary(e.to_string())
                })?,
        })
    }

    // TODO(0-copy): this should never fail.
    /// Convert this reference to an owned `UniversalExecutable` value.
    pub fn to_owned(self) -> Result<UniversalExecutable, DeserializeError> {
        rkyv::api::high::deserialize(self.archive)
            .map_err(|e: rkyv::rancor::Error| DeserializeError::CorruptedBinary(format!("{:?}", e)))
    }
}

/// A wasm module compiled to some shape, ready to be loaded with `UniversalEngine` to produce an
/// `UniversalArtifact`.
///
/// This is the result obtained after validating and compiling a WASM module with any of the
/// supported compilers. This type falls in-between a module and [`Artifact`](crate::Artifact).
#[derive(rkyv::Archive, rkyv::Deserialize, rkyv::Serialize)]
pub struct UniversalExecutable {
    pub(crate) function_bodies: PrimaryMap<LocalFunctionIndex, FunctionBody>,
    pub(crate) function_relocations: PrimaryMap<LocalFunctionIndex, Vec<Relocation>>,
    pub(crate) function_jt_offsets: PrimaryMap<LocalFunctionIndex, JumpTableOffsets>,
    pub(crate) function_frame_info: PrimaryMap<LocalFunctionIndex, CompiledFunctionFrameInfo>,
    pub(crate) function_call_trampolines: PrimaryMap<SignatureIndex, FunctionBody>,
    pub(crate) dynamic_function_trampolines: PrimaryMap<FunctionIndex, FunctionBody>,
    pub(crate) custom_sections: PrimaryMap<SectionIndex, CustomSection>,
    pub(crate) custom_section_relocations: PrimaryMap<SectionIndex, Vec<Relocation>>,
    // The section indices corresponding to the Dwarf debug info
    pub(crate) debug: Option<Dwarf>,
    // the Trampoline for Arm arch
    pub(crate) trampolines: Option<TrampolinesSection>,
    pub(crate) compile_info: CompileModuleInfo,
    pub(crate) data_initializers: Vec<OwnedDataInitializer>,
    pub(crate) cpu_features: u64,
}

#[derive(thiserror::Error, Debug)]
pub enum ExecutableSerializeError {
    #[error("could not serialize the executable data")]
    Executable(#[source] rkyv::rancor::Error),
}

impl UniversalExecutable {
    /// Serialize the executable into bytes for storage.
    pub fn serialize(
        &self,
    ) -> Result<Vec<u8>, Box<(dyn std::error::Error + Send + Sync + 'static)>> {
        // The format is as thus:
        //
        // HEADER
        // RKYV PAYLOAD
        // RKYV POSITION
        //
        // It is expected that any framing for message length is handled by the caller.
        let mut output = Vec::new();
        output.extend_from_slice(&MAGIC_HEADER);
        rkyv::api::high::to_bytes_in(self, &mut output)
            .map_err(ExecutableSerializeError::Executable)?;
        let pos =
            rkyv::api::root_position::<Archived<Self>>(output.len() - MAGIC_HEADER.len()) as u64;
        let pos_bytes = pos.to_le_bytes();
        output.extend_from_slice(&pos_bytes);
        Ok(output)
    }
}

impl<'a> UniversalExecutableRef<'a> {
    /// Get the name for specified function index.
    ///
    /// Test-only API
    pub fn function_name(&self, index: FunctionIndex) -> Option<&str> {
        let aidx = Archived::<FunctionIndex>::new(index.index());
        let module = &self.compile_info.module;
        // First, lets see if there's a name by which this function is exported.
        for (name, idx) in module.exports.iter() {
            match idx {
                Archived::<ExportIndex>::Function(fi) if fi == &aidx => return Some(&*name),
                _ => continue,
            }
        }
        if let Some(r) = module.function_names.get_key_value(&aidx) {
            return Some(r.1.as_str());
        }
        for (ArchivedTuple3(_, field, _), idx) in module.imports.iter() {
            match idx {
                Archived::<ImportIndex>::Function(fi) if fi == &aidx => return Some(&*field),
                _ => continue,
            }
        }
        None
    }
}

#[cfg(not(windows))]
pub(crate) fn unrkyv<T>(archive: &T::Archived) -> T
where
    T: rkyv::Archive,
    T::Archived: rkyv::Deserialize<T, rkyv::rancor::Strategy<(), rkyv::rancor::Panic>>,
{
    rkyv::rancor::ResultExt::<_, rkyv::rancor::Panic>::always_ok(rkyv::api::deserialize_using(
        archive,
        &mut (),
    ))
}

use crate::CodeOffset;
use near_vm_vm::TrapCode;

/// Information about trap.
#[derive(rkyv::Serialize, rkyv::Deserialize, rkyv::Archive, Clone, Debug, PartialEq, Eq)]
pub struct TrapInformation {
    /// The offset of the trapping instruction in native code. It is relative to the beginning of the function.
    pub code_offset: CodeOffset,
    /// Code of the trap.
    pub trap_code: TrapCode,
}

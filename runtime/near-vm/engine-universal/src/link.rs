//! Linking for Universal-compiled code.

use near_vm_compiler::{
    JumpTable, Relocation, RelocationKind, RelocationTarget, SectionIndex, TrampolinesSection,
};
use near_vm_types::entity::PrimaryMap;
use near_vm_types::LocalFunctionIndex;
use near_vm_vm::{SectionBodyPtr, VMLocalFunction};
use std::collections::HashMap;
use std::ptr::{read_unaligned, write_unaligned};

/// Add a new trampoline address, given the base adress of the Section. Return the address of the jump
/// The trampoline itself still have to be writen
fn trampolines_add(
    map: &mut HashMap<usize, usize>,
    trampoline: &TrampolinesSection,
    address: usize,
    baseaddress: usize,
) -> usize {
    if let Some(target) = map.get(&address) {
        return *target;
    }
    let ret = map.len();
    if ret == trampoline.slots {
        panic!("No more slot in Trampolines");
    }
    map.insert(address, baseaddress + ret * trampoline.size);
    baseaddress + ret * trampoline.size
}

fn use_trampoline(
    address: usize,
    allocated_sections: &PrimaryMap<SectionIndex, SectionBodyPtr>,
    trampolines: &Option<TrampolinesSection>,
    map: &mut HashMap<usize, usize>,
) -> Option<usize> {
    match trampolines {
        Some(trampolines) => Some(trampolines_add(
            map,
            trampolines,
            address,
            *allocated_sections[trampolines.section_index] as usize,
        )),
        _ => None,
    }
}

fn fill_trampoline_map(
    allocated_sections: &PrimaryMap<SectionIndex, SectionBodyPtr>,
    trampolines: &Option<TrampolinesSection>,
) -> HashMap<usize, usize> {
    let mut map: HashMap<usize, usize> = HashMap::new();
    match trampolines {
        Some(trampolines) => {
            let baseaddress = *allocated_sections[trampolines.section_index] as usize;
            for i in 0..trampolines.size {
                let jmpslot: usize = unsafe {
                    read_unaligned((baseaddress + i * trampolines.size + 8) as *mut usize)
                };
                if jmpslot != 0 {
                    map.insert(jmpslot, baseaddress + i * trampolines.size);
                }
            }
        }
        _ => {}
    };
    map
}

fn apply_relocation(
    body: usize,
    r: &Relocation,
    allocated_functions: &PrimaryMap<LocalFunctionIndex, VMLocalFunction>,
    jt_offsets: impl Fn(LocalFunctionIndex, JumpTable) -> near_vm_compiler::CodeOffset,
    allocated_sections: &PrimaryMap<SectionIndex, SectionBodyPtr>,
    trampolines: &Option<TrampolinesSection>,
    trampolines_map: &mut HashMap<usize, usize>,
) {
    let target_func_address: usize = match r.reloc_target {
        RelocationTarget::LocalFunc(index) => *allocated_functions[index].body as usize,
        RelocationTarget::LibCall(libcall) => libcall.function_pointer(),
        RelocationTarget::CustomSection(custom_section) => {
            *allocated_sections[custom_section] as usize
        }
        RelocationTarget::JumpTable(func_index, jt) => {
            let offset = jt_offsets(func_index, jt);
            *allocated_functions[func_index].body as usize + offset as usize
        }
    };

    match r.kind {
        #[cfg(target_pointer_width = "64")]
        RelocationKind::Abs8 => unsafe {
            let (reloc_address, reloc_delta) = r.for_address(body, target_func_address as u64);
            write_unaligned(reloc_address as *mut u64, reloc_delta);
        },
        #[cfg(target_pointer_width = "32")]
        RelocationKind::X86PCRel4 => unsafe {
            let (reloc_address, reloc_delta) = r.for_address(body, target_func_address as u64);
            write_unaligned(reloc_address as *mut u32, reloc_delta as _);
        },
        #[cfg(target_pointer_width = "64")]
        RelocationKind::X86PCRel8 => unsafe {
            let (reloc_address, reloc_delta) = r.for_address(body, target_func_address as u64);
            write_unaligned(reloc_address as *mut u64, reloc_delta);
        },
        RelocationKind::X86CallPCRel4 => unsafe {
            let (reloc_address, reloc_delta) = r.for_address(body, target_func_address as u64);
            write_unaligned(reloc_address as *mut u32, reloc_delta as _);
        },
        RelocationKind::X86PCRelRodata4 => {}
        RelocationKind::Arm64Call => unsafe {
            let (reloc_address, mut reloc_delta) = r.for_address(body, target_func_address as u64);
            if (reloc_delta as i64).abs() >= 0x1000_0000 {
                let new_address = match use_trampoline(
                    target_func_address,
                    allocated_sections,
                    trampolines,
                    trampolines_map,
                ) {
                    Some(new_address) => new_address,
                    _ => panic!(
                        "Relocation to big for {:?} for {:?} with {:x}, current val {:x}",
                        r.kind,
                        r.reloc_target,
                        reloc_delta,
                        read_unaligned(reloc_address as *mut u32)
                    ),
                };
                write_unaligned((new_address + 8) as *mut u64, target_func_address as u64); // write the jump address
                let (_, new_delta) = r.for_address(body, new_address as u64);
                reloc_delta = new_delta;
            }
            let reloc_delta = (((reloc_delta / 4) as u32) & 0x3ff_ffff)
                | read_unaligned(reloc_address as *mut u32);
            write_unaligned(reloc_address as *mut u32, reloc_delta);
        },
        RelocationKind::Arm64Movw0 => unsafe {
            let (reloc_address, reloc_delta) = r.for_address(body, target_func_address as u64);
            let reloc_delta =
                (((reloc_delta & 0xffff) as u32) << 5) | read_unaligned(reloc_address as *mut u32);
            write_unaligned(reloc_address as *mut u32, reloc_delta);
        },
        RelocationKind::Arm64Movw1 => unsafe {
            let (reloc_address, reloc_delta) = r.for_address(body, target_func_address as u64);
            let reloc_delta = ((((reloc_delta >> 16) & 0xffff) as u32) << 5)
                | read_unaligned(reloc_address as *mut u32);
            write_unaligned(reloc_address as *mut u32, reloc_delta);
        },
        RelocationKind::Arm64Movw2 => unsafe {
            let (reloc_address, reloc_delta) = r.for_address(body, target_func_address as u64);
            let reloc_delta = ((((reloc_delta >> 32) & 0xffff) as u32) << 5)
                | read_unaligned(reloc_address as *mut u32);
            write_unaligned(reloc_address as *mut u32, reloc_delta);
        },
        RelocationKind::Arm64Movw3 => unsafe {
            let (reloc_address, reloc_delta) = r.for_address(body, target_func_address as u64);
            let reloc_delta = ((((reloc_delta >> 48) & 0xffff) as u32) << 5)
                | read_unaligned(reloc_address as *mut u32);
            write_unaligned(reloc_address as *mut u32, reloc_delta);
        },
        kind => panic!("Relocation kind unsupported in the current architecture {}", kind),
    }
}

/// Links a module, patching the allocated functions with the
/// required relocations and jump tables.
#[tracing::instrument(skip_all)]
pub fn link_module(
    allocated_functions: &PrimaryMap<LocalFunctionIndex, VMLocalFunction>,
    jt_offsets: impl Fn(LocalFunctionIndex, JumpTable) -> near_vm_compiler::CodeOffset,
    function_relocations: impl Iterator<Item = (LocalFunctionIndex, impl Iterator<Item = Relocation>)>,
    allocated_sections: &PrimaryMap<SectionIndex, SectionBodyPtr>,
    section_relocations: impl Iterator<Item = (SectionIndex, impl Iterator<Item = Relocation>)>,
    trampolines: &Option<TrampolinesSection>,
) {
    let mut trampolines_map = fill_trampoline_map(allocated_sections, trampolines);
    for (i, section_relocs) in section_relocations {
        let body = *allocated_sections[i] as usize;
        for r in section_relocs {
            apply_relocation(
                body,
                &r,
                allocated_functions,
                &jt_offsets,
                allocated_sections,
                trampolines,
                &mut trampolines_map,
            );
        }
    }
    for (i, function_relocs) in function_relocations {
        let body = *allocated_functions[i].body as usize;
        for r in function_relocs {
            apply_relocation(
                body,
                &r,
                allocated_functions,
                &jt_offsets,
                allocated_sections,
                trampolines,
                &mut trampolines_map,
            );
        }
    }
}

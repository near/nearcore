use std::fs;

extern crate parity_wasm;
use parity_wasm::{builder, elements};

fn memory_section(module: &mut elements::Module) -> Option<&mut elements::MemorySection> {
    for section in module.sections_mut() {
        if let elements::Section::Memory(ref mut sect) = *section {
            return Some(sect);
        }
    }
    None
}

fn externalize_mem(
    mut module: elements::Module,
    adjust_pages: Option<u32>,
    max_pages: u32,
) -> elements::Module {
    let mut entry = memory_section(&mut module)
        .expect("Memory section to exist")
        .entries_mut()
        .pop()
        .expect("Own memory entry to exist in memory section");

    if let Some(adjust_pages) = adjust_pages {
        assert!(adjust_pages <= max_pages);
        entry = elements::MemoryType::new(adjust_pages, Some(max_pages));
    }

    if entry.limits().maximum().is_none() {
        entry = elements::MemoryType::new(entry.limits().initial(), Some(max_pages));
    }

    let mut builder = builder::from_module(module);
    builder.push_import(elements::ImportEntry::new(
        "env".to_owned(),
        "memory".to_owned(),
        elements::External::Memory(entry),
    ));

    builder.build()
}

fn main() {
    let wasm_binary = fs::read("../to_wasm.wasm").expect("Unable to read file");

    // Load wasm binary and prepare it for instantiation.
    let elements_module = parity_wasm::elements::deserialize_buffer(wasm_binary.as_ref())
        .expect("deserialize failed");

    let module_with_mem = externalize_mem(elements_module, None, 16);

    parity_wasm::elements::serialize_to_file("../../res/wasm_with_mem.wasm", module_with_mem)
        .expect("Can't write the file")
}

use near_vm_logic::MemoryLike;
use wasmer_runtime::units::Pages;
use wasmer_runtime::wasm::MemoryDescriptor;
use wasmer_runtime::Memory;

pub struct WasmerMemory(Memory);

impl WasmerMemory {
    pub fn new(initial_memory_pages: u32, max_memory_pages: u32) -> Self {
        WasmerMemory(
            Memory::new(
                MemoryDescriptor::new(
                    Pages(initial_memory_pages),
                    Some(Pages(max_memory_pages)),
                    false,
                )
                .unwrap(),
            )
            .expect("TODO creating memory cannot fail"),
        )
    }

    pub fn clone(&self) -> Memory {
        self.0.clone()
    }
}

impl WasmerMemory {
    fn with_memory<F>(&self, offset: u64, len: usize, func: F) -> Result<(), ()>
    where
        F: FnOnce(core::slice::Iter<'_, std::cell::Cell<u8>>),
    {
        let start = usize::try_from(offset).map_err(|_| ())?;
        let end = start.checked_add(len).ok_or(())?;
        self.0.view().get(start..end).map(|mem| func(mem.iter())).ok_or(())
    }
}

impl MemoryLike for WasmerMemory {
    fn fits_memory(&self, offset: u64, len: u64) -> Result<(), ()> {
        self.with_memory(offset, usize::try_from(len).map_err(|_| ())?, |_| ())
    }

    fn read_memory(&self, offset: u64, buffer: &mut [u8]) -> Result<(), ()> {
        self.with_memory(offset, buffer.len(), |mem| {
            buffer.iter_mut().zip(mem).for_each(|(dst, src)| *dst = src.get());
        })
    }

    fn write_memory(&mut self, offset: u64, buffer: &[u8]) -> Result<(), ()> {
        self.with_memory(offset, buffer.len(), |mem| {
            mem.zip(buffer.iter()).for_each(|(dst, src)| dst.set(*src));
        })
    }
}

#[test]
fn test_memory_like() {
    crate::tests::test_memory_like(|| Box::new(WasmerMemory::new(1, 1)));
}

use near_vm_logic::MemoryLike;
use wasmer_runtime::units::{Bytes, Pages};
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
        let start = usize::try_from(offset).unwrap();
        if let Some(end) = start.checked_add(len) {
            if Bytes(end) <= self.0.size().bytes() {
                func(self.0.view()[start..end].iter());
                return Ok(());
            }
        }
        Err(())
    }
}

impl MemoryLike for WasmerMemory {
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

#[cfg(test)]
mod tests {
    use near_vm_logic::MemoryLike;

    use wasmer_types::WASM_PAGE_SIZE;

    #[test]
    fn memory_read() {
        let memory = super::WasmerMemory::new(1, 1);
        let mut buffer = vec![42; WASM_PAGE_SIZE];
        memory.read_memory(0, &mut buffer).unwrap();
        // memory should be zeroed at creation.
        assert!(buffer.iter().all(|&v| v == 0));
    }

    #[test]
    fn memory_read_oob() {
        let memory = super::WasmerMemory::new(1, 1);
        let mut buffer = vec![42; WASM_PAGE_SIZE + 1];
        assert!(memory.read_memory(0, &mut buffer).is_err());
    }

    #[test]
    fn memory_write() {
        let mut memory = super::WasmerMemory::new(1, 1);
        let mut buffer = vec![42; WASM_PAGE_SIZE];
        memory.write_memory(WASM_PAGE_SIZE as u64 / 2, &buffer[..WASM_PAGE_SIZE / 2]).unwrap();
        memory.read_memory(0, &mut buffer).unwrap();
        assert!(buffer[..WASM_PAGE_SIZE / 2].iter().all(|&v| v == 0));
        assert!(buffer[WASM_PAGE_SIZE / 2..].iter().all(|&v| v == 42));
        // Now the buffer is half 0s and half 42s

        memory.write_memory(0, &buffer[WASM_PAGE_SIZE / 4..3 * (WASM_PAGE_SIZE / 4)]).unwrap();
        memory.read_memory(0, &mut buffer).unwrap();
        assert!(buffer[..WASM_PAGE_SIZE / 4].iter().all(|&v| v == 0));
        assert!(buffer[WASM_PAGE_SIZE / 4..].iter().all(|&v| v == 42));
    }

    #[test]
    fn memory_write_oob() {
        let mut memory = super::WasmerMemory::new(1, 1);
        let mut buffer = vec![42; WASM_PAGE_SIZE + 1];
        assert!(memory.write_memory(0, &mut buffer).is_err());
    }
}

use super::{MemSlice, MemoryLike};

/// Tests for implementation of MemoryLike interface.
///
/// The `factory` returns a [`MemoryLike`] implementation to be tested.  The
/// memory must be configured with 64 KiB (i.e. single WASM page) of memory
/// available.
///
/// Panics if any of the tests fails.
pub fn test_memory_like(factory: impl FnOnce() -> Box<dyn MemoryLike>) {
    const PAGE: u64 = 0x10000;

    struct TestContext {
        mem: Box<dyn MemoryLike>,
        buf: [u8; PAGE as usize + 1],
    }

    impl TestContext {
        fn test_read(&mut self, ptr: u64, len: u64, value: u8) {
            self.buf.fill(!value);
            self.mem.fits_memory(MemSlice { ptr, len }).unwrap();
            self.mem.read_memory(ptr, &mut self.buf[..(len as usize)]).unwrap();
            assert!(self.buf[..(len as usize)].iter().all(|&v| v == value));
        }

        fn test_write(&mut self, ptr: u64, len: u64, value: u8) {
            self.buf.fill(value);
            self.mem.fits_memory(MemSlice { ptr, len }).unwrap();
            self.mem.write_memory(ptr, &self.buf[..(len as usize)]).unwrap();
        }

        fn test_oob(&mut self, ptr: u64, len: u64) {
            self.buf.fill(42);
            self.mem.fits_memory(MemSlice { ptr, len }).unwrap_err();
            self.mem.read_memory(ptr, &mut self.buf[..(len as usize)]).unwrap_err();
            assert!(self.buf[..(len as usize)].iter().all(|&v| v == 42));
            self.mem.write_memory(ptr, &self.buf[..(len as usize)]).unwrap_err();
        }
    }

    let mut ctx = TestContext { mem: factory(), buf: [0; PAGE as usize + 1] };

    // Test memory is initialised to zero.
    ctx.test_read(0, PAGE, 0);
    ctx.test_read(PAGE, 0, 0);
    ctx.test_read(0, PAGE / 2, 0);
    ctx.test_read(PAGE / 2, PAGE / 2, 0);

    // Test writing works.
    ctx.test_write(0, PAGE / 2, 42);
    ctx.test_read(0, PAGE / 2, 42);
    ctx.test_read(PAGE / 2, PAGE / 2, 0);

    ctx.test_write(PAGE / 4, PAGE / 4, 24);
    ctx.test_read(0, PAGE / 4, 42);
    ctx.test_read(PAGE / 4, PAGE / 4, 24);
    ctx.test_read(PAGE / 2, PAGE / 2, 0);

    // Zero memory.
    ctx.test_write(0, PAGE, 0);
    ctx.test_read(0, PAGE, 0);

    // Test out-of-bounds checks.
    ctx.test_oob(0, PAGE + 1);
    ctx.test_oob(1, PAGE);
    ctx.test_oob(PAGE - 1, 2);
    ctx.test_oob(PAGE, 1);

    // None of the writes in OOB should have any effect.
    ctx.test_read(0, PAGE, 0);
}

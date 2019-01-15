use crate::types::PrepareError as Error;
use wasmi::memory_units::{Bytes, Pages};
use wasmi::{MemoryInstance, MemoryRef};

#[derive(Clone)]
pub struct Memory {
    pub memref: MemoryRef,
}

#[allow(unused)]
impl Memory {
    pub fn init(initial: u32, maximum: Option<u32>) -> Result<Memory, Error> {
        Ok(Memory {
            memref: MemoryInstance::alloc(
                Pages(initial as usize),
                maximum.map(|m| Pages(m as usize)),
            ).map_err(|_| Error::Memory)?,
        })
    }

    pub fn get_into(&self, ptr: u32, buf: &mut [u8]) -> Result<(), Error> {
        self.memref.get_into(ptr, buf).map_err(|_| Error::Memory)?;
        Ok(())
    }

    pub fn get(&self, ptr: u32, len: usize) -> Result<Vec<u8>, Error> {
        Ok(self.memref.get(ptr, len).map_err(|_| Error::Memory)?)
    }

    pub fn get_u32(&self, ptr: u32) -> Result<u32, Error> {
        Ok(self.memref.get_value(ptr).map_err(|_| Error::Memory)?)
    }

    pub fn get_i32(&self, ptr: u32) -> Result<i32, Error> {
        Ok(self.memref.get_value(ptr).map_err(|_| Error::Memory)?)
    }

    pub fn set(&self, ptr: u32, value: &[u8]) -> Result<(), Error> {
        self.memref.set(ptr, value).map_err(|_| Error::Memory)?;
        Ok(())
    }

    pub fn can_fit(&self, ptr: usize, len: usize) -> bool {
        if let Some(size) = ptr.checked_add(len) {
            Bytes(ptr) <= self.memref.current_size().into()
        } else {
            false
        }
    }
}

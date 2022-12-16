use crate::dependencies::MemoryLike;
use crate::gas_counter::GasCounter;

use near_primitives_core::config::ExtCosts::*;
use near_primitives_core::config::VMLimitConfig;
use near_vm_errors::{HostError, VMLogicError};

use core::mem::size_of;

type Result<T> = ::std::result::Result<T, VMLogicError>;

/// Guest memory.
///
/// Provides interface to access the guest memory while correctly accounting for
/// gas usage.
///
/// Really the main point of this struct is that it is a separate object so when
/// its methods are called, such as `memory.get_into(&mut gas_counter, ...)`,
/// the compiler can deconstruct the access to each field of [`VMLogic`] and do
/// more granular lifetime analysis.  In particular, this design is what allows
/// us to forgo copying register value in [`VMLogic::read_register`].
pub(super) struct Memory<'a>(&'a mut dyn MemoryLike);

macro_rules! memory_get {
    ($_type:ty, $name:ident) => {
        pub(super) fn $name(
            &mut self,
            gas_counter: &mut GasCounter,
            offset: u64,
        ) -> Result<$_type> {
            let mut array = [0u8; size_of::<$_type>()];
            self.get_into(gas_counter, offset, &mut array)?;
            Ok(<$_type>::from_le_bytes(array))
        }
    };
}

macro_rules! memory_set {
    ($_type:ty, $name:ident) => {
        pub(super) fn $name(
            &mut self,
            gas_counter: &mut GasCounter,
            offset: u64,
            value: $_type,
        ) -> Result<()> {
            self.set(gas_counter, offset, &value.to_le_bytes())
        }
    };
}

impl<'a> Memory<'a> {
    pub(super) fn new(mem: &'a mut dyn MemoryLike) -> Self {
        Self(mem)
    }

    /// Copies data from guest memory into provided buffer accounting for gas.
    fn get_into(&self, gas_counter: &mut GasCounter, offset: u64, buf: &mut [u8]) -> Result<()> {
        gas_counter.pay_base(read_memory_base)?;
        let len = u64::try_from(buf.len()).map_err(|_| HostError::MemoryAccessViolation)?;
        gas_counter.pay_per(read_memory_byte, len)?;
        self.0.read_memory(offset, buf).map_err(|_| HostError::MemoryAccessViolation.into())
    }

    /// Copies data from guest memory into a newly allocated vector accounting for gas.
    pub(super) fn get_vec(
        &self,
        gas_counter: &mut GasCounter,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>> {
        gas_counter.pay_base(read_memory_base)?;
        gas_counter.pay_per(read_memory_byte, len)?;
        self.get_vec_for_free(offset, len)
    }

    /// Like [`Self::get_vec`] but does not pay gas fees.
    pub(super) fn get_vec_for_free(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        // This check is redundant in the sense that read_memory will perform it
        // as well however it’s here to validate that `len` is a valid value.
        // See documentation of MemoryLike::read_memory for more information.
        self.0.fits_memory(offset, len).map_err(|_| HostError::MemoryAccessViolation)?;
        let mut buf = vec![0; len as usize];
        self.0.read_memory(offset, &mut buf).map_err(|_| HostError::MemoryAccessViolation)?;
        Ok(buf)
    }

    /// Copies data from provided buffer into guest memory accounting for gas.
    pub(super) fn set(
        &mut self,
        gas_counter: &mut GasCounter,
        offset: u64,
        buf: &[u8],
    ) -> Result<()> {
        gas_counter.pay_base(write_memory_base)?;
        gas_counter.pay_per(write_memory_byte, buf.len() as _)?;
        self.0.write_memory(offset, buf).map_err(|_| HostError::MemoryAccessViolation.into())
    }

    memory_get!(u128, get_u128);
    memory_get!(u32, get_u32);
    memory_get!(u16, get_u16);
    memory_get!(u8, get_u8);
    memory_set!(u128, set_u128);
}

/// Registers to use by the guest.
///
/// Provides interface to access registers while correctly accounting for gas
/// usage.
///
/// See documentation of [`Memory`] for more motivation for this struct.
#[derive(Default, Clone)]
pub(super) struct Registers(std::collections::HashMap<u64, Box<[u8]>>);

impl Registers {
    /// Returns register with given index.
    ///
    /// Returns an error if (i) there’s not enough gas to perform the register
    /// read or (ii) register with given index doesn’t exist.
    pub(super) fn get<'s>(
        &'s self,
        gas_counter: &mut GasCounter,
        register_id: u64,
    ) -> Result<&'s [u8]> {
        if let Some(data) = self.0.get(&register_id) {
            gas_counter.pay_base(read_register_base)?;
            let len = u64::try_from(data.len()).map_err(|_| HostError::MemoryAccessViolation)?;
            gas_counter.pay_per(read_register_byte, len)?;
            Ok(&data[..])
        } else {
            Err(HostError::InvalidRegisterId { register_id }.into())
        }
    }

    /// Returns length of register with given index or None if no such register.
    pub(super) fn get_len(&self, register_id: u64) -> Option<u64> {
        self.0.get(&register_id).map(|data| data.len() as u64)
    }

    /// Sets register with given index.
    ///
    /// Returns an error if (i) there’s not enough gas to perform the register
    /// write or (ii) if setting the register would violate configured limits.
    pub(super) fn set<T>(
        &mut self,
        gas_counter: &mut GasCounter,
        config: &VMLimitConfig,
        register_id: u64,
        data: T,
    ) -> Result<()>
    where
        T: Into<Box<[u8]>> + AsRef<[u8]>,
    {
        let data_len =
            u64::try_from(data.as_ref().len()).map_err(|_| HostError::MemoryAccessViolation)?;
        gas_counter.pay_base(write_register_base)?;
        gas_counter.pay_per(write_register_byte, data_len)?;
        // Fun fact: if we are at the limit and we replace a register, we’ll
        // fail even though we should be succeeding.  This bug is now part of
        // the protocol so we can’t change it.
        if data_len > config.max_register_size || self.0.len() as u64 >= config.max_number_registers
        {
            return Err(HostError::MemoryAccessViolation.into());
        }
        match self.0.insert(register_id, data.into()) {
            Some(old_value) if old_value.len() as u64 >= data_len => {
                // If there was old value and it was no shorter than the new
                // one, there’s no need to check new memory usage since it
                // didn’t increase.
            }
            _ => {
                // Calculate and check the new memory usage.
                // TODO(mina86): Memorise usage in a field so we don’t have to
                // go through all registers each time.
                let usage: usize = self.0.values().map(|v| size_of::<u64>() + v.len()).sum();
                if usage as u64 > config.registers_memory_limit {
                    return Err(HostError::MemoryAccessViolation.into());
                }
            }
        }
        Ok(())
    }
}

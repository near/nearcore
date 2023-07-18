use super::dependencies::{MemSlice, MemoryLike};
use super::gas_counter::GasCounter;

use super::errors::{HostError, VMLogicError};
use near_primitives_core::config::ExtCosts::*;
use near_primitives_core::config::VMLimitConfig;

use core::mem::size_of;
use std::borrow::Cow;
use std::collections::hash_map::Entry;

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

    /// Returns view of the guest memory.
    ///
    /// Not all runtimes support returning a view to the guest memory so this
    /// may return an owned vector.
    pub(super) fn view<'s>(
        &'s self,
        gas_counter: &mut GasCounter,
        slice: MemSlice,
    ) -> Result<Cow<'s, [u8]>> {
        gas_counter.pay_base(read_memory_base)?;
        gas_counter.pay_per(read_memory_byte, slice.len)?;
        self.0.view_memory(slice).map_err(|_| HostError::MemoryAccessViolation.into())
    }

    /// Like [`Self::view`] but does not pay gas fees.
    pub(super) fn view_for_free(&self, slice: MemSlice) -> Result<Cow<[u8]>> {
        self.0.view_memory(slice).map_err(|_| HostError::MemoryAccessViolation.into())
    }

    /// Copies data from guest memory into provided buffer accounting for gas.
    fn get_into(&self, gas_counter: &mut GasCounter, offset: u64, buf: &mut [u8]) -> Result<()> {
        gas_counter.pay_base(read_memory_base)?;
        let len = u64::try_from(buf.len()).map_err(|_| HostError::MemoryAccessViolation)?;
        gas_counter.pay_per(read_memory_byte, len)?;
        self.0.read_memory(offset, buf).map_err(|_| HostError::MemoryAccessViolation.into())
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

    #[cfg(test)]
    pub(super) fn set_for_free(&mut self, offset: u64, buf: &[u8]) -> Result<()> {
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
pub(super) struct Registers {
    /// Values of each existing register.
    registers: std::collections::HashMap<u64, Box<[u8]>>,

    /// Total memory usage as counted for the purposes of the contract
    /// execution.
    ///
    /// Usage of each register is counted as its value’s length plus eight
    /// (i.e. size of `u64`).  Total usage is sum over all registers.  This only
    /// approximates actual usage in memory.
    total_memory_usage: u64,
}

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
        if let Some(data) = self.registers.get(&register_id) {
            gas_counter.pay_base(read_register_base)?;
            let len = u64::try_from(data.len()).map_err(|_| HostError::MemoryAccessViolation)?;
            gas_counter.pay_per(read_register_byte, len)?;
            Ok(&data[..])
        } else {
            Err(HostError::InvalidRegisterId { register_id }.into())
        }
    }

    #[cfg(test)]
    pub(super) fn get_for_free<'s>(&'s self, register_id: u64) -> Option<&'s [u8]> {
        self.registers.get(&register_id).map(|data| &data[..])
    }

    /// Returns length of register with given index or None if no such register.
    pub(super) fn get_len(&self, register_id: u64) -> Option<u64> {
        self.registers.get(&register_id).map(|data| data.len() as u64)
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
        let entry = self.check_set_register(config, register_id, data_len)?;
        let data = data.into();
        match entry {
            Entry::Occupied(mut entry) => {
                entry.insert(data);
            }
            Entry::Vacant(entry) => {
                entry.insert(data);
            }
        };
        Ok(())
    }

    /// Checks and updates registers usage limits before setting given register
    /// to value with given length.
    ///
    /// On success, returns Entry which must be used to insert the new value
    /// into the registers.
    fn check_set_register<'a>(
        &'a mut self,
        config: &VMLimitConfig,
        register_id: u64,
        data_len: u64,
    ) -> Result<Entry<'a, u64, Box<[u8]>>> {
        if data_len > config.max_register_size {
            return Err(HostError::MemoryAccessViolation.into());
        }
        // Fun fact: if we are at the limit and we replace a register, we’ll
        // fail even though we should be succeeding.  This bug is now part of
        // the protocol so we can’t change it.
        if self.registers.len() as u64 >= config.max_number_registers {
            return Err(HostError::MemoryAccessViolation.into());
        }

        let entry = self.registers.entry(register_id);
        let calc_usage = |len: u64| len + size_of::<u64>() as u64;
        let old_mem_usage = match &entry {
            Entry::Occupied(entry) => calc_usage(entry.get().len() as u64),
            Entry::Vacant(_) => 0,
        };
        let usage = self
            .total_memory_usage
            .checked_sub(old_mem_usage)
            .unwrap()
            .checked_add(calc_usage(data_len))
            .ok_or(HostError::MemoryAccessViolation)?;
        if usage > config.registers_memory_limit {
            return Err(HostError::MemoryAccessViolation.into());
        }
        self.total_memory_usage = usage;
        Ok(entry)
    }
}

/// Reads data from guest memory or register.
///
/// If `len` is `u64::MAX` read register with index `ptr`.  Otherwise, reads
/// `len` bytes of guest memory starting at given offset.  Returns error if
/// there’s insufficient gas, memory interval is out of bounds or given register
/// isn’t set.
///
/// This is not a method on `VMLogic` so that the compiler can track borrowing
/// of gas counter, memory and registers separately.  This allows `VMLogic` to
/// borrow value from a register and then continue constructing mutable
/// references to other fields in the structure..
pub(super) fn get_memory_or_register<'a, 'b>(
    gas_counter: &mut GasCounter,
    memory: &'b Memory<'a>,
    registers: &'b Registers,
    ptr: u64,
    len: u64,
) -> Result<Cow<'b, [u8]>> {
    if len == u64::MAX {
        registers.get(gas_counter, ptr).map(Cow::Borrowed)
    } else {
        memory.view(gas_counter, MemSlice { ptr, len })
    }
}

#[cfg(test)]
mod tests {
    use super::Registers;

    use crate::logic::gas_counter::GasCounter;

    use super::HostError;
    use near_primitives_core::config::{ExtCostsConfig, VMLimitConfig};

    struct RegistersTestContext {
        gas: GasCounter,
        cfg: VMLimitConfig,
        regs: Registers,
    }

    impl RegistersTestContext {
        fn new() -> Self {
            let costs = ExtCostsConfig::test();
            Self {
                gas: GasCounter::new(costs, u64::MAX, 0, u64::MAX, false),
                cfg: VMLimitConfig::test(),
                regs: Default::default(),
            }
        }

        #[track_caller]
        fn assert_set_success(&mut self, register_id: u64, value: &str) {
            self.regs.set(&mut self.gas, &self.cfg, register_id, value.as_bytes()).unwrap();
            self.assert_read(register_id, Some(value));
        }

        #[track_caller]
        fn assert_set_failure(&mut self, register_id: u64, value: &str) {
            let want = Err(HostError::MemoryAccessViolation.into());
            let got = self.regs.set(&mut self.gas, &self.cfg, register_id, value.as_bytes());
            assert_eq!(want, got);
        }

        #[track_caller]
        fn assert_read(&mut self, register_id: u64, value: Option<&str>) {
            if let Some(value) = value {
                assert_eq!(Ok(value.as_bytes()), self.regs.get(&mut self.gas, register_id));
                assert_eq!(Some(value.len() as u64), self.regs.get_len(register_id));
            } else {
                let err = HostError::InvalidRegisterId { register_id }.into();
                assert_eq!(Err(err), self.regs.get(&mut self.gas, register_id));
                assert_eq!(None, self.regs.get_len(register_id));
            }
        }

        #[track_caller]
        fn assert_used_gas(&self, gas: u64) {
            assert_eq!((gas, gas), (self.gas.burnt_gas(), self.gas.used_gas()));
        }
    }

    /// Tests basic setting and reading of registers.
    #[test]
    fn registers_set() {
        let mut ctx = RegistersTestContext::new();
        ctx.assert_read(42, None);
        ctx.assert_read(24, None);
        ctx.assert_set_success(42, "foo");
        ctx.assert_read(24, None);
        ctx.assert_used_gas(5394388050);
    }

    /// Tests limit on number of registers.
    #[test]
    fn registers_max_number_limit() {
        let mut ctx = RegistersTestContext::new();
        ctx.cfg.max_number_registers = 2;

        ctx.assert_set_success(42, "foo");
        ctx.assert_set_success(24, "bar");

        // max_number_registers is 2 so cannot set third register
        ctx.assert_set_failure(12, "baz");

        // Due to historical bug, changing a register is not possible either
        // once limit is reached:
        ctx.assert_set_failure(42, "O_o");
        ctx.assert_set_failure(24, "O_o");

        ctx.assert_used_gas(19419557634);
    }

    /// Tests limit on a size of a single register.
    #[test]
    fn registers_register_size_limit() {
        let mut ctx = RegistersTestContext::new();
        ctx.cfg.max_register_size = 3;
        ctx.assert_set_success(42, "foo");
        ctx.assert_set_failure(24, "quux");
        ctx.assert_used_gas(8275116792);
    }

    /// Tests limit on total memory usage.
    #[test]
    fn registers_usage_limit() {
        let mut ctx = RegistersTestContext::new();
        ctx.cfg.registers_memory_limit = 11;
        ctx.assert_set_success(42, "foo");
        // Replacing value is fine.
        ctx.assert_set_success(42, "bar");
        ctx.assert_set_success(42, "");
        ctx.assert_set_success(42, "baz");
        // But three bytes is a limit (usage is sizeof(u64) + data.len()).
        ctx.assert_set_failure(42, "quux");
        ctx.assert_used_gas(24446580564);
    }
}

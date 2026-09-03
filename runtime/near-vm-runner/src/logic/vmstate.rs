// cspell:ignore regs

use super::errors::{HostError, VMLogicError};
use super::gas_counter::GasCounter;
use core::mem::size_of;
use near_parameters::ExtCosts::*;
use near_parameters::vm::LimitConfig;
use std::collections::hash_map::Entry;
use std::rc::Rc;

type Result<T> = ::std::result::Result<T, VMLogicError>;

/// Registers to use by the guest.
///
/// Provides interface to access registers while correctly accounting for gas
/// usage.
///
/// Being a separate object lets the compiler track borrows of the registers
/// independently from the gas counter and the guest memory.
#[derive(Default, Clone)]
pub(crate) struct Registers {
    /// Values of each existing register.
    registers: std::collections::HashMap<u64, Rc<[u8]>>,

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
    pub(crate) fn get<'s>(
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
    pub(crate) fn get_len(&self, register_id: u64) -> Option<u64> {
        self.registers.get(&register_id).map(|data| data.len() as u64)
    }

    /// Sets register with given index.
    ///
    /// Returns an error if (i) there’s not enough gas to perform the register
    /// write or (ii) if setting the register would violate configured limits.
    pub(crate) fn set<T>(
        &mut self,
        gas_counter: &mut GasCounter,
        config: &LimitConfig,
        register_id: u64,
        data: T,
    ) -> Result<()>
    where
        T: Into<Rc<[u8]>> + AsRef<[u8]>,
    {
        self.set_impl(gas_counter, config, register_id, data, true)
    }

    /// Sets register with given index from an existing `Rc<[u8]>` without
    /// charging per-byte gas (the data is shared, not copied).
    ///
    /// Returns an error if (i) there’s not enough gas to perform the register
    /// write or (ii) if setting the register would violate configured limits.
    pub(crate) fn set_rc_data(
        &mut self,
        gas_counter: &mut GasCounter,
        config: &LimitConfig,
        register_id: u64,
        data: Rc<[u8]>,
    ) -> Result<()> {
        self.set_impl(gas_counter, config, register_id, data, false)
    }

    fn set_impl<T>(
        &mut self,
        gas_counter: &mut GasCounter,
        config: &LimitConfig,
        register_id: u64,
        data: T,
        charge_bytes_gas: bool,
    ) -> Result<()>
    where
        T: Into<Rc<[u8]>> + AsRef<[u8]>,
    {
        let data_len =
            u64::try_from(data.as_ref().len()).map_err(|_| HostError::MemoryAccessViolation)?;
        gas_counter.pay_base(write_register_base)?;
        if charge_bytes_gas {
            gas_counter.pay_per(write_register_byte, data_len)?;
        }
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
        config: &LimitConfig,
        register_id: u64,
        data_len: u64,
    ) -> Result<Entry<'a, u64, Rc<[u8]>>> {
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

#[cfg(test)]
mod tests {
    use super::HostError;
    use super::Registers;
    use crate::logic::LimitConfig;
    use crate::logic::gas_counter::GasCounter;
    use crate::tests::test_vm_config;
    use near_parameters::ExtCostsConfig;
    use near_primitives_core::types::Gas;

    struct RegistersTestContext {
        gas: GasCounter,
        cfg: LimitConfig,
        regs: Registers,
    }

    impl RegistersTestContext {
        fn new() -> Self {
            let costs = ExtCostsConfig::test();
            Self {
                gas: GasCounter::new(costs, Gas::MAX, 0, Gas::MAX, false),
                cfg: test_vm_config(None).limit_config,
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
            assert_eq!(
                (Gas::from_gas(gas), Gas::from_gas(gas)),
                (self.gas.burnt_gas(), self.gas.used_gas())
            );
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

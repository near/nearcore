use crate::logic::mocks::mock_external::MockedExternal;
use crate::logic::mocks::mock_memory::MockedMemory;
use crate::logic::{Config, ExecutionResultState, MemSlice, VMContext, VMLogic, VMOutcome};
use crate::tests::test_vm_config;
use crate::wasmtime_runner::test_logic::WasmtimeTestLogic;
use near_parameters::RuntimeFeesConfig;
use near_primitives_core::types::{Balance, Gas};
use std::sync::Arc;

#[derive(Clone, Copy)]
pub(super) enum Backend {
    Legacy,
    Wasmtime,
}

pub(super) struct VMLogicBuilder {
    pub ext: MockedExternal,
    pub config: Config,
    pub fees_config: RuntimeFeesConfig,
    pub memory: MockedMemory,
    pub context: VMContext,
}

impl Default for VMLogicBuilder {
    fn default() -> Self {
        VMLogicBuilder {
            config: test_vm_config(None),
            fees_config: RuntimeFeesConfig::test(),
            ext: MockedExternal::default(),
            memory: MockedMemory::default(),
            context: get_context(),
        }
    }
}

impl VMLogicBuilder {
    pub fn view() -> Self {
        let mut builder = Self::default();
        let max_gas_burnt = builder.config.limit_config.max_gas_burnt;
        builder.context.view_config =
            Some(near_primitives_core::config::ViewConfig { max_gas_burnt });
        builder
    }

    pub fn build(&mut self) -> TestVMLogic<'_> {
        self.build_with_backend(Backend::Legacy)
    }

    pub fn build_with_backend(&mut self, backend: Backend) -> TestVMLogic<'_> {
        match backend {
            Backend::Legacy => {
                let result_state = ExecutionResultState::new(
                    &self.context,
                    self.context.make_gas_counter(&self.config),
                    Arc::new(self.config.clone()),
                );
                TestVMLogic::Legacy {
                    logic: VMLogic::new(
                        &mut self.ext,
                        &self.context,
                        Arc::new(self.fees_config.clone()),
                        result_state,
                        &mut self.memory,
                    ),
                    mem_write_offset: 0,
                }
            }
            Backend::Wasmtime => TestVMLogic::Wasmtime(WasmtimeTestLogic::new(
                &mut self.ext,
                &self.context,
                self.fees_config.clone(),
                self.config.clone(),
            )),
        }
    }

    pub fn free() -> Self {
        VMLogicBuilder {
            config: {
                let mut config = test_vm_config(None);
                config.make_free();
                config
            },
            fees_config: RuntimeFeesConfig::free(),
            ext: MockedExternal::default(),
            memory: MockedMemory::default(),
            context: get_context(),
        }
    }
}

fn get_context() -> VMContext {
    VMContext {
        current_account_id: "alice.near".parse().unwrap(),
        signer_account_id: "bob.near".parse().unwrap(),
        signer_account_pk: vec![0, 1, 2, 3, 4],
        predecessor_account_id: "carol.near".parse().unwrap(),
        refund_to_account_id: "david.near".parse().unwrap(),
        input: std::rc::Rc::new([0, 1, 2, 3, 4]),
        promise_results: vec![].into(),
        block_height: 10,
        block_timestamp: 42,
        epoch_height: 1,
        account_balance: Balance::from_yoctonear(100),
        storage_usage: 0,
        account_locked_balance: Balance::from_yoctonear(50),
        account_contract: near_primitives_core::account::AccountContract::None,
        attached_deposit: Balance::from_yoctonear(10),
        prepaid_gas: Gas::from_teragas(100),
        random_seed: vec![0, 1, 2],
        view_config: None,
        output_data_receivers: vec![],
    }
}

/// Wrapper around `VMLogic` or `WasmtimeTestLogic` which adds helper test
/// methods.
#[allow(clippy::large_enum_variant)]
pub(super) enum TestVMLogic<'a> {
    Legacy {
        logic: VMLogic<'a>,
        /// Offset at which `internal_memory_write` will write next.
        mem_write_offset: u64,
    },
    Wasmtime(WasmtimeTestLogic),
}

impl TestVMLogic<'_> {
    /// Writes data into guest memory and returns pointer at its location.
    ///
    /// Subsequent calls to the method write buffers one after the other.  It
    /// makes it convenient to populate the memory with various different data
    /// to later use in function calls.
    pub(super) fn internal_mem_write(&mut self, data: &[u8]) -> MemSlice {
        match self {
            Self::Legacy { logic, mem_write_offset } => {
                logic.memory().set_for_free(*mem_write_offset, data).unwrap();
                let slice =
                    MemSlice { ptr: *mem_write_offset, len: u64::try_from(data.len()).unwrap() };
                *mem_write_offset += slice.len;
                slice
            }
            Self::Wasmtime(w) => w.internal_mem_write(data),
        }
    }

    /// Writes data into guest memory at given location.
    pub(super) fn internal_mem_write_at(&mut self, ptr: u64, data: &[u8]) -> MemSlice {
        match self {
            Self::Legacy { logic, .. } => {
                logic.memory().set_for_free(ptr, data).unwrap();
                MemSlice { len: u64::try_from(data.len()).unwrap(), ptr }
            }
            Self::Wasmtime(w) => w.internal_mem_write_at(ptr, data),
        }
    }

    /// Reads data from guest memory into a Vector.
    pub(super) fn internal_mem_read(&mut self, ptr: u64, len: u64) -> Vec<u8> {
        match self {
            Self::Legacy { logic, .. } => {
                logic.memory().view_for_free(MemSlice { ptr, len }).unwrap().into_owned()
            }
            Self::Wasmtime(w) => w.internal_mem_read(ptr, len),
        }
    }

    /// Calls `logic.read_register` and then on success reads data from guest
    /// memory comparing it to expected value.
    ///
    /// The `read_register` call is made as if contract has made it.  In
    /// particular, gas is charged for it.  Later reading of the contents of the
    /// memory is done for free.  Panics if the register is not set or contracts
    /// runs out of gas.
    ///
    /// The value of the register is read onto the end of the guest memory
    /// overriding anything that might already be there.
    #[track_caller]
    pub(super) fn assert_read_register(&mut self, want: &[u8], register_id: u64) {
        match self {
            Self::Legacy { logic, .. } => {
                let len = logic.registers().get_len(register_id).unwrap();
                let ptr = MockedMemory::MEMORY_SIZE - len;
                logic.read_register(register_id, ptr).unwrap();
                let got = logic.memory().view_for_free(MemSlice { ptr, len }).unwrap();
                assert_eq!(want, &got[..]);
            }
            Self::Wasmtime(w) => w.assert_read_register(want, register_id),
        }
    }

    pub(super) fn compute_outcome(self) -> VMOutcome {
        match self {
            Self::Legacy { logic, .. } => logic.result_state.compute_outcome(),
            Self::Wasmtime(w) => w.compute_outcome(),
        }
    }
}

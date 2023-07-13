use crate::logic::mocks::mock_external::MockedExternal;
use crate::logic::mocks::mock_memory::MockedMemory;
use crate::logic::types::PromiseResult;
use crate::logic::{MemSlice, VMConfig, VMContext, VMLogic};
use near_primitives_core::runtime::fees::RuntimeFeesConfig;
use near_primitives_core::types::ProtocolVersion;

pub(super) const LATEST_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::MAX;

pub(super) struct VMLogicBuilder {
    pub ext: MockedExternal,
    pub config: VMConfig,
    pub fees_config: RuntimeFeesConfig,
    pub promise_results: Vec<PromiseResult>,
    pub memory: MockedMemory,
    pub current_protocol_version: ProtocolVersion,
    pub context: VMContext,
}

impl Default for VMLogicBuilder {
    fn default() -> Self {
        VMLogicBuilder {
            config: VMConfig::test(),
            fees_config: RuntimeFeesConfig::test(),
            ext: MockedExternal::default(),
            memory: MockedMemory::default(),
            promise_results: vec![],
            current_protocol_version: LATEST_PROTOCOL_VERSION,
            context: get_context(),
        }
    }
}

impl VMLogicBuilder {
    pub fn view() -> Self {
        let mut builder = Self::default();
        let max_gas_burnt = builder.config.limit_config.max_gas_burnt;
        builder.context.view_config = Some(crate::logic::ViewConfig { max_gas_burnt });
        builder
    }

    pub fn build(&mut self) -> TestVMLogic<'_> {
        let context = self.context.clone();
        TestVMLogic::from(VMLogic::new_with_protocol_version(
            &mut self.ext,
            context,
            &self.config,
            &self.fees_config,
            &self.promise_results,
            &mut self.memory,
            self.current_protocol_version,
        ))
    }

    pub fn free() -> Self {
        VMLogicBuilder {
            config: VMConfig::free(),
            fees_config: RuntimeFeesConfig::free(),
            ext: MockedExternal::default(),
            memory: MockedMemory::default(),
            promise_results: vec![],
            current_protocol_version: LATEST_PROTOCOL_VERSION,
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
        input: vec![0, 1, 2, 3, 4],
        block_height: 10,
        block_timestamp: 42,
        epoch_height: 1,
        account_balance: 100,
        storage_usage: 0,
        account_locked_balance: 50,
        attached_deposit: 10,
        prepaid_gas: 10u64.pow(14),
        random_seed: vec![0, 1, 2],
        view_config: None,
        output_data_receivers: vec![],
    }
}

/// Wrapper around `VMLogic` which adds helper test methods.
pub(super) struct TestVMLogic<'a> {
    logic: VMLogic<'a>,
    /// Offset at which `internal_memory_write` will write next.
    mem_write_offset: u64,
}

impl<'a> std::convert::From<VMLogic<'a>> for TestVMLogic<'a> {
    fn from(logic: VMLogic<'a>) -> Self {
        Self { logic, mem_write_offset: 0 }
    }
}

impl<'a> std::ops::Deref for TestVMLogic<'a> {
    type Target = VMLogic<'a>;
    fn deref(&self) -> &Self::Target {
        &self.logic
    }
}

impl std::ops::DerefMut for TestVMLogic<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.logic
    }
}

impl TestVMLogic<'_> {
    /// Writes data into guest memory and returns pointer at its location.
    ///
    /// Subsequent calls to the method write buffers one after the other.  It
    /// makes it convenient to populate the memory with various different data
    /// to later use in function calls.
    pub(super) fn internal_mem_write(&mut self, data: &[u8]) -> MemSlice {
        let slice = self.internal_mem_write_at(self.mem_write_offset, data);
        self.mem_write_offset += slice.len;
        slice
    }

    /// Writes data into guest memory at given location.
    pub(super) fn internal_mem_write_at(&mut self, ptr: u64, data: &[u8]) -> MemSlice {
        self.memory().set_for_free(ptr, data).unwrap();
        MemSlice { len: u64::try_from(data.len()).unwrap(), ptr }
    }

    /// Reads data from guest memory into a Vector.
    pub(super) fn internal_mem_read(&mut self, ptr: u64, len: u64) -> Vec<u8> {
        self.memory().view_for_free(MemSlice { ptr, len }).unwrap().into_owned()
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
        let len = self.registers().get_len(register_id).unwrap();
        let ptr = MockedMemory::MEMORY_SIZE - len;
        self.read_register(register_id, ptr).unwrap();
        let got = self.memory().view_for_free(MemSlice { ptr, len }).unwrap();
        assert_eq!(want, &got[..]);
    }

    pub fn compute_outcome_and_distribute_gas(self) -> crate::logic::VMOutcome {
        self.logic.compute_outcome_and_distribute_gas()
    }
}

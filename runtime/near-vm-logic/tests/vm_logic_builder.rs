use near_primitives_core::runtime::fees::RuntimeFeesConfig;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::mocks::mock_memory::MockedMemory;
use near_vm_logic::VMContext;
use near_vm_logic::{VMConfig, VMLogic};

use near_vm_logic::types::PromiseResult;
use near_vm_logic::ProtocolVersion;

pub(crate) const LATEST_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::MAX;

pub struct VMLogicBuilder {
    pub ext: MockedExternal,
    pub config: VMConfig,
    pub fees_config: RuntimeFeesConfig,
    pub promise_results: Vec<PromiseResult>,
    pub memory: MockedMemory,
    pub current_protocol_version: ProtocolVersion,
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
        }
    }
}

impl VMLogicBuilder {
    pub fn build(&mut self, context: VMContext) -> VMLogic<'_> {
        VMLogic::new_with_protocol_version(
            &mut self.ext,
            context,
            &self.config,
            &self.fees_config,
            &self.promise_results,
            &mut self.memory,
            self.current_protocol_version,
        )
    }
    #[allow(dead_code)]
    pub fn free() -> Self {
        VMLogicBuilder {
            config: VMConfig::free(),
            fees_config: RuntimeFeesConfig::free(),
            ext: MockedExternal::default(),
            memory: MockedMemory::default(),
            promise_results: vec![],
            current_protocol_version: LATEST_PROTOCOL_VERSION,
        }
    }
}

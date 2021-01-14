use near_runtime_fees::RuntimeFeesConfig;
use near_vm_logic::{
    mocks::{mock_external::MockedExternal, mock_memory::MockedMemory},
    VMConfig, VMContext, VMLogic,
};

use near_vm_logic::types::{PromiseResult, ProtocolVersion};

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
            config: VMConfig::default(),
            fees_config: RuntimeFeesConfig::default(),
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
            None,
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

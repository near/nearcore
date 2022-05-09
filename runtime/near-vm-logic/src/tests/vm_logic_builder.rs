use crate::mocks::mock_external::MockedExternal;
use crate::mocks::mock_memory::MockedMemory;
use crate::types::PromiseResult;
use crate::VMContext;
use crate::{VMConfig, VMLogic};
use near_primitives_core::runtime::fees::RuntimeFeesConfig;
use near_primitives_core::types::ProtocolVersion;

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
        let gas_counter = context.new_gas_counter(&self.config);
        VMLogic::new_with_protocol_version(
            &mut self.ext,
            context,
            &self.config,
            &self.fees_config,
            gas_counter,
            &self.promise_results,
            &mut self.memory,
            self.current_protocol_version,
        )
    }
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

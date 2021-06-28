use near_primitives_core::runtime::fees::RuntimeFeesConfig;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::mocks::mock_memory::MockedMemory;
use near_vm_logic::{VMConfig, VMLogic};
use near_vm_logic::{VMContext, VMLogicProtocolFeatures};

use near_vm_logic::types::PromiseResult;

pub struct VMLogicBuilder {
    pub ext: MockedExternal,
    pub config: VMConfig,
    pub fees_config: RuntimeFeesConfig,
    pub promise_results: Vec<PromiseResult>,
    pub memory: MockedMemory,
    pub protocol_features: VMLogicProtocolFeatures,
}

impl Default for VMLogicBuilder {
    fn default() -> Self {
        VMLogicBuilder {
            config: VMConfig::default(),
            fees_config: RuntimeFeesConfig::default(),
            ext: MockedExternal::default(),
            memory: MockedMemory::default(),
            promise_results: vec![],
            protocol_features: VMLogicProtocolFeatures {
                implicit_account_creation: true,
                allow_create_account_on_delete: true,
            },
        }
    }
}

impl VMLogicBuilder {
    pub fn build(&mut self, context: VMContext) -> VMLogic<'_> {
        VMLogic::new_with_protocol_features(
            &mut self.ext,
            context,
            &self.config,
            &self.fees_config,
            &self.promise_results,
            &mut self.memory,
            Default::default(),
            self.protocol_features,
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
            protocol_features: VMLogicProtocolFeatures {
                implicit_account_creation: true,
                allow_create_account_on_delete: true,
            },
        }
    }
}

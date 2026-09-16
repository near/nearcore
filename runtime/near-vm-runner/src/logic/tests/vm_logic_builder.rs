use crate::logic::mocks::mock_external::MockedExternal;
use crate::logic::{Config, VMContext};
use crate::tests::test_vm_config;
pub(super) use crate::wasmtime_runner::test_logic::WasmtimeTestLogic as TestVMLogic;
use near_parameters::RuntimeFeesConfig;
use near_primitives_core::types::{Balance, Gas};

pub(super) struct VMLogicBuilder {
    pub ext: MockedExternal,
    pub config: Config,
    pub fees_config: RuntimeFeesConfig,
    pub context: VMContext,
}

impl Default for VMLogicBuilder {
    fn default() -> Self {
        VMLogicBuilder {
            config: test_vm_config(None),
            fees_config: RuntimeFeesConfig::test(),
            ext: MockedExternal::default(),
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
        TestVMLogic::new(
            &mut self.ext,
            &self.context,
            self.fees_config.clone(),
            self.config.clone(),
        )
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

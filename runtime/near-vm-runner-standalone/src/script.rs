use std::fs;
use std::path::Path;

use near_primitives::contract::ContractCode;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::types::CompiledContractCache;
use near_primitives::version::PROTOCOL_VERSION;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{ProtocolVersion, VMConfig, VMContext};
use near_vm_runner::internal::VMKind;
use near_vm_runner::{MockCompiledContractCache, VMResult};

use crate::State;

#[derive(Clone, Copy)]
pub struct Contract(usize);

/// Constructs a "script" to execute several contracts in a row. This is mainly
/// intended for VM benchmarking.
pub struct Script {
    contracts: Vec<ContractCode>,
    vm_kind: VMKind,
    vm_config: VMConfig,
    protocol_version: ProtocolVersion,
    contract_cache: Option<Box<dyn CompiledContractCache>>,
    initial_state: Option<State>,
    steps: Vec<Step>,
}

pub struct Step {
    contract: Contract,
    method: String,
    vm_context: VMContext,
    promise_results: Vec<PromiseResult>,
    repeat: u32,
}

pub struct ScriptResults {
    pub outcomes: Vec<VMResult>,
    pub state: MockedExternal,
}

impl Default for Script {
    fn default() -> Self {
        let protocol_version = PROTOCOL_VERSION;
        let config_store = RuntimeConfigStore::new(None);
        let runtime_config = config_store.get_config(protocol_version).as_ref();
        Script {
            contracts: Vec::new(),
            vm_kind: VMKind::for_protocol_version(protocol_version),
            vm_config: runtime_config.wasm_config.clone(),
            protocol_version,
            contract_cache: None,
            initial_state: None,
            steps: Vec::new(),
        }
    }
}

impl Script {
    pub(crate) fn contract(&mut self, code: Vec<u8>) -> Contract {
        let res = Contract(self.contracts.len());
        self.contracts.push(ContractCode::new(code, None));
        res
    }

    #[allow(unused)]
    pub(crate) fn contract_from_file(&mut self, path: &Path) -> Contract {
        let data = fs::read(path).unwrap();
        self.contract(data)
    }

    pub(crate) fn vm_kind(&mut self, vm_kind: VMKind) {
        self.vm_kind = vm_kind;
    }

    pub(crate) fn vm_config(&mut self, vm_config: VMConfig) {
        self.vm_config = vm_config;
    }

    pub(crate) fn vm_config_from_file(&mut self, path: &Path) {
        let data = fs::read(path).unwrap();
        let vm_config = serde_json::from_slice(&data).unwrap();
        self.vm_config(vm_config)
    }

    pub(crate) fn protocol_version(&mut self, protocol_version: ProtocolVersion) {
        self.protocol_version = protocol_version;
    }

    #[allow(unused)]
    pub(crate) fn contract_cache(&mut self, yes: bool) {
        self.contract_cache =
            if yes { Some(Box::new(MockCompiledContractCache::default())) } else { None };
    }

    pub(crate) fn initial_state(&mut self, state: State) {
        self.initial_state = Some(state);
    }

    pub(crate) fn initial_state_from_file(&mut self, path: &Path) {
        let data = fs::read(path).unwrap();
        let state = serde_json::from_slice(&data).unwrap();
        self.initial_state(state)
    }

    pub(crate) fn step(&mut self, contract: Contract, method: &str) -> &mut Step {
        self.steps.push(Step::new(contract, method.to_string()));
        self.steps.last_mut().unwrap()
    }

    pub(crate) fn run(mut self) -> ScriptResults {
        let mut external = MockedExternal::new();
        if let Some(State(trie)) = self.initial_state.take() {
            external.fake_trie = trie;
        }

        let config_store = RuntimeConfigStore::new(None);
        let runtime_fees_config = &config_store.get_config(self.protocol_version).transaction_costs;
        let mut outcomes = Vec::new();
        if let Some(runtime) = self.vm_kind.runtime(self.vm_config.clone()) {
            for step in &self.steps {
                for _ in 0..step.repeat {
                    let res = runtime.run(
                        &self.contracts[step.contract.0],
                        &step.method,
                        &mut external,
                        step.vm_context.clone(),
                        runtime_fees_config,
                        &step.promise_results,
                        self.protocol_version,
                        self.contract_cache.as_deref(),
                    );
                    outcomes.push(res);
                }
            }
        } else {
            // TODO(nagisa): this probably could be reported to the user in a better way.
            panic!("the {:?} runtime has not been enabled at compile time", self.vm_kind);
        }
        ScriptResults { outcomes, state: external }
    }
}

impl Step {
    fn new(contract: Contract, method: String) -> Step {
        Step {
            contract,
            method,
            vm_context: default_vm_context(),
            promise_results: Vec::new(),
            repeat: 1,
        }
    }
    pub(crate) fn context(&mut self, context: VMContext) -> &mut Step {
        self.vm_context = context;
        self
    }
    pub(crate) fn context_from_file(&mut self, path: &Path) -> &mut Step {
        let data = fs::read(path).unwrap();
        let context = serde_json::from_slice(&data).unwrap();
        self.context(context)
    }
    pub(crate) fn input(&mut self, input: Vec<u8>) -> &mut Step {
        self.vm_context.input = input;
        self
    }
    pub(crate) fn promise_results(&mut self, promise_results: Vec<PromiseResult>) -> &mut Step {
        self.promise_results = promise_results;
        self
    }
    #[allow(unused)]
    pub(crate) fn repeat(&mut self, n: u32) -> &mut Step {
        self.repeat = n;
        self
    }
}

fn default_vm_context() -> VMContext {
    VMContext {
        current_account_id: "alice".parse().unwrap(),
        signer_account_id: "bob".parse().unwrap(),
        signer_account_pk: vec![0, 1, 2],
        predecessor_account_id: "carol".parse().unwrap(),
        input: vec![],
        block_index: 1,
        block_timestamp: 1586796191203000000,
        account_balance: 10u128.pow(25),
        account_locked_balance: 0,
        storage_usage: 100,
        attached_deposit: 0,
        prepaid_gas: 10u64.pow(18),
        random_seed: vec![0, 1, 2],
        view_config: None,
        output_data_receivers: vec![],
        epoch_height: 1,
    }
}

#[test]
fn vm_script_smoke_test() {
    use near_vm_logic::ReturnData;

    tracing_span_tree::span_tree().enable();

    let mut script = Script::default();
    script.contract_cache(true);

    let contract = script.contract(near_test_contracts::rs_contract().to_vec());

    script.step(contract, "log_something").repeat(3);
    script.step(contract, "sum_n").input(100u64.to_le_bytes().to_vec());

    let res = script.run();

    assert_eq!(res.outcomes.len(), 4);

    let logs = &res.outcomes[0].outcome().logs;
    assert_eq!(logs, &vec!["hello".to_string()]);

    let ret = res.outcomes.last().unwrap().outcome().return_data.clone();

    let expected = ReturnData::Value(4950u64.to_le_bytes().to_vec());
    assert_eq!(ret, expected);
}

#[test]
fn profile_data_is_per_outcome() {
    let mut script = Script::default();
    script.contract_cache(true);

    let contract = script.contract(near_test_contracts::rs_contract().to_vec());

    script.step(contract, "sum_n").input(100u64.to_le_bytes().to_vec());
    script.step(contract, "log_something").repeat(2);
    script.step(contract, "write_key_value");
    let res = script.run();
    assert_eq!(res.outcomes.len(), 4);
    assert_eq!(
        res.outcomes[1].outcome().profile.host_gas(),
        res.outcomes[2].outcome().profile.host_gas()
    );
    assert!(
        res.outcomes[1].outcome().profile.host_gas() > res.outcomes[3].outcome().profile.host_gas()
    );
}

#[cfg(feature = "no_cache")]
#[test]
fn test_evm_slow_deserialize_repro() {
    lazy_static_include::lazy_static_include_bytes! {
        ZOMBIE_OWNERSHIP_BIN => "../near-test-contracts/res/ZombieOwnership.bin",
    };

    fn evm_slow_deserialize_repro(vm_kind: VMKind) {
        println!("evm_slow_deserialize_repro of {:?}", &vm_kind);
        tracing_span_tree::span_tree().enable();

        let mut script = Script::default();
        script.vm_kind(vm_kind);
        script.contract_cache(true);

        // From near-evm repo, the version of when slow issue reported
        let contract =
            script.contract_from_file(Path::new("../near-test-contracts/res/near_evm.wasm"));

        let input = hex::decode(&ZOMBIE_OWNERSHIP_BIN[..]).unwrap();
        script.step(contract, "deploy_code").input(input).repeat(3);
        let res = script.run();
        assert_eq!(res.outcomes[0].error(), None);
        assert_eq!(res.outcomes[1].error(), None);
    }

    evm_slow_deserialize_repro(VMKind::Wasmer0);
    evm_slow_deserialize_repro(VMKind::Wasmer2);
}

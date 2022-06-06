use crate::internal::VMKind;
use near_primitives::{
    contract::ContractCode,
    runtime::{config_store::RuntimeConfigStore, fees::RuntimeFeesConfig},
    types::Gas,
    version::{ProtocolFeature, PROTOCOL_VERSION},
};
use near_vm_logic::{mocks::mock_external::MockedExternal, VMContext};
use std::collections::HashSet;
use std::fmt::Write;

pub(crate) fn test_builder() -> TestBuilder {
    let context = VMContext {
        current_account_id: "alice".parse().unwrap(),
        signer_account_id: "bob".parse().unwrap(),
        signer_account_pk: vec![0, 1, 2],
        predecessor_account_id: "carol".parse().unwrap(),
        input: Vec::new(),
        block_index: 10,
        block_timestamp: 42,
        epoch_height: 1,
        account_balance: 2u128,
        account_locked_balance: 0,
        storage_usage: 12,
        attached_deposit: 2u128,
        prepaid_gas: 10_u64.pow(14),
        random_seed: vec![0, 1, 2],
        view_config: None,
        output_data_receivers: vec![],
    };
    TestBuilder {
        code: ContractCode::new(Vec::new(), None),
        context,
        method: "main".to_string(),
        protocol_features: &[],
        skip: HashSet::new(),
        opaque_error: false,
        opaque_outcome: false,
    }
}

pub(crate) struct TestBuilder {
    code: ContractCode,
    context: VMContext,
    protocol_features: &'static [ProtocolFeature],
    method: String,
    skip: HashSet<VMKind>,
    opaque_error: bool,
    opaque_outcome: bool,
}

impl TestBuilder {
    pub(crate) fn wat(mut self, wat: &str) -> Self {
        let wasm = wat::parse_str(wat)
            .unwrap_or_else(|err| panic!("failed to parse input wasm: {err}\n{wat}"));
        self.code = ContractCode::new(wasm, None);
        self
    }

    pub(crate) fn wasm(mut self, wasm: &[u8]) -> Self {
        self.code = ContractCode::new(wasm.to_vec(), None);
        self
    }

    pub(crate) fn method(mut self, method: &str) -> Self {
        self.method = method.to_string();
        self
    }

    pub(crate) fn gas(mut self, gas: Gas) -> Self {
        self.context.prepaid_gas = gas;
        self
    }

    pub(crate) fn opaque_error(mut self) -> Self {
        self.opaque_error = true;
        self
    }

    pub(crate) fn opaque_outcome(mut self) -> Self {
        self.opaque_outcome = true;
        self
    }

    // We only test trapping tests on Wasmer, as of version 0.17, when tests executed in parallel,
    // Wasmer signal handlers may catch signals thrown from the Wasmtime, and produce fake failing tests.
    pub(crate) fn skip_wasmtime(mut self) -> Self {
        self.skip.insert(VMKind::Wasmtime);
        self
    }

    pub(crate) fn skip_wasmer0(mut self) -> Self {
        self.skip.insert(VMKind::Wasmer0);
        self
    }

    pub(crate) fn skip_wasmer2(mut self) -> Self {
        self.skip.insert(VMKind::Wasmer2);
        self
    }

    pub(crate) fn protocol_features(
        mut self,
        protocol_features: &'static [ProtocolFeature],
    ) -> Self {
        self.protocol_features = protocol_features;
        self
    }

    pub(crate) fn expect(self, want: expect_test::Expect) {
        self.expects(&[want])
    }

    pub(crate) fn expects(self, wants: &[expect_test::Expect]) {
        let runtime_config_store = RuntimeConfigStore::new(None);

        let mut protocol_versions = Vec::new();
        for feat in self.protocol_features {
            protocol_versions.push(feat.protocol_version() - 1)
        }
        protocol_versions.push(PROTOCOL_VERSION);
        assert_eq!(
            wants.len(),
            protocol_versions.len(),
            "specified {} features but only {} expectation ({} + 1 needed)",
            self.protocol_features.len(),
            wants.len(),
            self.protocol_features.len()
        );

        for (want, &protocol_version) in wants.iter().zip(&protocol_versions) {
            let mut results = vec![];
            for vm_kind in [VMKind::Wasmer2, VMKind::Wasmer0, VMKind::Wasmtime] {
                if self.skip.contains(&vm_kind) {
                    continue;
                }
                let runtime_config = runtime_config_store.get_config(protocol_version);

                let mut fake_external = MockedExternal::new();
                let config = runtime_config.wasm_config.clone();
                let fees = RuntimeFeesConfig::test();
                let context = self.context.clone();

                let promise_results = vec![];

                let runtime = vm_kind.runtime(config).expect("runtime has not been compiled");
                let res = runtime.run(
                    &self.code,
                    &self.method,
                    &mut fake_external,
                    context,
                    &fees,
                    &promise_results,
                    protocol_version,
                    None,
                );

                let mut got = if self.opaque_outcome {
                    String::new()
                } else {
                    format!("{:?}\n", res.outcome())
                };
                if let Some(err) = res.error() {
                    let mut err = err.to_string();
                    assert!(err.len() < 1000, "errors should be bounded in size");
                    if self.opaque_error {
                        err = "...".to_string();
                    }
                    writeln!(got, "Err: {err}").unwrap();
                }

                results.push((vm_kind, got));
            }

            if !results.is_empty() {
                want.assert_eq(&results[0].1);
                for i in 1..results.len() {
                    if results[i].1 != results[0].1 {
                        panic!(
                            "Inconsistent VM Output:\n{:?}:\n{}\n\n{:?}:\n{}",
                            results[0].0, results[0].1, results[i].0, results[i].1
                        )
                    }
                }
            }
        }
    }
}

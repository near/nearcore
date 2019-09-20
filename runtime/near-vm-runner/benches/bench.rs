use bencher::{benchmark_group, benchmark_main, Bencher};
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{Config, ReturnData, VMContext, VMOutcome};
use near_vm_runner::{run, VMError};
use std::fs;
use std::mem::size_of;
use std::path::PathBuf;

fn setup(input: u64) -> (MockedExternal, VMContext, Config, Vec<PromiseResult>, Vec<u8>) {
    let fake_external = MockedExternal::new();
    let config = Config::default();

    let input = input.to_le_bytes().to_vec();
    let context = VMContext {
        current_account_id: "alice".to_owned(),
        signer_account_id: "bob".to_owned(),
        signer_account_pk: vec![1, 2, 3],
        predecessor_account_id: "carol".to_owned(),
        input,
        block_index: 0,
        block_timestamp: 0,
        account_balance: 0,
        storage_usage: 0,
        attached_deposit: 0,
        prepaid_gas: 10u64.pow(15),
        random_seed: vec![0, 1, 2],
        free_of_charge: false,
        output_data_receivers: vec![],
    };
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/res/test_contract_rs.wasm");
    let code = fs::read(path).unwrap();
    (fake_external, context, config, vec![], code)
}

fn assert_run_result((outcome, err): (Option<VMOutcome>, Option<VMError>), expected_value: u64) {
    if let Some(_) = err {
        panic!("Failed execution");
    }

    if let Some(VMOutcome { return_data, .. }) = outcome {
        if let ReturnData::Value(value) = return_data {
            let mut arr = [0u8; size_of::<u64>()];
            arr.copy_from_slice(&value);
            let res = u64::from_le_bytes(arr);
            assert_eq!(res, expected_value);
        } else {
            panic!("Value was not returned");
        }
    } else {
        panic!("Failed execution");
    }
}

fn pass_through(bench: &mut Bencher) {
    let (mut external, context, config, promise_results, code) = setup(42);
    bench.iter(move || {
        let result = run(
            vec![],
            &code,
            b"pass_through",
            &mut external,
            context.clone(),
            &config,
            &promise_results,
        );
        assert_run_result(result, 42);
    });
}

fn benchmark_fake_storage_8b_1000(bench: &mut Bencher) {
    let (mut external, context, config, promise_results, code) = setup(1000);
    bench.iter(move || {
        let result = run(
            vec![],
            &code,
            b"benchmark_storage_8b",
            &mut external,
            context.clone(),
            &config,
            &promise_results,
        );
        assert_run_result(result, 999 * 1000 / 2);
    });
}

fn benchmark_fake_storage_10kib_1000(bench: &mut Bencher) {
    let (mut external, context, config, promise_results, code) = setup(1000);
    bench.iter(move || {
        let result = run(
            vec![],
            &code,
            b"benchmark_storage_10kib",
            &mut external,
            context.clone(),
            &config,
            &promise_results,
        );
        assert_run_result(result, 999 * 1000 / 2);
    });
}

fn sum_n_1000000(bench: &mut Bencher) {
    let (mut external, context, config, promise_results, code) = setup(1000000);
    bench.iter(move || {
        let result =
            run(vec![], &code, b"sum_n", &mut external, context.clone(), &config, &promise_results);
        assert_run_result(result, (1000000 - 1) * 1000000 / 2);
    });
}

benchmark_group!(
    vm_benches,
    pass_through,
    benchmark_fake_storage_8b_1000,
    benchmark_fake_storage_10kib_1000,
    sum_n_1000000
);
benchmark_main!(vm_benches);

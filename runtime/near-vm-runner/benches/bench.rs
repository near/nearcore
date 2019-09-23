use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{Config, ReturnData, VMContext, VMOutcome};
use near_vm_runner::{run, VMError};
use std::fs;
use std::mem::size_of;
use std::path::PathBuf;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

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
/*
fn pass_through(c: &mut Criterion) {
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

fn benchmark_fake_storage_8b_1000(c: &mut Criterion) {
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

fn benchmark_fake_storage_10kib_1000(c: &mut Criterion) {
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
*/
fn sum_n(c: &mut Criterion) {
    let mut group = c.benchmark_group("sum_n");
    let args = vec![1, 10, 100, 1_000, 10_000, 100_000, 1_000_000];
    for n in args {
        let (mut external, context, config, promise_results, code) = setup(n);
        let benchmark_param_display = format!("n={}", n);
        let mut expected_value = 0u128;
        for i in 1..n + 1 {
            expected_value += (i * i) as u128;
        }
        expected_value /= n as u128;
        group.bench_function(BenchmarkId::new("wasm", benchmark_param_display.clone()), |b| {
            b.iter(|| {
                let result = run(
                    vec![],
                    &code,
                    b"sum_n",
                    &mut external,
                    context.clone(),
                    &config,
                    &promise_results,
                );
                assert_run_result(result, expected_value as u64);
            });
        });
        group.bench_function(BenchmarkId::new("rust", benchmark_param_display.clone()), |b| {
            b.iter(|| {
                let mut x = 0u128;
                for i in 1..n + 1 {
                    x += (i * i) as u128;
                }
                x /= n as u128;
                assert_eq!(x as u64, expected_value as u64);
            });
        });
    }
    group.finish();
}

criterion_group!(
    vm_benches,
    /*
        pass_through,
        benchmark_fake_storage_8b_1000,
        benchmark_fake_storage_10kib_1000,
    */
    sum_n
);
criterion_main!(vm_benches);

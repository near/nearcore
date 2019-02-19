#[macro_use]
extern crate bencher;

use bencher::Bencher;

use node_runtime::test_utils::{get_runtime_and_trie, User, setup_test_contract};

fn runtime_send_money(bench: &mut Bencher) {
    let (runtime, trie, root) = get_runtime_and_trie();
    let (mut user, mut root) = User::new(runtime, "alice.near", trie, root);
    bench.iter(|| {
        let (new_root, _) = user.send_money(root, "bob.near", 1);
        root = new_root;
    });
}

fn runtime_wasm_bad_code(bench: &mut Bencher) {
    let code = include_bytes!("../../../tests/hello.wasm");
    let code = wasm::prepare::prepare_contract(code, &wasm::types::Config::default()).unwrap();
    let (mut user, mut root) = setup_test_contract(&code);
    bench.iter(|| {
        let (new_root, _) = user.call_function(
            root, "test_contract", "benchmark", b"{}".to_vec(), 0
        );
        root = new_root;
    });
}

fn runtime_wasm_set_value(bench: &mut Bencher) {
    let (mut user, mut root) = setup_test_contract(include_bytes!("../../../tests/hello.wasm"));
    bench.iter(|| {
        let (new_root, _) = user.call_function(
            root, "test_contract", "setValue", b"{\"value\":\"123\"}".to_vec(), 0
        );
        root = new_root;
    });
}

fn runtime_wasm_benchmark_10_reads_legacy(bench: &mut Bencher) {
    let (mut user, mut root) = setup_test_contract(include_bytes!("../../../tests/hello.wasm"));
    bench.iter(|| {
        let (new_root, _) = user.call_function(root, "test_contract", "benchmark", b"{}".to_vec(), 0);
        root = new_root;
    });
}

fn runtime_wasm_benchmark_storage_100(bench: &mut Bencher) {
    let (mut user, mut root) = setup_test_contract(include_bytes!("../../../tests/hello.wasm"));
    bench.iter(|| {
        let (new_root, _) = user.call_function(
            root,
            "test_contract",
            "benchmark_storage",
            b"{\"n\":100}".to_vec(),
            0);
        root = new_root;
    });
}

fn runtime_wasm_benchmark_storage_1000(bench: &mut Bencher) {
    let (mut user, mut root) = setup_test_contract(include_bytes!("../../../tests/hello.wasm"));
    bench.iter(|| {
        let (new_root, _) = user.call_function(
            root,
            "test_contract",
            "benchmark_storage",
            b"{\"n\":1000}".to_vec(),
            0);
        root = new_root;
    });
}

fn runtime_wasm_benchmark_sum_1000(bench: &mut Bencher) {
    let (mut user, mut root) = setup_test_contract(include_bytes!("../../../tests/hello.wasm"));
    bench.iter(|| {
        let (new_root, _) = user.call_function(
            root,
            "test_contract",
            "benchmark_sum_n",
            b"{\"n\":1000}".to_vec(),
            0);
        root = new_root;
    });
}

fn runtime_wasm_benchmark_sum_1000000(bench: &mut Bencher) {
    let (mut user, mut root) = setup_test_contract(include_bytes!("../../../tests/hello.wasm"));
    bench.iter(|| {
        let (new_root, _) = user.call_function(
            root,
            "test_contract",
            "benchmark_sum_n",
            b"{\"n\":1000000}".to_vec(),
            0);
        root = new_root;
    });
}

benchmark_group!(runtime_benches, runtime_send_money);
benchmark_group!(wasm_benches,
    runtime_wasm_set_value,
    runtime_wasm_bad_code,
    runtime_wasm_benchmark_10_reads_legacy,
    runtime_wasm_benchmark_storage_100,
    runtime_wasm_benchmark_storage_1000,
    runtime_wasm_benchmark_sum_1000,
    runtime_wasm_benchmark_sum_1000000);
benchmark_main!(runtime_benches, wasm_benches);

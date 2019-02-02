#[macro_use]
extern crate bencher;

use bencher::Bencher;

use node_runtime::test_utils::{get_runtime_and_state_db_viewer, User, setup_test_contract};

fn runtime_send_money(bench: &mut Bencher) {
    let (runtime, _, mut root) = get_runtime_and_state_db_viewer();
    let mut user = User::new(runtime, "alice.near");
    bench.iter(|| {
        root = user.send_money(root, "bob.near", 1);
    });
}

fn runtime_wasm_set_value(bench: &mut Bencher) {
    let (mut user, mut root) = setup_test_contract(include_bytes!("../../../tests/hello.wasm"));
    bench.iter(|| {
        root = user.call_function(root, "test_contract", "setValue", "{\"value\": \"123\"}");
    });
}

fn runtime_wasm_benchmark(bench: &mut Bencher) {
    let (mut user, mut root) = setup_test_contract(include_bytes!("../../../tests/hello.wasm"));
    bench.iter(|| {
        root = user.call_function(root, "test_contract", "benchmark", "{}");
    });
}

benchmark_group!(runtime_benches, runtime_send_money);
benchmark_group!(wasm_benches, runtime_wasm_set_value, runtime_wasm_benchmark);
benchmark_main!(runtime_benches, wasm_benches);

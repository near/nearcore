#[macro_use]
extern crate bencher;

use bencher::Bencher;

use near_primitives::contract::ContractCode;
use near_primitives::transaction::{
    CreateAccountTransaction, DeployContractTransaction, TransactionBody,
};
use near_primitives::types::Balance;
use testlib::node::{Node, RuntimeNode};

fn runtime_send_money(bench: &mut Bencher) {
    let node = RuntimeNode::new(&"alice.near".to_string());
    let node_user = node.user();
    bench.iter(|| {
        node_user.send_money("alice.near".to_string(), "bob.near".to_string(), 1);
    });
}

const FUNCTION_CALL_AMOUNT: Balance = 1_000_000_000;

fn setup_test_contract(wasm_binary: &[u8]) -> RuntimeNode {
    let node = RuntimeNode::new(&"alice.near".to_string());
    let account_id = node.account_id().unwrap();
    let transaction = TransactionBody::CreateAccount(CreateAccountTransaction {
        nonce: node.get_account_nonce(&account_id).unwrap_or_default() + 1,
        originator: account_id.clone(),
        new_account_id: "test_contract".to_string(),
        public_key: node.signer().public_key().0[..].to_vec(),
        amount: 0,
    })
    .sign(&*node.signer());
    let user = node.user();
    user.add_transaction(transaction).unwrap();

    let transaction = TransactionBody::DeployContract(DeployContractTransaction {
        nonce: node.get_account_nonce(&account_id).unwrap_or_default() + 1,
        contract_id: "test_contract".to_string(),
        wasm_byte_array: wasm_binary.to_vec(),
    })
    .sign(&*node.signer());
    user.add_transaction(transaction).unwrap();
    node
}

fn runtime_wasm_bad_code(bench: &mut Bencher) {
    let code = include_bytes!("../../../tests/hello.wasm");
    let code = ContractCode::new(code.to_vec());
    let code = wasm::prepare::prepare_contract(&code, &wasm::types::Config::default()).unwrap();
    let node = setup_test_contract(&code);
    let node_user = node.user();
    bench.iter(|| {
        node_user.function_call("alice.near".to_string(), "test_contract".to_string(), "benchmark", b"{}".to_vec(), FUNCTION_CALL_AMOUNT);
    });
}

fn runtime_wasm_set_value(bench: &mut Bencher) {
    let node = setup_test_contract(include_bytes!("../../../tests/hello.wasm"));
    let node_user = node.user();
    bench.iter(|| {
        node_user.function_call("alice.near".to_string(), "test_contract".to_string(),
            "setValue",
            b"{\"value\":\"123\"}".to_vec(),
            FUNCTION_CALL_AMOUNT,
        );
    });
}

fn runtime_wasm_benchmark_10_reads_legacy(bench: &mut Bencher) {
    let node = setup_test_contract(include_bytes!("../../../tests/hello.wasm"));
    let node_user = node.user();
    bench.iter(|| {
        node_user.function_call("alice.near".to_string(), "test_contract".to_string(), "benchmark", b"{}".to_vec(), FUNCTION_CALL_AMOUNT);
    });
}

fn runtime_wasm_benchmark_storage_100(bench: &mut Bencher) {
    let node = setup_test_contract(include_bytes!("../../../tests/hello.wasm"));
    let node_user = node.user();
    bench.iter(|| {
        node_user.function_call("alice.near".to_string(), "test_contract".to_string(),
            "benchmark_storage",
            b"{\"n\":100}".to_vec(),
            FUNCTION_CALL_AMOUNT,
        );
    });
}

fn runtime_wasm_benchmark_storage_1000(bench: &mut Bencher) {
    let node = setup_test_contract(include_bytes!("../../../tests/hello.wasm"));
    let node_user = node.user();
    bench.iter(|| {
        node_user.function_call("alice.near".to_string(), "test_contract".to_string(),
            "benchmark_storage",
            b"{\"n\":1000}".to_vec(),
            FUNCTION_CALL_AMOUNT,
        );
    });
}

fn runtime_wasm_benchmark_sum_1000(bench: &mut Bencher) {
    let node = setup_test_contract(include_bytes!("../../../tests/hello.wasm"));
    let node_user = node.user();
    bench.iter(|| {
        node_user.function_call("alice.near".to_string(), "test_contract".to_string(),
            "benchmark_sum_n",
            b"{\"n\":1000}".to_vec(),
            FUNCTION_CALL_AMOUNT,
        );
    });
}

fn runtime_wasm_benchmark_sum_1000000(bench: &mut Bencher) {
    let node = setup_test_contract(include_bytes!("../../../tests/hello.wasm"));
    let node_user = node.user();
    bench.iter(|| {
        node_user.function_call("alice.near".to_string(), "test_contract".to_string(),
            "benchmark_sum_n",
            b"{\"n\":1000000}".to_vec(),
            FUNCTION_CALL_AMOUNT,
        );
    });
}

benchmark_group!(runtime_benches, runtime_send_money);
benchmark_group!(
    wasm_benches,
    runtime_wasm_set_value,
    runtime_wasm_bad_code,
    runtime_wasm_benchmark_10_reads_legacy,
    runtime_wasm_benchmark_storage_100,
    runtime_wasm_benchmark_storage_1000,
    runtime_wasm_benchmark_sum_1000,
    runtime_wasm_benchmark_sum_1000000
);
benchmark_main!(runtime_benches, wasm_benches);

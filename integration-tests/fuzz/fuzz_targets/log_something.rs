#![no_main]

use integration_tests::node::{RuntimeNode, Node};
use testlib::runtime_utils::{alice_account, bob_account};


fn create_runtime_node() -> impl Node {
    RuntimeNode::new(&alice_account())
}

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let node = create_runtime_node();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result = node_user
        .function_call(alice_account(), bob_account(), "log_something", data.to_vec(), 10u64.pow(14), 0)
        .unwrap();
});
#![no_main]

use integration_tests::node::{RuntimeNode, Node};
use testlib::runtime_utils::{alice_account, bob_account};

use rand::seq::IteratorRandom;


#[derive(arbitrary::Arbitrary, Debug)]
struct Params {
    args: Vec<u8>,
}

impl Params {
    fn random_method_name(&self) -> String {
        let method_names = vec![
            "storage_deposit",
            "storage_withdraw",
            "panic_with_message",
            "abort_with_zero",
            "panic_after_logging",
            "log_something",
            "loop_forever",
            "sum_with_input",
            "call_promise",
        ];
        method_names.iter().choose(&mut rand::thread_rng()).unwrap().to_string()
    }
}

fn create_runtime_node() -> impl Node {
    RuntimeNode::new(&alice_account())
}

libfuzzer_sys::fuzz_target!(|args: Params| {
    let node = create_runtime_node();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result = node_user
        .function_call(alice_account(), bob_account(), args.random_method_name().as_str(), args.args, 10u64.pow(14), 0)
        .unwrap();
});


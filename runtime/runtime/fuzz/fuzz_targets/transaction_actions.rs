#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;

use near_primitives::transaction::{SignedTransaction, Action};
use node_runtime::runtime_group_tools::RuntimeGroup;
use near_primitives::hash::CryptoHash;


#[derive(Debug, Clone, Arbitrary)]
struct ActionsFactoryParams {
    actions: Vec<Action>,
}

// runtime_fuzzer is a proper scenario fuzzing, this is just a search for an unhandled crash
fuzz_target!(|params: ActionsFactoryParams| {
    let group = RuntimeGroup::new(3, 2, near_test_contracts::rs_contract());
    let signer_sender = group.signers[0].clone();
    let signer_receiver = group.signers[1].clone();

    let signed_transaction = SignedTransaction::from_actions(
        1,
        signer_sender.account_id.clone(),
        signer_receiver.account_id,
        &signer_sender,
        params.actions,
        CryptoHash::default(),
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }
});
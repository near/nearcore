#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;

use near_primitives::transaction::{Transaction, SignedTransaction, Action};
use node_runtime::tests::setup_runtime;
use node_runtime::runtime_group_tools::RuntimeGroup;
use near_primitives::types::{Balance, Gas, Nonce};

#[derive(Debug, Clone, Arbitrary)]
struct ActionsFactoryParams {
    initial_balance: Balance,
    initial_locked: Balance,
    gas_limit: Gas,
    nonce: Nonce,

    actions: Vec<Action>,
}

fuzz_target!(|params: ActionsFactoryParams| {
    let (mut runtime, genesis_config, _root_key) = setup_runtime(
        initial_balance: params.initial_balance,
        initial_locked: params.initial_locked,
        gas_limit: params.gas_limit,
    );

    let group = RuntimeGroup::new(3, 2, near_test_contracts::rs_contract());
    let signer_sender = group.signers[0].clone();
    let signer_receiver = group.signers[1].clone();
    let signer_new_account = group.signers[2].clone();

    let signed_transaction = SignedTransaction::from_actions(
        params.nonce,
        signer_sender.account_id.clone(),
        signer_receiver.account_id,
        &signer_sender,
        &params.actions,
        CryptoHash::default(),
    );

    let handles = RuntimeGroup::start_runtimes(group.clone(), vec![signed_transaction.clone()]);
    for h in handles {
        h.join().unwrap();
    }
});
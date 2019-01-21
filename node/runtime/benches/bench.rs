#[macro_use]
extern crate bencher;

use bencher::Bencher;

extern crate primitives;
extern crate node_runtime;
extern crate transaction;

use primitives::hash::CryptoHash;
use primitives::signature::{DEFAULT_SIGNATURE, get_key_pair};

use transaction::{TransactionBody, SendMoneyTransaction, SignedTransaction, Transaction};
use node_runtime::ApplyState;
use node_runtime::test_utils::{generate_test_chain_spec, get_runtime_and_state_db_viewer_from_chain_spec};

fn runtime_send_money(bench: &mut Bencher) {
    let (mut chain_spec, _) = generate_test_chain_spec();
    let public_key = get_key_pair().0;
    for i in 0..100 {
        chain_spec.accounts.push((format!("account{}", i), public_key.to_string(), 10000, 10000));
    }
    let (mut runtime, viewer) = get_runtime_and_state_db_viewer_from_chain_spec(&chain_spec);
    let mut root = viewer.get_root();
    bench.iter(|| {
        for _ in 0..100 {
            let mut transactions = vec![];
            for i in 0..100 {
                transactions.push(Transaction::SignedTransaction(SignedTransaction::new(
                    DEFAULT_SIGNATURE,
                    TransactionBody::SendMoney(SendMoneyTransaction {
                        nonce: 1,
                        originator: format!("account{}", i % 100),
                        receiver: format!("account{}", ((i + 1) % 100)),
                        amount: 1,
                    }))));
            }
            let apply_state = ApplyState {
                root,
                shard_id: 0,
                parent_block_hash: CryptoHash::default(),
                block_index: 0
            };
            let apply_result = runtime.apply_all(
                apply_state, transactions
            );
            runtime.state_db.commit(apply_result.transaction).unwrap();
            root = apply_result.root;
        }
    })
}

benchmark_group!(benches, runtime_send_money);
benchmark_main!(benches);

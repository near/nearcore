#[macro_use]
extern crate bencher;

use bencher::Bencher;

use nightshade::nightshade::BareState;
use primitives::hash::hash_struct;
use primitives::signer::{BlockSigner, InMemorySigner};

fn bare_state() -> BareState {
    BareState::new(0, hash_struct(&0))
}

fn bs_encode(bench: &mut Bencher) {
    let bs = bare_state();
    bench.iter(|| (0..100000).fold(0, |acc, _| acc ^ bs.bs_encode()[0]))
}

fn sign(bench: &mut Bencher) {
    let signer = InMemorySigner::default();
    let bs = bare_state();

    bench.iter(|| signer.bls_sign(&bs.bs_encode()))
}

benchmark_group!(benches, bs_encode, sign);
benchmark_main!(benches);

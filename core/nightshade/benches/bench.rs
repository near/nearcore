#[macro_use]
extern crate bencher;

use bencher::Bencher;

use nightshade::nightshade::BareState;
use primitives::aggregate_signature::BlsSecretKey;
use primitives::hash::hash_struct;

fn bare_state() -> BareState {
    BareState::new(0, hash_struct(&0))
}

fn bs_encode(bench: &mut Bencher) {
    let bs = bare_state();
    bench.iter(|| {
        (0..100000).fold(0, |acc, _| {
            acc ^ bs.bs_encode()[0]
        })
    })
}

fn sign(bench: &mut Bencher) {
    let sk = BlsSecretKey::generate();
    let bs = bare_state();

    bench.iter(|| {
        bs.sign(&sk)
    })
}

benchmark_group!(benches, bs_encode, sign);
benchmark_main!(benches);

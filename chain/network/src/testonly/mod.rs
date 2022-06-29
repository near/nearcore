pub mod actix;
pub mod fake_client;
pub mod stream;

pub type Rng = rand_pcg::Pcg32;

pub fn make_rng(seed: u64) -> Rng {
    Rng::new(seed, 0xa02bdbf7bb3c0a7)
}

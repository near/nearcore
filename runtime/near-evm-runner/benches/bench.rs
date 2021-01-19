use criterion::{criterion_group, criterion_main};

#[cfg(feature = "protocol_feature_evm")]
mod benches;
#[cfg(feature = "protocol_feature_evm")]
use benches::cryptozombies;

#[cfg(feature = "protocol_feature_evm")]
criterion_group!(cryptozombies, cryptozombies::create_random, cryptozombies::deploy_code);
#[cfg(feature = "protocol_feature_evm")]
criterion_main!(cryptozombies);

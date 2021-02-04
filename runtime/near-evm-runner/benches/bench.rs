use criterion::{criterion_group, criterion_main};

#[cfg(feature = "protocol_feature_evm")]
mod benches;
#[cfg(feature = "protocol_feature_evm")]
use benches::cryptozombies;

// cargo bench --package near-evm-runner --features=nightly_protocol_features
#[cfg(feature = "protocol_feature_evm")]
criterion_group!(
    cryptozombies,
    cryptozombies::create_random,
    cryptozombies::deploy_code,
    cryptozombies::transfer_erc721
);
#[cfg(feature = "protocol_feature_evm")]
criterion_main!(cryptozombies);

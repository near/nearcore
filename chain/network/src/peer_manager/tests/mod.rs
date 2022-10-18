use crate::config;
use crate::peer_manager::testonly::NormalAccountData;
use near_primitives::types::EpochId;

mod accounts_data;
mod connection_pool;
mod nonce;
mod routing;
mod tier1;

fn peer_account_data(e: &EpochId, vc: &config::ValidatorConfig) -> NormalAccountData {
    NormalAccountData {
        epoch_id: e.clone(),
        account_id: vc.signer.validator_id().clone(),
        peers: match &vc.proxies {
            config::ValidatorProxies::Static(proxies) => proxies.clone(),
            config::ValidatorProxies::Dynamic(_) => {
                panic!("tests only support Static in validator config")
            }
        },
    }
}

use std::sync::{Arc, RwLock};
use super::BlockChain;

pub fn get_blockchain_storage<H, B, S>(chain: BlockChain<H, B, S>) -> Arc<RwLock<S>> {
    chain.storage.clone()
}

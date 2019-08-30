use std::collections::HashMap;
use std::sync::Arc;

use log::LevelFilter;

use near_crypto::{Signature, Signer};

use crate::block::Block;
use crate::transaction::{SignedTransaction, Transaction};
use crate::types::{BlockIndex, EpochId};

pub fn init_test_logger() {
    let _ = env_logger::Builder::new()
        .filter_module("tokio_reactor", LevelFilter::Info)
        .filter_module("tokio_core", LevelFilter::Info)
        .filter_module("hyper", LevelFilter::Info)
        .filter(None, LevelFilter::Debug)
        .try_init();
}

pub fn init_test_module_logger(module: &str) {
    let _ = env_logger::Builder::new()
        .filter_module("tokio_reactor", LevelFilter::Info)
        .filter_module("tokio_core", LevelFilter::Info)
        .filter_module("hyper", LevelFilter::Info)
        .filter_module("cranelift_wasm", LevelFilter::Warn)
        .filter_module(module, LevelFilter::Debug)
        .filter(None, LevelFilter::Info)
        .try_init();
}

pub fn init_integration_logger() {
    let _ = env_logger::Builder::new()
        .filter(None, LevelFilter::Info)
        .filter(Some("actix_web"), LevelFilter::Warn)
        .try_init();
}

impl Transaction {
    pub fn sign(self, signer: &dyn Signer) -> SignedTransaction {
        let signature = signer.sign(self.get_hash().as_ref());
        SignedTransaction::new(signature, self)
    }
}

impl Block {
    pub fn empty_with_epoch(
        prev: &Block,
        height: BlockIndex,
        epoch_id: EpochId,
        signer: Arc<dyn Signer>,
    ) -> Self {
        Self::empty_with_approvals(prev, height, epoch_id, HashMap::default(), signer)
    }

    pub fn empty_with_height(prev: &Block, height: BlockIndex, signer: Arc<dyn Signer>) -> Self {
        Self::empty_with_epoch(prev, height, prev.header.inner.epoch_id.clone(), signer)
    }

    pub fn empty(prev: &Block, signer: Arc<dyn Signer>) -> Self {
        Self::empty_with_height(prev, prev.header.inner.height + 1, signer)
    }

    /// This is not suppose to be used outside of chain tests, because this doesn't refer to correct chunks.
    /// Done because chain tests don't have a good way to store chunks right now.
    pub fn empty_with_approvals(
        prev: &Block,
        height: BlockIndex,
        epoch_id: EpochId,
        approvals: HashMap<usize, Signature>,
        signer: Arc<dyn Signer>,
    ) -> Self {
        Block::produce(
            &prev.header,
            height,
            prev.chunks.clone(),
            epoch_id,
            vec![],
            approvals,
            0,
            0,
            signer,
        )
    }
}

use std::sync::{Mutex, Once};

use log::LevelFilter;

use lazy_static::lazy_static;
use near_crypto::{EmptySigner, PublicKey, Signer};

use crate::account::{AccessKey, AccessKeyPermission};
use crate::block::{Approval, Block};
use crate::hash::CryptoHash;
use crate::transaction::{
    Action, AddKeyAction, CreateAccountAction, SignedTransaction, StakeAction, Transaction,
    TransferAction,
};
use crate::types::{AccountId, Balance, BlockHeight, EpochId, Nonce};
use crate::validator_signer::ValidatorSigner;

lazy_static! {
    static ref HEAVY_TESTS_LOCK: Mutex<()> = Mutex::new(());
}

pub fn heavy_test<F>(f: F)
where
    F: FnOnce() -> (),
{
    let _guard = HEAVY_TESTS_LOCK.lock();
    f();
}

pub fn init_test_logger() {
    let _ = env_logger::Builder::new()
        .filter_module("tokio_reactor", LevelFilter::Info)
        .filter_module("tokio_core", LevelFilter::Info)
        .filter_module("hyper", LevelFilter::Info)
        .filter(None, LevelFilter::Debug)
        .try_init();
    init_stop_on_panic();
}

pub fn init_test_logger_allow_panic() {
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
        .filter_module(module, LevelFilter::Info)
        .filter(None, LevelFilter::Info)
        .try_init();
    init_stop_on_panic();
}

pub fn init_integration_logger() {
    let _ = env_logger::Builder::new()
        .filter(None, LevelFilter::Info)
        .filter(Some("actix_web"), LevelFilter::Warn)
        .try_init();
    init_stop_on_panic();
}

static SET_PANIC_HOOK: Once = Once::new();

/// This is a workaround to make actix/tokio runtime stop when a task panics.
pub fn init_stop_on_panic() {
    SET_PANIC_HOOK.call_once(|| {
        let default_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            default_hook(info);
            actix::System::with_current(|sys| sys.stop_with_code(1));
        }));
    })
}

impl Transaction {
    pub fn sign(self, signer: &dyn Signer) -> SignedTransaction {
        let signature = signer.sign(self.get_hash().as_ref());
        SignedTransaction::new(signature, self)
    }
}

impl SignedTransaction {
    pub fn send_money(
        nonce: Nonce,
        signer_id: AccountId,
        receiver_id: AccountId,
        signer: &dyn Signer,
        deposit: Balance,
        block_hash: CryptoHash,
    ) -> Self {
        Self::from_actions(
            nonce,
            signer_id,
            receiver_id,
            signer,
            vec![Action::Transfer(TransferAction { deposit })],
            block_hash,
        )
    }

    pub fn stake(
        nonce: Nonce,
        signer_id: AccountId,
        signer: &dyn Signer,
        stake: Balance,
        public_key: PublicKey,
        block_hash: CryptoHash,
    ) -> Self {
        Self::from_actions(
            nonce,
            signer_id.clone(),
            signer_id,
            signer,
            vec![Action::Stake(StakeAction { stake, public_key })],
            block_hash,
        )
    }

    pub fn create_account(
        nonce: Nonce,
        originator: AccountId,
        new_account_id: AccountId,
        amount: Balance,
        public_key: PublicKey,
        signer: &dyn Signer,
        block_hash: CryptoHash,
    ) -> Self {
        Self::from_actions(
            nonce,
            originator,
            new_account_id,
            signer,
            vec![
                Action::CreateAccount(CreateAccountAction {}),
                Action::AddKey(AddKeyAction {
                    public_key,
                    access_key: AccessKey { nonce: 0, permission: AccessKeyPermission::FullAccess },
                }),
                Action::Transfer(TransferAction { deposit: amount }),
            ],
            block_hash,
        )
    }

    pub fn empty(block_hash: CryptoHash) -> Self {
        Self::from_actions(0, "".to_string(), "".to_string(), &EmptySigner {}, vec![], block_hash)
    }
}

impl Block {
    pub fn empty_with_epoch(
        prev: &Block,
        height: BlockHeight,
        epoch_id: EpochId,
        next_epoch_id: EpochId,
        next_bp_hash: CryptoHash,
        signer: &dyn ValidatorSigner,
    ) -> Self {
        Self::empty_with_approvals(
            prev,
            height,
            epoch_id,
            next_epoch_id,
            vec![],
            signer,
            next_bp_hash,
        )
    }

    pub fn empty_with_height(
        prev: &Block,
        height: BlockHeight,
        signer: &dyn ValidatorSigner,
    ) -> Self {
        Self::empty_with_epoch(
            prev,
            height,
            prev.header.inner_lite.epoch_id.clone(),
            if prev.header.prev_hash == CryptoHash::default() {
                EpochId(prev.hash())
            } else {
                prev.header.inner_lite.next_epoch_id.clone()
            },
            prev.header.inner_lite.next_bp_hash,
            signer,
        )
    }

    pub fn empty(prev: &Block, signer: &dyn ValidatorSigner) -> Self {
        Self::empty_with_height(prev, prev.header.inner_lite.height + 1, signer)
    }

    /// This is not suppose to be used outside of chain tests, because this doesn't refer to correct chunks.
    /// Done because chain tests don't have a good way to store chunks right now.
    pub fn empty_with_approvals(
        prev: &Block,
        height: BlockHeight,
        epoch_id: EpochId,
        next_epoch_id: EpochId,
        approvals: Vec<Approval>,
        signer: &dyn ValidatorSigner,
        next_bp_hash: CryptoHash,
    ) -> Self {
        Block::produce(
            &prev.header,
            height,
            prev.chunks.clone(),
            epoch_id,
            next_epoch_id,
            approvals,
            0,
            0,
            Some(0),
            vec![],
            vec![],
            signer,
            0.into(),
            CryptoHash::default(),
            CryptoHash::default(),
            CryptoHash::default(),
            next_bp_hash,
        )
    }
}

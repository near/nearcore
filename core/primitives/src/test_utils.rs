use near_crypto::{EmptySigner, PublicKey, Signature, Signer};

use crate::account::{AccessKey, AccessKeyPermission, Account};
use crate::block::Block;
use crate::hash::CryptoHash;
use crate::transaction::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeployContractAction,
    FunctionCallAction, SignedTransaction, StakeAction, Transaction, TransferAction,
};
use crate::types::{AccountId, Balance, BlockHeight, EpochId, Gas, Nonce};
use crate::validator_signer::ValidatorSigner;
use num_rational::Rational;

pub fn account_new(amount: Balance, code_hash: CryptoHash) -> Account {
    Account { amount, locked: 0, code_hash, storage_usage: std::mem::size_of::<Account>() as u64 }
}

impl Transaction {
    pub fn sign(self, signer: &dyn Signer) -> SignedTransaction {
        let signature = signer.sign(self.get_hash().as_ref());
        SignedTransaction::new(signature, self)
    }
}

impl SignedTransaction {
    pub fn from_actions(
        nonce: Nonce,
        signer_id: AccountId,
        receiver_id: AccountId,
        signer: &dyn Signer,
        actions: Vec<Action>,
        block_hash: CryptoHash,
    ) -> Self {
        Transaction {
            nonce,
            signer_id,
            public_key: signer.public_key(),
            receiver_id,
            block_hash,
            actions,
        }
        .sign(signer)
    }

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

    pub fn create_contract(
        nonce: Nonce,
        originator: AccountId,
        new_account_id: AccountId,
        code: Vec<u8>,
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
                Action::DeployContract(DeployContractAction { code }),
            ],
            block_hash,
        )
    }

    pub fn call(
        nonce: Nonce,
        signer_id: AccountId,
        receiver_id: AccountId,
        signer: &dyn Signer,
        deposit: Balance,
        method_name: String,
        args: Vec<u8>,
        gas: Gas,
        block_hash: CryptoHash,
    ) -> Self {
        Self::from_actions(
            nonce,
            signer_id,
            receiver_id,
            signer,
            vec![Action::FunctionCall(FunctionCallAction { args, method_name, gas, deposit })],
            block_hash,
        )
    }

    pub fn delete_account(
        nonce: Nonce,
        signer_id: AccountId,
        receiver_id: AccountId,
        beneficiary_id: AccountId,
        signer: &dyn Signer,
        block_hash: CryptoHash,
    ) -> Self {
        Self::from_actions(
            nonce,
            signer_id,
            receiver_id,
            signer,
            vec![Action::DeleteAccount(DeleteAccountAction { beneficiary_id })],
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
        approvals: Vec<Option<Signature>>,
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
            Rational::from_integer(0),
            0,
            Some(0),
            vec![],
            vec![],
            signer,
            next_bp_hash,
        )
    }
}

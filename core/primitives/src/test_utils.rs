use std::collections::HashMap;

use num_rational::Rational;

use near_crypto::{EmptySigner, PublicKey, Signature, Signer};

use crate::account::{AccessKey, AccessKeyPermission, Account};
use crate::block::Block;
use crate::block_header::BlockHeader;
#[cfg(not(feature = "protocol_feature_block_header_v3"))]
use crate::block_header::BlockHeaderV2;
#[cfg(feature = "protocol_feature_block_header_v3")]
use crate::block_header::BlockHeaderV3;
use crate::errors::{EpochError, TxExecutionError};
use crate::hash::CryptoHash;
use crate::merkle::PartialMerkleTree;
use crate::serialize::from_base64;
use crate::sharding::ShardChunkHeader;
use crate::transaction::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
    DeployContractAction, FunctionCallAction, SignedTransaction, StakeAction, Transaction,
    TransferAction,
};
use crate::types::{AccountId, Balance, BlockHeight, EpochId, EpochInfoProvider, Gas, Nonce};
use crate::validator_signer::ValidatorSigner;
use crate::version::PROTOCOL_VERSION;
use crate::views::FinalExecutionStatus;

pub fn account_new(amount: Balance, code_hash: CryptoHash) -> Account {
    Account::new(amount, 0, code_hash, std::mem::size_of::<Account>() as u64)
}

impl Transaction {
    pub fn new(
        signer_id: AccountId,
        public_key: PublicKey,
        receiver_id: AccountId,
        nonce: Nonce,
        block_hash: CryptoHash,
    ) -> Self {
        Self { signer_id, public_key, nonce, receiver_id, block_hash, actions: vec![] }
    }

    pub fn sign(self, signer: &dyn Signer) -> SignedTransaction {
        let signature = signer.sign(self.get_hash_and_size().0.as_ref());
        SignedTransaction::new(signature, self)
    }

    pub fn create_account(mut self) -> Self {
        self.actions.push(Action::CreateAccount(CreateAccountAction {}));
        self
    }

    pub fn deploy_contract(mut self, code: Vec<u8>) -> Self {
        self.actions.push(Action::DeployContract(DeployContractAction { code }));
        self
    }

    pub fn function_call(
        mut self,
        method_name: String,
        args: Vec<u8>,
        gas: Gas,
        deposit: Balance,
    ) -> Self {
        self.actions.push(Action::FunctionCall(FunctionCallAction {
            method_name,
            args,
            gas,
            deposit,
        }));
        self
    }

    pub fn transfer(mut self, deposit: Balance) -> Self {
        self.actions.push(Action::Transfer(TransferAction { deposit }));
        self
    }

    pub fn stake(mut self, stake: Balance, public_key: PublicKey) -> Self {
        self.actions.push(Action::Stake(StakeAction { stake, public_key }));
        self
    }
    pub fn add_key(mut self, public_key: PublicKey, access_key: AccessKey) -> Self {
        self.actions.push(Action::AddKey(AddKeyAction { public_key, access_key }));
        self
    }

    pub fn delete_key(mut self, public_key: PublicKey) -> Self {
        self.actions.push(Action::DeleteKey(DeleteKeyAction { public_key }));
        self
    }

    pub fn delete_account(mut self, beneficiary_id: AccountId) -> Self {
        self.actions.push(Action::DeleteAccount(DeleteAccountAction { beneficiary_id }));
        self
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

impl BlockHeader {
    #[cfg(feature = "protocol_feature_block_header_v3")]
    pub fn get_mut(&mut self) -> &mut BlockHeaderV3 {
        match self {
            BlockHeader::BlockHeaderV1(_) | BlockHeader::BlockHeaderV2(_) => {
                panic!("old header should not appear in tests")
            }
            BlockHeader::BlockHeaderV3(header) => header,
        }
    }

    #[cfg(not(feature = "protocol_feature_block_header_v3"))]
    pub fn get_mut(&mut self) -> &mut BlockHeaderV2 {
        match self {
            BlockHeader::BlockHeaderV1(_) => panic!("old header should not appear in tests"),
            BlockHeader::BlockHeaderV2(header) => header,
        }
    }

    pub fn resign(&mut self, signer: &dyn ValidatorSigner) {
        let (hash, signature) = signer.sign_block_header_parts(
            *self.prev_hash(),
            &self.inner_lite_bytes(),
            &self.inner_rest_bytes(),
        );
        let mut header = self.get_mut();
        header.hash = hash;
        header.signature = signature;
    }
}

impl Block {
    pub fn mut_header(&mut self) -> &mut BlockHeader {
        match self {
            Block::BlockV1(block) => &mut block.header,
            Block::BlockV2(block) => &mut block.header,
        }
    }

    pub fn set_chunks(&mut self, chunks: Vec<ShardChunkHeader>) {
        match self {
            Block::BlockV1(block) => {
                let legacy_chunks = chunks
                    .into_iter()
                    .map(|chunk| match chunk {
                        ShardChunkHeader::V1(header) => header,
                        ShardChunkHeader::V2(_) => {
                            panic!("Attempted to set V1 block chunks with V2")
                        }
                        #[cfg(feature = "protocol_feature_block_header_v3")]
                        ShardChunkHeader::V3(_) => {
                            panic!("Attempted to set V1 block chunks with V3")
                        }
                    })
                    .collect();
                block.as_mut().chunks = legacy_chunks;
            }
            Block::BlockV2(block) => {
                block.as_mut().chunks = chunks;
            }
        }
    }

    pub fn empty_with_epoch(
        prev: &Block,
        height: BlockHeight,
        epoch_id: EpochId,
        next_epoch_id: EpochId,
        next_bp_hash: CryptoHash,
        signer: &dyn ValidatorSigner,
        block_merkle_tree: &mut PartialMerkleTree,
    ) -> Self {
        block_merkle_tree.insert(*prev.hash());
        Self::empty_with_approvals(
            prev,
            height,
            epoch_id,
            next_epoch_id,
            vec![],
            signer,
            next_bp_hash,
            block_merkle_tree.root(),
        )
    }

    pub fn empty_with_height(
        prev: &Block,
        height: BlockHeight,
        signer: &dyn ValidatorSigner,
    ) -> Self {
        Self::empty_with_height_and_block_merkle_tree(
            prev,
            height,
            signer,
            &mut PartialMerkleTree::default(),
        )
    }

    pub fn empty_with_height_and_block_merkle_tree(
        prev: &Block,
        height: BlockHeight,
        signer: &dyn ValidatorSigner,
        block_merkle_tree: &mut PartialMerkleTree,
    ) -> Self {
        Self::empty_with_epoch(
            prev,
            height,
            prev.header().epoch_id().clone(),
            if prev.header().prev_hash() == &CryptoHash::default() {
                EpochId(*prev.hash())
            } else {
                prev.header().next_epoch_id().clone()
            },
            *prev.header().next_bp_hash(),
            signer,
            block_merkle_tree,
        )
    }

    pub fn empty_with_block_merkle_tree(
        prev: &Block,
        signer: &dyn ValidatorSigner,
        block_merkle_tree: &mut PartialMerkleTree,
    ) -> Self {
        Self::empty_with_height_and_block_merkle_tree(
            prev,
            prev.header().height() + 1,
            signer,
            block_merkle_tree,
        )
    }

    pub fn empty(prev: &Block, signer: &dyn ValidatorSigner) -> Self {
        Self::empty_with_block_merkle_tree(prev, signer, &mut PartialMerkleTree::default())
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
        block_merkle_root: CryptoHash,
    ) -> Self {
        Block::produce(
            PROTOCOL_VERSION,
            prev.header(),
            height,
            #[cfg(feature = "protocol_feature_block_header_v3")]
            (prev.header().block_ordinal() + 1),
            prev.chunks().iter().cloned().collect(),
            epoch_id,
            next_epoch_id,
            #[cfg(feature = "protocol_feature_block_header_v3")]
            None,
            approvals,
            Rational::from_integer(0),
            0,
            0,
            Some(0),
            vec![],
            vec![],
            signer,
            next_bp_hash,
            block_merkle_root,
        )
    }
}

#[derive(Default)]
pub struct MockEpochInfoProvider {
    pub validators: HashMap<AccountId, Balance>,
}

impl MockEpochInfoProvider {
    pub fn new(validators: impl Iterator<Item = (AccountId, Balance)>) -> Self {
        MockEpochInfoProvider { validators: validators.collect() }
    }
}

impl EpochInfoProvider for MockEpochInfoProvider {
    fn validator_stake(
        &self,
        _epoch_id: &EpochId,
        _last_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<Option<Balance>, EpochError> {
        Ok(self.validators.get(account_id).cloned())
    }

    fn validator_total_stake(
        &self,
        _epoch_id: &EpochId,
        _last_block_hash: &CryptoHash,
    ) -> Result<Balance, EpochError> {
        Ok(self.validators.values().sum())
    }

    fn minimum_stake(&self, _prev_block_hash: &CryptoHash) -> Result<Balance, EpochError> {
        Ok(0)
    }
}

impl FinalExecutionStatus {
    pub fn as_success(self) -> Option<String> {
        match self {
            FinalExecutionStatus::SuccessValue(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_failure(self) -> Option<TxExecutionError> {
        match self {
            FinalExecutionStatus::Failure(failure) => Some(failure),
            _ => None,
        }
    }

    pub fn as_success_decoded(self) -> Option<Vec<u8>> {
        self.as_success().and_then(|value| from_base64(&value).ok())
    }
}

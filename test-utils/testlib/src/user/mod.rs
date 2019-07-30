use std::sync::Arc;

use futures::Future;

use near_chain::Block;
use near_primitives::account::AccessKey;
use near_primitives::crypto::signature::PublicKey;
use near_primitives::crypto::signer::EDSigner;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::ReceiptInfo;
use near_primitives::rpc::{AccountViewCallResult, ViewStateResult};
use near_primitives::serialize::BaseEncode;
use near_primitives::transaction::{
    CreateAccountTransaction, DeleteAccountTransaction, FinalTransactionResult,
    FunctionCallTransaction, ReceiptTransaction, SendMoneyTransaction, SignedTransaction,
    StakeTransaction, TransactionBody, TransactionResult,
};
use near_primitives::types::{AccountId, Balance, MerkleHash};

pub use crate::user::runtime_user::RuntimeUser;

pub mod rpc_user;
pub mod runtime_user;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

pub trait User {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountViewCallResult, String>;

    fn view_balance(&self, account_id: &AccountId) -> Result<Balance, String> {
        Ok(self.view_account(account_id)?.amount)
    }

    fn view_state(&self, account_id: &AccountId) -> Result<ViewStateResult, String>;

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String>;

    fn commit_transaction(
        &self,
        transaction: SignedTransaction,
    ) -> Result<FinalTransactionResult, String>;

    fn add_receipt(&self, receipt: ReceiptTransaction) -> Result<(), String>;

    fn get_account_nonce(&self, account_id: &AccountId) -> Option<u64>;

    fn get_best_block_index(&self) -> Option<u64>;

    fn get_block(&self, index: u64) -> Option<Block>;

    fn get_transaction_result(&self, hash: &CryptoHash) -> TransactionResult;

    fn get_transaction_final_result(&self, hash: &CryptoHash) -> FinalTransactionResult;

    fn get_state_root(&self) -> MerkleHash;

    fn get_receipt_info(&self, hash: &CryptoHash) -> Option<ReceiptInfo>;

    fn get_access_key(
        &self,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<Option<AccessKey>, String>;

    fn signer(&self) -> Arc<dyn EDSigner>;

    fn sign_and_commit_transaction(&self, body: TransactionBody) -> FinalTransactionResult {
        let tx = body.sign(&*self.signer());
        self.commit_transaction(tx).unwrap()
    }

    fn send_money(
        &self,
        originator_id: AccountId,
        receiver_id: AccountId,
        amount: Balance,
    ) -> FinalTransactionResult {
        self.sign_and_commit_transaction(TransactionBody::SendMoney(SendMoneyTransaction {
            nonce: self.get_account_nonce(&originator_id).unwrap_or_default() + 1,
            originator: originator_id,
            receiver: receiver_id,
            amount,
        }))
    }

    fn function_call(
        &self,
        originator_id: AccountId,
        contract_id: AccountId,
        method_name: &str,
        args: Vec<u8>,
        amount: Balance,
    ) -> FinalTransactionResult {
        self.sign_and_commit_transaction(TransactionBody::FunctionCall(FunctionCallTransaction {
            nonce: self.get_account_nonce(&originator_id).unwrap_or_default() + 1,
            originator: originator_id,
            contract_id,
            method_name: method_name.as_bytes().to_vec(),
            args,
            amount,
        }))
    }

    fn create_account(
        &self,
        originator_id: AccountId,
        new_account_id: AccountId,
        public_key: PublicKey,
        amount: Balance,
    ) -> FinalTransactionResult {
        self.sign_and_commit_transaction(TransactionBody::CreateAccount(CreateAccountTransaction {
            nonce: self.get_account_nonce(&originator_id).unwrap_or_default() + 1,
            originator: originator_id,
            new_account_id,
            public_key: public_key.0[..].to_vec(),
            amount,
        }))
    }

    fn delete_account(
        &self,
        originator_id: AccountId,
        receiver_id: AccountId,
    ) -> FinalTransactionResult {
        self.sign_and_commit_transaction(TransactionBody::DeleteAccount(DeleteAccountTransaction {
            nonce: self.get_account_nonce(&originator_id).unwrap_or_default() + 1,
            originator_id,
            receiver_id,
        }))
    }

    fn stake(
        &self,
        originator_id: AccountId,
        public_key: PublicKey,
        amount: Balance,
    ) -> FinalTransactionResult {
        self.sign_and_commit_transaction(
            TransactionBody::Stake(StakeTransaction {
                nonce: self.get_account_nonce(&originator_id).unwrap_or_default() + 1,
                originator: originator_id,
                amount,
                public_key: public_key.to_base(),
            }),
        )
    }
}

/// Same as `User` by provides async API that can be used inside tokio.
pub trait AsyncUser: Send + Sync {
    fn view_account(
        &self,
        account_id: &AccountId,
    ) -> Box<dyn Future<Item = AccountViewCallResult, Error = String>>;

    fn view_balance(
        &self,
        account_id: &AccountId,
    ) -> Box<dyn Future<Item = Balance, Error = String>> {
        Box::new(self.view_account(account_id).map(|acc| acc.amount))
    }

    fn view_state(
        &self,
        account_id: &AccountId,
    ) -> Box<dyn Future<Item = ViewStateResult, Error = String>>;

    fn add_transaction(
        &self,
        transaction: SignedTransaction,
    ) -> Box<dyn Future<Item = (), Error = String> + Send>;

    fn add_receipt(
        &self,
        receipt: ReceiptTransaction,
    ) -> Box<dyn Future<Item = (), Error = String>>;

    fn get_account_nonce(
        &self,
        account_id: &AccountId,
    ) -> Box<dyn Future<Item = u64, Error = String>>;

    fn get_best_block_index(&self) -> Box<dyn Future<Item = u64, Error = String>>;

    fn get_transaction_result(
        &self,
        hash: &CryptoHash,
    ) -> Box<dyn Future<Item = TransactionResult, Error = String>>;

    fn get_transaction_final_result(
        &self,
        hash: &CryptoHash,
    ) -> Box<dyn Future<Item = FinalTransactionResult, Error = String>>;

    fn get_state_root(&self) -> Box<dyn Future<Item = MerkleHash, Error = String>>;

    fn get_receipt_info(
        &self,
        hash: &CryptoHash,
    ) -> Box<dyn Future<Item = ReceiptInfo, Error = String>>;

    fn get_access_key(
        &self,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Box<dyn Future<Item = Option<AccessKey>, Error = String>>;
}

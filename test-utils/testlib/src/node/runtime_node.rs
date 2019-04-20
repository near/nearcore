use crate::node::{Node, ProcessNode, ThreadNode};
use crate::runtime_utils::get_runtime_and_trie;
use crate::user::{User, RuntimeUser};
use crate::user::runtime_user::MockClient;

use primitives::crypto::signer::InMemorySigner;
use primitives::transaction::{FunctionCallTransaction, TransactionBody};
use primitives::types::{AccountId, Balance};

use std::sync::{Arc, RwLock};

pub struct RuntimeNode {
    pub client: Arc<RwLock<MockClient>>,
    pub signer: Arc<InMemorySigner>,
}

impl RuntimeNode {
    pub fn new(account_id: &AccountId) -> Self {
        let signer = Arc::new(InMemorySigner::from_seed(account_id, account_id));
        let (runtime, trie, root) = get_runtime_and_trie();
        let client = Arc::new(RwLock::new(MockClient { runtime, trie, state_root: root }));
        RuntimeNode { signer, client }
    }


    pub fn send_money(&self, account_id: &AccountId, amount: Balance) {
        let nonce = self.get_account_nonce(account_id).unwrap_or_default() + 1;
        let transaction =
            TransactionBody::send_money(nonce, self.account_id().unwrap(), account_id, amount)
                .sign(&*self.signer());
        self.user().add_transaction(transaction).unwrap();
    }

    pub fn call_function(
        &self,
        contract_id: &str,
        method_name: &str,
        args: Vec<u8>,
        amount: Balance,
    ) {
        let account_id = self.account_id().unwrap();
        let nonce = self.get_account_nonce(account_id).unwrap_or_default() + 1;
        let transaction = TransactionBody::FunctionCall(FunctionCallTransaction {
            nonce,
            originator: account_id.to_string(),
            contract_id: contract_id.to_string(),
            method_name: method_name.as_bytes().to_vec(),
            args,
            amount,
        })
        .sign(&*self.signer());
        self.user().add_transaction(transaction).unwrap();
    }
}

impl Node for RuntimeNode {
    fn account_id(&self) -> Option<&AccountId> {
        Some(&self.signer.account_id)
    }

    fn start(&mut self) {}

    fn kill(&mut self) {}

    fn signer(&self) -> Arc<InMemorySigner> {
        self.signer.clone()
    }

    fn as_process_mut(&mut self) -> &mut ProcessNode {
        unimplemented!()
    }

    fn as_thread_mut(&mut self) -> &mut ThreadNode {
        unimplemented!()
    }

    fn is_running(&self) -> bool {
        true
    }

    fn user(&self) -> Box<dyn User> {
        Box::new(RuntimeUser::new(&self.signer.account_id, self.client.clone()))
    }
}

use std::sync::{Arc, RwLock};

use near_primitives::crypto::signer::{EDSigner, InMemorySigner};
use near_primitives::transaction::{FunctionCallTransaction, TransactionBody};
use near_primitives::types::{AccountId, Balance};

use crate::node::Node;
use crate::runtime_utils::get_runtime_and_trie;
use crate::user::runtime_user::MockClient;
use crate::user::{RuntimeUser, User};

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
        let nonce = self.get_account_nonce(&self.account_id().unwrap()).unwrap_or_default() + 1;
        let transaction =
            TransactionBody::send_money(nonce, &self.account_id().unwrap(), account_id, amount)
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
        let nonce = self.get_account_nonce(&account_id).unwrap_or_default() + 1;
        let transaction = TransactionBody::FunctionCall(FunctionCallTransaction {
            nonce,
            originator_id: account_id.to_string(),
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
    fn account_id(&self) -> Option<AccountId> {
        Some(self.signer.account_id.clone())
    }

    fn start(&mut self) {}

    fn kill(&mut self) {}

    fn signer(&self) -> Arc<dyn EDSigner> {
        self.signer.clone()
    }

    fn is_running(&self) -> bool {
        true
    }

    fn user(&self) -> Box<dyn User> {
        Box::new(RuntimeUser::new(&self.signer.account_id, self.client.clone()))
    }
}

#[cfg(test)]
mod tests {
    use crate::node::runtime_node::RuntimeNode;
    use crate::node::Node;

    #[test]
    pub fn test_send_money() {
        let node = RuntimeNode::new(&"alice.near".to_string());
        node.send_money(&"bob.near".to_string(), 1);
        let (alice1, bob1) = (
            node.view_balance(&"alice.near".to_string()).unwrap(),
            node.view_balance(&"bob.near".to_string()).unwrap(),
        );
        node.send_money(&"bob.near".to_string(), 1);
        let (alice2, bob2) = (
            node.view_balance(&"alice.near".to_string()).unwrap(),
            node.view_balance(&"bob.near".to_string()).unwrap(),
        );
        assert_eq!(alice2, alice1 - 1);
        assert_eq!(bob2, bob1 + 1);
    }

}

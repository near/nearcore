use std::sync::{Arc, RwLock};

use near::GenesisConfig;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_primitives::types::AccountId;

use crate::node::Node;
use crate::runtime_utils::{alice_account, bob_account, get_runtime_and_trie_from_genesis};
use crate::user::runtime_user::MockClient;
use crate::user::{RuntimeUser, User};

pub struct RuntimeNode {
    pub client: Arc<RwLock<MockClient>>,
    pub signer: Arc<InMemorySigner>,
}

impl RuntimeNode {
    pub fn new(account_id: &AccountId) -> Self {
        let genesis_config =
            GenesisConfig::test(vec![&alice_account(), &bob_account(), "carol.near"], 3);
        Self::new_from_genesis(account_id, genesis_config)
    }

    pub fn new_from_genesis(account_id: &AccountId, genesis_config: GenesisConfig) -> Self {
        let signer = Arc::new(InMemorySigner::from_seed(account_id, KeyType::ED25519, account_id));
        let (runtime, trie, root) = get_runtime_and_trie_from_genesis(&genesis_config);
        let client = Arc::new(RwLock::new(MockClient {
            runtime,
            trie,
            state_root: root,
            epoch_length: genesis_config.epoch_length,
        }));
        RuntimeNode { signer, client }
    }

    pub fn free(account_id: &AccountId) -> Self {
        let genesis_config =
            GenesisConfig::test_free(vec![&alice_account(), &bob_account(), "carol.near"], 3);
        Self::new_from_genesis(account_id, genesis_config)
    }
}

impl Node for RuntimeNode {
    fn account_id(&self) -> Option<AccountId> {
        Some(self.signer.account_id.clone())
    }

    fn start(&mut self) {}

    fn kill(&mut self) {}

    fn signer(&self) -> Arc<dyn Signer> {
        self.signer.clone()
    }

    fn block_signer(&self) -> Arc<dyn Signer> {
        self.signer.clone()
    }

    fn is_running(&self) -> bool {
        true
    }

    fn user(&self) -> Box<dyn User> {
        Box::new(RuntimeUser::new(
            &self.signer.account_id,
            self.signer.clone(),
            self.client.clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::fees_utils::{gas_burnt_to_reward, transfer_cost};
    use crate::node::runtime_node::RuntimeNode;
    use crate::node::Node;
    use crate::runtime_utils::{alice_account, bob_account};

    #[test]
    pub fn test_send_money() {
        let node = RuntimeNode::new(&"alice.near".to_string());
        let node_user = node.user();
        let transaction_result = node_user.send_money(alice_account(), bob_account(), 1).unwrap();
        let transfer_cost = transfer_cost();
        let (alice1, bob1) = (
            node.view_balance(&alice_account()).unwrap(),
            node.view_balance(&bob_account()).unwrap(),
        );
        node_user.send_money(alice_account(), bob_account(), 1).unwrap();
        let (alice2, bob2) = (
            node.view_balance(&alice_account()).unwrap(),
            node.view_balance(&bob_account()).unwrap(),
        );
        assert_eq!(alice2, alice1 - 1 - transfer_cost);
        assert_eq!(bob2, bob1 + 1);
    }
}

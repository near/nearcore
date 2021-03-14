use std::sync::{Arc, RwLock};

use near_chain_configs::Genesis;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_primitives::account::Account;
use near_primitives::hash::CryptoHash;
use near_primitives::state_record::StateRecord;
use near_primitives::types::AccountId;
use neard::config::{GenesisExt, TESTING_INIT_BALANCE};

use crate::node::Node;
use crate::runtime_utils::{
    add_test_contract, alice_account, bob_account, evm_account, get_runtime_and_trie_from_genesis,
};
use crate::user::runtime_user::MockClient;
use crate::user::{RuntimeUser, User};

pub struct RuntimeNode {
    pub client: Arc<RwLock<MockClient>>,
    pub signer: Arc<InMemorySigner>,
    pub genesis: Genesis,
}

impl RuntimeNode {
    pub fn new(account_id: &AccountId) -> Self {
        let mut genesis = Genesis::test(vec![&alice_account(), &bob_account(), "carol.near"], 3);
        add_test_contract(&mut genesis, &alice_account());
        add_test_contract(&mut genesis, &bob_account());
        genesis.records.as_mut().push(StateRecord::Account {
            account_id: evm_account(),
            account: Account {
                amount: TESTING_INIT_BALANCE,
                locked: 0,
                code_hash: CryptoHash::default(),
                storage_usage: 0,
            },
        });
        Self::new_from_genesis(account_id, genesis)
    }

    pub fn new_from_genesis(account_id: &AccountId, genesis: Genesis) -> Self {
        let signer = Arc::new(InMemorySigner::from_seed(account_id, KeyType::ED25519, account_id));
        let (runtime, tries, root) = get_runtime_and_trie_from_genesis(&genesis);
        let client = Arc::new(RwLock::new(MockClient {
            runtime,
            tries,
            state_root: root,
            epoch_length: genesis.config.epoch_length,
            runtime_config: genesis.config.runtime_config.clone(),
        }));
        RuntimeNode { signer, client, genesis }
    }

    pub fn free(account_id: &AccountId) -> Self {
        let mut genesis =
            Genesis::test_free(vec![&alice_account(), &bob_account(), "carol.near"], 3);
        add_test_contract(&mut genesis, &bob_account());
        Self::new_from_genesis(account_id, genesis)
    }
}

impl Node for RuntimeNode {
    fn genesis(&self) -> &Genesis {
        &self.genesis
    }

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
    use crate::fees_utils::FeeHelper;
    use crate::node::runtime_node::RuntimeNode;
    use crate::node::Node;
    use crate::runtime_utils::{alice_account, bob_account};

    #[test]
    pub fn test_send_money() {
        let node = RuntimeNode::new(&"alice.near".to_string());
        let node_user = node.user();
        let (alice1, bob1) = (
            node.view_balance(&alice_account()).unwrap(),
            node.view_balance(&bob_account()).unwrap(),
        );
        node_user.send_money(alice_account(), bob_account(), 1).unwrap();
        let fee_helper = FeeHelper::new(
            node.genesis().config.runtime_config.transaction_costs.clone(),
            node.genesis().config.min_gas_price,
        );
        let transfer_cost = fee_helper.transfer_cost();
        let (alice2, bob2) = (
            node.view_balance(&alice_account()).unwrap(),
            node.view_balance(&bob_account()).unwrap(),
        );
        assert_eq!(alice2, alice1 - 1 - transfer_cost);
        assert_eq!(bob2, bob1 + 1);
    }
}

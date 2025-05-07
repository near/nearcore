use std::sync::Arc;

use near_chain_configs::Genesis;
use near_crypto::{InMemorySigner, Signer};
use near_parameters::{RuntimeConfig, RuntimeConfigStore};
use near_primitives::types::{AccountId, Balance};
use parking_lot::RwLock;
use testlib::runtime_utils::{add_test_contract, alice_account, bob_account, carol_account};

use crate::node::Node;
use crate::user::runtime_user::MockClient;
use crate::user::{RuntimeUser, User};
use crate::utils::runtime_utils::get_runtime_and_trie_from_genesis;

pub struct RuntimeNode {
    pub client: Arc<RwLock<MockClient>>,
    pub signer: Arc<Signer>,
    pub genesis: Genesis,
    account_id: AccountId,
    gas_price: Balance,
}

impl RuntimeNode {
    pub fn new(account_id: &AccountId) -> Self {
        let mut genesis = Genesis::test(vec![alice_account(), bob_account(), carol_account()], 3);
        add_test_contract(&mut genesis, &alice_account());
        add_test_contract(&mut genesis, &bob_account());
        add_test_contract(&mut genesis, &carol_account());
        Self::new_from_genesis(account_id, genesis)
    }

    pub fn new_from_genesis_and_config(
        account_id: &AccountId,
        genesis: Genesis,
        runtime_config: RuntimeConfig,
    ) -> Self {
        let signer = Arc::new(InMemorySigner::test_signer(&account_id));
        let (runtime, tries, root) = get_runtime_and_trie_from_genesis(&genesis);
        let client = Arc::new(RwLock::new(MockClient {
            runtime,
            tries,
            state_root: root,
            epoch_length: genesis.config.epoch_length,
            runtime_config,
        }));
        let gas_price = genesis.config.min_gas_price;
        RuntimeNode { signer, client, genesis, account_id: account_id.clone(), gas_price }
    }

    pub fn new_from_genesis(account_id: &AccountId, genesis: Genesis) -> Self {
        let store = RuntimeConfigStore::new(None);
        let config = RuntimeConfig::clone(store.get_config(genesis.config.protocol_version));
        Self::new_from_genesis_and_config(account_id, genesis, config)
    }

    /// Same as `RuntimeNode::new`, but allows to modify the RuntimeConfig.
    pub fn new_with_modified_config(
        account_id: &AccountId,
        modify_config: impl FnOnce(&mut RuntimeConfig),
    ) -> Self {
        let mut genesis = Genesis::test(vec![alice_account(), bob_account(), carol_account()], 3);
        add_test_contract(&mut genesis, &alice_account());
        add_test_contract(&mut genesis, &bob_account());
        add_test_contract(&mut genesis, &carol_account());

        let store = RuntimeConfigStore::new(None);
        let mut runtime_config =
            RuntimeConfig::clone(store.get_config(genesis.config.protocol_version));
        modify_config(&mut runtime_config);
        Self::new_from_genesis_and_config(account_id, genesis, runtime_config)
    }

    pub fn free(account_id: &AccountId) -> Self {
        let mut genesis =
            Genesis::test(vec![alice_account(), bob_account(), "carol.near".parse().unwrap()], 3);
        genesis.config.min_gas_price = 0;
        add_test_contract(&mut genesis, &bob_account());
        Self::new_from_genesis_and_config(account_id, genesis, RuntimeConfig::free())
    }
}

impl Node for RuntimeNode {
    fn genesis(&self) -> &Genesis {
        &self.genesis
    }

    fn account_id(&self) -> Option<AccountId> {
        Some(self.account_id.clone())
    }

    fn start(&mut self) {}

    fn kill(&mut self) {}

    fn signer(&self) -> Arc<Signer> {
        self.signer.clone()
    }

    fn block_signer(&self) -> Arc<Signer> {
        self.signer.clone()
    }

    fn is_running(&self) -> bool {
        true
    }

    fn user(&self) -> Box<dyn User> {
        Box::new(RuntimeUser::new(
            self.account_id.clone(),
            self.signer.clone(),
            self.client.clone(),
            self.gas_price,
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::node::Node;
    use crate::node::runtime_node::RuntimeNode;
    use testlib::fees_utils::FeeHelper;
    use testlib::runtime_utils::{alice_account, bob_account};

    #[test]
    pub fn test_send_money() {
        let (alice, bob) = (alice_account(), bob_account());
        let node = RuntimeNode::new(&alice);
        let node_user = node.user();
        let (alice1, bob1) = (node.view_balance(&alice).unwrap(), node.view_balance(&bob).unwrap());
        node_user.send_money(alice.clone(), bob.clone(), 1).unwrap();
        let runtime_config = node.client.as_ref().read().runtime_config.clone();
        let fee_helper = FeeHelper::new(runtime_config, node.genesis().config.min_gas_price);
        let transfer_cost = fee_helper.transfer_cost();
        let (alice2, bob2) = (node.view_balance(&alice).unwrap(), node.view_balance(&bob).unwrap());
        assert_eq!(alice2, alice1 - 1 - transfer_cost);
        assert_eq!(bob2, bob1 + 1);
    }
}

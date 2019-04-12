use crate::node::{Node, ProcessNode, ThreadNode};
use crate::user::runtime_user::{MockClient, RuntimeUser};
use crate::user::User;
use node_runtime::test_utils::get_runtime_and_trie;
use primitives::crypto::signer::InMemorySigner;
use primitives::types::AccountId;
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

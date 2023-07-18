use std::sync::Arc;

use crate::runtime_utils::get_runtime_and_trie_from_genesis;
use near_chain_configs::Genesis;
use near_crypto::InMemorySigner;
use near_primitives::{
    receipt::Receipt,
    runtime::migration_data::{MigrationData, MigrationFlags},
    test_utils::MockEpochInfoProvider,
    transaction::{Action, SignedTransaction, TransferAction},
    types::{EpochInfoProvider, StateRoot},
};
use near_primitives_core::{types::AccountId, version::PROTOCOL_VERSION};
use near_store::{PartialStorage, ShardTries, ShardUId, Trie};
use nearcore::config::{GenesisExt, MIN_GAS_PRICE};
use node_runtime::{config::RuntimeConfig, ApplyState, Runtime};
use testlib::runtime_utils::{alice_account, bob_account};

struct StateWitnessTestFixture {
    pub runtime: Runtime,
    pub tries: ShardTries,
    pub state_root: StateRoot,
    pub runtime_config: RuntimeConfig,
    pub epoch_info_provider: Arc<dyn EpochInfoProvider>,
}

impl StateWitnessTestFixture {
    pub fn new() -> Self {
        let genesis = Genesis::test(vec![alice_account(), bob_account()], 2);
        let (runtime, tries, state_root) = get_runtime_and_trie_from_genesis(&genesis);
        let runtime_config = RuntimeConfig::test();
        let epoch_info_provider = Arc::new(MockEpochInfoProvider::default());
        Self { runtime, tries, state_root, runtime_config, epoch_info_provider }
    }

    fn apply_state(&self) -> ApplyState {
        ApplyState {
            block_height: 1,
            prev_block_hash: Default::default(),
            block_hash: Default::default(),
            block_timestamp: 0,
            epoch_height: 0,
            gas_price: MIN_GAS_PRICE,
            gas_limit: None,
            random_seed: Default::default(),
            epoch_id: Default::default(),
            current_protocol_version: PROTOCOL_VERSION,
            config: Arc::new(self.runtime_config.clone()),
            cache: None,
            is_new_chunk: true,
            migration_data: Arc::new(MigrationData::default()),
            migration_flags: MigrationFlags::default(),
        }
    }

    pub fn sign_tx(
        &self,
        signer_id: AccountId,
        nonce: u64,
        receiver_id: AccountId,
        actions: Vec<Action>,
    ) -> SignedTransaction {
        SignedTransaction::from_actions(
            nonce,
            signer_id.clone(),
            receiver_id,
            &InMemorySigner::from_seed(
                signer_id.clone(),
                near_crypto::KeyType::ED25519,
                &signer_id,
            ),
            actions,
            Default::default(),
        )
    }

    /// Apply the given transition and also record a state proof.
    pub fn transition_for_recording(
        &self,
        request: &TransitionRequest,
    ) -> (TransitionResult, PartialStorage) {
        let trie = self
            .tries
            .get_trie_for_shard(ShardUId::single_shard(), request.state_root)
            .recording_reads();
        let result = self
            .runtime
            .apply(
                trie,
                &None,
                &self.apply_state(),
                &request.receipts,
                &request.transactions,
                &*self.epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        let mut update = self.tries.store_update();
        self.tries.apply_all(&result.trie_changes, ShardUId::single_shard(), &mut update);
        update.commit().unwrap();
        return (
            TransitionResult {
                outgoing_receipts: result.outgoing_receipts,
                new_state_root: result.state_root,
            },
            result.proof.unwrap(),
        );
    }

    /// Apply the given transition using the state proof as opposed to the
    /// storage.
    pub fn transition_for_replay(
        &self,
        request: &TransitionRequest,
        recorded: PartialStorage,
    ) -> TransitionResult {
        let trie = Trie::from_recorded_storage(recorded, request.state_root);
        let result = self
            .runtime
            .apply(
                trie,
                &None,
                &self.apply_state(),
                &request.receipts,
                &request.transactions,
                &*self.epoch_info_provider,
                Default::default(),
            )
            .unwrap();
        TransitionResult {
            outgoing_receipts: result.outgoing_receipts,
            new_state_root: result.state_root,
        }
    }
}

struct TransitionRequest {
    pub receipts: Vec<Receipt>,
    pub transactions: Vec<SignedTransaction>,
    pub state_root: StateRoot,
}

#[derive(Debug, PartialEq, Eq)]
struct TransitionResult {
    pub outgoing_receipts: Vec<Receipt>,
    pub new_state_root: StateRoot,
}

#[test]
fn test_basic() {
    let f = StateWitnessTestFixture::new();

    // We'll make a transaction and then in three rounds, apply it against
    // storage, record the state proof, and then re-apply it again using only
    // the state proof, and then assert that the results are identical.
    let request1 = TransitionRequest {
        receipts: vec![],
        transactions: vec![f.sign_tx(
            alice_account(),
            1,
            bob_account(),
            vec![Action::Transfer(TransferAction { deposit: 1 })],
        )],
        state_root: f.state_root,
    };
    let (result1_expected, recorded1) = f.transition_for_recording(&request1);
    let result1_actual = f.transition_for_replay(&request1, recorded1);
    assert_eq!(result1_expected, result1_actual);

    // Second request is the receipt on bob's side.
    let request2 = TransitionRequest {
        receipts: result1_expected.outgoing_receipts,
        transactions: vec![],
        state_root: result1_expected.new_state_root,
    };
    let (result2_expected, recorded2) = f.transition_for_recording(&request2);
    let result2_actual = f.transition_for_replay(&request2, recorded2);
    assert_eq!(result2_expected, result2_actual);

    // Third request is a system gas refund to alice.
    let request3 = TransitionRequest {
        receipts: result2_expected.outgoing_receipts,
        transactions: vec![],
        state_root: result2_expected.new_state_root,
    };

    let (result3_expected, recorded3) = f.transition_for_recording(&request3);
    let result3_actual = f.transition_for_replay(&request3, recorded3);
    assert_eq!(result3_expected, result3_actual);
}

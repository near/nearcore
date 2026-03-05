use crate::env::nightshade_setup::TestEnvNightshadeSetupExt;
use crate::env::test_env::TestEnv;
use assert_matches::assert_matches;
use near_chain::{Error, Provenance};
use near_chain_configs::test_genesis::{TestGenesisBuilder, ValidatorsSpec};
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::Balance;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_store::Trie;

/// Check that attempt to process block on top of incorrect state root fails with the expected error.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_invalid_chunk_state() {
    let genesis = TestGenesisBuilder::new()
        .epoch_length(5)
        .validators_spec(ValidatorsSpec::desired_roles(&["test0"], &[]))
        .add_user_account_simple("test0".parse().unwrap(), Balance::from_near(1_000_000_000))
        .build();
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();
    env.produce_block(0, 1);
    let block_hash = env.clients[0].chain.get_block_hash_by_height(1).unwrap();

    {
        let mut chunk_extra = ChunkExtra::clone(
            &env.clients[0].chain.get_chunk_extra(&block_hash, &ShardUId::single_shard()).unwrap(),
        );
        let store = env.clients[0].chain.mut_chain_store();
        let mut store_update = store.store_update();
        assert_ne!(chunk_extra.state_root(), &Trie::EMPTY_ROOT);
        *chunk_extra.state_root_mut() = Trie::EMPTY_ROOT;
        store_update.save_chunk_extra(&block_hash, &ShardUId::single_shard(), chunk_extra.into());
        store_update.commit().unwrap();
    }

    let block = env.clients[0].produce_block(2).unwrap().unwrap();
    let result = env.clients[0].process_block_test(block.into(), Provenance::NONE);
    assert_matches!(result.unwrap_err(), Error::InvalidStateRoot);
}

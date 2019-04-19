use std::sync::{Arc, RwLock};

use configs::authority::get_authority_config;
use configs::ChainSpec;
use primitives::beacon::{SignedBeaconBlock, SignedBeaconBlockHeader};
use storage::BeaconChainStorage;

use crate::authority::{get_authority, Authority};

pub type BeaconBlockChain =
    chain::BlockChain<SignedBeaconBlockHeader, SignedBeaconBlock, BeaconChainStorage>;

pub struct BeaconClient {
    pub chain: BeaconBlockChain,
    pub authority: RwLock<Box<Authority>>,
}

impl BeaconClient {
    pub fn new(
        genesis: SignedBeaconBlock,
        chain_spec: &ChainSpec,
        storage: Arc<RwLock<BeaconChainStorage>>,
    ) -> Self {
        let chain = chain::BlockChain::new(genesis, storage.clone());
        let authority_config = get_authority_config(chain_spec);
        let authority = RwLock::new(get_authority(authority_config, &chain, storage));
        BeaconClient { chain, authority }
    }
}

#[cfg(test)]
mod tests {
    use chain::test_utils::get_blockchain_storage;
    use chain::BlockChain;
    use configs::chain_spec::{AuthorityRotation, DefaultIdType};
    use primitives::block_traits::SignedBlock;
    use primitives::block_traits::SignedHeader;
    use primitives::crypto::signer::InMemorySigner;
    use primitives::hash::CryptoHash;
    use primitives::types::BlockId;
    use storage::test_utils::create_beacon_shard_storages;

    use super::*;

    fn get_beacon_client() -> (BeaconClient, SignedBeaconBlock) {
        let storage = create_beacon_shard_storages().0;
        let genesis =
            SignedBeaconBlock::new(0, CryptoHash::default(), vec![], CryptoHash::default());
        let chain_spec = ChainSpec::testing_spec(
            DefaultIdType::Named,
            3,
            1,
            AuthorityRotation::ThresholdedProofOfStake { epoch_length: 2, num_seats_per_slot: 1 },
        )
        .0;
        let beacon_client = BeaconClient::new(genesis.clone(), &chain_spec, storage);
        (beacon_client, genesis)
    }

    #[test]
    fn test_genesis() {
        let (bc, genesis) = get_beacon_client();
        assert_eq!(bc.chain.get_block(&BlockId::Hash(genesis.block_hash())).unwrap(), genesis);
        assert_eq!(bc.chain.get_block(&BlockId::Number(0)).unwrap(), genesis);
    }

    #[test]
    #[should_panic]
    fn test_invalid_genesis() {
        let (bc, _) = get_beacon_client();
        let storage = get_blockchain_storage(bc.chain);
        let invalid_genesis_block =
            SignedBeaconBlock::new(1, CryptoHash::default(), vec![], CryptoHash::default());
        let _ = BlockChain::new(invalid_genesis_block, storage);
    }

    #[test]
    fn test_restart_chain() {
        let (bc, genesis) = get_beacon_client();
        let mut block1 =
            SignedBeaconBlock::new(1, genesis.block_hash(), vec![], CryptoHash::default());
        let signer = Arc::new(InMemorySigner::from_random());
        let sig = block1.sign(&*signer);
        block1.add_signature(&sig, 0);
        bc.chain.insert_block(block1.clone());
        let best_block_header = bc.chain.best_header();
        assert_eq!(best_block_header.block_hash(), block1.block_hash());
        assert_eq!(best_block_header.index(), 1);
        // Create new BlockChain that reads from the same storage.
        let storage = get_blockchain_storage(bc.chain);
        let other_bc = BlockChain::new(genesis.clone(), storage);
        assert_eq!(other_bc.best_hash(), block1.block_hash());
        assert_eq!(other_bc.best_index(), 1);
        assert_eq!(other_bc.get_block(&BlockId::Hash(block1.block_hash())).unwrap(), block1);
    }

    #[test]
    fn test_light_client() {
        let (bc, genesis) = get_beacon_client();
        let block1 = SignedBeaconBlock::new(1, genesis.block_hash(), vec![], CryptoHash::default());
        let block1_hash = block1.block_hash();
        bc.chain.insert_header(block1.header());
        assert_eq!(bc.chain.best_index(), 1);
        assert!(bc.chain.is_known_header(&block1_hash));
        assert!(!bc.chain.is_known_block(&block1_hash));
        let block2 = SignedBeaconBlock::new(2, block1_hash, vec![], CryptoHash::default());
        bc.chain.insert_header(block2.header());
        assert_eq!(bc.chain.best_index(), 2);
        bc.chain.insert_block(block1);
        assert_eq!(bc.chain.best_index(), 2);
        assert!(bc.chain.is_known_block(&block1_hash));
    }

    //    fn test_fork_choice_rule_helper(graph: Vec<(u32, u32, usize)>, expect: u32) {
    //        let storage = Arc::new(create_memory_db());
    //
    //        let genesis =
    //            SignedBeaconBlock::new(0, CryptoHash::default(), vec![], CryptoHash::default());
    //        let bc = BlockChain::new(genesis.clone(), storage);
    //        let mut blocks: HashMap<u32, SignedBeaconBlock> = HashMap::new();
    //        blocks.insert(0, genesis.clone());
    //
    //        for (self_id, parent_id, sign_count) in graph.iter() {
    //            let mut block;
    //            {
    //                let parent = blocks.get(parent_id).unwrap();
    //                block = SignedBeaconBlock::new(
    //                    parent.body.header.index + 1,
    //                    parent.block_hash(),
    //                    vec![],
    //                    hash(&[*self_id as u8]),
    //                );
    //            }
    //            for i in 0..*sign_count {
    //                // Having proper signing here is far too slow, and unnecessary for this test
    //                let sig = BlsSignature::empty();
    //                block.add_signature(&sig, i);
    //            }
    //            blocks.insert(*self_id, block.clone());
    //            assert_eq!(bc.insert_block(block.clone()), false);
    //        }
    //        let best_hash = bc.best_block().block_hash();
    //        assert_eq!(best_hash, blocks.get(&expect).unwrap().block_hash());
    //    }
    //
    //    #[test]
    //    fn test_fork_choice_rule() {
    //        // First 3 examples from https://ethresear.ch/t/immediate-message-driven-ghost-as-ffg-fork-choice-rule/2561
    //
    //        //    15 - 16 - 65
    //        //  /
    //        // -
    //        //  \
    //        //    55 - 56
    //        //
    //        // We prefer the bottom fork, even though the top is longer.
    //        test_fork_choice_rule_helper(
    //            vec![(1, 0, 15), (2, 1, 16), (3, 2, 65), (4, 0, 55), (5, 4, 56)],
    //            5,
    //        );
    //        test_fork_choice_rule_helper(
    //            vec![(4, 0, 55), (5, 4, 56), (1, 0, 15), (2, 1, 16), (3, 2, 65)],
    //            5,
    //        );
    //
    //        //    15 - 51
    //        //  /
    //        // -
    //        //  \
    //        //    65 - 20
    //        test_fork_choice_rule_helper(vec![(1, 0, 15), (2, 0, 65), (3, 1, 51), (4, 2, 20)], 4);
    //
    //        //    40 - 51
    //        //  /
    //        // -
    //        //  \
    //        //    60 - 5
    //        test_fork_choice_rule_helper(vec![(1, 0, 40), (2, 0, 60), (3, 1, 51), (4, 2, 5)], 3);
    //
    //        //    65 - 20
    //        //  /
    //        // -
    //        //  \      35
    //        //   \   /
    //        //     30
    //        //       \
    //        //         40
    //        //
    //        // If we were using GHOST, we would prefer the bottom fork, because at each step the total
    //        // subtree weight of the lower fork is higher.  As is, we prefer the top fork.
    //        test_fork_choice_rule_helper(
    //            vec![(1, 0, 65), (2, 1, 20), (3, 0, 30), (4, 3, 35), (5, 3, 40)],
    //            2,
    //        );
    //    }
}

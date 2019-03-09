use std::sync::{Arc, RwLock};

use configs::authority::get_authority_config;
use configs::ChainSpec;
use primitives::beacon::{SignedBeaconBlock, SignedBeaconBlockHeader};
use storage::BeaconChainStorage;

use crate::authority::Authority;

pub type BeaconBlockChain =
    chain::BlockChain<SignedBeaconBlockHeader, SignedBeaconBlock, BeaconChainStorage>;

pub struct BeaconClient {
    pub chain: BeaconBlockChain,
    pub authority: RwLock<Authority>,
}

impl BeaconClient {
    pub fn new(genesis: SignedBeaconBlock, chain_spec: &ChainSpec, storage: Arc<RwLock<BeaconChainStorage>>) -> Self {
        let chain = chain::BlockChain::new(genesis, storage);
        let authority_config = get_authority_config(chain_spec);
        let authority = RwLock::new(Authority::new(authority_config, &chain));
        BeaconClient { chain, authority }
    }
}

#[cfg(test)]
mod tests {
    use chain::BlockChain;
    use chain::test_utils::get_blockchain_storage;
    use primitives::block_traits::SignedBlock;
    use primitives::block_traits::SignedHeader;
    use primitives::hash::CryptoHash;
    use primitives::signer::InMemorySigner;
    use primitives::types::BlockId;
    use storage::test_utils::create_beacon_shard_storages;

    use super::*;

    #[test]
    fn test_genesis() {
        let storage = create_beacon_shard_storages().0;
        let genesis =
            SignedBeaconBlock::new(0, CryptoHash::default(), vec![], CryptoHash::default());
        let bc = BlockChain::new(genesis.clone(), storage);
        assert_eq!(bc.get_block(&BlockId::Hash(genesis.block_hash())).unwrap(), genesis);
        assert_eq!(bc.get_block(&BlockId::Number(0)).unwrap(), genesis);
    }

    #[test]
    #[should_panic]
    fn test_invalid_genesis() {
        let storage = create_beacon_shard_storages().0;
        let genesis =
            SignedBeaconBlock::new(0, CryptoHash::default(), vec![], CryptoHash::default());
        let bc = BlockChain::new(genesis.clone(), storage);
        let storage = get_blockchain_storage(bc);
        let invalid_genesis_block = 
            SignedBeaconBlock::new(1, CryptoHash::default(), vec![], CryptoHash::default());
        let _ = BlockChain::new(invalid_genesis_block, storage);
    }

    #[test]
    fn test_restart_chain() {
        let storage = create_beacon_shard_storages().0;
        let genesis =
            SignedBeaconBlock::new(0, CryptoHash::default(), vec![], CryptoHash::default());
        let bc = BlockChain::new(genesis.clone(), storage.clone());
        let mut block1 =
            SignedBeaconBlock::new(1, genesis.block_hash(), vec![], CryptoHash::default());
        let signer = Arc::new(InMemorySigner::default());
        let sig = block1.sign(signer);
        block1.add_signature(&sig, 0);
        bc.insert_block(block1.clone());
        let best_block = bc.best_block();
        let best_block_header = best_block.header();
        assert_eq!(best_block.block_hash(), block1.block_hash());
        assert_eq!(best_block_header.block_hash(), block1.block_hash());
        assert_eq!(best_block_header.index(), 1);
        // Create new BlockChain that reads from the same storage.
        let other_bc = BlockChain::new(genesis.clone(), storage.clone());
        assert_eq!(other_bc.best_block().block_hash(), block1.block_hash());
        assert_eq!(other_bc.best_block().header().index(), 1);
        assert_eq!(other_bc.get_block(&BlockId::Hash(block1.block_hash())).unwrap(), block1);
    }

//    #[test]
//    fn test_two_chains() {
//        let storage = Arc::new(create_memory_db());
//        let genesis1 =
//            SignedBeaconBlock::new(0, CryptoHash::default(), vec![], CryptoHash::default());
//        let genesis2 =
//            SignedBeaconBlock::new(0, CryptoHash::default(), vec![], genesis1.block_hash());
//        let bc1 = BlockChain::new(genesis1.clone(), storage.clone());
//        let bc2 = BlockChain::new(genesis2.clone(), storage.clone());
//        assert_eq!(bc1.best_block().block_hash(), genesis1.block_hash());
//        assert_eq!(bc2.best_block().block_hash(), genesis2.block_hash());
//    }
//
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

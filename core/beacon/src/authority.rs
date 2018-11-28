use chain::BlockChain;
use primitives::signature::PublicKey;
use primitives::types::AccountId;
use std::collections::HashMap;
use types::BeaconBlock;

/// Configure the authority rotation.
pub struct AuthorityConfig {
    /// List of initial authorities at genesis block.
    pub initial_authorities: Vec<PublicKey>,
    /// Authority epoch length.
    pub epoch_length: u64,
}

#[derive(Default)]
pub struct AuthorityChangeSet {
    pub proposed: HashMap<AccountId, (PublicKey, u64)>,
}

pub struct Authority {
    /// Authority configuation.
    authority_config: AuthorityConfig,
    /// Cache of current authorities for given index.
    _current: HashMap<u64, Vec<PublicKey>>,
}

impl Authority {
    /// Builds authority for given valid blockchain.
    /// Starting from best block, figure out current authorities.
    pub fn new(authority_config: AuthorityConfig, blockchain: &BlockChain<BeaconBlock>) -> Self {
        let authority = Authority { authority_config, _current: HashMap::default() };
        let last_index = blockchain.best_block().header.index;
        let _current_epoch = last_index / authority.authority_config.epoch_length;

        authority
    }

    /// Returns authorities for given block number.
    pub fn get_authorities(&self, _index: u64) -> Vec<PublicKey> {
        // TODO: use cache to retrieve current authorities or compute future ones.
        self.authority_config.initial_authorities.clone()
    }

    /// Updates authority for given block.
    pub fn update_authority(&mut self, _change_set: &AuthorityChangeSet) {
        // TODO: update cache of current authorities.
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use chain::ChainConfig;
    use primitives::hash::CryptoHash;
    use primitives::traits::Block;
    use primitives::types::MerkleHash;
    use std::sync::Arc;
    use storage::test_utils::MemoryStorage;

    fn test_blockchain(num_blocks: u64) -> BlockChain<BeaconBlock> {
        let storage = Arc::new(MemoryStorage::default());
        let chain_config = ChainConfig {
            extra_col: storage::COL_BEACON_EXTRA,
            header_col: storage::COL_BEACON_HEADERS,
            block_col: storage::COL_BEACON_BLOCKS,
            index_col: storage::COL_BEACON_INDEX,
        };
        let mut last_block =
            BeaconBlock::new(0, CryptoHash::default(), MerkleHash::default(), vec![]);
        let bc = BlockChain::new(chain_config, last_block.clone(), storage);
        for i in 1..num_blocks {
            let block = BeaconBlock::new(i, last_block.hash(), MerkleHash::default(), vec![]);
            bc.insert_block(block.clone());
            last_block = block;
        }
        bc
    }

    #[test]
    fn test_authority_genesis() {
        let authority_config = AuthorityConfig { initial_authorities: vec![], epoch_length: 2 };
        let bc = test_blockchain(0);
        let authority = Authority::new(authority_config, &bc);
        assert_eq!(authority.get_authorities(0), vec![]);
    }
}

use chain::{Block, Header};
use primitives::hash::{CryptoHash, hash_struct};
use primitives::signature::PublicKey;
use primitives::types::{AuthorityMask, MultiSignature, PartialSignature};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct AuthorityProposal {
    /// Public key of the proposed authority.
    pub public_key: PublicKey,
    /// Stake / weight of the authority.
    pub amount: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BeaconBlockHeaderBody {
    /// Parent hash.
    pub parent_hash: CryptoHash,
    /// Block index.
    pub index: u64,
    /// Authority proposals.
    pub authority_proposal: Vec<AuthorityProposal>,
    /// Hash of the shard block.
    pub shard_block_hash: CryptoHash,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BeaconBlockHeader {
    pub body: BeaconBlockHeaderBody,
    pub block_hash: CryptoHash,
    pub signature: MultiSignature,
    pub authority_mask: AuthorityMask,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BeaconBlockBody {
    pub header: BeaconBlockHeaderBody,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BeaconBlock {
    pub body: BeaconBlockBody,
    pub signature: MultiSignature,
    pub authority_mask: AuthorityMask,
}

impl Header for BeaconBlockHeader {
    fn hash(&self) -> CryptoHash {
        self.block_hash
    }
    fn index(&self) -> u64 {
        self.body.index
    }
    fn parent_hash(&self) -> CryptoHash {
        self.body.parent_hash
    }
}

impl BeaconBlock {
    pub fn new(index: u64, parent_hash: CryptoHash, authority_proposal: Vec<AuthorityProposal>, shard_block_hash: CryptoHash) -> BeaconBlock {
        BeaconBlock {
            body: BeaconBlockBody {
                header: BeaconBlockHeaderBody {
                    index,
                    parent_hash,
                    authority_proposal,
                    shard_block_hash
                }
            },
            signature: vec![],
            authority_mask: vec![],
        }
    }

    pub fn genesis(shard_block_hash: CryptoHash) -> BeaconBlock {
        BeaconBlock::new(0, CryptoHash::default(), vec![], shard_block_hash)
    }
}

impl Block for BeaconBlock {
    type Header = BeaconBlockHeader;

    fn header(&self) -> Self::Header {
        BeaconBlockHeader {
            body: self.body.header.clone(),
            block_hash: self.hash(),
            signature: self.signature.clone(),
            authority_mask: self.authority_mask.clone(),
        }
    }

    fn hash(&self) -> CryptoHash {
        hash_struct(&self.body)
    }

    fn add_signature(&mut self, signature: PartialSignature) {
        self.signature.push(signature);
    }
}

pub type BeaconBlockChain = chain::BlockChain<BeaconBlock>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chain::BlockChain;
    use primitives::types::BlockId;
    use storage::test_utils::create_memory_db;

    use super::*;

    #[test]
    fn test_genesis() {
        let storage = Arc::new(create_memory_db());
        let genesis = BeaconBlock::new(
            0, CryptoHash::default(), vec![], CryptoHash::default()
        );
        let bc = BlockChain::new(genesis.clone(), storage);
        assert_eq!(bc.get_block(&BlockId::Hash(genesis.hash())).unwrap(), genesis);
        assert_eq!(bc.get_block(&BlockId::Number(0)).unwrap(), genesis);
    }

    #[test]
    fn test_restart_chain() {
        let storage = Arc::new(create_memory_db());
        let genesis = BeaconBlock::new(
            0, CryptoHash::default(), vec![], CryptoHash::default()
        );
        let bc = BlockChain::new(genesis.clone(), storage.clone());
        let block1 = BeaconBlock::new(
            1, genesis.hash(), vec![], CryptoHash::default()
        );
        assert_eq!(bc.insert_block(block1.clone()), false);
        let best_block = bc.best_block();
        let best_block_header = best_block.header();
        assert_eq!(best_block.hash(), block1.hash());
        assert_eq!(best_block_header.hash(), block1.hash());
        assert_eq!(best_block_header.index(), 1);
        // Create new BlockChain that reads from the same storage.
        let other_bc = BlockChain::new(genesis.clone(), storage.clone());
        assert_eq!(other_bc.best_block().hash(), block1.hash());
        assert_eq!(other_bc.best_block().header().index(), 1);
        assert_eq!(other_bc.get_block(&BlockId::Hash(block1.hash())).unwrap(), block1);
    }

    #[test]
    fn test_two_chains() {
        let storage = Arc::new(create_memory_db());
        let genesis1 = BeaconBlock::new(
            0, CryptoHash::default(), vec![], CryptoHash::default()
        );
        let genesis2 = BeaconBlock::new(
            0, CryptoHash::default(), vec![], genesis1.hash()
        );
        let bc1 = BlockChain::new(genesis1.clone(), storage.clone());
        let bc2 = BlockChain::new(genesis2.clone(), storage.clone());
        assert_eq!(bc1.best_block().hash(), genesis1.hash());
        assert_eq!(bc2.best_block().hash(), genesis2.hash());
    }
}

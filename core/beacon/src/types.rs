use chain::{SignedBlock, SignedHeader};
use primitives::hash::{hash_struct, CryptoHash};
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
pub struct BeaconBlockHeader {
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
pub struct SignedBeaconBlockHeader {
    pub body: BeaconBlockHeader,
    pub hash: CryptoHash,
    pub signature: MultiSignature,
    pub authority_mask: AuthorityMask,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct BeaconBlock {
    pub header: BeaconBlockHeader,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct SignedBeaconBlock {
    pub body: BeaconBlock,
    pub hash: CryptoHash,
    pub signature: MultiSignature,
    pub authority_mask: AuthorityMask,
}

impl SignedHeader for SignedBeaconBlockHeader {
    #[inline]
    fn block_hash(&self) -> CryptoHash {
        self.hash
    }
    #[inline]
    fn index(&self) -> u64 {
        self.body.index
    }
    #[inline]
    fn parent_hash(&self) -> CryptoHash {
        self.body.parent_hash
    }
}

impl SignedBeaconBlock {
    pub fn new(
        index: u64,
        parent_hash: CryptoHash,
        authority_proposal: Vec<AuthorityProposal>,
        shard_block_hash: CryptoHash,
    ) -> SignedBeaconBlock {
        let header = BeaconBlockHeader {
            index,
            parent_hash,
            authority_proposal,
            shard_block_hash,
        };
        let hash = hash_struct(&header);
        SignedBeaconBlock {
            body: BeaconBlock {
                header
            },
            hash,
            signature: vec![],
            authority_mask: vec![],
        }
    }

    pub fn genesis(shard_block_hash: CryptoHash) -> SignedBeaconBlock {
        SignedBeaconBlock::new(0, CryptoHash::default(), vec![], shard_block_hash)
    }
}

impl SignedBlock for SignedBeaconBlock {
    type SignedHeader = SignedBeaconBlockHeader;

    fn header(&self) -> Self::SignedHeader {
        SignedBeaconBlockHeader {
            body: self.body.header.clone(),
            hash: self.hash,
            signature: self.signature.clone(),
            authority_mask: self.authority_mask.clone(),
        }
    }

    #[inline]
    fn block_hash(&self) -> CryptoHash {
        self.hash
    }

    fn add_signature(&mut self, signature: PartialSignature) {
        self.signature.push(signature);
    }
}

pub type BeaconBlockChain = chain::BlockChain<SignedBeaconBlock>;

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
        let genesis = SignedBeaconBlock::new(0, CryptoHash::default(), vec![], CryptoHash::default());
        let bc = BlockChain::new(genesis.clone(), storage);
        assert_eq!(bc.get_block(&BlockId::Hash(genesis.block_hash())).unwrap(), genesis);
        assert_eq!(bc.get_block(&BlockId::Number(0)).unwrap(), genesis);
    }

    #[test]
    fn test_restart_chain() {
        let storage = Arc::new(create_memory_db());
        let genesis = SignedBeaconBlock::new(0, CryptoHash::default(), vec![], CryptoHash::default());
        let bc = BlockChain::new(genesis.clone(), storage.clone());
        let block1 = SignedBeaconBlock::new(1, genesis.block_hash(), vec![], CryptoHash::default());
        assert_eq!(bc.insert_block(block1.clone()), false);
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

    #[test]
    fn test_two_chains() {
        let storage = Arc::new(create_memory_db());
        let genesis1 = SignedBeaconBlock::new(0, CryptoHash::default(), vec![], CryptoHash::default());
        let genesis2 = SignedBeaconBlock::new(0, CryptoHash::default(), vec![], genesis1.block_hash());
        let bc1 = BlockChain::new(genesis1.clone(), storage.clone());
        let bc2 = BlockChain::new(genesis2.clone(), storage.clone());
        assert_eq!(bc1.best_block().block_hash(), genesis1.block_hash());
        assert_eq!(bc2.best_block().block_hash(), genesis2.block_hash());
    }
}

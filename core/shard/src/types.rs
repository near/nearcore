use chain::SignedBlock;
use chain::SignedHeader;
use near_protos::block as block_proto;
use primitives::hash::{CryptoHash, hash_struct};
use primitives::types::{AuthorityMask, MerkleHash, MultiSignature, PartialSignature, ShardId};
use primitives::serialize::{Encode, Decode, EncodeResult, DecodeResult};
use transaction::Transaction;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardBlockHeader {
    pub parent_hash: CryptoHash,
    pub shard_id: ShardId,
    pub index: u64,
    pub merkle_root_state: MerkleHash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedShardBlockHeader {
    pub body: ShardBlockHeader,
    pub hash: CryptoHash,
    pub authority_mask: AuthorityMask,
    pub signature: MultiSignature,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardBlock {
    pub header: ShardBlockHeader,
    pub transactions: Vec<Transaction>,
    pub new_receipts: Vec<Transaction>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedShardBlock {
    pub body: ShardBlock,
    pub hash: CryptoHash,
    pub authority_mask: AuthorityMask,
    pub signature: MultiSignature,
}

impl SignedHeader for SignedShardBlockHeader {
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

impl Encode for ShardBlockHeader {
    fn encode(&self) -> EncodeResult {
        let mut m = block_proto::ShardBlockHeader::new();
        m.set_parent_hash(self.parent_hash.as_ref().to_vec());
        m.set_shard_id(self.shard_id);
        m.set_index(self.index);
        m.set_merkle_root_state(self.merkle_root_state.as_ref().to_vec());
        near_protos::encode(&m)
    }
}

impl Encode for SignedShardBlockHeader {
    fn encode(&self) -> EncodeResult {
        let mut m = block_proto::SignedShardBlockHeader::new();
        m.set_body(self.body.encode()?);
        for x in self.authority_mask.iter() {
            m.mut_authority_mask().push(*x);
        }
        m.set_signature(self.signature[0].as_ref().to_vec());
        near_protos::encode(&m)
    }
}

impl Decode for SignedShardBlockHeader {
    fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        Err("WTF".to_string())
    }
}

impl SignedShardBlock {
    pub fn new(
        shard_id: ShardId,
        index: u64,
        parent_hash: CryptoHash,
        merkle_root_state: MerkleHash,
        transactions: Vec<Transaction>,
        new_receipts: Vec<Transaction>,
    ) -> Self {
        let header = ShardBlockHeader {
            shard_id,
            index,
            parent_hash,
            merkle_root_state,
        };
        let hash = hash_struct(&header);
        SignedShardBlock {
            body: ShardBlock {
                header,
                transactions,
                new_receipts,
            },
            hash,
            signature: vec![],
            authority_mask: vec![],
        }
    }

    pub fn genesis(merkle_root_state: MerkleHash) -> SignedShardBlock {
        SignedShardBlock::new(
            0, 0, CryptoHash::default(), merkle_root_state, vec![], vec![]
        )
    }
}

impl SignedBlock for SignedShardBlock {
    type SignedHeader = SignedShardBlockHeader;

    fn header(&self) -> Self::SignedHeader {
        SignedShardBlockHeader {
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

    fn weight(&self) -> u128 {
        1
    }
}

impl Encode for SignedShardBlock {
    fn encode(&self) -> EncodeResult {
        Ok(vec![])
    }
}

impl Decode for SignedShardBlock {
    fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        Err("WTF".to_string())
    }
}
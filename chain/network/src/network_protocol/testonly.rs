use super::*;

use crate::config;
use crate::types::{AccountKeys, ChainInfo, Handshake, RoutingTableUpdate};
use near_crypto::{InMemorySigner, KeyType, SecretKey};
use near_network_primitives::time;
use near_network_primitives::types::{
    AccountOrPeerIdOrHash, Edge, PartialEdgeInfo, PeerInfo, RawRoutedMessage, RoutedMessageBody,
};
use near_primitives::block::{genesis_chunks, Block, BlockHeader, GenesisId};
use near_primitives::challenge::{BlockDoubleSign, Challenge, ChallengeBody};
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::num_rational::Ratio;
use near_primitives::sharding::{
    ChunkHash, EncodedShardChunk, EncodedShardChunkBody, PartialEncodedChunkPart,
    ReedSolomonWrapper, ShardChunk,
};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockHeight, EpochId, StateRoot};
use near_primitives::validator_signer::{InMemoryValidatorSigner, ValidatorSigner};
use near_primitives::version::PROTOCOL_VERSION;
use rand::distributions::Standard;
use rand::Rng;
use std::collections::HashMap;
use std::net;
use std::sync::Arc;

pub fn make_genesis_block(_clock: &time::Clock, chunks: Vec<ShardChunk>) -> Block {
    Block::genesis(
        PROTOCOL_VERSION,
        chunks.into_iter().map(|c| c.take_header()).collect(),
        // TODO: this should be clock.now(), but Block::genesis has to be migrated
        // from chrono to time first.
        chrono::Utc::now(),
        0,                     // height
        1000,                  // initial_gas_price
        1000,                  // initial_total_supply
        CryptoHash::default(), // next_bp_hash (next block producers' hash)
    )
}

pub fn make_block(
    _clock: &time::Clock,
    signer: &dyn ValidatorSigner,
    prev: &Block,
    chunks: Vec<ShardChunk>,
) -> Block {
    Block::produce(
        PROTOCOL_VERSION,                                      // this_epoch_protocol_version
        PROTOCOL_VERSION,                                      // next_epoch_protocol_version
        prev.header(),                                         // prev
        prev.header().height() + 5,                            // height
        prev.header().block_ordinal() + 1,                     // block_ordinal
        chunks.into_iter().map(|c| c.take_header()).collect(), // chunks
        EpochId::default(),                                    // epoch_id
        EpochId::default(),                                    // next_epoch_id
        None,                                                  // epoch_sync_data_hash
        vec![],                                                // approvals
        Ratio::from_integer(0),                                // gas_price_adjustment_rate
        0,                                                     // min_gas_price
        0,                                                     // max_gas_price
        Some(0),                                               // minted_amount
        vec![],                                                // challenges_result
        vec![],                                                // challenges
        signer,
        CryptoHash::default(), // next_bp_hash
        CryptoHash::default(), // block_merkle_root
        // TODO: migrate to clock.now()
        Some(chrono::Utc::now()), // timestamp_override
    )
}

pub fn make_account_id<R: Rng>(rng: &mut R) -> AccountId {
    format!("account{}", rng.gen::<u32>()).parse().unwrap()
}

pub fn make_secret_key<R: Rng>(rng: &mut R) -> SecretKey {
    SecretKey::from_seed(KeyType::ED25519, &rng.gen::<u64>().to_string())
}

pub fn make_peer_id<R: Rng>(rng: &mut R) -> PeerId {
    PeerId::new(make_secret_key(rng).public_key())
}

pub fn make_signer<R: Rng>(rng: &mut R) -> InMemorySigner {
    InMemorySigner::from_secret_key(make_account_id(rng), make_secret_key(rng))
}

pub fn make_validator_signer<R: Rng>(rng: &mut R) -> InMemoryValidatorSigner {
    let account_id = make_account_id(rng);
    let seed = rng.gen::<u64>().to_string();
    InMemoryValidatorSigner::from_seed(account_id, KeyType::ED25519, &seed)
}

pub fn make_peer_info<R: Rng>(rng: &mut R) -> PeerInfo {
    let signer = make_signer(rng);
    PeerInfo {
        id: PeerId::new(signer.public_key),
        addr: Some(make_addr(rng)),
        account_id: Some(signer.account_id),
    }
}

pub fn make_announce_account<R: Rng>(rng: &mut R) -> AnnounceAccount {
    let peer_id = make_peer_id(rng);
    let validator_signer = make_validator_signer(rng);
    let signature = validator_signer.sign_account_announce(
        validator_signer.validator_id(),
        &peer_id,
        &EpochId::default(),
    );
    AnnounceAccount {
        account_id: validator_signer.validator_id().clone(),
        peer_id: peer_id,
        epoch_id: EpochId::default(),
        signature,
    }
}

pub fn make_partial_edge<R: Rng>(rng: &mut R) -> PartialEdgeInfo {
    let a = make_signer(rng);
    let b = make_signer(rng);
    PartialEdgeInfo::new(
        &PeerId::new(a.public_key),
        &PeerId::new(b.public_key),
        rng.gen(),
        &a.secret_key,
    )
}

pub fn make_edge(a: &InMemorySigner, b: &InMemorySigner) -> Edge {
    let (a, b) = if a.public_key < b.public_key { (a, b) } else { (b, a) };
    let ap = PeerId::new(a.public_key.clone());
    let bp = PeerId::new(b.public_key.clone());
    let nonce = 1; // Make it an active edge.
    let hash = Edge::build_hash(&ap, &bp, nonce);
    Edge::new(ap, bp, nonce, a.secret_key.sign(hash.as_ref()), b.secret_key.sign(hash.as_ref()))
}

pub fn make_edge_tombstone(a: &InMemorySigner, b: &InMemorySigner) -> Edge {
    make_edge(a, b).remove_edge(PeerId::new(a.public_key.clone()), &a.secret_key)
}

pub fn make_routing_table<R: Rng>(rng: &mut R) -> RoutingTableUpdate {
    let signers: Vec<_> = (0..7).map(|_| make_signer(rng)).collect();
    RoutingTableUpdate {
        accounts: (0..10).map(|_| make_announce_account(rng)).collect(),
        edges: {
            let mut e = vec![];
            for i in 0..signers.len() {
                for j in 0..i {
                    e.push(make_edge(&signers[i], &signers[j]));
                }
            }
            e
        },
    }
}

pub fn make_signed_transaction<R: Rng>(rng: &mut R) -> SignedTransaction {
    let sender = make_signer(rng);
    let receiver = make_account_id(rng);
    SignedTransaction::send_money(
        rng.gen(),
        sender.account_id.clone(),
        receiver,
        &sender,
        15,
        CryptoHash::default(),
    )
}

pub fn make_challenge<R: Rng>(rng: &mut R) -> Challenge {
    Challenge::produce(
        ChallengeBody::BlockDoubleSign(BlockDoubleSign {
            left_block_header: rng.sample_iter(&Standard).take(65).collect(),
            right_block_header: rng.sample_iter(&Standard).take(34).collect(),
        }),
        &make_validator_signer(rng),
    )
}

// Based on ShardsManager::prepare_partial_encoded_chunk_response_from_chunk.
// I give no guarantee that it will produce correct data, I'm just approximating
// the real thing, since this functionality is not encapsulated in
// the production code well enough to reuse it in tests.
pub fn make_chunk_parts(chunk: ShardChunk) -> Vec<PartialEncodedChunkPart> {
    let mut rs = ReedSolomonWrapper::new(10, 5);
    let (parts, _) = EncodedShardChunk::encode_transaction_receipts(
        &mut rs,
        chunk.transactions().to_vec(),
        &chunk.receipts(),
    )
    .unwrap();
    let mut content = EncodedShardChunkBody { parts };
    content.reconstruct(&mut rs).unwrap();
    let (_, merkle_paths) = content.get_merkle_hash_and_paths();
    let mut parts = vec![];
    for ord in 0..rs.total_shard_count() {
        parts.push(PartialEncodedChunkPart {
            part_ord: ord as u64,
            part: content.parts[ord].take().unwrap(),
            merkle_proof: merkle_paths[ord].clone(),
        });
    }
    parts
}

struct ChunkSet {
    chunks: HashMap<ChunkHash, ShardChunk>,
}

impl ChunkSet {
    pub fn new() -> Self {
        Self { chunks: HashMap::default() }
    }
    pub fn make(&mut self) -> Vec<ShardChunk> {
        // TODO: these are always genesis chunks.
        // Consider making this more realistic.
        let chunks = genesis_chunks(
            vec![StateRoot::new()], // state_roots
            4,                      // num_shards
            1000,                   // initial_gas_limit
            0,                      // genesis_height
            PROTOCOL_VERSION,
        );
        self.chunks.extend(chunks.iter().map(|c| (c.chunk_hash(), c.clone())));
        chunks
    }
}

pub fn make_epoch_id<R: Rng>(rng: &mut R) -> EpochId {
    EpochId(CryptoHash::hash_bytes(&rng.gen::<[u8; 19]>()))
}

pub struct Chain {
    pub genesis_id: GenesisId,
    pub blocks: Vec<Block>,
    pub chunks: HashMap<ChunkHash, ShardChunk>,
    pub tier1_accounts: Vec<(EpochId, InMemoryValidatorSigner)>,
}

impl Chain {
    pub fn make<R: Rng>(clock: &mut time::FakeClock, rng: &mut R, block_count: usize) -> Chain {
        let mut chunks = ChunkSet::new();
        let mut blocks = vec![];
        blocks.push(make_genesis_block(&clock.clock(), chunks.make()));
        let signer = make_validator_signer(rng);
        for _ in 1..block_count {
            clock.advance(time::Duration::seconds(15));
            blocks.push(make_block(&clock.clock(), &signer, blocks.last().unwrap(), chunks.make()));
        }
        Chain {
            genesis_id: GenesisId {
                chain_id: format!("testchain{}", rng.gen::<u32>()),
                hash: Default::default(),
            },
            blocks,
            tier1_accounts: (0..10)
                .map(|_| (make_epoch_id(rng), make_validator_signer(rng)))
                .collect(),
            chunks: chunks.chunks,
        }
    }

    pub fn height(&self) -> BlockHeight {
        self.tip().height()
    }

    pub fn tip(&self) -> &BlockHeader {
        self.blocks.last().unwrap().header()
    }

    pub fn get_tier1_accounts(&self) -> AccountKeys {
        self.tier1_accounts
            .iter()
            .map(|(epoch_id, v)| {
                ((epoch_id.clone(), v.validator_id().clone()), v.public_key().clone())
            })
            .collect()
    }

    pub fn get_chain_info(&self) -> ChainInfo {
        ChainInfo {
            tracked_shards: Default::default(),
            height: self.height(),
            tier1_accounts: Arc::new(self.get_tier1_accounts()),
        }
    }

    pub fn get_peer_chain_info(&self) -> PeerChainInfoV2 {
        PeerChainInfoV2 {
            genesis_id: self.genesis_id.clone(),
            tracked_shards: Default::default(),
            archival: false,
            height: self.height(),
        }
    }

    pub fn get_block_headers(&self) -> Vec<BlockHeader> {
        self.blocks.iter().map(|b| b.header().clone()).collect()
    }

    pub fn make_config<R: Rng>(&self, rng: &mut R) -> config::NetworkConfig {
        let port = crate::test_utils::open_port();
        let seed = &rng.gen::<u64>().to_string();
        let mut cfg = config::NetworkConfig::from_seed(&seed, port);
        // Currently, in unit tests PeerManagerActor is not allowed to try to establish
        // connections on its own.
        cfg.outbound_disabled = true;
        cfg
    }

    pub fn make_tier1_data<R: Rng>(
        &self,
        rng: &mut R,
        clock: &time::Clock,
    ) -> Vec<Arc<SignedAccountData>> {
        self.tier1_accounts
            .iter()
            .map(|(epoch_id, v)| {
                Arc::new(
                    make_account_data(
                        rng,
                        clock.now_utc(),
                        epoch_id.clone(),
                        v.validator_id().clone(),
                    )
                    .sign(v)
                    .unwrap(),
                )
            })
            .collect()
    }
}

pub fn make_handshake<R: Rng>(rng: &mut R, chain: &Chain) -> Handshake {
    let a = make_signer(rng);
    let b = make_signer(rng);
    let a_id = PeerId::new(a.public_key);
    let b_id = PeerId::new(b.public_key);
    Handshake::new(
        PROTOCOL_VERSION,
        a_id,
        b_id,
        Some(rng.gen()),
        chain.get_peer_chain_info(),
        make_partial_edge(rng),
    )
}

pub fn make_routed_message<R: Rng>(rng: &mut R, body: RoutedMessageBody) -> Box<RoutedMessageV2> {
    let signer = make_signer(rng);
    let peer_id = PeerId::new(signer.public_key);
    RawRoutedMessage { target: AccountOrPeerIdOrHash::PeerId(peer_id.clone()), body }.sign(
        peer_id,
        &signer.secret_key,
        /*ttl=*/ 1,
        None,
    )
}
pub fn make_ipv4(rng: &mut impl Rng) -> net::IpAddr {
    net::IpAddr::V4(net::Ipv4Addr::from(rng.gen::<[u8; 4]>()))
}

pub fn make_ipv6(rng: &mut impl Rng) -> net::IpAddr {
    net::IpAddr::V6(net::Ipv6Addr::from(rng.gen::<[u8; 16]>()))
}

pub fn make_addr<R: Rng>(rng: &mut R) -> net::SocketAddr {
    net::SocketAddr::new(make_ipv4(rng), rng.gen())
}

pub fn make_peer_addr(rng: &mut impl Rng, ip: net::IpAddr) -> PeerAddr {
    PeerAddr { addr: net::SocketAddr::new(ip, rng.gen()), peer_id: make_peer_id(rng) }
}

pub fn make_account_data(
    rng: &mut impl Rng,
    timestamp: time::Utc,
    epoch_id: EpochId,
    account_id: AccountId,
) -> AccountData {
    AccountData {
        peers: vec![
            // Can't inline make_ipv4/ipv6 calls, because 2-phase borrow
            // doesn't work.
            {
                let ip = make_ipv4(rng);
                make_peer_addr(rng, ip)
            },
            {
                let ip = make_ipv4(rng);
                make_peer_addr(rng, ip)
            },
            {
                let ip = make_ipv6(rng);
                make_peer_addr(rng, ip)
            },
        ],
        account_id,
        epoch_id,
        timestamp,
    }
}

pub fn make_signed_account_data(rng: &mut impl Rng, clock: &time::Clock) -> SignedAccountData {
    let signer = make_validator_signer(rng);
    let epoch_id = make_epoch_id(rng);
    make_account_data(rng, clock.now_utc(), epoch_id, signer.validator_id().clone())
        .sign(&signer)
        .unwrap()
}

// Accessors for creating malformed SignedAccountData
impl SignedAccountData {
    pub(crate) fn payload_mut(&mut self) -> &mut Vec<u8> {
        &mut self.payload.payload
    }
    pub(crate) fn signature_mut(&mut self) -> &mut near_crypto::Signature {
        &mut self.payload.signature
    }
}

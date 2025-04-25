use super::*;
use crate::config;
use crate::network_protocol::{
    Edge, PartialEdgeInfo, PeerInfo, RawRoutedMessage, RoutedMessageBody,
};
use crate::tcp;
use crate::types::{AccountKeys, ChainInfo, Handshake, RoutingTableUpdate};
use near_async::time;
use near_crypto::{InMemorySigner, KeyType, SecretKey, Signer};
use near_primitives::block::{Block, BlockHeader};
use near_primitives::genesis::{genesis_block, genesis_chunks};
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::num_rational::Ratio;
use near_primitives::reed_solomon::reed_solomon_encode;
use near_primitives::sharding::{
    ChunkHash, EncodedShardChunkBody, PartialEncodedChunkPart, ShardChunk,
};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockHeight, EpochId, StateRoot};
use near_primitives::validator_signer::{InMemoryValidatorSigner, ValidatorSigner};
use near_primitives::version;
use rand::Rng;
use reed_solomon_erasure::galois_8::ReedSolomon;
use std::collections::HashMap;
use std::net;
use std::sync::Arc;

pub fn make_genesis_block(clock: &time::Clock, chunks: Vec<ShardChunk>) -> Block {
    genesis_block(
        version::PROTOCOL_VERSION,
        chunks.into_iter().map(|c| c.take_header()).collect(),
        clock.now_utc(),
        0,
        1000,
        1000,
        &vec![],
    )
}

pub fn make_block(
    clock: time::Clock,
    signer: &ValidatorSigner,
    prev: &Block,
    chunks: Vec<ShardChunk>,
) -> Block {
    Block::produce(
        version::PROTOCOL_VERSION,
        prev.header(),
        prev.header().height() + 5,
        prev.header().block_ordinal() + 1,
        chunks.iter().cloned().map(|c| c.take_header()).collect(),
        vec![vec![]; chunks.len()],
        EpochId::default(),
        EpochId::default(),
        None,
        vec![],
        Ratio::from_integer(0),
        0,
        0,
        Some(0),
        signer,
        CryptoHash::default(),
        CryptoHash::default(),
        clock,
        None,
        None,
    )
}

pub fn make_account_id<R: Rng>(rng: &mut R) -> AccountId {
    format!("account{}", rng.r#gen::<u32>()).parse().unwrap()
}

pub fn make_secret_key<R: Rng>(rng: &mut R) -> SecretKey {
    SecretKey::from_seed(KeyType::ED25519, &rng.r#gen::<u64>().to_string())
}

pub fn make_peer_id<R: Rng>(rng: &mut R) -> PeerId {
    PeerId::new(make_secret_key(rng).public_key())
}

pub fn make_signer<R: Rng>(rng: &mut R) -> Signer {
    InMemorySigner::from_secret_key(make_account_id(rng), make_secret_key(rng))
}

pub fn make_validator_signer<R: Rng>(rng: &mut R) -> ValidatorSigner {
    let account_id = make_account_id(rng);
    let seed = rng.r#gen::<u64>().to_string();
    InMemoryValidatorSigner::from_seed(account_id, KeyType::ED25519, &seed)
}

pub fn make_peer_info<R: Rng>(rng: &mut R) -> PeerInfo {
    let signer = make_signer(rng);
    PeerInfo {
        id: PeerId::new(signer.public_key()),
        addr: Some(make_addr(rng)),
        account_id: Some(signer.get_account_id()),
    }
}

pub fn make_announce_account<R: Rng>(rng: &mut R) -> AnnounceAccount {
    let peer_id = make_peer_id(rng);
    let validator_signer = make_validator_signer(rng);
    AnnounceAccount::new(&validator_signer, peer_id, EpochId::default())
}

pub fn make_partial_edge<R: Rng>(rng: &mut R) -> PartialEdgeInfo {
    let account_id = make_account_id(rng);
    let secret_key = make_secret_key(rng);
    let a = InMemorySigner::from_secret_key(account_id, secret_key.clone());
    let b = make_signer(rng);

    PartialEdgeInfo::new(
        &PeerId::new(a.public_key()),
        &PeerId::new(b.public_key()),
        rng.r#gen(),
        &secret_key,
    )
}

pub fn make_edge(a: &SecretKey, b: &SecretKey, nonce: u64) -> Edge {
    let (a, b) = if a.public_key() < b.public_key() { (a, b) } else { (b, a) };
    let ap = PeerId::new(a.public_key());
    let bp = PeerId::new(b.public_key());
    let hash = Edge::build_hash(&ap, &bp, nonce);
    Edge::new(ap, bp, nonce, a.sign(hash.as_ref()), b.sign(hash.as_ref()))
}

pub fn make_routing_table<R: Rng>(rng: &mut R) -> RoutingTableUpdate {
    let signers: Vec<_> = (0..7).map(|_| make_secret_key(rng)).collect();
    RoutingTableUpdate {
        accounts: (0..10).map(|_| make_announce_account(rng)).collect(),
        edges: {
            let mut e = vec![];
            for i in 0..signers.len() {
                for j in 0..i {
                    e.push(make_edge(&signers[i], &signers[j], 1));
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
        rng.r#gen(),
        sender.get_account_id(),
        receiver,
        &sender,
        15,
        CryptoHash::default(),
    )
}

// Based on ShardsManager::prepare_partial_encoded_chunk_response_from_chunk.
// I give no guarantee that it will produce correct data, I'm just approximating
// the real thing, since this functionality is not encapsulated in
// the production code well enough to reuse it in tests.
pub fn make_chunk_parts(chunk: ShardChunk) -> Vec<PartialEncodedChunkPart> {
    let total_shard_count = 10;
    let parity_shard_count = 5;
    let rs = ReedSolomon::new(total_shard_count, parity_shard_count).unwrap();
    let transaction_receipts =
        (chunk.to_transactions().to_vec(), chunk.prev_outgoing_receipts().to_vec());
    let (parts, _) = reed_solomon_encode(&rs, &transaction_receipts);

    let mut content = EncodedShardChunkBody { parts };
    let (_, merkle_paths) = content.get_merkle_hash_and_paths();
    let mut parts = vec![];
    for ord in 0..total_shard_count {
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
        let shard_ids: Vec<_> = (0..4).into_iter().map(ShardId::new).collect();
        // TODO: these are always genesis chunks.
        // Consider making this more realistic.
        let chunks = genesis_chunks(
            vec![StateRoot::new()],
            vec![Default::default(); shard_ids.len()],
            &shard_ids,
            1000,
            0,
            version::PROTOCOL_VERSION,
        );
        self.chunks.extend(chunks.iter().map(|c| (c.chunk_hash(), c.clone())));
        chunks
    }
}

pub fn make_hash<R: Rng>(rng: &mut R) -> CryptoHash {
    CryptoHash::hash_bytes(&rng.r#gen::<[u8; 19]>())
}

pub fn make_account_keys(signers: &[ValidatorSigner]) -> AccountKeys {
    let mut account_keys = AccountKeys::new();
    for s in signers {
        account_keys.entry(s.validator_id().clone()).or_default().insert(s.public_key());
    }
    account_keys
}

pub struct Chain {
    pub genesis_id: GenesisId,
    pub blocks: Vec<Block>,
    pub chunks: HashMap<ChunkHash, ShardChunk>,
    pub tier1_accounts: Vec<ValidatorSigner>,
}

impl Chain {
    pub fn make<R: Rng>(clock: &time::FakeClock, rng: &mut R, block_count: usize) -> Chain {
        let mut chunks = ChunkSet::new();
        let mut blocks = vec![];
        blocks.push(make_genesis_block(&clock.clock(), chunks.make()));
        let signer = make_validator_signer(rng);
        for _ in 1..block_count {
            clock.advance(time::Duration::seconds(15));
            blocks.push(make_block(clock.clock(), &signer, blocks.last().unwrap(), chunks.make()));
        }
        Chain {
            genesis_id: GenesisId {
                chain_id: format!("testchain{}", rng.r#gen::<u32>()),
                hash: Default::default(),
            },
            blocks,
            tier1_accounts: (0..10).map(|_| make_validator_signer(rng)).collect(),
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
        make_account_keys(&self.tier1_accounts)
    }

    pub fn get_chain_info(&self) -> ChainInfo {
        ChainInfo {
            tracked_shards: Default::default(),
            block: self.blocks.last().unwrap().clone(),
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
        let seed = &rng.r#gen::<u64>().to_string();
        let mut cfg =
            config::NetworkConfig::from_seed(&seed, tcp::ListenerAddr::reserve_for_test());
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
            .map(|v| {
                let peer_id = make_peer_id(rng);
                Arc::new(
                    make_account_data(rng, 1, clock.now_utc(), v.public_key(), peer_id)
                        .sign(&v.clone().into())
                        .unwrap(),
                )
            })
            .collect()
    }
}

pub fn make_handshake<R: Rng>(rng: &mut R, chain: &Chain) -> Handshake {
    let a = make_signer(rng);
    let b = make_signer(rng);
    let a_id = PeerId::new(a.public_key());
    let b_id = PeerId::new(b.public_key());
    Handshake {
        protocol_version: version::PROTOCOL_VERSION,
        oldest_supported_version: version::PEER_MIN_ALLOWED_PROTOCOL_VERSION,
        sender_peer_id: a_id,
        target_peer_id: b_id,
        sender_listen_port: Some(rng.r#gen()),
        sender_chain_info: chain.get_peer_chain_info(),
        partial_edge_info: make_partial_edge(rng),
        owned_account: None,
    }
}

pub fn make_routed_message<R: Rng>(rng: &mut R, body: RoutedMessageBody) -> RoutedMessageV2 {
    let secret_key = make_secret_key(rng);
    let peer_id = PeerId::new(secret_key.public_key());
    RawRoutedMessage { target: PeerIdOrHash::PeerId(peer_id), body }.sign(
        &secret_key,
        /*ttl=*/ 1,
        None,
    )
}
pub fn make_ipv4(rng: &mut impl Rng) -> net::IpAddr {
    net::IpAddr::V4(net::Ipv4Addr::from(rng.r#gen::<[u8; 4]>()))
}

pub fn make_ipv6(rng: &mut impl Rng) -> net::IpAddr {
    net::IpAddr::V6(net::Ipv6Addr::from(rng.r#gen::<[u8; 16]>()))
}

pub fn make_addr<R: Rng>(rng: &mut R) -> net::SocketAddr {
    net::SocketAddr::new(make_ipv4(rng), rng.r#gen())
}

pub fn make_peer_addr(rng: &mut impl Rng, ip: net::IpAddr) -> PeerAddr {
    PeerAddr { addr: net::SocketAddr::new(ip, rng.r#gen()), peer_id: make_peer_id(rng) }
}

pub fn make_account_data(
    rng: &mut impl Rng,
    version: u64,
    timestamp: time::Utc,
    account_key: PublicKey,
    peer_id: PeerId,
) -> VersionedAccountData {
    VersionedAccountData {
        data: AccountData {
            proxies: vec![
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
            peer_id,
        },
        account_key,
        version,
        timestamp,
    }
}

pub fn make_signed_account_data(rng: &mut impl Rng, clock: &time::Clock) -> SignedAccountData {
    let signer = make_validator_signer(rng);
    let peer_id = make_peer_id(rng);
    make_account_data(rng, 1, clock.now_utc(), signer.public_key(), peer_id).sign(&signer).unwrap()
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

use crate::types::PeerMessage;
use actix::Message;
use near_primitives::{hash::CryptoHash, network::PeerId};
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
};
use tracing::info;

#[derive(Clone, Copy)]
pub enum Status {
    Sent,
    Received,
}
#[derive(Default, Debug)]
struct CountSize {
    count: usize,
    bytes: usize,
}

impl CountSize {
    fn update(&mut self, bytes: usize) {
        self.count += 1;
        self.bytes += bytes;
    }
}

#[derive(Default, Debug)]
struct SentReceived {
    sent: CountSize,
    received: CountSize,
}

impl SentReceived {
    fn get(&mut self, status: Status) -> &mut CountSize {
        match status {
            Status::Sent => &mut self.sent,
            Status::Received => &mut self.received,
        }
    }
}

#[derive(Default)]
struct HashAggregator {
    total: usize,
    all: HashSet<CryptoHash>,
}

impl HashAggregator {
    fn add(&mut self, hash: CryptoHash) {
        self.total += 1;
        self.all.insert(hash);
    }

    fn different(&self) -> usize {
        self.all.len()
    }
}

impl Debug for HashAggregator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HashAggregator {{ total: {}, different: {} }}", self.total, self.different())
    }
}

#[derive(Default, Debug)]
pub struct MetricRecorder {
    overall: SentReceived,
    per_type: HashMap<String, SentReceived>,
    per_peer: HashMap<Option<PeerId>, SentReceived>,
    graph: Vec<(PeerId, PeerId)>,
    challenge_hashes: HashAggregator,
    block_hashes: HashAggregator,
}

impl MetricRecorder {
    pub fn handle_peer_message(&mut self, peer_message_metadata: PeerMessageMetadata) {
        self.overall
            .get(peer_message_metadata.status.unwrap())
            .update(peer_message_metadata.size.unwrap());

        self.per_type
            .entry(peer_message_metadata.message_type.clone())
            .or_insert(SentReceived::default())
            .get(peer_message_metadata.status.unwrap())
            .update(peer_message_metadata.size.unwrap());

        let peer = peer_message_metadata.other_peer();

        self.per_peer
            .entry(peer)
            .or_insert(SentReceived::default())
            .get(peer_message_metadata.status.unwrap())
            .update(peer_message_metadata.size.unwrap());

        match peer_message_metadata.message_type.as_str() {
            "Challenge" => self.challenge_hashes.add(peer_message_metadata.hash.unwrap()),
            "Block" => self.block_hashes.add(peer_message_metadata.hash.unwrap()),
            _ => {}
        }
    }

    pub fn report(&self) {
        info!(target: "stats", "{:?}", self);
    }

    pub fn set_graph(&mut self, graph: &HashMap<PeerId, HashSet<PeerId>>) {
        self.graph.clear();
        for (u, u_adj) in graph.iter() {
            for v in u_adj {
                if u < v {
                    self.graph.push((u.clone(), v.clone()));
                }
            }
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct PeerMessageMetadata {
    source: Option<PeerId>,
    target: Option<PeerId>,
    status: Option<Status>,
    message_type: String,
    size: Option<usize>,
    hash: Option<CryptoHash>,
}

impl PeerMessageMetadata {
    pub fn into_metadata(msg: &PeerMessage) -> Self {
        let hash = match msg {
            PeerMessage::Challenge(challenge) => Some(challenge.hash),
            PeerMessage::Block(block) => Some(block.hash()),
            _ => None,
        };

        Self {
            source: None,
            target: None,
            status: None,
            message_type: msg.to_string(),
            size: None,
            hash,
        }
    }

    pub fn set_source(mut self, peer_id: PeerId) -> Self {
        self.source = Some(peer_id);
        self
    }

    pub fn set_target(mut self, peer_id: PeerId) -> Self {
        self.target = Some(peer_id);
        self
    }

    pub fn set_status(mut self, status: Status) -> Self {
        self.status = Some(status);
        self
    }

    pub fn set_size(mut self, size: usize) -> Self {
        self.size = Some(size);
        self
    }

    fn other_peer(&self) -> Option<PeerId> {
        match self.status {
            Some(Status::Received) => self.source.clone(),
            Some(Status::Sent) => self.target.clone(),
            _ => None,
        }
    }
}

use borsh::{BorshDeserialize, BorshSerialize};
use near_async::time;
use near_crypto::{KeyType, SecretKey, Signature};
use near_primitives::borsh::maybestd::sync::Arc;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use once_cell::sync::Lazy;

// We'd treat all nonces that are below this values as 'old style' (without any expiration time).
// And all nonces above this value as new style (that would expire after some time).
// This value is set to August 8, 2022.
// TODO: Remove this in Dec 2022 - once we finish migration to new nonces.
pub const EDGE_MIN_TIMESTAMP_NONCE: Lazy<time::Utc> =
    Lazy::new(|| time::Utc::from_unix_timestamp(1660000000).unwrap());

/// Information that will be ultimately used to create a new edge.
/// It contains nonce proposed for the edge with signature from peer.
#[derive(Clone, BorshSerialize, BorshDeserialize, PartialEq, Eq, Debug, Default)]
pub struct PartialEdgeInfo {
    pub nonce: u64,
    pub signature: Signature,
}

impl PartialEdgeInfo {
    pub fn new(peer0: &PeerId, peer1: &PeerId, nonce: u64, secret_key: &SecretKey) -> Self {
        let data = Edge::build_hash(peer0, peer1, nonce);
        let signature = secret_key.sign(data.as_ref());
        Self { nonce, signature }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum InvalidNonceError {
    #[error("nonce is overflowing i64: {nonce}")]
    NonceOutOfBoundsError { nonce: u64 },
}

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "test_features", derive(serde::Serialize, serde::Deserialize))]
pub struct Edge(pub Arc<EdgeInner>);

impl Edge {
    /// Create an addition edge.
    pub fn new(
        peer0: PeerId,
        peer1: PeerId,
        nonce: u64,
        signature0: Signature,
        signature1: Signature,
    ) -> Self {
        Edge(Arc::new(EdgeInner::new(peer0, peer1, nonce, signature0, signature1)))
    }

    pub fn with_removal_info(mut self, ri: Option<(bool, Signature)>) -> Edge {
        Arc::make_mut(&mut self.0).removal_info = ri;
        self
    }

    pub fn key(&self) -> &(PeerId, PeerId) {
        &self.0.key
    }

    pub fn nonce(&self) -> u64 {
        self.0.nonce
    }

    pub fn signature0(&self) -> &Signature {
        &self.0.signature0
    }

    pub fn signature1(&self) -> &Signature {
        &self.0.signature1
    }

    pub fn removal_info(&self) -> Option<&(bool, Signature)> {
        self.0.removal_info.as_ref()
    }

    pub fn make_fake_edge(peer0: PeerId, peer1: PeerId, nonce: u64) -> Self {
        Self(Arc::new(EdgeInner {
            key: if peer0 < peer1 { (peer0, peer1) } else { (peer1, peer0) },
            nonce,
            signature0: Signature::empty(KeyType::ED25519),
            signature1: Signature::empty(KeyType::ED25519),
            removal_info: None,
        }))
    }

    /// Build a new edge with given information from the other party.
    pub fn build_with_secret_key(
        peer0: PeerId,
        peer1: PeerId,
        nonce: u64,
        secret_key: &SecretKey,
        signature1: Signature,
    ) -> Self {
        let hash = if peer0 < peer1 {
            Self::build_hash(&peer0, &peer1, nonce)
        } else {
            Self::build_hash(&peer1, &peer0, nonce)
        };
        let signature0 = secret_key.sign(hash.as_ref());
        Self::new(peer0, peer1, nonce, signature0, signature1)
    }

    /// Build the hash of the edge given its content.
    /// It is important that peer0 < peer1 at this point.
    pub fn build_hash(peer0: &PeerId, peer1: &PeerId, nonce: u64) -> CryptoHash {
        let (peer0, peer1) = if peer0 < peer1 { (peer0, peer1) } else { (peer1, peer0) };
        CryptoHash::hash_borsh((peer0, peer1, nonce))
    }

    pub fn make_key(peer0: PeerId, peer1: PeerId) -> (PeerId, PeerId) {
        if peer0 < peer1 {
            (peer0, peer1)
        } else {
            (peer1, peer0)
        }
    }

    /// Helper function when adding a new edge and we receive information from new potential peer
    /// to verify the signature.
    pub fn partial_verify(peer0: &PeerId, peer1: &PeerId, edge_info: &PartialEdgeInfo) -> bool {
        let pk = peer1.public_key();
        let data = Edge::build_hash(peer0, peer1, edge_info.nonce);
        edge_info.signature.verify(data.as_ref(), pk)
    }

    /// Next nonce of valid addition edge.
    pub fn next_nonce(nonce: u64) -> u64 {
        if nonce % 2 == 1 {
            nonce + 2
        } else {
            nonce + 1
        }
    }

    /// Create a fresh nonce (based on the current time).
    pub fn create_fresh_nonce(clock: &time::Clock) -> u64 {
        let mut nonce = clock.now_utc().unix_timestamp() as u64;
        // Even nonce means that the edge should be removed, so if the timestamp is even, add one to get the odd value.
        if nonce % 2 == 0 {
            nonce += 1;
        }
        nonce
    }

    /// Create the remove edge change from an added edge change.
    pub fn remove_edge(&self, my_peer_id: PeerId, sk: &SecretKey) -> Edge {
        assert_eq!(self.edge_type(), EdgeState::Active);
        let mut edge = self.0.as_ref().clone();
        edge.nonce += 1;
        let me = edge.key.0 == my_peer_id;
        let hash = edge.hash();
        let signature = sk.sign(hash.as_ref());
        edge.removal_info = Some((me, signature));
        Edge(Arc::new(edge))
    }

    pub(crate) fn hash(&self) -> CryptoHash {
        Edge::build_hash(&self.key().0, &self.key().1, self.nonce())
    }

    fn prev_hash(&self) -> CryptoHash {
        Edge::build_hash(&self.key().0, &self.key().1, self.nonce() - 1)
    }

    pub fn verify(&self) -> bool {
        if self.key().0 > self.key().1 {
            return false;
        }

        match self.edge_type() {
            EdgeState::Active => {
                let data = self.hash();

                self.removal_info().is_none()
                    && self.signature0().verify(data.as_ref(), self.key().0.public_key())
                    && self.signature1().verify(data.as_ref(), self.key().1.public_key())
            }
            EdgeState::Removed => {
                // nonce should be an even positive number
                if self.nonce() == 0 {
                    return false;
                }

                // Check referring added edge is valid.
                let add_hash = self.prev_hash();
                if !self.signature0().verify(add_hash.as_ref(), self.key().0.public_key())
                    || !self.signature1().verify(add_hash.as_ref(), self.key().1.public_key())
                {
                    return false;
                }

                if let Some((party, signature)) = self.removal_info() {
                    let peer = if *party { &self.key().0 } else { &self.key().1 };
                    let del_hash = self.hash();
                    signature.verify(del_hash.as_ref(), peer.public_key())
                } else {
                    false
                }
            }
        }
    }

    /// It will be considered as a new edge if the nonce is odd, otherwise it is canceling the
    /// previous edge.
    pub fn edge_type(&self) -> EdgeState {
        if self.nonce() % 2 == 1 {
            EdgeState::Active
        } else {
            EdgeState::Removed
        }
    }
    /// Next nonce of valid addition edge.
    pub fn next(&self) -> u64 {
        Edge::next_nonce(self.nonce())
    }

    pub fn contains_peer(&self, peer_id: &PeerId) -> bool {
        self.key().0 == *peer_id || self.key().1 == *peer_id
    }

    /// Find a peer id in this edge different from `me`.
    pub fn other(&self, me: &PeerId) -> Option<&PeerId> {
        if self.key().0 == *me {
            Some(&self.key().1)
        } else if self.key().1 == *me {
            Some(&self.key().0)
        } else {
            None
        }
    }

    // Checks if edge was created before a given timestamp.
    pub fn is_edge_older_than(&self, utc_timestamp: time::Utc) -> bool {
        Edge::nonce_to_utc(self.nonce()).map_or(false, |maybe_timestamp| {
            // Old-style nonce - for now, assume that they are always fresh.
            maybe_timestamp.map_or(false, |nonce_timestamp| nonce_timestamp < utc_timestamp)
        })
    }

    pub fn nonce_to_utc(nonce: u64) -> Result<Option<time::Utc>, InvalidNonceError> {
        if let Ok(nonce_as_i64) = i64::try_from(nonce) {
            time::Utc::from_unix_timestamp(nonce_as_i64)
                .map(
                    |nonce_ts| {
                        if nonce_ts > *EDGE_MIN_TIMESTAMP_NONCE {
                            Some(nonce_ts)
                        } else {
                            None
                        }
                    },
                )
                .map_err(|_| InvalidNonceError::NonceOutOfBoundsError { nonce })
        } else {
            Err(InvalidNonceError::NonceOutOfBoundsError { nonce })
        }
    }

    // Returns a single edge with the highest nonce for each key of the input edges.
    pub fn deduplicate<'a>(mut edges: Vec<Edge>) -> Vec<Edge> {
        edges.sort_by(|a, b| (b.key(), b.nonce()).cmp(&(a.key(), a.nonce())));
        edges.dedup_by_key(|e| e.key().clone());
        edges
    }
}

/// An `Edge` represents a direct connection between two peers in Near Protocol P2P network.
///
/// Note that edge might either in `Active` or `Removed` state.
/// We need to keep explicitly `Removed` edges, in order to be able to proof, that given `Edge`
/// isn't `Active` anymore. In case, someone delivers a proof that the edge existed.
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "test_features", derive(serde::Serialize, serde::Deserialize))]
pub struct EdgeInner {
    /// Each edge consists of unordered pair of public keys of both peers.
    /// `key.0 < key.1` holds true.
    key: (PeerId, PeerId),
    /// `nonce` is unique number representing state of an edge.
    /// Odd number indicates that `edge` has been added, `even` number that it was removed.
    /// New edge starts with value of `1`.
    /// We update the edge only if it's `nonce` is higher. All versions of `Edge` with lower
    /// `nonce` will be ignored.
    nonce: u64,
    /// Each `edge` consists of two signatures, one for each `peer`.
    /// It's generated by signing triple (key.0, key.1, nonce) by each `peer` private key.
    /// `Signature` is generated at the time when edge is added, that is when `nonce` is `odd`.
    /// `Signature` can be verified by checking `peers` `PublicKey` against the signature.
    /// `Signature` from peer `key.0`.
    signature0: Signature,
    /// `Signature` from peer `key.1`.
    signature1: Signature,
    /// There are two cases:
    /// - `nonce` is odd, then `removal_info` will be None
    /// - `nonce` is even, then the structure will be a pair with a signature of the party removing
    ///           the edge:
    ///           - `bool` - `false` if `peer0` signed the `edge` `true` if `peer1`.
    ///           - `Signature` - `Signature` of either `peer0` or `peer1`, depending on which peer
    ///           removed the edge.
    removal_info: Option<(bool, Signature)>,
}

impl EdgeInner {
    /// Create an addition edge.
    fn new(
        peer0: PeerId,
        peer1: PeerId,
        nonce: u64,
        signature0: Signature,
        signature1: Signature,
    ) -> Self {
        let (peer0, signature0, peer1, signature1) = if peer0 < peer1 {
            (peer0, signature0, peer1, signature1)
        } else {
            (peer1, signature1, peer0, signature0)
        };

        Self { key: (peer0, peer1), nonce, signature0, signature1, removal_info: None }
    }

    fn hash(&self) -> CryptoHash {
        Edge::build_hash(&self.key.0, &self.key.1, self.nonce)
    }
}

/// State of a given edge.
/// Every edge starts in `Active` state. It can be removed and go to `Removed` state, and then
/// added back to go to `Active` state, etc.
#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, Debug, Hash)]
pub enum EdgeState {
    /// `Edge` is `Active` if there is an active connection between two peers on the network.
    Active,
    /// `Edge` is in `Removed` state if it was previously in `Active` state, but has been removed.
    /// A signature of one of the peers is requires, otherwise the edge will stay active.
    /// Though, it may be removed  from memory if both peers become unreachable.
    Removed,
}

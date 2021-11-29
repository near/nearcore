use borsh::{BorshDeserialize, BorshSerialize};
#[cfg(feature = "deepsize_feature")]
use deepsize::DeepSizeOf;
use near_crypto::{KeyType, SecretKey, Signature};
use near_primitives::borsh::maybestd::sync::Arc;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;

/// Information that will be ultimately used to create a new edge.
/// It contains nonce proposed for the edge with signature from peer.
#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(Clone, BorshSerialize, BorshDeserialize, PartialEq, Eq, Debug, Default)]
pub struct EdgeInfo {
    pub nonce: u64,
    pub signature: Signature,
}
impl EdgeInfo {
    pub fn new(peer0: &PeerId, peer1: &PeerId, nonce: u64, secret_key: &SecretKey) -> Self {
        let data = if peer0 < peer1 {
            Edge::build_hash(peer0, peer1, nonce)
        } else {
            Edge::build_hash(peer1, peer0, nonce)
        };

        let signature = secret_key.sign(data.as_ref());
        Self { nonce, signature }
    }
}

#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq)]
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
            key: (peer0, peer1),
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
        CryptoHash::hash_borsh(&(peer0, peer1, nonce))
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
    pub fn partial_verify(peer0: PeerId, peer1: PeerId, edge_info: &EdgeInfo) -> bool {
        let pk = peer1.public_key();
        let data = if peer0 < peer1 {
            Edge::build_hash(&peer0, &peer1, edge_info.nonce)
        } else {
            Edge::build_hash(&peer1, &peer0, edge_info.nonce)
        };
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
    pub fn to_simple_edge(&self) -> SimpleEdge {
        SimpleEdge::new(self.key().0.clone(), self.key().1.clone(), self.nonce())
    }

    /// Create the remove edge change from an added edge change.
    pub fn remove_edge(&self, my_peer_id: PeerId, sk: &SecretKey) -> Edge {
        assert_eq!(self.edge_type(), EdgeType::Added);
        let mut edge = self.0.as_ref().clone();
        edge.nonce += 1;
        let me = edge.key.0 == my_peer_id;
        let hash = edge.hash();
        let signature = sk.sign(hash.as_ref());
        edge.removal_info = Some((me, signature));
        Edge(Arc::new(edge))
    }

    fn hash(&self) -> CryptoHash {
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
            EdgeType::Added => {
                let data = self.hash();

                self.removal_info().is_none()
                    && self.signature0().verify(data.as_ref(), self.key().0.public_key())
                    && self.signature1().verify(data.as_ref(), self.key().1.public_key())
            }
            EdgeType::Removed => {
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
    pub fn edge_type(&self) -> EdgeType {
        if self.nonce() % 2 == 1 {
            EdgeType::Added
        } else {
            EdgeType::Removed
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
}

/// Edge object. Contains information relative to a new edge that is being added or removed
/// from the network. This is the information that is required.
#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "test_features", derive(serde::Serialize, serde::Deserialize))]
pub struct EdgeInner {
    /// Since edges are not directed `key.0 < peer1` should hold.
    key: (PeerId, PeerId),
    /// Nonce to keep tracking of the last update on this edge.
    /// It must be even
    nonce: u64,
    /// Signature from parties validating the edge. These are signature of the added edge.
    signature0: Signature,
    signature1: Signature,
    /// Info necessary to declare an edge as removed.
    /// The bool says which party is removing the edge: false for Peer0, true for Peer1
    /// The signature from the party removing the edge.
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

/// Represents edge between two nodes. Unlike `Edge` it doesn't contain signatures.
#[cfg_attr(feature = "deepsize_feature", derive(DeepSizeOf))]
#[derive(Hash, Clone, Eq, PartialEq, Debug)]
#[cfg_attr(feature = "test_features", derive(serde::Serialize, serde::Deserialize))]
pub struct SimpleEdge {
    key: (PeerId, PeerId),
    nonce: u64,
}

impl SimpleEdge {
    pub fn new(peer0: PeerId, peer1: PeerId, nonce: u64) -> SimpleEdge {
        let (peer0, peer1) = Edge::make_key(peer0, peer1);
        SimpleEdge { key: (peer0, peer1), nonce }
    }

    pub fn key(&self) -> &(PeerId, PeerId) {
        &self.key
    }

    pub fn nonce(&self) -> u64 {
        self.nonce
    }

    pub fn edge_type(&self) -> EdgeType {
        if self.nonce % 2 == 1 {
            EdgeType::Added
        } else {
            EdgeType::Removed
        }
    }
}

/// Status of the edge
#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, Debug, Hash)]
pub enum EdgeType {
    Added,
    Removed,
}

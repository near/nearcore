# Messages

All messages sent in the network are of type `PeerMessage`. A `PeerMessage` is encoded with [Protocol Buffers](https://protobuf.dev/), using the schema in [network.proto](https://github.com/near/nearcore/blob/master/chain/network/src/network_protocol/network.proto). Some fields of that schema are `bytes` fields that hold [Borsh](https://borsh.io/) data, for example signatures, public keys, `PeerInfo`, edges, blocks, block headers, transactions and routed messages. The Rust structures below show either this Borsh data or the Rust types that nearcore uses in memory. For details about data structures used as part of the message see the [reference code](https://github.com/nearprotocol/nearcore).

## Encoding

A `PeerMessage` is converted into an array of bytes (`Vec<u8>`) using protobuf serialization. An encoded message is the length of the serialized `PeerMessage` as a 4-byte little-endian unsigned integer, followed by the serialized `PeerMessage`.

The Borsh data inside `bytes` fields follows the [Borsh specification](https://github.com/nearprotocol/borsh#specification).

## Data structures

### PeerID

The id of a peer in the network is its [PublicKey](https://github.com/nearprotocol/nearcore/blob/master/core/crypto/src/signature.rs).

```rust
struct PeerId(PublicKey);
```

### PeerInfo


```rust
struct PeerInfo {
    id: PeerId,
    addr: Option<SocketAddr>,
    account_id: Option<AccountId>,
}
```

`PeerInfo` contains relevant information to try to connect to other peer. [`SocketAddr`](https://doc.rust-lang.org/std/net/enum.SocketAddr.html) is a tuple of the form: `IP:port`.

### AccountID

```rust
type AccountId = String;
```

### PeerMessage

```rust
enum PeerMessage {
    Tier1Handshake(Handshake),
    Tier2Handshake(Handshake),
    Tier3Handshake(Handshake),
    HandshakeFailure(PeerInfo, HandshakeFailureReason),
    /// When a failed nonce is used by some peer, this message is sent back as evidence.
    LastEdge(Edge),
    /// Contains edge information.
    SyncRoutingTable(RoutingTableUpdate),
    RequestUpdateNonce(PartialEdgeInfo),

    SyncAccountsData(SyncAccountsData),

    PeersRequest(PeersRequest),
    PeersResponse(PeersResponse),

    BlockHeadersRequest(Vec<CryptoHash>),
    BlockHeaders(Vec<Arc<BlockHeader>>),

    BlockRequest(CryptoHash),
    Block(Arc<Block>),
    OptimisticBlock(OptimisticBlock),

    Transaction(SignedTransaction),
    Routed(Box<RoutedMessage>),

    /// Gracefully disconnect from other peer.
    Disconnect(Disconnect),
    Challenge(Box<Challenge>),

    SyncSnapshotHosts(SyncSnapshotHosts),
    StateRequestHeader(ShardId, CryptoHash),
    StateRequestPart(ShardId, CryptoHash, u64),
    VersionedStateResponse(StateResponseInfo),

    EpochSyncRequest,
    EpochSyncResponse(CompressedEpochSyncProof),
}
```

This is the Rust enum that nearcore 2.14.0 uses in memory. `Arc` and `Box` do not change the encoding. On the wire, each variant is sent in its own field of the protobuf `PeerMessage`, and a message sets only one of these fields. For example, `Routed` is sent in the `routed` field. Its `borsh` field holds the Borsh encoding of the [`RoutedMessageV1`](#routedmessage) structure below. nearcore also accepts a routed message in the `routed_v3` field, but it does not send one. A received `update_nonce_response` field is handled as a `SyncRoutingTable` message with one edge. nearcore does not send this field.

`RequestUpdateNonce` carries a `PartialEdgeInfo`, which is the [EdgeInfo](#edgeinfo) structure below. `PeersRequest` has the optional limits `max_peers` and `max_direct_peers`. `PeersResponse` has two lists of `PeerInfo`: `peers` and `direct_peers`. `Disconnect` has a `remove_from_connection_store` flag. Challenges are disabled, and nodes ignore a `Challenge` message.

### Handshake

```rust
struct Handshake {
    /// Protocol version.
    pub version: u32,
    /// Oldest supported protocol version.
    pub oldest_supported_version: u32,
    /// Sender's peer id.
    pub peer_id: PeerId,
    /// Receiver's peer id.
    pub target_peer_id: PeerId,
    /// Sender's listening addr.
    pub listen_port: Option<u16>,
    /// Peer's chain information.
    pub chain_info: PeerChainInfoV2,
    /// Info for new edge.
    pub edge_info: EdgeInfo,
}
```

<!-- TODO: Make diagram about handshake process, since it is very complex -->

### Edge

```rust
struct Edge {
    /// Since edges are not directed `peer0 < peer1` should hold.
    peer0: PeerId,
    peer1: PeerId,
    /// Nonce to keep tracking of the last update on this edge.
    nonce: u64,
    /// Signature from parties validating the edge. These are signature of the added edge.
    signature0: Signature,
    signature1: Signature,
    /// Info necessary to declare an edge as removed.
    /// The bool says which party is removing the edge: false for Peer0, true for Peer1
    /// The signature from the party removing the edge.
    removal_info: Option<(bool, Signature)>,
}
```

### EdgeInfo

```rust
struct EdgeInfo {
    nonce: u64,
    signature: Signature,
}
```

### RoutedMessage

```rust
struct RoutedMessageV1 {
    /// Peer id which is directed this message.
    /// If `target` is hash, this a message should be routed back.
    target: PeerIdOrHash,
    /// Original sender of this message
    author: PeerId,
    /// Signature from the author of the message. If this signature is invalid we should ban
    /// last sender of this message. If the message is invalid we should ben author of the message.
    signature: Signature,
    /// Time to live for this message. After passing through some hop this number should be
    /// decreased by 1. If this number is 0, drop this message.
    ttl: u8,
    /// Message
    body: RoutedMessageBody,
}
```

### RoutedMessageBody

```rust
#[borsh(use_discriminant = true)]
#[repr(u8)]
enum RoutedMessageBody {
    BlockApproval(Approval) = 0,
    ForwardTx(SignedTransaction) = 1,
    TxStatusRequest(AccountId, CryptoHash) = 2,
    TxStatusResponse(FinalExecutionOutcomeView) = 3,
    PartialEncodedChunkRequest(PartialEncodedChunkRequestMsg) = 11,
    PartialEncodedChunkResponse(PartialEncodedChunkResponseMsg) = 12,
    /// Ping/Pong used for testing networking and routing.
    Ping(Ping) = 14,
    Pong(Pong) = 15,
    VersionedPartialEncodedChunk(PartialEncodedChunk) = 16,
    PartialEncodedChunkForward(PartialEncodedChunkForwardMsg) = 18,
    ChunkStateWitnessAck(ChunkStateWitnessAck) = 21,
    PartialEncodedStateWitness(PartialEncodedStateWitness) = 22,
    PartialEncodedStateWitnessForward(PartialEncodedStateWitness) = 23,
    VersionedChunkEndorsement(ChunkEndorsement) = 24,
    StatePartRequest(StatePartRequest) = 27,
    ChunkContractAccesses(ChunkContractAccesses) = 28,
    ContractCodeRequest(ContractCodeRequest) = 29,
    ContractCodeResponse(ContractCodeResponse) = 30,
    PartialEncodedContractDeploys(PartialEncodedContractDeploys) = 31,
    StateHeaderRequest(StateHeaderRequest) = 32,
    SpicePartialData(SpicePartialData) = 33,
    StateRequestAck(StateRequestAck) = 34,
    SpiceChunkEndorsement(SpiceChunkEndorsement) = 35,
    SpiceDataRequest(SpiceDataRequest) = 36,
    SpiceChunkContractAccesses(SpiceChunkContractAccesses) = 37,
    SpiceContractCodeRequest(SpiceContractCodeRequest) = 38,
    SpiceContractCodeResponse(SpiceContractCodeResponse) = 39,
    VersionedPartialEncodedStateWitness(VersionedPartialEncodedStateWitness) = 40,
    VersionedPartialEncodedStateWitnessForward(VersionedPartialEncodedStateWitness) = 41,
}
```

The number after each variant is its Borsh enum tag. Tags that are missing from the list belonged to variants that nearcore removed. The `Spice` variants belong to a protocol feature that is not part of any stable protocol version.

## CryptoHash

`CryptoHash` are objects with 256 bits of information.

```rust
pub struct Digest(pub [u8; 32]);

pub struct CryptoHash(pub Digest);
```

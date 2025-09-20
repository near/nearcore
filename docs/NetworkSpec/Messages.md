# Messages

All message sent in the network are of type `PeerMessage`. They are encoded using [Borsh](https://borsh.io/) which allows a rich structure, small size and fast encoding/decoding. For details about data structures used as part of the message see the [reference code](https://github.com/nearprotocol/nearcore).

## Encoding

A `PeerMessage` is converted into an array of bytes (`Vec<u8>`) using borsh serialization. An encoded message is conformed by 4 bytes with the length of the serialized `PeerMessage` concatenated with the serialized `PeerMessage`.

Check [Borsh specification](https://github.com/nearprotocol/borsh#specification) details to see how it handles each data structure.

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
    Handshake(Handshake),
    HandshakeFailure(PeerInfo, HandshakeFailureReason),
    /// When a failed nonce is used by some peer, this message is sent back as evidence.
    LastEdge(Edge),
    /// Contains accounts and edge information.
    Sync(SyncData),
    RequestUpdateNonce(EdgeInfo),
    ResponseUpdateNonce(Edge),
    PeersRequest,
    PeersResponse(Vec<PeerInfo>),
    BlockHeadersRequest(Vec<CryptoHash>),
    BlockHeaders(Vec<BlockHeader>),
    BlockRequest(CryptoHash),
    Block(Block),
    Transaction(SignedTransaction),
    Routed(RoutedMessage),
    /// Gracefully disconnect from other peer.
    Disconnect,
    Challenge(Challenge),
}
```

### AnnounceAccount

Each peer should announce its account

```rust
struct AnnounceAccount {
    /// AccountId to be announced.
    account_id: AccountId,
    /// PeerId from the owner of the account.
    peer_id: PeerId,
    /// This announcement is only valid for this `epoch`.
    epoch_id: EpochId,
    /// Signature using AccountId associated secret key.
    signature: Signature,
}
```

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
struct RoutedMessage {
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
enum RoutedMessageBody {
    BlockApproval(Approval),
    ForwardTx(SignedTransaction),

    TxStatusRequest(AccountId, CryptoHash),
    TxStatusResponse(FinalExecutionOutcomeView),
    QueryRequest {
        query_id: String,
        block_id_or_finality: BlockIdOrFinality,
        request: QueryRequest,
    },
    QueryResponse {
        query_id: String,
        response: Result<QueryResponse, String>,
    },
    ReceiptOutcomeRequest(CryptoHash),
    ReceiptOutComeResponse(ExecutionOutcomeWithIdAndProof),
    StateRequestHeader(ShardId, CryptoHash),
    StateRequestPart(ShardId, CryptoHash, u64),
    StateResponse(StateResponseInfo),
    PartialEncodedChunkRequest(PartialEncodedChunkRequestMsg),
    PartialEncodedChunkResponse(PartialEncodedChunkResponseMsg),
    PartialEncodedChunk(PartialEncodedChunk),
    Ping(Ping),
    Pong(Pong),
}
```

## CryptoHash

`CryptoHash` are objects with 256 bits of information.

```rust
pub struct Digest(pub [u8; 32]);

pub struct CryptoHash(pub Digest);
```

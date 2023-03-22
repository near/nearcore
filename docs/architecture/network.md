This document describes how our network works. At this moment, it is known to be
somewhat outdated, as we are in the process of refactoring the network protocol
somewhat significantly.

# 1. Overview

Near Protocol uses its own implementation of a custom peer-to-peer network. Peers
who join the network are represented by nodes and connections between them by edges.

The purpose of this document is to describe the inner workings of the `near-network`
package; and to be used as reference by future engineers to understand the network
code without any prior knowledge.

# 2. Code structure

`near-network` runs on top of the `actor` framework called 
[`Actix`](https://actix.rs/docs/). Code structure is split between 4 actors
`PeerManagerActor`, `PeerActor`, `RoutingTableActor`, `EdgeValidatorActor`

### 2.1 `EdgeValidatorActor` (currently called `EdgeVerifierActor` in the code<!-- TODO: rename -->)

`EdgeValidatorActor` runs on separate thread. The purpose of this `actor` is to
validate `edges`, where each `edge` represents a connection between two peers,
and it's signed with a cryptographic signature of both parties. The process of
edge validation involves verifying cryptographic signatures, which can be quite
expensive, and therefore was moved to another thread.

Responsibilities:
* Validating edges by checking whenever cryptographic signatures match.

### 2.2 `RoutingTableActor`

`RoutingTableActor` maintains a view of the `P2P network` represented by a set of
nodes and edges.

In case a message needs to be sent between two nodes, that can be done directly
through a `TCP connection`. Otherwise, `RoutingTableActor` is responsible for pinging
the best path between them.

Responsibilities:
* Keep set of all edges of `P2P network` called routing table.
* Connects to `EdgeValidatorActor`, and asks for edges to be validated, when
  needed.
* Has logic related to exchanging edges between peers.

### 2.3 `PeerActor`

Whenever a new connection gets accepted, an instance of `PeerActor` gets
created. Each `PeerActor` keeps a physical `TCP connection` to exactly one
peer.

Responsibilities:
* Maintaining physical connection.
* Reading messages from peers, decoding them, and then forwarding them to the
  right place.
* Encoding messages, sending them to peers on physical layer.
* Routing messages between `PeerManagerActor` and other peers.

### 2.4 `PeerManagerActor`

`PeerManagerActor` is the main actor of `near-network` crate. It acts as a
bridge connecting to the world outside, the other peers, and `ClientActor` and
`ClientViewActor`, which handle processing any operations on the chain.
`PeerManagerActor` maintains information about p2p network via `RoutingTableActor`,
and indirectly, through `PeerActor`, connections to all nodes on the network.
All messages going to other nodes, or coming from other nodes will be routed
through this `Actor`. `PeerManagerActor` is responsible for accepting incoming
connections from the outside world and creating `PeerActors` to manage them.

Responsibilities:
* Accepting new connections.
* Maintaining the list of `PeerActors`, creating, deleting them.
* Routing information about new edges between `PeerActors` and
  `RoutingTableManager`.
* Routing messages between `ViewClient`, `ViewClientActor` and `PeerActors`, and
  consequently other peers.
* Maintains `RouteBack` structure, which has information on how to send replies to messages.

# 3. Code flow - initialization

First, the `PeerManagerActor` actor gets started. `PeerManagerActor` opens the
TCP server, which listens to incoming connections. It starts the
`RoutingTableActor`, which then starts the `EdgeValidatorActor`. When
a incoming connection gets accepted, it starts a new `PeerActor`
on its own thread.

# 4. NetworkConfig

`near-network` reads configuration from `NetworkConfig`, which is a part of `client
config`.

Here is a list of features read from config:
* `boot_nodes` - list of nodes to connect to on start.
* `addr` - listening address.
* `max_num_peers` - by default we connect up to 40 peers, current implementation
  supports upto 128.

# 5. Connecting to other peers.

Each peer maintains a list of known peers. They are stored in the database. If
the database is empty, the list of peers, called boot nodes, will be read from
the `boot_nodes` option in the config. The peer to connect to is chosen at
random from a list of known nodes by the `PeerManagerActor::sample_random_peer`
method.

# 6. Edges & network - in code representation

`P2P network` is represented by a list of `peers`, where each `peer` is
represented by a structure `PeerId`, which is defined by the `peer`'s public key
`PublicKey`, and a list of edges, where each edge is represented by the
structure `Edge`.

Both are defined below.

# 6.1 PublicKey

We use two types of public keys:
* a 256 bit `ED25519` public key.
* a 512 bit `Secp256K1` public key.

Public keys are defined in the `PublicKey` enum, which consists of those two
variants.

```rust
pub struct ED25519PublicKey(pub [u8; 32]);
pub struct Secp256K1PublicKey([u8; 64]);
pub enum PublicKey {
    ED25519(ED25519PublicKey),
    SECP256K1(Secp256K1PublicKey),
}
```

# 6.2 PeerId

Each `peer` is uniquely defined by its `PublicKey`, and represented by `PeerId`
struct.

```rust
pub struct PeerId(PublicKey);
```

# 6.3 Edge

Each `edge` is represented by the `Edge` structure. It contains the following:
* pair of nodes represented by their public keys.
* `nonce` - a unique number representing the state of an edge. Starting with `1`.
  Odd numbers represent an active edge. Even numbers represent an edge in which
  one of the nodes, confirmed that the edge is removed.
* Signatures from both peers for active edges.
* Signature from one peer in case an edge got removed.

# 6.4 Graph representation

`RoutingTableActor` is responsible for storing and maintaining the set of all edges.
They are kept in the `edge_info` data structure of the type `HashSet<Edge>`.

```rust
pub struct RoutingTableActor {
    /// Collection of edges representing P2P network.
    /// It's indexed by `Edge::key()` key and can be search through by calling `get()` function
    /// with `(PeerId, PeerId)` as argument.
    pub edges_info: HashSet<Edge>,
    /// ...
}
```

# 7. Code flow - connecting to a peer - handshake

When `PeerManagerActor` starts, it starts to listen to a specific port.

## 7.1 - Step 1 - `monitor_peers_trigger` runs

`PeerManager` checks if we need to connect to another peer by running the
`PeerManager::is_outbound_bootstrap_needed` method. If `true` we will try to
connect to a new node. Let's call the current node, node `A`.

## 7.2 - Step 2 - choosing the node to connect to

Method `PeerManager::sample_random_peer` will be called, and it returns node `B`
that we will try to connect to.

## 7.3 - Step 3 - `OutboundTcpConnect` message

`PeerManagerActor` will send itself a message `OutboundTcpConnect` in order
to connect to node `B`.

```rust
pub struct OutboundTcpConnect {
    /// Peer information of the outbound connection
    pub target_peer_info: PeerInfo,
}
```

## 7.4 - Step 4 - `OutboundTcpConnect` message

On receiving the message the `handle_msg_outbound_tcp_connect` method will be
called, which calls `TcpStream::connect` to create a new connection.

## 7.5 - Step 5 - Connection gets established

Once connection with the outgoing peer gets established. The `try_connect_peer`
method will be called. And then a new `PeerActor` will be created and started. Once
the `PeerActor` starts it will send a `Handshake` message to the outgoing node `B`
over a tcp connection.

This message contains `protocol_version`, node `A`'s metadata, as well as all
information necessary to create an `Edge`.

```rust
pub struct Handshake {
    /// Current protocol version.
    pub(crate) protocol_version: u32,
    /// Oldest supported protocol version.
    pub(crate) oldest_supported_version: u32,
    /// Sender's peer id.
    pub(crate) sender_peer_id: PeerId,
    /// Receiver's peer id.
    pub(crate) target_peer_id: PeerId,
    /// Sender's listening addr.
    pub(crate) sender_listen_port: Option<u16>,
    /// Peer's chain information.
    pub(crate) sender_chain_info: PeerChainInfoV2,
    /// Represents new `edge`. Contains only `none` and `Signature` from the sender.
    pub(crate) partial_edge_info: PartialEdgeInfo,
}
```

## 7.6 - Step 6 - `Handshake` arrives at node `B`

Node `B` receives a `Handshake` message. Then it performs various validation
checks. That includes:
* Check signature of edge from the other peer.
* Whenever `nonce` is the edge, send matches.
* Check whether the protocol is above the minimum
  `OLDEST_BACKWARD_COMPATIBLE_PROTOCOL_VERSION`.
* Other node `view of chain` state.

If everything is successful, `PeerActor` will send a `RegisterPeer` message to
`PeerManagerActor`. This message contains everything needed to add `PeerActor`
to the list of active connections in `PeerManagerActor`.

Otherwise, `PeerActor` will be stopped immediately or after some timeout.

```rust
pub struct RegisterPeer {
    pub(crate) actor: Addr<PeerActor>,
    pub(crate) peer_info: PeerInfo,
    pub(crate) peer_type: PeerType,
    pub(crate) chain_info: PeerChainInfoV2,
    // Edge information from this node.
    // If this is None it implies we are outbound connection, so we need to create our
    // EdgeInfo part and send it to the other peer.
    pub(crate) this_edge_info: Option<EdgeInfo>,
    // Edge information from other node.
    pub(crate) other_edge_info: EdgeInfo,
    // Protocol version of new peer. May be higher than ours.
    pub(crate) peer_protocol_version: ProtocolVersion,
}
```

## 7.7 - Step 7 - `PeerManagerActor` receives `RegisterPeer` message - node `B`

In the `handle_msg_consolidate` method, the `RegisterPeer` message will be validated.
If successful, the `register_peer` method will be called, which adds the `PeerActor`
to the list of connected peers.

Each connected peer is represented in `PeerActorManager` in `ActivePeer` the data
structure.

<!-- TODO: Rename `ActivePeer` to `RegisterPeer` -->

```rust
/// Contains information relevant to an active peer.
struct ActivePeer { // will be renamed to `ConnectedPeer` see #5428
    addr: Addr<PeerActor>,
    full_peer_info: FullPeerInfo,
    /// Number of bytes we've received from the peer.
    received_bytes_per_sec: u64,
    /// Number of bytes we've sent to the peer.
    sent_bytes_per_sec: u64,
    /// Last time requested peers.
    last_time_peer_requested: Instant,
    /// Last time we received a message from this peer.
    last_time_received_message: Instant,
    /// Time where the connection was established.
    connection_established_time: Instant,
    /// Who started connection. Inbound (other) or Outbound (us).
    peer_type: PeerType,
}
```

## 7.8 - Step 8 - Exchange routing table part 1 - node `B`

At the end of the `register_peer` method node `B` will perform a
`RoutingTableSync` sync. Sending the list of known `edges` representing a
full graph, and a list of known `AnnounceAccount`. Those will be
covered later, in their dedicated sections see sections (to be added). <!-- TODO: TODO1, TODO2 -->

```rust, ignore
message: PeerMessage::RoutingTableSync(SyncData::edge(new_edge)),
```

```rust
/// Contains metadata used for routing messages to particular `PeerId` or `AccountId`.
pub struct RoutingTableSync { // also known as `SyncData` (#5489)
    /// List of known edges from `RoutingTableActor::edges_info`.
    pub(crate) edges: Vec<Edge>,
    /// List of known `account_id` to `PeerId` mappings.
    /// Useful for `send_message_to_account` method, to route message to particular account.
    pub(crate) accounts: Vec<AnnounceAccount>,
}
```

## 7.9 - Step 9 -  Exchange routing table part 2 - node `A`

Upon receiving a `RoutingTableSync` message. Node `A` will reply with its own
`RoutingTableSync` message.

## 7.10 - Step 10 -  Exchange routing table part 2 - node `B`

Node `B` will get the message from `A` and update its routing table.

# 8. Adding new edges to routing tables

This section covers the process of adding new edges, received from another
node, to the routing table. It consists of several steps covered below.

## 8.1 Step 1

`PeerManagerActor` receives `RoutingTableSync` message containing list of new
`edges` to add. `RoutingTableSync` contains list of edges of the P2P network.
This message is then forwarded to `RoutingTableActor`.

## 8.2 Step 2

`PeerManagerActor` forwards those edges to `RoutingTableActor` inside of
the `ValidateEdgeList` struct.

`ValidateEdgeList` contains:
* list of edges to verify.
* peer who sent us the edges.

## 8.3 Step 3

`RoutingTableActor` gets the `ValidateEdgeList` message. Filters out `edges`
that have already been verified, those that are already in
`RoutingTableActor::edges_info`.

Then, it updates `edge_verifier_requests_in_progress` to mark that edge
verifications are in progress, and edges shouldn't be pruned from Routing Table
(see section (to be added)<!-- TODO: add section link -->).

Then, after removing already validated edges, the modified message is forwarded
to `EdgeValidatorActor`.

## 8.4 Step 4

`EdgeValidatorActor` goes through the list of all edges. It checks whether all edges
are valid (their cryptographic signatures match, etc.).

If any edge is not valid, the peer will be banned.

Edges that are validated are written to a concurrent queue
`ValidateEdgeList::sender`. This queue is used to transfer edges from
`EdgeValidatorActor` back to `PeerManagerActor`.

## 8.5 Step 5

`broadcast_validated_edges_trigger` runs, and gets validated edges from
`EdgeVerifierActor`.

Every new edge will be broadcast to all connected peers.

And then, all validated edges received from `EdgeVerifierActor` will be sent
again to `RoutingTableActor` inside `AddVerifiedEdges`.

## 8.5 Step 6

When `RoutingTableActor` receives `RoutingTableMessages::AddVerifiedEdges`, the
method `add_verified_edges_to_routing_table` will be called. It will add edges to
`RoutingTableActor::edges_info` struct, and mark routing table, that it needs
a recalculation (see `RoutingTableActor::needs_routing_table_recalculation`).

# 9 Routing table computation

Routing table computation does a few things:
* For each peer `B`, calculates set of peers `|C_b|`, such that each peer is on
  the shortest path to `B`.
* Removes unreachable edges from memory and stores them to disk.
* The distance is calculated as the minimum number of nodes on the path from
  given node `A`, to each other node on the network. That is, `A` has a distance
  of `0` to itself. It's neighbors will have a distance of `1`. The neighbors of
  theirs neighbors will have a distance of `2`, etc.

## 9.1 Step 1

`PeerManagerActor` runs a `update_routing_table_trigger` every
`UPDATE_ROUTING_TABLE_INTERVAL` seconds.

`RoutingTableMessages::RoutingTableUpdate` message is sent to
`RoutingTableActor` to request routing table re-computation.

## 9.2 Step 2

`RoutingTableActor` receives the message, and then:
* calls `recalculate_routing_table` method, which computes
  `RoutingTableActor::peer_forwarding: HashMap<PeerId, Vec<PeerId>>`. For each
  `PeerId` on the network, gives a list of connected peers, which are on the
  shortest path to the destination. It marks reachable peers in the
  `peer_last_time_reachable` struct.
* calls `prune_edges` which removes from memory all the edges that were not
  reachable for at least 1 hour, based on the `peer_last_time_reachable` data
  structure. Those edges are then stored to disk.

## 9.3 Step 3

`RoutingTableActor` sends a `RoutingTableUpdateResponse` message back to
`PeerManagerActor`.

`PeerManagerActor` keeps a local copy of `edges_info`, called `local_edges_info`
containing only edges adjacent to current node.
* `RoutingTableUpdateResponse` contains a list of local edges, which
  `PeerManagerActor` should remove.
* `peer_forwarding` which represents how to route messages in the P2P network
* `peers_to_ban` represents a list of peers to ban for sending us edges, which failed
  validation in `EdgeVerifierActor`.


## 9.4 Step 4

`PeerManagerActor` receives `RoutingTableUpdateResponse` and then:
* updates local copy of `peer_forwarding`, used for routing messages.
* removes `local_edges_to_remove` from `local_edges_info`.
* bans peers, who sent us invalid edges.

# 10. Message transportation layers.

This section describes different protocols of sending messages currently used in
`Near`.

## 10.1 Messages between Actors.

`Near` is build on `Actix`'s `actor`
[framework](https://actix.rs/book/actix/sec-2-actor.html). Usually each actor
runs on its own dedicated thread. Some, like `PeerActor` have one thread per
each instance. Only messages implementing `actix::Message`, can be sent
using between threads. Each actor has its own queue; Processing of messages
happens asynchronously.

We should not leak implementation details into the spec.

Actix messages can be found by looking for `impl actix::Message`.

## 10.2 Messages sent through TCP

Near is using `borsh` serialization to exchange messages between nodes (See
[borsh.io](https://borsh.io/)). We should be careful when making changes to
them. We have to maintain backward compatibility. Only messages implementing
`BorshSerialize`, `BorshDeserialize` can be sent. We also use `borsh` for
database storage.

## 10.3 Messages sent/received through `chain/jsonrpc`

Near runs a `json REST server`. (See `actix_web::HttpServer`). All messages sent
and received must implement `serde::Serialize` and `serde::Deserialize`.

# 11. Code flow - routing a message

This is the example of the message that is being sent between nodes
[`RawRoutedMessage`](https://github.com/near/nearcore/blob/fa8749dc60fe0de8e94c3046571731c622326e9f/chain/network-primitives/src/types.rs#L362).

Each of these methods have a `target` - that is either the `account_id` or `peer_id`
or hash (which seems to be used only for route back...). If target is the
account - it will be converted using `routing_table.account_owner` to the peer.

Upon receiving the message, the `PeerManagerActor`
[will sign it](https://github.com/near/nearcore/blob/master/chain/network/src/peer_manager.rs#L1285)
and convert into RoutedMessage (which also have things like TTL etc.).

Then it will use the `routing_table`, to find the route to the target peer (add
`route_back` if needed) and then send the message over the network as
`PeerMessage::Routed`. Details about routing table computations are covered in
[section 8](#8-adding-new-edges-to-routing-tables). 

When Peer receives this message (as `PeerMessage::Routed`), it will pass it to
PeerManager (as `RoutedMessageFrom`), which would then check if the message is
for the current `PeerActor`. (if yes, it would pass it for the client) and if
not - it would pass it along the network.

All these messages are handled by `receive_client_message` in Peer.
(`NetworkClientMessags`) - and transferred to `ClientActor` in
(`chain/client/src/client_actor.rs`)

`NetworkRequests` to `PeerManager` actor trigger the `RawRoutedMessage` for
messages that are meant to be sent to another `peer`.

`lib.rs` (`ShardsManager`) has a `network_adapter` - coming from the client’s
`network_adapter` that comes from `ClientActor` that comes from the `start_client` call
that comes from `start_with_config` (that creates `PeerManagerActor` - that is
passed as target to `network_recipent`).

# 12. Database

### 12.1 Storage of deleted edges

Every time a group of peers becomes unreachable at the same time; We store edges
belonging to them in components. We remove all of those edges from memory, and
save them to the database. If any of them were to be reachable again, we would
re-add them. This is useful in case there is a network split, to recover edges
if needed.

Each component is assigned a unique `nonce`, where first one is assigned nonce
0. Each new component gets assigned a consecutive integer.

To store components, we have the following columns in the DB.
* `DBCol::LastComponentNonce` Stores `component_nonce: u64`, which is the last
  used nonce.
* `DBCol::ComponentEdges` Mapping from `component_nonce` to a list of edges.
* `DBCol::PeerComponent` Mapping from `peer_id` to the last component `nonce` it belongs to.

### 12.2 Storage of `account_id` to `peer_id` mapping

`ColAccountAnouncements` -> Stores a mapping from `account_id` to a tuple
(`account_id`, `peer_id`, `epoch_id`, `signature`).

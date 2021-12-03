# 1. Overview
Near Protocol uses its own implementation of a custom peer-to-peer network
Peers who join network are represented by nodes and connections between them by edges.

The purpose of this document is to describe inner workings of `near-network` package;
and to be used as reference by future engineers to understand network code without any prior knowledge.

# 2. Code structure
`near-network` runs on top of `actor` framework called `Actix` (https://actix.rs/docs/).
Code structure is split between 4 actors `PeerManagerActor`, `PeerActor`, `RoutingTableActor`, `EdgeValidatorActor`

### 2.1 `EdgeValidatorActor` (currently called `EdgeVerifierActor` in the code (TODO rename))
`EdgeValidatorActor` runs on separate thread.
The purpose of this `actor` is to validate `edges`, where each `edge` represents a connection between two peers,
and it's signed with a cryptographic signature of both parties.
The process of edge validation involves verifying cryptographic signatures, which can be quite expensive,
and therefore was moved to another thread.

Responsibilities:
- validating edges by checking whenever cryptographic signatures match.

### 2.2 `RoutingTableActor`
`RoutingTableActor` maintain view of the `P2P network` represented by set of nodes and edges.

In case a message needs to be sent between two nodes, that can be done directly through `Tcp` connection.
Otherwise, `RoutingTableActor` is responsible for ping the best path between them.

Responsibilities:
- keep set of all edges of `P2P network` called routing table
- connects to `EdgeValidatorActor`, and asks for edges to be validated, when needed
- has logic related to exchanging edges between peers

### 2.3 `PeerActor`
Whenever a new connection gets accepted, an instance of `PeerActor` gets created.
Each `PeerActor` keeps a physical a `TCP connection` to exactly one peer.

Responsibilities:
- Maintaining physical connection.
- Reading messages from peers, decoding them, and then forwarding them to the right place.
- Encoding messages, sending them to peers on physical layer.
- Routing messages between `PeerManagerActor` and other peers.

### 2.4 `PeerManagerActor`
`PeerManagerActor` is the main actor of `near-network` crate.
It's acts as a bridge connecting to the world outside, the other peers, and `ClientActor` and `ClientViewActor`, which
handle processing any operations on the chain.
`PeerManagerActor` maintains information about p2p network via (Routing Table Actor),
and indirectly, through `PeerActor`, connections to all some nodes on the network.
All messages going to other nodes, or coming from other nodes will be routed through this `Actor`.
`PeerManagerActor` is responsible for accepting incoming connections from the outside world
and creating `PeerActors` to manage them.

Responsibilities:
- Accepting new connections
- Maintaining list of `PeerActors`, creating, deleting them
- Routing information about new edges between `PeerActors` and `RoutingTableManager`
- Routing messages between `ViewClient`, `ViewClientActor` and `PeerActors`, and consequently other peers.
- Maintains `RouteBack` structure, which has information on how to send replies to messages

# 3. Code flow - initialization
`PeerManagerActor` actor gets started.
`PeerManagerActor` open tcp server, which listens to incoming connection.
It starts `RoutingTableActor`, which then starts `EdgeValidatorActor`.
When connection incoming connection gets accepted, it starts a new `PeerActor` on its own thread.
# 8 Routing table computation

Routing table computation does a few things:
- For each peer `B`, calculates set of peers `|C_b|`, such that each peer is on the shortest path to `B`.
- Removing unreachable edges from memory and storing them to disk.
- The distance is calculated as the minimum number of nodes on the path from given node `A`, to each other node 
on the network. That is, `A` has a distance of `0` to itself. It's neighbors will have a distance of `1`.
The neighbors of theirs neighbors will have a distance of `2`, etc.

## 8.1 Step 1
`PeerManagerActor` runs a `update_routing_table_trigger` every `UPDATE_ROUTING_TABLE_INTERVAL` seconds.

`RoutingTableMessages::RoutingTableUpdate` message is sent to `RoutingTableActor` to request routing table re-computation.

```rust,ignore
RoutingTableMessages::RoutingTableUpdate {
    /// An enum, used for testing, by default pruning is done once an hour.
    prune: Prune,
    /// A duration on when to prune edges, by default we will remove peers not reachable for an hour.
    prune_edges_not_reachable_for: Duration,
},
```

## 8.2 Step 2
`RoutingTableActor` receives the message, and then
- calls `recalculate_routing_table` method, which computes `RoutingTableActor::peer_forwarding: HashMap<PeerId, Vec<PeerId>>`.
For each `PeerId` on the network, gives list of connected peers, which are on the shortest path to the destination.
It marks reachable peers in `peer_last_time_reachable` struct.
- calls `prune_edges` which removes from memory all edges, that were not reachable for at least 1 hour, based on `peer_last_time_reachable` data structure.
Those edges are then stored to disk.

## 8.3 Step 3
`RoutingTableActor` sends `RoutingTableUpdateResponse` message back to `PeerManagerActor`.

`PeerManagerActor` keep local copy of `edges_info`, called `local_edges_info` containing only edges adjacent to current node.

- This message contains list of local edges, which `PeerManagerActor` should remove.
- `peer_forwarding` which represent on how to route messages in the P2P network
- `peers_to_ban` - list of peers to ban for sending us edges, which failed validation in `EdgeVerifierActor`.

```rust,ignore
    RoutingTableUpdateResponse {
        /// PeerManager maintains list of local edges. We will notify `PeerManager`
        /// to remove those edges.
        local_edges_to_remove: Vec<Edge>,
        /// Active PeerId that are part of the shortest path to each PeerId.
        peer_forwarding: Arc<HashMap<PeerId, Vec<PeerId>>>,
        /// List of peers to ban for sending invalid edges.
        peers_to_ban: Vec<PeerId>,
    },
```

## 8.4 Step 4
`PeerManagerActor` received `RoutingTableUpdateResponse` and then:
- updates local copy of`peer_forwarding`, used for routing messages.
- removes `local_edges_to_remove` from `local_edges_info`.
- bans peers, who sent us invalid edges.

# 11. Code flow - routing a message
This is the example of the message that is being sent between nodes (`RawRoutedMessage`) (https://github.com/near/nearcore/blob/fa8749dc60fe0de8e94c3046571731c622326e9f/chain/network-primitives/src/types.rs#L362)

Each of these methods have a `target` - that is either the account_id or peer_id or hash (which seems to be used only for route back...). 
If target is the account - it will be converted using `routing_table.account_owner` to the peer.

Upon receiving the message, the `PeerManagerActor` will sign it (https://github.com/near/nearcore/blob/master/chain/network/src/peer_manager.rs#L1285)
And convert into RoutedMessage (which also have things like TTL etc.).

Then it will use the `routing_table`, to find the route to the target peer (add `route_back` if needed) and then send the message over the network as `PeerMessage::Routed`.
Details about routing table computations are covered in section 8.

When Peer receives this message (as `PeerMessage::Routed`), it will pass it to PeerManager (as `RoutedMessageFrom`), 
which would then check if the message is for the current `PeerActor`. (if yes, it would pass it for the client) and if not - it would pass it along the network.

All these messages are handled by `receive_client_message` in Peer. (`NetworkClientMessags`) - and transferred to `ClientActor` in (`chain/client/src/client_actor.rs`)

`NetworkRequests` to `PeerManager` actor trigger the `RawRoutedMessage` for messages that are meant to be sent to another `peer`.

`lib.rs` (`ShardsManager`) has a `network_adapter` - coming from client’s network_adapter that comes from `ClientActor` that comes from start_client call that comes from `start_with_config`
(that crates `PeerManagerActor` - that is passed as target to `network_recipent`).

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

# 7. Adding new edges to routing tables

This section covers the process of adding new edges, received from another nodes,
to the routing table. It consists of several steps covered below.

## 7.1 Step 1

`PeerManagerActor` receives `RoutingTableSync` message containing list of new `edges` to add.
`RoutingTableSync` contains list of edges of the P2P network.
This message is then forwarded to `RoutingTableActor`.

## 7.2 Step 2

`PeerManagerActor` forwards those edges to `RoutingTableActor` inside of `ValidateEdgeList` struct.

`ValidateEdgeList` contains:
- list of edges to verify
- peer who send us the edges

## 7.3 Step 3

`RoutingTableActor` gets the `ValidateEdgeList` message.
Filters out `edges` that have already been verified, those that are already in `RoutingTableActor::edges_info`.

Then, it updates `edge_verifier_requests_in_progress` to mark that edge verifications are in progress, and edges shouldn't
be pruned from Routing Table (see section TODO).

Then, after removing already validated edges, the modified message is forwarded to `EdgeValidatorActor`.

## 7.4 Step 4

`EdgeValidatorActor` goes through list of all edges.
It checks whenever all edges are valid (their cryptographic signatures match, etc.).

If any edge is not valid peer will be banned.

Edges that are validated are written to a concurrent queue `ValidateEdgeList::sender`.
This queue is used to transfer edges from `EdgeValidatorActor`, back to `PeerManagerActor`.

## 7.5 Step 5

`broadcast_validated_edges_trigger` runs, and gets validated edges from `EdgeVerifierActor`.

Every new edge will be broadcast to all connected peers.

And then, all validated edges received from `EdgeVerifierActor` will be sent again to `RoutingTableActor` inside
`AddVerifiedEdges`.


## 7.5 Step 6

When `RoutingTableActor` receives `RoutingTableMessages::AddVerifiedEdges`, the method`add_verified_edges_to_routing_table` will be called.
It will add edges to `RoutingTableActor::edges_info` struct, and mark routing table, that it needs recalculation
see `RoutingTableActor::needs_routing_table_recalculation`.

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

- `RoutingTableUpdateResponse` contains list of local edges, which `PeerManagerActor` should remove.
- `peer_forwarding` which represent on how to route messages in the P2P network
- `peers_to_ban` - list of peers to ban for sending us edges, which failed validation in `EdgeVerifierActor`.


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

# 12. Database

### 12.1 Storage of deleted edges

Everytime a group of peers becomes unreachable at the same time; We store edges belonging to
them in components. We remove all of those edges from memory, and save them to database,
If any of them were to be reachable again, we would re-add them. This is useful in case there is a network split,
to recover edges if needed.

Each component is assigned a unique `nonce`, where first one is assigned nonce 0. Each new component, a get
assigned a consecutive integer.

To store components, we have the following columns in the DB.
- `ColLastComponentNonce` Stores `component_nonce: u64`, which is the last used nonce.
- `ColComponentEdges` Mapping from `component_nonce` to list of edges.
- `ColPeerComponent` Mapping from `peer_id` to last component `nonce` it belongs to.

### 12.2 Storage of `account_id` to `peer_id` mapping

`ColAccountAnouncements` -> Stores a mapping from `account_id` to tuple (`account_id`, `peer_id`, `epoch_id`, `signature`).

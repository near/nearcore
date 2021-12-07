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

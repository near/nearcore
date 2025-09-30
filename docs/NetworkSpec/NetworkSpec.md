# Network

<!-- cspell:ignore permissioned isinstance -->
Network layer constitutes the lower level of the NEAR protocol and is ultimately responsible of transporting messages between peers. To provide an efficient routing it maintains a routing table between all peers actively connected to the network, and sends messages between them using best paths. There is a mechanism in place that allows new peers joining the network to discover other peers, and rebalance network connections in such a way that latency is minimized. Cryptographic signatures are used to check identities from peers participating in the protocol since it is non-permissioned system.

This document should serve as reference for all the clients to implement networking layer.

## Messages

Data structures used for messages between peers are enumerated in [Message](Messages.md).

## Discovering the network

When a node starts for the first time it tries to connect to a list of bootstrap nodes specified via a config file. The address for each node

It is expected that a node periodically requests a list of peers from its neighboring nodes to learn about other nodes in the network. This will allow every node to discover each other, and have relevant information to try to establish a new connection with it. When a node receives a message of type [`PeersRequest`](Messages.md#peermessage) it is expected to answer with a message of type [`PeersResponse`](Messages.md#peermessage) with information from healthy peers known to this node.

### Handshakes

To establish a new connections between pair of nodes, they will follow the following protocol. Node A open a connection with node B and sends a [Handshake](Messages.md#handshake) to it. If handshake is valid (see reasons to [decline the handshake](#decline-handshake)) then node B will proceed to send [Handshake](Messages.md#handshake) to node A. After each node accept a handshake it will mark the other node as an active connection, until one of them stop the connection.

[Handshake](Messages.md#handshake) contains relevant information about the node, the current chain and information to create a new edge between both nodes.

#### Decline handshake

When a node receives a handshake from other node it will decline this connection if one of the following situations happens:

1. Other node has different genesis.
2. Edge nonce is too low

#### Edge

Edges are used to let other nodes in the network know that there is currently an active connection between a pair of nodes. See the definition of [this data structure](Messages.md#edge).

If the nonce of the edge is odd, it denotes an `Added` edge, otherwise it denotes a `Removed` edge. Each node should keep track of the nonce used for edges between every pair of nodes. Peer C believes that the peers A and B are currently connected if and only if the edge with the highest nonce known to C for them has an odd nonce.

When two nodes successfully connect to each other, they broadcast the new edge to let other peers know about this connection. When a node is disconnected from other node, it should bump the nonce by 1, sign the new edge and broadcast it to let other nodes know that the connection was removed.

A removed connection will be valid, if it contains valid information from the added edge it is invalidating. This prevents peers bump nonce by more than one when deleting an edge.

When node A proposes an edge to B with nonce X, it will only accept it and sign it iff:

- X = 1 and B doesn't know about any previous edge between A and B
- X is odd and X > Y where Y is the nonce of the edge with the highest nonce between A and B known to B.

<!-- TODO: What is a valid edge: pseudo code -->

## Routing Table

Every node maintains a routing table with all existing connections and relevant information to route messages. The explicit graph with all active connection is stored at all times.

```rust
struct RoutingTable {
    /// PeerId associated for every known account id.
    account_peers: HashMap<AccountId, AnnounceAccount>,
    /// Active PeerId that are part of the shortest path to each PeerId.
    peer_forwarding: HashMap<PeerId, HashSet<PeerId>>,
    /// Store last update for known edges.
    edges_info: HashMap<(PeerId, PeerId), Edge>,
    /// Hash of messages that requires routing back to respective previous hop.
    route_back: HashMap<CryptoHash, PeerId>,
    /// Current view of the network. Nodes are Peers and edges are active connections.
    raw_graph: Graph,
}

```

- `account_peers` is a mapping from each known account to the correspondent [announcement](Messages.md#announceaccount). Given that validators are known by its [AccountId](Messages.md#accountid) when a node needs to send a message to a validator it finds the [PeerId](Messages.md#peerid) associated with the [AccountId](Messages.md#accountid) in this table.

- `peer_forwarding`: For node `S`, `peer_forwarding` constitutes a mapping from each [PeerId](Messages.md#peerid) `T`, to the set of peers that are directly connected to `S` and belong to the shortest route, in terms of number of edges, between `S` and `T`. When node `S` needs to send a message to node `T` and they are not directly connected, `S` choose one peer among the set `peer_forwarding[S]` and sends a routed message to it with destination `T`.

<!-- TODO: Add example. Draw a graph. Show routing table for each node. -->
<!-- TODO: Notice when two nodes are totally disconnected, and when two nodes are directly connected -->

- `edges_info` is a mapping between each unordered pair of peers `A` and `B` to the edge with highest nonce known between those peers. It might be

- `route_back` used to compute the route for certain messages. Read more about it on [Routing back section](#routing-back)

- `raw_graph` is the explicit [graph](https://en.wikipedia.org/wiki/Graph_(discrete_mathematics)) representation of the network. Each vertex of this graph is a peer, and each edge is an active connection between a pair of peers, i.e. the edge with highest nonce between this pair of peers is of type `Added`. It is used to compute the shortest path from the source to all other peers.

### Updates

`RoutingTable` should be update accordingly when the node receives updates from the network:

- New edges: `edges_info` map is updated with new edges if their nonce is higher. This is relevant to know whether a new connection was created, or some connection stopped.

```python
def on_edges_received(self, edges):
    for edge in edges:
        # Check the edge is valid
        if edge.is_valid():
            peer0 = edge.peer0
            peer1 = edge.peer1

            # Find edge with higher nonce known up to this point.
            current_edge = self.routing_table.edges_info.get((peer0, peer1))

            # If there is no known edge, or the known edge has smaller nonce
            if current_edge is None or current_edge.nonce < edge.nonce:
                # Update with the new edge
                self.routing_table.edges_info[(peer0, peer1)] = edge
```

- Announce account: `account_peers` map is updated with announcements from more recent epochs.

```python
def on_announce_accounts_received(self, announcements):
    for announce_account in announcements:
        # Check the announcement is valid
        if announce_account.is_valid():
            account_id = announce_account.account_id

            # Find most recent announcement for account_id being announced
            current_announce_account = self.routing_table.account_peers.get(account_id)

            # If new epoch is happens after current epoch.
            if announce_account.epoch > current_announce_account.epoch:
                # Update with the new announcement
                self.routing_table.account_peers[announce_id] = announce_account
```

- Route back messages: Read about it on [Routing back section](#routing-back)

### Routing

When a node needs to send a message to another peer, it checks in the routing table if it is connected to that peer, possibly not directly but through several hops. Then it select one of the shortest path to the target peer and sends a [`RoutedMessage`](Messages.md#routedmessage) to the first peer in the path.

When it receives a [`RoutedMessage`](Messages.md#routedmessage), it check if it is the target, in that case consume the body of the message, otherwise it finds a route to the target following described approach and sends the message again. It is important that before routing a message each peer check signature from original author of the message, passing a message with invalid signature can result in ban for the sender. It is not required however checking the content of the message itself.

Each [`RoutedMessage`](Messages.md#routedmessage) is equipped with a time-to-live integer. If this message is not for the node processing it, it decrement the field by one before routing it; if the value is 0, the node drops the message instead of routing it.

#### Routing back

It is possible that node `A` is known to `B` but not the other way around. In case node `A` sends a request that requires a response to `B`, the response is routed back through the same path used to send the message from `A` to `B`. When a node receives a [RoutedMessage](Messages.md#routedmessage) that requires a response it stores in the map `route_back` the hash of the message mapped to the [PeerId](Messages.md#peerid) of the sender.   After the message reaches the final destination and response is computed it is routed back using as target the hash of the original message. When a node receives a Routed Message such that the target is a hash, the node checks for the previous sender in the `route_back` map, and sends the message to it if it exists, otherwise it drops the message.

The hash of a `RoutedMessage` to be stored on the map `route_back` is computed as:

```python
def route_back_hash(routed_message):
    return sha256(concat(
        borsh(routed_message.target),
        borsh(routed_message.author),
        borsh(routed_message.body)
    ))
```

```python
def send_routed_message(self, routed_message, sender):
    """
    sender: PeerId of the node through which the message was received.

    Don't confuse sender with routed_message.author:
    routed_message.author is the PeerId from the original creator of the message

    The only situation in which sender == routed_message.author is when the message was
    not received from the network, but was created by the node and should be routed.
    """
    if routed_message.requires_response():
        crypto_hash = route_back_hash(routed_message)
        self.routing_table.route_back[crypto_hash] = sender

    next_peer = self.find_closest_peer_to(routed_message.target)
    self.send_message(next_peer, routed_message)

def on_routed_message_received(self, routed_message, sender):
    # routed_message.target is of type CryptoHash or PeerId
    if isinstance(routed_message.target, CryptoHash):
        # This is the response for a routed message.
        # `target` is the PeerId that sent this message.
        target = self.routing_table.route_back.get(routed_message.target)

        if target is None:
            # Drop message if there is no known route back for it
            return
        else:
            del self.routing_table.route_back[routed_message.target]
    else:
        target = routed_message.target

    if target == self.peer_id:
        self.handle_message(routed_message.body)
    else:
        self.send_routed_message(routed_message, sender)
```

### Synchronization

When two node connect to each other they exchange all known edges (from `RoutingTable::edges_info`) and account announcements (from `RoutingTable::account_peers`). Also they broadcast the newly created edge to all nodes directly connected to them. After a node learns about a new `AnnounceAccount` or a new `Edge` they automatically broadcast this information to the rest of the nodes, so everyone is kept up to date.

## Security

Messages exchanged between peers (both direct or routed) are cryptographically signed with sender private key. Nodes ID contains public key of each node that allow other peer to verify this messages. To keep secure communication most of this message requires some nonce/timestamp that forbid a malicious actor reuse a signed message out of context.

In the case of routing messages each intermediate hop should verify that message hash and signatures are valid before routing to next hop.

### Abusive behavior

When a node A sends more than `MAX_PEER_MSG_PER_MIN` messages per minute to node B, it will be banned and unable to keep sending messages to it. This a protection mechanism against abusive node to avoid being spammed by some peers.

## Implementation Details

There are some issues that should be handled by the network layer but details about how to implement them are not enforced by the protocol, however we propose here how to address them.

### Balancing network

[Github comment](https://github.com/nearprotocol/nearcore/issues/2395#issuecomment-610077017)

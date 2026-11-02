# Routing table exchange algorithm

Every node on the network stores its own copy of the [Routing Graph](./NetworkSpec.md#routing-table).
nearcore exchanges this graph in two steps.
When a connection is set up, the two nodes do a full sync.
After that, they send only incremental updates.

After a handshake on a TIER2 connection completes, each node sends one `SyncRoutingTable` message to the other node.
The message holds a `RoutingTableUpdate` with all edges that the node knows.
After that, a node broadcasts only edges that it has not seen before, also in `SyncRoutingTable` messages.
Up to nearcore 2.14.0, these messages also carry account announcements.
TIER1 and TIER3 connections do not exchange the routing table.
See the [Synchronization](./NetworkSpec.md#synchronization) section of the network specification and the [PeerMessage](./Messages.md#peermessage) listing.

# Removed proposal

An earlier [proposal](https://github.com/near/NEPs/pull/220) described a partial exchange that sends only the difference between two routing tables.
It used Inverse Bloom Filters and a `RoutingSyncV2` message.
nearcore had a prototype of it behind a nightly-only build feature.
nearcore 1.28.0 removed the prototype, and it was never active on mainnet or testnet.
The `RoutingSyncV2` message does not exist on the network.

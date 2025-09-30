# Routing table exchange algorithm

<!-- cspell:ignore neps -->
- Proposal Name: New routing table exchange algorithm
- Start Date: 6/21/2021
- NEP PR: [`nearprotocol/neps#0000`](https://github.com/near/nearcore/pull/4112)
- Issue(s): https://github.com/near/nearcore/issues/3838.
- [GitHub PR](https://github.com/near/NEPs/pull/220)

# Summary

Currently, every node on the network stores its own copy of the [Routing Graph](./NetworkSpec.md#routing-table).
The process of exchanging and verifying edges requires sending full copy of routing table on every synchronization.
Sending full copy of routing table is wasteful, in this spec we propose a solution, which allows doing partial  synchronization.
This should reduce both bandwidth used, and amount of CPU time.

Typically, only one full sync would be required when a node connects to the network for the first time.
For given two nodes doing synchronization, we propose a method, which requires bandwidth/processing time proportional to the size of data which needs to be exchanged for given nodes.

# Algorithm

In this spec we describe the implementation of `Routing Graph Reconciliation` algorithm using Inverse Bloom Filters.
An overview of different `Routing Graph Reconciliation` algorithms can be a found at
[ROUTING_GRAPH_RECONCILIATION.pdf](https://github.com/pmnoxx/docs/blob/piotr-test-markdown/near/ROUTING_GRAPH_RECONCILIATION.pdf)

The process of exchanging `routing tables` involves exchanging edges between a pair of nodes.
For given nodes `A`, `B`, that involves adding edges which `A` has, but `B` doesn't and vice-versa.
For each connection we create a data structure [IbfSet](#ibfset).
It stores `Inverse Bloom Filters` [Ibf](#ibf) of powers of `2^k` for `k` in range `10..17`.
This allows us to recover around `2^17 / 1.3` edges with `99%` probability according to [Efficient Set Reconciliation without Prior Context](https://www.ics.uci.edu/~eppstein/pubs/EppGooUye-SIGCOMM-11.pdf).
Otherwise, we do full routing table exchange.

We have chosen a minimum size of `Ibf` to be `2^10`, because the overhead of processing `IBF` of smaller size of negligible, and to reduce the number of messages required in exchange.
On the other side, we limit `Ibf` to size of `2^17` to reduce memory usage required for each data structure.


# Routing Table

We are extending [Routing Table](NetworkSpec.md#Routing Table) by additional field `peer_ibf_set`

```rust
pub struct RoutingTable {
   /// Other fields
   ///    ..
   /// Data structure used for exchanging routing tables.
   pub peer_ibf_set: IbfPeerSet,
}
```

- `peer_ibf_set` - a helper data structure, which allows doing partial synchronization.

# IbfPeerSet

`IbfPeerSet` contains a mapping between `PeerId` and `IbfSet`.
It's used to store metadata for each peer, which can be used to compute set difference between routing tables of current peer and peer contained in the mapping.

```rust
pub struct SimpleEdge {
  pub key: (PeerId, PeerId),
  pub nonce: u64,
}

pub struct IbfPeerSet {
    peer2seed: HashMap<PeerId, u64>,
    unused_seeds: Vec<u64>,
    seed2ibf: HashMap<u64, IbfSet<SimpleEdge>>,
    slot_map: SlotMap,
    edges: u64,
}
```

- `peer2seed` - contains a mapping from `PeerId` to `seed` used to generate `IbfSet`
- `unused_seeds` - list of currently unused seeds, which will be used for new incoming connections
- `seed2ibf` - contains a mapping from `seed` to `IbfSet`
- `slot_map` - a helper data structure, which is used to store `Edges`, this allows us to save memory by not storing
  duplicates.
- `edges` - total number of edges in the data structure

# Messages

```rust
pub struct RoutingSyncV2 {
    pub version: u64,
    pub known_edges: u64,
    pub ibf_level: u64,
    pub ibf: Vec<IbfElem>,
    pub request_all_edges: bool,
    pub seed: u64,
    pub edges: Vec<Edge>,
    pub requested_edges: Vec<u64>,
    pub done: bool,
}
```

- `version` - version of routing table, currently 0, for future use
- `known_edges` - number of edges peer knows about, this allows us to decide whenever to request all edges
  immediately or not.
- `ibf_level` - level of inverse bloom filter requested from `10` to `17`.
- `request_all_edges` - true if peer decides to request all edges from other peer
- `seed` - seed used to generate ibf
- `edges` - list of edges send to the other peer
- `requested_edges` - list of hashes of edges peer want to get
- `done` - true if it's the last message in synchronization

## IbfSet

Structure used to represent set of `Inverse Bloom Filters` for given peer.

```rust
pub struct IbfSet<T: Hash + Clone> {
    seed: u64,
    ibf: Vec<Ibf>,
    h2e: HashMap<u64, SlotMapId>,
    hasher: DefaultHasher,
    pd: PhantomData<T>,
}
```

- `seed` - seed used to generate IbfSet
- `ibf` - list of `Inverse Bloom Filters` with sizes from `10` to `17`.
- `h2e` - mapping from hash of given edge to id of the edge. This is used to save memory, to avoid storing multiple
  copies of given edge.
- `hasher` - hashing function

## Ibf

Structure representing Reverse Bloom Filter.

```rust
pub struct Ibf {
    k: i32,
    pub data: Vec<IbfElem>,
    hasher: IbfHasher,
    pub seed: u64,
}
```

- `k` - number of elements in vector
- `data` - vector of elements of bloom filter
- `seed` - seed of each inverse bloom filter
- `hasher` - hashing function

## IbfElem

Element of bloom filter

```rust
pub struct IbfElem {
    xor_elem: u64,
    xor_hash: u64,
}
```

- `xor_element` - xor of hashes of edges stored in given box
- `xor_hash` - xor of hashes of hashes of edges stored in give n box

## Routing table exchange

The process of exchanging routing tables involves multiple steps.
It could be reduced if needed by adding an estimator of how many edges differ, but it's not implemented for sake of simplicity.

## Step 1

Peer `A` connects to peer `B`.

## Step 2

Peer `B` chooses unique `seed`, and generates `IbfPeerSet` based on seed and all known edges in
[Routing Table](NetworkSpec.md#Routing Table).

## Step 3 - 10

On odd steps peer `B` sends message to peer `A`, with Ibf of size `2^(step+7)`.
On even steps, peer `A` does it.

- Case 1 - if we were unable to find all edges we continue to the next step.
- Case 2 - otherwise, we know what the set difference is.
We compute the set of hashes of edges given node knows about, and doesn't know about.
Current peer sends to the other peer the edges it knows about, and asks about edges, based on hashes, it doesn't know about the other peer.
The other peer sends the response, and the routing table exchange ends.

## Step 11

In case the synchronization isn't done yet, each side sends list of hashes of edges it knows about.

## Step 12

Each side sends list of edges the other side requested.

# Security considerations


### Avoid extra computation if another peer reconnects to server

In order to avoid extra computation whenever a peer reconnects to server, we can keep a pool of `IbfPeerSet` structures.
The number of such structures which we need to keep is going to be equal to the peak number of incoming connections, which servers have had plus the number of outgoing connections.

### Producing edges, where there is a hash collision

We use unique `IbfPeerSet` structure, for each connection, this prevents seeds to be guessed.
Therefore, we are resistant to that type of attack.


# Memory overhead

For each connection we need to use approximately `(2^10 + ... 2^17) * sizeof(IbfElem) bytes = 2^18 * 16 bytes = 4 MiB`.
Assuming we keep extra `40` of such data structures, we would need extra `160 MiB`.

# Performance overhead

On each update we need up update each `IbfSet` structure `3*8 = 24` times.
Assuming we keep `40` such data structures, that requires up to `960` updates.

# Alternative approaches

### Only use one `IbfPeerSet` structure for everyone

This would reduce number of updates we need to do, and memory usage.
However, it would be possible to guess the hash function used, which would expose us to security vulnerability.
It would be simple to produce two edges, such that there is a hash collision, which would cause routing table exchange to fail.

### Increase number of `IbfSet` structures per `IbfPeerSet`

In theory, we could increase the sizes of `IBF` structures used from `2^10..2^17` to `2^10..2^20`.
This would allow us to recover the set difference if it's up to `2^20/3` instead of `2^17/3` at cost of increasing memory overhead from `160 MiB to 640 Mib`.

### Simplify algorithm to only exchange list of hashes of known edges of each peer

Each edge has a size of about 400 bytes.
Let's assume we need to send 1 million edges.
By sending list of 4 bytes hashes of all known edges on each synchronization we would only need to send `4 MiB` of metadata plus the size of edges that differ.
This approach would be simpler, but not as efficient in terms of bandwidth.
That would still be an improvement of having to send just `4 MiB` over `400 MiB` with existing implementation.

# Future improvements

## Reduce memory usage

### Used a fixed number of `IbfPeerSet` structures for each node Theoretically, smaller number of sets could be used. For example 10, this would require estimating how likely it is to produce edges such that they produce collisions

It may be still possible to generate offline such set of edges that can cause recovery of edges from `IBF` to fail for one node.


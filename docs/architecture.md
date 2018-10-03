# NEAR Core Architecture

## Basics

### Blocks / Transactions

TODO(ek)


### Persistent Merkle tree

TODO(ek)


### State storage

TODO(ek)


## Thresholded Proof-of-Stake

TODO(illia)

## Snapshot chain

TODO(ek)


## TxFlow (consensus)

TODO(nearmax)


## Network

Requirements:
  - Supporting regular p2p networking
  - Relaying messages between nodes that don't have open ports (e.g. mobile devices behind NATs)
  - Handling NATs more generally
  - Sharding the networking, to allow independent communication in shards and cross shard communication.
  - Push for direct messaging whenever possible instead of broadcasts.

Considerations:
  
  - libp2p-rust
  - devp2p-rust

## Smart-contracts & VM

TODO

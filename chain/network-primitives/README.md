# Overview
TODO - already in another PR

# Project structure
- `network_protocol.rs` - contains types, which are part of network protocol, they should be changed with care. 
All of them derive `BorshSerialize` / `BorshDeserialize`
- `types.rs` - TODO
- `config.rs` - TODO
- `actix.rs` - TODO
 
# `near-network-primitives`

`near-network-primitives` is a package that contains network types that are meant to be shared
between multiple crates, without having to import `near-network`. 
Note that, the current split doesn't follow that logic. 

We have a few use cases:
- Messages used by `ClientActor`, `ViewClientActor`, `near-network`.
- external packages, which want to analyze traffic, they don't need to import `near-network`, importing just network types should be enough for them

Dependencies:
- `deepsize` - optional - provides api for counting sizes of structs
- `actix` - required - provides implementation `actix` `Message` - we could make it optional
- `borsh` - required - serialization / deserialization of network messages - could be made optional

# Current state of the package

## `near-network`
`near-network` exports the following to the outside:
- `PeerManagerActor`
- `PeerManagerActor` related `Actix` messages
- Part of `borsh` serialized network messages exchanged between nodes.

Actors below, their related messages, are considered private:
- `PeerActor`
- `RoutingTableActor`
- `EdgeValidatorActor`


## `near-network-primitives`
`near-network-primitives` provides
- `ClientActor` related `Actix` messages
- `ViewClientActor` related `Actix` messages
- Part of `borsh` serialized network messages exchanged between nodes.
- `NetworkConfig` - its logic is split between `utils.rs` and `types.rs`.

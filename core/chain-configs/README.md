# Chain configs crate

This crate provides typed interfaces to the NEAR Genesis and Client Configs, together with the functions to validate their correctness.

## Genesis config
Genesis config is the one that 'defines' the chain. It was set at the beginning and generally is not mutable. 

## Client config

Client config is the part of the config that client can configure on their own - it controls things like: how many peers it should connect to before syncing, which shards to track etc.

## Protocol config
This is the type that is spanning over GenesisConfig and RuntimeConfig. People should not use it directly, but use the ProtocolConfigView class instead.
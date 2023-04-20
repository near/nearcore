This document describes the advanced network options that you can configure
by modifying the "network" section of your "config.json" file:


```
{
  // ...
  "network": {
    // ...
    "public_addrs": [],
    "allow_private_ip_in_public_addrs": false,
    "experimental": {
      "inbound_disabled": false,
      "connect_only_to_boot_nodes": false,
      "skip_sending_tombstones_seconds": 0,
      "tier1_enable_inbound": true,
      "tier1_enable_outbound": false,
      "tier1_connect_interval": {
        "secs": 60,
        "nanos": 0
      },
      "tier1_new_connections_per_attempt": 50
    }
  },
  // ...
}
```

### TIER1 network

Participants of the BFT consensus (block & chunk producers) now can establish
direct (aka TIER1) connections between each other, which will optimize the
communication latency and minimize the number of dropped chunks. If you are a
validator, you can enable TIER1 connections by setting the following fields in the config:

* [public_addrs](https://github.com/near/nearcore/blob/d95a5f58d998c69cb8d4e965ad6b0a440cf3f233/chain/network/src/config_json.rs#L154)
  * this is a list of the public addresses (in the format `"<node public key>@<IP>:<port>"`)
    of trusted nodes, which are willing to route messages to your node
  * this list will be broadcasted to the network so that other validator nodes can connect
    to your node.
  * if your node has a static public IP, set `public_addrs` to a list with a single entry
    with the public key and address of your node, for example:
    `"public_addrs": ["ed25519:86EtEy7epneKyrcJwSWP7zsisTkfDRH5CFVszt4qiQYw@31.192.22.209:24567"]`.
  * if your node doesn't have a public IP (for example, it is hidden behind a NAT), set
    `public_addrs` to a list (<=10 entries) of proxy nodes that you trust (arbitrary nodes
    with static public IPs).
  * support for nodes with dynamic public IPs is not implemented yet.
* [experimental.tier1_enable_outbound](https://github.com/near/nearcore/blob/d95a5f58d998c69cb8d4e965ad6b0a440cf3f233/chain/network/src/config_json.rs#L213)
  * makes your node actively try to establish outbound TIER1 connections (recommended)
    once it learns about the public addresses of other validator nodes. If disabled, your
    node won't try to establish outbound TIER1 connections, but it still may accept
    incoming TIER1 connections from other nodes.
  * currently `false` by default, but will be changed to `true` by default in the future
* [experimental.tier1_enable_inbound](https://github.com/near/nearcore/blob/d95a5f58d998c69cb8d4e965ad6b0a440cf3f233/chain/network/src/config_json.rs#L209)
  * makes your node accept inbound TIER1 connections from other validator nodes.
  * disable both `tier1_enable_inbound` and `tier1_enable_outbound` if you want to opt-out
    from the TIER1 communication entirely
  * disable `tier1_enable_inbound` if you are not a validator AND you don't want your
    node to act as a proxy for validators.
  * `true` by default

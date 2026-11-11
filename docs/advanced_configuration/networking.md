This document describes the advanced network options that you can configure
by modifying the "network" section of your "config.json" file:

```
{
  // ...
  "network": {
    // ...
    "public_addrs": [],
    "allow_private_ip_in_public_addrs": false,
    "tier1": {
      "enable_inbound": true,
      "connect_interval": {
        "secs": 60,
        "nanos": 0
      },
      "new_connections_per_attempt": 50,
      "advertise_proxies_interval": {
        "secs": 900,
        "nanos": 0
      }
    },
    "experimental": {
      "inbound_disabled": false,
      "connect_only_to_boot_nodes": false,
      "skip_sending_tombstones_seconds": 0
    }
  },
  // ...
}
```

### TIER1 network

Validators (block producers, chunk producers and chunk validators) exchange consensus
messages, such as block approvals, chunk parts, state witnesses and chunk endorsements,
over direct connections between each other. These connections form the TIER1 network.
Consensus messages travel only over TIER1, so TIER1 connectivity is required to operate
a validator. A validator node:

* should be reachable at a public address, which it advertises so that the other
  validators connect to it.
* connects to the addresses that the other validators advertise, so it has to be allowed
  to open outbound TCP connections to them.

Two validators that both lack such an address exchange messages through a third
validator, which is slower and less reliable than a direct connection.

TIER1 is configured by the following fields:

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
  * if `public_addrs` is empty, your node discovers its public IP through the STUN servers
    listed in `trusted_stun_servers`, and advertises it with the port of `addr`.
  * your node advertises its own address once it has connected to that address itself,
    so the address has to accept TCP connections from your node as well as from the
    other validators.
* `tier1.enable_inbound`
  * makes your node accept inbound TIER1 connections from other validator nodes.
  * disable it only if your node is not a validator AND you don't want it to act as a
    proxy for validators.
  * `true` by default

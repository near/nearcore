## Protocol Upgrade

This document describes the entire cycle of how a protocol upgrade is done, from
the initial PR to the final release. It is important for everyone who
contributes to the development of the protocol and its client(s) to understand
this process.

### Background

At NEAR, we use the term protocol version to mean the version of the blockchain
protocol and is separate from the version of some specific client (such as nearcore),
since the protocol version defines the protocol rather than some specific
implementation of the protocol. More concretely, for each epoch, there is a
corresponding protocol version that is agreed upon by validators through
[a voting mechanism](https://github.com/near/NEPs/blob/master/specs/ChainSpec/Upgradability.md).
Our upgrade scheme dictates that protocol version X is backward compatible with
protocol version X-1 so that nodes in the network can seamlessly upgrade to
the new protocol. However, there is **no guarantee** that protocol version X is
backward compatible with protocol version X-2.

Despite the upgrade mechanism, rolling out a protocol change can be scary,
especially if the change is invasive. For those changes, we may want to have
several months of testing before we are confident that the change itself works
and that it doesn't break other parts of the system.

### Protocol version voting and upgrade

When a new neard version, containing a new protocol version, is released, all node maintainers need 
to upgrade their binary. That typically means stopping neard, downloading or compiling the new neard
binary and restarting neard. However the protocol version of the whole network is not immediately 
bumped to the new protocol version. Instead a process called voting takes place and determines if and 
when the protocol version upgrade will take place. 

Voting is a fully automated process in which all block producers across the network vote in support 
or against upgrading the protocol version. The voting happens in the last block every epoch. Upgraded
nodes will begin voting in favour of the new protocol version after a predetermined date. The voting 
date is configured by the release owner [like this](https://github.com/near/nearcore/commit/9b0275de057a01f87c259580f93e58f746da75aa). 
Once at least 80% of the stake votes in favour of the protocol change in the last block of epoch X, the 
protocol version will be upgraded in the first block of epoch X+2. 

For mainnet releases, the release on github typically happens on a Monday or Tuesday, the voting 
typically happens a week later and the protocol version upgrade happens 1-2 epochs after the voting. This 
gives the node maintainers enough time to upgrade their neard nodes. The node maintainers can upgrade
their nodes at any time between the release and the voting but it is recommended to upgrade soon after the
release. This is to accomodate for any database migrations or miscellaneous delays. 

Starting a neard node with protocol version voting in the future in a network that is already operating 
at that protocol version is supported as well. This is useful in the scenario where there is a mainnet 
security release where mainnet has not yet voted or upgraded to the new version. That same binary with
protocol voting date in the future can be released in testnet even though it has already upgraded to 
the new protocol version.

### Nightly Protocol features

To make protocol upgrades more robust, we introduce the concept of a nightly
protocol version together with the protocol feature flags to allow easy testing
of the cutting-edge protocol changes without jeopardizing the stability of the
codebase overall. In `Cargo.toml` file of the crates we have in nearcore, we
introduce rust compile-time features `nightly_protocol` and `nightly`

```toml
nightly_protocol = []
nightly = [
    "nightly_protocol",
    ...
]
```

where `nightly_protocol` is a marker feature that indicates that we are on
nightly protocol whereas `nightly` is a collection of new protocol features
which also implies `nightly_protocol`. For example, when we introduce EVM as a
new protocol change, suppose the current protocol version is 40, then we would
do the following change in `Cargo.toml`:

```toml
nightly_protocol = []
nightly = [
    "nightly_protocol",
    "protocol_features_evm",
    ...
]
```

In [core/primitives/src/version.rs](https://github.com/near/nearcore/blob/master/core/primitives/src/version.rs), we would
change the protocol version by:

```rust
#[cfg(feature = “nightly_protocol”)]
pub const PROTOCOL_VERSION: u32 = 100;
#[cfg(not(feature = “nightly_protocol”)]
pub const PROTOCOL_VERSION: u32 = 40;
```

This way the stable versions remain unaffected after the change. Note that
nightly protocol version intentionally starts at a much higher number to make
the distinction between the stable protocol and nightly protocol clearer.

To determine whether a protocol feature is enabled, we do the following:

* We maintain a `ProtocolFeature` enum where each variant corresponds to some
  protocol feature. For nightly protocol features, the variant is gated by the
  corresponding rust compile-time feature.
* We implement a function `protocol_version` to return, for each variant, the
  corresponding protocol version in which the feature is enabled.
* When we need to decide whether to use the new feature based on the protocol
  version of the current network, we can simply compare it to the protocol
  version of the feature. To make this simpler, we also introduced a macro
  `checked_feature`

For more details, please refer to
[core/primitives/src/version.rs](https://github.com/near/nearcore/blob/master/core/primitives/src/version.rs).

### Feature Gating

It is worth mentioning that there are two types of checks related to protocol features:

* For stable features, we check whether they should be enabled by checking the
  protocol version of the current epoch. This does not involve any rust
  compile-time features.
* For nightly features, we have both the check of protocol version and the rust
  compile-time feature gating.

### Testing

Nightly protocol features allow us to enable the most bleeding-edge code in some
testing environments. We can choose to enable all nightly protocol features by

```rust
cargo build -p neard --release --features nightly
```

or enable some specific protocol feature by

```rust
cargo build -p neard --release --features nightly_protocol,<protocol_feature>
```

In practice, we have all nightly protocol features enabled for Nayduck tests and
on betanet, which is updated daily.

### Feature Stabilization

New protocol features are introduced first as nightly features and when the
author of the feature thinks that the feature is ready to be stabilized, they
should submit a pull request to stabilize the feature using
[this template](../../.github/PULL_REQUEST_TEMPLATE/feature_stabilization.md).
In this pull request, they should do the feature gating, increase the
`PROTOCOL_VERSION` constant (if it hasn't been increased since the last
release), and change the `protocol_version` implementation to map the
stabilized features to the new protocol version.

A feature stabilization request must be approved by at least **two**
[nearcore code owners](https://github.com/orgs/near/teams/nearcore-codeowners).
Unless it is a security-related fix, a protocol feature cannot be included in
any release until at least **one** week after its stabilization. This is to ensure
that feature implementation and stabilization are not rushed.


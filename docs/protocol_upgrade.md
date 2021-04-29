## Protocol Upgrade

This document describes the entire cycle of how a protocol upgrade is done, from the initial PR to the final release.
It is important for everyone who contributes to the development of the protocol and its client(s) to understand this process.

### Background

At NEAR, we use protocol version to mean the version of the blockchain protocol and is separate from the version of some specific client (such as nearcore),
since the protocol version defines the protocol rather than some specific implementation of the protocol.
Our upgrade scheme dictates that protocol version X is backward compatible with protocol version X-1,
so that nodes in the network can seamlessly upgrade into the new protocol. The upgrade is done through [a voting mechanism](https://github.com/near/NEPs/blob/master/specs/ChainSpec/Upgradability.md)
on the protocol level.

However, rolling out a protocol change can be scary, especially if the change is invasive.
For those changes, we may want to have several months of testing before we are confident that the change itself works
and that it doesn't break other parts of the system.

### Nightly Protocol features

To make protocol upgrades more robust, we introduce the concept of nightly protocol version together with the protocol feature flags to allow easy testing of the cutting-edge protocol changes without jeopardizing the stability of the codebase overall.
In `Cargo.toml` file of the crates we have in nearcore, we introduce rust compile-time features `nightly_protocol` and `nightly_protocol_features`
```
nightly_protocol = []
nightly_protocol_features = [...]
```
where `nightly_protocol` is a marker feature that indicates that we are on nightly protocol
whereas `nightly_protocol_features` is a collection of new protocol features.
For example, when we introduce EVM as a new protocol change, suppose the current protocol version is 40, then we would do the following change in Cargo.toml:
```
nightly_protocol = []
nightly_protocol_features = [“protocol_features_evm”, ...]
```
In `core/primitives/src/versions.rs`, we would change the protocol version by:
```
#[cfg(feature = “nightly_protocol”)]
pub const PROTOCOL_VERSION: u32 = 100;
#[cfg(not(feature = “nightly_protocol”)]
pub const PROTOCOL_VERSION: u32 = 40;
```
This way the stable versions remain unaffected after the change. Note that nightly protocol version intentionally starts
at a much higher number to make the distinction between stable protocol and nightly protocol more clear.

To determine whether a protocol feature is enabled, we do the following:
- We maintain a `ProtocolFeature` enum where each variant corresponds to some protocol feature. For nightly protocol features,
the variant is gated by the corresponding rust compile-time feature. For example, the evm feature is introduced as follows
```rust
pub enum ProtocolFeatures {
  #[feature("protocol_feature_evm")],
  EVM
}
```
- We implement a function `protocol_version` to return, for each variant, the corresponding protocol version in which the
feature is enabled. For example,
```rust
impl ProtocolFeature {
    pub const fn protocol_version(self) -> ProtocolVersion {
        match self {
            // Stable features
            // ...

            // Nightly features
            #[cfg(feature = "protocol_feature_evm")]
            ProtocolFeature::EVM => 100,
        }
    }
}  
```
- When we need to decide whether to use the new feature based on the protocol version of the current network,
  we can simply compare it to the protocol version of the feature. For example,
```rust
#[cfg(feature = “protocol_version_evm”)]
if current_protocol_version >= ProtocolFeature::EVM.protocol_version() {
   do_evm_change();
} else {
   do_old_stuff();
}
#[cfg(not(feature = “protocol_version_evm”))] {
   do_old_stuff();
}
```
To make this simpler, we write a macro `checked_feature` that conveniently abstracts this logic:
```rust
#[macro_export]
macro_rules! checked_feature {
    ($feature_name:tt, $feature:ident, $current_protocol_version:expr) => {{
        #[cfg(feature = $feature_name)]
        let is_feature_enabled = $crate::version::ProtocolFeature::$feature.protocol_version()
            <= $current_protocol_version;
        #[cfg(not(feature = $feature_name))]
        let is_feature_enabled = {
            // Workaround unused variable warning
            let _ = $current_protocol_version;

            false
        };
        is_feature_enabled
    }};

    ($feature_name:tt, $feature:ident, $current_protocol_version:expr, $feature_block:block) => {{
        checked_feature!($feature_name, $feature, $current_protocol_version, $feature_block, {})
    }};

    ($feature_name:tt, $feature:ident, $current_protocol_version:expr, $feature_block:block, $non_feature_block:block) => {{
        #[cfg(feature = $feature_name)]
        {
            if checked_feature!($feature_name, $feature, $current_protocol_version) {
                $feature_block
            } else {
                $non_feature_block
            }
        }
        // Workaround unused variable warning
        #[cfg(not(feature = $feature_name))]
        {
            let _ = $current_protocol_version;
            $non_feature_block
        }
    }};
}
```
With this macro, the previous example can be simplified into
```rust
checked_feature!(“protocol_feature_evm”, EVM, {do_evm_stuff();}, {do_old_stuff();});
```

### Testing

Nightly protocol features allow us to enable the most bleeding-edge code in some testing environment. We can choose to
enable all nightly protocol features by
```
cargo build -p neard --release --features nightly_protocol_features
```
or enable some specific protocol feature by
```
cargo build -p neard --release --features nightly_protocol,<protocol_feature>
```

In practice, we have all nightly protocol features enabled for Nayduck tests and on betanet, which is updated daily.

### Feature Stabilization

When we are ready to make a new release, we examine which features should be stabilized and for those that should be, we
remove the feature gating everywhere. One difference is that we now use this rule of the `checked_feature` macro:
```rust
("stable", $feature:ident, $current_protocol_version:expr) => {{
        $crate::version::ProtocolFeature::$feature.protocol_version() <= $current_protocol_version
}};
```
where we need to check whether a feature should be enabled. In addition, we increment the `PROTOCOL_VERSION` constant and
change the `protocol_version` implementation to map the stabilized features to the new protocol version.

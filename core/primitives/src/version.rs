use serde::{Deserialize, Serialize};

use crate::types::Balance;

/// Data structure for semver version and github tag or commit.
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Version {
    pub version: String,
    pub build: String,
}

/// Database version.
pub type DbVersion = u32;

/// Current version of the database.
pub const DB_VERSION: DbVersion = 31;

/// Protocol version type.
pub use near_primitives_core::types::ProtocolVersion;

/// Minimum gas price proposed in NEP 92 and the associated protocol version
pub const MIN_GAS_PRICE_NEP_92: Balance = 1_000_000_000;
pub const MIN_PROTOCOL_VERSION_NEP_92: ProtocolVersion = 31;

/// Minimum gas price proposed in NEP 92 (fixed) and the associated protocol version
pub const MIN_GAS_PRICE_NEP_92_FIX: Balance = 100_000_000;
pub const MIN_PROTOCOL_VERSION_NEP_92_FIX: ProtocolVersion = 32;

pub const CORRECT_RANDOM_VALUE_PROTOCOL_VERSION: ProtocolVersion = 33;

/// See [NEP 71](https://github.com/nearprotocol/NEPs/pull/71)
pub const IMPLICIT_ACCOUNT_CREATION_PROTOCOL_VERSION: ProtocolVersion = 35;

/// The protocol version that enables reward on mainnet.
pub const ENABLE_INFLATION_PROTOCOL_VERSION: ProtocolVersion = 36;

/// Fix upgrade to use the latest voted protocol version instead of the current epoch protocol
/// version when there is no new change in protocol version.
pub const UPGRADABILITY_FIX_PROTOCOL_VERSION: ProtocolVersion = 37;

/// Updates the way receipt ID, data ID and random seeds are constructed.
pub const CREATE_HASH_PROTOCOL_VERSION: ProtocolVersion = 38;

/// Fix the storage usage of the delete key action.
pub const DELETE_KEY_STORAGE_USAGE_PROTOCOL_VERSION: ProtocolVersion = 40;

pub const SHARD_CHUNK_HEADER_UPGRADE_VERSION: ProtocolVersion = 41;

/// Updates the way receipt ID is constructed to use current block hash instead of last block hash
pub const CREATE_RECEIPT_ID_SWITCH_TO_CURRENT_BLOCK_VERSION: ProtocolVersion = 42;

pub struct ProtocolVersionRange {
    lower: ProtocolVersion,
    upper: Option<ProtocolVersion>,
}

impl ProtocolVersionRange {
    pub fn new(lower: ProtocolVersion, upper: Option<ProtocolVersion>) -> Self {
        Self { lower, upper }
    }

    pub fn contains(&self, version: ProtocolVersion) -> bool {
        self.lower <= version && self.upper.map_or(true, |upper| version < upper)
    }
}

pub fn is_implicit_account_creation_enabled(protocol_version: ProtocolVersion) -> bool {
    protocol_version >= IMPLICIT_ACCOUNT_CREATION_PROTOCOL_VERSION
}

/// New Protocol features should go here. Features are guarded by their corresponding feature flag.
/// For example, if we have `ProtocolFeature::EVM` and a corresponding feature flag `evm`, it will look
/// like
///
/// #[cfg(feature = "protocol_feature_evm")]
/// EVM code
///
#[derive(Hash, PartialEq, Eq, Clone, Copy, Debug)]
pub enum ProtocolFeature {
    // stable features
    ForwardChunkParts,
    RectifyInflation,
    /// Add `AccessKey` nonce range by setting nonce to `(block_height - 1) * 1e6`, see
    /// <https://github.com/near/nearcore/issues/3779>.
    AccessKeyNonceRange,
    FixApplyChunks,
    LowerStorageCost,
    DeleteActionRestriction,
    /// Add versions to `Account` data structure
    AccountVersions,
    TransactionSizeLimit,
    /// Fix a bug in `storage_usage` for account caused by #3824
    FixStorageUsage,
    /// Cap maximum gas price to 2,000,000,000 yoctoNEAR
    CapMaxGasPrice,
    CountRefundReceiptsInGasLimit,
    /// Add `ripemd60` and `ecrecover` host function
    MathExtension,
    /// Restore receipts that were previously stuck because of
    /// <https://github.com/near/nearcore/pull/4228>.
    RestoreReceiptsAfterFixApplyChunks,
    /// This feature switch our WASM engine implementation from wasmer 0.* to
    /// wasmer 2.*, bringing better performance and reliability.
    ///
    /// The implementations should be sufficiently similar for this to not be a
    /// protocol upgrade, but we conservatively do a protocol upgrade to be on
    /// the safe side.
    ///
    /// Although wasmer2 is faster, we don't change fees with this protocol
    /// version -- we can safely do that in a separate step.
    Wasmer2,
    SimpleNightshade,
    LowerDataReceiptAndEcrecoverBaseCost,
    /// Lowers the cost of wasm instruction due to switch to wasmer2.
    LowerRegularOpCost,
    /// Lowers the cost of wasm instruction due to switch to faster,
    /// compiler-intrinsics based gas counter.
    LowerRegularOpCost2,
    /// Limit number of wasm functions in one contract. See
    /// <https://github.com/near/nearcore/pull/4954> for more details.
    LimitContractFunctionsNumber,
    BlockHeaderV3,
    /// Changes how we select validators for epoch and how we select validators within epoch. See
    /// https://github.com/near/NEPs/pull/167 for general description, note that we would not
    /// introduce chunk-only validators with this feature
    AliasValidatorSelectionAlgorithm,
    /// Make block producers produce chunks for the same block they would later produce to avoid
    /// network delays
    SynchronizeBlockChunkProduction,
    /// Change the algorithm to count WASM stack usage to avoid undercounting in
    /// some cases.
    CorrectStackLimit,
    /// Add `AccessKey` nonce range for implicit accounts, as in `AccessKeyNonceRange` feature.
    AccessKeyNonceForImplicitAccounts,

    // nightly features
    #[cfg(feature = "protocol_feature_alt_bn128")]
    AltBn128,
    #[cfg(feature = "protocol_feature_chunk_only_producers")]
    ChunkOnlyProducers,
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    RoutingExchangeAlgorithm,
}

/// Both, outgoing and incoming tcp connections to peers, will be rejected if `peer's`
/// protocol version is lower than this.
pub const PEER_MIN_ALLOWED_PROTOCOL_VERSION: ProtocolVersion = MAIN_NET_PROTOCOL_VERSION - 2;

/// Current protocol version used on the main net.
/// Some features (e. g. FixStorageUsage) require that there is at least one epoch with exactly
/// the corresponding version
const MAIN_NET_PROTOCOL_VERSION: ProtocolVersion = 51;

/// Version used by this binary.
#[cfg(not(feature = "nightly_protocol"))]
pub const PROTOCOL_VERSION: ProtocolVersion = MAIN_NET_PROTOCOL_VERSION;
/// Current latest nightly version of the protocol.
#[cfg(feature = "nightly_protocol")]
pub const PROTOCOL_VERSION: ProtocolVersion = 125;

impl ProtocolFeature {
    pub const fn protocol_version(self) -> ProtocolVersion {
        match self {
            // Stable features
            ProtocolFeature::LowerStorageCost => 42,
            ProtocolFeature::DeleteActionRestriction => 43,
            ProtocolFeature::FixApplyChunks => 44,
            ProtocolFeature::ForwardChunkParts
            | ProtocolFeature::RectifyInflation
            | ProtocolFeature::AccessKeyNonceRange => 45,
            ProtocolFeature::AccountVersions
            | ProtocolFeature::TransactionSizeLimit
            | ProtocolFeature::FixStorageUsage
            | ProtocolFeature::CapMaxGasPrice
            | ProtocolFeature::CountRefundReceiptsInGasLimit
            | ProtocolFeature::MathExtension => 46,
            ProtocolFeature::RestoreReceiptsAfterFixApplyChunks => 47,
            ProtocolFeature::Wasmer2
            | ProtocolFeature::LowerDataReceiptAndEcrecoverBaseCost
            | ProtocolFeature::LowerRegularOpCost
            | ProtocolFeature::SimpleNightshade => 48,
            ProtocolFeature::LowerRegularOpCost2
            | ProtocolFeature::LimitContractFunctionsNumber
            | ProtocolFeature::BlockHeaderV3
            | ProtocolFeature::AliasValidatorSelectionAlgorithm => 49,
            ProtocolFeature::SynchronizeBlockChunkProduction
            | ProtocolFeature::CorrectStackLimit => 50,
            ProtocolFeature::AccessKeyNonceForImplicitAccounts => 51,

            // Nightly features
            #[cfg(feature = "protocol_feature_alt_bn128")]
            ProtocolFeature::AltBn128 => 105,
            #[cfg(feature = "protocol_feature_chunk_only_producers")]
            ProtocolFeature::ChunkOnlyProducers => 124,
            #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
            ProtocolFeature::RoutingExchangeAlgorithm => 117,
        }
    }
}

#[macro_export]
macro_rules! checked_feature {
    ("stable", $feature:ident, $current_protocol_version:expr) => {{
        $crate::version::ProtocolFeature::$feature.protocol_version() <= $current_protocol_version
    }};
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

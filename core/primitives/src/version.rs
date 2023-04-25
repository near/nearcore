use crate::types::Balance;
use once_cell::sync::Lazy;

/// Data structure for semver version and github tag or commit.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct Version {
    pub version: String,
    pub build: String,
    #[serde(default)]
    pub rustc_version: String,
}

use crate::upgrade_schedule::{get_protocol_version_internal, ProtocolUpgradeVotingSchedule};
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
    /// This feature switch our WASM engine implementation from wasmer 2.* to
    /// near-vm, bringing better performance and reliability.
    ///
    /// Although near-vm is faster, we don't change fees with this protocol
    /// version -- we can safely do that in a separate step.
    NearVm,
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
    /// Changes how we select validators for epoch and how we select validators
    /// within epoch.  See <https://github.com/near/NEPs/pull/167> for general
    /// description, note that we would not introduce chunk-only validators with
    /// this feature
    AliasValidatorSelectionAlgorithm,
    /// Make block producers produce chunks for the same block they would later produce to avoid
    /// network delays
    SynchronizeBlockChunkProduction,
    /// Change the algorithm to count WASM stack usage to avoid undercounting in
    /// some cases.
    CorrectStackLimit,
    /// Add `AccessKey` nonce range for implicit accounts, as in `AccessKeyNonceRange` feature.
    AccessKeyNonceForImplicitAccounts,
    /// Increase cost per deployed code byte to cover for the compilation steps
    /// that a deployment triggers. Only affects the action execution cost.
    IncreaseDeploymentCost,
    FunctionCallWeight,
    /// This feature enforces a global limit on the function local declarations in a WebAssembly
    /// contract. See <...> for more information.
    LimitContractLocals,
    /// Ensure caching all nodes in the chunk for which touching trie node cost was charged. Charge for each such node
    /// only once per chunk at the first access time.
    ChunkNodesCache,
    /// Lower `max_length_storage_key` limit, which itself limits trie node sizes.
    LowerStorageKeyLimit,
    // alt_bn128_g1_multiexp, alt_bn128_g1_sum, alt_bn128_pairing_check host functions
    AltBn128,
    ChunkOnlyProducers,
    /// Ensure the total stake of validators that are kicked out does not exceed a percentage of total stakes
    MaxKickoutStake,
    /// Validate account id for function call access keys.
    AccountIdInFunctionCallPermission,
    /// Zero Balance Account NEP 448: https://github.com/near/NEPs/pull/448
    ZeroBalanceAccount,
    /// Execute a set of actions on behalf of another account.
    ///
    /// Meta Transaction NEP-366: https://github.com/near/NEPs/blob/master/neps/nep-0366.md
    DelegateAction,
    Ed25519Verify,
    /// Decouple compute and gas costs of operations to safely limit the compute time it takes to
    /// process the chunk.
    ///
    /// Compute Costs NEP-455: https://github.com/near/NEPs/blob/master/neps/nep-0455.md
    ComputeCosts,
    /// Enable flat storage for reads, reducing number of DB accesses from `2 * key.len()` in
    /// the worst case to 2.
    ///
    /// Flat Storage NEP-399: https://github.com/near/NEPs/blob/master/neps/nep-0399.md
    FlatStorageReads,

    /// In case not all validator seats are occupied our algorithm provide incorrect minimal seat
    /// price - it reports as alpha * sum_stake instead of alpha * sum_stake / (1 - alpha), where
    /// alpha is min stake ratio
    #[cfg(feature = "protocol_feature_fix_staking_threshold")]
    FixStakingThreshold,
    /// Charge for contract loading before it happens.
    #[cfg(feature = "protocol_feature_fix_contract_loading_cost")]
    FixContractLoadingCost,
    #[cfg(feature = "protocol_feature_reject_blocks_with_outdated_protocol_version")]
    RejectBlocksWithOutdatedProtocolVersions,
}

/// Both, outgoing and incoming tcp connections to peers, will be rejected if `peer's`
/// protocol version is lower than this.
pub const PEER_MIN_ALLOWED_PROTOCOL_VERSION: ProtocolVersion = STABLE_PROTOCOL_VERSION - 2;

/// Current protocol version used on the mainnet.
/// Some features (e. g. FixStorageUsage) require that there is at least one epoch with exactly
/// the corresponding version
const STABLE_PROTOCOL_VERSION: ProtocolVersion = 61;

/// Largest protocol version supported by the current binary.
pub const PROTOCOL_VERSION: ProtocolVersion = if cfg!(feature = "nightly_protocol") {
    // On nightly, pick big enough version to support all features.
    136
} else {
    // Enable all stable features.
    STABLE_PROTOCOL_VERSION
};

/// The points in time after which the voting for the latest protocol version
/// should start.
///
/// In non-release builds this is typically a date in the far past (meaning that
/// nightly builds will vote for new protocols immediately).  On release builds
/// it’s set according to the schedule for that protocol upgrade.  Release
/// candidates usually have separate schedule to final releases.
pub const PROTOCOL_UPGRADE_SCHEDULE: Lazy<ProtocolUpgradeVotingSchedule> = Lazy::new(|| {
    // Update to according to schedule when making a release.
    // Keep in mind that the protocol upgrade will happen 1-2 epochs (15h-30h)
    // after the set date. Ideally that should be during working hours.
    // e.g. ProtocolUpgradeVotingSchedule::from_env_or_str("2000-01-01 15:00:00").unwrap());

    ProtocolUpgradeVotingSchedule::default()
});

/// Gives new clients an option to upgrade without announcing that they support
/// the new version.  This gives non-validator nodes time to upgrade.  See
/// <https://github.com/near/NEPs/issues/205>
pub fn get_protocol_version(next_epoch_protocol_version: ProtocolVersion) -> ProtocolVersion {
    get_protocol_version_internal(
        next_epoch_protocol_version,
        PROTOCOL_VERSION,
        *PROTOCOL_UPGRADE_SCHEDULE,
    )
}

impl ProtocolFeature {
    pub const fn protocol_version(self) -> ProtocolVersion {
        match self {
            // Stable features
            ProtocolFeature::LowerStorageCost => 42,
            ProtocolFeature::DeleteActionRestriction => 43,
            ProtocolFeature::FixApplyChunks => 44,
            ProtocolFeature::RectifyInflation | ProtocolFeature::AccessKeyNonceRange => 45,
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
            ProtocolFeature::IncreaseDeploymentCost
            | ProtocolFeature::FunctionCallWeight
            | ProtocolFeature::LimitContractLocals
            | ProtocolFeature::ChunkNodesCache
            | ProtocolFeature::LowerStorageKeyLimit => 53,
            ProtocolFeature::AltBn128 => 55,
            ProtocolFeature::ChunkOnlyProducers | ProtocolFeature::MaxKickoutStake => 56,
            ProtocolFeature::AccountIdInFunctionCallPermission => 57,
            ProtocolFeature::Ed25519Verify
            | ProtocolFeature::ZeroBalanceAccount
            | ProtocolFeature::DelegateAction => 59,
            ProtocolFeature::ComputeCosts
            | ProtocolFeature::NearVm
            | ProtocolFeature::FlatStorageReads => 61,

            // Nightly features
            #[cfg(feature = "protocol_feature_fix_staking_threshold")]
            ProtocolFeature::FixStakingThreshold => 126,
            #[cfg(feature = "protocol_feature_fix_contract_loading_cost")]
            ProtocolFeature::FixContractLoadingCost => 129,
            #[cfg(feature = "protocol_feature_reject_blocks_with_outdated_protocol_version")]
            ProtocolFeature::RejectBlocksWithOutdatedProtocolVersions => 132,
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

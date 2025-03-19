use crate::types::ProtocolVersion;

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
    _DeprecatedImplicitAccountCreation,
    RectifyInflation,
    /// Add `AccessKey` nonce range by setting nonce to `(block_height - 1) * 1e6`, see
    /// <https://github.com/near/nearcore/issues/3779>.
    AccessKeyNonceRange,
    /// Don't process any receipts for shard when chunk is not present.
    /// Always use gas price computed in the previous block.
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
    /// Minimum protocol version for NEP-92
    _DeprecatedMinProtocolVersionNep92,
    /// Minimum protocol version for NEP-92 fix
    _DeprecatedMinProtocolVersionNep92Fix,
    /// Creates a unique random seed to be provided to `VMContext` from a given `action_hash` and a given `random_seed`
    _DeprecatedCorrectRandomValue,
    /// The protocol version that enables reward on mainnet
    _DeprecatedEnableInflation,
    /// Fix upgrade to use the latest voted protocol version instead of the current epoch protocol
    /// version when there is no new change in protocol version
    UpgradabilityFix,
    /// Updates the way receipt ID, data ID and random seeds are constructed
    CreateHash,
    /// Fix the storage usage of the delete key action
    DeleteKeyStorageUsage,
    /// Upgrade for shard chunk header
    ShardChunkHeaderUpgrade,
    /// Updates the way receipt ID is constructed to use current block hash instead of last block hash
    CreateReceiptIdSwitchToCurrentBlock,
    /// Pessimistic gas price estimation uses a fixed value of `minimum_new_receipt_gas` to stop being
    /// tied to the function call base cost
    FixedMinimumNewReceiptGas,
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
    /// Changes how we select validators for epoch and how we select validators
    /// within epoch.  See <https://github.com/near/NEPs/pull/167> for general
    /// description, note that we would not introduce chunk-only validators with
    /// this feature
    AliasValidatorSelectionAlgorithm,
    /// Make block producers produce chunks for the same block they would later produce to avoid
    /// network delays
    SynchronizeBlockChunkProduction,
    /// Change the algorithm to count WASM stack usage to avoid under counting in
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
    /// Zero Balance Account NEP 448: <https://github.com/near/NEPs/pull/448>
    ZeroBalanceAccount,
    /// Execute a set of actions on behalf of another account.
    ///
    /// Meta Transaction NEP-366: <https://github.com/near/NEPs/blob/master/neps/nep-0366.md>
    DelegateAction,
    Ed25519Verify,
    /// Decouple compute and gas costs of operations to safely limit the compute time it takes to
    /// process the chunk.
    ///
    /// Compute Costs NEP-455: <https://github.com/near/NEPs/blob/master/neps/nep-0455.md>
    ComputeCosts,
    /// Decrease the cost of function call action. Only affects the execution cost.
    DecreaseFunctionCallBaseCost,
    /// Enable flat storage for reads, reducing number of DB accesses from `2 * key.len()` in
    /// the worst case to 2.
    ///
    /// Flat Storage NEP-399: <https://github.com/near/NEPs/blob/master/neps/nep-0399.md>
    FlatStorageReads,
    /// Enables preparation V2. Note that this setting is not supported in production settings
    /// without NearVmRuntime enabled alongside it, as the VM runner would be too slow.
    PreparationV2,
    /// Enables Near-Vm. Note that this setting is not at all supported without PreparationV2,
    /// as it hardcodes preparation v2 code into the generated assembly.
    NearVmRuntime,
    BlockHeaderV4,
    /// Resharding V2. A new implementation for resharding and a new shard
    /// layout for the production networks.
    SimpleNightshadeV2,
    /// Built on top of Resharding V2. Changes shard layout to V3 to split shard 2 into two parts.
    SimpleNightshadeV3,
    /// In case not all validator seats are occupied our algorithm provide incorrect minimal seat
    /// price - it reports as alpha * sum_stake instead of alpha * sum_stake / (1 - alpha), where
    /// alpha is min stake ratio
    FixStakingThreshold,
    /// In case not all validator seats are occupied, the minimum seat price of a chunk producer
    /// used to depend on the number of existing shards, which is no longer the case.
    FixChunkProducerStakingThreshold,
    /// Charge for contract loading before it happens.
    FixContractLoadingCost,
    /// Enables rejection of blocks with outdated protocol versions.
    RejectBlocksWithOutdatedProtocolVersions,
    // NEP: https://github.com/near/NEPs/pull/488
    BLS12381,
    RestrictTla,
    /// Increases the number of chunk producers.
    TestnetFewerBlockProducers,
    /// Enables stateless validation which is introduced in <https://github.com/near/NEPs/pull/509>
    /// LowerValidatorKickoutPercentForDebugging: lower block and chunk validator kickout percent from 90 to 50.
    /// SingleShardTracking: single shard tracking for stateless validation.
    /// StateWitnessSizeLimit: state witness size limits.
    /// PerReceiptHardStorageProofLimit: limit the size of storage proof generated by a single receipt.
    /// WitnessTransactionLimits: size limits for transactions included in a ChunkStateWitness.
    /// NoChunkOnlyProducers: no chunk-only producers in stateless validation.
    StatelessValidation,
    EthImplicitAccounts,
    /// Enables yield execution which is introduced in <https://github.com/near/NEPs/pull/519>
    YieldExecution,
    /// Bring minimum required validator stake effectively to ~10K NEAR as of 2024-08-15.
    /// Fixes increase to 100K NEAR in the previous protocol version.
    /// See #11953 for more details.
    FixMinStakeRatio,
    /// Increases main_storage_proof_size_soft_limit parameter from 3mb to 4mb
    IncreaseStorageProofSizeSoftLimit,

    // Shuffle shard assignments for chunk producers at every epoch.
    ShuffleShardAssignments,
    /// Cross-shard congestion control according to <https://github.com/near/NEPs/pull/539>.
    CongestionControl,
    /// Remove account with long storage key.
    RemoveAccountWithLongStorageKey,
    /// Change the structure of ChunkEndorsement to have (shard_id, epoch_id, height_created)
    /// instead of chunk_hash
    ChunkEndorsementV2,
    // Include a bitmap of endorsements from chunk validator in the block header
    // in order to calculate the rewards and kickouts for the chunk validators.
    // This feature introduces BlockHeaderV5.
    ChunkEndorsementsInBlockHeader,
    /// Store receipts in State in the StateStoredReceipt format.
    StateStoredReceipt,
    /// Resharding V3 - Adding "game.hot.tg-0" boundary.
    SimpleNightshadeV4,
    /// Resharding V3 - Adding "earn.kaiching" boundary.
    SimpleNightshadeV5,
    /// Exclude contract code from the chunk state witness and distribute it to chunk validators separately.
    ExcludeContractCodeFromStateWitness,
    /// A scheduler which limits bandwidth for sending receipts between shards.
    BandwidthScheduler,
    /// Indicates that the "sync_hash" used to identify the point in the chain to sync state to
    /// should no longer be the first block of the epoch, but a couple blocks after that in order
    /// to sync the current epoch's state. This is not strictly a protocol feature, but is included
    /// here to coordinate among nodes
    CurrentEpochStateSync,
    /// Relaxed validation of transactions included in a chunk.
    ///
    /// Chunks no longer become entirely invalid in case invalid transactions are included in the
    /// chunk. Instead the transactions are discarded during their conversion to receipts.
    RelaxedChunkValidation,
    /// This enables us to remove the expensive check_balance call from the runtime.
    RemoveCheckBalance,
    /// Exclude existing contract code in deploy-contract and delete-account actions from the chunk state witness.
    /// Instead of sending code in the witness, the code checks the code-size using the internal trie nodes.
    ExcludeExistingCodeFromWitnessForCodeLen,
    /// Use the block height instead of the block hash to calculate the receipt ID.
    BlockHeightForReceiptId,
    /// Enable optimistic block production.
    ProduceOptimisticBlock,
    GlobalContracts,
}

impl ProtocolFeature {
    pub const fn protocol_version(self) -> ProtocolVersion {
        match self {
            // Stable features
            ProtocolFeature::_DeprecatedMinProtocolVersionNep92 => 31,
            ProtocolFeature::_DeprecatedMinProtocolVersionNep92Fix => 32,
            ProtocolFeature::_DeprecatedCorrectRandomValue => 33,
            ProtocolFeature::_DeprecatedImplicitAccountCreation => 35,
            ProtocolFeature::_DeprecatedEnableInflation => 36,
            ProtocolFeature::UpgradabilityFix => 37,
            ProtocolFeature::CreateHash => 38,
            ProtocolFeature::DeleteKeyStorageUsage => 40,
            ProtocolFeature::ShardChunkHeaderUpgrade => 41,
            ProtocolFeature::CreateReceiptIdSwitchToCurrentBlock => 42,
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
            ProtocolFeature::ComputeCosts | ProtocolFeature::FlatStorageReads => 61,
            ProtocolFeature::PreparationV2 | ProtocolFeature::NearVmRuntime => 62,
            ProtocolFeature::BlockHeaderV4 => 63,
            ProtocolFeature::RestrictTla
            | ProtocolFeature::TestnetFewerBlockProducers
            | ProtocolFeature::SimpleNightshadeV2 => 64,
            ProtocolFeature::SimpleNightshadeV3 => 65,
            ProtocolFeature::DecreaseFunctionCallBaseCost
            | ProtocolFeature::FixedMinimumNewReceiptGas => 66,
            ProtocolFeature::YieldExecution => 67,
            ProtocolFeature::CongestionControl
            | ProtocolFeature::RemoveAccountWithLongStorageKey => 68,
            ProtocolFeature::StatelessValidation => 69,
            ProtocolFeature::BLS12381 | ProtocolFeature::EthImplicitAccounts => 70,
            ProtocolFeature::FixMinStakeRatio => 71,
            ProtocolFeature::IncreaseStorageProofSizeSoftLimit
            | ProtocolFeature::ChunkEndorsementV2
            | ProtocolFeature::ChunkEndorsementsInBlockHeader
            | ProtocolFeature::StateStoredReceipt => 72,
            ProtocolFeature::ExcludeContractCodeFromStateWitness => 73,
            ProtocolFeature::FixStakingThreshold
            | ProtocolFeature::RejectBlocksWithOutdatedProtocolVersions
            | ProtocolFeature::FixChunkProducerStakingThreshold
            | ProtocolFeature::RelaxedChunkValidation
            | ProtocolFeature::RemoveCheckBalance
            // BandwidthScheduler and CurrentEpochStateSync must be enabled
            // before ReshardingV3! When releasing this feature please make sure
            // to schedule separate protocol upgrades for these features.
            | ProtocolFeature::BandwidthScheduler
            | ProtocolFeature::CurrentEpochStateSync => 74,
            ProtocolFeature::SimpleNightshadeV4 => 75,
            ProtocolFeature::SimpleNightshadeV5 => 76,
            ProtocolFeature::GlobalContracts
            | ProtocolFeature::BlockHeightForReceiptId
            | ProtocolFeature::ProduceOptimisticBlock => 77,

            // Nightly features:
            ProtocolFeature::FixContractLoadingCost => 129,
            // TODO(#11201): When stabilizing this feature in mainnet, also remove the temporary code
            // that always enables this for mocknet (see config_mocknet function).
            ProtocolFeature::ShuffleShardAssignments => 143,
            ProtocolFeature::ExcludeExistingCodeFromWitnessForCodeLen => 148,
            // Place features that are not yet in Nightly below this line.
        }
    }

    pub fn enabled(&self, protocol_version: ProtocolVersion) -> bool {
        protocol_version >= self.protocol_version()
    }
}

/// The protocol version of the genesis block on mainnet and testnet.
pub const PROD_GENESIS_PROTOCOL_VERSION: ProtocolVersion = 29;

/// Minimum supported protocol version for the current binary
pub const MIN_SUPPORTED_PROTOCOL_VERSION: ProtocolVersion = 37;

/// Current protocol version used on the mainnet with all stable features.
const STABLE_PROTOCOL_VERSION: ProtocolVersion = 77;

// On nightly, pick big enough version to support all features.
const NIGHTLY_PROTOCOL_VERSION: ProtocolVersion = 149;

/// Largest protocol version supported by the current binary.
pub const PROTOCOL_VERSION: ProtocolVersion =
    if cfg!(feature = "nightly") { NIGHTLY_PROTOCOL_VERSION } else { STABLE_PROTOCOL_VERSION };

/// Both, outgoing and incoming tcp connections to peers, will be rejected if `peer's`
/// protocol version is lower than this.
/// TODO(pugachag): revert back to `- 3` after mainnet is upgraded
pub const PEER_MIN_ALLOWED_PROTOCOL_VERSION: ProtocolVersion = STABLE_PROTOCOL_VERSION - 4;

#[cfg(test)]
mod tests {
    use super::ProtocolFeature;

    #[test]
    fn test_resharding_dependencies() {
        let state_sync = ProtocolFeature::CurrentEpochStateSync.protocol_version();
        let bandwidth_scheduler = ProtocolFeature::BandwidthScheduler.protocol_version();
        let resharding_v3 = ProtocolFeature::SimpleNightshadeV4.protocol_version();

        assert!(state_sync < resharding_v3);
        assert!(bandwidth_scheduler < resharding_v3);
    }
}

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
    #[deprecated]
    _DeprecatedImplicitAccountCreation,
    #[deprecated]
    _DeprecatedRectifyInflation,
    /// Add `AccessKey` nonce range by setting nonce to `(block_height - 1) * 1e6`, see
    /// <https://github.com/near/nearcore/issues/3779>.
    #[deprecated]
    _DeprecatedAccessKeyNonceRange,
    /// Don't process any receipts for shard when chunk is not present.
    /// Always use gas price computed in the previous block.
    #[deprecated]
    _DeprecatedFixApplyChunks,
    #[deprecated]
    _DeprecatedLowerStorageCost,
    #[deprecated]
    _DeprecatedDeleteActionRestriction,
    /// Add versions to `Account` data structure
    #[deprecated]
    _DeprecatedAccountVersions,
    #[deprecated]
    _DeprecatedTransactionSizeLimit,
    /// Fix a bug in `storage_usage` for account caused by #3824
    #[deprecated]
    _DeprecatedFixStorageUsage,
    /// Cap maximum gas price to 2,000,000,000 yoctoNEAR
    #[deprecated]
    _DeprecatedCapMaxGasPrice,
    #[deprecated]
    _DeprecatedCountRefundReceiptsInGasLimit,
    /// Add `ripemd60` and `ecrecover` host function
    #[deprecated]
    _DeprecatedMathExtension,
    /// Restore receipts that were previously stuck because of
    /// <https://github.com/near/nearcore/pull/4228>.
    #[deprecated]
    _DeprecatedRestoreReceiptsAfterFixApplyChunks,
    /// Minimum protocol version for NEP-92
    #[deprecated]
    _DeprecatedMinProtocolVersionNep92,
    /// Minimum protocol version for NEP-92 fix
    #[deprecated]
    _DeprecatedMinProtocolVersionNep92Fix,
    /// Creates a unique random seed to be provided to `VMContext` from a given `action_hash` and a given `random_seed`
    #[deprecated]
    _DeprecatedCorrectRandomValue,
    /// The protocol version that enables reward on mainnet
    #[deprecated]
    _DeprecatedEnableInflation,
    /// Fix upgrade to use the latest voted protocol version instead of the current epoch protocol
    /// version when there is no new change in protocol version
    #[deprecated]
    _DeprecatedUpgradabilityFix,
    /// Updates the way receipt ID, data ID and random seeds are constructed
    #[deprecated]
    _DeprecatedCreateHash,
    /// Fix the storage usage of the delete key action
    #[deprecated]
    _DeprecatedDeleteKeyStorageUsage,
    /// Upgrade for shard chunk header
    #[deprecated]
    _DeprecatedShardChunkHeaderUpgrade,
    /// Updates the way receipt ID is constructed to use current block hash instead of last block hash
    #[deprecated]
    _DeprecatedCreateReceiptIdSwitchToCurrentBlock,
    /// Pessimistic gas price estimation uses a fixed value of `minimum_new_receipt_gas` to stop being
    /// tied to the function call base cost
    #[deprecated]
    _DeprecatedFixedMinimumNewReceiptGas,
    /// This feature switch our WASM engine implementation from wasmer 0.* to
    /// wasmer 2.*, bringing better performance and reliability.
    ///
    /// The implementations should be sufficiently similar for this to not be a
    /// protocol upgrade, but we conservatively do a protocol upgrade to be on
    /// the safe side.
    ///
    /// Although wasmer2 is faster, we don't change fees with this protocol
    /// version -- we can safely do that in a separate step.
    #[deprecated]
    _DeprecatedWasmer2,
    SimpleNightshade,
    #[deprecated]
    _DeprecatedLowerDataReceiptAndEcrecoverBaseCost,
    /// Lowers the cost of wasm instruction due to switch to wasmer2.
    #[deprecated]
    _DeprecatedLowerRegularOpCost,
    /// Lowers the cost of wasm instruction due to switch to faster,
    /// compiler-intrinsics based gas counter.
    #[deprecated]
    _DeprecatedLowerRegularOpCost2,
    /// Limit number of wasm functions in one contract. See
    /// <https://github.com/near/nearcore/pull/4954> for more details.
    #[deprecated]
    _DeprecatedLimitContractFunctionsNumber,
    #[deprecated]
    _DeprecatedBlockHeaderV3,
    /// Changes how we select validators for epoch and how we select validators
    /// within epoch.  See <https://github.com/near/NEPs/pull/167> for general
    /// description, note that we would not introduce chunk-only validators with
    /// this feature
    #[deprecated]
    _DeprecatedAliasValidatorSelectionAlgorithm,
    /// Make block producers produce chunks for the same block they would later produce to avoid
    /// network delays
    #[deprecated]
    _DeprecatedSynchronizeBlockChunkProduction,
    /// Change the algorithm to count WASM stack usage to avoid under counting in
    /// some cases.
    #[deprecated]
    _DeprecatedCorrectStackLimit,
    /// Add `AccessKey` nonce range for implicit accounts, as in `AccessKeyNonceRange` feature.
    #[deprecated]
    _DeprecatedAccessKeyNonceForImplicitAccounts,
    /// Increase cost per deployed code byte to cover for the compilation steps
    /// that a deployment triggers. Only affects the action execution cost.
    #[deprecated]
    _DeprecatedIncreaseDeploymentCost,
    #[deprecated]
    _DeprecatedFunctionCallWeight,
    /// This feature enforces a global limit on the function local declarations in a WebAssembly
    /// contract. See <...> for more information.
    #[deprecated]
    _DeprecatedLimitContractLocals,
    /// Ensure caching all nodes in the chunk for which touching trie node cost was charged. Charge for each such node
    /// only once per chunk at the first access time.
    #[deprecated]
    _DeprecatedChunkNodesCache,
    /// Lower `max_length_storage_key` limit, which itself limits trie node sizes.
    #[deprecated]
    _DeprecatedLowerStorageKeyLimit,
    // alt_bn128_g1_multiexp, alt_bn128_g1_sum, alt_bn128_pairing_check host functions
    #[deprecated]
    _DeprecatedAltBn128,
    #[deprecated]
    _DeprecatedChunkOnlyProducers,
    /// Ensure the total stake of validators that are kicked out does not exceed a percentage of total stakes
    #[deprecated]
    _DeprecatedMaxKickoutStake,
    /// Validate account id for function call access keys.
    #[deprecated]
    _DeprecatedAccountIdInFunctionCallPermission,
    /// Zero Balance Account NEP 448: <https://github.com/near/NEPs/pull/448>
    #[deprecated]
    _DeprecatedZeroBalanceAccount,
    /// Execute a set of actions on behalf of another account.
    ///
    /// Meta Transaction NEP-366: <https://github.com/near/NEPs/blob/master/neps/nep-0366.md>
    #[deprecated]
    _DeprecatedDelegateAction,
    #[deprecated]
    _DeprecatedEd25519Verify,
    /// Decouple compute and gas costs of operations to safely limit the compute time it takes to
    /// process the chunk.
    ///
    /// Compute Costs NEP-455: <https://github.com/near/NEPs/blob/master/neps/nep-0455.md>
    #[deprecated]
    _DeprecatedComputeCosts,
    /// Decrease the cost of function call action. Only affects the execution cost.
    #[deprecated]
    _DeprecatedDecreaseFunctionCallBaseCost,
    /// Enable flat storage for reads, reducing number of DB accesses from `2 * key.len()` in
    /// the worst case to 2.
    ///
    /// Flat Storage NEP-399: <https://github.com/near/NEPs/blob/master/neps/nep-0399.md>
    #[deprecated]
    _DeprecatedFlatStorageReads,
    /// Enables preparation V2. Note that this setting is not supported in production settings
    /// without NearVmRuntime enabled alongside it, as the VM runner would be too slow.
    #[deprecated]
    _DeprecatedPreparationV2,
    /// Enables Near-Vm. Note that this setting is not at all supported without PreparationV2,
    /// as it hardcodes preparation v2 code into the generated assembly.
    #[deprecated]
    _DeprecatedNearVmRuntime,
    #[deprecated]
    _DeprecatedBlockHeaderV4,
    /// Resharding V2. A new implementation for resharding and a new shard
    /// layout for the production networks.
    SimpleNightshadeV2,
    /// Built on top of Resharding V2. Changes shard layout to V3 to split shard 2 into two parts.
    SimpleNightshadeV3,
    /// In case not all validator seats are occupied our algorithm provide incorrect minimal seat
    /// price - it reports as alpha * sum_stake instead of alpha * sum_stake / (1 - alpha), where
    /// alpha is min stake ratio
    #[deprecated]
    _DeprecatedFixStakingThreshold,
    /// In case not all validator seats are occupied, the minimum seat price of a chunk producer
    /// used to depend on the number of existing shards, which is no longer the case.
    #[deprecated]
    _DeprecatedFixChunkProducerStakingThreshold,
    /// Charge for contract loading before it happens.
    FixContractLoadingCost,
    /// Enables rejection of blocks with outdated protocol versions.
    #[deprecated]
    _DeprecatedRejectBlocksWithOutdatedProtocolVersions,
    // NEP: https://github.com/near/NEPs/pull/488
    #[deprecated]
    _DeprecatedBLS12381,
    #[deprecated]
    _DeprecatedRestrictTla,
    /// Increases the number of chunk producers.
    #[deprecated]
    _DeprecatedTestnetFewerBlockProducers,
    /// Enables stateless validation which is introduced in <https://github.com/near/NEPs/pull/509>
    /// LowerValidatorKickoutPercentForDebugging: lower block and chunk validator kickout percent from 90 to 50.
    /// SingleShardTracking: single shard tracking for stateless validation.
    /// StateWitnessSizeLimit: state witness size limits.
    /// PerReceiptHardStorageProofLimit: limit the size of storage proof generated by a single receipt.
    /// WitnessTransactionLimits: size limits for transactions included in a ChunkStateWitness.
    /// NoChunkOnlyProducers: no chunk-only producers in stateless validation.
    #[deprecated]
    _DeprecatedStatelessValidation,
    #[deprecated]
    _DeprecatedEthImplicitAccounts,
    /// Enables yield execution which is introduced in <https://github.com/near/NEPs/pull/519>
    #[deprecated]
    _DeprecatedYieldExecution,
    /// Bring minimum required validator stake effectively to ~10K NEAR as of 2024-08-15.
    /// Fixes increase to 100K NEAR in the previous protocol version.
    /// See #11953 for more details.
    #[deprecated]
    _DeprecatedFixMinStakeRatio,
    /// Increases main_storage_proof_size_soft_limit parameter from 3mb to 4mb
    #[deprecated]
    _DeprecatedIncreaseStorageProofSizeSoftLimit,

    // Shuffle shard assignments for chunk producers at every epoch.
    ShuffleShardAssignments,
    /// Cross-shard congestion control according to <https://github.com/near/NEPs/pull/539>.
    #[deprecated]
    _DeprecatedCongestionControl,
    /// Remove account with long storage key.
    #[deprecated]
    _DeprecatedRemoveAccountWithLongStorageKey,
    /// Change the structure of ChunkEndorsement to have (shard_id, epoch_id, height_created)
    /// instead of chunk_hash
    #[deprecated]
    _DeprecatedChunkEndorsementV2,
    // Include a bitmap of endorsements from chunk validator in the block header
    // in order to calculate the rewards and kickouts for the chunk validators.
    // This feature introduces BlockHeaderV5.
    #[deprecated]
    _DeprecatedChunkEndorsementsInBlockHeader,
    /// Store receipts in State in the StateStoredReceipt format.
    #[deprecated]
    _DeprecatedStateStoredReceipt,
    /// Resharding V3 - Adding "game.hot.tg-0" boundary.
    SimpleNightshadeV4,
    /// Resharding V3 - Adding "earn.kaiching" boundary.
    SimpleNightshadeV5,
    /// Resharding V3 - Adding "750" boundary.
    SimpleNightshadeV6,
    /// Exclude contract code from the chunk state witness and distribute it to chunk validators separately.
    #[deprecated]
    _DeprecatedExcludeContractCodeFromStateWitness,
    /// A scheduler which limits bandwidth for sending receipts between shards.
    #[deprecated]
    _DeprecatedBandwidthScheduler,
    /// Indicates that the "sync_hash" used to identify the point in the chain to sync state to
    /// should no longer be the first block of the epoch, but a couple blocks after that in order
    /// to sync the current epoch's state. This is not strictly a protocol feature, but is included
    /// here to coordinate among nodes
    _DeprecatedCurrentEpochStateSync,
    /// Relaxed validation of transactions included in a chunk.
    ///
    /// Chunks no longer become entirely invalid in case invalid transactions are included in the
    /// chunk. Instead the transactions are discarded during their conversion to receipts.
    ///
    /// support for code that does not do relaxed chunk validation has now been removed.
    #[deprecated(
        note = "Was used for protocol versions without relaxed chunk validation which is not supported anymore."
    )]
    _DeprecatedRelaxedChunkValidation,
    /// This enables us to remove the expensive check_balance call from the
    /// runtime.
    ///
    /// Support for code that does check balances has now been removed.
    #[deprecated(
        note = "Was used for protocol versions where we checked balances which is not supported anymore."
    )]
    _DeprecatedRemoveCheckBalance,
    /// Exclude existing contract code in deploy-contract and delete-account actions from the chunk state witness.
    /// Instead of sending code in the witness, the code checks the code-size using the internal trie nodes.
    ExcludeExistingCodeFromWitnessForCodeLen,
    /// Use the block height instead of the block hash to calculate the receipt ID.
    BlockHeightForReceiptId,
    /// Enable optimistic block production.
    ProduceOptimisticBlock,
    GlobalContracts,
    /// NEP: https://github.com/near/NEPs/pull/536
    ///
    /// Reduce the number of gas refund receipts by charging the current gas
    /// price rather than a pessimistic gas price. Also, introduce a new fee of
    /// 5% for gas refunds and charge the signer this fee for gas refund
    /// receipts.
    ReducedGasRefunds,
    /// Move from ChunkStateWitness being a single struct to a versioned enum.
    VersionedStateWitness,
    SaturatingFloatToInt,
}

impl ProtocolFeature {
    // The constructor must initialize all fields of the struct but some fields
    // are deprecated.  So unfortunately, we need this attribute here.  A better
    // fix is being discussed on
    // https://github.com/rust-lang/rust/issues/102777.
    #[allow(deprecated)]
    pub const fn protocol_version(self) -> ProtocolVersion {
        match self {
            // Stable features
            ProtocolFeature::_DeprecatedMinProtocolVersionNep92 => 31,
            ProtocolFeature::_DeprecatedMinProtocolVersionNep92Fix => 32,
            ProtocolFeature::_DeprecatedCorrectRandomValue => 33,
            ProtocolFeature::_DeprecatedImplicitAccountCreation => 35,
            ProtocolFeature::_DeprecatedEnableInflation => 36,
            ProtocolFeature::_DeprecatedUpgradabilityFix => 37,
            ProtocolFeature::_DeprecatedCreateHash => 38,
            ProtocolFeature::_DeprecatedDeleteKeyStorageUsage => 40,
            ProtocolFeature::_DeprecatedShardChunkHeaderUpgrade => 41,
            ProtocolFeature::_DeprecatedCreateReceiptIdSwitchToCurrentBlock
            | ProtocolFeature::_DeprecatedLowerStorageCost => 42,
            ProtocolFeature::_DeprecatedDeleteActionRestriction => 43,
            ProtocolFeature::_DeprecatedFixApplyChunks => 44,
            ProtocolFeature::_DeprecatedRectifyInflation
            | ProtocolFeature::_DeprecatedAccessKeyNonceRange => 45,
            ProtocolFeature::_DeprecatedAccountVersions
            | ProtocolFeature::_DeprecatedTransactionSizeLimit
            | ProtocolFeature::_DeprecatedFixStorageUsage
            | ProtocolFeature::_DeprecatedCapMaxGasPrice
            | ProtocolFeature::_DeprecatedCountRefundReceiptsInGasLimit
            | ProtocolFeature::_DeprecatedMathExtension => 46,
            ProtocolFeature::_DeprecatedRestoreReceiptsAfterFixApplyChunks => 47,
            ProtocolFeature::_DeprecatedWasmer2
            | ProtocolFeature::_DeprecatedLowerDataReceiptAndEcrecoverBaseCost
            | ProtocolFeature::_DeprecatedLowerRegularOpCost
            | ProtocolFeature::SimpleNightshade => 48,
            ProtocolFeature::_DeprecatedLowerRegularOpCost2
            | ProtocolFeature::_DeprecatedLimitContractFunctionsNumber
            | ProtocolFeature::_DeprecatedBlockHeaderV3
            | ProtocolFeature::_DeprecatedAliasValidatorSelectionAlgorithm => 49,
            ProtocolFeature::_DeprecatedSynchronizeBlockChunkProduction
            | ProtocolFeature::_DeprecatedCorrectStackLimit => 50,
            ProtocolFeature::_DeprecatedAccessKeyNonceForImplicitAccounts => 51,
            ProtocolFeature::_DeprecatedIncreaseDeploymentCost
            | ProtocolFeature::_DeprecatedFunctionCallWeight
            | ProtocolFeature::_DeprecatedLimitContractLocals
            | ProtocolFeature::_DeprecatedChunkNodesCache
            | ProtocolFeature::_DeprecatedLowerStorageKeyLimit => 53,
            ProtocolFeature::_DeprecatedAltBn128 => 55,
            ProtocolFeature::_DeprecatedChunkOnlyProducers
            | ProtocolFeature::_DeprecatedMaxKickoutStake => 56,
            ProtocolFeature::_DeprecatedAccountIdInFunctionCallPermission => 57,
            ProtocolFeature::_DeprecatedEd25519Verify
            | ProtocolFeature::_DeprecatedZeroBalanceAccount
            | ProtocolFeature::_DeprecatedDelegateAction => 59,
            ProtocolFeature::_DeprecatedComputeCosts
            | ProtocolFeature::_DeprecatedFlatStorageReads => 61,
            ProtocolFeature::_DeprecatedPreparationV2
            | ProtocolFeature::_DeprecatedNearVmRuntime => 62,
            ProtocolFeature::_DeprecatedBlockHeaderV4 => 63,
            ProtocolFeature::_DeprecatedRestrictTla
            | ProtocolFeature::_DeprecatedTestnetFewerBlockProducers
            | ProtocolFeature::SimpleNightshadeV2 => 64,
            ProtocolFeature::SimpleNightshadeV3 => 65,
            ProtocolFeature::_DeprecatedDecreaseFunctionCallBaseCost
            | ProtocolFeature::_DeprecatedFixedMinimumNewReceiptGas => 66,
            ProtocolFeature::_DeprecatedYieldExecution => 67,
            ProtocolFeature::_DeprecatedCongestionControl
            | ProtocolFeature::_DeprecatedRemoveAccountWithLongStorageKey => 68,
            ProtocolFeature::_DeprecatedStatelessValidation => 69,
            ProtocolFeature::_DeprecatedBLS12381
            | ProtocolFeature::_DeprecatedEthImplicitAccounts => 70,
            ProtocolFeature::_DeprecatedFixMinStakeRatio => 71,
            ProtocolFeature::_DeprecatedIncreaseStorageProofSizeSoftLimit
            | ProtocolFeature::_DeprecatedChunkEndorsementV2
            | ProtocolFeature::_DeprecatedChunkEndorsementsInBlockHeader
            | ProtocolFeature::_DeprecatedStateStoredReceipt => 72,
            ProtocolFeature::_DeprecatedExcludeContractCodeFromStateWitness => 73,
            ProtocolFeature::_DeprecatedFixStakingThreshold
            | ProtocolFeature::_DeprecatedRejectBlocksWithOutdatedProtocolVersions
            | ProtocolFeature::_DeprecatedFixChunkProducerStakingThreshold
            | ProtocolFeature::_DeprecatedRelaxedChunkValidation
            | ProtocolFeature::_DeprecatedRemoveCheckBalance
            | ProtocolFeature::_DeprecatedBandwidthScheduler
            | ProtocolFeature::_DeprecatedCurrentEpochStateSync => 74,
            ProtocolFeature::SimpleNightshadeV4 => 75,
            ProtocolFeature::SimpleNightshadeV5 => 76,
            ProtocolFeature::GlobalContracts
            | ProtocolFeature::BlockHeightForReceiptId
            | ProtocolFeature::ProduceOptimisticBlock => 77,
            ProtocolFeature::SimpleNightshadeV6
            | ProtocolFeature::VersionedStateWitness
            | ProtocolFeature::SaturatingFloatToInt
            | ProtocolFeature::ReducedGasRefunds => 78,
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
pub const MIN_SUPPORTED_PROTOCOL_VERSION: ProtocolVersion = 75;

/// Current protocol version used on the mainnet with all stable features.
const STABLE_PROTOCOL_VERSION: ProtocolVersion = 78;

// On nightly, pick big enough version to support all features.
const NIGHTLY_PROTOCOL_VERSION: ProtocolVersion = 149;

/// Largest protocol version supported by the current binary.
pub const PROTOCOL_VERSION: ProtocolVersion =
    if cfg!(feature = "nightly") { NIGHTLY_PROTOCOL_VERSION } else { STABLE_PROTOCOL_VERSION };

/// Both, outgoing and incoming tcp connections to peers, will be rejected if `peer's`
/// protocol version is lower than this.
/// TODO(pugachag): revert back to `- 3` after mainnet is upgraded
pub const PEER_MIN_ALLOWED_PROTOCOL_VERSION: ProtocolVersion = STABLE_PROTOCOL_VERSION - 4;

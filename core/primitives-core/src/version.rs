use crate::types::ProtocolVersion;

/// New Protocol features should go here. Features are guarded by their corresponding feature flag.
/// For example, if we have `ProtocolFeature::EVM` and a corresponding feature flag `evm`, it will look
/// like
///
/// #[cfg(feature = "protocol_feature_evm")]
/// EVM code
///
#[derive(Hash, PartialEq, Eq, Clone, Copy, Debug)]
#[cfg_attr(test, derive(strum::EnumIter))]
pub enum ProtocolFeature {
    /// Charge for contract loading before it happens.
    FixContractLoadingCost,
    // Shuffle shard assignments for chunk producers at every epoch.
    ShuffleShardAssignments,
    DynamicResharding,
    GasKeys,
    /// Meta transactions with gas key support via `Action::DelegateV2`.
    /// Note: Later disabled by `RejectDelegateV2`.
    DelegateV2,
    /// Fix missing early return on DepositWithFunctionCall error path in
    /// validate_delegate_action_key. Previously the error could be
    /// overwritten by a subsequent receiver_id or method_name check.
    FixDelegateActionDepositWithFunctionCallError,
    Spice,
    ContinuousEpochSync,
    /// Fix `action_delete_account` not subtracting the global contract
    /// identifier storage usage. Previously only local contract code was
    /// subtracted, overstating storage usage for accounts with global
    /// contracts and making them marginally harder to delete.
    FixDeleteAccountGlobalContractStorageUsage,
    /// Skip transactions whose hash already appeared earlier in the same chunk.
    /// A transaction hash is also its outcome id, and outcomes are committed
    /// (via the chunk outcome root) keyed by that id. Including a transaction
    /// twice would otherwise commit two conflicting outcomes (a success and an
    /// InvalidNonce failure) under one id.
    UniqueChunkTransactions,
    /// Opt-in strict nonce mode for transactions. When enabled, TransactionV1
    /// can carry `NonceMode::Strict` which requires `tx_nonce == ak_nonce + 1`
    /// (sequential ordering). Transactions with a nonce gap are held in the
    /// pool rather than discarded.
    StrictNonce,
    /// Pre-compute and persist chunk producer assignments in `DBCol::ChunkProducers`
    /// during header sync and block processing. Foundation for early chunk producer
    /// kickout without epoch manager recomputation.
    EarlyKickout,
    /// Extend the existing sticky chunk-producer-to-shard assignment to
    /// resharding boundaries. Previously stickiness was keyed by
    /// `ShardIndex`, which is unstable across a shard layout change; switch
    /// to keying by `ShardId`, and when a shard splits distribute the
    /// parent's chunk producers across its child shards using greedy
    /// stake-balanced bin-packing. Reduces unnecessary state sync after
    /// resharding.
    StickyReshardingValidatorAssignment,
    /// Add FIPS 204 ML-DSA-65 (post-quantum) as a third transaction signature
    /// scheme alongside ed25519 and secp256k1. Pre-feature blocks reject any
    /// transaction or `AddKey` action carrying an ML-DSA-65 key/signature, so
    /// post-feature there is no question of grandfathered keys.
    PostQuantumSignatures,
    /// Allow creating `DeterministicStateInitAction` from a delegated action by
    /// fixing the receiver id check.
    FixDelegatedDeterministicStateInit,
    /// Fix same-chunk calls to a just-distributed global contract by recording the deploy.
    GlobalContractSameChunkCallFix,
    /// Emit `ExecutionMetadata::V4` from chunk producers. V4 carries a
    /// per-action `Vec<AccountContract>`: one entry per action in the
    /// receipt, recording the contract attached to the receiver account
    /// immediately before that action ran. Captured unconditionally for
    /// every action kind (not just `FunctionCall`), so consumers can see
    /// what code an account had even on receipts that did not invoke a
    /// contract. `AccountContract::None` is emitted when the account has
    /// no contract deployed, when it did not yet exist (e.g. the
    /// `CreateAccount` slot that materialized it), or for unexecuted
    /// trailing slots padded after a mid-receipt failure. Order matches
    /// the receipt's `actions` vector. This is relevant when the receiver
    /// account and the contract source diverge — e.g. global contracts
    /// and `UseGlobalContract` flows. Wire format changes (new borsh
    /// discriminant), so the cutover must be coordinated across the
    /// network.
    ExecutionMetadataV4,
    /// New host functions `promise_yield_create_with_id` and `promise_yield_resume_with_yield_id`
    /// that allow contracts to provide a custom yield ID for yield/resume.
    YieldWithId,
    /// Increase account creation cost
    AccountCostIncrease,
    /// Recompute `block_ordinal` and `epoch_sync_data_hash` against local chain
    /// state when validating received block headers.
    ValidateBlockOrdinalAndEpochSyncDataHash,
    /// Authenticate `ContractCodeResponse` messages with a chunk-producer
    /// signature, matching the signed-message pattern already used by
    /// `ChunkContractAccesses` and `ContractCodeRequest`. Senders emit
    /// `ContractCodeResponseV2` (with a signed inner payload); receivers
    /// require a verifiable signature before processing the response.
    SignedContractCodeResponse,
    ClampOutgoingGasAdmission,
    /// Charge the contract-loading fee (and finalize as a gas-bearing abort
    /// rather than a zero-gas nop) when a compiled module fails to load at
    /// `Module::deserialize`.
    FixContractLoadingError,
    /// Bound the combined size of the promise inputs a single receipt consumes.
    ReceiptPromiseInputSizeLimit,
    /// Reject `FunctionCall` actions with an empty `method_name` during action validation.
    RejectEmptyMethodName,
    /// Reject `Action::DelegateV2`. This disables meta transactions from gas
    /// keys, because the inner nonce advances a gas key of the delegate sender
    /// and `PendingTransactionQueue` does not see it: the queue reads only the
    /// outer transaction's signer, public key and nonce index, so its nonce and
    /// gas key balance commitments would miss that key. The `DelegateV2`
    /// variant and `VersionedDelegateActionPayload` remain so a later delegate
    /// action version can reuse them.
    RejectDelegateV2,
    /// Reject a `WithdrawFromGasKey` action nested inside a delegate action.
    /// The SPICE pending transaction queue scans only the top level actions of
    /// a transaction for `WithdrawFromGasKey`, so a nested one drains a gas key
    /// that the queue still counts as funded.
    RejectWithdrawFromGasKeyInDelegate,
    EnforcePerReceiptStorageProofLimit,
    /// Extend the per-receipt storage proof limit to every action kind. The
    /// `RecordedStorageCounter` only runs inside the VM, so it bounds
    /// `FunctionCall` actions alone; other actions in the same receipt could
    /// record proof past the limit. Check the receipt's recorded size after
    /// each action and fail the receipt with
    /// `ActionErrorKind::ReceiptStorageProofSizeExceeded` once it goes over.
    EnforceStorageProofLimitForAllActions,
    /// Remove gas rewards: stop paying part of the gas burned by a
    /// `FunctionCall` back to the contract account as a reward. Sets the
    /// `burnt_gas_reward` parameter from 30% (3/10) to 0%.
    RemoveGasRewards,
    /// Fix two related ML-DSA-65 cost-charging issues (both harmless for
    /// classical schemes, where the relevant quantities coincide):
    /// - Gas keys: price the exec (storage) fee on the on-trie identifier length
    ///   (`trie_id_len()`) and the send (transmission) fee on the wire length
    ///   (`len()`), rather than pricing the exec fee on the wire length.
    /// - Meta transactions: meter the inner `DelegateAction` signature
    ///   verification compute on the receiver shard that actually runs the
    ///   verification, instead of on the signer shard, so it counts against the
    ///   right `compute_limit`.
    FixMlDsaCostCharging,
    /// Calls to an account whose global contract was never deployed fail with
    /// `CodeDoesNotExist`. Previously chunk validators rejected such a state
    /// witness as incomplete, which stalled the shard.
    FailCallToMissingGlobalContract,
    /// Universal accounts: the `0u` account scheme. Enables the `UniversalStateInit`
    /// action, which creates an account whose ID is derived from its canonical state
    /// init (contract code, storage, and access keys).
    UniversalAccounts,
    /// Use the new version of the eth-wallet contract.
    /// If an account references the global contract hash of the old eth-wallet
    /// contract then it will automatically resolve to the new version instead.
    UpdatedEthWalletContract,
}

impl ProtocolFeature {
    pub const fn protocol_version(self) -> ProtocolVersion {
        match self {
            // Stable features
            ProtocolFeature::FixDelegateActionDepositWithFunctionCallError
            | ProtocolFeature::FixDeleteAccountGlobalContractStorageUsage
            | ProtocolFeature::FixDelegatedDeterministicStateInit
            | ProtocolFeature::GasKeys
            | ProtocolFeature::ContinuousEpochSync
            | ProtocolFeature::DynamicResharding
            | ProtocolFeature::StickyReshardingValidatorAssignment
            | ProtocolFeature::StrictNonce
            | ProtocolFeature::PostQuantumSignatures
            | ProtocolFeature::UniqueChunkTransactions
            | ProtocolFeature::ValidateBlockOrdinalAndEpochSyncDataHash
            | ProtocolFeature::YieldWithId
            | ProtocolFeature::ExecutionMetadataV4
            | ProtocolFeature::SignedContractCodeResponse
            | ProtocolFeature::ClampOutgoingGasAdmission
            | ProtocolFeature::AccountCostIncrease
            | ProtocolFeature::DelegateV2 => 85,
            ProtocolFeature::EnforcePerReceiptStorageProofLimit => 86,
            ProtocolFeature::FixContractLoadingError => 87,
            ProtocolFeature::RejectEmptyMethodName => 87,
            ProtocolFeature::RejectDelegateV2 => 87,
            ProtocolFeature::RejectWithdrawFromGasKeyInDelegate => 87,
            ProtocolFeature::RemoveGasRewards => 87,
            ProtocolFeature::EnforceStorageProofLimitForAllActions => 87,
            ProtocolFeature::ReceiptPromiseInputSizeLimit => 87,
            ProtocolFeature::EarlyKickout => 87,
            ProtocolFeature::FixMlDsaCostCharging => 87,
            ProtocolFeature::GlobalContractSameChunkCallFix => 87,
            ProtocolFeature::UniversalAccounts => 87,
            ProtocolFeature::FailCallToMissingGlobalContract => 88,
            ProtocolFeature::UpdatedEthWalletContract => 88,

            // Nightly features:
            ProtocolFeature::FixContractLoadingCost => 129,
            // TODO(#11201): When stabilizing this feature in mainnet, also remove the temporary code
            // that always enables this for mocknet (see config_mocknet function).
            ProtocolFeature::ShuffleShardAssignments => 143,
            // Spice is setup to include nightly, but not be part of it for now so that features
            // that are released before spice can be tested properly.
            ProtocolFeature::Spice => 180,
            // Place features that are not yet in Nightly below this line.
        }
    }

    pub const fn enabled(&self, protocol_version: ProtocolVersion) -> bool {
        protocol_version >= self.protocol_version()
    }
}

/// The protocol version of the genesis block on mainnet and testnet.
pub const PROD_GENESIS_PROTOCOL_VERSION: ProtocolVersion = 29;

/// Minimum supported protocol version for the current binary
pub const MIN_SUPPORTED_PROTOCOL_VERSION: ProtocolVersion = 84;

/// Returns the effective protocol version to use for processing a request.
///
/// Archival nodes can serve requests for blocks from protocol versions older than
/// `MIN_SUPPORTED_PROTOCOL_VERSION`. Some features from those old versions may no longer be
/// available in the current binary (e.g. the Wasmer0/Wasmer2 VM backends have been removed).
/// For read-only view calls that don't produce on-chain state, it is safe to clamp the protocol
/// version to `MIN_SUPPORTED_PROTOCOL_VERSION` so the request is processed with the config of
/// the oldest fully-supported version.
pub fn clamp_to_supported_protocol_version(
    current_protocol_version: ProtocolVersion,
) -> ProtocolVersion {
    current_protocol_version.max(MIN_SUPPORTED_PROTOCOL_VERSION)
}

/// Panics if `current_protocol_version` is below `MIN_SUPPORTED_PROTOCOL_VERSION`.
///
/// Use this at callee boundaries to enforce that the caller has already clamped the version
/// via [`clamp_to_supported_protocol_version`].
pub fn assert_supported_protocol_version(current_protocol_version: ProtocolVersion) {
    assert!(
        current_protocol_version >= MIN_SUPPORTED_PROTOCOL_VERSION,
        "protocol version {current_protocol_version} is below minimum supported {MIN_SUPPORTED_PROTOCOL_VERSION}"
    );
}

/// Current protocol version used on the mainnet with all stable features.
const STABLE_PROTOCOL_VERSION: ProtocolVersion = 88;

// On nightly, pick big enough version to support all features.
const NIGHTLY_PROTOCOL_VERSION: ProtocolVersion = 157;

// TODO(spice): Once spice is mature and close to release make it part of nightly - at the point in
// time cargo feature for spice should be removed as well.
// For spice we want to include all nightly features, but for now we don't want nightly to run with
// spice.
const SPICE_PROTOCOL_VERSION: ProtocolVersion = 200;

/// Largest protocol version supported by the current binary.
pub const PROTOCOL_VERSION: ProtocolVersion = if cfg!(feature = "protocol_feature_spice") {
    SPICE_PROTOCOL_VERSION
} else if cfg!(feature = "nightly") {
    NIGHTLY_PROTOCOL_VERSION
} else {
    STABLE_PROTOCOL_VERSION
};

#[cfg(test)]
mod tests {
    use crate::version::{MIN_SUPPORTED_PROTOCOL_VERSION, ProtocolFeature};
    use strum::IntoEnumIterator;

    #[test]
    fn all_features_at_or_above_min_supported_protocol_version() {
        for feature in ProtocolFeature::iter() {
            let version = feature.protocol_version();
            assert!(
                version >= MIN_SUPPORTED_PROTOCOL_VERSION,
                "{feature:?} has protocol version {version}, \
                 which is below MIN_SUPPORTED_PROTOCOL_VERSION ({MIN_SUPPORTED_PROTOCOL_VERSION})",
            );
        }
    }
}

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
    Spice,
    /// Pre-compute and persist chunk producer assignments in `DBCol::ChunkProducers`
    /// during header sync and block processing. Foundation for early chunk producer
    /// kickout without epoch manager recomputation.
    EarlyKickout,
    /// Fix same-chunk calls to a just-distributed global contract by recording the deploy.
    GlobalContractSameChunkCallFix,
    /// Charge the contract-loading fee (and finalize as a gas-bearing abort
    /// rather than a zero-gas nop) when a compiled module fails to load at
    /// `Module::deserialize`.
    FixContractLoadingError,
    /// Bound the combined size of the promise inputs a single receipt consumes.
    ReceiptPromiseInputSizeLimit,
    /// Raise `per_receipt_storage_proof_size_limit` to 4'500'000.
    RaisePerReceiptStorageProofLimit,
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
    /// A bandwidth request for a destination shard that is listed as its own parent
    /// no longer chains that shard's outgoing buffer onto itself.
    FixSelfMappedShardBandwidthRequest,
    /// Universal accounts: the `0u` account scheme. Enables the `UniversalStateInit`
    /// action, which creates an account whose ID is derived from its canonical state
    /// init (contract code, storage, and access keys).
    UniversalAccounts,
    /// Reject new transfers to `0u` ids until `UniversalAccounts` is enabled, so no receipt
    /// funded without the account creation fee executes once the fee applies.
    RejectUniversalAccountTransfers,
    /// A universal state init on an already initialized account refunds the whole deposit.
    /// Before it, the deposit also tops up storage staking.
    RefundRepeatedUniversalStateInitDeposit,
    /// A `UniversalStateInit` is accepted only in its canonical borsh encoding, so that each
    /// state init has exactly one valid encoding and one account id. Turns on the check behind
    /// the status of the `universal_state_init_to_account_id` host function, and comes with the
    /// fees that price the decode and canonical check.
    CanonicalUniversalStateInit,
    /// Use the new version of the eth-wallet contract.
    /// If an account references the global contract hash of the old eth-wallet
    /// contract then it will automatically resolve to the new version instead.
    UpdatedEthWalletContract,
    /// External contract calls (ECC): unsigned transactions carrying no signer, public key,
    /// nonce or signature. See <https://github.com/near/nearcore/issues/16423>.
    ExternalContractCalls,
    /// Reject `BlockHeaderV7` in epochs before Spice. Without this feature, `validate_header`
    /// accepts it there and the header is handled like `BlockHeaderV6`.
    RejectBlockHeaderV7BeforeSpice,
}

impl ProtocolFeature {
    pub const fn protocol_version(self) -> ProtocolVersion {
        match self {
            // Stable features
            ProtocolFeature::EnforcePerReceiptStorageProofLimit => 86,
            ProtocolFeature::FixContractLoadingError => 87,
            ProtocolFeature::RejectEmptyMethodName => 87,
            ProtocolFeature::RejectDelegateV2 => 87,
            ProtocolFeature::RejectWithdrawFromGasKeyInDelegate => 87,
            ProtocolFeature::RemoveGasRewards => 87,
            ProtocolFeature::EnforceStorageProofLimitForAllActions => 87,
            ProtocolFeature::EarlyKickout => 87,
            ProtocolFeature::GlobalContractSameChunkCallFix => 87,
            ProtocolFeature::RejectBlockHeaderV7BeforeSpice => 87,
            ProtocolFeature::RejectUniversalAccountTransfers => 87,
            ProtocolFeature::UniversalAccounts => 88,
            ProtocolFeature::RefundRepeatedUniversalStateInitDeposit => 88,
            ProtocolFeature::CanonicalUniversalStateInit => 88,
            ProtocolFeature::FailCallToMissingGlobalContract => 89,
            ProtocolFeature::FixSelfMappedShardBandwidthRequest => 89,
            ProtocolFeature::UpdatedEthWalletContract => 89,
            ProtocolFeature::FixContractLoadingCost => 89,
            ProtocolFeature::RaisePerReceiptStorageProofLimit => 89,

            // Nightly features:
            // TODO(#11201): When stabilizing this feature in mainnet, also remove the temporary code
            // that always enables this for mocknet (see config_mocknet function).
            ProtocolFeature::ShuffleShardAssignments => 143,
            ProtocolFeature::ExternalContractCalls => 150,
            ProtocolFeature::FixMlDsaCostCharging => 157,
            ProtocolFeature::ReceiptPromiseInputSizeLimit => 157,
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
pub const MIN_SUPPORTED_PROTOCOL_VERSION: ProtocolVersion = 85;

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
const STABLE_PROTOCOL_VERSION: ProtocolVersion = 89;

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

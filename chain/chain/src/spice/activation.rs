//! Runtime spice-activation gate for the spice actors.

use crate::metrics;
use near_chain_primitives::Error;
use near_primitives::hash::CryptoHash;
use near_store::adapter::chain_store::ChainStoreAdapter;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SpiceMessageKind {
    ChunkEndorsement,
    PartialData,
    PartialDataRequest,
    ContractAccesses,
    ContractCodeRequest,
    ContractCodeResponse,
    StateWitness,
}

impl SpiceMessageKind {
    pub const ALL: [Self; 7] = [
        Self::ChunkEndorsement,
        Self::PartialData,
        Self::PartialDataRequest,
        Self::ContractAccesses,
        Self::ContractCodeRequest,
        Self::ContractCodeResponse,
        Self::StateWitness,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::ChunkEndorsement => "chunk_endorsement",
            Self::PartialData => "partial_data",
            Self::PartialDataRequest => "partial_data_request",
            Self::ContractAccesses => "contract_accesses",
            Self::ContractCodeRequest => "contract_code_request",
            Self::ContractCodeResponse => "contract_code_response",
            Self::StateWitness => "state_witness",
        }
    }
}

/// Whether the block `block_hash` is a spice block.
///
/// Errors when the header is not on disk; callers that can be handed an
/// arbitrary block hash by a peer should use [`accept_spice_network_message`],
/// which folds that case into a drop decision.
pub fn spice_enabled_for_block(
    chain_store: &ChainStoreAdapter,
    block_hash: &CryptoHash,
) -> Result<bool, Error> {
    Ok(chain_store.get_block_header(block_hash)?.is_spice())
}

/// Whether spice is active at the current head.
///
/// For startup work, which has no particular block to key on, and as the
/// fallback in [`accept_spice_network_message`].
pub fn spice_enabled_at_head(chain_store: &ChainStoreAdapter) -> Result<bool, Error> {
    let head = chain_store.head()?;
    spice_enabled_for_block(chain_store, &head.last_block_hash)
}

/// [`spice_enabled_at_head`] for actor startup, where there is no caller to return an
/// error to.
///
/// A store with no head yet is reported as pre-spice. Any other storage error is
/// fatal: the recovery paths this gates already panic on a store they cannot read, so
/// skipping them would trade a crash for a node silently running with unrecovered state.
pub fn spice_enabled_at_head_on_startup(chain_store: &ChainStoreAdapter) -> bool {
    match spice_enabled_at_head(chain_store) {
        Ok(enabled) => enabled,
        Err(Error::DBNotFoundErr(_)) => false,
        Err(err) => panic!("failed to determine whether spice is active at head: {err}"),
    }
}

/// Whether an inbound spice message referencing `block_hash` should be processed. A
/// dropped message is counted under `kind`.
///
/// The authoritative answer is the referenced block itself. When that block is
/// not on disk we cannot ask it, and we must not simply drop: spice legitimately
/// receives data ahead of its block and buffers it. So fall back to the head — a
/// node whose head is still pre-spice has no legitimate spice sender and drops,
/// while a node past activation keeps buffering exactly as before.
///
/// TODO(spice): around the activation boundary this drops data about the first spice
/// block that arrives before we hold that block's header, because the head is
/// still pre-spice.
pub fn accept_spice_network_message(
    chain_store: &ChainStoreAdapter,
    kind: SpiceMessageKind,
    block_hash: &CryptoHash,
) -> bool {
    let enabled = match spice_enabled_for_block(chain_store, block_hash) {
        Ok(enabled) => enabled,
        Err(_) => match spice_enabled_at_head(chain_store) {
            Ok(enabled) => enabled,
            // Neither the block nor the head is readable: we know nothing about
            // this chain, so we cannot claim spice is active on it.
            Err(err) => {
                tracing::debug!(
                    target: "spice_activation",
                    ?err,
                    kind = kind.as_str(),
                    %block_hash,
                    "cannot resolve spice-ness for spice message, dropping",
                );
                false
            }
        },
    };
    if !enabled {
        tracing::debug!(
            target: "spice_activation",
            kind = kind.as_str(),
            %block_hash,
            "dropping spice message, spice is not active",
        );
        metrics::SPICE_PRE_ACTIVATION_MESSAGES_DROPPED.with_label_values(&[kind.as_str()]).inc();
    }
    enabled
}

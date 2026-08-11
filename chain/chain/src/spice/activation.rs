//! Runtime spice-activation gate for the spice actors.

use crate::metrics;
use near_chain_primitives::Error;
use near_primitives::hash::CryptoHash;
use near_store::adapter::chain_store::ChainStoreAdapter;

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

/// Whether an inbound spice message referencing `block_hash` should be
/// processed. A dropped message is counted under `kind`, which must be one of the
/// values enumerated on [`metrics::SPICE_PRE_ACTIVATION_MESSAGES_DROPPED`].
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
    kind: &'static str,
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
                    kind,
                    %block_hash,
                    "cannot resolve spice-ness for spice message; dropping",
                );
                false
            }
        },
    };
    if !enabled {
        tracing::debug!(
            target: "spice_activation",
            kind,
            %block_hash,
            "dropping spice message: spice is not active",
        );
        metrics::SPICE_PRE_ACTIVATION_MESSAGES_DROPPED.with_label_values(&[kind]).inc();
    }
    enabled
}

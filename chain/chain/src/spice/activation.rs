//! Runtime spice-activation gate for the spice actors.

use crate::metrics;
use near_chain_primitives::Error;
use near_primitives::hash::CryptoHash;
use near_store::adapter::chain_store::ChainStoreAdapter;
#[cfg(feature = "test_features")]
use std::collections::HashMap;
use strum::{EnumIter, IntoStaticStr};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, EnumIter, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
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
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

/// Whether the block `block_hash` is a spice block.
///
/// Errors when the header is not on disk; callers that can be handed an
/// arbitrary block hash by a peer should use [`SpiceMessageGate::accept`], which folds
/// that case into a drop decision.
pub fn spice_enabled_for_block(
    chain_store: &ChainStoreAdapter,
    block_hash: &CryptoHash,
) -> Result<bool, Error> {
    Ok(chain_store.get_block_header(block_hash)?.is_spice())
}

/// Whether spice is active at the current head.
///
/// For startup work, which has no particular block to key on, and as the
/// fallback in [`SpiceMessageGate::accept`].
pub fn spice_enabled_at_head(chain_store: &ChainStoreAdapter) -> Result<bool, Error> {
    let head = chain_store.head()?;
    spice_enabled_for_block(chain_store, &head.last_block_hash)
}

/// Whether spice is active at the head, for actor startup, where there is no caller to
/// return an error to.
///
/// A store with no head at all is reported as pre-spice. Every other storage error,
/// including a head whose header is missing, is fatal: the recovery paths this gates
/// already panic on a store they cannot read, so skipping them would trade a crash for a
/// node that silently skipped recovery.
pub fn spice_enabled_at_head_on_startup(chain_store: &ChainStoreAdapter) -> bool {
    let head = match chain_store.head() {
        Ok(head) => head,
        Err(Error::DBNotFoundErr(_)) => return false,
        Err(err) => panic!("failed to read the chain head: {err}"),
    };
    match spice_enabled_for_block(chain_store, &head.last_block_hash) {
        Ok(enabled) => enabled,
        Err(err) => panic!("failed to determine whether spice is active at head: {err}"),
    }
}

/// Decides whether inbound spice messages should be processed, and tallies the ones it
/// drops.
///
/// Every actor that a peer can route a spice message to holds one. The per-kind tally
/// exists only under `test_features`, so that a test can assert a message was *dropped*. Production
/// observability is [`metrics::SPICE_PRE_ACTIVATION_MESSAGES_DROPPED`], which is updated
/// either way.
#[derive(Debug, Default)]
pub struct SpiceMessageGate {
    #[cfg(feature = "test_features")]
    dropped: HashMap<SpiceMessageKind, u64>,
}

impl SpiceMessageGate {
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
    pub fn accept(
        &mut self,
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
            metrics::SPICE_PRE_ACTIVATION_MESSAGES_DROPPED
                .with_label_values(&[kind.as_str()])
                .inc();
            #[cfg(feature = "test_features")]
            {
                *self.dropped.entry(kind).or_default() += 1;
            }
        }
        enabled
    }

    /// How many `kind` messages this gate has dropped.
    #[cfg(feature = "test_features")]
    pub fn dropped_count(&self, kind: SpiceMessageKind) -> u64 {
        self.dropped.get(&kind).copied().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::spice_enabled_at_head_on_startup;
    use near_primitives::block::Tip;
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::EpochId;
    use near_store::adapter::StoreAdapter as _;
    use near_store::test_utils::create_test_store;
    use near_store::{DBCol, HEAD_KEY};

    /// A node that has not synced anything yet is pre-spice rather than a panic.
    #[test]
    fn no_head_is_pre_spice() {
        let store = create_test_store();
        assert!(!spice_enabled_at_head_on_startup(&store.chain_store()));
    }

    /// A head whose header is missing is a corrupt store, not a pre-spice chain.
    #[test]
    #[should_panic(expected = "failed to determine whether spice is active at head")]
    fn head_without_a_header_panics() {
        let store = create_test_store();
        let mut update = store.store_update();
        update.set_ser(
            DBCol::BlockMisc,
            HEAD_KEY,
            &Tip {
                height: 1,
                last_block_hash: CryptoHash::hash_bytes(b"a block with no header"),
                prev_block_hash: CryptoHash::default(),
                epoch_id: EpochId(CryptoHash::default()),
                next_epoch_id: EpochId(CryptoHash::default()),
            },
        );
        update.commit();

        spice_enabled_at_head_on_startup(&store.chain_store());
    }
}

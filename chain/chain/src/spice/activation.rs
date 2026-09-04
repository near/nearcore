//! Runtime spice-activation gate for the spice actors.

use crate::metrics;
use crate::spice::boundary::is_spice_activation_parent;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::version::ProtocolFeature;
use near_store::adapter::chain_store::ChainStoreAdapter;
#[cfg(feature = "test_features")]
use std::collections::HashMap;
use strum::{EnumIter, IntoStaticStr};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, EnumIter, IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum SpiceMessageKind {
    ChunkEndorsement,
    PartialData,
    DataRequest,
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
/// arbitrary block hash by a peer should use [`SpiceMessageGate::should_process`], which folds
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
/// fallback in [`SpiceMessageGate::should_process`].
pub fn spice_enabled_at_head(chain_store: &ChainStoreAdapter) -> Result<bool, Error> {
    Ok(chain_store.head_header()?.is_spice())
}

/// Whether the epoch after the head's is a spice epoch: the head is in the last
/// pre-spice epoch, so a message about a block this node has not received yet can
/// concern the activation boundary and must not be dropped for pre-spice-ness.
fn spice_activation_imminent_at_head(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<bool, Error> {
    let head = chain_store.head()?;
    let next_epoch_protocol_version =
        epoch_manager.get_next_epoch_protocol_version(&head.last_block_hash)?;
    Ok(ProtocolFeature::Spice.enabled(next_epoch_protocol_version))
}

/// Whether spice work exists for `block_hash`: any spice block, and an activation
/// parent, which spice certifies through the boundary bootstrap.
pub fn spice_relevant_block(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    block_hash: &CryptoHash,
) -> Result<bool, Error> {
    Ok(spice_enabled_for_block(chain_store, block_hash)?
        || is_spice_activation_parent(epoch_manager, block_hash)?)
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

/// What a single drop counts, so that entries of a batched message do not land in the
/// per-message metric.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DropUnit {
    Message,
    Entry,
}

impl SpiceMessageGate {
    /// Whether an inbound spice message referencing `block_hash` should be processed. One drop
    /// counts one message.
    ///
    /// The authoritative answer is the referenced block itself. A resolved pre-spice
    /// block is still accepted when it is a verified activation parent: its execution
    /// results are certified under spice, so the endorsements, witnesses and data
    /// doing that all reference its hash, and there is no resend — dropping them
    /// while heads are still pre-spice would starve certification. When the block
    /// is not on disk we cannot ask it, and we must not simply drop: spice legitimately
    /// receives data ahead of its block and buffers it. So fall back to the head.
    pub fn should_process(
        &mut self,
        chain_store: &ChainStoreAdapter,
        epoch_manager: &dyn EpochManagerAdapter,
        kind: SpiceMessageKind,
        block_hash: &CryptoHash,
    ) -> bool {
        self.decide(chain_store, epoch_manager, kind, block_hash, DropUnit::Message)
    }

    /// Whether one entry of a batched spice message should be processed. The other entries are
    /// unaffected. Drops count under [`metrics::SPICE_PRE_ACTIVATION_REQUEST_ENTRIES_DROPPED`],
    /// keeping entries out of the per-message metric.
    pub fn should_process_entry(
        &mut self,
        chain_store: &ChainStoreAdapter,
        epoch_manager: &dyn EpochManagerAdapter,
        kind: SpiceMessageKind,
        block_hash: &CryptoHash,
    ) -> bool {
        self.decide(chain_store, epoch_manager, kind, block_hash, DropUnit::Entry)
    }

    fn decide(
        &mut self,
        chain_store: &ChainStoreAdapter,
        epoch_manager: &dyn EpochManagerAdapter,
        kind: SpiceMessageKind,
        block_hash: &CryptoHash,
        unit: DropUnit,
    ) -> bool {
        let enabled = match spice_enabled_for_block(chain_store, block_hash) {
            Ok(true) => true,
            Ok(false) => match is_spice_activation_parent(epoch_manager, block_hash) {
                Ok(is_activation_parent) => is_activation_parent,
                // The block is on disk but its epoch info is not readable: we cannot
                // verify it as an activation parent, so treat it as plainly pre-spice.
                Err(err) => {
                    tracing::warn!(
                        target: "spice_activation",
                        ?err,
                        kind = kind.as_str(),
                        %block_hash,
                        "cannot verify activation parent for spice message, dropping",
                    );
                    false
                }
            },
            Err(_) => match spice_enabled_at_head(chain_store).and_then(|enabled| {
                Ok(enabled || spice_activation_imminent_at_head(chain_store, epoch_manager)?)
            }) {
                Ok(enabled) => enabled,
                // Neither the block nor the head is readable: we know nothing about
                // this chain, so we cannot claim spice is active on it.
                Err(err) => {
                    tracing::warn!(
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
            match unit {
                DropUnit::Message => metrics::SPICE_PRE_ACTIVATION_MESSAGES_DROPPED
                    .with_label_values(&[kind.as_str()])
                    .inc(),
                DropUnit::Entry => metrics::SPICE_PRE_ACTIVATION_REQUEST_ENTRIES_DROPPED
                    .with_label_values(&[kind.as_str()])
                    .inc(),
            }
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
    use super::{SpiceMessageGate, SpiceMessageKind, spice_enabled_at_head_on_startup};
    use crate::Chain;
    use crate::test_utils::get_chain_with_genesis;
    use near_async::time::Clock;
    use near_chain_configs::Genesis;
    use near_epoch_manager::test_utils::{
        default_reward_calculator, epoch_config_at_version, record_block_with_version, stake,
    };
    use near_epoch_manager::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
    use near_primitives::block::Tip;
    use near_primitives::hash::CryptoHash;
    use near_primitives::test_utils::{
        TestBlockBuilder, create_test_signer, pre_spice_protocol_version,
    };
    use near_primitives::types::{Balance, EpochId, ProtocolVersion};
    use near_primitives::version::ProtocolFeature;
    use near_store::adapter::{StoreAdapter as _, StoreUpdateAdapter as _};
    use near_store::test_utils::create_test_store;
    use near_store::{DBCol, HEAD_KEY};
    use num_rational::Rational32;
    use std::sync::Arc;
    use strum::IntoEnumIterator;

    const EPOCH_LENGTH: u64 = 2;
    const NUM_BLOCKS: usize = 12;

    /// A chain whose headers are all on disk as pre-spice headers, paired with an
    /// epoch manager that recorded the same hashes with every block after genesis
    /// voting `vote`. The gate reads spice-ness off the headers and activation off
    /// the epoch manager, so this exercises both of its inputs consistently.
    fn setup_gated_chain(vote: ProtocolVersion) -> (Chain, EpochManagerHandle, Vec<CryptoHash>) {
        let genesis_protocol_version = pre_spice_protocol_version();
        let mut genesis =
            Genesis::test_sharded(Clock::real(), vec!["test1".parse().unwrap()], 1, 1);
        genesis.config.epoch_length = EPOCH_LENGTH;
        genesis.config.transaction_validity_period = EPOCH_LENGTH * 2;
        genesis.config.protocol_version = genesis_protocol_version;
        let chain = get_chain_with_genesis(Clock::real(), genesis);

        let config = epoch_config_at_version(
            EPOCH_LENGTH,
            1,
            2,
            100,
            90,
            60,
            0,
            Rational32::new(0, 1),
            genesis_protocol_version,
        );
        let mut reward_calculator = default_reward_calculator();
        reward_calculator.genesis_protocol_version = genesis_protocol_version;
        let mut epoch_manager = EpochManager::new(
            create_test_store().epoch_store(),
            config,
            reward_calculator,
            vec![stake("test1".parse().unwrap(), Balance::from_yoctonear(1_000_000))],
        )
        .unwrap();

        let genesis_block = chain.get_block(&chain.genesis().hash().clone()).unwrap();
        record_block_with_version(
            &mut epoch_manager,
            CryptoHash::default(),
            *genesis_block.hash(),
            0,
            vec![],
            genesis_protocol_version,
        );

        let signer = Arc::new(create_test_signer("test1"));
        let mut hashes = vec![*genesis_block.hash()];
        let mut store_update = chain.chain_store().store().store_update();
        let mut prev_block = genesis_block;
        for height in 1..NUM_BLOCKS as u64 {
            let block =
                TestBlockBuilder::from_prev_block(Clock::real(), &prev_block, signer.clone())
                    .protocol_version(genesis_protocol_version)
                    .build();
            store_update.chain_store_update().set_block_header_only(block.header());
            record_block_with_version(
                &mut epoch_manager,
                *prev_block.hash(),
                *block.hash(),
                height,
                vec![],
                vote,
            );
            hashes.push(*block.hash());
            prev_block = block;
        }
        store_update.commit();
        (chain, epoch_manager.into_handle(), hashes)
    }

    /// Index of the last block whose epoch is pre-spice while its successor's is spice.
    fn activation_parent_index(epoch_manager: &EpochManagerHandle, hashes: &[CryptoHash]) -> usize {
        let versions: Vec<_> = hashes
            .iter()
            .map(|hash| {
                let epoch_id = epoch_manager.get_epoch_id(hash).unwrap();
                epoch_manager.get_epoch_protocol_version(&epoch_id).unwrap()
            })
            .collect();
        versions
            .iter()
            .zip(versions.iter().skip(1))
            .position(|(version, next_version)| {
                !ProtocolFeature::Spice.enabled(*version)
                    && ProtocolFeature::Spice.enabled(*next_version)
            })
            .unwrap()
    }

    /// Messages about the activation parent pass the gate; messages about other
    /// pre-spice blocks on the same chain still drop.
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn gate_accepts_messages_about_the_activation_parent() {
        let (chain, epoch_manager, hashes) =
            setup_gated_chain(ProtocolFeature::Spice.protocol_version());
        let chain_store = chain.chain_store().store().chain_store();
        let parent_index = activation_parent_index(&epoch_manager, &hashes);
        let mut gate = SpiceMessageGate::default();

        for kind in SpiceMessageKind::iter() {
            assert!(gate.should_process(&chain_store, &epoch_manager, kind, &hashes[parent_index]));
            assert!(gate.should_process_entry(
                &chain_store,
                &epoch_manager,
                kind,
                &hashes[parent_index]
            ));
            #[cfg(feature = "test_features")]
            assert_eq!(gate.dropped_count(kind), 0);
        }

        // An ordinary pre-spice block right before the boundary is still dropped.
        assert!(!gate.should_process(
            &chain_store,
            &epoch_manager,
            SpiceMessageKind::ChunkEndorsement,
            &hashes[parent_index - 1]
        ));
        #[cfg(feature = "test_features")]
        assert_eq!(gate.dropped_count(SpiceMessageKind::ChunkEndorsement), 1);
    }

    /// On a chain that never votes spice the gate keeps dropping everything,
    /// epoch-final blocks included.
    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn gate_still_drops_everything_on_a_pre_spice_chain() {
        let (chain, epoch_manager, hashes) = setup_gated_chain(pre_spice_protocol_version());
        let chain_store = chain.chain_store().store().chain_store();
        let mut gate = SpiceMessageGate::default();

        for (i, hash) in hashes.iter().enumerate() {
            assert!(
                !gate.should_process(
                    &chain_store,
                    &epoch_manager,
                    SpiceMessageKind::ChunkEndorsement,
                    hash
                ),
                "block at index {i}",
            );
        }
        #[cfg(feature = "test_features")]
        assert_eq!(gate.dropped_count(SpiceMessageKind::ChunkEndorsement), hashes.len() as u64);
    }

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

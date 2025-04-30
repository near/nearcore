use crate::types::RuntimeAdapter;
use crate::{Chain, ChainGenesis, ChainStore, ChainStoreAccess, ChainStoreUpdate};
use itertools::Itertools;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_primitives::bandwidth_scheduler::BandwidthRequests;
use near_primitives::block::{Block, Tip};
use near_primitives::chains::{MAINNET, TESTNET};
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::genesis::{
    genesis_block, genesis_chunks, prod_genesis_block, prod_genesis_chunks,
};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::ShardChunk;
use near_primitives::types::chunk_extra::{ChunkExtra, ChunkExtraV2};
use near_primitives::types::{EpochId, Gas, ShardId, StateRoot};
use near_primitives::version::PROD_GENESIS_PROTOCOL_VERSION;
use near_store::adapter::StoreUpdateAdapter;
use near_store::get_genesis_state_roots;
use near_vm_runner::logic::ProtocolVersion;
use node_runtime::bootstrap_congestion_info;

impl Chain {
    /// Builds genesis block and chunks from the current configuration obtained through the arguments.
    pub fn make_genesis_block(
        epoch_manager: &dyn EpochManagerAdapter,
        runtime_adapter: &dyn RuntimeAdapter,
        chain_genesis: &ChainGenesis,
        state_roots: Vec<CryptoHash>,
    ) -> Result<(Block, Vec<ShardChunk>), Error> {
        if chain_genesis.protocol_version == PROD_GENESIS_PROTOCOL_VERSION {
            Self::make_prod_genesis_block(epoch_manager, chain_genesis, state_roots)
        } else {
            Self::make_latest_genesis_block(
                epoch_manager,
                runtime_adapter,
                chain_genesis,
                state_roots,
            )
        }
    }

    fn make_latest_genesis_block(
        epoch_manager: &dyn EpochManagerAdapter,
        runtime_adapter: &dyn RuntimeAdapter,
        chain_genesis: &ChainGenesis,
        state_roots: Vec<CryptoHash>,
    ) -> Result<(Block, Vec<ShardChunk>), Error> {
        let congestion_infos =
            get_genesis_congestion_infos(epoch_manager, runtime_adapter, &state_roots)?;
        let genesis_chunks = genesis_chunks(
            state_roots,
            congestion_infos,
            &epoch_manager.shard_ids(&EpochId::default())?,
            chain_genesis.gas_limit,
            chain_genesis.height,
            chain_genesis.protocol_version,
        );

        let validator_stakes =
            epoch_manager.get_epoch_block_producers_ordered(&EpochId::default())?;
        let genesis_block = genesis_block(
            chain_genesis.protocol_version,
            genesis_chunks.iter().map(|chunk| chunk.cloned_header()).collect(),
            chain_genesis.time,
            chain_genesis.height,
            chain_genesis.min_gas_price,
            chain_genesis.total_supply,
            &validator_stakes,
        );

        Ok((genesis_block, genesis_chunks))
    }

    fn make_prod_genesis_block(
        epoch_manager: &dyn EpochManagerAdapter,
        chain_genesis: &ChainGenesis,
        state_roots: Vec<CryptoHash>,
    ) -> Result<(Block, Vec<ShardChunk>), Error> {
        let genesis_chunks = prod_genesis_chunks(
            state_roots,
            &epoch_manager.shard_ids(&EpochId::default())?,
            chain_genesis.gas_limit,
            chain_genesis.height,
        );

        let validator_stakes =
            epoch_manager.get_epoch_block_producers_ordered(&EpochId::default())?;
        let genesis_block = prod_genesis_block(
            genesis_chunks.iter().map(|chunk| chunk.cloned_header()).collect(),
            chain_genesis.time,
            chain_genesis.height,
            chain_genesis.min_gas_price,
            chain_genesis.total_supply,
            &validator_stakes,
        );

        // verify that the genesis block hash matches either mainnet or testnet
        let hash = genesis_block.hash().to_string();
        if &chain_genesis.chain_id == MAINNET {
            assert_eq!(hash, "EPnLgE7iEq9s7yTkos96M3cWymH5avBAPm3qx3NXqR8H");
        }

        if &chain_genesis.chain_id == TESTNET {
            assert_eq!(hash, "FWJ9kR6KFWoyMoNjpLXXGHeuiy7tEY6GmoFeCA5yuc6b");
        }

        Ok((genesis_block, genesis_chunks))
    }

    pub(crate) fn save_genesis_block_and_chunks(
        epoch_manager: &dyn EpochManagerAdapter,
        runtime_adapter: &dyn RuntimeAdapter,
        chain_store: &mut ChainStore,
        genesis: &Block,
        genesis_chunks: &[ShardChunk],
    ) -> Result<(), Error> {
        let state_roots = genesis_chunks.iter().map(|chunk| chunk.prev_state_root()).collect_vec();
        let mut store_update = chain_store.store_update();
        for chunk in genesis_chunks {
            store_update.save_chunk(chunk.clone());
        }
        store_update.merge(epoch_manager.add_validator_proposals(
            BlockInfo::from_header(
                genesis.header(),
                // genesis height is considered final
                genesis.header().height(),
            ),
            *genesis.header().random_value(),
        )?);
        store_update.save_block_header(genesis.header().clone())?;
        store_update.save_block(genesis.clone());
        Self::save_genesis_chunk_extras(&genesis, &state_roots, epoch_manager, &mut store_update)?;

        let block_head = Tip::from_header(genesis.header());
        let header_head = block_head.clone();
        store_update.save_head(&block_head)?;
        store_update.save_final_head(&header_head)?;

        // Set the root block of flat state to be the genesis block. Later, when we
        // init FlatStorages, we will read the from this column in storage, so it
        // must be set here.
        let flat_storage_manager = runtime_adapter.get_flat_storage_manager();
        let genesis_epoch_id = genesis.header().epoch_id();
        let mut tmp_store_update = store_update.store().store_update();
        for shard_uid in epoch_manager.get_shard_layout(genesis_epoch_id)?.shard_uids() {
            flat_storage_manager.set_flat_storage_for_genesis(
                &mut tmp_store_update.flat_store_update(),
                shard_uid,
                genesis.hash(),
                genesis.header().height(),
            )
        }
        store_update.merge(tmp_store_update);
        store_update.commit()?;
        tracing::info!(target: "chain", "Init: saved genesis: #{} {} / {:?}", block_head.height, block_head.last_block_hash, state_roots);
        Ok(())
    }

    fn create_genesis_chunk_extra(
        state_root: &StateRoot,
        gas_limit: Gas,
        congestion_info: Option<CongestionInfo>,
    ) -> ChunkExtra {
        ChunkExtra::new(
            state_root,
            CryptoHash::default(),
            vec![],
            0,
            gas_limit,
            0,
            congestion_info,
            BandwidthRequests::empty(),
        )
    }

    fn create_prod_genesis_chunk_extra(state_root: &StateRoot, gas_limit: Gas) -> ChunkExtra {
        ChunkExtra::V2(ChunkExtraV2 {
            state_root: *state_root,
            outcome_root: CryptoHash::default(),
            validator_proposals: vec![],
            gas_used: 0,
            gas_limit,
            balance_burnt: 0,
        })
    }

    pub fn genesis_chunk_extra(
        &self,
        shard_layout: &ShardLayout,
        shard_id: ShardId,
        congestion_info: Option<CongestionInfo>,
    ) -> Result<ChunkExtra, Error> {
        let shard_index = shard_layout.get_shard_index(shard_id)?;
        let state_root = *get_genesis_state_roots(&self.chain_store.store())?
            .ok_or_else(|| Error::Other("genesis state roots do not exist in the db".to_owned()))?
            .get(shard_index)
            .ok_or_else(|| {
                Error::Other(format!("genesis state root does not exist for shard id {shard_id} shard index {shard_index}"))
            })?;
        let gas_limit = self
            .genesis
            .chunks()
            .get(shard_index)
            .ok_or_else(|| {
                Error::Other(format!(
                    "genesis chunk does not exist for shard {shard_id} shard index {shard_index}"
                ))
            })?
            .gas_limit();
        Ok(Self::create_genesis_chunk_extra(&state_root, gas_limit, congestion_info))
    }

    /// Saves the `[ChunkExtra]`s for all shards in the genesis block.
    pub fn save_genesis_chunk_extras(
        genesis: &Block,
        state_roots: &Vec<CryptoHash>,
        epoch_manager: &dyn EpochManagerAdapter,
        store_update: &mut ChainStoreUpdate,
    ) -> Result<(), Error> {
        let genesis_protocol_version = genesis.header().latest_protocol_version();
        for (chunk_header, state_root) in genesis.chunks().iter_deprecated().zip(state_roots.iter())
        {
            let chunk_extra = if genesis_protocol_version == PROD_GENESIS_PROTOCOL_VERSION {
                Self::create_prod_genesis_chunk_extra(state_root, chunk_header.gas_limit())
            } else {
                let congestion_info = genesis
                    .block_congestion_info()
                    .get(&chunk_header.shard_id())
                    .map(|info| info.congestion_info);

                Self::create_genesis_chunk_extra(
                    state_root,
                    chunk_header.gas_limit(),
                    congestion_info,
                )
            };

            store_update.save_chunk_extra(
                genesis.hash(),
                &shard_id_to_uid(epoch_manager, chunk_header.shard_id(), &EpochId::default())?,
                chunk_extra,
            );
        }
        Ok(())
    }
}

/// This method calculates the congestion info for the genesis chunks. It uses
/// the congestion info bootstrapping logic. This method is just a wrapper
/// around the [`get_genesis_congestion_infos_impl`]. It logs an error if one
/// happens.
pub fn get_genesis_congestion_infos(
    epoch_manager: &dyn EpochManagerAdapter,
    runtime: &dyn RuntimeAdapter,
    state_roots: &Vec<CryptoHash>,
) -> Result<Vec<CongestionInfo>, Error> {
    get_genesis_congestion_infos_impl(epoch_manager, runtime, state_roots).map_err(|err| {
        tracing::error!(target: "chain", ?err, "Failed to get the genesis congestion infos.");
        err
    })
}

fn get_genesis_congestion_infos_impl(
    epoch_manager: &dyn EpochManagerAdapter,
    runtime: &dyn RuntimeAdapter,
    state_roots: &Vec<CryptoHash>,
) -> Result<Vec<CongestionInfo>, Error> {
    let genesis_prev_hash = CryptoHash::default();
    let genesis_epoch_id = epoch_manager.get_epoch_id_from_prev_block(&genesis_prev_hash)?;
    let genesis_protocol_version = epoch_manager.get_epoch_protocol_version(&genesis_epoch_id)?;
    let genesis_shard_layout = epoch_manager.get_shard_layout(&genesis_epoch_id)?;

    // Check we had already computed the congestion infos from the genesis state roots.
    if let Some(saved_infos) = near_store::get_genesis_congestion_infos(runtime.store())? {
        tracing::debug!(target: "chain", "Reading genesis congestion infos from database.");
        return Ok(saved_infos);
    }

    let mut new_infos = vec![];
    for (shard_index, &state_root) in state_roots.iter().enumerate() {
        let shard_id = genesis_shard_layout.get_shard_id(shard_index)?;
        let congestion_info = get_genesis_congestion_info(
            runtime,
            genesis_protocol_version,
            &genesis_prev_hash,
            shard_id,
            state_root,
        )?;
        new_infos.push(congestion_info);
    }

    // Store it in DB so that we can read it later, instead of recomputing from genesis state roots.
    // Note that this is necessary because genesis state roots will be garbage-collected and will not
    // be available, for example, when the node restarts later.
    tracing::debug!(target: "chain", "Saving genesis congestion infos to database.");
    let mut store_update = runtime.store().store_update();
    near_store::set_genesis_congestion_infos(&mut store_update, &new_infos);
    store_update.commit()?;

    Ok(new_infos)
}

fn get_genesis_congestion_info(
    runtime: &dyn RuntimeAdapter,
    protocol_version: ProtocolVersion,
    prev_hash: &CryptoHash,
    shard_id: ShardId,
    state_root: StateRoot,
) -> Result<CongestionInfo, Error> {
    // Get the view trie because it's possible that the chain is ahead of
    // genesis and doesn't have this block in flat state and memtrie.
    let trie = runtime.get_view_trie_for_shard(shard_id, prev_hash, state_root)?;
    let runtime_config = runtime.get_runtime_config(protocol_version);
    let congestion_info = bootstrap_congestion_info(&trie, runtime_config, shard_id)?;
    tracing::debug!(target: "chain", ?shard_id, ?state_root, ?congestion_info, "Computed genesis congestion info.");
    Ok(congestion_info)
}

#[cfg(test)]
mod test {
    use std::path::Path;
    use std::str::FromStr;

    use near_async::time::FakeClock;
    use near_chain_configs::test_genesis::{TestEpochConfigBuilder, TestGenesisBuilder};
    use near_epoch_manager::EpochManager;
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::Balance;
    use near_primitives::version::PROD_GENESIS_PROTOCOL_VERSION;
    use near_store::test_utils::create_test_store;
    use num_rational::Rational32;

    use crate::runtime::NightshadeRuntime;
    use crate::{Chain, ChainGenesis};

    #[test]
    fn test_prod_genesis_protocol_version_block_consistency() {
        // Create a genesis that closely resembles the mainnet genesis.
        // It should have the protocol version set as PROD_GENESIS_PROTOCOL_VERSION (29)
        // It's great to have parameters like epoch_length, gas_prices, etc. to be the same as mainnet genesis.
        let genesis = TestGenesisBuilder::new()
            .protocol_version(PROD_GENESIS_PROTOCOL_VERSION)
            .genesis_height(9820210)
            .genesis_time_from_clock(&FakeClock::default().clock())
            .epoch_length(43200)
            .gas_prices(1e9 as Balance, 1e22 as Balance)
            .gas_limit_one_petagas()
            .transaction_validity_period(86400)
            .max_inflation_rate(Rational32::new(0, 1))
            .protocol_reward_rate(Rational32::new(0, 1))
            .add_user_account_simple("alice".parse().unwrap(), 1_000_000)
            .build();

        let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
            .build_store_for_genesis_protocol_version();

        let store = create_test_store();
        let epoch_manager = EpochManager::new_arc_handle_from_epoch_config_store(
            store.clone(),
            &genesis.config,
            epoch_config_store,
        );
        let runtime =
            NightshadeRuntime::test(Path::new("."), store, &genesis.config, epoch_manager.clone());

        // Create the genesis block. Use a random state root.
        let (genesis_block, _) = Chain::make_genesis_block(
            epoch_manager.as_ref(),
            runtime.as_ref(),
            &ChainGenesis::new(&genesis.config),
            vec![CryptoHash::from_str("8EhZRfDTYujfZoUZtZ3eSMB9gJyFo5zjscR12dEcaxGU").unwrap()],
        )
        .unwrap();

        // In case this test fails, please make sure the changes do not change the structure of the genesis block.
        let hash = genesis_block.hash().to_string();
        assert_eq!(hash, "93CRibQrTXr4eGB1zBCdVqrCNS3jFwwmx8oQ6wFxsx5j");
    }
}

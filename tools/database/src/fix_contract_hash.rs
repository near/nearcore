use near_chain_configs::GenesisValidationMode;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::contract_distribution::CodeHash;
use near_primitives::stateless_validation::stored_chunk_state_transition_data::StoredChunkStateTransitionData;
use near_primitives::types::ShardId;
use near_primitives::utils::get_block_shard_id;
use near_store::{DBCol, NodeStorage};
use std::path::Path;

#[derive(clap::Args)]
pub(crate) struct FixContractHashCommand {
    /// Block hash to patch.
    #[clap(long)]
    block_hash: String,
    /// Shard id to patch.
    #[clap(long)]
    shard_id: u64,
    /// Contract code hash to add to contract_accesses.
    #[clap(long)]
    code_hash: String,
}

impl FixContractHashCommand {
    pub(crate) fn run(
        &self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let block_hash: CryptoHash = self.block_hash.parse().map_err(|e| anyhow::anyhow!("{e}"))?;
        let shard_id = ShardId::new(self.shard_id);
        let code_hash: CryptoHash = self.code_hash.parse().map_err(|e| anyhow::anyhow!("{e}"))?;

        let near_config = nearcore::config::load_config(home_dir, genesis_validation)?;
        let opener = NodeStorage::opener(
            home_dir,
            &near_config.config.store,
            near_config.config.cold_store.as_ref(),
            near_config.cloud_storage_context(),
        );

        let storage = opener.open()?;
        let store = storage.get_hot_store();

        let db_key = get_block_shard_id(&block_hash, shard_id);
        let data: StoredChunkStateTransitionData =
            store.get_ser(DBCol::StateTransitionData, &db_key).ok_or_else(|| {
                anyhow::anyhow!("no StateTransitionData for block {block_hash} shard {shard_id}")
            })?;

        println!("current contract_accesses: {:?}", data.contract_accesses());

        let new_access = CodeHash::from(code_hash);
        if data.contract_accesses().contains(&new_access) {
            println!("code hash {code_hash} already present, nothing to do");
            return Ok(());
        }

        let StoredChunkStateTransitionData::V1(mut v1) = data;
        v1.contract_accesses.push(new_access);

        let patched = StoredChunkStateTransitionData::V1(v1);
        println!("new contract_accesses: {:?}", patched.contract_accesses());

        let mut store_update = store.store_update();
        store_update.set_ser(DBCol::StateTransitionData, &db_key, &patched);
        store_update.commit();

        println!("patched StateTransitionData for block {block_hash} shard {shard_id}");
        Ok(())
    }
}

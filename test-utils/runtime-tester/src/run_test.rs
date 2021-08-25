use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use near_chain::{Block, ChainGenesis, Provenance, RuntimeAdapter};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_client_primitives::types::Error;
use near_crypto::InMemorySigner;
use near_primitives::transaction::{Action, SignedTransaction};
use near_primitives::types::{AccountId, BlockHeight, Nonce};
use near_store::test_utils::create_test_store;
use nearcore::{config::GenesisExt, NightshadeRuntime};

use near_primitives::runtime::config_store::RuntimeConfigStore;
use serde::{Deserialize, Serialize};

impl Scenario {
    pub fn from_file(path: &Path) -> io::Result<Scenario> {
        serde_json::from_str::<Scenario>(&std::fs::read_to_string(path)?).map_err(io::Error::from)
    }

    pub fn run(&self) -> Result<RuntimeStats, Error> {
        let genesis = Genesis::test(
            self.network_config.seeds.iter().map(|x| x.parse().unwrap()).collect(),
            1,
        );

        let mut env = TestEnv::new_with_runtime(
            ChainGenesis::from(&genesis),
            1,
            1,
            vec![Arc::new(NightshadeRuntime::new(
                Path::new("."),
                create_test_store(),
                &genesis,
                vec![],
                vec![],
                None,
                None,
                RuntimeConfigStore::default(),
            )) as Arc<dyn RuntimeAdapter>],
        );

        let mut last_block = env.clients[0].chain.get_block_by_height(0).unwrap().clone();

        let mut runtime_stats = RuntimeStats::default();

        for block in &self.blocks {
            let mut block_stats = BlockStats::at_height(block.height);

            for tx in &block.transactions {
                env.clients[0].process_tx(tx.to_signed_transaction(&last_block), false, false);
            }

            let start_time = Instant::now();

            last_block = env.clients[0]
                .produce_block(block.height)?
                .ok_or_else(|| Error::Other(String::from("No block has been produced")))?;
            env.process_block(0, last_block.clone(), Provenance::PRODUCED);

            block_stats.block_production_time = start_time.elapsed();

            runtime_stats.blocks_stats.push(block_stats);
        }

        Ok(runtime_stats)
    }
}

#[derive(Serialize, Deserialize)]
pub struct Scenario {
    pub network_config: NetworkConfig,
    pub blocks: Vec<BlockConfig>,
}

#[derive(Serialize, Deserialize)]
pub struct NetworkConfig {
    pub seeds: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct BlockConfig {
    pub height: BlockHeight,
    pub transactions: Vec<TransactionConfig>,
}

#[derive(Serialize, Deserialize)]
pub struct TransactionConfig {
    pub nonce: Nonce,
    pub signer_id: AccountId,
    pub receiver_id: AccountId,
    pub signer: InMemorySigner,
    pub actions: Vec<Action>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct RuntimeStats {
    pub blocks_stats: Vec<BlockStats>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct BlockStats {
    pub height: u64,
    pub block_production_time: Duration,
}

impl std::fmt::Debug for Scenario {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string_pretty(&self).unwrap())
    }
}

impl BlockConfig {
    pub fn at_height(height: BlockHeight) -> Self {
        Self { height, transactions: vec![] }
    }
}

impl TransactionConfig {
    fn to_signed_transaction(&self, last_block: &Block) -> SignedTransaction {
        SignedTransaction::from_actions(
            self.nonce,
            self.signer_id.clone(),
            self.receiver_id.clone(),
            &self.signer,
            self.actions.clone(),
            *last_block.hash(),
        )
    }
}

impl BlockStats {
    fn at_height(height: BlockHeight) -> Self {
        Self { height, block_production_time: Duration::default() }
    }
}

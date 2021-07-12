use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use near_chain::{Block, ChainGenesis, Provenance, RuntimeAdapter};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_crypto::InMemorySigner;
use near_primitives::transaction::{Action, SignedTransaction};
use near_primitives::types::{BlockHeight, Nonce};
use near_store::test_utils::create_test_store;
use nearcore::config::GenesisExt;

use serde::{Deserialize, Serialize};

pub fn run_from_json(path: &str) -> Result<RuntimeStats, io::Error> {
    let reader = io::BufReader::new(File::open(path)?);
    let scenario: Scenario = serde_json::from_reader::<io::BufReader<File>, Scenario>(reader)?;
    Ok(run(&scenario))
}

pub fn run(scenario: &Scenario) -> RuntimeStats {
    let genesis =
        Genesis::test(scenario.network_config.seeds.iter().map(|x| x.as_ref()).collect(), 1);

    let mut env = TestEnv::new_with_runtime(
        ChainGenesis::from(&genesis),
        1,
        1,
        vec![Arc::new(nearcore::NightshadeRuntime::new(
            Path::new("."),
            create_test_store(),
            &genesis,
            vec![],
            vec![],
            None,
            None,
        )) as Arc<dyn RuntimeAdapter>],
    );

    let mut last_block = env.clients[0].chain.get_block_by_height(0).unwrap().clone();

    let mut runtime_stats = RuntimeStats::default();

    for block in &scenario.blocks {
        let mut block_stats = BlockStats::at_height(block.height);

        for tx in &block.transactions {
            env.clients[0].process_tx(
                tx.to_signed_transaction(&last_block),
                false, false);
        };

        let start_time = Instant::now();

        last_block = env.clients[0].produce_block(block.height).unwrap().unwrap();
        env.process_block(0, last_block.clone(), Provenance::PRODUCED);

        block_stats.block_production_time = start_time.elapsed();

        runtime_stats.blocks_stats.push(block_stats);
    }

    runtime_stats
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
    pub signer_id: String,
    pub receiver_id: String,
    pub signer: InMemorySigner,
    pub actions: Vec<Action>,
}

#[derive(Serialize, Deserialize, Default)]
pub struct RuntimeStats {
    pub blocks_stats: Vec<BlockStats>,
}

#[derive(Serialize, Deserialize, Default)]
pub struct BlockStats {
    pub height: u64,
    pub block_production_time: Duration,
}

impl BlockConfig {
    pub fn at_height(height: BlockHeight) -> Self {
        Self {
            height,
            transactions: vec![],
        }
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
        Self {
            height,
            block_production_time: Duration::default(),
        }
    }
}

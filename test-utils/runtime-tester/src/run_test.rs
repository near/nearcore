use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use near_chain::{Block, ChainGenesis, Provenance};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_client_primitives::types::Error;
use near_crypto::InMemorySigner;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{Action, SignedTransaction};
use near_primitives::types::{AccountId, BlockHeight, Gas, Nonce};
use near_store::create_store;
use near_store::test_utils::create_test_store;
use nearcore::{config::GenesisExt, NightshadeRuntime};

use near_primitives::runtime::config_store::RuntimeConfigStore;
use serde::{Deserialize, Serialize};

pub struct ScenarioResult<T, E> {
    pub result: std::result::Result<T, E>,

    /// If scenario was run with on-disk storage (i.e. `use_in_memory_store` was
    /// `false`, the home directory of the node.
    pub homedir: Option<tempfile::TempDir>,
    pub env: TestEnv,
}

impl Scenario {
    pub fn from_file(path: &Path) -> io::Result<Scenario> {
        serde_json::from_str::<Scenario>(&std::fs::read_to_string(path)?).map_err(io::Error::from)
    }

    pub fn run(&self) -> ScenarioResult<RuntimeStats, Error> {
        let accounts: Vec<AccountId> =
            self.network_config.seeds.iter().map(|x| x.parse().unwrap()).collect();
        let clients = vec![accounts[0].clone()];
        let genesis = Genesis::test(accounts, 1);
        let mut runtime_config = near_primitives::runtime::config::RuntimeConfig::test();
        runtime_config.wasm_config.limit_config.max_total_prepaid_gas =
            self.runtime_config.max_total_prepaid_gas;
        let runtime_config_store = RuntimeConfigStore::with_one_config(runtime_config);

        let (tempdir, store) = if self.use_in_memory_store {
            (None, create_test_store())
        } else {
            let tempdir = tempfile::tempdir()
                .unwrap_or_else(|err| panic!("failed to create temporary directory: {}", err));
            let store = create_store(&nearcore::get_store_path(tempdir.path()));
            (Some(tempdir), store)
        };

        let mut env = TestEnv::builder(ChainGenesis::from(&genesis))
            .clients(clients.clone())
            .validators(clients)
            .runtime_adapters(vec![Arc::new(NightshadeRuntime::test_with_runtime_config_store(
                if let Some(tempdir) = &tempdir { tempdir.path() } else { Path::new(".") },
                store,
                &genesis,
                runtime_config_store,
            ))])
            .build();

        let result = self.process_blocks(&mut env);
        ScenarioResult { result: result, homedir: tempdir, env: env }
    }

    fn process_blocks(&self, env: &mut TestEnv) -> Result<RuntimeStats, Error> {
        let mut last_block = env.clients[0].chain.get_block_by_height(0).unwrap().clone();

        let mut runtime_stats = RuntimeStats::default();

        for block in &self.blocks {
            let mut block_stats = BlockStats::at_height(block.height);

            for tx in &block.transactions {
                let signed_tx = tx.to_signed_transaction(&last_block);
                block_stats.tx_hashes.push(signed_tx.get_hash());
                env.clients[0].process_tx(signed_tx, false, false);
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
    pub runtime_config: RuntimeConfig,
    pub blocks: Vec<BlockConfig>,
    pub use_in_memory_store: bool,
}

#[derive(Serialize, Deserialize)]
pub struct NetworkConfig {
    pub seeds: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct RuntimeConfig {
    pub max_total_prepaid_gas: Gas,
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
    pub tx_hashes: Vec<CryptoHash>,
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
        Self { height, block_production_time: Duration::default(), tx_hashes: vec![] }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::path::Path;
    use std::time::{Duration, Instant};

    use log::info;
    use near_logger_utils::init_test_logger;

    #[test]
    #[ignore]
    fn test_scenario_json() {
        init_test_logger();
        let path = Path::new("./fuzz/scenario.json");

        let scenario = Scenario::from_file(path).expect("Failed to deserialize the scenario file.");
        let starting_time = Instant::now();
        let runtime_stats = scenario.run().result.expect("Error while running scenario");
        info!("Time to run: {:?}", starting_time.elapsed());
        for block_stats in runtime_stats.blocks_stats {
            if block_stats.block_production_time > Duration::from_secs(1) {
                info!(
                    "Time to produce block {} is {:?}",
                    block_stats.height, block_stats.block_production_time
                );
            }
        }
    }
}

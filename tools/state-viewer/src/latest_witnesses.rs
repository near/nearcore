use std::rc::Rc;

use clap::Parser;
use near_chain::ChainStore;
use near_primitives::types::EpochId;
use near_store::Store;
use nearcore::NearConfig;

#[derive(Parser)]
pub struct LatestWitnessesCmd {
    /// Block height
    #[arg(long)]
    height: Option<u64>,

    /// Shard id
    #[arg(long)]
    shard_id: Option<u64>,

    /// Epoch Id
    #[arg(long)]
    epoch_id: Option<EpochId>,

    /// Pretty-print using the "{:#?}" formatting.
    #[arg(long)]
    pretty: bool,

    /// Print the raw &[u8], can be pasted into rust code
    #[arg(long)]
    binary: bool,
}

impl LatestWitnessesCmd {
    pub(crate) fn run(&self, near_config: NearConfig, store: Store) {
        let chain_store =
            Rc::new(ChainStore::new(store, near_config.genesis.config.genesis_height, false));

        let witnesses = chain_store
            .get_latest_witnesses(self.height, self.shard_id, self.epoch_id.clone())
            .unwrap();
        println!("Found {} witnesses:", witnesses.len());
        for (i, witness) in witnesses.iter().enumerate() {
            println!(
                "#{} (height: {}, shard_id: {}, epoch_id: {:?}):",
                i,
                witness.chunk_header.height_created(),
                witness.chunk_header.shard_id(),
                witness.epoch_id
            );
            if self.pretty {
                println!("{:#?}", witness);
            } else if self.binary {
                println!("{:?}", borsh::to_vec(witness).unwrap());
            } else {
                println!("{:?}", witness);
            }
        }
    }
}

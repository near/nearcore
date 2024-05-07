use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockHeight, Gas};
use near_store::{DBCol, NodeStorage, Store};
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use std::path::Path;

#[derive(clap::Parser)]
pub(crate) struct HighLoadStatsCommand {
    #[clap(long)]
    start_height: BlockHeight,
    #[clap(long)]
    end_height: BlockHeight,
    #[clap(long)]
    account: AccountId,
    #[clap(long)]
    num_threads: usize,
}

struct BlockStats {
    pub height: BlockHeight,
    pub chunk_mask: Vec<bool>,
    pub gas_used: Vec<Gas>,
    pub gas_used_by_account: Vec<Gas>,
    pub tx_by_account: Vec<usize>,
    pub receipts_by_account: Vec<usize>,
}

fn add_to_line(line: &mut String, new_string: String) {
    *line = [line.clone(), new_string].join("\t");
}

fn push_stats<T: Default + Clone + std::fmt::Debug>(
    line: &mut String,
    stat_vec: &Vec<T>,
    shard_num: usize,
) {
    for i in 0..shard_num {
        let mut stat = T::default();
        stat_vec.get(i).map(|val| stat = (*val).clone());
        add_to_line(line, format!("{:?}", stat))
    }
}

fn push_header(header_parts: &mut Vec<String>, name: String, num_shards: usize) {
    for i in 0..num_shards {
        header_parts.push(format!("{name}_{i}"));
    }
}

impl BlockStats {
    pub fn print(&self) {
        let mut stat_line = format!("{}", self.height);
        let shard_num = 4;
        push_stats(&mut stat_line, &self.chunk_mask, shard_num);
        push_stats(&mut stat_line, &self.gas_used, shard_num);
        push_stats(&mut stat_line, &self.gas_used_by_account, shard_num);
        push_stats(&mut stat_line, &self.tx_by_account, shard_num);
        push_stats(&mut stat_line, &self.receipts_by_account, shard_num);
        println!("{stat_line}");
    }

    pub fn print_header() {
        let mut header_parts = vec!["height".to_string()];
        let num_shards = 4;
        push_header(&mut header_parts, "has_chunk".to_string(), num_shards);
        push_header(&mut header_parts, "gas_used".to_string(), num_shards);
        push_header(&mut header_parts, "gas_used_by_account".to_string(), num_shards);
        push_header(&mut header_parts, "tx_by_account".to_string(), num_shards);
        push_header(&mut header_parts, "receipts_by_account".to_string(), num_shards);

        let header = header_parts.join("\t");
        println!("{header}");
    }
}

impl HighLoadStatsCommand {
    pub(crate) fn run(&self, home: &Path) -> anyhow::Result<()> {
        let near_config = nearcore::config::Config::from_file_skip_validation(
            &home.join(nearcore::config::CONFIG_FILENAME),
        )?;
        let opener = NodeStorage::opener(
            home,
            near_config.archive,
            &near_config.store,
            near_config.cold_store.as_ref(),
        );
        let storage = opener.open()?;
        let store = std::sync::Arc::new(
            storage.get_split_store().unwrap_or_else(|| storage.get_hot_store()),
        );

        let num_threads = self.num_threads;
        let account_id = self.account.clone();

        let mut stats = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .map_err(|_| anyhow::anyhow!("Failed to create rayon pool"))?
            .install(|| -> anyhow::Result<Vec<BlockStats>> {
                (self.start_height..=self.end_height)
                    .into_par_iter() // Process every cold column as a separate task in thread pool in parallel.
                    // Copy column to cold db.
                    .map(|height: BlockHeight| -> anyhow::Result<Vec<BlockStats>> {
                        let stats_per_height = Self::collect_stats_per_height(
                            account_id.clone(),
                            height,
                            store.clone(),
                        )?;
                        if let Some(stats) = stats_per_height {
                            Ok(vec![stats])
                        } else {
                            Ok(vec![])
                        }
                    })
                    // Return first found error, or Ok(())
                    .reduce(
                        || Ok(vec![]), // Ok(vec![]) by default
                        // First found Err, or Ok(concat of two vectors)
                        |left, right| -> anyhow::Result<Vec<BlockStats>> {
                            match (left, right) {
                                (Err(e), _) => Err(e),
                                (Ok(_), Err(e)) => Err(e),
                                (Ok(mut left), Ok(mut right)) => {
                                    left.append(&mut right);
                                    Ok(left)
                                }
                            }
                        },
                    )
            })?;
        stats.sort_by(|a, b| a.height.cmp(&b.height));

        BlockStats::print_header();
        for stat in stats.into_iter() {
            stat.print();
        }

        Ok(())
    }

    fn collect_stats_per_height(
        target_account_id: AccountId,
        height: BlockHeight,
        store: std::sync::Arc<Store>,
    ) -> anyhow::Result<Option<BlockStats>> {
        let height_key = height.to_le_bytes();
        let block_hash_vec = store.get(DBCol::BlockHeight, &height_key)?;
        if block_hash_vec.is_none() {
            return Ok(None);
        }
        let block_hash_vec = block_hash_vec.unwrap();
        let block_hash_key = block_hash_vec.as_slice();
        let block = store.get_ser::<Block>(DBCol::Block, &block_hash_key)?.ok_or_else(|| {
            anyhow::anyhow!("Block header not found for {height} with {block_hash_vec:?}")
        })?;

        let mut gas_used = vec![0; 4];
        let mut gas_used_by_account = vec![0; 4];
        let mut tx_by_account = vec![0; 4];
        let mut receipts_by_account = vec![0; 4];

        for chunk_header in block.chunks().iter() {
            let shard_id = chunk_header.shard_id();

            // let mut gas_usage_in_shard = GasUsageInShard::new();

            let outcome_ids = store
                .get_ser::<Vec<CryptoHash>>(
                    DBCol::OutcomeIds,
                    &near_primitives::utils::get_block_shard_id(block.hash(), shard_id),
                )?
                .unwrap_or_default();

            for outcome_id in outcome_ids {
                let outcome = store
                    .get_ser::<near_primitives::transaction::ExecutionOutcomeWithProof>(
                        DBCol::TransactionResultForBlock,
                        &near_primitives::utils::get_outcome_id_block_hash(
                            &outcome_id,
                            block.hash(),
                        ),
                    )?
                    .ok_or_else(|| {
                        anyhow::anyhow!("no outcome found for {outcome_id:?} at {height}")
                    })?
                    .outcome;

                let (account_id, gas_used_by_tx) = (outcome.executor_id, outcome.gas_burnt);
                gas_used[shard_id as usize] += gas_used_by_tx;
                if account_id == target_account_id {
                    gas_used_by_account[shard_id as usize] += gas_used_by_tx;
                    tx_by_account[shard_id as usize] += 1;
                    receipts_by_account[shard_id as usize] += outcome.receipt_ids.len();
                }
            }
        }

        return Ok(Some(BlockStats {
            height,
            chunk_mask: block.header().chunk_mask().to_vec(),
            gas_used,
            gas_used_by_account,
            tx_by_account,
            receipts_by_account,
        }));
    }
}

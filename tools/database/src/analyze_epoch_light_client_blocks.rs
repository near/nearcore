use crate::utils::open_rocksdb;
use clap::Parser;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockHeight, EpochId};
use near_store::db::Database;
use near_store::light_client_block::StoredLightClientBlock;
use near_store::{DBCol, Mode};
use nearcore::migrations::{LightClientRowLayout, read_light_client_row};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Reports which layout every `DBCol::EpochLightClientBlocks` row is in, without
/// writing anything. This is the dry run of the version 50 to 51 migration: it reads
/// the rows with the same code the migration uses, so a clean report means the
/// migration converts every row.
///
/// The database is opened read-only and directly, so neither the database version
/// check nor any migration runs.
#[derive(Parser)]
pub(crate) struct AnalyzeEpochLightClientBlocksCommand {
    /// Directory to write the rows the migration stops on, one file per epoch id, for
    /// decoding.
    #[arg(long)]
    rejected_rows_dir: Option<PathBuf>,
}

/// The rows one layout accounts for. The block heights say which era the layout
/// covers; the epoch id keys are hashes and carry no order.
#[derive(Default)]
struct LayoutCounts {
    rows: usize,
    lowest_height: Option<BlockHeight>,
    highest_height: Option<BlockHeight>,
}

impl AnalyzeEpochLightClientBlocksCommand {
    pub(crate) fn run(&self, home: &Path) -> anyhow::Result<()> {
        let db = open_rocksdb(home, Mode::ReadOnly)?;

        let mut counts: BTreeMap<LightClientRowLayout, LayoutCounts> = BTreeMap::new();
        let mut rejected = Vec::new();
        let mut total = 0;

        for (key, value) in db.iter(DBCol::EpochLightClientBlocks) {
            total += 1;
            let epoch_id = CryptoHash::try_from(key.as_ref()).map(EpochId).map_err(|err| {
                anyhow::anyhow!("epoch light client block key is not a hash: {err}")
            })?;

            let (layout, stored) = match read_light_client_row(&epoch_id, &value) {
                Ok(reading) => reading,
                Err(err) => {
                    rejected.push((epoch_id, value.to_vec(), err));
                    continue;
                }
            };
            let StoredLightClientBlock::V1(stored) = stored;
            let height = stored.inner_lite.height;
            let entry = counts.entry(layout).or_default();
            entry.rows += 1;
            entry.lowest_height = Some(entry.lowest_height.unwrap_or(height).min(height));
            entry.highest_height = Some(entry.highest_height.unwrap_or(height).max(height));
        }

        println!("{total} rows in {}", DBCol::EpochLightClientBlocks);
        println!();
        println!("rows the migration converts, by layout:");
        for (layout, count) in &counts {
            println!(
                "  {:>8}  heights {}..={}  {layout:?}",
                count.rows,
                count.lowest_height.unwrap(),
                count.highest_height.unwrap(),
            );
        }

        println!();
        println!("rows the migration stops on: {}", rejected.len());
        for (_, value, err) in &rejected {
            println!("  {err} ({} bytes)", value.len());
        }

        if let Some(dir) = &self.rejected_rows_dir {
            std::fs::create_dir_all(dir)?;
            for (epoch_id, value, _) in &rejected {
                std::fs::write(dir.join(epoch_id.0.to_string()), value)?;
            }
            println!("wrote {} of them to {}", rejected.len(), dir.display());
        }

        if rejected.is_empty() {
            println!();
            println!("the migration converts every row in this database");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::AnalyzeEpochLightClientBlocksCommand;
    use near_primitives::hash::CryptoHash;
    use near_store::db::{DBTransaction, Database, RocksDB};
    use near_store::light_client_block::{
        StoredBlockHeaderInnerLiteV1, StoredLightClientBlock, StoredLightClientBlockV1,
    };
    use near_store::{DBCol, Mode, StoreConfig, Temperature};

    fn versioned_row() -> Vec<u8> {
        borsh::to_vec(&StoredLightClientBlock::V1(StoredLightClientBlockV1 {
            prev_block_hash: CryptoHash::hash_bytes(b"prev"),
            next_block_inner_hash: CryptoHash::hash_bytes(b"next"),
            inner_lite: StoredBlockHeaderInnerLiteV1 {
                height: 42,
                epoch_id: CryptoHash::hash_bytes(b"epoch"),
                next_epoch_id: CryptoHash::hash_bytes(b"next_epoch"),
                prev_state_root: CryptoHash::hash_bytes(b"state"),
                outcome_root: CryptoHash::hash_bytes(b"outcome"),
                timestamp: 7,
                timestamp_nanosec: 7,
                next_bp_hash: CryptoHash::hash_bytes(b"bp"),
                block_merkle_root: CryptoHash::hash_bytes(b"merkle"),
                chunk_execution_root: None,
            },
            inner_rest_hash: CryptoHash::hash_bytes(b"rest"),
            next_bps: None,
            approvals_after_next: vec![None],
        }))
        .unwrap()
    }

    #[test]
    fn writes_rejected_rows_to_a_directory() {
        let home = tempfile::tempdir().unwrap();
        let config = nearcore::config::Config::default();
        std::fs::write(
            home.path().join(nearcore::config::CONFIG_FILENAME),
            serde_json::to_string(&config).unwrap(),
        )
        .unwrap();

        let db_path = home.path().join("data");
        let store_config = StoreConfig::default();
        {
            let db =
                RocksDB::open(&db_path, &store_config, Mode::Create, Temperature::Hot).unwrap();
            let mut transaction = DBTransaction::new();
            transaction.set(
                DBCol::EpochLightClientBlocks,
                CryptoHash::default().as_bytes().to_vec(),
                b"not a light client block".to_vec(),
            );
            transaction.set(
                DBCol::EpochLightClientBlocks,
                CryptoHash::hash_bytes(b"epoch").as_bytes().to_vec(),
                versioned_row(),
            );
            db.write(transaction);
        }

        let rejected_rows_dir = home.path().join("rejected");
        let command = AnalyzeEpochLightClientBlocksCommand {
            rejected_rows_dir: Some(rejected_rows_dir.clone()),
        };
        command.run(home.path()).unwrap();

        let dumped: Vec<_> = std::fs::read_dir(&rejected_rows_dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(dumped, vec![CryptoHash::default().to_string()]);
        assert_eq!(
            std::fs::read(rejected_rows_dir.join(CryptoHash::default().to_string())).unwrap(),
            b"not a light client block",
        );
    }
}

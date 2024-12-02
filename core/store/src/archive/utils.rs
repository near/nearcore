use crate::cold_storage::ColdMigrationStore;
use crate::db::Database;
use borsh::BorshDeserialize;
use near_primitives::{
    block::{BlockHeader, Tip},
    types::BlockHeight,
};
use std::{collections::HashMap, io, sync::Arc};
use strum::IntoEnumIterator;

use crate::{
    db::{ColdDB, DBTransaction},
    DBCol, Store, COLD_HEAD_KEY, HEAD_KEY,
};

/// Creates a transaction to write head to the archival storage.
pub(crate) fn set_head_tx(tip: &Tip) -> io::Result<DBTransaction> {
    let mut tx = DBTransaction::new();
    // TODO: Write these to the same file for external storage.
    tx.set(DBCol::BlockMisc, HEAD_KEY.to_vec(), borsh::to_vec(&tip)?);
    tx.set(DBCol::BlockMisc, COLD_HEAD_KEY.to_vec(), borsh::to_vec(&tip)?);
    Ok(tx)
}

/// Reads the head from the Cold DB.
pub(crate) fn read_cold_head(cold_db: &Arc<ColdDB>) -> io::Result<Option<Tip>> {
    cold_db
        .get_raw_bytes(DBCol::BlockMisc, HEAD_KEY)?
        .as_deref()
        .map(Tip::try_from_slice)
        .transpose()
}

/// Saves the cold head in the hot DB store.
pub(crate) fn save_cold_head(hot_store: &Store, tip: &Tip) -> io::Result<()> {
    let mut transaction = DBTransaction::new();
    transaction.set(DBCol::BlockMisc, COLD_HEAD_KEY.to_vec(), borsh::to_vec(&tip)?);
    hot_store.storage.write(transaction)
}

/// Returns the `Tip` at the given block height.
pub(crate) fn get_tip_at_height(hot_store: &Store, height: &BlockHeight) -> io::Result<Tip> {
    let height_key = height.to_le_bytes();
    let block_hash_key =
        hot_store.get_or_err_for_cold(DBCol::BlockHeight, &height_key)?.as_slice().to_vec();
    let tip_header =
        &hot_store.get_ser_or_err_for_cold::<BlockHeader>(DBCol::BlockHeader, &block_hash_key)?;
    let tip = Tip::from_header(tip_header);
    Ok(tip)
}

/// Builds a map from column to relative path to store (key, value) pairs for the respective column.
///
/// If `container` is present, prepends it to the path for each column.
pub(crate) fn map_cold_column_to_path(
    container: Option<&std::path::Path>,
) -> HashMap<DBCol, std::path::PathBuf> {
    DBCol::iter()
        .filter_map(|col| {
            // BlockMisc is managed in the cold/archival storage but not marked as cold column.
            if col.is_cold() || col == DBCol::BlockMisc {
                let dirname = cold_column_dirname(col)
                    .unwrap_or_else(|| panic!("Cold column {} has no entry for dirname", col));
                let path = container
                    .map_or_else(|| dirname.into(), |c| c.join(std::path::Path::new(dirname)));
                Some((col, path))
            } else {
                None
            }
        })
        .collect()
}

/// Maps each column to a string to be used as a directory name in the storage.
/// This mapping is partial and only supports cold DB columns and `DBCol::BlockMisc`.
fn cold_column_dirname(col: DBCol) -> Option<&'static str> {
    let dirname = match col {
        DBCol::BlockMisc => "BlockMisc",
        DBCol::Block => "Block",
        DBCol::BlockExtra => "BlockExtra",
        DBCol::BlockInfo => "BlockInfo",
        DBCol::BlockPerHeight => "BlockPerHeight",
        DBCol::ChunkExtra => "ChunkExtra",
        DBCol::ChunkHashesByHeight => "ChunkHashesByHeight",
        DBCol::Chunks => "Chunks",
        DBCol::IncomingReceipts => "IncomingReceipts",
        DBCol::NextBlockHashes => "NextBlockHashes",
        DBCol::OutcomeIds => "OutcomeIds",
        DBCol::OutgoingReceipts => "OutgoingReceipts",
        DBCol::Receipts => "Receipts",
        DBCol::State => "State",
        DBCol::StateChanges => "StateChanges",
        DBCol::StateChangesForSplitStates => "StateChangesForSplitStates",
        DBCol::StateHeaders => "StateHeaders",
        DBCol::TransactionResultForBlock => "TransactionResultForBlock",
        DBCol::Transactions => "Transactions",
        DBCol::StateShardUIdMapping => "StateShardUIdMapping",
        _ => "",
    };
    dirname.is_empty().then_some(dirname)
}

#[cfg(test)]
mod tests {
    use super::cold_column_dirname;
    use crate::DBCol;
    use strum::IntoEnumIterator;

    /// Tets that all cold-DB columns and BlockMisc have mappings in `cold_column_dirname` function.
    #[test]
    fn test_cold_column_dirname() {
        for col in DBCol::iter() {
            if col.is_cold() || col == DBCol::BlockMisc {
                assert!(
                    cold_column_dirname(col).is_some(),
                    "Cold column {} has no entry for dirname",
                    col
                );
            }
        }
    }
}

/// Set of tests over the 'Database' interface, that we can run over multiple implementations
/// to make sure that they are working correctly.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        db::{DBTransaction, Database, TestDB},
        DBCol, NodeStorage,
    };

    // Returns test & rocksDB databases.
    fn test_and_rocksdb() -> Vec<Arc<dyn Database>> {
        let (_tmp_dir, opener) = NodeStorage::test_opener();
        let store = opener.open().unwrap().get_hot_store();
        vec![TestDB::new(), store.storage.clone()]
    }

    /// Tests the behavior of the iterators. Iterators don't really work over cold storage, so we're not testing it here.
    #[test]
    fn test_db_iter() {
        for db in test_and_rocksdb() {
            let mut transaction = DBTransaction::new();
            transaction.insert(DBCol::Block, "a".into(), "val_a".into());
            transaction.insert(DBCol::Block, "aa".into(), "val_aa".into());
            transaction.insert(DBCol::Block, "aa1".into(), "val_aa1".into());
            transaction.insert(DBCol::Block, "bb1".into(), "val_bb1".into());
            transaction.insert(DBCol::Block, "cc1".into(), "val_cc1".into());
            db.write(transaction).unwrap();

            let keys: Vec<_> = db
                .iter(DBCol::Block)
                .map(|data| String::from_utf8(data.unwrap().0.to_vec()).unwrap())
                .collect();
            assert_eq!(keys, vec!["a", "aa", "aa1", "bb1", "cc1"]);

            let keys: Vec<_> = db
                .iter_range(DBCol::Block, Some("aa".as_bytes()), Some("bb1".as_bytes()))
                .map(|data| String::from_utf8(data.unwrap().0.to_vec()).unwrap())
                .collect();
            assert_eq!(keys, vec!["aa", "aa1"]);
        }
    }
}

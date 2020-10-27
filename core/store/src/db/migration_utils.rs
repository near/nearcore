use rocksdb::IteratorMode;

use crate::db::RocksDB;
use crate::DBCol;

impl RocksDB {
    /// Clears the column using delete_range_cf()
    pub fn clear_column(&self, column: DBCol) {
        let cf_handle = unsafe { &*self.cfs[column as usize] };

        let opt_first = self.db.iterator(IteratorMode::Start).next();
        let opt_last = self.db.iterator(IteratorMode::End).next();
        assert_eq!(opt_first.is_some(), opt_last.is_some());

        if let Some((min_key, _)) = opt_first {
            if let Some((max_key, _)) = opt_last {
                if min_key != max_key {
                    self.db
                        .delete_range_cf(cf_handle, &min_key, &max_key)
                        .expect("clear column failed");
                }
                self.db.delete_cf(cf_handle, max_key).expect("clear column failed");
            }
        }

        assert!(self.db.iterator(IteratorMode::Start).next().is_none());
    }
}

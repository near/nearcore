use itertools::EitherOrBoth;
use std::cmp::Ordering;
use std::io;
use std::sync::Arc;

use crate::db::{DBIterator, DBIteratorItem, DBSlice, DBTransaction, Database, StoreStatistics};
use crate::DBCol;

#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
pub enum ReadOrder {
    ReadDBFirst,
    WriteDBFirst,
}

pub struct MixedDB {
    /// Read-only DB.
    read_db: Arc<dyn Database>,
    /// DB for writes.
    write_db: Arc<dyn Database>,
    /// order of data lookup.
    read_order: ReadOrder,
}

impl MixedDB {
    #[allow(dead_code)]
    pub fn new(
        read_db: Arc<dyn Database>,
        write_db: Arc<dyn Database>,
        read_order: ReadOrder,
    ) -> Arc<Self> {
        return Arc::new(MixedDB { read_db, write_db, read_order });
    }

    fn first_db(&self) -> &Arc<dyn Database> {
        match self.read_order {
            ReadOrder::ReadDBFirst => &self.read_db,
            ReadOrder::WriteDBFirst => &self.write_db,
        }
    }

    fn second_db(&self) -> &Arc<dyn Database> {
        match self.read_order {
            ReadOrder::ReadDBFirst => &self.write_db,
            ReadOrder::WriteDBFirst => &self.read_db,
        }
    }

    /// The cmp function for the DBIteratorItems.
    ///
    /// Note that this does not implement total ordering because there isn't a
    /// clear way to compare errors to errors or errors to values. Instead it
    /// implements total order on values but always compares the error on the
    /// left as lesser. This isn't even partial order. It is fine for merging
    /// lists but should not be used for anything more complex like sorting.
    fn db_iter_item_cmp(a: &DBIteratorItem, b: &DBIteratorItem) -> Ordering {
        match (a, b) {
            // Always put errors first.
            (Err(_), _) => Ordering::Less,
            (_, Err(_)) => Ordering::Greater,
            // When comparing two (key, value) paris only compare the keys.
            // - values in hot and cold may differ in rc but they are still the same
            // - values written to cold should be immutable anyway
            // - values writted to cold should be final
            (Ok((a_key, _)), Ok((b_key, _))) => Ord::cmp(a_key, b_key),
        }
    }

    /// Returns merge iterator for the given two DBIterators. The returned
    /// iterator will contain unique and sorted items from both input iterators.
    ///
    /// All errors from both inputs will be returned.
    fn merge_iter<'a>(a: DBIterator<'a>, b: DBIterator<'a>) -> DBIterator<'a> {
        // Merge the two iterators using the cmp function. The result will be an
        // iter of EitherOrBoth.
        let iter = itertools::merge_join_by(a, b, Self::db_iter_item_cmp);
        // Flatten the EitherOrBoth while discarding duplicates. Each input
        // iterator should only contain unique and sorted items. If any item is
        // present in both iterors before the merge, it will get mapped to a
        // EitherOrBoth::Both in the merged iter. Pick either and discard the
        // other - this will guarantee uniqueness of the resulting iterator.
        let iter = iter.map(|item| match item {
            EitherOrBoth::Both(item, _) => item,
            EitherOrBoth::Left(item) => item,
            EitherOrBoth::Right(item) => item,
        });
        Box::new(iter)
    }
}

impl Database for MixedDB {
    fn get_raw_bytes(&self, col: DBCol, key: &[u8]) -> io::Result<Option<DBSlice<'_>>> {
        if let Some(first_result) = self.first_db().get_raw_bytes(col, key)? {
            return Ok(Some(first_result));
        }
        self.second_db().get_raw_bytes(col, key)
    }

    fn get_with_rc_stripped(&self, col: DBCol, key: &[u8]) -> io::Result<Option<DBSlice<'_>>> {
        assert!(col.is_rc());

        if let Some(first_result) = self.first_db().get_with_rc_stripped(col, key)? {
            return Ok(Some(first_result));
        }
        self.second_db().get_with_rc_stripped(col, key)
    }

    fn iter<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        Self::merge_iter(self.read_db.iter(col), self.write_db.iter(col))
    }

    /// TODO(posvyatokum): None of the iteration works for this DB
    fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> DBIterator<'a> {
        return Self::merge_iter(
            self.read_db.iter_prefix(col, key_prefix),
            self.write_db.iter_prefix(col, key_prefix),
        );
    }

    /// TODO(posvyatokum): None of the iteration works for this DB
    fn iter_range<'a>(
        &'a self,
        col: DBCol,
        lower_bound: Option<&[u8]>,
        upper_bound: Option<&[u8]>,
    ) -> DBIterator<'a> {
        return Self::merge_iter(
            self.read_db.iter_range(col, lower_bound, upper_bound),
            self.write_db.iter_range(col, lower_bound, upper_bound),
        );
    }

    /// TODO(posvyatokum): None of the iteration works for this DB
    fn iter_raw_bytes<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        return Self::merge_iter(
            self.read_db.iter_raw_bytes(col),
            self.write_db.iter_raw_bytes(col),
        );
    }

    /// The split db, in principle, should be read only and only used in view client.
    /// However the view client *does* write to the db in order to update cache.
    /// Hence we need to allow writing to the split db but only write to the hot db.
    fn write(&self, batch: DBTransaction) -> io::Result<()> {
        self.write_db.write(batch)
    }

    fn flush(&self) -> io::Result<()> {
        self.write_db.flush()
    }

    fn compact(&self) -> io::Result<()> {
        self.write_db.compact()
    }

    fn get_store_statistics(&self) -> Option<StoreStatistics> {
        self.write_db.get_store_statistics()
    }

    fn create_checkpoint(
        &self,
        path: &std::path::Path,
        columns_to_keep: Option<&[DBCol]>,
    ) -> anyhow::Result<()> {
        self.write_db.create_checkpoint(path, columns_to_keep)
    }
}

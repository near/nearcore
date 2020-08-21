use std::io::Cursor;
use std::marker::PhantomPinned;

use byteorder::{LittleEndian, ReadBytesExt};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, MergeOperands, Options, DB};
use strum::IntoEnumIterator;

use crate::db::{rocksdb_column_options, rocksdb_options, DBError, RocksDB};
use crate::DBCol;

fn refcount_merge_v6(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &mut MergeOperands,
) -> Option<Vec<u8>> {
    let mut result = vec![];
    if let Some(val) = existing_val {
        merge_refcounted_records_v6(&mut result, val);
    }
    for val in operands {
        merge_refcounted_records_v6(&mut result, val);
    }
    Some(result)
}

fn vec_to_rc(bytes: &[u8]) -> i32 {
    let mut cursor = Cursor::new(&bytes[bytes.len() - 4..]);
    cursor.read_i32::<LittleEndian>().unwrap()
}

fn merge_refcounted_records_v6(result: &mut Vec<u8>, val: &[u8]) {
    if val.is_empty() {
        return;
    }
    let add_rc = vec_to_rc(val);
    if !result.is_empty() {
        let result_rc = vec_to_rc(result) + add_rc;

        debug_assert_eq!(result[0..(result.len() - 4)], val[0..(val.len() - 4)]);
        let len = result.len();
        result[(len - 4)..].copy_from_slice(&result_rc.to_le_bytes());
        if result_rc == 0 {
            *result = vec![];
        }
    } else {
        *result = val.to_vec();
    }
}

fn rocksdb_column_options_v6(col: DBCol) -> Options {
    let mut opts = rocksdb_column_options(DBCol::ColDbVersion);

    if col == DBCol::ColState {
        opts.set_merge_operator("refcount merge", refcount_merge_v6, None);
        opts.set_compaction_filter("empty value filter", RocksDB::empty_value_compaction_filter);
    }
    opts
}

impl RocksDB {
    pub(crate) fn new_v6<P: AsRef<std::path::Path>>(path: P) -> Result<Self, DBError> {
        let options = rocksdb_options();
        let cf_names: Vec<_> = DBCol::iter().map(|col| format!("col{}", col as usize)).collect();
        let cf_descriptors = DBCol::iter().map(|col| {
            ColumnFamilyDescriptor::new(
                format!("col{}", col as usize),
                rocksdb_column_options_v6(col),
            )
        });
        let db = DB::open_cf_descriptors(&options, path, cf_descriptors)?;

        let cfs =
            cf_names.iter().map(|n| db.cf_handle(n).unwrap() as *const ColumnFamily).collect();
        Ok(Self { db, cfs, _pin: PhantomPinned })
    }
}

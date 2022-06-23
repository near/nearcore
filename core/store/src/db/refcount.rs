use std::cmp::Ordering;
use std::io::Cursor;

use byteorder::{LittleEndian, ReadBytesExt};
use rocksdb::compaction_filter::Decision;
use rocksdb::MergeOperands;

use crate::db::RocksDB;
use crate::DBCol;

/// Refcounted columns store value with rc.
///
/// Write write refcount records, reads merge them.
/// The merged rc should always be positive, if it's not there is a bug in gc.
///
/// Refcount record format:
/// rc = 0 => empty
/// rc < 0 => 8 bytes little endian rc
/// rc > 0 => value followed by 8 bytes little endian rc
///
pub(crate) fn merge_refcounted_records(result: &mut Vec<u8>, val: &[u8]) {
    let (bytes, add_rc) = decode_value_with_rc(val);
    if add_rc == 0 {
        return;
    }
    let (result_bytes, result_rc) = decode_value_with_rc(result);
    if result_rc == 0 {
        result.extend_from_slice(val);
    } else {
        let rc = result_rc + add_rc;
        debug_assert!(result_rc <= 0 || add_rc <= 0 || result_bytes == bytes);
        match rc.cmp(&0) {
            Ordering::Less => {
                result.clear();
                result.extend_from_slice(&rc.to_le_bytes());
            }
            Ordering::Equal => {
                result.clear();
            }
            Ordering::Greater => {
                if result_rc < 0 {
                    result.clear();
                    result.extend_from_slice(val);
                }
                let len = result.len();
                result[len - 8..].copy_from_slice(&rc.to_le_bytes());
            }
        }
    }
}

/// Returns
/// (Some(value), rc) if rc > 0
/// (None, rc) if rc <= 0
pub fn decode_value_with_rc(bytes: &[u8]) -> (Option<&[u8]>, i64) {
    if bytes.len() < 8 {
        debug_assert!(bytes.is_empty());
        return (None, 0);
    }
    let mut cursor = Cursor::new(&bytes[bytes.len() - 8..]);
    let rc = cursor.read_i64::<LittleEndian>().unwrap();
    if rc < 0 {
        (None, rc)
    } else {
        (Some(&bytes[..bytes.len() - 8]), rc)
    }
}

/// Adds a positive reference count to the value.
///
/// This is roughly equivalent to `encode_value_with_rc(data, rc)`.
pub(crate) fn add_refcount(data: &[u8], rc: std::num::NonZeroU32) -> Vec<u8> {
    let rc = std::num::NonZeroI64::from(rc).get();
    let mut value = Vec::with_capacity(data.len() + 8);
    value.extend_from_slice(data);
    value.extend_from_slice(&rc.to_le_bytes());
    value
}

/// Returns empty value with encoded negative reference count.
///
/// `rc` gives the absolute value of the reference count.  This is roughly
/// equivalent to `encode_value_with_rc("", -rc)`.
pub(crate) fn encode_refcount_decrease(rc: std::num::NonZeroU32) -> Vec<u8> {
    (-(rc.get() as i64)).to_le_bytes().to_vec()
}

impl RocksDB {
    /// DBCol::State has refcounted values.
    /// Merge adds refcounts, zero refcount becomes empty value.
    /// Empty values get filtered by get methods, and removed by compaction.
    pub(crate) fn refcount_merge(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result = vec![];
        if let Some(val) = existing_val {
            merge_refcounted_records(&mut result, val);
        }
        for val in operands {
            merge_refcounted_records(&mut result, val);
        }
        Some(result)
    }

    /// Compaction filter for DBCol::State
    pub(crate) fn empty_value_compaction_filter(
        _level: u32,
        _key: &[u8],
        value: &[u8],
    ) -> Decision {
        if value.is_empty() {
            Decision::Remove
        } else {
            Decision::Keep
        }
    }

    /// Treats empty value as no value and strips refcount
    pub(crate) fn get_with_rc_logic(column: DBCol, value: Option<Vec<u8>>) -> Option<Vec<u8>> {
        if column.is_rc() {
            value.and_then(|vec| decode_value_with_rc(&vec).0.map(|v| v.to_vec()))
        } else {
            value
        }
    }

    /// Iterator treats empty value as no value and strips refcount
    pub(crate) fn iter_with_rc_logic<'a, I>(
        column: DBCol,
        iterator: I,
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a>
    where
        I: Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a,
    {
        if column.is_rc() {
            Box::new(iterator.filter_map(|(k, v_rc)| {
                decode_value_with_rc(&v_rc).0.map(|v| (k, v.to_vec().into_boxed_slice()))
            }))
        } else {
            Box::new(iterator)
        }
    }
}

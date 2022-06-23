//! Functions for handling reference counted columns.
//!
//! Some of the columns in the database are reference counted.  Those are the
//! ones for which [`crate::DBCol::is_rc`] returns `true`.  Inserting value to
//! such column increases the reference count and removing value decreases it.
//! The key is really removed from the database when reference count reaches
//! zero.
//!
//! The reference counts are stored together with the values by simply attaching
//! little-endian encoded 64-bit signed integer at the end of it.  See
//! [`add_positive_refcount`] and [`encode_negative_refcount`] functions for
//! a way to encode reference count.  During compaction, RocksDB merges the
//! values by adding the reference counts.  When the reference count reaches
//! zero RocksDB removes the key from the database.

use std::cmp::Ordering;
use std::io::Cursor;

use byteorder::{LittleEndian, ReadBytesExt};
use rocksdb::compaction_filter::Decision;

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

/// Extracts reference count from raw value and returns it along with the value.
///
/// Decodes the raw value extracting the actual value and reference count:
/// - rc > 0 ⇒ returns `(Some(value), rc)`,
/// - rc ≤ 0 ⇒ returns `(None, rc)`.
///
/// In builds with debug assertions enabled, panics if `bytes` are non-empty but
/// too short to fit 64-bit reference count.
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

/// Encode a positive reference count into the value.
pub(crate) fn add_positive_refcount(data: &[u8], rc: std::num::NonZeroU32) -> Vec<u8> {
    let rc = std::num::NonZeroI64::from(rc).get();
    let mut value = Vec::with_capacity(data.len() + 8);
    value.extend_from_slice(data);
    value.extend_from_slice(&rc.to_le_bytes());
    value
}

/// Returns empty value with encoded negative reference count.
///
/// `rc` gives the absolute value of the reference count.
pub(crate) fn encode_negative_refcount(rc: std::num::NonZeroU32) -> Vec<u8> {
    (-(rc.get() as i64)).to_le_bytes().to_vec()
}

/// Merge reference counted values together.
///
/// Extracts reference count from all provided value and sums them together and
/// returns result depending on rc:
/// - rc = 0 ⇒ empty,
/// - rc < 0 ⇒ encoded reference count,
/// - rc > 0 ⇒ value with encoded reference count.
///
/// Assumes that all provided values with positive reference count have the same
/// value so that the function is free to pick any of the values.  In build with
/// debug assertions panics if this is not true.
fn refcount_merge<'a>(
    existing: Option<&'a [u8]>,
    operands: impl std::iter::IntoIterator<Item = &'a [u8]>,
) -> Option<Vec<u8>> {
    let mut result = vec![];
    if let Some(val) = existing {
        merge_refcounted_records(&mut result, val);
    }
    for val in operands {
        merge_refcounted_records(&mut result, val);
    }
    Some(result)
}

/// Returns value with reference count stripped if column is refcounted.
///
/// If the column is not refcounted, returns the value unchanged.
///
/// If the column is refcounted, extracts the reference count from it and
/// returns value based on that.  If reference count is non-positive, returns
/// `None`; otherwise returns the value with reference count stripped.  Empty
/// values are treated as values with reference count zero.
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

impl RocksDB {
    /// DBCol::State has refcounted values.
    /// Merge adds refcounts, zero refcount becomes empty value.
    /// Empty values get filtered by get methods, and removed by compaction.
    pub(crate) fn refcount_merge(
        _new_key: &[u8],
        existing: Option<&[u8]>,
        operands: &rocksdb::MergeOperands,
    ) -> Option<Vec<u8>> {
        self::refcount_merge(existing, operands)
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
}

#[cfg(test)]
mod test {
    use crate::DBCol;

    const MINUS_TWO: &[u8] = b"\xfe\xff\xff\xff\xff\xff\xff\xff";
    const MINUS_ONE: &[u8] = b"\xff\xff\xff\xff\xff\xff\xff\xff";
    const ZERO: &[u8] = b"\0\0\0\0\0\0\0\0";
    const PLUS_ONE: &[u8] = b"\x01\0\0\0\0\0\0\0";
    const PLUS_TWO: &[u8] = b"\x02\0\0\0\0\0\0\0";

    fn check_debug_assert_or<F, P, R>(callback: F, predicate: P)
    where
        F: FnOnce() -> R + std::panic::UnwindSafe,
        P: FnOnce(R),
        R: std::fmt::Debug,
    {
        let res = std::panic::catch_unwind(callback);
        if cfg!(debug_assertions) {
            assert!(res.is_err(), "{res:?}");
        } else {
            predicate(res.unwrap());
        }
    }

    #[test]
    fn decode_value_with_rc() {
        fn test(want_value: Option<&[u8]>, want_rc: i64, bytes: &[u8]) {
            let got = super::decode_value_with_rc(bytes);
            assert_eq!((want_value, want_rc), got);
        }

        test(None, -2, MINUS_TWO);
        test(None, -2, b"foobar\xfe\xff\xff\xff\xff\xff\xff\xff");
        test(None, 0, b"");
        // TODO(mina86): The next two should return None.
        test(Some(b""), 0, ZERO);
        test(Some(b"bar"), 0, b"bar\0\0\0\0\0\0\0\0");
        test(Some(b""), 2, PLUS_TWO);
        test(Some(b"baz"), 2, b"baz\x02\0\0\0\0\0\0\0");

        check_debug_assert_or(
            || super::decode_value_with_rc(b"short"),
            |got| assert_eq!((None, 0), got),
        );
    }

    #[test]
    fn add_encode_refcount() {
        fn test(want: &[u8], data: &[u8], rc: u32) {
            let rc = std::num::NonZeroU32::new(rc).unwrap();
            assert_eq!(want, &super::add_positive_refcount(data, rc));
        }

        test(PLUS_TWO, b"", 2);
        test(b"foo\x02\0\0\0\0\0\0\0", b"foo", 2);

        let rc = std::num::NonZeroU32::new(2).unwrap();
        assert_eq!(MINUS_TWO, &super::encode_negative_refcount(rc));
    }

    #[test]
    fn refcount_merge() {
        fn test(want: &[u8], operands: &[&[u8]]) {
            let it = operands.into_iter().copied();
            assert_eq!(want, super::refcount_merge(None, it).unwrap().as_slice());
            if !operands.is_empty() {
                let it = operands[1..].into_iter().copied();
                assert_eq!(want, &super::refcount_merge(Some(operands[0]), it).unwrap());
            }
        }

        test(b"", &[]);
        test(b"", &[ZERO]);
        test(b"", &[PLUS_ONE, MINUS_ONE]);
        test(b"", &[PLUS_TWO, MINUS_ONE, MINUS_ONE]);
        test(b"", &[b"foo\x01\0\0\0\0\0\0\0", MINUS_ONE]);
        test(b"", &[b"foo\x02\0\0\0\0\0\0\0", MINUS_ONE, MINUS_ONE]);
        test(b"", &[b"foo\x02\0\0\0\0\0\0\0", MINUS_TWO]);

        test(MINUS_ONE, &[MINUS_ONE]);
        test(MINUS_ONE, &[b"", MINUS_ONE]);
        test(MINUS_ONE, &[ZERO, MINUS_ONE]);
        test(MINUS_ONE, &[b"foo\x01\0\0\0\0\0\0\0", MINUS_TWO]);
        test(MINUS_ONE, &[b"foo\x01\0\0\0\0\0\0\0", MINUS_ONE, MINUS_ONE]);

        test(b"foo\x02\0\0\0\0\0\0\0", &[b"foo\x01\0\0\0\0\0\0\0", b"foo\x01\0\0\0\0\0\0\0"]);
        test(b"foo\x01\0\0\0\0\0\0\0", &[b"foo\x01\0\0\0\0\0\0\0"]);
        test(b"foo\x01\0\0\0\0\0\0\0", &[b"foo\x02\0\0\0\0\0\0\0", MINUS_ONE]);
    }

    #[test]
    fn compaction_filter() {
        use rocksdb::compaction_filter::Decision;

        fn test(want: Decision, value: &[u8]) {
            let got = super::RocksDB::empty_value_compaction_filter(42, b"key", value);
            assert_eq!(std::mem::discriminant(&want), std::mem::discriminant(&got));
        }

        test(Decision::Remove, b"");
        test(Decision::Keep, PLUS_ONE);
        test(Decision::Keep, MINUS_ONE);
        test(Decision::Keep, b"foo\x01\0\0\0\0\0\0\0");
        test(Decision::Keep, b"foo\xff\xff\xff\xff\xff\xff\xff\xff");

        // This never happens in production since the filter is only ever run on
        // refcounted values which have length ≥ 8.
        test(Decision::Keep, b"foo");
        // And this never happens because zero is encoded as empty value.
        test(Decision::Keep, ZERO);
    }

    #[test]
    fn get_with_rc_logic() {
        fn get(col: DBCol, value: &[u8]) -> Option<Vec<u8>> {
            assert_eq!(None, super::get_with_rc_logic(col, None));
            super::get_with_rc_logic(col, Some(value.to_vec()))
        }

        fn test(want: Option<&[u8]>, col: DBCol, value: &[u8]) {
            assert_eq!(want, get(col, value).as_ref().map(Vec::as_slice));
        }

        // Column without reference counting.  Values are returned as is.
        assert!(!DBCol::Block.is_rc());
        for value in [&b""[..], &b"foo"[..], MINUS_ONE, ZERO, PLUS_ONE] {
            test(Some(value), DBCol::Block, value);
        }

        // Column with reference counting.  Count is extracted.
        const RC_COL: DBCol = DBCol::State;
        assert!(RC_COL.is_rc());

        test(None, RC_COL, MINUS_ONE);
        test(None, RC_COL, b"foo\xff\xff\xff\xff\xff\xff\xff\xff");
        test(None, RC_COL, b"");
        // TODO(mina86): The next two should return None.
        test(Some(b""), RC_COL, ZERO);
        test(Some(b"foo"), RC_COL, b"foo\x00\0\0\0\0\0\0\0");
        test(Some(b""), RC_COL, PLUS_ONE);
        test(Some(b"foo"), RC_COL, b"foo\x01\0\0\0\0\0\0\0");

        check_debug_assert_or(
            || {
                let value = Some(b"short".to_vec());
                super::get_with_rc_logic(RC_COL, value)
            },
            |got| assert_eq!(None, got),
        );
    }

    #[test]
    fn iter_with_rc_logic() {
        fn into_box(data: &[u8]) -> Box<[u8]> {
            data.to_vec().into_boxed_slice()
        }

        fn test(want: &[&[u8]], col: DBCol, values: &[&[u8]]) {
            use std::ops::Deref;

            const KEY: &[u8] = b"key";
            let iter = values.into_iter().map(|value| (into_box(KEY), into_box(value)));
            let got = super::iter_with_rc_logic(col, iter)
                .map(|(key, value)| {
                    assert_eq!(KEY, key.deref());
                    value
                })
                .collect::<Vec<_>>();
            let got = got.iter().map(Box::deref).collect::<Vec<_>>();
            assert_eq!(want, got.as_slice());
        }

        // Column without reference counting.  ALl values are returned as is.
        assert!(!DBCol::Block.is_rc());
        test(
            &[&b""[..], &b"foo"[..], MINUS_ONE, ZERO, PLUS_ONE],
            DBCol::Block,
            &[&b""[..], &b"foo"[..], MINUS_ONE, ZERO, PLUS_ONE],
        );

        // Column with reference counting.  Count is extracted.
        const RC_COL: DBCol = DBCol::State;
        assert!(RC_COL.is_rc());

        test(
            &[b"", b"foo", b"", b"foo"],
            RC_COL,
            &[
                MINUS_ONE,
                b"foo\xff\xff\xff\xff\xff\xff\xff\xff",
                b"",
                ZERO,
                b"foo\x00\0\0\0\0\0\0\0",
                PLUS_ONE,
                b"foo\x01\0\0\0\0\0\0\0",
            ],
        );
    }
}

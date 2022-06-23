//! Functions for handling reference counted columns.
//!
//! Some of the columns in the database are reference counted.  Those are the
//! ones for which [`crate::DBCol::is_rc`] returns `true`.  Inserting value to
//! such column increases the reference count and removing value decreases it.
//! The key is really removed from the database when reference count reaches
//! zero.
//!
//! Note that it is an error to try to store different values under the same
//! key.  That is, when increasing the reference count of an existing key, the
//! same value must be inserted.  If different values are inserted it’s not
//! defined which one will be later read from the database.
//!
//! The reference counts are stored together with the values by simply attaching
//! little-endian encoded 64-bit signed integer at the end of it.  See
//! [`add_positive_refcount`] and [`encode_negative_refcount`] functions for
//! a way to encode reference count.  During compaction, RocksDB merges the
//! values by adding the reference counts.  When the reference count reaches
//! zero RocksDB removes the key from the database.

use std::cmp::Ordering;

use rocksdb::compaction_filter::Decision;

use crate::db::RocksDB;
use crate::DBCol;

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
    let (head, tail) = bytes.split_at(bytes.len() - 8);
    let rc = i64::from_le_bytes(tail.try_into().unwrap());
    if rc <= 0 {
        (None, rc)
    } else {
        (Some(head), rc)
    }
}

/// Encode a positive reference count into the value.
pub(crate) fn add_positive_refcount(data: &[u8], rc: std::num::NonZeroU32) -> Vec<u8> {
    [data, &i64::from(rc.get()).to_le_bytes()].concat()
}

/// Returns empty value with encoded negative reference count.
///
/// `rc` gives the absolute value of the reference count.
pub(crate) fn encode_negative_refcount(rc: std::num::NonZeroU32) -> Vec<u8> {
    (-i64::from(rc.get())).to_le_bytes().to_vec()
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
pub(crate) fn refcount_merge<'a>(
    existing: Option<&'a [u8]>,
    operands: impl std::iter::IntoIterator<Item = &'a [u8]>,
) -> Vec<u8> {
    let (mut payload, mut rc) = existing.map_or((None, 0), decode_value_with_rc);
    for (new_payload, delta) in operands.into_iter().map(decode_value_with_rc) {
        if payload.is_none() {
            payload = new_payload;
        } else if new_payload.is_some() {
            debug_assert_eq!(payload, new_payload);
        }
        rc += delta;
    }

    match rc.cmp(&0) {
        Ordering::Less => rc.to_le_bytes().to_vec(),
        Ordering::Equal => Vec::new(),
        Ordering::Greater => [payload.unwrap_or(b""), &rc.to_le_bytes()].concat(),
    }
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
        let mut value = value?;
        decode_value_with_rc(&value).0?;
        value.truncate(value.len() - 8);
        Some(value)
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
    /// Merge adds refcounts, zero refcount becomes empty value.
    /// Empty values get filtered by get methods, and removed by compaction.
    pub(crate) fn refcount_merge(
        _new_key: &[u8],
        existing: Option<&[u8]>,
        operands: &rocksdb::MergeOperands,
    ) -> Option<Vec<u8>> {
        Some(self::refcount_merge(existing, operands))
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
        test(None, 0, ZERO);
        test(None, 0, b"bar\0\0\0\0\0\0\0\0");
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
            let got = super::refcount_merge(None, it);
            assert_eq!(want, got.as_slice());

            if !operands.is_empty() {
                let it = operands[1..].into_iter().copied();
                let got = super::refcount_merge(Some(operands[0]), it);
                assert_eq!(want, got.as_slice());
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
        test(None, RC_COL, ZERO);
        test(None, RC_COL, b"foo\x00\0\0\0\0\0\0\0");
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
            &[b"", b"foo"],
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

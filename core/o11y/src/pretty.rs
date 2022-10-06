use near_primitives::hash::CryptoHash;

/// A wrapper for bytes slice which tries to guess best way to format it.
///
/// If the slice is exactly 32-byte long, it’s assumed to be a hash and is
/// converted into base58 and printed surrounded by backtics.  If the slice
/// contains printable ASCII characters only, it’s represented as a string
/// surrounded by single quotes (as a consequence, empty value is converted to
/// pair of single quotes).  In all other cases, it converts the value into
/// base64.
///
/// The motivation for such choices is that we only ever use base58 to format
/// hashes which are 32-byte long.  It’s therefore not useful to use it for any
/// other types of keys.
///
/// The intended usage for this type is when trying to format binary data whose
/// structure isn’t known to the caller.  For example, when generating debugging
/// or tracing data at database layer where everything is just slices of bytes.
/// At higher levels of abstractions, if the structure of the data is known,
/// it’s usually better to format data in a way that makes sense for the given
/// type.
///
/// The type can be used as with `tracing::info!` and similar calls.  For
/// example:
///
/// ```ignore
/// tracing::trace!(target: "store",
///                 db_op = "insert",
///                 col = %col,
///                 key = %near_o11y::pretty::Bytes(key),
///                 size = value.len())
/// ```
pub struct Bytes<'a>(pub &'a [u8]);

impl<'a> std::fmt::Display for Bytes<'a> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.len() == 32 {
            write!(fmt, "`{}`", CryptoHash(self.0.try_into().unwrap()))
        } else if self.0.iter().all(|ch| 0x20 <= *ch && *ch <= 0x7E) {
            // SAFETY: We’ve just checked that the value contains ASCII
            // characters only.
            let value = unsafe { std::str::from_utf8_unchecked(self.0) };
            write!(fmt, "'{value}'")
        } else {
            near_primitives::serialize::base64_display(self.0).fmt(fmt)
        }
    }
}

#[test]
fn test_bytes() {
    #[track_caller]
    fn test(want: &str, slice: &[u8]) {
        assert_eq!(want, Bytes(slice).to_string())
    }

    test("''", b"");
    test("'foo'", b"foo");
    test("'foo bar'", b"foo bar");
    test("WsOzxYJ3", "Zółw".as_bytes());
    test("EGZvbyBiYXI=", b"\x10foo bar");
    test("f2ZvbyBiYXI=", b"\x7Ffoo bar");
    test("`11111111111111111111111111111111`", &[0; 32]);
    let hash = CryptoHash::hash_bytes(b"foo");
    test("`3yMApqCuCjXDWPrbjfR5mjCPTHqFG8Pux1TxQrEM35jj`", hash.as_bytes());
}

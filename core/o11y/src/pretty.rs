use near_primitives::hash::CryptoHash;

/// A wrapper for bytes slice which tries to guess best way to format it.
///
/// If the slice contains printable ASCII characters only, it’s represented as
/// a string surrounded by single quotes (as a consequence, empty value is
/// converted to pair of single quotes).  Otherwise, it converts the value into
/// base64.
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
/// tracing::trace!(target: "state",
///                 db_op = "insert",
///                 key = %near_o11y::pretty::Bytes(key),
///                 size = value.len())
/// ```
///
/// See also [`StorageKey`] which tries to guess if the data is not a crypto
/// hash.
pub struct Bytes<'a>(pub &'a [u8]);

impl<'a> std::fmt::Display for Bytes<'a> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        bytes_format(self.0, fmt, false)
    }
}

/// A wrapper for bytes slice which tries to guess best way to format it.
///
/// If the slice is exactly 32-byte long, it’s assumed to be a hash and is
/// converted into base58 and printed surrounded by backtics.  Otherwise,
/// behaves like [`Bytes`] representing the data as string if it contains ASCII
/// printable bytes only or base64 otherwise.
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
/// tracing::info!(target: "store",
///                op = "set",
///                col = %col,
///                key = %near_o11y::pretty::StorageKey(key),
///                size = value.len())
/// ```
pub struct StorageKey<'a>(pub &'a [u8]);

impl<'a> std::fmt::Display for StorageKey<'a> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        bytes_format(self.0, fmt, true)
    }
}

/// Implementation of [`Bytes`] and [`StorageKey`] formatting.
///
/// If the `consider_hash` argument is false, formats bytes as described in
/// [`Bytes`].  If it’s true, formats the bytes as described in [`StorageKey`].
fn bytes_format(
    bytes: &[u8],
    fmt: &mut std::fmt::Formatter<'_>,
    consider_hash: bool,
) -> std::fmt::Result {
    if consider_hash && bytes.len() == 32 {
        write!(fmt, "`{}`", CryptoHash(bytes.try_into().unwrap()))
    } else if bytes.iter().all(|ch| 0x20 <= *ch && *ch <= 0x7E) {
        // SAFETY: We’ve just checked that the value contains ASCII
        // characters only.
        let value = unsafe { std::str::from_utf8_unchecked(bytes) };
        write!(fmt, "'{value}'")
    } else {
        std::fmt::Display::fmt(&near_primitives::serialize::base64_display(bytes), fmt)
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
    test("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", &[0; 32]);
    let hash = CryptoHash::hash_bytes(b"foo");
    test("LCa0a2j/xo/5m0U8HTBBNBNCLXBkg7+g+YpeiGJm564=", hash.as_bytes());
}

#[test]
fn test_storage_key() {
    #[track_caller]
    fn test(want: &str, slice: &[u8]) {
        assert_eq!(want, StorageKey(slice).to_string())
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

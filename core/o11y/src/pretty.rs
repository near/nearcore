use near_primitives_core::hash::CryptoHash;
use near_primitives_core::serialize::{base64_display, from_base64};
use std::str::FromStr;

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

impl<'a> Bytes<'a> {
    /// Reverses `bytes_format` to allow decoding `Bytes` written with `Display`.
    ///
    /// This looks  similar to `FromStr` but due to lifetime constraints on
    /// input and output, the trait cannot be implemented.
    ///
    /// Error: Returns an error when the input does not look like an output from
    /// `bytes_format`.
    pub fn from_str(s: &str) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        if s.starts_with("`") && s.ends_with("`") {
            // hash encoded as base58
            let hash = CryptoHash::from_str(&s[1..s.len() - 1])?;
            Ok(hash.0.to_vec())
        } else if s.starts_with("'") && s.ends_with("'") {
            // plain string
            let bytes = s[1..s.len() - 1].as_bytes();
            Ok(bytes.to_vec())
        } else {
            // encoded with base64
            let bytes = from_base64(s)?;
            Ok(bytes.to_vec())
        }
    }
}

/// A wrapper for bytes slice which tries to guess best way to format it
/// truncating the value if it’s too long.
///
/// Behaves like [`Bytes`] but truncates the formatted string to around 128
/// characters.  If the value is longer then that, the length of the value in
/// bytes is included at the beginning and ellipsis is included at the end of
/// the value.
pub struct AbbrBytes<T>(pub T);

impl<'a> std::fmt::Display for AbbrBytes<&'a [u8]> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        truncated_bytes_format(self.0, fmt)
    }
}

impl<'a> std::fmt::Display for AbbrBytes<&'a Vec<u8>> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        AbbrBytes(self.0.as_slice()).fmt(fmt)
    }
}

impl<'a> std::fmt::Display for AbbrBytes<Option<&'a [u8]>> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            None => fmt.write_str("None"),
            Some(bytes) => truncated_bytes_format(bytes, fmt),
        }
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
        std::fmt::Display::fmt(&base64_display(bytes), fmt)
    }
}

/// Implementation of [`AbbrBytes`].
fn truncated_bytes_format(bytes: &[u8], fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    const LIMIT: usize = 128;
    let len = bytes.len();
    if bytes.iter().take(LIMIT - 2).all(|ch| 0x20 <= *ch && *ch <= 0x7E) {
        if len <= LIMIT - 2 {
            // SAFETY: We’ve just checked that the value contains ASCII
            // characters only.
            let value = unsafe { std::str::from_utf8_unchecked(bytes) };
            write!(fmt, "'{value}'")
        } else {
            let bytes = &bytes[..LIMIT - 9];
            let value = unsafe { std::str::from_utf8_unchecked(bytes) };
            write!(fmt, "({len})'{value}'…")
        }
    } else {
        if bytes.len() <= LIMIT / 4 * 3 {
            std::fmt::Display::fmt(&base64_display(bytes), fmt)
        } else {
            let bytes = &bytes[..(LIMIT - 8) / 4 * 3];
            let value = base64_display(bytes);
            write!(fmt, "({len}){value}…")
        }
    }
}

#[cfg(test)]
macro_rules! do_test_bytes_formatting {
    ($type:ident, $consider_hash:expr, $truncate:expr) => {{
        #[track_caller]
        fn test(want: &str, slice: &[u8]) {
            assert_eq!(want, $type(slice).to_string(), "unexpected formatting");
            if !$truncate {
                assert_eq!(&Bytes::from_str(want).expect("decode fail"), slice, "wrong decoding");
            }
        }

        #[track_caller]
        fn test2(cond: bool, want_true: &str, want_false: &str, slice: &[u8]) {
            test(if cond { want_true } else { want_false }, slice);
        }

        test("''", b"");
        test("'foo'", b"foo");
        test("'foo bar'", b"foo bar");
        test("WsOzxYJ3", "Zółw".as_bytes());
        test("EGZvbyBiYXI=", b"\x10foo bar");
        test("f2ZvbyBiYXI=", b"\x7Ffoo bar");

        test2(
            $consider_hash,
            "`11111111111111111111111111111111`",
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            &[0; 32],
        );
        let hash = CryptoHash::hash_bytes(b"foo");
        test2(
            $consider_hash,
            "`3yMApqCuCjXDWPrbjfR5mjCPTHqFG8Pux1TxQrEM35jj`",
            "LCa0a2j/xo/5m0U8HTBBNBNCLXBkg7+g+YpeiGJm564=",
            hash.as_bytes(),
        );

        let long_str = "rabarbar".repeat(16);
        test2(
            $truncate,
            &format!("(128)'{}'…", &long_str[..119]),
            &format!("'{long_str}'"),
            long_str.as_bytes(),
        );
        test2(
            $truncate,
            &format!("(102){}…", &"deadbeef".repeat(15)),
            &"deadbeef".repeat(17),
            &b"u\xe6\x9dm\xe7\x9f".repeat(17),
        );
    }};
}

#[test]
fn test_bytes() {
    do_test_bytes_formatting!(Bytes, false, false);
}

#[test]
fn test_truncated_bytes() {
    do_test_bytes_formatting!(AbbrBytes, false, true);
}

#[test]
fn test_storage_key() {
    do_test_bytes_formatting!(StorageKey, true, false);
}

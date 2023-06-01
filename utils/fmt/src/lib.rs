use std::ops::RangeInclusive;

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
///                 key = %near_fmt::Bytes(key),
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

impl<'a> std::fmt::Debug for Bytes<'a> {
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
        if s.len() >= 2 && s.starts_with('`') && s.ends_with('`') {
            // hash encoded as base58
            let hash = CryptoHash::from_str(&s[1..s.len().checked_sub(1).expect("s.len() >= 2 ")])?;
            Ok(hash.as_bytes().to_vec())
        } else if s.len() >= 2 && s.starts_with('\'') && s.ends_with('\'') {
            // plain string
            Ok(s[1..s.len().checked_sub(1).expect("s.len() >= 2 ")].as_bytes().to_vec())
        } else {
            // encoded with base64
            from_base64(s).map_err(|err| err.into())
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

impl<'a> std::fmt::Debug for AbbrBytes<&'a [u8]> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        truncated_bytes_format(self.0, fmt)
    }
}

impl<'a> std::fmt::Debug for AbbrBytes<&'a Vec<u8>> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        AbbrBytes(self.0.as_slice()).fmt(fmt)
    }
}

impl<'a> std::fmt::Debug for AbbrBytes<Option<&'a [u8]>> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            None => fmt.write_str("None"),
            Some(bytes) => truncated_bytes_format(bytes, fmt),
        }
    }
}

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
///                key = %near_fmt::StorageKey(key),
///                size = value.len())
/// ```
pub struct StorageKey<'a>(pub &'a [u8]);

impl<'a> std::fmt::Display for StorageKey<'a> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        bytes_format(self.0, fmt, true)
    }
}

impl<'a> std::fmt::Debug for StorageKey<'a> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        bytes_format(self.0, fmt, true)
    }
}

/// A wrapper for slices which formats the slice limiting the length.
///
/// If the slice has no more than five elements, it’s printed in full.
/// Otherwise, only the first two and last two elements are printed to limit the
/// length of the formatted value.
pub struct Slice<'a, T>(pub &'a [T]);

impl<'a, T: std::fmt::Debug> std::fmt::Debug for Slice<'a, T> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let slice = self.0;

        struct Ellipsis;

        impl std::fmt::Debug for Ellipsis {
            fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                fmt.write_str("…")
            }
        }

        if let [a, b, _c, .., _x, y, z] = slice {
            write!(fmt, "({})", slice.len())?;
            fmt.debug_list().entry(a).entry(b).entry(&Ellipsis).entry(y).entry(z).finish()
        } else {
            std::fmt::Debug::fmt(&slice, fmt)
        }
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
    const PRINTABLE_ASCII: RangeInclusive<u8> = 0x20..=0x7E;
    const OVERALL_LIMIT: usize = 128;
    const DISPLAY_ASCII_FULL_LIMIT: usize = OVERALL_LIMIT - 2;
    const DISPLAY_ASCII_PREFIX_LIMIT: usize = OVERALL_LIMIT - 9;
    const DISPLAY_BASE64_FULL_LIMIT: usize = OVERALL_LIMIT / 4 * 3;
    const DISPLAY_BASE64_PREFIX_LIMIT: usize = (OVERALL_LIMIT - 8) / 4 * 3;
    let len = bytes.len();
    if bytes.iter().take(DISPLAY_ASCII_FULL_LIMIT).all(|ch| PRINTABLE_ASCII.contains(ch)) {
        if len <= DISPLAY_ASCII_FULL_LIMIT {
            // SAFETY: We’ve just checked that the value contains ASCII
            // characters only.
            let value = unsafe { std::str::from_utf8_unchecked(bytes) };
            write!(fmt, "'{value}'")
        } else {
            let bytes = &bytes[..DISPLAY_ASCII_PREFIX_LIMIT];
            let value = unsafe { std::str::from_utf8_unchecked(bytes) };
            write!(fmt, "({len})'{value}'…")
        }
    } else if bytes.len() <= DISPLAY_BASE64_FULL_LIMIT {
        std::fmt::Display::fmt(&base64_display(bytes), fmt)
    } else {
        let bytes = &bytes[..DISPLAY_BASE64_PREFIX_LIMIT];
        let value = base64_display(bytes);
        write!(fmt, "({len}){value}…")
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

#[test]
fn test_slice() {
    macro_rules! test {
        ($want:literal, $fmt:literal, $len:expr) => {
            assert_eq!(
                $want,
                format!($fmt, Slice(&[0u8, 11, 22, 33, 44, 55, 66, 77, 88, 99][..$len]))
            )
        };
    }

    test!("[]", "{:?}", 0);
    test!("[0, 11, 22, 33]", "{:?}", 4);
    test!("[0, b, 16, 21]", "{:x?}", 4);
    test!("(10)[0, 11, …, 88, 99]", "{:?}", 10);
    test!("(10)[0, b, …, 58, 63]", "{:x?}", 10);
}

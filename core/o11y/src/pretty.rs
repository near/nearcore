use near_primitives::hash::CryptoHash;

/// A wrapper for bytes slice which tries to guess best way to format it.
///
/// If the slice is empty, it’s represented as an empty set symbol.  If the
/// slice is exactly 32-byte long, it’s assumed to be a hash and is converted
/// into base58.  If the slice contains graphic ASCII characters only, it’s
/// represented as a string.  In other cases, it converts the value into base64.
///
/// The motivation for such choices is that we only ever use base58 to format
/// hashes which are 32-byte long.  It’s therefore not useful to use it for any
/// other types of keys.
///
/// The type can be used as with `tracing::info!` and similar calls.  For
/// example:
///
/// ```ignore
/// tracing::trace!(target: "store",
///                 db_op = "insert",
///                 col = %col,
///                 key = %near_o11y::pretty::Key(key),
///                 size = value.len())
/// ```
pub struct Key<'a>(pub &'a [u8]);

impl<'a> std::fmt::Display for Key<'a> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            fmt.write_str("∅")
        } else if self.0.len() == 32 {
            CryptoHash(self.0.try_into().unwrap()).fmt(fmt)
        } else if self.0.iter().all(u8::is_ascii_graphic) {
            // SAFETY: We’ve just checked that the value contains ASCII
            // characters only.
            let value = unsafe { std::str::from_utf8_unchecked(self.0) };
            fmt.write_str(value)
        } else {
            near_primitives::serialize::base64_display(self.0).fmt(fmt)
        }
    }
}

#[test]
fn test_key() {
    #[track_caller]
    fn test(want: &str, slice: &[u8]) {
        assert_eq!(want, Key(slice).to_string())
    }

    test("∅", b"");
    test("foo", b"foo");
    test("Zm9vIGJhcg==", b"foo bar");
    test("11111111111111111111111111111111", &[0; 32]);
    test("CuoNgQBWsXnTqup6FY3UXNz6RRufnYyQVxx8HKZLUaRt", CryptoHash::hash_bytes(b"foo").as_bytes());
}

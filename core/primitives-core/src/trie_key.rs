use std::mem::size_of;

/// Length of the column prefix byte in a trie key (a single `u8`).
const COLUMN_PREFIX_LEN: usize = size_of::<u8>();

/// Length of the access key separator byte in a trie key (a single `u8`).
const ACCESS_KEY_SEPARATOR_LEN: usize = size_of::<u8>();

/// Returns the length of the trie key for an access key.
///
/// The trie key format is: `[col_prefix] [account_id] [separator] [public_key]`
/// where the prefix and separator are each a single `u8` byte.
pub fn access_key_key_len(account_id_len: usize, public_key_len: usize) -> usize {
    COLUMN_PREFIX_LEN + account_id_len + ACCESS_KEY_SEPARATOR_LEN + public_key_len
}

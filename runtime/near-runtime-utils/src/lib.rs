//! Contains utility functions for runtime.

/// Returns true if the account ID length is 64 characters and it's a hex representation.
pub fn is_account_id_64_len_hex(account_id: &str) -> bool {
    account_id.len() == 64
        && account_id.as_bytes().iter().all(|&b| match b {
            b'a'..=b'f' | b'0'..=b'9' => true,
            _ => false,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_account_id_64_len_hex() {
        let valid_64_len_hex_account_ids = vec![
            "0000000000000000000000000000000000000000000000000000000000000000",
            "6174617461746174617461746174617461746174617461746174617461746174",
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "20782e20662e64666420482123494b6b6c677573646b6c66676a646b6c736667",
        ];
        for valid_account_id in valid_64_len_hex_account_ids {
            assert!(
                is_account_id_64_len_hex(valid_account_id),
                "Account ID {} should be valid 64-len hex",
                valid_account_id
            );
        }

        let invalid_64_len_hex_account_ids = vec![
            "000000000000000000000000000000000000000000000000000000000000000",
            "6.74617461746174617461746174617461746174617461746174617461746174",
            "012-456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "fffff_ffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "oooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo",
            "00000000000000000000000000000000000000000000000000000000000000",
        ];
        for invalid_account_id in invalid_64_len_hex_account_ids {
            assert!(
                !is_account_id_64_len_hex(invalid_account_id),
                "Account ID {} should be invalid 64-len hex",
                invalid_account_id
            );
        }
    }
}

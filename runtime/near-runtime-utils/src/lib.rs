//! Contains utility functions for runtime.
#[macro_use]
extern crate lazy_static;

use regex::Regex;

type AccountId = String;

pub const MIN_ACCOUNT_ID_LEN: usize = 2;
pub const MAX_ACCOUNT_ID_LEN: usize = 64;

lazy_static! {
    /// See NEP#0006
    static ref VALID_ACCOUNT_ID: Regex =
        Regex::new(r"^(([a-z\d]+[\-_])*[a-z\d]+\.)*([a-z\d]+[\-_])*[a-z\d]+$").unwrap();
    /// Represents a part of an account ID with a suffix of as a separator `.`.
    static ref VALID_ACCOUNT_PART_ID_WITH_TAIL_SEPARATOR: Regex =
        Regex::new(r"^([a-z\d]+[\-_])*[a-z\d]+\.$").unwrap();
    /// Represents a top level account ID.
    static ref VALID_TOP_LEVEL_ACCOUNT_ID: Regex =
        Regex::new(r"^([a-z\d]+[\-_])*[a-z\d]+$").unwrap();
}

/// const does not allow function call, so have to resort to this
pub fn system_account() -> AccountId {
    "system".to_string()
}

pub fn is_valid_account_id(account_id: &AccountId) -> bool {
    account_id.len() >= MIN_ACCOUNT_ID_LEN
        && account_id.len() <= MAX_ACCOUNT_ID_LEN
        && VALID_ACCOUNT_ID.is_match(account_id)
}

pub fn is_valid_top_level_account_id(account_id: &AccountId) -> bool {
    account_id.len() >= MIN_ACCOUNT_ID_LEN
        && account_id.len() <= MAX_ACCOUNT_ID_LEN
        && account_id != &system_account()
        && VALID_TOP_LEVEL_ACCOUNT_ID.is_match(account_id)
}

/// Returns true if the signer_id can create a direct sub-account with the given account Id.
/// It assumes the signer_id is a valid account_id
pub fn is_valid_sub_account_id(signer_id: &AccountId, sub_account_id: &AccountId) -> bool {
    if !is_valid_account_id(sub_account_id) {
        return false;
    }
    if signer_id.len() >= sub_account_id.len() {
        return false;
    }
    // Will not panic, since valid account id is utf-8 only and the length is checked above.
    // e.g. when `near` creates `aa.near`, it splits into `aa.` and `near`
    let (prefix, suffix) = sub_account_id.split_at(sub_account_id.len() - signer_id.len());
    if suffix != signer_id {
        return false;
    }
    VALID_ACCOUNT_PART_ID_WITH_TAIL_SEPARATOR.is_match(prefix)
}

/// Returns true if the account ID length is 64 characters and it's a hex representation.
pub fn is_account_id_64_len_hex(account_id: &str) -> bool {
    account_id.len() == 64
        && account_id.as_bytes().iter().all(|&b| match b {
            b'a'..=b'f' | b'0'..=b'9' => true,
            _ => false,
        })
}

/// Returns true if the account ID is suppose to be EVM machine.
pub fn is_account_evm(account_id: &str) -> bool {
    account_id == "evm" || account_id.ends_with(".evm")
}

#[cfg(test)]
mod tests {
    use super::*;

    const OK_ACCOUNT_IDS: &[&str] = &[
        "aa",
        "a-a",
        "a-aa",
        "100",
        "0o",
        "com",
        "near",
        "bowen",
        "b-o_w_e-n",
        "b.owen",
        "bro.wen",
        "a.ha",
        "a.b-a.ra",
        "system",
        "over.9000",
        "google.com",
        "illia.cheapaccounts.near",
        "0o0ooo00oo00o",
        "alex-skidanov",
        "10-4.8-2",
        "b-o_w_e-n",
        "no_lols",
        "0123456789012345678901234567890123456789012345678901234567890123",
        // Valid, but can't be created
        "near.a",
    ];

    #[test]
    fn test_is_valid_account_id() {
        for account_id in OK_ACCOUNT_IDS {
            assert!(
                is_valid_account_id(&account_id.to_string()),
                "Valid account id {:?} marked invalid",
                account_id
            );
        }

        let bad_account_ids = vec![
            "a",
            "A",
            "Abc",
            "-near",
            "near-",
            "-near-",
            "near.",
            ".near",
            "near@",
            "@near",
            "неар",
            "@@@@@",
            "0__0",
            "0_-_0",
            "0_-_0",
            "..",
            "a..near",
            "nEar",
            "_bowen",
            "hello world",
            "abcdefghijklmnopqrstuvwxyz.abcdefghijklmnopqrstuvwxyz.abcdefghijklmnopqrstuvwxyz",
            "01234567890123456789012345678901234567890123456789012345678901234",
            // `@` separators are banned now
            "some-complex-address@gmail.com",
            "sub.buy_d1gitz@atata@b0-rg.c_0_m",
        ];
        for account_id in bad_account_ids {
            assert!(
                !is_valid_account_id(&account_id.to_string()),
                "Invalid account id {:?} marked valid",
                account_id
            );
        }
    }

    #[test]
    fn test_is_valid_top_level_account_id() {
        let ok_top_level_account_ids = vec![
            "aa",
            "a-a",
            "a-aa",
            "100",
            "0o",
            "com",
            "near",
            "bowen",
            "b-o_w_e-n",
            "0o0ooo00oo00o",
            "alex-skidanov",
            "b-o_w_e-n",
            "no_lols",
            "0123456789012345678901234567890123456789012345678901234567890123",
        ];
        for account_id in ok_top_level_account_ids {
            assert!(
                is_valid_top_level_account_id(&account_id.to_string()),
                "Valid top level account id {:?} marked invalid",
                account_id
            );
        }

        let bad_top_level_account_ids = vec![
            "near.a",
            "b.owen",
            "bro.wen",
            "a.ha",
            "a.b-a.ra",
            "some-complex-address@gmail.com",
            "sub.buy_d1gitz@atata@b0-rg.c_0_m",
            "over.9000",
            "google.com",
            "illia.cheapaccounts.near",
            "10-4.8-2",
            "a",
            "A",
            "Abc",
            "-near",
            "near-",
            "-near-",
            "near.",
            ".near",
            "near@",
            "@near",
            "неар",
            "@@@@@",
            "0__0",
            "0_-_0",
            "0_-_0",
            "..",
            "a..near",
            "nEar",
            "_bowen",
            "hello world",
            "abcdefghijklmnopqrstuvwxyz.abcdefghijklmnopqrstuvwxyz.abcdefghijklmnopqrstuvwxyz",
            "01234567890123456789012345678901234567890123456789012345678901234",
            // Valid regex and length, but reserved
            "system",
        ];
        for account_id in bad_top_level_account_ids {
            assert!(
                !is_valid_top_level_account_id(&account_id.to_string()),
                "Invalid top level account id {:?} marked valid",
                account_id
            );
        }
    }

    #[test]
    fn test_is_valid_sub_account_id() {
        let ok_pairs = vec![
            ("test", "a.test"),
            ("test-me", "abc.test-me"),
            ("gmail.com", "abc.gmail.com"),
            ("gmail.com", "abc-lol.gmail.com"),
            ("gmail.com", "abc_lol.gmail.com"),
            ("gmail.com", "bro-abc_lol.gmail.com"),
            ("g0", "0g.g0"),
            ("1g", "1g.1g"),
            ("5-3", "4_2.5-3"),
        ];
        for (signer_id, sub_account_id) in ok_pairs {
            assert!(
                is_valid_sub_account_id(&signer_id.to_string(), &sub_account_id.to_string()),
                "Failed to create sub-account {:?} by account {:?}",
                sub_account_id,
                signer_id
            );
        }

        let bad_pairs = vec![
            ("test", ".test"),
            ("test", "test"),
            ("test", "est"),
            ("test", ""),
            ("test", "st"),
            ("test5", "ббб"),
            ("test", "a-test"),
            ("test", "etest"),
            ("test", "a.etest"),
            ("test", "retest"),
            ("test-me", "abc-.test-me"),
            ("test-me", "Abc.test-me"),
            ("test-me", "-abc.test-me"),
            ("test-me", "a--c.test-me"),
            ("test-me", "a_-c.test-me"),
            ("test-me", "a-_c.test-me"),
            ("test-me", "_abc.test-me"),
            ("test-me", "abc_.test-me"),
            ("test-me", "..test-me"),
            ("test-me", "a..test-me"),
            ("gmail.com", "a.abc@gmail.com"),
            ("gmail.com", ".abc@gmail.com"),
            ("gmail.com", ".abc@gmail@com"),
            ("gmail.com", "abc@gmail@com"),
            ("test", "a@test"),
            ("test_me", "abc@test_me"),
            ("gmail.com", "abc@gmail.com"),
            ("gmail@com", "abc.gmail@com"),
            ("gmail.com", "abc-lol@gmail.com"),
            ("gmail@com", "abc_lol.gmail@com"),
            ("gmail@com", "bro-abc_lol.gmail@com"),
            ("gmail.com", "123456789012345678901234567890123456789012345678901234567890@gmail.com"),
            (
                "123456789012345678901234567890123456789012345678901234567890",
                "1234567890.123456789012345678901234567890123456789012345678901234567890",
            ),
            ("aa", "ъ@aa"),
            ("aa", "ъ.aa"),
        ];
        for (signer_id, sub_account_id) in bad_pairs {
            assert!(
                !is_valid_sub_account_id(&signer_id.to_string(), &sub_account_id.to_string()),
                "Invalid sub-account {:?} created by account {:?}",
                sub_account_id,
                signer_id
            );
        }
    }

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

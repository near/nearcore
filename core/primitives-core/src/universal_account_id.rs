//! Encoder for `0u` universal account ids (UAIDs).
//!
//! A UAID encodes a 32-byte hash as `0u` + 52 Crockford-base32 symbols of the hash
//! = 54 characters, all lowercase `[0-9a-z]`, which is a valid NEAR account id.
//!
//! This turns a hash into an address. Hashing a `StateInit` into the 32-byte input
//! lives with the account-id derivation, not here, and `AccountType` decides whether
//! a given account id is one of these.
//!
//! The base32 is implemented here rather than taken from a crate: it is a handful of
//! lines and not worth a dependency in this foundational crate. The implementation was
//! cross-checked against the `data-encoding` crate over 40M random cases with zero
//! correctness divergence and on-par performance.

// cspell:words crockford uaid nbits kats

use crate::types::AccountId;

/// Scheme + hash-function marker. A different hash function gets a different letter.
pub const UAID_PREFIX: &str = "0u";
/// Base32 symbols encoding the 256-bit hash (`ceil(256 / 5)`).
pub const UAID_DATA_SYMBOLS: usize = 52;
/// Total UAID length: prefix + data.
pub const UAID_LEN: usize = UAID_PREFIX.len() + UAID_DATA_SYMBOLS;

/// Crockford base32, lowercase, excluding `i l o u` to reduce transcription errors.
const CROCKFORD: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz"; // cspell:disable-line

/// Encode a 32-byte hash as a `0u` universal account id.
pub fn encode_universal_account_id(hash: &[u8; 32]) -> AccountId {
    let data = base32_encode(hash);
    let mut s = String::with_capacity(UAID_LEN);
    s.push_str(UAID_PREFIX);
    for &v in &data {
        s.push(CROCKFORD[v as usize] as char);
    }
    debug_assert_eq!(s.len(), UAID_LEN);
    // Safe: the emitted charset and length are always a valid account id.
    s.parse::<AccountId>().expect("uaid codec must produce a valid account id")
}

/// 32 bytes -> 52 five-bit symbol values, MSB-first. The 256 bits leave 1 bit in
/// the final symbol, padded on the right with 4 zero bits.
fn base32_encode(hash: &[u8; 32]) -> [u8; UAID_DATA_SYMBOLS] {
    let mut out = [0u8; UAID_DATA_SYMBOLS];
    let mut acc: u32 = 0;
    let mut nbits: u32 = 0;
    let mut idx = 0;
    for &byte in hash {
        acc = (acc << 8) | byte as u32;
        nbits += 8;
        while nbits >= 5 {
            nbits -= 5;
            out[idx] = ((acc >> nbits) & 0x1f) as u8;
            idx += 1;
        }
        acc &= (1u32 << nbits) - 1;
    }
    out[idx] = ((acc << (5 - nbits)) & 0x1f) as u8;
    idx += 1;
    debug_assert_eq!(idx, UAID_DATA_SYMBOLS);
    debug_assert_eq!(nbits, 1);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::id::AccountType;

    #[test]
    fn layout_constants() {
        // Stay within the 64-char account-id limit.
        assert_eq!(UAID_LEN, 54);
        assert!(UAID_LEN <= 64);
    }

    /// Canonical known-answer vectors, cross-checked against the `data-encoding`
    /// crate's Crockford base32. Keep these stable: they are reused by derivation
    /// tests and the NEP.
    const KATS: &[(&[u8; 32], &str)] = &[
        // all zero
        (&[0x00; 32], "0u0000000000000000000000000000000000000000000000000000"),
        // all 0xff (note the final data symbol is `g`, not `z`: it carries 4 zero pad bits)
        (&[0xff; 32], "0uzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzg"), // cspell:disable-line
        // 0x00..=0x1f
        (
            &[
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
                0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
                0x1c, 0x1d, 0x1e, 0x1f,
            ],
            "0u000g40r40m30e209185gr38e1w8124gk2gahc5rr34d1p70x3rfg",
        ),
        // (7 * i) mod 256
        (
            &[
                0x00, 0x07, 0x0e, 0x15, 0x1c, 0x23, 0x2a, 0x31, 0x38, 0x3f, 0x46, 0x4d, 0x54, 0x5b,
                0x62, 0x69, 0x70, 0x77, 0x7e, 0x85, 0x8c, 0x93, 0x9a, 0xa1, 0xa8, 0xaf, 0xb6, 0xbd,
                0xc4, 0xcb, 0xd2, 0xd9,
            ],
            "0u003gw58w4cn32e1z8s6n8pv2d5r7ezm5hj9sn8d8nyvbvh6btbcg",
        ),
    ];

    #[test]
    fn known_answer_vectors() {
        for &(hash, expected) in KATS {
            let id = encode_universal_account_id(hash);
            assert_eq!(id.as_str(), expected, "encode mismatch for {hash:?}");
            assert_eq!(id.len(), UAID_LEN);
        }
    }

    #[test]
    fn body_never_contains_forbidden_glyphs() {
        let forbidden = |c: char| matches!(c, 'i' | 'l' | 'o' | 'u');
        for &(hash, _) in KATS {
            let id = encode_universal_account_id(hash);
            assert!(!id.as_str()[UAID_PREFIX.len()..].contains(forbidden));
        }
        for seed in 0u8..64 {
            let id = encode_universal_account_id(&[seed.wrapping_mul(37); 32]);
            assert!(!id.as_str()[UAID_PREFIX.len()..].contains(forbidden));
        }
    }

    /// What the encoder emits is a universal account, and an edit that breaks the length,
    /// the prefix, the alphabet or the padding rule stops it being one. An edit that keeps
    /// all four is the address of a different hash, so it stays universal.
    #[test]
    fn an_edited_encoder_output_is_not_universal() {
        let account_type = |id: &str| id.parse::<AccountId>().ok().map(|id| id.get_account_type());
        let valid = encode_universal_account_id(&[0x11; 32]).as_str().to_owned();
        assert_eq!(account_type(&valid), Some(AccountType::UniversalAccount));

        // One symbol short, and one symbol too many.
        assert_eq!(account_type(&valid[..UAID_LEN - 1]), Some(AccountType::NamedAccount));
        assert_eq!(account_type(&format!("{valid}0")), Some(AccountType::NamedAccount));

        // Right length, wrong prefix. The all-hex bodies are the ones the other two
        // prefixed account types would take if their own length rule ever went.
        let mut wrong_prefix = valid.clone();
        wrong_prefix.replace_range(0..UAID_PREFIX.len(), "0s");
        assert_eq!(account_type(&wrong_prefix), Some(AccountType::NamedAccount));
        for prefix in ["0s", "0x"] {
            let hex_body = format!("{prefix}{}", "0".repeat(UAID_DATA_SYMBOLS));
            assert_eq!(hex_body.len(), UAID_LEN);
            assert_eq!(account_type(&hex_body), Some(AccountType::NamedAccount));
        }

        // A universal id inside a longer name is a named account.
        assert_eq!(account_type(&format!("{valid}.near")), Some(AccountType::NamedAccount));
        assert_eq!(account_type(&format!("sub.{valid}")), Some(AccountType::NamedAccount));

        // Every byte in a body position, so the alphabet rule is pinned whole rather than
        // at a few sampled characters. A symbol addresses another hash; anything else
        // either stops being an account id or becomes a named one.
        let middle = UAID_LEN / 2;
        for byte in 0..=u8::MAX {
            let mut bytes = valid.clone().into_bytes();
            bytes[middle] = byte;
            let Ok(edited) = String::from_utf8(bytes) else {
                continue;
            };
            match account_type(&edited) {
                Some(AccountType::UniversalAccount) => {
                    assert!(CROCKFORD.contains(&byte), "byte {byte} must not be universal")
                }
                Some(other) => {
                    assert!(!CROCKFORD.contains(&byte), "byte {byte} must stay universal");
                    assert_eq!(other, AccountType::NamedAccount, "byte {byte}");
                }
                None => assert!(!CROCKFORD.contains(&byte), "byte {byte} must parse"),
            }
        }

        // Every symbol in the final position. It carries one hash bit and four padding
        // bits, so only the two spellings that leave the padding clear are universal.
        let mut accepted = 0;
        for &symbol in CROCKFORD {
            let mut bytes = valid.clone().into_bytes();
            *bytes.last_mut().unwrap() = symbol;
            let edited = String::from_utf8(bytes).unwrap();
            if account_type(&edited) == Some(AccountType::UniversalAccount) {
                accepted += 1;
                assert!(
                    matches!(symbol, b'0' | b'g'),
                    "symbol {} left padding set",
                    symbol as char
                );
            } else {
                assert_eq!(account_type(&edited), Some(AccountType::NamedAccount));
            }
        }
        assert_eq!(accepted, 2);
    }

    /// What this encoder emits is what `AccountType` calls a universal account, so
    /// the two cannot drift apart.
    #[test]
    fn encoder_output_classifies_as_universal() {
        for &(hash, _) in KATS {
            let id = encode_universal_account_id(hash);
            assert_eq!(id.get_account_type(), AccountType::UniversalAccount, "{id}");
        }
        for seed in 0u8..64 {
            let id = encode_universal_account_id(&[seed.wrapping_mul(37); 32]);
            assert_eq!(id.get_account_type(), AccountType::UniversalAccount, "{id}");
        }

        let named: AccountId = "alice.near".parse().unwrap();
        assert_ne!(named.get_account_type(), AccountType::UniversalAccount);
    }
}

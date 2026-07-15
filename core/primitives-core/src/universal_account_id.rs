//! Codec for `0u` universal account ids (UAIDs).
//!
//! A UAID encodes a 32-byte hash as
//! `0u` + 52 Crockford-base32 symbols of the hash + a 6-symbol Bech32m BCH checksum
//! = 60 characters, all lowercase `[0-9a-z]`, which is a valid NEAR account id.
//!
//! This is a pure codec: it turns a hash into an address and back and validates
//! the checksum. Hashing a `StateInit` into the 32-byte input lives with the
//! account-id derivation, not here.
//!
//! The base32 and checksum are implemented in this module rather than taken from
//! an external crate. No existing crate fits the whole codec: the `bech32` crates
//! implement full Bech32/Bech32m framing and their own alphabet, which we don't
//! use, and a base32 crate would cover only the encoding half while the checksum
//! still lives here, so it would add a dependency to this foundational crate
//! without shrinking what we maintain. Keeping both halves together also avoids a
//! glyph-to-value pass, since they share one 5-bit symbol pipeline. The
//! implementation was cross-checked against the `data-encoding` crate over 40M
//! random cases with zero correctness divergence and on-par performance.
//!
//! The checksum reuses the Bech32m polymod from
//! [BIP-350](https://github.com/bitcoin/bips/blob/master/bip-0350.mediawiki).

// cspell:words bech polymod crockford uaid nbits kats multibyte

use crate::types::AccountId;

/// Scheme + hash-function marker. A different hash function gets a different letter.
pub const UAID_PREFIX: &str = "0u";
/// Base32 symbols encoding the 256-bit hash (`ceil(256 / 5)`).
pub const UAID_DATA_SYMBOLS: usize = 52;
/// Checksum symbols.
pub const UAID_CHECKSUM_SYMBOLS: usize = 6;
/// Total UAID length: prefix + data + checksum.
pub const UAID_LEN: usize = UAID_PREFIX.len() + UAID_DATA_SYMBOLS + UAID_CHECKSUM_SYMBOLS;

/// Crockford base32, lowercase, excluding `i l o u` to reduce transcription errors.
const CROCKFORD: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz"; // cspell:disable-line

/// Reverse lookup: ascii byte -> symbol value, or `-1` if not a Crockford symbol.
/// Strict: no uppercase and no digit/letter aliases, so decoding is canonical.
const DECODE: [i8; 256] = build_decode_table();

const fn build_decode_table() -> [i8; 256] {
    let mut table = [-1i8; 256];
    let mut i = 0;
    while i < 32 {
        table[CROCKFORD[i] as usize] = i as i8;
        i += 1;
    }
    table
}

/// Bech32m polymod generators and constant (BIP-350). We reuse only this math;
/// the symbols themselves are emitted in the Crockford alphabet, not Bech32's.
const GEN: [u32; 5] = [0x3b6a_57b2, 0x2650_8e6d, 0x1ea1_19fa, 0x3d42_33dd, 0x2a14_62b3];
const BECH32M_CONST: u32 = 0x2bc8_30a3;

/// `hrp_expand("0u")` per BIP-173: `[c >> 5 …] ++ [0] ++ [c & 31 …]`.
/// Assumes a 2-symbol prefix (checked in tests).
const HRP_EXPANDED: [u8; 5] = hrp_expanded();

const fn hrp_expanded() -> [u8; 5] {
    let hrp = UAID_PREFIX.as_bytes();
    [hrp[0] >> 5, hrp[1] >> 5, 0, hrp[0] & 31, hrp[1] & 31]
}

/// Error returned when parsing a `0u` universal account id.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum ParseUaidError {
    #[error("invalid universal account id length")]
    BadLength,
    #[error("invalid universal account id prefix")]
    BadPrefix,
    #[error("invalid character in universal account id")]
    InvalidSymbol,
    #[error("non-canonical universal account id encoding: padding bits set")]
    NonCanonical,
    #[error("universal account id checksum mismatch")]
    BadChecksum,
}

/// Encode a 32-byte hash as a `0u` universal account id.
pub fn encode_universal_account_id(hash: &[u8; 32]) -> AccountId {
    let data = base32_encode(hash);
    let checksum = create_checksum(&data);
    let mut s = String::with_capacity(UAID_LEN);
    s.push_str(UAID_PREFIX);
    for &v in data.iter().chain(checksum.iter()) {
        s.push(CROCKFORD[v as usize] as char);
    }
    debug_assert_eq!(s.len(), UAID_LEN);
    // Safe: the emitted charset and length are always a valid account id.
    s.parse::<AccountId>().expect("uaid codec must produce a valid account id")
}

/// Parse and fully validate a `0u` universal account id (prefix, length, charset,
/// checksum, canonicity), returning its 32-byte hash.
pub fn decode_universal_account_id(id: &str) -> Result<[u8; 32], ParseUaidError> {
    let bytes = id.as_bytes();
    if bytes.len() != UAID_LEN {
        return Err(ParseUaidError::BadLength);
    }
    if !id.starts_with(UAID_PREFIX) {
        return Err(ParseUaidError::BadPrefix);
    }
    let body = &bytes[UAID_PREFIX.len()..];
    let mut data = [0u8; UAID_DATA_SYMBOLS];
    for (slot, &c) in data.iter_mut().zip(&body[..UAID_DATA_SYMBOLS]) {
        *slot = decode_symbol(c)?;
    }
    let mut checksum = [0u8; UAID_CHECKSUM_SYMBOLS];
    for (slot, &c) in checksum.iter_mut().zip(&body[UAID_DATA_SYMBOLS..]) {
        *slot = decode_symbol(c)?;
    }
    if !verify_checksum(&data, &checksum) {
        return Err(ParseUaidError::BadChecksum);
    }
    base32_decode(&data)
}

/// Whether `id` is structurally a UAID with a valid checksum. No allocation.
/// Used later by account-type classification.
pub fn is_universal_account_id(id: &str) -> bool {
    decode_universal_account_id(id).is_ok()
}

fn decode_symbol(c: u8) -> Result<u8, ParseUaidError> {
    let v = DECODE[c as usize];
    if v < 0 {
        return Err(ParseUaidError::InvalidSymbol);
    }
    Ok(v as u8)
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

/// 52 symbol values -> 32 bytes. Rejects a non-canonical final symbol (padding bits set).
fn base32_decode(data: &[u8; UAID_DATA_SYMBOLS]) -> Result<[u8; 32], ParseUaidError> {
    let mut out = [0u8; 32];
    let mut acc: u32 = 0;
    let mut nbits: u32 = 0;
    let mut idx = 0;
    for &v in data {
        acc = (acc << 5) | v as u32;
        nbits += 5;
        while nbits >= 8 {
            nbits -= 8;
            out[idx] = ((acc >> nbits) & 0xff) as u8;
            idx += 1;
            acc &= (1u32 << nbits) - 1;
        }
    }
    debug_assert_eq!(idx, 32);
    debug_assert_eq!(nbits, 4);
    // The 4 leftover bits are padding and must be zero for a canonical encoding.
    if acc != 0 {
        return Err(ParseUaidError::NonCanonical);
    }
    Ok(out)
}

fn create_checksum(data: &[u8; UAID_DATA_SYMBOLS]) -> [u8; UAID_CHECKSUM_SYMBOLS] {
    let values = HRP_EXPANDED
        .iter()
        .copied()
        .chain(data.iter().copied())
        .chain([0u8; UAID_CHECKSUM_SYMBOLS]);
    let poly = polymod(values) ^ BECH32M_CONST;
    let mut out = [0u8; UAID_CHECKSUM_SYMBOLS];
    for (i, slot) in out.iter_mut().enumerate() {
        *slot = ((poly >> (5 * (5 - i))) & 0x1f) as u8;
    }
    out
}

fn verify_checksum(data: &[u8; UAID_DATA_SYMBOLS], checksum: &[u8; UAID_CHECKSUM_SYMBOLS]) -> bool {
    let values =
        HRP_EXPANDED.iter().copied().chain(data.iter().copied()).chain(checksum.iter().copied());
    polymod(values) == BECH32M_CONST
}

fn polymod(values: impl IntoIterator<Item = u8>) -> u32 {
    let mut chk: u32 = 1;
    for v in values {
        let b = chk >> 25;
        chk = ((chk & 0x01ff_ffff) << 5) ^ v as u32;
        for i in 0..5 {
            if (b >> i) & 1 == 1 {
                chk ^= GEN[i];
            }
        }
    }
    chk
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_constants() {
        // hrp_expanded() hardcodes a 2-symbol prefix.
        assert_eq!(UAID_PREFIX.len(), 2);
        // Stay within the 64-char account-id limit.
        assert_eq!(UAID_LEN, 60);
        assert!(UAID_LEN <= 64);
    }

    /// Canonical known-answer vectors, cross-checked against an independent
    /// BIP-350 bech32 reference over the Crockford alphabet. Keep these stable:
    /// they are reused by derivation tests and the NEP.
    const KATS: &[(&[u8; 32], &str)] = &[
        // all zero
        (&[0x00; 32], "0u00000000000000000000000000000000000000000000000000007d5yhp"),
        // all 0xff (note the final data symbol is `g`, not `z`: it carries 4 zero pad bits)
        (&[0xff; 32], "0uzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzg5k6fr1"), // cspell:disable-line
        // 0x00..=0x1f
        (
            &[
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
                0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
                0x1c, 0x1d, 0x1e, 0x1f,
            ],
            "0u000g40r40m30e209185gr38e1w8124gk2gahc5rr34d1p70x3rfgesewhf",
        ),
        // (7 * i) mod 256
        (
            &[
                0x00, 0x07, 0x0e, 0x15, 0x1c, 0x23, 0x2a, 0x31, 0x38, 0x3f, 0x46, 0x4d, 0x54, 0x5b,
                0x62, 0x69, 0x70, 0x77, 0x7e, 0x85, 0x8c, 0x93, 0x9a, 0xa1, 0xa8, 0xaf, 0xb6, 0xbd,
                0xc4, 0xcb, 0xd2, 0xd9,
            ],
            "0u003gw58w4cn32e1z8s6n8pv2d5r7ezm5hj9sn8d8nyvbvh6btbcgwk3mk3",
        ),
    ];

    #[test]
    fn known_answer_vectors() {
        for &(hash, expected) in KATS {
            let id = encode_universal_account_id(hash);
            assert_eq!(id.as_str(), expected, "encode mismatch for {hash:?}");
            assert_eq!(decode_universal_account_id(expected).unwrap(), *hash);
            assert_eq!(id.len(), UAID_LEN);
        }
    }

    #[test]
    fn round_trip() {
        for seed in 0u16..=255 {
            let mut hash = [0u8; 32];
            for (i, b) in hash.iter_mut().enumerate() {
                *b = (seed as u8).wrapping_add(i as u8).wrapping_mul(31);
            }
            let id = encode_universal_account_id(&hash);
            assert_eq!(decode_universal_account_id(id.as_str()).unwrap(), hash);
            assert!(is_universal_account_id(id.as_str()));
        }
    }

    #[test]
    fn rejects_single_symbol_substitution() {
        let id = encode_universal_account_id(&[0x42; 32]).as_str().to_owned();
        let original = id.into_bytes();
        for pos in UAID_PREFIX.len()..UAID_LEN {
            for &sym in CROCKFORD {
                if sym == original[pos] {
                    continue;
                }
                let mut corrupted = original.clone();
                corrupted[pos] = sym;
                let corrupted = String::from_utf8(corrupted).unwrap();
                assert!(
                    decode_universal_account_id(&corrupted).is_err(),
                    "substitution at {pos} to {} was accepted",
                    sym as char
                );
            }
        }
    }

    #[test]
    fn rejects_adjacent_transposition() {
        let id = encode_universal_account_id(&[0x9a; 32]).as_str().to_owned();
        let original = id.into_bytes();
        for pos in UAID_PREFIX.len()..UAID_LEN - 1 {
            if original[pos] == original[pos + 1] {
                continue; // swapping equal symbols is a no-op
            }
            let mut swapped = original.clone();
            swapped.swap(pos, pos + 1);
            let swapped = String::from_utf8(swapped).unwrap();
            assert!(
                decode_universal_account_id(&swapped).is_err(),
                "adjacent transposition at {pos} was accepted"
            );
        }
    }

    #[test]
    fn rejects_non_canonical_padding() {
        // The last data symbol holds 1 real hash bit (its top bit) and 4 padding bits
        // (its low 4 bits), which must be zero for a canonical encoding. Every non-zero
        // padding pattern must be rejected.
        for padding in 1..=0b1111 {
            let mut data = base32_encode(&[0x00; 32]); // last symbol starts at 0
            data[UAID_DATA_SYMBOLS - 1] = padding; // inject the padding bits under test
            let checksum = create_checksum(&data);

            let mut s = String::with_capacity(UAID_LEN);
            s.push_str(UAID_PREFIX);
            for &v in data.iter().chain(checksum.iter()) {
                s.push(CROCKFORD[v as usize] as char);
            }

            assert_eq!(
                decode_universal_account_id(&s),
                Err(ParseUaidError::NonCanonical),
                "padding pattern {padding:#06b} in the final data symbol was accepted",
            );
        }
    }

    #[test]
    fn rejects_structural_errors() {
        let valid = encode_universal_account_id(&[0x11; 32]).as_str().to_owned();

        // Wrong length.
        assert_eq!(
            decode_universal_account_id(&valid[..UAID_LEN - 1]),
            Err(ParseUaidError::BadLength)
        );
        assert_eq!(
            decode_universal_account_id(&format!("{valid}0")),
            Err(ParseUaidError::BadLength)
        );

        // Wrong prefix, same length.
        let mut wrong_prefix = valid.clone();
        wrong_prefix.replace_range(0..2, "0s");
        assert_eq!(decode_universal_account_id(&wrong_prefix), Err(ParseUaidError::BadPrefix));

        // Out-of-alphabet characters in the body: the excluded glyphs and uppercase.
        for bad in ['i', 'l', 'o', 'u', 'A', 'Z'] {
            let mut bytes = valid.clone().into_bytes();
            bytes[UAID_PREFIX.len()] = bad as u8;
            let s = String::from_utf8(bytes).unwrap();
            assert_eq!(
                decode_universal_account_id(&s),
                Err(ParseUaidError::InvalidSymbol),
                "char {bad} should be rejected"
            );
        }
    }

    #[test]
    fn rejects_bad_checksum_by_variant() {
        // Flip a checksum symbol within the alphabet: the data stays canonical, so
        // only the checksum can reject, pinning the exact variant.
        let mut bytes = encode_universal_account_id(&[0x11; 32]).as_str().to_owned().into_bytes();
        let last = UAID_LEN - 1;
        bytes[last] = if bytes[last] == b'z' { b'0' } else { b'z' };
        let s = String::from_utf8(bytes).unwrap();
        assert_eq!(decode_universal_account_id(&s), Err(ParseUaidError::BadChecksum));
    }

    #[test]
    fn rejects_more_structural_errors() {
        let valid = encode_universal_account_id(&[0x11; 32]).as_str().to_owned();

        // Empty string.
        assert_eq!(decode_universal_account_id(""), Err(ParseUaidError::BadLength));

        // Too long via a multibyte char (byte length, not char count, matters).
        assert_eq!(
            decode_universal_account_id(&format!("{valid}é")),
            Err(ParseUaidError::BadLength)
        );

        // Exactly UAID_LEN bytes but with a multibyte char in the body.
        let body_tail = &valid[UAID_PREFIX.len() + 2..];
        let multibyte = format!("{UAID_PREFIX}é{body_tail}"); // 'é' occupies 2 bytes
        assert_eq!(multibyte.len(), UAID_LEN);
        assert_eq!(decode_universal_account_id(&multibyte), Err(ParseUaidError::InvalidSymbol));

        // Uppercase body: Crockford decoding is strict lowercase, no aliases.
        let upper = format!("{UAID_PREFIX}{}", valid[UAID_PREFIX.len()..].to_uppercase());
        assert_eq!(upper.len(), UAID_LEN);
        assert_eq!(decode_universal_account_id(&upper), Err(ParseUaidError::InvalidSymbol));
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

    #[test]
    fn is_universal_agrees_with_decode() {
        let valid = encode_universal_account_id(&[0x33; 32]).as_str().to_owned();
        assert!(is_universal_account_id(&valid));
        assert!(!is_universal_account_id("alice.near"));
        assert!(!is_universal_account_id(&valid[..UAID_LEN - 1]));

        // A single flipped checksum symbol is not a UAID.
        let mut bytes = valid.into_bytes();
        let last = UAID_LEN - 1;
        bytes[last] = if bytes[last] == b'0' { b'1' } else { b'0' };
        let bad = String::from_utf8(bytes).unwrap();
        assert!(!is_universal_account_id(&bad));
    }
}

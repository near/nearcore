//! The wallet's checks that need no host function, so that they can be tested on the host.

/// Nonces are block heights times this, as for access key nonces.
pub const NONCE_MULTIPLIER: u64 = 1_000_000;

/// The size of a SHA-256 digest.
pub const HASH_LEN: usize = 32;

/// WebAuthn's user-present flag, bit 0 of the flags byte of the authenticator data.
const USER_PRESENT: u8 = 0x01;

/// The authenticator data's rpIdHash (32 bytes), flags (1) and signCount (4).
const MIN_AUTHENTICATOR_DATA_LEN: usize = HASH_LEN + 1 + 4;

/// Why the wallet refuses a call. The contract panics with the message, which refuses the call at
/// no cost to the wallet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    MalformedPayload,
    MalformedRequest,
    UnsupportedVersion,
    WrongChain,
    WrongContract,
    Expired,
    MalformedAuthenticatorData,
    WrongRelyingParty,
    UserNotPresent,
    WrongClientDataType,
    WrongChallenge,
    BadSignature,
    NonceTooLow,
    NonceTooHigh,
    MalformedState,
}

impl Refusal {
    pub fn message(self) -> &'static str {
        match self {
            Refusal::MalformedPayload => "malformed payload",
            Refusal::MalformedRequest => "malformed request",
            Refusal::UnsupportedVersion => "unsupported request version",
            Refusal::WrongChain => "request is for another chain",
            Refusal::WrongContract => "request is for another contract",
            Refusal::Expired => "request expired",
            Refusal::MalformedAuthenticatorData => "malformed authenticator data",
            Refusal::WrongRelyingParty => "authenticator data is for another relying party",
            Refusal::UserNotPresent => "user presence flag not set",
            Refusal::WrongClientDataType => "client data is not a webauthn.get assertion",
            Refusal::WrongChallenge => "client data challenge does not match the request",
            Refusal::BadSignature => "invalid signature",
            Refusal::NonceTooLow => "nonce too low",
            Refusal::NonceTooHigh => "nonce too high",
            Refusal::MalformedState => "wallet state is malformed",
        }
    }
}

/// A request may run up to and including its expiry height.
pub fn check_expiry(expiry_height: u64, block_height: u64) -> Result<(), Refusal> {
    if block_height > expiry_height {
        return Err(Refusal::Expired);
    }
    Ok(())
}

/// Checks the authenticator data comes from the wallet's relying party, with the user present.
/// The signCount is ignored: passkeys often report 0.
pub fn check_authenticator_data(data: &[u8], rp_id_hash: &[u8]) -> Result<(), Refusal> {
    if data.len() < MIN_AUTHENTICATOR_DATA_LEN {
        return Err(Refusal::MalformedAuthenticatorData);
    }
    if &data[..HASH_LEN] != rp_id_hash {
        return Err(Refusal::WrongRelyingParty);
    }
    if data[HASH_LEN] & USER_PRESENT == 0 {
        return Err(Refusal::UserNotPresent);
    }
    Ok(())
}

/// Checks the client data is an assertion over `challenge`, the hash of the signed request.
///
/// This looks for the two fields as they appear in a browser's serialization, with no JSON
/// parse. That is enough for an example, since the signature covers the client data; a real
/// wallet should parse it and check the origin too.
pub fn check_client_data(json: &[u8], challenge: &[u8; HASH_LEN]) -> Result<(), Refusal> {
    if !contains(json, br#""type":"webauthn.get""#) {
        return Err(Refusal::WrongClientDataType);
    }
    let field = format!(r#""challenge":"{}""#, base64url(challenge));
    if !contains(json, field.as_bytes()) {
        return Err(Refusal::WrongChallenge);
    }
    Ok(())
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|window| window == needle)
}

/// Base64url without padding (RFC 4648 section 5), as WebAuthn encodes the challenge.
pub fn base64url(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = u32::from_be_bytes([0, b[0], b[1], b[2]]);
        // A chunk of k bytes gives k + 1 characters.
        for i in 0..=chunk.len() {
            let index = (n >> (18 - 6 * i)) & 0x3f;
            out.push(ALPHABET[index as usize] as char);
        }
    }
    out
}

/// What a call does with the wallet's nonce.
#[derive(Debug, PartialEq, Eq)]
pub enum NonceUpdate {
    /// The wallet's first call: store this floor and run nothing.
    SetFloor(u64),
    /// Store the request's nonce and run the request.
    Use(u64),
}

/// The nonce rules. `stored` is the wallet's nonce, `None` before its first call.
///
/// The first call sets the floor to `block_height * NONCE_MULTIPLIER`. Later calls need a nonce
/// above the stored one and below `(block_height + 1) * NONCE_MULTIPLIER`, so that every nonce
/// the wallet accepts is below the floor a re-created wallet sets. The README explains why.
pub fn check_nonce(
    stored: Option<u64>,
    nonce: u64,
    block_height: u64,
) -> Result<NonceUpdate, Refusal> {
    let Some(stored) = stored else {
        return Ok(NonceUpdate::SetFloor(block_height.saturating_mul(NONCE_MULTIPLIER)));
    };
    if nonce <= stored {
        return Err(Refusal::NonceTooLow);
    }
    if nonce >= block_height.saturating_add(1).saturating_mul(NONCE_MULTIPLIER) {
        return Err(Refusal::NonceTooHigh);
    }
    Ok(NonceUpdate::Use(nonce))
}

/// The wallet's nonce as `get_nonce` returns it: a JSON string of the decimal number, as NEAR
/// encodes u64s for JavaScript clients, or `null` before the first call.
pub fn nonce_json(stored: Option<u64>) -> String {
    match stored {
        Some(nonce) => format!("\"{nonce}\""),
        None => "null".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base64url() {
        // RFC 4648 test vectors, without padding.
        let vectors: [(&[u8], &str); 7] = [
            (b"", ""),
            (b"f", "Zg"),
            (b"fo", "Zm8"),
            (b"foo", "Zm9v"),
            (b"foob", "Zm9vYg"),
            (b"fooba", "Zm9vYmE"),
            (b"foobar", "Zm9vYmFy"),
        ];
        for (input, output) in vectors {
            assert_eq!(base64url(input), output);
        }
        // The url-safe characters, where standard base64 has '+' and '/'.
        assert_eq!(base64url(&[0xfb, 0xff]), "-_8");
    }

    #[test]
    fn test_nonce_json() {
        assert_eq!(nonce_json(None), "null");
        assert_eq!(nonce_json(Some(0)), "\"0\"");
        assert_eq!(nonce_json(Some(u64::MAX)), "\"18446744073709551615\"");
    }

    #[test]
    fn test_expiry() {
        assert_eq!(check_expiry(10, 9), Ok(()));
        assert_eq!(check_expiry(10, 10), Ok(()));
        assert_eq!(check_expiry(10, 11), Err(Refusal::Expired));
    }

    fn authenticator_data(rp_id_hash: [u8; 32], flags: u8) -> Vec<u8> {
        let mut data = rp_id_hash.to_vec();
        data.push(flags);
        data.extend_from_slice(&0u32.to_be_bytes());
        data
    }

    #[test]
    fn test_authenticator_data() {
        let rp = [7; 32];
        assert_eq!(check_authenticator_data(&authenticator_data(rp, 0x05), &rp), Ok(()));
        assert_eq!(
            check_authenticator_data(&authenticator_data([8; 32], 0x05), &rp),
            Err(Refusal::WrongRelyingParty)
        );
        assert_eq!(
            check_authenticator_data(&authenticator_data(rp, 0x04), &rp),
            Err(Refusal::UserNotPresent)
        );
        let short = &authenticator_data(rp, 0x05)[..36];
        assert_eq!(check_authenticator_data(short, &rp), Err(Refusal::MalformedAuthenticatorData));
    }

    #[test]
    fn test_client_data() {
        let challenge = [0xfb; 32];
        let encoded = base64url(&challenge);
        let json = format!(
            r#"{{"type":"webauthn.get","challenge":"{encoded}","origin":"https://example.com","crossOrigin":false}}"#
        );
        assert_eq!(check_client_data(json.as_bytes(), &challenge), Ok(()));

        let create = json.replace("webauthn.get", "webauthn.create");
        assert_eq!(
            check_client_data(create.as_bytes(), &challenge),
            Err(Refusal::WrongClientDataType)
        );
        assert_eq!(check_client_data(json.as_bytes(), &[0; 32]), Err(Refusal::WrongChallenge));
        // A challenge that only starts with the right one.
        let longer = json.replace(&encoded, &format!("{encoded}A"));
        assert_eq!(check_client_data(longer.as_bytes(), &challenge), Err(Refusal::WrongChallenge));
    }

    #[test]
    fn test_first_call_sets_the_floor() {
        assert_eq!(check_nonce(None, 0, 42), Ok(NonceUpdate::SetFloor(42 * NONCE_MULTIPLIER)));
        // The request's nonce doesn't matter on the first call.
        assert_eq!(
            check_nonce(None, u64::MAX, 42),
            Ok(NonceUpdate::SetFloor(42 * NONCE_MULTIPLIER))
        );
    }

    #[test]
    fn test_nonce_bounds() {
        let floor = 42 * NONCE_MULTIPLIER;
        assert_eq!(check_nonce(Some(floor), floor, 50), Err(Refusal::NonceTooLow));
        assert_eq!(check_nonce(Some(floor), floor - 1, 50), Err(Refusal::NonceTooLow));
        assert_eq!(check_nonce(Some(floor), floor + 1, 50), Ok(NonceUpdate::Use(floor + 1)));
        let limit = 51 * NONCE_MULTIPLIER;
        assert_eq!(check_nonce(Some(floor), limit - 1, 50), Ok(NonceUpdate::Use(limit - 1)));
        assert_eq!(check_nonce(Some(floor), limit, 50), Err(Refusal::NonceTooHigh));
    }

    /// A wallet re-created at a later height refuses every nonce the old one could accept.
    #[test]
    fn test_floor_is_above_every_earlier_nonce() {
        let old_height = 50;
        let largest_old_nonce = (old_height + 1) * NONCE_MULTIPLIER - 1;
        assert!(check_nonce(Some(0), largest_old_nonce, old_height).is_ok());
        let NonceUpdate::SetFloor(floor) = check_nonce(None, 0, old_height + 1).unwrap() else {
            panic!("the first call sets the floor");
        };
        assert_eq!(check_nonce(Some(floor), largest_old_nonce, 60), Err(Refusal::NonceTooLow));
    }
}

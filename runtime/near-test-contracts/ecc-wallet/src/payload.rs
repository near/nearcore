//! The bytes an external call to the wallet carries. The README documents the byte layout.

use crate::checks::Refusal;
use borsh::{BorshDeserialize, BorshSerialize};

/// The only request version the wallet accepts, the first byte of a [`SignedRequest`].
pub const REQUEST_VERSION: u8 = 0;

/// The payload of an external call: a request and the passkey's assertion over it.
#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq, Eq)]
pub struct ExternalCallPayload {
    /// `borsh(SignedRequest)`: the exact bytes the passkey's challenge commits to. They are kept
    /// as bytes so that the wallet hashes what the client signed, not a re-encoding of it.
    pub signed: Vec<u8>,
    /// WebAuthn authenticator data: rpIdHash (32) || flags (1) || signCount (4) || ...
    pub authenticator_data: Vec<u8>,
    /// WebAuthn clientDataJSON, as the authenticator returned it.
    pub client_data_json: Vec<u8>,
    /// ECDSA P-256 signature, r || s, each 32 bytes big-endian.
    pub signature: [u8; 64],
}

impl ExternalCallPayload {
    /// Parses a call's payload. Trailing bytes are refused.
    pub fn parse(bytes: &[u8]) -> Result<Self, Refusal> {
        Self::try_from_slice(bytes).map_err(|_| Refusal::MalformedPayload)
    }
}

/// What the passkey signs: the request, and what binds it to one chain, one contract and one
/// point in time.
#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq, Eq)]
pub struct SignedRequest {
    /// [`REQUEST_VERSION`].
    pub version: u8,
    /// Must equal the chain id the wallet runs on.
    pub chain_id: String,
    /// Must equal the wallet's account id.
    pub contract_id: String,
    /// See the README's section on nonces.
    pub nonce: u64,
    /// The last block height, as the call sees it, at which the request may run.
    pub expiry_height: u64,
    /// The account the actions go to.
    pub receiver_id: String,
    /// Sent as one batch of actions. A request with no actions sends nothing, and only uses up
    /// its nonce.
    pub actions: Vec<WalletAction>,
}

impl SignedRequest {
    /// Parses the signed bytes. The version is checked first, so that a request of another
    /// version is refused as such rather than as malformed. Trailing bytes are refused.
    pub fn parse(bytes: &[u8]) -> Result<Self, Refusal> {
        match bytes.first() {
            Some(&REQUEST_VERSION) => {}
            Some(_) => return Err(Refusal::UnsupportedVersion),
            None => return Err(Refusal::MalformedRequest),
        }
        Self::try_from_slice(bytes).map_err(|_| Refusal::MalformedRequest)
    }
}

/// An action the wallet sends on the owner's behalf.
#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq, Eq)]
pub enum WalletAction {
    Transfer { deposit: u128 },
    FunctionCall { method_name: String, args: Vec<u8>, deposit: u128, gas: u64 },
    DeleteAccount { beneficiary_id: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> SignedRequest {
        SignedRequest {
            version: REQUEST_VERSION,
            chain_id: "localnet".to_string(),
            contract_id: "wallet.near".to_string(),
            nonce: 7,
            expiry_height: 100,
            receiver_id: "bob.near".to_string(),
            actions: vec![
                WalletAction::Transfer { deposit: 1 },
                WalletAction::FunctionCall {
                    method_name: "go".to_string(),
                    args: vec![1, 2],
                    deposit: 0,
                    gas: 5,
                },
                WalletAction::DeleteAccount { beneficiary_id: "bob.near".to_string() },
            ],
        }
    }

    fn payload() -> ExternalCallPayload {
        ExternalCallPayload {
            signed: borsh::to_vec(&request()).unwrap(),
            authenticator_data: vec![3; 37],
            client_data_json: b"{}".to_vec(),
            signature: [9; 64],
        }
    }

    #[test]
    fn test_parse_round_trip() {
        let bytes = borsh::to_vec(&payload()).unwrap();
        let parsed = ExternalCallPayload::parse(&bytes).unwrap();
        assert_eq!(parsed, payload());
        assert_eq!(SignedRequest::parse(&parsed.signed).unwrap(), request());
    }

    /// The layout the README documents, built by hand.
    #[test]
    fn test_request_layout() {
        let mut expected = vec![REQUEST_VERSION];
        for string in ["localnet", "wallet.near"] {
            expected.extend_from_slice(&(string.len() as u32).to_le_bytes());
            expected.extend_from_slice(string.as_bytes());
        }
        expected.extend_from_slice(&7u64.to_le_bytes());
        expected.extend_from_slice(&100u64.to_le_bytes());
        expected.extend_from_slice(&8u32.to_le_bytes());
        expected.extend_from_slice(b"bob.near");
        expected.extend_from_slice(&3u32.to_le_bytes());
        // Transfer.
        expected.push(0);
        expected.extend_from_slice(&1u128.to_le_bytes());
        // FunctionCall.
        expected.push(1);
        expected.extend_from_slice(&2u32.to_le_bytes());
        expected.extend_from_slice(b"go");
        expected.extend_from_slice(&2u32.to_le_bytes());
        expected.extend_from_slice(&[1, 2]);
        expected.extend_from_slice(&0u128.to_le_bytes());
        expected.extend_from_slice(&5u64.to_le_bytes());
        // DeleteAccount.
        expected.push(2);
        expected.extend_from_slice(&8u32.to_le_bytes());
        expected.extend_from_slice(b"bob.near");
        assert_eq!(borsh::to_vec(&request()).unwrap(), expected);
    }

    /// The payload's signature is a fixed 64 bytes with no length prefix.
    #[test]
    fn test_payload_layout() {
        let payload = ExternalCallPayload {
            signed: vec![1],
            authenticator_data: vec![2, 2],
            client_data_json: vec![3],
            signature: [4; 64],
        };
        let mut expected = vec![];
        expected.extend_from_slice(&1u32.to_le_bytes());
        expected.push(1);
        expected.extend_from_slice(&2u32.to_le_bytes());
        expected.extend_from_slice(&[2, 2]);
        expected.extend_from_slice(&1u32.to_le_bytes());
        expected.push(3);
        expected.extend_from_slice(&[4; 64]);
        assert_eq!(borsh::to_vec(&payload).unwrap(), expected);
    }

    #[test]
    fn test_trailing_bytes_refused() {
        let mut bytes = borsh::to_vec(&payload()).unwrap();
        bytes.push(0);
        assert_eq!(ExternalCallPayload::parse(&bytes), Err(Refusal::MalformedPayload));

        let mut signed = borsh::to_vec(&request()).unwrap();
        signed.push(0);
        assert_eq!(SignedRequest::parse(&signed), Err(Refusal::MalformedRequest));
    }

    #[test]
    fn test_truncated_payload_refused() {
        let bytes = borsh::to_vec(&payload()).unwrap();
        let truncated = &bytes[..bytes.len() - 1];
        assert_eq!(ExternalCallPayload::parse(truncated), Err(Refusal::MalformedPayload));
        assert_eq!(ExternalCallPayload::parse(&[]), Err(Refusal::MalformedPayload));
    }

    #[test]
    fn test_request_version() {
        let mut signed = borsh::to_vec(&request()).unwrap();
        signed[0] = REQUEST_VERSION + 1;
        assert_eq!(SignedRequest::parse(&signed), Err(Refusal::UnsupportedVersion));
        assert_eq!(SignedRequest::parse(&[]), Err(Refusal::MalformedRequest));
    }

    #[test]
    fn test_unknown_action_refused() {
        let mut signed = borsh::to_vec(&SignedRequest { actions: vec![], ..request() }).unwrap();
        // One action, with a tag no action has.
        let count = signed.len() - 4;
        signed[count..].copy_from_slice(&1u32.to_le_bytes());
        signed.push(3);
        assert_eq!(SignedRequest::parse(&signed), Err(Refusal::MalformedRequest));
    }
}

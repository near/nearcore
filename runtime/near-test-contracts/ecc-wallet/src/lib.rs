//! An example wallet contract for external contract calls (ECCs).
//!
//! NOT FOR PRODUCTION USE: see the README, which also documents the payload layout, the wallet's
//! state and the nonce rules.
//!
//! The wallet runs requests signed by a WebAuthn passkey on P-256, a key a NEAR access key can't
//! be. An external call carries the request and the passkey's assertion; `__external_call` checks
//! them and sends the request's actions. A refused call costs the wallet nothing.

// Off wasm, only the unit tests use the checks and the payload types.
#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]

mod checks;
#[cfg(target_arch = "wasm32")]
mod host;
mod payload;

#[cfg(target_arch = "wasm32")]
mod contract {
    use crate::checks::{
        NonceUpdate, Refusal, check_authenticator_data, check_client_data, check_expiry,
        check_nonce, nonce_json,
    };
    use crate::host;
    use crate::payload::{ExternalCallPayload, SignedRequest, WalletAction};

    /// The passkey's compressed SEC1 P-256 public key, 33 bytes. Set by the state init.
    const PUBLIC_KEY_KEY: &[u8] = b"pk";
    /// SHA-256 of the WebAuthn relying party id, 32 bytes. Set by the state init.
    const RP_ID_HASH_KEY: &[u8] = b"rp";
    /// The last nonce used, u64 little-endian. Set by the first call.
    const NONCE_KEY: &[u8] = b"nonce";

    /// The method an external call runs. Returning accepts the call, and panicking refuses it.
    #[unsafe(no_mangle)]
    pub extern "C" fn __external_call() {
        if let Err(refusal) = run() {
            host::refuse(refusal.message());
        }
    }

    /// A view method that returns the wallet's nonce as JSON: a decimal string, or `null` before
    /// the first call has set the floor. Pick a nonce above it for the next request.
    #[unsafe(no_mangle)]
    pub extern "C" fn get_nonce() {
        match stored_nonce() {
            Ok(nonce) => host::value_return(nonce_json(nonce).as_bytes()),
            Err(refusal) => host::refuse(refusal.message()),
        }
    }

    /// The wallet's nonce, or `None` before the first call.
    fn stored_nonce() -> Result<Option<u64>, Refusal> {
        let Some(bytes) = host::storage_read(NONCE_KEY) else {
            return Ok(None);
        };
        let bytes = bytes.try_into().map_err(|_| Refusal::MalformedState)?;
        Ok(Some(u64::from_le_bytes(bytes)))
    }

    fn run() -> Result<(), Refusal> {
        let payload = ExternalCallPayload::parse(&host::input())?;
        let request = SignedRequest::parse(&payload.signed)?;

        if request.chain_id.as_bytes() != host::chain_id() {
            return Err(Refusal::WrongChain);
        }
        if request.contract_id.as_bytes() != host::current_account_id() {
            return Err(Refusal::WrongContract);
        }
        let block_height = host::block_height();
        check_expiry(request.expiry_height, block_height)?;

        check_passkey(&payload)?;

        match check_nonce(stored_nonce()?, request.nonce, block_height)? {
            NonceUpdate::SetFloor(floor) => {
                host::storage_write(NONCE_KEY, &floor.to_le_bytes());
                host::log(&format!("nonce floor set to {floor}; request not run"));
            }
            NonceUpdate::Use(nonce) => {
                host::storage_write(NONCE_KEY, &nonce.to_le_bytes());
                send(&request);
            }
        }
        Ok(())
    }

    /// Checks the WebAuthn assertion: the authenticator data, the client data's challenge, and
    /// the signature over `sha256(authenticator_data || sha256(client_data_json))`.
    fn check_passkey(payload: &ExternalCallPayload) -> Result<(), Refusal> {
        let rp_id_hash = host::storage_read(RP_ID_HASH_KEY).ok_or(Refusal::MalformedState)?;
        check_authenticator_data(&payload.authenticator_data, &rp_id_hash)?;

        let challenge = host::sha256(&payload.signed);
        check_client_data(&payload.client_data_json, &challenge)?;

        let mut signed_data = payload.authenticator_data.clone();
        signed_data.extend_from_slice(&host::sha256(&payload.client_data_json));
        let digest = host::sha256(&signed_data);
        let public_key = host::storage_read(PUBLIC_KEY_KEY).ok_or(Refusal::MalformedState)?;
        if !host::p256_verify(&payload.signature, &digest, &public_key) {
            return Err(Refusal::BadSignature);
        }
        Ok(())
    }

    /// Sends the request's actions as one batch. A request with no actions sends nothing.
    fn send(request: &SignedRequest) {
        if request.actions.is_empty() {
            return;
        }
        let promise = host::promise_batch_create(&request.receiver_id);
        for action in &request.actions {
            match action {
                WalletAction::Transfer { deposit } => {
                    host::promise_batch_action_transfer(promise, *deposit);
                }
                WalletAction::FunctionCall { method_name, args, deposit, gas } => {
                    host::promise_batch_action_function_call(
                        promise,
                        method_name,
                        args,
                        *deposit,
                        *gas,
                    );
                }
                WalletAction::DeleteAccount { beneficiary_id } => {
                    host::promise_batch_action_delete_account(promise, beneficiary_id);
                }
            }
        }
    }
}

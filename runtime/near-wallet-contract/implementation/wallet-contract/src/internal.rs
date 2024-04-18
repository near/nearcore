use crate::{
    error::{AccountIdError, CallerError, Error, RelayerError, UserError},
    eth_emulation, ethabi_utils, near_action,
    types::{
        Action, ExecutionContext, TransactionValidationOutcome, ADD_KEY_SELECTOR,
        ADD_KEY_SIGNATURE, DELETE_KEY_SELECTOR, DELETE_KEY_SIGNATURE, FUNCTION_CALL_SELECTOR,
        FUNCTION_CALL_SIGNATURE, TRANSFER_SELECTOR, TRANSFER_SIGNATURE,
    },
};
use aurora_engine_transactions::{EthTransactionKind, NormalizedEthTransaction};
use base64::Engine;
use ethabi::{ethereum_types::U256, Address};
use near_sdk::{AccountId, NearToken};

// TODO(eth-implicit): Decide on chain id.
pub const CHAIN_ID: u64 = std::include!("CHAIN_ID");
const U64_MAX: U256 = U256([u64::MAX, 0, 0, 0]);
/// Only up to this amount of yoctoNear can be directly mentioned in an action,
/// the rest should be included in the `value` field of the Ethereum transaction.
pub const MAX_YOCTO_NEAR: u32 = 1_000_000;

/// The largest accepted `value` field of a transaction.
/// Computed as `(2**128 - 1) // 1_000_000` since Near balances are
/// 128-bit numbers, but with 24 decimals instead of 18. So to covert
/// an Ethereum transaction value into a Near value we need to multiply
/// by `1_000_000` and then add back any lower digits that were truncated.
const VALUE_MAX: U256 = U256([10175519178963368024, 18446744073709, 0, 0]);

/// Given an RLP-encoded Ethereum transaction (bytes encoded in base64),
/// a Near account the transaction is supposed to interact with, the current
/// account ID, and the current nonce, this function will attempt to transform
/// the Ethereum transaction into a Near action.
pub fn parse_rlp_tx_to_action(
    tx_bytes_b64: &str,
    target: &AccountId,
    context: &ExecutionContext,
    expected_nonce: &mut u64,
) -> Result<(near_action::Action, TransactionValidationOutcome), Error> {
    let tx_bytes = decode_b64(tx_bytes_b64)?;
    let tx_kind: EthTransactionKind = tx_bytes.as_slice().try_into()?;
    let tx: NormalizedEthTransaction = tx_kind.try_into()?;
    let validation_outcome = validate_tx_relayer_data(&tx, target, context, *expected_nonce)?;

    // If the transaction is valid then increment the nonce to prevent replay
    *expected_nonce = expected_nonce.saturating_add(1);

    let to = tx.to.ok_or(Error::User(UserError::EvmDeployDisallowed))?.raw();
    let action = if to != context.current_address
        && extract_address(target).map(|a| a == to).unwrap_or(false)
    {
        // If target is another Ethereum implicit account then the action
        // must be a transfer (because EOAs are not contracts on Ethereum).
        Action::Transfer { receiver_id: target.to_string(), yocto_near: 0 }
    } else {
        parse_tx_data(target, &tx, context)?
    };
    validate_tx_value(&tx, context, &action)?;

    // Call to `low_u128` here is safe because of the validation done in `validate_tx_value`
    let near_action = action
        .try_into_near_action(tx.value.raw().low_u128().saturating_mul(MAX_YOCTO_NEAR.into()))?;

    Ok((near_action, validation_outcome))
}

/// Extracts a 20-byte address from a Near account ID.
/// This is done by assuming the account ID is of the form `^0x[0-9a-f]{40}`,
/// i.e. it starts with `0x` and then hex-encoded 20 bytes.
pub fn extract_address(current_account_id: &AccountId) -> Result<Address, Error> {
    let hex_str = current_account_id.as_bytes();

    // The length must be at least 42 characters because it begins with
    // `0x` and then a 20-byte hex-encoded string. In production it will
    // be exactly 42 characters because eth-implicit accounts will always
    // be top-level, but for testing we may have them be sub-accounts.
    // In this case then the length will be longer than 42 characters.
    if hex_str.len() < 42 {
        return Err(Error::AccountId(AccountIdError::AccountIdTooShort));
    }

    if &hex_str[0..2] != b"0x" {
        return Err(Error::AccountId(AccountIdError::Missing0xPrefix));
    }

    let mut bytes = [0u8; 20];
    hex::decode_to_slice(&hex_str[2..42], &mut bytes)
        .map_err(|_| Error::AccountId(AccountIdError::InvalidHex))?;

    Ok(bytes.into())
}

/// Decode a base-64 encoded string into raw bytes.
fn decode_b64(input: &str) -> Result<Vec<u8>, Error> {
    let engine = base64::engine::general_purpose::STANDARD;
    engine.decode(input).map_err(|_| Error::Relayer(RelayerError::InvalidBase64))
}

/// Coverts any Near account ID into a 20-byte address by taking the last 20 bytes
/// of the keccak256 hash.
pub fn account_id_to_address(account_id: &AccountId) -> Address {
    let hash = keccak256(account_id.as_bytes());
    let mut result = [0u8; 20];
    result.copy_from_slice(&hash[12..32]);
    result.into()
}

pub fn keccak256(bytes: &[u8]) -> [u8; 32] {
    #[cfg(test)]
    {
        use sha3::{Digest, Keccak256};
        let hash = Keccak256::digest(bytes);
        hash.into()
    }

    #[cfg(not(test))]
    near_sdk::env::keccak256_array(bytes)
}

fn parse_tx_data(
    target: &AccountId,
    tx: &NormalizedEthTransaction,
    context: &ExecutionContext,
) -> Result<Action, Error> {
    if tx.data.len() < 4 {
        return Err(Error::User(UserError::InvalidAbiEncodedData));
    }
    match &tx.data[0..4] {
        FUNCTION_CALL_SELECTOR => {
            let (receiver_id, method_name, args, gas, yocto_near): (String, _, _, _, _) =
                ethabi_utils::abi_decode(&FUNCTION_CALL_SIGNATURE, &tx.data[4..])?;
            if target.as_str() != receiver_id.as_str() {
                return Err(Error::Relayer(RelayerError::InvalidTarget));
            }
            if yocto_near > MAX_YOCTO_NEAR {
                return Err(Error::User(UserError::ExcessYoctoNear));
            }
            Ok(Action::FunctionCall { receiver_id, method_name, args, gas, yocto_near })
        }
        TRANSFER_SELECTOR => {
            let (receiver_id, yocto_near): (String, u32) =
                ethabi_utils::abi_decode(&TRANSFER_SIGNATURE, &tx.data[4..])?;
            if target.as_str() != receiver_id.as_str() {
                return Err(Error::Relayer(RelayerError::InvalidTarget));
            }
            if yocto_near > MAX_YOCTO_NEAR {
                return Err(Error::User(UserError::ExcessYoctoNear));
            }
            Ok(Action::Transfer { receiver_id, yocto_near })
        }
        ADD_KEY_SELECTOR => {
            let (
                public_key_kind,
                public_key,
                nonce,
                is_full_access,
                is_limited_allowance,
                allowance,
                receiver_id,
                method_names,
            ) = ethabi_utils::abi_decode(&ADD_KEY_SIGNATURE, &tx.data[4..])?;
            Ok(Action::AddKey {
                public_key_kind,
                public_key,
                nonce,
                is_full_access,
                is_limited_allowance,
                allowance,
                receiver_id,
                method_names,
            })
        }
        DELETE_KEY_SELECTOR => {
            let (public_key_kind, public_key) =
                ethabi_utils::abi_decode(&DELETE_KEY_SIGNATURE, &tx.data[4..])?;
            Ok(Action::DeleteKey { public_key_kind, public_key })
        }
        _ => eth_emulation::try_emulation(target, tx, context),
    }
}

/// Validates the transaction is following the Wallet Contract protocol.
/// This includes checks for:
/// - from address matches current account address
/// - to address is present and matches the target address (or hash of target account ID)
/// - nonce matches expected nonce
/// If this validation fails then the relayer that sent it is faulty and should be banned.
fn validate_tx_relayer_data(
    tx: &NormalizedEthTransaction,
    target: &AccountId,
    context: &ExecutionContext,
    expected_nonce: u64,
) -> Result<TransactionValidationOutcome, Error> {
    if tx.address.raw() != context.current_address {
        return Err(Error::Relayer(RelayerError::InvalidSender));
    }

    if tx.chain_id != Some(CHAIN_ID) {
        return Err(Error::Relayer(RelayerError::InvalidChainId));
    }

    let to = tx.to.ok_or(Error::User(UserError::EvmDeployDisallowed))?.raw();
    let target_as_address = extract_address(target).ok();
    let to_equals_target = target_as_address.map(|target| to == target).unwrap_or(false);

    // Only valid targets satisfy `to == target` or `to == hash(target)`
    if !to_equals_target && to != account_id_to_address(target) {
        return Err(Error::Relayer(RelayerError::InvalidTarget));
    }

    let nonce = if tx.nonce <= U64_MAX {
        tx.nonce.low_u64()
    } else {
        return Err(Error::Relayer(RelayerError::InvalidNonce));
    };
    if nonce != expected_nonce {
        return Err(Error::Relayer(RelayerError::InvalidNonce));
    }

    // If `to == target` and this is not a self-transaction then the address must not
    // be registered in the address registry. The purpose of this check is to prevent
    // lazy relayers from skipping this check themselves (relayers are supposed to use
    // the address registry to fill in the `target`).
    if to_equals_target && to != context.current_address {
        Ok(TransactionValidationOutcome::AddressCheckRequired(to))
    } else {
        Ok(TransactionValidationOutcome::Validated)
    }
}

fn validate_tx_value(
    tx: &NormalizedEthTransaction,
    context: &ExecutionContext,
    action: &Action,
) -> Result<(), Error> {
    if tx.value.raw() > VALUE_MAX {
        return Err(Error::User(UserError::ValueTooLarge));
    }

    let total_value = tx
        .value
        .raw()
        .low_u128()
        .saturating_mul(MAX_YOCTO_NEAR.into())
        .saturating_add(action.value().as_yoctonear());

    if total_value > 0 {
        let is_self_call = context.predecessor_account_id == context.current_account_id;
        let sufficient_attached_deposit =
            context.attached_deposit >= NearToken::from_yoctonear(total_value);
        if !is_self_call && !sufficient_attached_deposit {
            return Err(Error::Caller(CallerError::InsufficientAttachedValue));
        }
    }

    Ok(())
}

#[test]
fn test_value_max() {
    assert_eq!(VALUE_MAX, U256::from(u128::MAX / 1_000_000));
}

#[test]
fn test_account_id_to_address() {
    let account_id: AccountId = "aurora".parse().unwrap();
    let address =
        Address::from_slice(&hex::decode("4444588443c3a91288c5002483449aba1054192b").unwrap());
    assert_eq!(account_id_to_address(&account_id), address);
}

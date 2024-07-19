use crate::{
    error::{AccountIdError, Error, RelayerError, UserError},
    eth_emulation, ethabi_utils, near_action,
    types::{
        Action, EthEmulationKind, ExecutionContext, ParsableTransactionKind, TargetKind,
        TransactionKind, ADD_KEY_SELECTOR, ADD_KEY_SIGNATURE, DELETE_KEY_SELECTOR,
        DELETE_KEY_SIGNATURE, FUNCTION_CALL_SELECTOR, FUNCTION_CALL_SIGNATURE, TRANSFER_SELECTOR,
        TRANSFER_SIGNATURE,
    },
};
use aurora_engine_transactions::{EthTransactionKind, NormalizedEthTransaction};
use base64::Engine;
use ethabi::{ethereum_types::U256, Address};
use near_sdk::{env, AccountId, NearToken};

/// The chain ID is pulled from a file to allow this contract to be easily
/// compiled with the appropriate value for the network it will be deployed on.
/// The chain ID for Near mainnet is [397](https://chainlist.org/chain/397)
/// while the value for testnet is [398](https://chainlist.org/chain/398).
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

/// To make gas values look more familiar to Ethereum users, we say that
/// 1 EVM gas is equal to 0.1 GGas on Near. This means 2.1 Tgas on Near
/// is equal to 21k EVM gas and both amounts are "small" in their respective
/// ecosystems.
pub const GAS_MULTIPLIER: u64 = 100_000_000;

/// Given an RLP-encoded Ethereum transaction (bytes encoded in base64),
/// a Near account the transaction is supposed to interact with, the current
/// account ID, and the current nonce, this function will attempt to transform
/// the Ethereum transaction into a Near action.
pub fn parse_rlp_tx_to_action(
    tx_bytes_b64: &str,
    target: &AccountId,
    context: &ExecutionContext,
    expected_nonce: u64,
) -> Result<(near_action::Action, TransactionKind), Error> {
    let tx_bytes = decode_b64(tx_bytes_b64)?;
    let tx_kind: EthTransactionKind = tx_bytes.as_slice().try_into()?;
    let tx: NormalizedEthTransaction = tx_kind.try_into()?;
    let target_kind = validate_tx_relayer_data(&tx, target, context, expected_nonce)?;

    // Compute the fee based on the user's Ethereum transaction.
    // This is sent as a refund to the relayer in the case of an emulated base token
    // transfer or ERC-20 transfer. The reason for this refund is that it allows a
    // user with $NEAR to use a relayer service from their wallet immediately without
    // additional on-boarding.
    let tx_fee = {
        // Limit the cost by `VALUE_MAX` since we will convert this to a $NEAR amount.
        // The call to `low_u128` is safe because `VALUE_MAX` is the largest accepted value.
        let wei_amount = tx.max_fee_per_gas.saturating_mul(tx.gas_limit).min(VALUE_MAX).low_u128();
        NearToken::from_yoctonear(wei_amount.saturating_mul(MAX_YOCTO_NEAR as u128))
    };

    // The way an honest relayer assigns `target` is as follows:
    // 1. If the Ethereum transaction payload represents a Near action then use the receiver_id,
    // 2. If the payload looks like a supported Ethereum emulation then use the address registrar:
    // 2.a. if the tx.to address is registered then use the associated account id,
    // 2.b. otherwise, tx.to == target
    // 3. Otherwise, tx.to == target
    // Given this algorithm, the only way to have `TargetKind::EthImplicit` is in the
    // following cases:
    // I)   The Ethereum transaction payload is not parseable as a known action,
    // II)  The payload is parsable as a Near action and the receiver_id is an eth-implicit account
    // III) The payload is parsable as a supported Ethereum emulation but the to address is
    //      not registered in the address registrar.
    // Therefore, to determine if the relayer is honest we must always parse the payload and
    // we only need to check the registrar if the payload is parseable as an Ethereum emulation.
    // Note: the `TargetKind` is determined in `validate_tx_relayer_data` above, and that function
    // also confirms that the `target` is compatible with the user's `tx.to`.

    let (action, transaction_kind) = match parse_tx_data(target, &tx, tx_fee, context) {
        Ok((action, ParsableTransactionKind::NearNativeAction)) => {
            (action, TransactionKind::NearNativeAction)
        }
        Ok((action, ParsableTransactionKind::SelfNearNativeAction)) => {
            if let TargetKind::EthImplicit(_) = target_kind {
                // The calldata was parseable as a Near native action where the target
                // should be the current account, but the target is some other wallet contract.
                // This is technically allowed under the Ethereum standard for base token transfers
                // (where any calldata can be used when sending tokens to another EOA), so we
                // assume such a transfer must have been the user's intent. No address check is
                // required in this case because no Near account other than the current account
                // can be the receiver of these actions.
                (
                    Action::Transfer { receiver_id: target.to_string(), yocto_near: 0 },
                    TransactionKind::EthEmulation(EthEmulationKind::EOABaseTokenTransfer {
                        address_check: None,
                        fee: tx_fee,
                    }),
                )
            } else {
                (action, TransactionKind::NearNativeAction)
            }
        }
        Ok((action, ParsableTransactionKind::EthEmulation(eth_emulation))) => {
            if let TargetKind::EthImplicit(address) = target_kind {
                // Even though the action was parsable, the target is another wallet contract,
                // so the action _must_ still be a base token transfer, but we need
                // to check if the target is not registered (otherwise the relayer is faulty).
                (
                    Action::Transfer { receiver_id: target.to_string(), yocto_near: 0 },
                    TransactionKind::EthEmulation(EthEmulationKind::EOABaseTokenTransfer {
                        address_check: Some(address),
                        fee: tx_fee,
                    }),
                )
            } else {
                (action, TransactionKind::EthEmulation(eth_emulation.into()))
            }
        }
        Err(
            error @ (Error::User(UserError::InvalidAbiEncodedData)
            | Error::User(UserError::UnknownFunctionSelector)),
        ) => {
            match target_kind {
                TargetKind::EthImplicit(_) => {
                    // Unparsable actions can still be base token transfers, but no
                    // registrar check is required.
                    (
                        Action::Transfer { receiver_id: target.to_string(), yocto_near: 0 },
                        TransactionKind::EthEmulation(EthEmulationKind::EOABaseTokenTransfer {
                            address_check: None,
                            fee: tx_fee,
                        }),
                    )
                }
                TargetKind::CurrentAccount => {
                    // Base token transfers to self are also allowed by the Ethereum standard.
                    (
                        Action::Transfer {
                            receiver_id: context.current_account_id.to_string(),
                            yocto_near: 0,
                        },
                        TransactionKind::EthEmulation(EthEmulationKind::SelfBaseTokenTransfer),
                    )
                }
                TargetKind::OtherNearAccount(_) => {
                    // No interaction with other Near accounts is possible
                    // when the payload is not parsable
                    return Err(error);
                }
            }
        }
        Err(other_err) => return Err(other_err),
    };

    validate_tx_value(&tx)?;

    // Call to `low_u128` here is safe because of the validation done in `validate_tx_value`
    let near_action = action
        .try_into_near_action(tx.value.raw().low_u128().saturating_mul(MAX_YOCTO_NEAR.into()))?;

    Ok((near_action, transaction_kind))
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

fn parse_target(target: &AccountId, current_address: Address) -> TargetKind<'_> {
    match extract_address(target) {
        Ok(address) => {
            if address == current_address {
                TargetKind::CurrentAccount
            } else {
                TargetKind::EthImplicit(address)
            }
        }
        Err(_) => TargetKind::OtherNearAccount(target),
    }
}

fn parse_tx_data(
    target: &AccountId,
    tx: &NormalizedEthTransaction,
    fee: NearToken,
    context: &ExecutionContext,
) -> Result<(Action, ParsableTransactionKind), Error> {
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
            if yocto_near >= MAX_YOCTO_NEAR {
                return Err(Error::User(UserError::ExcessYoctoNear));
            }
            Ok((
                Action::FunctionCall { receiver_id, method_name, args, gas, yocto_near },
                ParsableTransactionKind::NearNativeAction,
            ))
        }
        TRANSFER_SELECTOR => {
            let (receiver_id, yocto_near): (String, u32) =
                ethabi_utils::abi_decode(&TRANSFER_SIGNATURE, &tx.data[4..])?;
            if target.as_str() != receiver_id.as_str() {
                return Err(Error::Relayer(RelayerError::InvalidTarget));
            }
            if yocto_near >= MAX_YOCTO_NEAR {
                return Err(Error::User(UserError::ExcessYoctoNear));
            }
            Ok((
                Action::Transfer { receiver_id, yocto_near },
                ParsableTransactionKind::NearNativeAction,
            ))
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
            Ok((
                Action::AddKey {
                    public_key_kind,
                    public_key,
                    nonce,
                    is_full_access,
                    is_limited_allowance,
                    allowance,
                    receiver_id,
                    method_names,
                },
                ParsableTransactionKind::SelfNearNativeAction,
            ))
        }
        DELETE_KEY_SELECTOR => {
            let (public_key_kind, public_key) =
                ethabi_utils::abi_decode(&DELETE_KEY_SIGNATURE, &tx.data[4..])?;
            Ok((
                Action::DeleteKey { public_key_kind, public_key },
                ParsableTransactionKind::SelfNearNativeAction,
            ))
        }
        _ => {
            let (action, emulation_kind) = eth_emulation::try_emulation(target, tx, fee, context)?;
            Ok((action, ParsableTransactionKind::EthEmulation(emulation_kind)))
        }
    }
}

/// Validates the transaction is following the Wallet Contract protocol.
/// This includes checks for:
/// - from address matches current account address
/// - to address is present and matches the target address (or hash of target account ID)
/// - nonce matches expected nonce
/// If this validation fails then the relayer that sent it is faulty and should be banned.
fn validate_tx_relayer_data<'a>(
    tx: &NormalizedEthTransaction,
    target: &'a AccountId,
    context: &ExecutionContext,
    expected_nonce: u64,
) -> Result<TargetKind<'a>, Error> {
    if tx.address.raw() != context.current_address {
        return Err(Error::Relayer(RelayerError::InvalidSender));
    }

    if tx.chain_id != Some(CHAIN_ID) {
        return Err(Error::Relayer(RelayerError::InvalidChainId));
    }

    let to = tx.to.ok_or(Error::User(UserError::EvmDeployDisallowed))?.raw();

    let target_kind = parse_target(target, context.current_address);

    // valid targets satisfy `to == target` or `to == hash(target)`
    let is_valid_target = match target_kind {
        TargetKind::CurrentAccount if to == context.current_address => {
            target == &context.current_account_id
        }
        TargetKind::EthImplicit(address) if to == address => {
            target.as_str()
                == format!("0x{}{}", hex::encode(address), context.current_account_suffix())
        }
        _ => to == account_id_to_address(target),
    };

    if !is_valid_target {
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

    // Relayers must attach at least as much gas as the user requested.
    let gas_limit = if tx.gas_limit < U64_MAX { tx.gas_limit.as_u64() } else { u64::MAX };
    if env::prepaid_gas().as_gas() < gas_limit.saturating_mul(GAS_MULTIPLIER) {
        return Err(Error::Relayer(RelayerError::InsufficientGas));
    }

    Ok(target_kind)
}

fn validate_tx_value(tx: &NormalizedEthTransaction) -> Result<(), Error> {
    if tx.value.raw() > VALUE_MAX {
        return Err(Error::User(UserError::ValueTooLarge));
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

//! This module contains logic for emulating Ethereum standards with the
//! corresponding Near actions. For now only the ERC-20 standard is supported
//! (which corresponds to Near's NEP-141).

use crate::{
    error::{Error, UserError},
    ethabi_utils,
    types::{Action, ExecutionContext, ParsableEthEmulationKind},
};
use aurora_engine_transactions::NormalizedEthTransaction;
use ethabi::{Address, ParamType};
use near_sdk::{env, AccountId, NearToken};

const FIVE_TERA_GAS: u64 = near_sdk::Gas::from_tgas(5).as_gas();

pub const ERC20_BALANCE_OF_SELECTOR: &[u8] = &[0x70, 0xa0, 0x82, 0x31];
const ERC20_BALANCE_OF_SIGNATURE: [ParamType; 1] = [ParamType::Address];

pub const ERC20_TRANSFER_SELECTOR: &[u8] = &[0xa9, 0x05, 0x9c, 0xbb];
const ERC20_TRANSFER_SIGNATURE: [ParamType; 2] = [
    ParamType::Address,   // to
    ParamType::Uint(256), // value
];

pub const ERC20_TOTAL_SUPPLY_SELECTOR: &[u8] = &[0x18, 0x16, 0x0d, 0xdd];

pub fn try_emulation(
    target: &AccountId,
    tx: &NormalizedEthTransaction,
    fee: NearToken,
    context: &ExecutionContext,
) -> Result<(Action, ParsableEthEmulationKind), Error> {
    if tx.data.len() < 4 {
        return Err(Error::User(UserError::InvalidAbiEncodedData));
    }

    let suffix = context.current_account_suffix();
    match &tx.data[0..4] {
        ERC20_BALANCE_OF_SELECTOR => {
            let (address,): (Address,) =
                ethabi_utils::abi_decode(&ERC20_BALANCE_OF_SIGNATURE, &tx.data[4..])?;
            // The account ID is assumed to have the same suffix as the current account because
            // (1) in production this is correct as all eth-implicit accounts are top-level and
            // (2) in testing environments where the addresses are sub-accounts, they are still
            // assumed to all be deployed to the same namespace so that they will all have the
            // same suffix.
            let args = format!(r#"{{"account_id": "0x{}{}"}}"#, hex::encode(address), suffix);
            Ok((
                Action::FunctionCall {
                    receiver_id: target.to_string(),
                    method_name: "ft_balance_of".into(),
                    args: args.into_bytes(),
                    gas: FIVE_TERA_GAS,
                    yocto_near: 0,
                },
                ParsableEthEmulationKind::ERC20Balance,
            ))
        }
        ERC20_TRANSFER_SELECTOR => {
            // We intentionally map to `u128` instead of `U256` because the NEP-141 standard
            // is to use u128.
            let (to, value): (Address, u128) =
                ethabi_utils::abi_decode(&ERC20_TRANSFER_SIGNATURE, &tx.data[4..])?;
            let receiver_id: AccountId = format!("0x{}{}", hex::encode(to), suffix)
                .parse()
                .unwrap_or_else(|_| env::panic_str("eth-implicit accounts are valid account ids"));

            // Include any data after the main args as a memo in the transfer.
            // The main data takes 68 bytes because there is a 4-byte selector followed
            // by two arguments which are each allocated 32 bytes according to the
            // Solidity ABI standard.
            let memo = if tx.data.len() > 68 {
                Some(format!(r#""0x{}""#, hex::encode(&tx.data[68..])))
            } else {
                None
            };
            let args = format!(
                r#"{{"receiver_id": "{}", "amount": "{}", "memo": {}}}"#,
                receiver_id.as_str(),
                value,
                memo.as_deref().unwrap_or("null"),
            );
            Ok((
                Action::FunctionCall {
                    receiver_id: target.to_string(),
                    method_name: "ft_transfer".into(),
                    args: args.into_bytes(),
                    gas: 2 * FIVE_TERA_GAS,
                    yocto_near: 1,
                },
                ParsableEthEmulationKind::ERC20Transfer { receiver_id, fee },
            ))
        }
        ERC20_TOTAL_SUPPLY_SELECTOR => Ok((
            Action::FunctionCall {
                receiver_id: target.to_string(),
                method_name: "ft_total_supply".into(),
                args: Vec::new(),
                gas: FIVE_TERA_GAS,
                yocto_near: 0,
            },
            ParsableEthEmulationKind::ERC20TotalSupply,
        )),
        _ => Err(Error::User(UserError::UnknownFunctionSelector)),
    }
}

#[test]
fn test_function_selectors() {
    let balance_of_signature = ethabi::short_signature("balanceOf", &ERC20_BALANCE_OF_SIGNATURE);

    let transfer_signature = ethabi::short_signature("transfer", &ERC20_TRANSFER_SIGNATURE);

    let total_supply_signature = ethabi::short_signature("totalSupply", &[]);

    assert_eq!(balance_of_signature, ERC20_BALANCE_OF_SELECTOR); // 0x70a08231
    assert_eq!(transfer_signature, ERC20_TRANSFER_SELECTOR); // 0xa9059cbb
    assert_eq!(total_supply_signature, ERC20_TOTAL_SUPPLY_SELECTOR); // 0x18160ddd
}

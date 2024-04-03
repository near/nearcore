//! This module contains logic for emulating Ethereum standards with the
//! corresponding Near actions. For now only the ERC-20 standard is supported
//! (which corresponds to Near's NEP-141).

use crate::{
    error::{Error, UserError},
    ethabi_utils,
    types::{Action, ExecutionContext},
};
use aurora_engine_transactions::NormalizedEthTransaction;
use ethabi::{Address, ParamType};
use near_sdk::AccountId;

const FIVE_TERA_GAS: u64 = near_sdk::Gas::from_tgas(5).as_gas();

pub const ERC20_BALANCE_OF_SELECTOR: &[u8] = &[0x70, 0xa0, 0x82, 0x31];
const ERC20_BALANCE_OF_SIGNATURE: [ParamType; 1] = [ParamType::Address];

pub const ERC20_TRANSFER_SELECTOR: &[u8] = &[0xa9, 0x05, 0x9c, 0xbb];
const ERC20_TRANSFER_SIGNATURE: [ParamType; 2] = [
    ParamType::Address,   // to
    ParamType::Uint(256), // value
];

pub fn try_emulation(
    target: &AccountId,
    tx: &NormalizedEthTransaction,
    context: &ExecutionContext,
) -> Result<Action, Error> {
    if tx.data.len() < 4 {
        return Err(Error::User(UserError::InvalidAbiEncodedData));
    }
    let suffix = context
        .current_account_id
        .as_str()
        .find('.')
        .map(|index| &context.current_account_id.as_str()[index..])
        .unwrap_or("");
    match &tx.data[0..4] {
        ERC20_BALANCE_OF_SELECTOR => {
            let (address,): (Address,) =
                ethabi_utils::abi_decode(&ERC20_BALANCE_OF_SIGNATURE, &tx.data[4..])?;
            let args = format!(r#"{{"account_id": "0x{}{}"}}"#, hex::encode(address), suffix);
            Ok(Action::FunctionCall {
                receiver_id: target.to_string(),
                method_name: "ft_balance_of".into(),
                args: args.into_bytes(),
                gas: FIVE_TERA_GAS,
                yocto_near: 0,
            })
        }
        ERC20_TRANSFER_SELECTOR => {
            // We intentionally map to `u128` instead of `U256` because the NEP-141 standard
            // is to use u128.
            let (to, value): (Address, u128) =
                ethabi_utils::abi_decode(&ERC20_TRANSFER_SIGNATURE, &tx.data[4..])?;
            let args = format!(
                r#"{{"receiver_id": "0x{}{}", "amount": "{}", "memo": null}}"#,
                hex::encode(to),
                suffix,
                value
            );
            Ok(Action::FunctionCall {
                receiver_id: target.to_string(),
                method_name: "ft_transfer".into(),
                args: args.into_bytes(),
                gas: 2 * FIVE_TERA_GAS,
                yocto_near: 1,
            })
        }
        _ => Err(Error::User(UserError::UnknownFunctionSelector)),
    }
}

#[test]
fn test_function_selectors() {
    let balance_of_signature = ethabi::short_signature("balanceOf", &ERC20_BALANCE_OF_SIGNATURE);

    let transfer_signature = ethabi::short_signature("transfer", &ERC20_TRANSFER_SIGNATURE);

    assert_eq!(balance_of_signature, ERC20_BALANCE_OF_SELECTOR); // 0x70a08231
    assert_eq!(transfer_signature, ERC20_TRANSFER_SELECTOR); // 0xa9059cbb
}

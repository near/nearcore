use ethereum_types::{Address, U256};
use rlp::Encodable;

use near_crypto::{InMemorySigner, KeyType};

use crate::utils::{
    accounts, create_context, encode_meta_call_function_args, setup, sign_eth_transaction, CHAIN_ID,
};
use near_evm_runner::utils::{near_erc712_domain, parse_meta_call, prepare_meta_call_args};

mod utils;

/// Test various invalid inputs to function calls.
#[test]
fn test_invalid_input() {
    let (mut fake_external, vm_config, fees_config) = setup();
    let mut context = create_context(&mut fake_external, &vm_config, &fees_config, accounts(1), 10);
    assert!(context.get_nonce(vec![]).is_err());
    assert!(context.get_balance(vec![]).is_err());
    assert!(context.get_code(vec![]).is_err());
    assert!(context.get_storage_at(vec![]).is_err());
    assert!(context.view_call_function(vec![]).is_err());
    assert!(context.view_call_function(vec![0u8; 20]).is_err());
    assert!(context.call_function(vec![]).is_err());
    assert!(context.call_function(vec![0u8; 20]).is_err());
    assert!(context.deposit(vec![]).is_err());
    assert!(context.withdraw(vec![]).is_err());
    assert!(context.transfer(vec![]).is_err());
}

#[test]
fn test_invalid_view_args() {
    let args = vec![vec![1u8; 20], vec![2u8; 20], vec![0u8; 32], vec![1u8, 0u8, 0u8, 0u8], vec![1]]
        .concat();
    let (mut fake_external, vm_config, fees_config) = setup();
    let mut context = create_context(&mut fake_external, &vm_config, &fees_config, accounts(1), 0);
    assert_eq!(
        context.view_call_function(args).unwrap_err().to_string(),
        "EvmError(ContractNotFound)"
    );
}

#[test]
fn test_invalid_raw_call_args() {
    let signer = InMemorySigner::from_seed("doesnt", KeyType::SECP256K1, "a");
    let mut signed_transaction = sign_eth_transaction(
        &signer,
        CHAIN_ID,
        U256::zero(),
        U256::from(200),
        U256::from(24000),
        Some(Address::zero()),
        U256::from(100),
        vec![],
    );
    signed_transaction.v = 30;

    let (mut fake_external, vm_config, fees_config) = setup();
    let mut context = create_context(&mut fake_external, &vm_config, &fees_config, accounts(1), 0);
    assert_eq!(
        context.raw_call_function(signed_transaction.rlp_bytes()).unwrap_err().to_string(),
        "EvmError(InvalidEcRecoverSignature)"
    );

    let signed_transaction = sign_eth_transaction(
        &signer,
        1,
        U256::zero(),
        U256::from(200),
        U256::from(24000),
        Some(Address::zero()),
        U256::from(100),
        vec![],
    );
    assert_eq!(
        context.raw_call_function(signed_transaction.rlp_bytes()).unwrap_err().to_string(),
        "EvmError(InvalidChainId)"
    );
}

#[test]
fn test_wrong_meta_tx() {
    let domain_separator = near_erc712_domain(U256::from(CHAIN_ID));
    let err = prepare_meta_call_args(
        &domain_separator,
        &"evm".to_string(),
        U256::from(14),
        U256::from(6),
        Address::from_slice(&[0u8; 20]),
        Address::from_slice(&[0u8; 20]),
        U256::from(100),
        "wrong_method",
        &hex::decode("ca").unwrap(),
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "EvmError(InvalidMetaTransactionMethodName)");
    let err = prepare_meta_call_args(
        &domain_separator,
        &"evm".to_string(),
        U256::from(14),
        U256::from(6),
        Address::from_slice(&[0u8; 20]),
        Address::from_slice(&[0u8; 20]),
        U256::from(100),
        "wrong_method([])",
        &hex::decode("ca").unwrap(),
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "EvmError(InvalidMetaTransactionMethodName)");
}

#[test]
fn wrong_signed_meta_tx() {
    let signer = InMemorySigner::from_seed("doesnt", KeyType::SECP256K1, "a");
    let domain_separator = near_erc712_domain(U256::from(CHAIN_ID));
    let meta_tx = encode_meta_call_function_args(
        &signer,
        CHAIN_ID,
        U256::from(14),
        U256::from(6),
        Address::from_slice(&[0u8; 20]),
        Address::from_slice(&[0u8; 20]),
        U256::from(0),
        "test()",
        vec![],
    );
    let err = parse_meta_call(
        &domain_separator,
        &"evm".to_string(),
        meta_tx[..meta_tx.len() - 1].to_vec(),
    )
    .unwrap_err();
    assert_eq!(err.to_string(), "EvmError(ArgumentParseError)");
}

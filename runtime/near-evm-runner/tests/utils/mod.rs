use ethereum_types::{Address, U256};
use keccak_hash::keccak;

use borsh::BorshSerialize;
use near_crypto::{InMemorySigner, PublicKey, Signature, Signer};
use near_evm_runner::types::{EthSignedTransaction, EthTransaction, MetaCallArgs};
use near_evm_runner::utils::{near_erc712_domain, prepare_meta_call_args, u256_to_arr};
use near_evm_runner::EvmContext;
use near_primitives::config::VMConfig;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::{AccountId, Balance};
use near_vm_logic::mocks::mock_external::MockedExternal;
use rlp::RlpStream;

/// See https://github.com/ethereum-lists/chains/blob/master/_data/chains/1313161555.json
pub const CHAIN_ID: u64 = 1313161555;

pub fn accounts(num: usize) -> AccountId {
    ["evm", "alice", "bob", "chad"][num].parse().unwrap()
}

pub fn setup() -> (MockedExternal, VMConfig, RuntimeFeesConfig) {
    let vm_config = VMConfig::default();
    let fees_config = RuntimeFeesConfig::default();
    let fake_external = MockedExternal::new();
    (fake_external, vm_config, fees_config)
}

pub fn create_context<'a>(
    external: &'a mut MockedExternal,
    vm_config: &'a VMConfig,
    fees_config: &'a RuntimeFeesConfig,
    account_id: String,
    attached_deposit: Balance,
) -> EvmContext<'a> {
    EvmContext::new(
        external,
        CHAIN_ID,
        vm_config,
        fees_config,
        1000,
        "evm".parse().unwrap(),
        account_id.parse().unwrap(),
        account_id.parse().unwrap(),
        attached_deposit,
        0,
        10u64.pow(14),
        false,
        1_000_000_000.into(),
    )
}

#[cfg(test)]
#[allow(dead_code)]
pub fn show_evm_gas_used(context: &EvmContext) {
    println!("Accumulated EVM gas used: {}", &context.evm_gas_counter.used_gas);
}

/// Linter is suboptimal, because doesn't see that this is used in standard_ops.rs.
#[allow(dead_code)]
pub fn public_key_to_address(public_key: PublicKey) -> Address {
    match public_key {
        PublicKey::ED25519(_) => panic!("Wrong PublicKey"),
        PublicKey::SECP256K1(pubkey) => {
            let pk: [u8; 64] = pubkey.into();
            let bytes = keccak(&pk.to_vec());
            let mut result = Address::zero();
            result.as_bytes_mut().copy_from_slice(&bytes[12..]);
            result
        }
    }
}

/// Linter is suboptimal, because doesn't see that this is used in standard_ops.rs.
#[allow(dead_code)]
pub fn encode_meta_call_function_args(
    signer: &dyn Signer,
    chain_id: u64,
    nonce: U256,
    fee_amount: U256,
    fee_token: Address,
    address: Address,
    value: U256,
    method_def: &str,
    args: Vec<u8>,
) -> Vec<u8> {
    let domain_separator = near_erc712_domain(U256::from(chain_id));
    let (msg, _) = prepare_meta_call_args(
        &domain_separator,
        &"evm".parse().unwrap(),
        nonce,
        fee_amount,
        fee_token,
        address,
        value,
        method_def,
        &args,
    )
    .unwrap();
    match signer.sign(&msg) {
        Signature::ED25519(_) => panic!("Wrong Signer"),
        Signature::SECP256K1(sig) => {
            let array = Into::<[u8; 65]>::into(sig.clone()).to_vec();
            let mut signature = [0u8; 64];
            signature.copy_from_slice(&array[..64]);
            MetaCallArgs {
                signature,
                // Add 27 to align eth-sig-util signature format
                v: array[64] + 27,
                nonce: u256_to_arr(&nonce),
                fee_amount: u256_to_arr(&fee_amount),
                fee_address: fee_token.0,
                contract_address: address.0,
                value: u256_to_arr(&value),
                method_def: method_def.to_string(),
                args,
            }
            .try_to_vec()
            .expect("Failed to serialize")
        }
    }
}

#[allow(dead_code)]
pub fn sign_eth_transaction(
    signer: &InMemorySigner,
    chain_id: u64,
    nonce: U256,
    gas_price: U256,
    gas: U256,
    to: Option<Address>,
    value: U256,
    data: Vec<u8>,
) -> EthSignedTransaction {
    let transaction = EthTransaction { nonce, gas_price, gas, to, value, data };
    let mut rlp_stream = RlpStream::new();
    transaction.rlp_append_unsigned(&mut rlp_stream, Some(chain_id));
    let message_hash = keccak(rlp_stream.as_raw());
    let signature = signer.sign(&message_hash.0).try_to_vec().unwrap();
    let mut r = [0u8; 32];
    let mut s = [0u8; 32];
    r.copy_from_slice(&signature[1..33]);
    s.copy_from_slice(&signature[33..65]);
    let v = signature[65] as u64;
    EthSignedTransaction {
        transaction,
        r: U256::from(r),
        s: U256::from(s),
        v: 35 + chain_id * 2 + v,
    }
}

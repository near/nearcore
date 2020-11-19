use ethereum_types::{Address, U256};
use keccak_hash::keccak;
use near_crypto::{PublicKey, Signature, Signer};
use near_evm_runner::utils::{near_erc721_domain, prepare_meta_call_args, u256_to_arr};
use near_evm_runner::EvmContext;
use near_runtime_fees::RuntimeFeesConfig;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::types::Balance;
use near_vm_logic::VMConfig;

/// See https://github.com/ethereum-lists/chains/blob/master/_data/chains/1313161555.json
pub const CHAIN_ID: u128 = 1313161555;

pub fn accounts(num: usize) -> String {
    ["evm", "alice", "bob", "chad"][num].to_string()
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
        "evm".to_string(),
        account_id.to_string(),
        account_id.to_string(),
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
    chain_id: u128,
    nonce: U256,
    fee_amount: U256,
    fee_token: Address,
    address: Address,
    method_name: &str,
    args: Vec<u8>,
) -> Vec<u8> {
    let domain_separator = near_erc721_domain(U256::from(chain_id));
    let msg = prepare_meta_call_args(
        &domain_separator,
        &"evm".to_string(),
        nonce,
        fee_amount,
        fee_token,
        address,
        method_name,
        &args,
    );
    match signer.sign(&msg) {
        Signature::ED25519(_) => panic!("Wrong Signer"),
        Signature::SECP256K1(sig) => [
            Into::<[u8; 65]>::into(sig).to_vec(),
            u256_to_arr(&nonce).to_vec(),
            u256_to_arr(&fee_amount).to_vec(),
            fee_token.0.to_vec(),
            address.0.to_vec(),
            vec![method_name.len() as u8],
            method_name.as_bytes().to_vec(),
            args,
        ]
        .concat(),
    }
}

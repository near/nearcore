use ethereum_types::{Address, U256};
use keccak_hash::keccak;
use near_crypto::{PublicKey, Signature, Signer};
use near_evm_runner::utils::{
    encode_call_function_args, near_erc721_domain, prepare_meta_call_args,
};
use near_evm_runner::EvmContext;
use near_runtime_fees::RuntimeFeesConfig;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::types::Balance;
use near_vm_logic::VMConfig;

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
    )
}

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

pub fn encode_meta_call_function_args(
    signer: &dyn Signer,
    address: Address,
    input: Vec<u8>,
) -> Vec<u8> {
    let domain_separator = near_erc721_domain(U256::from(0x4e454152));
    let call_args = encode_call_function_args(address, input);
    let args = prepare_meta_call_args(&domain_separator, &"evm".to_string(), &call_args);
    match signer.sign(&args) {
        Signature::ED25519(_) => panic!("Wrong Signer"),
        Signature::SECP256K1(sig) => {
            let sig: [u8; 65] = sig.into();
            let mut vsr = vec![0u8; 96];
            vsr[31] = sig[64] + 27;
            vsr[32..].copy_from_slice(&sig[..64]);
            [vsr, call_args].concat()
        }
    }
}

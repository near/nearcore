use std::io::Write;

use byteorder::WriteBytesExt;
use ethereum_types::{Address, H256, U256};
use keccak_hash::keccak;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use vm::CreateContractAddress;

use near_vm_logic::types::AccountId;

pub fn safe_next_address(addr: &[u8; 20]) -> [u8; 20] {
    let mut expanded_addr = [0u8; 32];
    expanded_addr[12..].copy_from_slice(addr);
    let mut result = [0u8; 32];
    U256::from_big_endian(&expanded_addr)
        .saturating_add(U256::from(1u8))
        .to_big_endian(&mut result);
    let mut address = [0u8; 20];
    address.copy_from_slice(&result[12..]);
    address
}

pub fn internal_storage_key(address: &Address, key: [u8; 32]) -> [u8; 52] {
    let mut k = [0u8; 52];
    k[..20].copy_from_slice(address.as_ref());
    k[20..].copy_from_slice(&key);
    k
}

pub fn near_account_bytes_to_evm_address(addr: &[u8]) -> Address {
    Address::from_slice(&keccak(addr)[12..])
}

pub fn near_account_id_to_evm_address(account_id: &str) -> Address {
    near_account_bytes_to_evm_address(&account_id.to_string().into_bytes())
}

pub fn hex_to_evm_address(address: &str) -> Address {
    let addr = hex::decode(&address).expect("Hex string not valid hex");
    Address::from_slice(&addr)
}

pub fn encode_call_function_args(address: Address, input: Vec<u8>) -> Vec<u8> {
    let mut result = Vec::with_capacity(20 + input.len());
    result.extend_from_slice(&address.0);
    result.extend_from_slice(&input);
    result
}

pub fn encode_view_call_function_args(
    sender: Address,
    address: Address,
    amount: U256,
    input: Vec<u8>,
) -> Vec<u8> {
    let mut result = Vec::with_capacity(72 + input.len());
    result.extend_from_slice(&sender.0);
    result.extend_from_slice(&address.0);
    result.extend_from_slice(&u256_to_arr(&amount));
    result.extend_from_slice(&input);
    result
}

pub fn address_from_arr(arr: &[u8]) -> Address {
    assert_eq!(arr.len(), 20);
    let mut address = [0u8; 20];
    address.copy_from_slice(&arr);
    Address::from(address)
}

pub fn u256_to_arr(val: &U256) -> [u8; 32] {
    let mut result = [0u8; 32];
    val.to_big_endian(&mut result);
    result
}

pub fn address_to_vec(val: &Address) -> Vec<u8> {
    val.to_fixed_bytes().to_vec()
}

pub fn vec_to_arr_32(v: Vec<u8>) -> Option<[u8; 32]> {
    if v.len() != 32 {
        return None;
    }
    let mut result = [0; 32];
    result.copy_from_slice(&v);
    Some(result)
}

/// Returns new address created from address, nonce, and code hash
/// Copied directly from the parity codebase
pub fn evm_contract_address(
    address_scheme: CreateContractAddress,
    sender: &Address,
    nonce: &U256,
    code: &[u8],
) -> (Address, Option<H256>) {
    use rlp::RlpStream;

    match address_scheme {
        CreateContractAddress::FromSenderAndNonce => {
            let mut stream = RlpStream::new_list(2);
            stream.append(sender);
            stream.append(nonce);
            (From::from(keccak(stream.as_raw())), None)
        }
        CreateContractAddress::FromSenderSaltAndCodeHash(salt) => {
            let code_hash = keccak(code);
            let mut buffer = [0u8; 1 + 20 + 32 + 32];
            buffer[0] = 0xff;
            buffer[1..(1 + 20)].copy_from_slice(&sender[..]);
            buffer[(1 + 20)..(1 + 20 + 32)].copy_from_slice(&salt[..]);
            buffer[(1 + 20 + 32)..].copy_from_slice(&code_hash[..]);
            (From::from(keccak(&buffer[..])), Some(code_hash))
        }
        CreateContractAddress::FromSenderAndCodeHash => {
            let code_hash = keccak(code);
            let mut buffer = [0u8; 20 + 32];
            buffer[..20].copy_from_slice(&sender[..]);
            buffer[20..].copy_from_slice(&code_hash[..]);
            (From::from(keccak(&buffer[..])), Some(code_hash))
        }
    }
}

#[derive(Eq, PartialEq, Debug, Ord, PartialOrd)]
pub struct Balance(pub u128);

impl Balance {
    pub fn from_be_bytes(bytes: [u8; 16]) -> Self {
        Balance(u128::from_be_bytes(bytes))
    }

    pub fn to_be_bytes(&self) -> [u8; 16] {
        self.0.to_be_bytes()
    }
}

impl Serialize for Balance {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> std::result::Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}", &self.0))
    }
}

impl<'de> Deserialize<'de> for Balance {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        u128::from_str_radix(&s, 10).map(Balance).map_err(serde::de::Error::custom)
    }
}

impl From<Balance> for u128 {
    fn from(balance: Balance) -> Self {
        balance.0
    }
}

pub fn format_log(topics: Vec<H256>, data: &[u8]) -> std::result::Result<Vec<u8>, std::io::Error> {
    let mut result = Vec::with_capacity(1 + topics.len() * 32 + data.len());
    result.write_u8(topics.len() as u8)?;
    for topic in topics.iter() {
        result.write(&topic.0)?;
    }
    result.write(data)?;
    Ok(result)
}

pub fn near_erc721_domain(chain_id: U256) -> [u8; 32] {
    let mut bytes = Vec::with_capacity(70);
    bytes.extend_from_slice(
        &keccak("EIP712Domain(string name,string version,uint256 chainId)".as_bytes()).as_bytes(),
    );
    bytes.extend_from_slice(b"NEAR");
    bytes.extend_from_slice(&[0x01]);
    bytes.extend_from_slice(&u256_to_arr(&chain_id));
    keccak(&bytes).into()
}

pub fn prepare_meta_call_args(
    domain_separator: &[u8; 32],
    account_id: &AccountId,
    nonce: U256,
    args: &[u8],
) -> [u8; 32] {
    let mut bytes = Vec::with_capacity(32 + account_id.len() + args.len());
    bytes.extend_from_slice(
        &keccak(
            "NearTx(string evmId, uint256 nonce, address contractAddress, bytes arguments)"
                .as_bytes(),
        )
        .as_bytes(),
    );
    bytes.extend_from_slice(account_id.as_bytes());
    bytes.extend_from_slice(&u256_to_arr(&nonce));
    bytes.extend_from_slice(args);
    let message: [u8; 32] = keccak(&bytes).into();
    let mut bytes = Vec::with_capacity(2 + 32 + 32);
    bytes.extend_from_slice(&[0x19, 0x01]);
    bytes.extend_from_slice(domain_separator);
    bytes.extend_from_slice(&message);
    keccak(&bytes).into()
}

/// Given signature and data, validates that signature is valid for given data and returns ecrecover address.
/// If signature is invalid or doesn't match, returns 0x0 address.
pub fn ecrecover_address(hash: &[u8; 32], signature: &[u8; 96]) -> Address {
    use sha3::Digest;

    let hash = secp256k1::Message::parse(&H256::from_slice(hash).0);
    let v = &signature[..32];
    let r = &signature[32..64];
    let s = &signature[64..96];

    let bit = match v[31] {
        27..=30 => v[31] - 27,
        _ => {
            // ??
            return Address::zero();
        }
    };

    let mut sig = [0u8; 64];
    sig[..32].copy_from_slice(&r);
    sig[32..].copy_from_slice(&s);
    let s = secp256k1::Signature::parse(&sig);

    if let Ok(rec_id) = secp256k1::RecoveryId::parse(bit) {
        if let Ok(p) = secp256k1::recover(&hash, &s, &rec_id) {
            // recover returns the 65-byte key, but addresses come from the raw 64-byte key
            let r = sha3::Keccak256::digest(&p.serialize()[1..]);
            return address_from_arr(&r[12..]);
        }
    }
    Address::zero()
}

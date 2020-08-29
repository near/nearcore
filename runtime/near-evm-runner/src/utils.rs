use ethereum_types::{Address, H256, U256};
use keccak_hash::keccak;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use vm::CreateContractAddress;

pub fn internal_storage_key(address: &Address, key: [u8; 32]) -> [u8; 52] {
    let mut k = [0u8; 52];
    k[..20].copy_from_slice(address.as_ref());
    k[20..].copy_from_slice(&key);
    k
}

pub fn evm_account_to_internal_address(addr: Address) -> [u8; 20] {
    addr.0
}

pub fn near_account_bytes_to_evm_address(addr: &[u8]) -> Address {
    Address::from_slice(&keccak(addr)[12..])
}

pub fn near_account_id_to_evm_address(account_id: &str) -> Address {
    near_account_bytes_to_evm_address(&account_id.to_string().into_bytes())
}

pub fn near_account_id_to_internal_address(account_id: &str) -> [u8; 20] {
    evm_account_to_internal_address(near_account_id_to_evm_address(account_id))
}

pub fn hex_to_evm_address(address: &str) -> Address {
    let addr = hex::decode(&address).expect("Hex string not valid hex");
    Address::from_slice(&addr)
}

pub fn balance_to_u256(val: &Balance) -> U256 {
    let mut bin = [0u8; 32];
    bin[16..].copy_from_slice(&val.to_be_bytes());
    bin.into()
}

pub fn u256_to_balance(val: &U256) -> Balance {
    let mut scratch = [0u8; 32];
    let mut bin = [0u8; 16];
    val.to_big_endian(&mut scratch);
    bin.copy_from_slice(&scratch[16..]);
    Balance::from_be_bytes(bin)
}

pub fn u256_to_vec(val: &U256) -> Vec<u8> {
    let mut result = [0u8; 32];
    val.to_big_endian(&mut result);
    result.to_vec()
}

pub fn address_to_vec(val: &Address) -> Vec<u8> {
    val.to_fixed_bytes().to_vec()
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
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}", &self.0))
    }
}

impl<'de> Deserialize<'de> for Balance {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
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

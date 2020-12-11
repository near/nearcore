use std::io::Write;

use borsh::BorshSerialize;
use byteorder::WriteBytesExt;
use ethereum_types::{Address, H160, H256, U256};
use keccak_hash::keccak;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use vm::CreateContractAddress;

use crate::types::{
    DataKey, FunctionCallArgs, MetaCallArgs, RawAddress, RawHash, RawU256, Result, ViewCallArgs,
};
use near_vm_errors::{EvmError, VMLogicError};
use near_vm_logic::types::AccountId;

pub fn saturating_next_address(addr: &RawAddress) -> RawAddress {
    let mut expanded_addr = [255u8; 32];
    expanded_addr[12..].copy_from_slice(addr);
    let mut result = [0u8; 32];
    U256::from_big_endian(&expanded_addr)
        .saturating_add(U256::from(1u8))
        .to_big_endian(&mut result);
    let mut address = [0u8; 20];
    address.copy_from_slice(&result[12..]);
    address
}

pub fn internal_storage_key(address: &Address, key: &RawU256) -> DataKey {
    let mut k = [0u8; 52];
    k[..20].copy_from_slice(address.as_ref());
    k[20..].copy_from_slice(key);
    k
}

pub fn near_account_bytes_to_evm_address(addr: &[u8]) -> Address {
    Address::from_slice(&keccak(addr)[12..])
}

pub fn near_account_id_to_evm_address(account_id: &str) -> Address {
    near_account_bytes_to_evm_address(&account_id.as_bytes().to_vec())
}

pub fn encode_call_function_args(address: Address, input: Vec<u8>) -> Vec<u8> {
    FunctionCallArgs { contract: address.into(), input }.try_to_vec().unwrap()
}

pub fn split_data_key(key: &DataKey) -> (Address, RawU256) {
    let mut addr = [0u8; 20];
    addr.copy_from_slice(&key[..20]);
    let mut subkey = [0u8; 32];
    subkey.copy_from_slice(&key[20..]);
    (H160(addr), subkey)
}

pub fn combine_data_key(addr: &Address, subkey: &RawU256) -> DataKey {
    let mut key = [0u8; 52];
    key[..20].copy_from_slice(&addr.0);
    key[20..52].copy_from_slice(subkey);
    key
}

pub fn encode_view_call_function_args(
    sender: Address,
    address: Address,
    amount: U256,
    input: Vec<u8>,
) -> Vec<u8> {
    ViewCallArgs { sender: sender.into(), address: address.into(), amount: amount.into(), input }
        .try_to_vec()
        .unwrap()
}

pub fn address_from_arr(arr: &[u8]) -> Address {
    assert_eq!(arr.len(), 20);
    let mut address = [0u8; 20];
    address.copy_from_slice(&arr);
    Address::from(address)
}

pub fn u256_to_arr(val: &U256) -> RawU256 {
    let mut result = [0u8; 32];
    val.to_big_endian(&mut result);
    result
}

pub fn address_to_vec(val: &Address) -> Vec<u8> {
    val.to_fixed_bytes().to_vec()
}

pub fn vec_to_arr_32(v: Vec<u8>) -> Option<RawU256> {
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

pub fn near_erc721_domain(chain_id: U256) -> RawU256 {
    let mut bytes = Vec::with_capacity(70);
    bytes.extend_from_slice(
        &keccak("EIP712Domain(string name,string version,uint256 chainId)".as_bytes()).as_bytes(),
    );
    let near = b"NEAR";
    let near: RawU256 = keccak(&near).into();
    bytes.extend_from_slice(&near);
    let version = b"1";
    let version: RawU256 = keccak(&version).into();
    bytes.extend_from_slice(&version);
    bytes.extend_from_slice(&u256_to_arr(&chain_id));
    keccak(&bytes).into()
}

pub fn method_name_to_rlp(method_name: String) -> [u8; 4] {
    let mut result = [0u8; 4];
    result.copy_from_slice(&keccak(method_name)[..4]);
    result
}

pub fn encode_address(addr: Address) -> Vec<u8> {
    let mut bytes = vec![0u8; 12];
    bytes.extend_from_slice(&addr.0);
    bytes
}

pub fn encode_string(s: &str) -> Vec<u8> {
    let mut bytes = vec![];
    bytes.extend_from_slice(&keccak(s.as_bytes()).as_bytes());
    bytes
}

#[derive(Debug, Eq, PartialEq)]
pub enum Value {
    Bytes(Vec<u8>),
    List(Vec<Value>),
}

#[derive(Debug, Eq, PartialEq)]
pub struct Arg {
    #[allow(dead_code)]
    pub name: String,
    pub t: String,
}

#[derive(Debug, Eq, PartialEq)]
pub struct Method {
    pub name: String,
    pub args: Vec<Arg>,
}

impl Arg {
    fn parse_arg(text: &str) -> Result<(Arg, &str)> {
        let (t, remains) = parse_ident(text)?;
        let remains = consume(remains, ' ')?;
        let (name, remains) = parse_ident(remains)?;
        Ok((Arg { t, name }, remains))
    }
}

impl Method {
    fn parse_method_args(text: &str) -> Result<(Vec<Arg>, &str)> {
        let mut remains = consume(text, '(')?;
        if remains.len() == 0 {
            return Err(VMLogicError::EvmError(EvmError::InvalidMetaTransactionMethodName));
        }
        let mut args = vec![];
        let first = remains.chars().next().unwrap();
        if is_arg_start(first) {
            let (arg, r) = Arg::parse_arg(remains)?;
            remains = r;
            args.push(arg);
            while remains.chars().next() == Some(',') {
                remains = consume(remains, ',')?;
                let (arg, r) = Arg::parse_arg(remains)?;
                remains = r;
                args.push(arg);
            }
        }

        let remains = consume(remains, ')')?;

        Ok((args, remains))
    }

    fn parse_method(method_name: &str) -> Result<(Method, &str)> {
        let (name, remains) = parse_ident(method_name)?;
        let (args, remains) = Method::parse_method_args(remains)?;
        Ok((Method { name, args }, remains))
    }

    pub fn methods_from_method_name(method_name: &str) -> Result<Vec<Method>> {
        let mut method_name = method_name;
        let mut methods = vec![];
        while method_name.len() > 0 {
            let (method, method_name_remains) = Method::parse_method(method_name)?;
            method_name = method_name_remains;
            methods.push(method);
        }
        Ok(methods)
    }
}

fn parse_ident(text: &str) -> Result<(String, &str)> {
    let mut chars = text.chars();
    if text.len() == 0 || !is_arg_start(chars.next().unwrap()) {
        return Err(VMLogicError::EvmError(EvmError::InvalidMetaTransactionMethodName));
    }

    let mut i = 1;
    for c in chars {
        if !is_arg_char(c) {
            break;
        }
        i += 1;
    }
    Ok((text[..i].to_string(), &text[i..]))
}

fn consume(text: &str, c: char) -> Result<&str> {
    let first = text.chars().next();
    if first.is_none() || first.unwrap() != c {
        return Err(VMLogicError::EvmError(EvmError::InvalidMetaTransactionMethodName));
    }

    Ok(&text[1..])
}

fn is_arg_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_arg_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Return a signature of the method_name
/// E.g. method_signature(Methods before parse: "adopt(uint256 petId)") -> "adopt(uint256)"
fn methods_signature(methods: &[Method]) -> String {
    methods
        .iter()
        .map(|m| {
            format!(
                "{}({})",
                &m.name,
                itertools::join(m.args.iter().map(|arg| arg.t.as_str()), ",")
            )
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct ParseMethodNameError;

/// decode rlp-encoded args into vector of Values
fn rlp_decode(args: &[u8]) -> Vec<Value> {
    vec![]
}

/// eip-712 hash a single argument, whose type is ty, and value is value. Definition of all types
/// is in methods
fn eip_712_hash(ty: &str, value: &Value, methods: &Vec<Method>) -> Vec<u8> {
    vec![]
}

/// eip-712 hash struct of entire meta txn
pub fn prepare_meta_call_args(
    domain_separator: &RawU256,
    account_id: &AccountId,
    nonce: U256,
    fee_amount: U256,
    fee_address: Address,
    contract_address: Address,
    method_name: &str,
    args: &[u8],
) -> Result<RawU256> {
    let mut bytes = Vec::new();
    let method_arg_start = method_name.find('(').unwrap();
    let arguments = "Arguments".to_string() + &method_name[method_arg_start..];
    let types = "NearTx(string evmId,uint256 nonce,uint256 feeAmount,address feeAddress,address contractAddress,string contractMethod,Arguments arguments)".to_string() + &arguments;
    bytes.extend_from_slice(&keccak(types.as_bytes()).as_bytes());
    bytes.extend_from_slice(&keccak(account_id.as_bytes()).as_bytes());
    bytes.extend_from_slice(&u256_to_arr(&nonce));
    bytes.extend_from_slice(&u256_to_arr(&fee_amount));
    bytes.extend_from_slice(&encode_address(fee_address));
    bytes.extend_from_slice(&encode_address(contract_address));
    let methods = Method::methods_from_method_name(method_name)?;
    let method_sig = methods_signature(&methods);
    bytes.extend_from_slice(&keccak(method_sig.as_bytes()).as_bytes());

    let mut arg_bytes = Vec::new();
    arg_bytes.extend_from_slice(&keccak(arguments.as_bytes()).as_bytes());
    let args_decoded: Vec<Value> = rlp_decode(args);
    for i in 0..args_decoded.len() {
        arg_bytes.extend_from_slice(&eip_712_hash(
            &methods[0].args[i].t,
            &args_decoded[i],
            &methods,
        ));
    }

    let arg_bytes_hash: RawU256 = keccak(&arg_bytes).into();
    bytes.extend_from_slice(&arg_bytes_hash);

    let message: RawU256 = keccak(&bytes).into();
    let mut bytes = Vec::with_capacity(2 + 32 + 32);
    bytes.extend_from_slice(&[0x19, 0x01]);
    bytes.extend_from_slice(domain_separator);
    bytes.extend_from_slice(&message);
    Ok(keccak(&bytes).into())
}

/// Format
/// [0..65): signature: v - 1 byte, s - 32 bytes, r - 32 bytes
/// [65..97): nonce: nonce of the `signer` account of the `signature`.
/// : fee_amount
/// : fee_address
/// [97..117): contract_id: address for contract to call
/// : method_name_length
/// : method_name
/// 117..: RLP encoded rest of arguments.
pub fn parse_meta_call(
    domain_separator: &RawU256,
    account_id: &AccountId,
    args: Vec<u8>,
) -> Result<MetaCallArgs> {
    if args.len() <= 169 {
        return Err(VMLogicError::EvmError(EvmError::ArgumentParseError));
    }
    let mut signature: [u8; 65] = [0; 65];
    // Signatures coming from outside are srv but ecrecover takes vsr, so move last byte to first position.
    signature[0] = args[64];
    signature[1..].copy_from_slice(&args[..64]);
    let nonce = U256::from_big_endian(&args[65..97]);
    let fee_amount = U256::from_big_endian(&args[97..129]);
    let fee_address = Address::from_slice(&args[129..149]);
    let contract_address = Address::from_slice(&args[149..169]);
    let method_name_len = args[169] as usize;
    if args.len() < method_name_len + 170 {
        return Err(VMLogicError::EvmError(EvmError::ArgumentParseError));
    }
    let method_name = String::from_utf8(args[170..170 + method_name_len].to_vec())
        .map_err(|_| VMLogicError::EvmError(EvmError::ArgumentParseError))?;
    let args = &args[170 + method_name_len..];

    let msg = prepare_meta_call_args(
        domain_separator,
        account_id,
        nonce,
        fee_amount,
        fee_address,
        contract_address,
        &method_name,
        args,
    )?;
    let sender = ecrecover_address(&msg, &signature);
    if sender == Address::zero() {
        return Err(VMLogicError::EvmError(EvmError::InvalidEcRecoverSignature));
    }
    let method_name_rlp = method_name_to_rlp(method_name);
    let input = [method_name_rlp.to_vec(), args.to_vec()].concat();
    Ok(MetaCallArgs { sender, nonce, fee_amount, fee_address, contract_address, input })
}

/// Given signature and data, validates that signature is valid for given data and returns ecrecover address.
/// If signature is invalid or doesn't match, returns 0x0 address.
pub fn ecrecover_address(hash: &RawHash, signature: &[u8; 65]) -> Address {
    use sha3::Digest;

    let hash = secp256k1::Message::parse(&H256::from_slice(hash).0);
    let v = signature[0];
    let bit = match v {
        0..=26 => v,
        _ => v - 27,
    };

    let mut sig = [0u8; 64];
    sig.copy_from_slice(&signature[1..65]);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_method() {
        let methods = Method::methods_from_method_name(
            "Blah(Foo q,Bar w)Foo(uint256 x,Bar z)Bar(T ttt)T(string x)",
        )
        .unwrap();
        assert!(
            methods
                == vec![
                    Method {
                        name: "Blah".into(),
                        args: vec![
                            Arg { name: "q".into(), t: "Foo".into() },
                            Arg { name: "w".into(), t: "Bar".into() }
                        ]
                    },
                    Method {
                        name: "Foo".into(),
                        args: vec![
                            Arg { name: "x".into(), t: "uint256".into() },
                            Arg { name: "z".into(), t: "Bar".into() }
                        ]
                    },
                    Method {
                        name: "Bar".into(),
                        args: vec![Arg { name: "ttt".into(), t: "T".into() }]
                    },
                    Method {
                        name: "T".into(),
                        args: vec![Arg { name: "x".into(), t: "string".into() }]
                    }
                ]
        );
    }
}

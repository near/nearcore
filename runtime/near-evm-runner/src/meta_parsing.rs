use std::collections::HashMap;

use borsh::BorshDeserialize;
use ethabi::{encode, Token as ABIToken};
use ethereum_types::{Address, U256};
use keccak_hash::keccak;
use lunarity_lexer::{Lexer, Token};
use rlp::{Decodable, DecoderError, Rlp};

use near_primitives::types::AccountId;
use near_vm_errors::{EvmError, VMLogicError};

use crate::types::{InternalMetaCallArgs, MetaCallArgs, RawU256, Result};
use crate::utils::{address_from_arr, ecrecover_address, u256_to_arr};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArgType {
    Address,
    Uint,
    Int,
    String,
    Bool,
    Bytes,
    Byte(u8),
    Custom(String),
    Array { length: Option<u64>, inner: Box<ArgType> },
}

/// the type string is being validated before it's parsed.
/// field_type: A single evm function arg type in string, without the argument name
/// e.g. "bytes" "uint256[][3]" "CustomStructName"
pub fn parse_type(field_type: &str) -> Result<ArgType> {
    #[derive(PartialEq)]
    enum State {
        Open,
        Close,
    }

    let mut lexer = Lexer::new(field_type);
    let mut token = None;
    let mut state = State::Close;
    let mut array_depth = 0;
    let mut current_array_length: Option<u64> = None;

    while lexer.token != Token::EndOfProgram {
        let type_ = match lexer.token {
            Token::Identifier => ArgType::Custom(lexer.slice().to_owned()),
            Token::TypeByte => ArgType::Byte(lexer.extras.0),
            Token::TypeBytes => ArgType::Bytes,
            Token::TypeBool => ArgType::Bool,
            Token::TypeUint => ArgType::Uint,
            Token::TypeInt => ArgType::Int,
            Token::TypeString => ArgType::String,
            Token::TypeAddress => ArgType::Address,
            Token::LiteralInteger => {
                let length = lexer.slice();
                current_array_length = Some(length.parse().map_err(|_| {
                    VMLogicError::EvmError(EvmError::InvalidMetaTransactionMethodName)
                })?);
                lexer.advance();
                continue;
            }
            Token::BracketOpen if token.is_some() && state == State::Close => {
                state = State::Open;
                lexer.advance();
                continue;
            }
            Token::BracketClose if array_depth < 10 => {
                if state == State::Open && token.is_some() {
                    let length = current_array_length.take();
                    state = State::Close;
                    token = Some(ArgType::Array {
                        inner: Box::new(token.expect("if statement checks for some; qed")),
                        length,
                    });
                    lexer.advance();
                    array_depth += 1;
                    continue;
                } else {
                    return Err(VMLogicError::EvmError(EvmError::InvalidMetaTransactionMethodName));
                }
            }
            Token::BracketClose if array_depth == 10 => {
                return Err(VMLogicError::EvmError(EvmError::InvalidMetaTransactionMethodName));
            }
            _ => {
                return Err(VMLogicError::EvmError(EvmError::InvalidMetaTransactionMethodName));
            }
        };

        token = Some(type_);
        lexer.advance();
    }

    Ok(token.ok_or(VMLogicError::EvmError(EvmError::InvalidMetaTransactionMethodName))?)
}

/// NEAR's domainSeparator
/// See https://eips.ethereum.org/EIPS/eip-712#definition-of-domainseparator
/// and https://eips.ethereum.org/EIPS/eip-712#rationale-for-domainseparator
/// for definition and rationale for domainSeparator.
pub fn near_erc712_domain(chain_id: U256) -> RawU256 {
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

/// method_sig: format like "adopt(uint256,PetObj)" (no additional PetObj definition)
pub fn method_sig_to_abi(method_sig: &str) -> [u8; 4] {
    let mut result = [0u8; 4];
    result.copy_from_slice(&keccak(method_sig)[..4]);
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
pub enum RlpValue {
    Bytes(Vec<u8>),
    List(Vec<RlpValue>),
}

impl Decodable for RlpValue {
    fn decode(rlp: &Rlp<'_>) -> std::result::Result<Self, DecoderError> {
        if rlp.is_list() {
            Ok(RlpValue::List(rlp.as_list()?))
        } else {
            Ok(RlpValue::Bytes(rlp.decoder().decode_value(|bytes| Ok(bytes.to_vec()))?))
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
/// An argument specified in a evm method definition
pub struct Arg {
    #[allow(dead_code)]
    pub name: String,
    pub type_raw: String,
    pub t: ArgType,
}

#[derive(Debug, Eq, PartialEq)]
/// A parsed evm method definition
pub struct Method {
    pub name: String,
    pub raw: String,
    pub args: Vec<Arg>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct MethodAndTypes {
    pub method: Method,
    pub type_sequences: Vec<String>,
    pub types: HashMap<String, Method>,
}

impl Arg {
    fn parse(text: &str) -> Result<(Arg, &str)> {
        let (type_raw, remains) = parse_type_raw(text)?;
        let t = parse_type(&type_raw)?;
        let remains = consume(remains, ' ')?;
        let (name, remains) = parse_ident(remains)?;
        Ok((Arg { type_raw, t, name }, remains))
    }

    fn parse_args(text: &str) -> Result<(Vec<Arg>, &str)> {
        let mut remains = consume(text, '(')?;
        if remains.len() == 0 {
            return Err(VMLogicError::EvmError(EvmError::InvalidMetaTransactionMethodName));
        }
        let mut args = vec![];
        let first = remains.chars().next().unwrap();
        if is_arg_start(first) {
            let (arg, r) = Arg::parse(remains)?;
            remains = r;
            args.push(arg);
            while remains.chars().next() == Some(',') {
                remains = consume(remains, ',')?;
                let (arg, r) = Arg::parse(remains)?;
                remains = r;
                args.push(arg);
            }
        }

        let remains = consume(remains, ')')?;

        Ok((args, remains))
    }
}

impl Method {
    fn parse(method_def: &str) -> Result<(Method, &str)> {
        let (name, remains) = parse_ident(method_def)?;
        let (args, remains) = Arg::parse_args(remains)?;
        Ok((
            Method { name, args, raw: method_def[..method_def.len() - remains.len()].to_string() },
            remains,
        ))
    }
}

impl MethodAndTypes {
    pub fn parse(method_def: &str) -> Result<Self> {
        let method_def = method_def;
        let mut parsed_types = HashMap::new();
        let mut type_sequences = vec![];
        let (method, mut types) = Method::parse(method_def)?;
        while types.len() > 0 {
            let (ty, remains) = Method::parse(types)?;
            type_sequences.push(ty.name.clone());
            parsed_types.insert(ty.name.clone(), ty);
            types = remains;
        }
        Ok(MethodAndTypes { method, types: parsed_types, type_sequences })
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

/// Tokenizer a type specifier from a method definition
/// E.g. text: "uint256[] petIds,..."
/// returns: "uint256[]", " petIds,..."
/// "uint256[]" is not parsed further to "an array of uint256" in this fn
fn parse_type_raw(text: &str) -> Result<(String, &str)> {
    let i =
        text.find(' ').ok_or(VMLogicError::EvmError(EvmError::InvalidMetaTransactionMethodName))?;
    Ok((text[..i].to_string(), &text[i..]))
}

/// Consume next char in text, it must be c or return parse error
/// return text without the first char
fn consume(text: &str, c: char) -> Result<&str> {
    let first = text.chars().next();
    if first.is_none() || first.unwrap() != c {
        return Err(VMLogicError::EvmError(EvmError::InvalidMetaTransactionMethodName));
    }

    Ok(&text[1..])
}

/// Return true if c can be used as first char of a evm method arg
fn is_arg_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

/// Return true if c can be used as consequent char of a evm method arg
fn is_arg_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Return a signature of the method_def with additional args
/// E.g. methods_signature(Methods before parse: "adopt(uint256 petId,PetObj petobj)PetObj(string name)")
/// -> "adopt(uint256,PetObj)"
fn method_signature(method_and_type: &MethodAndTypes) -> String {
    format!(
        "{}({})",
        method_and_type.method.name,
        itertools::join(method_and_type.method.args.iter().map(|arg| arg.type_raw.as_str()), ",")
    )
}

/// decode rlp-encoded args into vector of Values
fn rlp_decode(args: &[u8]) -> Result<Vec<RlpValue>> {
    let rlp = Rlp::new(args);
    let res: std::result::Result<Vec<RlpValue>, DecoderError> = rlp.as_list();
    res.map_err(|_| VMLogicError::EvmError(EvmError::InvalidMetaTransactionFunctionArg))
}

/// eip-712 hash a single argument, whose type is ty, and value is value. Definition of all types
/// is in types
fn eip_712_hash_argument(
    ty: &ArgType,
    value: &RlpValue,
    types: &HashMap<String, Method>,
) -> Result<Vec<u8>> {
    match ty {
        ArgType::String | ArgType::Bytes => {
            eip_712_rlp_value(value, |b| Ok(keccak(&b).as_bytes().to_vec()))
        }
        ArgType::Byte(_) => eip_712_rlp_value(value, |b| Ok(b.clone())),
        // TODO: ensure rlp int is encoded as sign extended uint256, otherwise this is wrong
        ArgType::Uint | ArgType::Int | ArgType::Bool => {
            eip_712_rlp_value(value, |b| Ok(u256_to_arr(&U256::from_big_endian(&b)).to_vec()))
        }
        ArgType::Address => eip_712_rlp_value(value, |b| Ok(encode_address(address_from_arr(b)))),
        ArgType::Array { inner, .. } => eip_712_rlp_list(value, |l| {
            let mut r = vec![];
            for element in l {
                r.extend_from_slice(&eip_712_hash_argument(inner, element, types)?);
            }
            Ok(keccak(r).as_bytes().to_vec())
        }),
        ArgType::Custom(type_name) => eip_712_rlp_list(value, |l| {
            let struct_type = types
                .get(type_name)
                .ok_or(VMLogicError::EvmError(EvmError::InvalidMetaTransactionFunctionArg))?;
            // struct_type.raw is with struct type with argument names (a "method_def"), so it follows
            // EIP-712 typeHash.
            let mut r = keccak(&struct_type.raw).as_bytes().to_vec();
            for (i, element) in l.iter().enumerate() {
                r.extend_from_slice(&eip_712_hash_argument(
                    &struct_type.args[i].t,
                    element,
                    types,
                )?);
            }
            Ok(keccak(r).as_bytes().to_vec())
        }),
    }
}

/// EIP-712 hash a RLP list. f must contain actual logic of EIP-712 encoding
/// This function serves as a guard to assert value is a List instead of Value
fn eip_712_rlp_list<F>(value: &RlpValue, f: F) -> Result<Vec<u8>>
where
    F: Fn(&Vec<RlpValue>) -> Result<Vec<u8>>,
{
    match value {
        RlpValue::Bytes(_) => {
            Err(VMLogicError::EvmError(EvmError::InvalidMetaTransactionFunctionArg))
        }
        RlpValue::List(l) => f(l),
    }
}

/// EIP-712 hash a RLP value. f must contain actual logic of EIP-712 encoding
/// This function serves as a guard to assert value is a Value instead of List
fn eip_712_rlp_value<F>(value: &RlpValue, f: F) -> Result<Vec<u8>>
where
    F: Fn(&Vec<u8>) -> Result<Vec<u8>>,
{
    match value {
        RlpValue::List(_) => {
            Err(VMLogicError::EvmError(EvmError::InvalidMetaTransactionFunctionArg))
        }
        RlpValue::Bytes(b) => f(b),
    }
}

fn eth_abi_encode_args(args_decoded: &Vec<RlpValue>, methods: &MethodAndTypes) -> Result<Vec<u8>> {
    let mut tokens = vec![];
    for (i, arg) in args_decoded.iter().enumerate() {
        tokens.push(arg_to_abi_token(&methods.method.args[i].t, arg, methods)?);
    }
    Ok(encode(&tokens))
}

fn arg_to_abi_token(ty: &ArgType, arg: &RlpValue, methods: &MethodAndTypes) -> Result<ABIToken> {
    match ty {
        ArgType::String | ArgType::Bytes => {
            value_to_abi_token(arg, |b| Ok(ABIToken::Bytes(b.clone())))
        }
        ArgType::Byte(_) => value_to_abi_token(arg, |b| Ok(ABIToken::FixedBytes(b.clone()))),
        ArgType::Uint | ArgType::Int | ArgType::Bool => {
            value_to_abi_token(arg, |b| Ok(ABIToken::Uint(U256::from_big_endian(&b))))
        }
        ArgType::Address => value_to_abi_token(arg, |b| Ok(ABIToken::Address(address_from_arr(b)))),
        ArgType::Array { inner, length: None } => list_to_abi_token(arg, |l| {
            let mut tokens = vec![];
            for arg in l {
                tokens.push(arg_to_abi_token(inner, arg, methods)?);
            }
            Ok(ABIToken::Array(tokens))
        }),
        ArgType::Array { inner, length: Some(_) } => list_to_abi_token(arg, |l| {
            let mut tokens = vec![];
            for arg in l {
                tokens.push(arg_to_abi_token(inner, arg, methods)?);
            }
            Ok(ABIToken::FixedArray(tokens))
        }),
        ArgType::Custom(type_name) => list_to_abi_token(arg, |l| {
            let struct_type = methods
                .types
                .get(type_name)
                .ok_or(VMLogicError::EvmError(EvmError::InvalidMetaTransactionFunctionArg))?;
            let mut tokens = vec![];
            for (i, element) in l.iter().enumerate() {
                tokens.push(arg_to_abi_token(&struct_type.args[i].t, element, methods)?);
            }
            Ok(ABIToken::Tuple(tokens))
        }),
    }
}

fn value_to_abi_token<F>(value: &RlpValue, f: F) -> Result<ABIToken>
where
    F: Fn(&Vec<u8>) -> Result<ABIToken>,
{
    match value {
        RlpValue::List(_) => {
            Err(VMLogicError::EvmError(EvmError::InvalidMetaTransactionFunctionArg))
        }
        RlpValue::Bytes(b) => f(b),
    }
}

fn list_to_abi_token<F>(value: &RlpValue, f: F) -> Result<ABIToken>
where
    F: Fn(&Vec<RlpValue>) -> Result<ABIToken>,
{
    match value {
        RlpValue::Bytes(_) => {
            Err(VMLogicError::EvmError(EvmError::InvalidMetaTransactionFunctionArg))
        }
        RlpValue::List(l) => f(l),
    }
}

/// eip-712 hash struct of entire meta txn and abi-encode function args to evm input
pub fn prepare_meta_call_args(
    domain_separator: &RawU256,
    account_id: &AccountId,
    nonce: U256,
    fee_amount: U256,
    fee_address: Address,
    contract_address: Address,
    value: U256,
    method_def: &str,
    args: &[u8],
) -> Result<(RawU256, Vec<u8>)> {
    let mut bytes = Vec::new();
    let method_arg_start = match method_def.find('(') {
        Some(index) => index,
        None => return Err(VMLogicError::EvmError(EvmError::InvalidMetaTransactionMethodName)),
    };
    let arguments = "Arguments".to_string() + &method_def[method_arg_start..];
    // Note: method_def is like "adopt(uint256 petId,PetObj petObj)PetObj(string name,address owner)",
    // MUST have no space after `,`. EIP-712 requires hashStruct start by packing the typeHash,
    // See "Rationale for typeHash" in https://eips.ethereum.org/EIPS/eip-712#definition-of-hashstruct
    // method_def is used here for typeHash
    let types = "NearTx(string evmId,uint256 nonce,uint256 feeAmount,address feeAddress,address contractAddress,uint256 value,string contractMethod,Arguments arguments)".to_string() + &arguments;
    bytes.extend_from_slice(&keccak(types.as_bytes()).as_bytes());
    bytes.extend_from_slice(&keccak(account_id.as_ref().as_bytes()).as_bytes());
    bytes.extend_from_slice(&u256_to_arr(&nonce));
    bytes.extend_from_slice(&u256_to_arr(&fee_amount));
    bytes.extend_from_slice(&encode_address(fee_address));
    bytes.extend_from_slice(&encode_address(contract_address));
    bytes.extend_from_slice(&u256_to_arr(&value));

    let methods = MethodAndTypes::parse(method_def)?;
    let method_sig = method_signature(&methods);
    bytes.extend_from_slice(&keccak(method_sig.as_bytes()).as_bytes());

    let mut arg_bytes = Vec::new();
    arg_bytes.extend_from_slice(&keccak(arguments.as_bytes()).as_bytes());
    let args_decoded: Vec<RlpValue> = rlp_decode(args)?;
    for i in 0..args_decoded.len() {
        arg_bytes.extend_from_slice(&eip_712_hash_argument(
            &methods.method.args[i].t,
            &args_decoded[i],
            &methods.types,
        )?);
    }

    // ETH-ABI require function selector to use method_sig, instead of method_name,
    // See https://docs.soliditylang.org/en/v0.7.5/abi-spec.html#function-selector
    // Above spec is not completely clear, this implementation shows signature is the one without
    // argument name:
    // https://github.com/rust-ethereum/ethabi/blob/69285cf6b6202d9faa19c7d0239df6a2bd79d55f/ethabi/src/signature.rs#L28
    let method_selector = method_sig_to_abi(&method_sig);
    let args_eth_abi = eth_abi_encode_args(&args_decoded, &methods)?;
    let input = [method_selector.to_vec(), args_eth_abi.to_vec()].concat();

    let arg_bytes_hash: RawU256 = keccak(&arg_bytes).into();
    bytes.extend_from_slice(&arg_bytes_hash);

    let message: RawU256 = keccak(&bytes).into();
    let mut bytes = Vec::with_capacity(2 + 32 + 32);
    bytes.extend_from_slice(&[0x19, 0x01]);
    bytes.extend_from_slice(domain_separator);
    bytes.extend_from_slice(&message);
    Ok((keccak(&bytes).into(), input))
}

pub fn parse_meta_call(
    domain_separator: &RawU256,
    account_id: &AccountId,
    args: Vec<u8>,
) -> Result<InternalMetaCallArgs> {
    let meta_tx = MetaCallArgs::try_from_slice(&args)
        .map_err(|_| VMLogicError::EvmError(EvmError::ArgumentParseError))?;
    // if args.len() <= 202 {
    //     return Err(VMLogicError::EvmError(EvmError::ArgumentParseError));
    // }
    // let mut signature: [u8; 65] = [0; 65];
    // // Signatures coming from outside are srv but ecrecover takes vsr, so move last byte to first position.
    // signature[0] = args[64];
    // signature[1..].copy_from_slice(&args[..64]);
    // let nonce = U256::from_big_endian(&args[65..97]);
    // let fee_amount = U256::from_big_endian(&args[97..129]);
    // let fee_address = Address::from_slice(&args[129..149]);
    // let contract_address = Address::from_slice(&args[149..169]);
    // let value = U256::from_big_endian(&args[169..201]);
    // let mut method_def_len_buf = [0u8; 2];
    // method_def_len_buf.copy_from_slice(&args[201..203]);
    // let method_def_len = u16::from_be_bytes(method_def_len_buf) as usize;
    // if args.len() < method_def_len + 203 {
    //     return Err(VMLogicError::EvmError(EvmError::ArgumentParseError));
    // }
    // let method_def = String::from_utf8(args[203..203 + method_def_len].to_vec())
    //     .map_err(|_| VMLogicError::EvmError(EvmError::ArgumentParseError))?;
    // let args = &args[203 + method_def_len..];
    let nonce = U256::from(meta_tx.nonce);
    let fee_amount = U256::from(meta_tx.fee_amount);
    let fee_address = Address::from(meta_tx.fee_address);
    let contract_address = Address::from(meta_tx.contract_address);
    let value = U256::from(meta_tx.value);

    let (msg, input) = prepare_meta_call_args(
        domain_separator,
        account_id,
        nonce,
        fee_amount,
        fee_address,
        contract_address,
        value,
        &meta_tx.method_def,
        &meta_tx.args,
    )?;
    let mut signature: [u8; 65] = [0; 65];
    signature[0] = meta_tx.v;
    signature[1..].copy_from_slice(&meta_tx.signature);
    match ecrecover_address(&msg, &signature) {
        Some(sender) => Ok(InternalMetaCallArgs {
            sender,
            nonce,
            fee_amount,
            fee_address,
            contract_address,
            value,
            input,
        }),
        None => Err(VMLogicError::EvmError(EvmError::InvalidEcRecoverSignature)),
    }
}

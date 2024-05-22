use crate::types::Action;
use aurora_engine_transactions::EthTransactionKind;

pub fn abi_encode(action: Action) -> Vec<u8> {
    let mut buf = Vec::new();
    match action {
        Action::FunctionCall { receiver_id, method_name, args, gas, yocto_near } => {
            buf.extend_from_slice(crate::types::FUNCTION_CALL_SELECTOR);
            let tokens = &[
                ethabi::Token::String(receiver_id),
                ethabi::Token::String(method_name),
                ethabi::Token::Bytes(args),
                ethabi::Token::Uint(gas.into()),
                ethabi::Token::Uint(yocto_near.into()),
            ];
            buf.extend_from_slice(&ethabi::encode(tokens));
        }
        Action::Transfer { receiver_id, yocto_near } => {
            buf.extend_from_slice(crate::types::TRANSFER_SELECTOR);
            let tokens =
                &[ethabi::Token::String(receiver_id), ethabi::Token::Uint(yocto_near.into())];
            buf.extend_from_slice(&ethabi::encode(tokens));
        }
        Action::AddKey {
            public_key_kind,
            public_key,
            nonce,
            is_full_access,
            is_limited_allowance,
            allowance,
            receiver_id,
            method_names,
        } => {
            buf.extend_from_slice(crate::types::ADD_KEY_SELECTOR);
            let tokens = &[
                ethabi::Token::Uint(public_key_kind.into()),
                ethabi::Token::Bytes(public_key),
                ethabi::Token::Uint(nonce.into()),
                ethabi::Token::Bool(is_full_access),
                ethabi::Token::Bool(is_limited_allowance),
                ethabi::Token::Uint(allowance.into()),
                ethabi::Token::String(receiver_id),
                ethabi::Token::Array(method_names.into_iter().map(ethabi::Token::String).collect()),
            ];
            buf.extend_from_slice(&ethabi::encode(tokens));
        }
        Action::DeleteKey { public_key_kind, public_key } => {
            buf.extend_from_slice(crate::types::DELETE_KEY_SELECTOR);
            let tokens =
                &[ethabi::Token::Uint(public_key_kind.into()), ethabi::Token::Bytes(public_key)];
            buf.extend_from_slice(&ethabi::encode(tokens));
        }
    };
    buf
}

pub fn rlp_encode(transaction: &EthTransactionKind) -> Vec<u8> {
    transaction.into()
}

pub fn encode_b64(input: &[u8]) -> String {
    use base64::Engine;
    let engine = base64::engine::general_purpose::STANDARD;
    engine.encode(input)
}

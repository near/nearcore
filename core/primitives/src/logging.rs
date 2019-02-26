use std::fmt::Debug;
use bs58;
use serde::Serialize;

const HASH_LENGTH_CHARS: usize = 44;
const SIGNATURE_LENGTH_BYTES: usize = 64;
const VECTOR_MAX_LENGTH: usize = 5;
const STRING_SHORT_LEN: usize = 6;

pub fn pretty_vec<T: Debug>(buf: &[T]) -> String {
    if buf.len() <= VECTOR_MAX_LENGTH {
        format!("{:#?}", buf)
    } else {
        format!("({})[{:#?}, {:#?}, … {:#?}, {:#?}]", buf.len(), buf[0], buf[1], buf[buf.len() - 2], buf[buf.len() - 1])
    }
}

pub fn pretty_str(s: &str, short_len: usize, print_len: usize) -> String {
    if s.len() <= print_len {
        format!("`{}`", s)
    } else {
        format!("({})`{}…{}`", s.len(), &s[..short_len], &s[s.len()-short_len..])
    }
}

pub fn pretty_hash(s: &str) -> String {
    pretty_str(s, STRING_SHORT_LEN, HASH_LENGTH_CHARS)
}

pub fn pretty_utf8(buf: &[u8]) -> String {
    match std::str::from_utf8(buf) {
        Ok(s) => pretty_hash(s),
        Err(_) => {
            if buf.len() <= SIGNATURE_LENGTH_BYTES {
                pretty_hash(&bs58::encode(buf).into_string())
            } else {
                pretty_vec(buf)
            }
        }
    }
}

pub fn pretty_result(result: &Option<Vec<u8>>) -> String {
    match result {
        Some(ref v) => pretty_utf8(&v),
        None => "None".to_string(),
    }
}

pub fn pretty_results(results: &Vec<Option<Vec<u8>>>) -> String {
    let v: Vec<String> = results.iter().map(pretty_result).collect();
    format!("{:?}", pretty_vec(&v))
}

pub fn pretty_serializable<T: Serialize>(s: &T) -> String {
    match bincode::serialize(&s) {
        Ok(buf) => pretty_hash(&bs58::encode(&buf).into_string()),
        Err(e) => format!("[failed to serialize: {}]", e)
    }
}
use std::fmt::Debug;

use crate::serialize::to_base58;

const VECTOR_MAX_LENGTH: usize = 5;
const STRING_PRINT_LEN: usize = 128;

pub fn pretty_vec<T: Debug>(buf: &[T]) -> String {
    if buf.len() <= VECTOR_MAX_LENGTH {
        format!("{:#?}", buf)
    } else {
        format!(
            "({})[{:#?}, {:#?}, … {:#?}, {:#?}]",
            buf.len(),
            buf[0],
            buf[1],
            buf[buf.len() - 2],
            buf[buf.len() - 1]
        )
    }
}

pub fn pretty_str(s: &str, print_len: usize) -> String {
    if s.len() <= print_len {
        format!("`{}`", s)
    } else {
        format!("({})`{}…`", s.len(), &s.chars().take(print_len).collect::<String>())
    }
}

pub fn pretty_hash(s: &str) -> String {
    pretty_str(s, STRING_PRINT_LEN)
}

pub fn pretty_utf8(buf: &[u8]) -> String {
    match std::str::from_utf8(buf) {
        Ok(s) => pretty_hash(s),
        Err(_) => {
            if buf.len() <= STRING_PRINT_LEN {
                pretty_hash(&to_base58(buf))
            } else {
                pretty_vec(buf)
            }
        }
    }
}

pub fn pretty_result(result: &Option<Vec<u8>>) -> String {
    match result {
        Some(ref v) => pretty_utf8(v),
        None => "None".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static HI_NEAR: &str = "Привет, NEAR";

    #[test]
    fn test_non_ut8_string_truncation() {
        assert_eq!(format!("({})`Привет…`", HI_NEAR.len()), pretty_str(HI_NEAR, 6));
    }

    #[test]
    fn test_non_ut8_more_bytes_same_char_count() {
        assert_eq!(
            format!("({})`{}…`", HI_NEAR.len(), HI_NEAR),
            pretty_str(HI_NEAR, HI_NEAR.chars().count())
        );
    }

    #[test]
    fn test_non_ut8_no_truncation() {
        assert_eq!(format!("`{}`", HI_NEAR), pretty_str(HI_NEAR, HI_NEAR.len()));
    }
}

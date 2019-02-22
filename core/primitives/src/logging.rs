use std::fmt::Debug;

pub fn pretty_vec<T: Debug>(buf: &[T]) -> String {
    if buf.len() <= 5 {
        format!("{:?}", buf)
    } else {
        format!("[{:?}, {:?}, {:?}, ...{} more]", buf[0], buf[1], buf[2], buf.len() - 3)
    }
}

pub fn pretty_str(s: &str, short_len: usize) -> String {
    if s.len() <= short_len + 10 {
        format!("\"{}\"", s)
    } else {
        format!("\"{}...{} more\"", &s[..short_len], s.len() - short_len)
    }
}

pub fn pretty_hash(s: &str) -> String {
    pretty_str(s, 8)
}

pub fn pretty_utf8(buf: &[u8]) -> String {
    match std::str::from_utf8(buf) {
        Ok(s) => pretty_str(s, 22).to_string(),
        Err(_) => pretty_vec(buf),
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
use lazy_static::lazy_static;
const ACCOUNT_PREFIXS: &'static str = include_str!("../accounts.txt");

lazy_static! {
    static ref ACCOUNT_PREFIX_VEC: Vec<&'static str> = ACCOUNT_PREFIXS.split("\n").collect();
}

pub fn account_k(k: usize) -> String {
    let i = k / ACCOUNT_PREFIXS.len();
    if i > 0 {
        return format!("{}{}_near", ACCOUNT_PREFIX_VEC[k % ACCOUNT_PREFIXS.len()], i);
    } else {
        return format!("{}_near", ACCOUNT_PREFIX_VEC[k % ACCOUNT_PREFIXS.len()]);
    }
}

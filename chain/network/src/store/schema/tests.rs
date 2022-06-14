use super::*;
use crate::tests::data;
use crate::tests::util;

#[test]
fn borsh_wrapper_is_transparent() {
    let mut rng = util::make_rng(423423);
    let rng = &mut rng;
    let s1 = data::make_signer(rng);
    let s2 = data::make_signer(rng);
    let e = data::make_edge(rng, &s1, &s2);
    assert_eq!(Borsh(e.clone()).try_to_vec().unwrap(), e.try_to_vec().unwrap());
}

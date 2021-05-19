use blake2::{Blake2b, Blake2s, Digest};
use hex_literal::hex;

#[test]
fn blake2s_persona() {
    let key_bytes = hex!("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f");
    let persona = "personal";
    let persona_bytes = persona.as_bytes();
    let ctx = Blake2s::with_params(&key_bytes, &[], persona_bytes);
    assert_eq!(
        ctx.finalize().as_slice(),
        &hex!("25a4ee63b594aed3f88a971e1877ef7099534f9097291f88fb86c79b5e70d022")[..]
    );
}

#[test]
fn blake2b_persona() {
    let key_bytes = hex!("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f");
    let persona = "personal";
    let persona_bytes = persona.as_bytes();
    let ctx = Blake2b::with_params(&key_bytes, &[], persona_bytes);
    assert_eq!(ctx.finalize().as_slice(), &hex!("03de3b295dcfc3b25b05abb09bc95fe3e9ff3073638badc68101d1e42019d0771dd07525a3aae8318e92c5e5d967ba92e4810d0021d7bf3b49da0b4b4a8a4e1f")[..]);
}

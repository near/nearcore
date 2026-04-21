// cspell:words Wycheproof Shamir FIPS Pubkeys

//! Wycheproof adversarial vectors for `p256_verify`.
//!
//! Vectors from C2SP/wycheproof ecdsa_secp256r1_sha256_p1363_test.json — the
//! "P1363" variant matches our raw r||s encoding. Pubkeys are SEC1 compressed
//! (33 bytes). These exist to catch regressions where the p256 crate or our
//! wrapper changes behavior around edge-case signatures.
//!
//! These tests cover the RustCrypto `p256` crate's behavior rather than our
//! own code — we pass signatures through unchanged. In particular, `p256`
//! accepts high-s signatures (FIPS 186-5 compliant; low-s enforcement is a
//! Bitcoin convention, BIP-62, not an ECDSA requirement). Changing this would
//! be protocol-breaking. See `test_p256_verify_wycheproof_high_s_accepted`.

use crate::logic::tests::vm_logic_builder::VMLogicBuilder;

fn run_wycheproof(pubkey_hex: &str, msg_hex: &str, sig_hex: &str, expected: u64) {
    let signature = hex::decode(sig_hex).expect("bad sig hex");
    let message = hex::decode(msg_hex).expect("bad msg hex");
    let public_key = hex::decode(pubkey_hex).expect("bad pk hex");

    let mut builder = VMLogicBuilder::default();
    let mut logic = builder.build();

    let sig_ptr = logic.internal_mem_write(&signature).ptr;
    let msg_ptr = logic.internal_mem_write(&message).ptr;
    let pk_ptr = logic.internal_mem_write(&public_key).ptr;

    let result = logic
        .p256_verify(
            signature.len() as u64,
            sig_ptr,
            message.len() as u64,
            msg_ptr,
            public_key.len() as u64,
            pk_ptr,
        )
        .expect("p256_verify should not abort for well-formed 64-byte sig + 33-byte pk");
    assert_eq!(result, expected, "wycheproof mismatch: expected {expected}, got {result}");
}

#[test]
fn test_p256_verify_wycheproof_valid() {
    // Wycheproof tcId 60 — "Edge case for Shamir multiplication". Valid.
    run_wycheproof(
        "022927b10512bae3eddcfe467828128bad2903269919f7086069c8c4df6c732838",
        "3639383139",
        "64a1aab5000d0e804f3e2fc02bdee9be8ff312334e2ba16d11547c97711c898e\
         6af015971cc30be6d1a206d4e013e0997772a2f91d73286ffd683b9bb2cf4f1b",
        1,
    );
}

#[test]
fn test_p256_verify_wycheproof_low_s() {
    // Wycheproof tcId 1 — "signature malleability", low-s half. Valid.
    run_wycheproof(
        "022927b10512bae3eddcfe467828128bad2903269919f7086069c8c4df6c732838",
        "313233343030",
        "2ba3a8be6b94d5ec80a6d9d1190a436effe50d85a1eee859b8cc6af9bd5c2e18\
         4cd60b855d442f5b3c7b11eb6c4e0ae7525fe710fab9aa7c77a67f79e6fadd76",
        1,
    );
}

#[test]
fn test_p256_verify_wycheproof_high_s_accepted() {
    // Wycheproof tcId 171 — "edge case for signature malleability". Wycheproof
    // labels this valid. We accept it because p256 crate does not enforce
    // low-s normalization. Flip this test the day we decide to enforce low-s.
    run_wycheproof(
        "0268ec6e298eafe16539156ce57a14b04a7047c221bafc3a582eaeb0d857c4d946",
        "313233343030",
        "7fffffff800000007fffffffffffffffde737d56d38bcf4279dce5617e3192a9\
         7fffffff800000007fffffffffffffffde737d56d38bcf4279dce5617e3192a9",
        1,
    );
}

#[test]
fn test_p256_verify_wycheproof_r_zero_s_zero() {
    // Wycheproof tcId 11 — r=0, s=0. Invalid.
    run_wycheproof(
        "022927b10512bae3eddcfe467828128bad2903269919f7086069c8c4df6c732838",
        "313233343030",
        "0000000000000000000000000000000000000000000000000000000000000000\
         0000000000000000000000000000000000000000000000000000000000000000",
        0,
    );
}

#[test]
fn test_p256_verify_wycheproof_r_one_s_zero() {
    // Wycheproof tcId 18 — r=1, s=0. Invalid.
    run_wycheproof(
        "022927b10512bae3eddcfe467828128bad2903269919f7086069c8c4df6c732838",
        "313233343030",
        "0000000000000000000000000000000000000000000000000000000000000001\
         0000000000000000000000000000000000000000000000000000000000000000",
        0,
    );
}

#[test]
fn test_p256_verify_wycheproof_r_eq_n() {
    // Wycheproof tcId 25 — r = curve order n, s=0. Invalid.
    run_wycheproof(
        "022927b10512bae3eddcfe467828128bad2903269919f7086069c8c4df6c732838",
        "313233343030",
        "ffffffff00000000ffffffffffffffffbce6faada7179e84f3b9cac2fc632551\
         0000000000000000000000000000000000000000000000000000000000000000",
        0,
    );
}

#[test]
fn test_p256_verify_wycheproof_r_replaced_by_n_minus_r() {
    // Wycheproof tcId 4 — r replaced by n - r. Invalid.
    run_wycheproof(
        "022927b10512bae3eddcfe467828128bad2903269919f7086069c8c4df6c732838",
        "313233343030",
        "d45c5740946b2a147f59262ee6f5bc90bd01ed280528b62b3aed5fc93f06f739\
         b329f479a2bbd0a5c384ee1493b1f5186a87139cac5df4087c134b49156847db",
        0,
    );
}

#[test]
fn test_p256_verify_wycheproof_invalid_public_key_tag() {
    // Valid Wycheproof tcId 1 signature, but pubkey's SEC1 tag byte is
    // mutated from 0x02 (compressed, even y) to 0x05 (reserved/invalid). The
    // p256 crate should fail to parse the key, and we should return 0.
    run_wycheproof(
        "052927b10512bae3eddcfe467828128bad2903269919f7086069c8c4df6c732838",
        "313233343030",
        "2ba3a8be6b94d5ec80a6d9d1190a436effe50d85a1eee859b8cc6af9bd5c2e18\
         4cd60b855d442f5b3c7b11eb6c4e0ae7525fe710fab9aa7c77a67f79e6fadd76",
        0,
    );
}

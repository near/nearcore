use crate::logic::tests::vm_logic_builder::VMLogicBuilder;
use crate::logic::{HostError, VMLogicError};
use std::fmt::Write;

/// Converts a sequence of integers to a single little-endian encoded byte
/// vector. `u8` literals like `0u8` are treated as u8, hex literals like
/// `0xbeef` are treated as `u256`.
///
/// Comas (`,`) can be used for readability, but don't affect semantics
macro_rules! le_bytes {
    ($($lit:tt)*) => (
        {
            #[allow(unused_mut)]
            let mut buf: Vec<u8> = Vec::new();
            $(parse_le_bytes(stringify!($lit), &mut buf);)*
            buf
        }
    )
}

fn parse_le_bytes(s: &str, buf: &mut Vec<u8>) {
    if s == "," {
        return;
    }
    if let Some(byte) = s.strip_suffix("u8") {
        buf.push(byte.parse::<u8>().unwrap());
        return;
    }
    let s = s.strip_prefix("0x").expect("0x prefix was expected");
    let zeros = "0".repeat(64 - s.len());
    let s = format!("{zeros}{s}");
    let hi = u128::from_str_radix(&s[..32], 16).unwrap();
    let lo = u128::from_str_radix(&s[32..], 16).unwrap();
    buf.extend(lo.to_le_bytes());
    buf.extend(hi.to_le_bytes());
}

/// Renders a slice of bytes representing a point on the curve (a pair of u256)
/// as `[le_bytes!]` compatible representation.
fn render_le_bytes(p: &[u8]) -> String {
    assert_eq!(p.len(), 64);
    let mut res = String::new();
    res.push_str("[");
    let mut first = true;
    for c in [&p[..32], &p[32..]] {
        if !first {
            res.push(' ');
        }
        first = false;
        res.push_str("0x");
        for b in c.iter().rev() {
            write!(res, "{:02x}", b).unwrap();
        }
    }
    res.push_str("]");
    res
}

#[track_caller]
fn assert_eq_points(left: &[u8], right: &[u8]) {
    assert_eq!(
        left,
        right,
        "differet poits on the cureve\n  left: {}\n  right {}\n",
        render_le_bytes(left),
        render_le_bytes(right)
    );
}

#[track_caller]
fn check_result<T, U>(
    actual: Result<T, VMLogicError>,
    expected: Result<U, &str>,
) -> Option<(T, U)> {
    match (actual, expected) {
        (Ok(actual), Ok(expected)) => Some((actual, expected)),
        (Err(VMLogicError::HostError(HostError::AltBn128InvalidInput { msg: err })), Err(msg)) => {
            let err = err;
            assert!(err.contains(msg), "expected `{msg}` error, got {err}");
            None
        }
        (Ok(_), Err(msg)) => panic!("expected `{msg}` error"),
        (Err(err), _) => panic!("unexpected eror: `{}`", err.to_string()),
    }
}

#[test]
fn test_alt_bn128_g1_multiexp() {
    #[track_caller]
    fn check(input: &[u8], expected: Result<&[u8], &str>) {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();
        let input = logic.internal_mem_write(input);

        let res = logic.alt_bn128_g1_multiexp(input.len, input.ptr, 0);
        if let Some(((), expected)) = check_result(res, expected) {
            let got = logic.registers().get_for_free(0).unwrap();
            assert_eq_points(&expected, got);
        }
    }
    #[track_caller]
    fn check_ok(input: &[u8], expcted: &[u8]) {
        check(input, Ok(expcted))
    }
    #[track_caller]
    fn check_err(input: &[u8], expected_err: &str) {
        check(input, Err(expected_err))
    }

    check_ok(&le_bytes![], &le_bytes![0x0 0x0]);
    check_ok(
        &le_bytes![
            0x2d6b17489d86fcd5f91e8e92eb55081d8cb4413e408047249ef4fb5baa1b518b 0x1e4d0a30dbadd9dad40f7847c7013754ded8d0371c052d19f01453f4ae1506d7 0x1,
        ],
        &le_bytes![0x2d6b17489d86fcd5f91e8e92eb55081d8cb4413e408047249ef4fb5baa1b518b 0x1e4d0a30dbadd9dad40f7847c7013754ded8d0371c052d19f01453f4ae1506d7],
    );

    check_ok(
        &le_bytes![
            0x12b453155ca3be5d0b14a4804cc39a0b4635b06d512735350cd031051644d3ec 0x2944829dcfa7dd72bb04d12e46869e6a6c8162698f9a6c35724f91f597e25fc4  0x112b450c0769c7cd80ffa552aaab2153adb5646664ee091639784a7f887411f7,
            0x032b2c8b9f1e7f5e53c262ae87ccd1366df1af019f2dbfe1a58a6749ba4b923f 0x0017741cde8d55ccce3a1dac210f531d22f574885226ea46fbbce4a4c75f7ed8  0x19e4956d3cf5e04f938bc9dee72f2fcab15934c7e0450ae15afee161606b071a,
        ],
        &le_bytes![
            0x2923a9d452a047e0f24ab419d7893ecbf0c32a842afd88f991a6723decba82aa 0x2e3a00f94191675c0730510133c2fca248160750d87b5157c534146d4d260b61
        ],
    );
    check_ok(
        &le_bytes![
            0x26a1602aeb36e32dd1c534b1c014e920b138f4a8b87f2833ea6051c8cbd5eea2 0x01eb929f0ab6720df89837a84f2787d6d8a8bd97e0daab0576321d85143633ee 0x1,
            0x15ad51e3d708bdf9ae99a3732af9354cc7b0f2ce71832b958b3e9b0215e63578 0x231d7b68932527abdeb71488bd5c1e339306c10490d3c65f7daaa651a367c618 0x1,
            0x1302ac2f870ef22bdec4cd48058f309bcef761a7b40f78744157f3bbf25a016d 0x0c75e87050e62a4c3bf1e261da5a0f11c7ccaa090a0585365658f7a2b4b96fee 0x1,
        ],
        &le_bytes![
            0x2ee5c54dee890fb79c9964f7a08c7295111990435dc3adff9fb6d63aadded21f 0x2c3fa1abf295e7df565379efcdb0398d08fc959a0a7e5d3f30118fefe9450a67
        ],
    );

    check_err(b"XXXX", "slice of size 4 cannot be precisely split into chunks of size 96");
    check_err(
        &le_bytes![0x92  0x2944829dcfa7dd72bb04d12e46869e6a6c8162698f9a6c35724f91f597e25fc4 0x112b450c0769c7cd80ffa552aaab2153adb5646664ee091639784a7f887411f7],
        "invalid g1",
    );
    check_err(
        &le_bytes![
            0x12b453155ca3be5d0b14a4804cc39a0b4635b06d512735350cd031051644d3ec 0x2944829dcfa7dd72bb04d12e46869e6a6c8162698f9a6c35724f91f597e25fc4  0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        ],
        "invalid fr",
    );
}

#[test]
fn test_alt_bn128_g1_sum() {
    #[track_caller]
    fn check(input: &[u8], expected: Result<&[u8], &str>) {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();
        let input = logic.internal_mem_write(input);

        let res = logic.alt_bn128_g1_sum(input.len, input.ptr, 0);
        if let Some(((), expected)) = check_result(res, expected) {
            let got = logic.registers().get_for_free(0).unwrap();
            assert_eq_points(&expected, got);
        }
    }
    #[track_caller]
    fn check_ok(input: &[u8], expcted: &[u8]) {
        check(input, Ok(expcted))
    }
    #[track_caller]
    fn check_err(input: &[u8], expected_err: &str) {
        check(input, Err(expected_err))
    }

    check_ok(&le_bytes![], &le_bytes![0x0 0x0]);
    check_ok(
        &le_bytes![
            0u8 0x2d6b17489d86fcd5f91e8e92eb55081d8cb4413e408047249ef4fb5baa1b518b 0x1e4d0a30dbadd9dad40f7847c7013754ded8d0371c052d19f01453f4ae1506d7,
        ],
        &le_bytes![0x2d6b17489d86fcd5f91e8e92eb55081d8cb4413e408047249ef4fb5baa1b518b 0x1e4d0a30dbadd9dad40f7847c7013754ded8d0371c052d19f01453f4ae1506d7,],
    );
    check_ok(
        &le_bytes![
            0u8  0x12b453155ca3be5d0b14a4804cc39a0b4635b06d512735350cd031051644d3ec 0x2944829dcfa7dd72bb04d12e46869e6a6c8162698f9a6c35724f91f597e25fc4,
            1u8  0x032b2c8b9f1e7f5e53c262ae87ccd1366df1af019f2dbfe1a58a6749ba4b923f 0x304cda5602a44a5cea16280a60720540748bf609164ae0464063a772111d7e6f,
        ],
        &le_bytes![
            0x1e2e53eecef3185d5c874e783cb184d302692a22c20f5f3b3993882e184d8fe8 0x03fc2454d3d88f9eac441e9b85a9041e925f30cca7a82d039d1ffb582bfb2004
        ],
    );

    check_ok(
        &le_bytes![
            0u8 0x26a1602aeb36e32dd1c534b1c014e920b138f4a8b87f2833ea6051c8cbd5eea2 0x01eb929f0ab6720df89837a84f2787d6d8a8bd97e0daab0576321d85143633ee,
            0u8 0x15ad51e3d708bdf9ae99a3732af9354cc7b0f2ce71832b958b3e9b0215e63578 0x231d7b68932527abdeb71488bd5c1e339306c10490d3c65f7daaa651a367c618,
            0u8 0x1302ac2f870ef22bdec4cd48058f309bcef761a7b40f78744157f3bbf25a016d 0x0c75e87050e62a4c3bf1e261da5a0f11c7ccaa090a0585365658f7a2b4b96fee,
        ],
        &le_bytes![
            0x2ee5c54dee890fb79c9964f7a08c7295111990435dc3adff9fb6d63aadded21f 0x2c3fa1abf295e7df565379efcdb0398d08fc959a0a7e5d3f30118fefe9450a67
        ],
    );

    check_err(&[92], "slice of size 1 cannot be precisely split into chunks of size 65");
    check_err(
        &le_bytes![
            0u8  0x111 0x222
        ],
        "invalid g1",
    );
    check_err(
        &le_bytes![92u8  0x12b453155ca3be5d0b14a4804cc39a0b4635b06d512735350cd031051644d3ec 0x2944829dcfa7dd72bb04d12e46869e6a6c8162698f9a6c35724f91f597e25fc4],
        "invalid bool",
    );
}

#[test]
fn test_alt_bn128_pairing_check() {
    #[track_caller]
    fn check(input: &[u8], expected: Result<u64, &str>) {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();
        let input = logic.internal_mem_write(input);

        let res = logic.alt_bn128_pairing_check(input.len, input.ptr);
        if let Some((res, expected)) = check_result(res, expected) {
            assert_eq!(res, expected)
        }
    }
    #[track_caller]
    fn check_ok(input: &[u8], expcted: u64) {
        check(input, Ok(expcted))
    }
    #[track_caller]
    fn check_err(input: &[u8], expected_err: &str) {
        check(input, Err(expected_err))
    }

    check_ok(&[], 1);
    check_ok(
        &le_bytes![
            0x0763111c06b5066cc812f8e058d5b82a7bc356c83a1a5ab743ea4e7163d90a75 0x041068a8ed41f6755c1ad2d30aacc3974a4fae3293aee34c7103b4c073358624

            0x291aea4fac742aaf8772caa00c8763d509c3d6f20d1be64d73c10d06db031a01 0x1de7a958e4adb3a128cd3b942b13bc85ee4124f0dee6f4266b39707c81c06fd4
            0x1e2d816ba8b96e5cb444883a2b095533becb805c654418fa2c65bba533a34311 0x195dd519dc841a301a14de412c520b858c01ccc7ba0bd4177dd85d4b116416bb,


            0x1f1cebcff0a999ffa7c1b582e9afd6c875760fde2679e9fd830bf3979504b04f 0x179fe95fb3fb3807ebbad5ed2b40ddd06f7b73525382a64a43489606b3454a53

            0x201eef3c872a7313da79f6aa628cc9795314fbac78484fdae2eba5086755ad6d 0x204e0376b942fe92ba6e2746d07d62f5dede7b8b6e172fa89ea0c5c4ccabaa31
            0x00e9f62300f921f5d4f2f7008aec59449a3f5e75add585ec4eccf04f5dc5b32f 0x17c150f0fb2ff472816b41868b98b9170233ee4647fe4bc41a1336c9a2c6567c
        ],
        1,
    );
    check_ok(
        &le_bytes![
            0x0763111c06b5066cc812f8e058d5b82a7bc356c83a1a5ab743ea4e7163d90a75 0x041068a8ed41f6755c1ad2d30aacc3974a4fae3293aee34c7103b4c073358624

            0x291aea4fac742aaf8772caa00c8763d509c3d6f20d1be64d73c10d06db031a01 0x1de7a958e4adb3a128cd3b942b13bc85ee4124f0dee6f4266b39707c81c06fd4
            0x1e2d816ba8b96e5cb444883a2b095533becb805c654418fa2c65bba533a34311 0x195dd519dc841a301a14de412c520b858c01ccc7ba0bd4177dd85d4b116416bb,


            0x0763111c06b5066cc812f8e058d5b82a7bc356c83a1a5ab743ea4e7163d90a75 0x041068a8ed41f6755c1ad2d30aacc3974a4fae3293aee34c7103b4c073358624

            0x201eef3c872a7313da79f6aa628cc9795314fbac78484fdae2eba5086755ad6d 0x204e0376b942fe92ba6e2746d07d62f5dede7b8b6e172fa89ea0c5c4ccabaa31
            0x00e9f62300f921f5d4f2f7008aec59449a3f5e75add585ec4eccf04f5dc5b32f 0x17c150f0fb2ff472816b41868b98b9170233ee4647fe4bc41a1336c9a2c6567c
        ],
        0,
    );

    check_err(b"XXXX", "slice of size 4 cannot be precisely split into chunks of size 192");
    check_err(&le_bytes![0x0 0x0  0x0 0x0 0x0 0x0, 0x0 0x0  0x0 0x0 0x0 0x111], "invalid g2");
}

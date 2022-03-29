use near_vm_errors::{HostError, VMLogicError};

use super::{fixtures::get_context, vm_logic_builder::VMLogicBuilder};

#[track_caller]
fn check_result<T, U>(
    actual: Result<T, VMLogicError>,
    expected: Result<U, &str>,
) -> Option<(T, U)> {
    match (actual, expected) {
        (Ok(actual), Ok(expected)) => Some((actual, expected)),
        (Err(VMLogicError::HostError(HostError::AltBn128InvalidInput { msg: err })), Err(msg)) => {
            let err = err.to_string();
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
    fn check(input: &str, expected: Result<&str, &str>) {
        let input = base64::decode(input).unwrap();
        let expected = expected.map(|it| base64::decode(it).unwrap());

        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build(get_context(vec![], false));

        let res = logic.alt_bn128_g1_multiexp(input.len() as _, input.as_ptr() as _, 0);
        if let Some(((), expected)) = check_result(res, expected) {
            let len = logic.register_len(0).unwrap();
            let mut res = vec![0u8; len as usize];
            logic.read_register(0, res.as_mut_ptr() as _).unwrap();
            assert_eq!(res, expected)
        }
    }
    #[track_caller]
    fn check_ok(input: &str, expcted: &str) {
        check(input, Ok(expcted))
    }
    #[track_caller]
    fn check_err(input: &str, expected_err: &str) {
        check(input, Err(expected_err))
    }

    check_ok(
        "",
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==",
    );
    check_ok(
        "7NNEFgUx0Aw1NSdRbbA1Rguaw0yApBQLXb6jXBVTtBLEX+KX9ZFPcjVsmo9pYoFsap6GRi7RBLty3afPnYJEKfcRdIh/Sng5FgnuZGZkta1TIauqUqX/gM3HaQcMRSsRP5JLuklniqXhvy2fAa/xbTbRzIeuYsJTXn8en4ssKwPYfl/HpOS8+0bqJlKIdPUiHVMPIawdOs7MVY3eHHQXABoHa2Bh4f5a4QpF4Mc0WbHKLy/n3smLk0/g9TxtleQZ",
    "qoK67D1yppH5iP0qhCrD8Ms+idcZtEry4EegUtSpIylhCyZNbRQ0xVdRe9hQBxZIovzCMwFRMAdcZ5FB+QA6Lg==",
    );

    check_err("XXXX", "leftover bytes");
    check_err(
        "XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "invalid g1",
    );
    check_err(
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAXXX",
        "invalid fr",
    );
}

#[test]
fn test_alt_bn128_g1_sum() {
    #[track_caller]
    fn check(input: &str, expected: Result<&str, &str>) {
        let input = base64::decode(input).unwrap();
        let expected = expected.map(|it| base64::decode(it).unwrap());

        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build(get_context(vec![], false));

        let res = logic.alt_bn128_g1_sum(input.len() as _, input.as_ptr() as _, 0);
        if let Some(((), expected)) = check_result(res, expected) {
            let len = logic.register_len(0).unwrap();
            let mut res = vec![0; len as usize];
            logic.read_register(0, res.as_mut_ptr() as _).unwrap();
            assert_eq!(res, expected)
        }
    }
    #[track_caller]
    fn check_ok(input: &str, expcted: &str) {
        check(input, Ok(expcted))
    }
    #[track_caller]
    fn check_err(input: &str, expected_err: &str) {
        check(input, Err(expected_err))
    }

    check_ok(
        "",
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==",
    );
    check_ok(
        "AOzTRBYFMdAMNTUnUW2wNUYLmsNMgKQUC12+o1wVU7QSxF/il/WRT3I1bJqPaWKBbGqehkYu0QS7ct2nz52CRCkBP5JLuklniqXhvy2fAa/xbTbRzIeuYsJTXn8en4ssKwNvfh0RcqdjQEbgShYJ9ot0QAVyYAooFupcSqQCVtpMMA==",
        "6I9NGC6Ikzk7Xw/CIippAtOEsTx4TodcXRjzzu5TLh4EIPsrWPsfnQMtqKfMMF+SHgSphZseRKyej9jTVCT8Aw=="
    );

    check_err("XXXX", "leftover bytes");
    check_err(
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAXAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        "invalid g1",
    );
    check_err(
        "XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        "invalid bool",
    );
}

#[test]
fn test_alt_bn128_pairing_check() {
    #[track_caller]
    fn check(input: &str, expected: Result<u64, &str>) {
        let input = base64::decode(input).unwrap();

        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build(get_context(vec![], false));

        let res = logic.alt_bn128_pairing_check(input.len() as _, input.as_ptr() as _);
        if let Some((res, expected)) = check_result(res, expected) {
            assert_eq!(res, expected)
        }
    }
    #[track_caller]
    fn check_ok(input: &str, expcted: u64) {
        check(input, Ok(expcted))
    }
    #[track_caller]
    fn check_err(input: &str, expected_err: &str) {
        check(input, Err(expected_err))
    }

    check_ok("", 1);
    check_ok(
        "dQrZY3FO6kO3Who6yFbDeyq41Vjg+BLIbAa1BhwRYwckhjVzwLQDcUzjrpMyrk9Kl8OsCtPSGlx19kHtqGgQBAEaA9sGDcFzTeYbDfLWwwnVY4cMoMpyh68qdKxP6hop1G/AgXxwOWsm9Obe8CRB7oW8EyuUO80oobOt5Fip5x0RQ6MzpbtlLPoYRGVcgMu+M1UJKzqIRLRcbrmoa4EtHrsWZBFLXdh9F9QLusfMAYyFC1IsQd4UGjAahNwZ1V0ZT7AElZfzC4P96Xkm3g92dcjWr+mCtcGn/5mp8M/rHB9TSkWzBpZIQ0qmglNSc3tv0N1AK+3VuusHOPuzX+mfF22tVWcIpevi2k9IeKz7FFN5yYxiqvZ52hNzKoc87x4gMaqrzMTFoJ6oLxdui3ve3vVifdBGJ266kv5CuXYDTiAvs8VdT/DMTuyF1a11Xj+aRFnsigD38tT1IfkAI/bpAHxWxqLJNhMaxEv+R0buMwIXuZiLhkFrgXL0L/vwUMEX",
        1,
    );

    check_err("XXXX", "leftover bytes");
    check_err(
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAXAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "invalid g2",
    );
}

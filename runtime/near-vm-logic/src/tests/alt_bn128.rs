use super::{fixtures::get_context, vm_logic_builder::VMLogicBuilder};

#[test]
fn test_alt_bn128_g1_sum() {
    let expected = base64::decode(
        "6I9NGC6Ikzk7Xw/CIippAtOEsTx4TodcXRjzzu5TLh4EIPsrWPsfnQMtqKfMMF+SHgSphZseRKyej9jTVCT8Aw==",
    )
    .unwrap();
    let input = base64::decode("AgAAAADs00QWBTHQDDU1J1FtsDVGC5rDTICkFAtdvqNcFVO0EsRf4pf1kU9yNWyaj2ligWxqnoZGLtEEu3Ldp8+dgkQpAT+SS7pJZ4ql4b8tnwGv8W020cyHrmLCU15/Hp+LLCsDb34dEXKnY0BG4EoWCfaLdEAFcmAKKBbqXEqkAlbaTDA=").unwrap();
    let input = &input[4..];

    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));

    logic.alt_bn128_g1_sum(input.len() as _, input.as_ptr() as _, 0).unwrap();
    let len = logic.register_len(0).unwrap();
    let mut res = vec![0; len as usize];
    logic.read_register(0, res.as_mut_ptr() as _).unwrap();
    assert_eq!(res, expected)
}

#[test]
fn test_alt_bn128_pairing_check() {
    let expected = 1;
    let input = base64::decode("AgAAAHUK2WNxTupDt1oaOshWw3squNVY4PgSyGwGtQYcEWMHJIY1c8C0A3FM466TMq5PSpfDrArT0hpcdfZB7ahoEAQBGgPbBg3Bc03mGw3y1sMJ1WOHDKDKcoevKnSsT+oaKdRvwIF8cDlrJvTm3vAkQe6FvBMrlDvNKKGzreRYqecdEUOjM6W7ZSz6GERlXIDLvjNVCSs6iES0XG65qGuBLR67FmQRS13YfRfUC7rHzAGMhQtSLEHeFBowGoTcGdVdGU+wBJWX8wuD/el5Jt4PdnXI1q/pgrXBp/+ZqfDP6xwfU0pFswaWSENKpoJTUnN7b9DdQCvt1brrBzj7s1/pnxdtrVVnCKXr4tpPSHis+xRTecmMYqr2edoTcyqHPO8eIDGqq8zExaCeqC8Xbot73t71Yn3QRiduupL+Qrl2A04gL7PFXU/wzE7shdWtdV4/mkRZ7IoA9/LU9SH5ACP26QB8VsaiyTYTGsRL/kdG7jMCF7mYi4ZBa4Fy9C/78FDBFw==").unwrap();
    let input = &input[4..];

    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));

    let res = logic.alt_bn128_pairing_check(input.len() as _, input.as_ptr() as _).unwrap();
    assert_eq!(res, expected)
}

#[test]
fn test_alt_bn128_g1_multiexp() {
    let expected = base64::decode(
        "qoK67D1yppH5iP0qhCrD8Ms+idcZtEry4EegUtSpIylhCyZNbRQ0xVdRe9hQBxZIovzCMwFRMAdcZ5FB+QA6Lg==",
    )
    .unwrap();
    let input = base64::decode("AgAAAOzTRBYFMdAMNTUnUW2wNUYLmsNMgKQUC12+o1wVU7QSxF/il/WRT3I1bJqPaWKBbGqehkYu0QS7ct2nz52CRCn3EXSIf0p4ORYJ7mRmZLWtUyGrqlKl/4DNx2kHDEUrET+SS7pJZ4ql4b8tnwGv8W020cyHrmLCU15/Hp+LLCsD2H5fx6TkvPtG6iZSiHT1Ih1TDyGsHTrOzFWN3hx0FwAaB2tgYeH+WuEKReDHNFmxyi8v597Ji5NP4PU8bZXkGQ==").unwrap();
    let input = &input[4..];

    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));

    logic.alt_bn128_g1_multiexp(input.len() as _, input.as_ptr() as _, 0).unwrap();
    let len = logic.register_len(0).unwrap();
    let mut res = vec![0; len as usize];
    logic.read_register(0, res.as_mut_ptr() as _).unwrap();
    assert_eq!(res, expected)
}

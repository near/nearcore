use super::{fixtures::get_context, vm_logic_builder::VMLogicBuilder};

#[test]
fn test_alt_bn128_g1_sum() {
    let expected = base64::decode(
        "6I9NGC6Ikzk7Xw/CIippAtOEsTx4TodcXRjzzu5TLh4EIPsrWPsfnQMtqKfMMF+SHgSphZseRKyej9jTVCT8Aw==",
    )
    .unwrap();
    let input = base64::decode("AOzTRBYFMdAMNTUnUW2wNUYLmsNMgKQUC12+o1wVU7QSxF/il/WRT3I1bJqPaWKBbGqehkYu0QS7ct2nz52CRCkBP5JLuklniqXhvy2fAa/xbTbRzIeuYsJTXn8en4ssKwNvfh0RcqdjQEbgShYJ9ot0QAVyYAooFupcSqQCVtpMMA==").unwrap();

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
    let input = base64::decode("dQrZY3FO6kO3Who6yFbDeyq41Vjg+BLIbAa1BhwRYwckhjVzwLQDcUzjrpMyrk9Kl8OsCtPSGlx19kHtqGgQBAEaA9sGDcFzTeYbDfLWwwnVY4cMoMpyh68qdKxP6hop1G/AgXxwOWsm9Obe8CRB7oW8EyuUO80oobOt5Fip5x0RQ6MzpbtlLPoYRGVcgMu+M1UJKzqIRLRcbrmoa4EtHrsWZBFLXdh9F9QLusfMAYyFC1IsQd4UGjAahNwZ1V0ZT7AElZfzC4P96Xkm3g92dcjWr+mCtcGn/5mp8M/rHB9TSkWzBpZIQ0qmglNSc3tv0N1AK+3VuusHOPuzX+mfF22tVWcIpevi2k9IeKz7FFN5yYxiqvZ52hNzKoc87x4gMaqrzMTFoJ6oLxdui3ve3vVifdBGJ266kv5CuXYDTiAvs8VdT/DMTuyF1a11Xj+aRFnsigD38tT1IfkAI/bpAHxWxqLJNhMaxEv+R0buMwIXuZiLhkFrgXL0L/vwUMEX").unwrap();

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
    let input = base64::decode("7NNEFgUx0Aw1NSdRbbA1Rguaw0yApBQLXb6jXBVTtBLEX+KX9ZFPcjVsmo9pYoFsap6GRi7RBLty3afPnYJEKfcRdIh/Sng5FgnuZGZkta1TIauqUqX/gM3HaQcMRSsRP5JLuklniqXhvy2fAa/xbTbRzIeuYsJTXn8en4ssKwPYfl/HpOS8+0bqJlKIdPUiHVMPIawdOs7MVY3eHHQXABoHa2Bh4f5a4QpF4Mc0WbHKLy/n3smLk0/g9TxtleQZ").unwrap();

    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build(get_context(vec![], false));

    logic.alt_bn128_g1_multiexp(input.len() as _, input.as_ptr() as _, 0).unwrap();
    let len = logic.register_len(0).unwrap();
    let mut res = vec![0; len as usize];
    logic.read_register(0, res.as_mut_ptr() as _).unwrap();
    assert_eq!(res, expected)
}

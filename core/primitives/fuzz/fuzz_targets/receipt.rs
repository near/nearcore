#![no_main]

use near_primitives::receipt::Receipt;
use borsh::{self, BorshDeserialize, BorshSerialize};

libfuzzer_sys::fuzz_target!(|receipt: Receipt| {
    // writer
    let mut serialized = vec![];
    let _ = receipt.serialize(
        &mut serialized,
    ).unwrap();
    let deserialized = Receipt::try_from_slice(&serialized).unwrap();

    assert_eq!(receipt, deserialized);
});


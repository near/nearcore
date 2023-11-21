#![no_main]

use near_primitives::receipt::{Receipt};

use arbitrary::Arbitrary;

libfuzzer_sys::fuzz_target!(|receipt: Receipt| {
    let serialized = receipt.try_to_vec().unwrap();
    let deserialized = Receipt::try_from_slice(&serialized).unwrap();

    assert_eq!(receipt, deserialized);
});


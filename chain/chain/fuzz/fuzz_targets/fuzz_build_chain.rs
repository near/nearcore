#![no_main]
use libfuzzer_sys::fuzz_target;

use chrono::TimeZone;
use std::str::FromStr;

use near_chain::{Block, Error,Chain, Provenance};
use near_chain::test_utils::{setup, build_chain_from_random_data};
use near_primitives::time::{Clock, MockClockGuard};


fuzz_target!(|data: &[u8]| {
    build_chain_from_random_data(data);
    /*
    let _mock_clock_guard = MockClockGuard::default();
    for i in 0..5 {
        Clock::add_utc(chrono::Utc.ymd(2020, 10, 1).and_hms_milli(0, 0, 3, 444 + i));
    }

    let (mut chain, _, signer) = setup();

    let prev_hash = *chain.head_header().unwrap().hash();
    #[cfg(feature = "nightly_protocol")]
    assert_eq!(
        prev_hash,
        CryptoHash::from_str("FrAMjjHhjJAB1N6BiDVpRfAMsjLHbTYZnkYKUs39JgEx").unwrap()
    );
    #[cfg(not(feature = "nightly_protocol"))]
    assert_eq!(
        prev_hash,
        CryptoHash::from_str("6C474NRQK1cLkGYrdaCpeVPKbBbsTxGeB7ZZWkdbD3uL").unwrap()
    );

    for i in 0..4 {
        let prev_hash = *chain.head_header().unwrap().hash();
        let prev = chain.get_block(&prev_hash).unwrap();
        let block = Block::empty(&prev, &*signer);
        let tip = process_block_test(&mut chain, &None, block).unwrap();
        assert_eq!(tip.unwrap().height, i + 1);
    }
    assert_eq!(chain.head().unwrap().height, 4);
    let count_instant = Clock::instant_call_count();
    let count_utc = Clock::utc_call_count();
    assert_eq!(count_utc, 5);
    assert_eq!(count_instant, 0);
    #[cfg(feature = "nightly_protocol")]
    assert_eq!(
        chain.head().unwrap().last_block_hash,
        CryptoHash::from_str("2zngdBZU9YEZKvtfpkZb3vjjxf7j7szMdo5jtPLDcg5Z").unwrap()
    );
    #[cfg(not(feature = "nightly_protocol"))]
    assert_eq!(
        chain.head().unwrap().last_block_hash,
        CryptoHash::from_str("5pXLqgQe98JSdhtQ66VQFjQzBRPdZaEiMaTpdGuziF4H").unwrap()
    );

     */
});

//! `validate_header` requires a block's spice-ness to agree with its epoch's protocol
//! version, in both directions.
//!
//! The direction covered here — a spice header in a pre-spice epoch — is the invariant
//! the spice actors' runtime activation gates rest on: they key on the block
//! (`BlockHeader::is_spice()`) rather than re-deriving spice-ness from the epoch, which
//! is only sound as long as a spice block cannot enter a pre-spice epoch. It is
//! reachable on the wire because the header version (`BlockHeaderV7`) and the body
//! version (`BlockBodyV3`) are independent, and `preprocess_block`'s own bidirectional
//! check looks only at the body. It needs a byzantine block producer, since the header
//! signature is checked first.

use crate::Error;
use crate::test_utils::get_chain_with_genesis;
use near_async::time::Clock;
use near_chain_configs::test_genesis::{TestGenesisBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature, ProtocolVersion};
use std::sync::Arc;

/// A protocol version that is supported by this binary but predates spice.
fn pre_spice_protocol_version() -> ProtocolVersion {
    ProtocolFeature::Spice.protocol_version() - 1
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn validate_header_rejects_a_spice_header_in_a_pre_spice_epoch() {
    init_test_logger();
    let clock = Clock::real();

    let genesis = TestGenesisBuilder::new()
        .genesis_time_from_clock(&clock)
        .protocol_version(pre_spice_protocol_version())
        .validators_spec(ValidatorsSpec::desired_roles(&["test-producer"], &[]))
        .build();
    let chain = get_chain_with_genesis(clock.clone(), genesis);

    let genesis_block = chain.get_block(&chain.genesis().hash().clone()).unwrap();
    let height = genesis_block.header().height() + 1;
    let block_producer = chain
        .epoch_manager
        .get_block_producer_info(genesis_block.header().epoch_id(), height)
        .unwrap();
    // The header signature is checked before the spice-ness check, so the block has to
    // be signed by the real producer to reach the check under test.
    let signer = Arc::new(create_test_signer(block_producer.account_id().as_str()));

    let block = TestBlockBuilder::from_prev_block(clock, &genesis_block, signer)
        .protocol_version(PROTOCOL_VERSION)
        .build();
    assert!(block.header().is_spice(), "test must offer a spice header");

    let result = chain.process_block_header(block.header());
    assert!(matches!(result, Err(Error::InvalidProtocolVersion)), "got {result:?}");
}

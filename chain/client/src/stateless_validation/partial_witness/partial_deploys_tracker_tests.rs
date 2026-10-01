use super::encoding::CONTRACT_DEPLOYS_RATIO_DATA_PARTS;
use super::partial_deploys_tracker::PartialEncodedContractDeploysTracker;
use near_primitives::hash::CryptoHash;
use near_primitives::reed_solomon::{ReedSolomonEncoder, ReedSolomonEncoderCache};
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::contract_distribution::{
    ChunkContractDeploys, CodeBytes, PartialEncodedContractDeploys,
    PartialEncodedContractDeploysPart,
};
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{EpochId, ShardId};
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::ProtocolFeature;
use std::sync::Arc;

const TOTAL_PARTS: usize = 4;

fn chunk_key() -> ChunkProductionKey {
    ChunkProductionKey {
        shard_id: ShardId::new(0),
        epoch_id: EpochId::default(),
        height_created: 42,
    }
}

fn encoder() -> Arc<ReedSolomonEncoder> {
    ReedSolomonEncoderCache::new(CONTRACT_DEPLOYS_RATIO_DATA_PARTS).entry(TOTAL_PARTS)
}

/// Encodes `contracts` into the messages a producer anchored at `anchor` would send.
fn messages_for(
    signer: &ValidatorSigner,
    anchor: CryptoHash,
    contracts: &[&[u8]],
) -> Vec<PartialEncodedContractDeploys> {
    let contracts = contracts.iter().map(|code| CodeBytes(code.to_vec().into())).collect();
    let deploys = ChunkContractDeploys::compress_contracts(&contracts).unwrap();
    let (parts, encoded_length) = encoder().encode(&deploys);
    parts
        .into_iter()
        .enumerate()
        .map(|(part_ord, data)| {
            let part =
                PartialEncodedContractDeploysPart { part_ord, data: data.unwrap(), encoded_length };
            PartialEncodedContractDeploys::new(
                chunk_key(),
                part,
                CryptoHash::hash_bytes(b"parent"),
                anchor,
                signer,
                // The tracker is version-agnostic; ask for the V2 shape explicitly so the test
                // runs on stable builds too, where `PROTOCOL_VERSION` predates the feature.
                ProtocolFeature::EarlyKickout.protocol_version(),
            )
        })
        .collect()
}

/// Two producers authorized by two different anchors assemble their deploys independently.
///
/// Keyed on the chunk key alone, the first message to arrive pinned the entry's `encoded_length`
/// and claimed part ord 0, so the second producer's parts were dropped as mismatched or as
/// already processed and its contracts never arrived. Both are validly signed under
/// `EarlyKickout`, so both have to be tracked.
#[test]
fn deploys_from_two_anchors_do_not_displace_each_other() {
    let signer = create_test_signer("test");
    let mut tracker = PartialEncodedContractDeploysTracker::new();

    // Different contracts, so the two payloads differ in encoded_length.
    let mine = messages_for(&signer, CryptoHash::hash_bytes(b"anchor_one"), &[b"contract_one"]);
    let theirs = messages_for(
        &signer,
        CryptoHash::hash_bytes(b"anchor_two"),
        &[b"a_rather_longer_contract_body", b"and_a_second_one"],
    );
    assert_ne!(
        mine[0].part().encoded_length,
        theirs[0].part().encoded_length,
        "the fixture needs two payloads of different length to pin the entry"
    );

    // The colliding producer's part lands first and claims the slot.
    assert!(!tracker.already_processed(&theirs[0]));
    assert!(
        tracker
            .store_partial_encoded_contract_deploys(theirs[0].clone(), encoder())
            .unwrap()
            .is_none()
    );

    // Our own parts are still wanted, and still assemble.
    assert!(!tracker.already_processed(&mine[0]), "a part for another anchor is not ours");
    let mut decoded = None;
    for message in &mine {
        assert!(!tracker.already_processed(message));
        decoded =
            tracker.store_partial_encoded_contract_deploys(message.clone(), encoder()).unwrap();
        if decoded.is_some() {
            break;
        }
    }
    let contracts = decoded.expect("our deploys should decode").decompress_contracts().unwrap();
    assert_eq!(contracts, vec![CodeBytes(b"contract_one".to_vec().into())]);

    // And the finished key is latched only for our anchor.
    assert!(tracker.already_processed(&mine[0]));
    assert!(!tracker.already_processed(&theirs[1]));
}

use super::*;
use crate::spice::data_manager::orphans::{
    MAX_PARKED_BYTES_PER_SENDER, OrphanPool, PARKED_BLOCK_MAX_AGE, PARKED_MESSAGE_CHARGE,
    ParkOutcome, ParkedParts,
};
use crate::spice::data_manager::sender::AuthenticatedSender;

/// A message from `sender` with one part of `part_bytes` bytes and no proof.
fn parked(sender: &AccountId, part_bytes: u64) -> ParkedParts {
    parked_with_proof(sender, part_bytes, 0)
}

/// A message from `sender` with one part of `part_bytes` bytes and a proof of `proof_items` items.
fn parked_with_proof(sender: &AccountId, part_bytes: u64, proof_items: u64) -> ParkedParts {
    let signer = create_test_signer(sender.as_str());
    let proof_item = MerklePathItem { hash: CryptoHash::default(), direction: Direction::Left };
    let parts = vec![SpiceDataPart {
        part_ord: 0,
        part: vec![0; part_bytes as usize].into_boxed_slice(),
        merkle_proof: vec![proof_item; proof_items as usize],
    }];
    let commitment = SpiceDataCommitment {
        hash: CryptoHash::default(),
        root: CryptoHash::default(),
        encoded_length: 0,
    };
    let data = signed_by(&signer, &item_id(), &commitment, parts);
    let (sender, verified) = AuthenticatedSender::verify(data, &[signer.public_key()]).unwrap();
    ParkedParts::new(sender, item_id(), verified.commitment, verified.parts)
}

fn block_hash(index: u64) -> CryptoHash {
    hash(&index.to_le_bytes())
}

/// A message from `sender` that takes its whole cap.
fn full_cap(sender: &AccountId) -> ParkedParts {
    parked(sender, MAX_PARKED_BYTES_PER_SENDER - PARKED_MESSAGE_CHARGE)
}

#[test]
fn a_sender_parks_at_most_its_cap_over_the_fixed_charge_messages() {
    let mut pool = OrphanPool::default();
    let sender = producer(0);
    let max_messages = MAX_PARKED_BYTES_PER_SENDER / (PARKED_MESSAGE_CHARGE + 1);
    for index in 0..max_messages {
        assert_eq!(pool.park(block_hash(index), parked(&sender, 1), 0), ParkOutcome::Parked);
    }

    let result = pool.park(block_hash(max_messages), parked(&sender, 1), 0);

    assert_eq!(result, ParkOutcome::SenderOverCap);
    assert_eq!(
        pool.park(block_hash(max_messages), parked(&producer(1), 1), 0),
        ParkOutcome::Parked
    );
}

#[test]
fn a_merkle_proof_counts_toward_the_senders_cap() {
    let mut pool = OrphanPool::default();
    let sender = producer(0);
    let proof_items =
        (MAX_PARKED_BYTES_PER_SENDER - PARKED_MESSAGE_CHARGE) / size_of::<MerklePathItem>() as u64;

    let result = pool.park(block_hash(0), parked_with_proof(&sender, 0, proof_items + 1), 0);
    assert_eq!(result, ParkOutcome::SenderOverCap);

    let result = pool.park(block_hash(0), parked_with_proof(&sender, 0, proof_items), 0);
    assert_eq!(result, ParkOutcome::Parked);
}

#[test]
fn a_sender_can_park_a_full_cap_again_once_its_block_is_taken() {
    let mut pool = OrphanPool::default();
    let sender = producer(0);
    assert_eq!(pool.park(block_hash(0), full_cap(&sender), 0), ParkOutcome::Parked);
    assert_eq!(pool.park(block_hash(1), parked(&sender, 1), 0), ParkOutcome::SenderOverCap);

    assert_eq!(pool.take(&block_hash(0)).len(), 1);

    assert_eq!(pool.park(block_hash(1), full_cap(&sender), 0), ParkOutcome::Parked);
}

#[test]
fn a_sender_can_park_a_full_cap_again_once_its_block_ages_out() {
    let mut pool = OrphanPool::default();
    let sender = producer(0);
    assert_eq!(pool.park(block_hash(0), full_cap(&sender), 0), ParkOutcome::Parked);
    assert_eq!(pool.park(block_hash(1), parked(&sender, 1), 0), ParkOutcome::SenderOverCap);

    pool.drop_aged(PARKED_BLOCK_MAX_AGE);

    assert_eq!(
        pool.park(block_hash(1), full_cap(&sender), PARKED_BLOCK_MAX_AGE),
        ParkOutcome::Parked
    );
    assert!(pool.take(&block_hash(0)).is_empty());
}

#[test]
fn one_sender_parking_for_many_blocks_does_not_displace_another_sender() {
    let mut pool = OrphanPool::default();
    let honest = producer(0);
    let flooder = producer(1);
    assert_eq!(pool.park(block_hash(0), parked(&honest, 1), 0), ParkOutcome::Parked);
    let max_messages = MAX_PARKED_BYTES_PER_SENDER / (PARKED_MESSAGE_CHARGE + 1);
    assert!(max_messages > 10, "the flood should outnumber the blocks the old pool kept");
    for index in 1..=max_messages {
        assert_eq!(pool.park(block_hash(index), parked(&flooder, 1), 0), ParkOutcome::Parked);
    }

    assert_eq!(pool.take(&block_hash(0)).len(), 1);
}

#[test]
fn a_parked_block_is_kept_one_height_before_the_age_limit_and_dropped_at_it() {
    let mut pool = OrphanPool::default();
    let parked_at = 10;
    for index in 0..2 {
        assert_eq!(
            pool.park(block_hash(index), parked(&producer(0), 1), parked_at),
            ParkOutcome::Parked
        );
    }

    pool.drop_aged(parked_at + PARKED_BLOCK_MAX_AGE - 1);
    assert_eq!(pool.take(&block_hash(0)).len(), 1);

    pool.drop_aged(parked_at + PARKED_BLOCK_MAX_AGE);
    assert!(pool.take(&block_hash(1)).is_empty());
}

/// The age counts from the block's first message: later messages do not extend it.
#[test]
fn a_parked_block_ages_from_its_first_message() {
    let mut pool = OrphanPool::default();
    assert_eq!(pool.park(block_hash(0), parked(&producer(0), 1), 0), ParkOutcome::Parked);
    let later = PARKED_BLOCK_MAX_AGE - 1;
    assert_eq!(pool.park(block_hash(0), parked(&producer(1), 1), later), ParkOutcome::Parked);

    pool.drop_aged(PARKED_BLOCK_MAX_AGE);

    assert!(pool.take(&block_hash(0)).is_empty());
}

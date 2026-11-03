use super::*;
use near_primitives::spice::partial_data::{SpiceDataIdentifier, SpicePartialData};

fn signed_by(signer: &str) -> SpicePartialData {
    let (commitment, parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
    SpicePartialData::new(
        SpiceDataIdentifier::from(&item_id()),
        commitment,
        parts,
        &create_test_signer(signer),
    )
}

#[test]
fn a_sender_carries_the_key_that_verified_its_signature() {
    let signing_key = create_test_signer("producer0.near").public_key();
    let other_key = create_test_signer("other.near").public_key();

    let (sender, verified) =
        AuthenticatedSender::verify(signed_by("producer0.near"), &[other_key, signing_key.clone()])
            .unwrap();

    assert_eq!(sender.account_id(), &account("producer0.near"));
    assert_eq!(sender.key(), &signing_key);
    assert_eq!(verified.sender, account("producer0.near"));
}

#[test]
fn a_signature_no_key_verifies_is_not_authenticated() {
    let keys = [create_test_signer("other.near").public_key()];

    assert!(AuthenticatedSender::verify(signed_by("producer0.near"), &keys).is_none());
}

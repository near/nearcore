use tracing::error;

/// Determines size of `PeerId` based on first byte of it's representation.
/// Size of `PeerId` depends on type of `PublicMessage it stores`.
/// PublicKey::ED25519 -> 1 + 32 bytes
/// PublicKey::SECP256K1 -> 1 + 64 bytes
fn peer_id_type_field_len(enum_var: u8) -> Option<usize> {
    // 1 byte for enum variant, then some number depending on the
    // public key type
    match enum_var {
        0 => Some(1 + 32),
        1 => Some(1 + 64),
        _ => None,
    }
}

/// Checks `bytes` represents `PeerMessage::Routed(RoutedMessage)`,
/// and `RoutedMessage.body` has type of `RoutedMessageBody::ForwardTx`.
///
/// This is done to avoid expensive borsch-deserializing.
pub(crate) fn is_forward_transaction(bytes: &[u8]) -> Option<bool> {
    // PeerMessage::Routed variant == 13
    let peer_message_variant = *bytes.get(0)?;
    if peer_message_variant != 13 {
        return Some(false);
    }

    // target: PeerIdOrHash
    let author_variant_idx = {
        let target_field_len = {
            let target_field_variant = *bytes.get(1)?;
            if target_field_variant == 0 {
                // PeerIdOrHash::PeerId
                let peer_id_variant = *bytes.get(2)?;
                peer_id_type_field_len(peer_id_variant)?
            } else if target_field_variant == 1 {
                // PeerIdOrHash::Hash is always 32 bytes
                32
            } else {
                error!("Unsupported variant of PeerIdOrHash {}", target_field_variant);
                return None;
            }
        };
        2 + target_field_len
    };

    // author: PeerId
    let signature_variant_idx = {
        let author_variant = *bytes.get(author_variant_idx)?;
        let author_field_len = peer_id_type_field_len(author_variant)?;

        author_variant_idx + author_field_len
    };

    // ttl: u8
    let ttl_idx = {
        let signature_variant = *bytes.get(signature_variant_idx)?;

        // pub signature: Signature
        let signature_field_len = match signature_variant {
            0 => 1 + 64, // Signature::ED25519
            1 => 1 + 65, // Signature::SECP256K1
            _ => {
                return None;
            }
        };
        signature_variant_idx + signature_field_len
    };

    // pub ttl: u8
    let message_body_idx = ttl_idx + 1;

    // check if type is `RoutedMessageBody::ForwardTx`
    let message_body_variant = *bytes.get(message_body_idx)?;
    Some(message_body_variant == 1)
}

#[cfg(test)]
mod tests {
    use crate::peer::utils::is_forward_transaction;
    use crate::types::PeerMessage;
    use borsh::BorshSerialize;
    use near_crypto::{KeyType, SecretKey};
    use near_network_primitives::types::{PeerIdOrHash, RoutedMessage, RoutedMessageBody};
    use near_primitives::hash::CryptoHash;
    use near_primitives::network::PeerId;
    use near_primitives::transaction::{SignedTransaction, Transaction};

    #[derive(Debug, Copy, Clone)]
    enum ForwardTxTargetType {
        Hash,
        PublicKey(KeyType),
    }

    #[derive(Debug, Copy, Clone)]
    struct ForwardTxType {
        target: ForwardTxTargetType,
        author: KeyType,
        tx: KeyType,
    }

    fn create_tx_forward(schema: ForwardTxType) -> PeerMessage {
        let target = match schema.target {
            ForwardTxTargetType::Hash => {
                PeerIdOrHash::Hash(CryptoHash::hash_bytes(b"peer_id_hash"))
            }
            ForwardTxTargetType::PublicKey(key_type) => {
                let secret_key = SecretKey::from_seed(key_type, "target_secret_key");
                PeerIdOrHash::PeerId(PeerId::new(secret_key.public_key()))
            }
        };

        let (author, signature) = {
            let secret_key = SecretKey::from_seed(schema.author, "author_secret_key");
            let public_key = secret_key.public_key();
            let author = PeerId::new(public_key);
            let msg_data = CryptoHash::hash_bytes(b"msg_data");
            let signature = secret_key.sign(msg_data.as_ref());

            (author, signature)
        };

        let tx = {
            let secret_key = SecretKey::from_seed(schema.tx, "tx_secret_key");
            let public_key = secret_key.public_key();
            let tx_hash = CryptoHash::hash_bytes(b"this_great_tx_data");
            let signature = secret_key.sign(tx_hash.as_ref());

            SignedTransaction::new(
                signature,
                Transaction::new(
                    "test_x".parse().unwrap(),
                    public_key,
                    "test_y".parse().unwrap(),
                    7,
                    tx_hash,
                ),
            )
        };

        PeerMessage::Routed(RoutedMessage {
            target,
            author,
            signature,
            ttl: 99,
            body: RoutedMessageBody::ForwardTx(tx),
        })
    }

    #[test]
    fn test_tx_forward() {
        let targets = [
            ForwardTxTargetType::PublicKey(KeyType::ED25519),
            ForwardTxTargetType::PublicKey(KeyType::SECP256K1),
            ForwardTxTargetType::Hash,
        ];
        let authors = [KeyType::ED25519, KeyType::SECP256K1];
        let txs_keys = [KeyType::ED25519, KeyType::SECP256K1];

        let schemas = targets
            .iter()
            .flat_map(|target| authors.iter().map(move |author| (*target, *author)))
            .flat_map(|(target, author)| {
                txs_keys.iter().map(move |tx| ForwardTxType { target, author, tx: *tx })
            });

        schemas.for_each(|s| {
            let msg = create_tx_forward(s);
            let bytes = msg.try_to_vec().unwrap();
            assert!(is_forward_transaction(&bytes).unwrap());
        })
    }
}

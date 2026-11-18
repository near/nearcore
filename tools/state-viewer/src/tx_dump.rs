use near_chain::{ChainStore, ChainStoreAccess};
use near_primitives::account::id::AccountId;
use near_primitives::block::Block;
use near_primitives::serialize::to_base64;
use near_primitives::transaction::TransactionEnvelope;

/// Returns a list of transactions found in the block, including ECCs.
pub fn dump_tx_from_block(
    chain_store: &ChainStore,
    block: &Block,
    select_account_ids: Option<&[AccountId]>,
) -> Vec<TransactionEnvelope> {
    let chunks = block.chunks();
    let mut res = vec![];
    for chunk_header in chunks.iter() {
        res.extend(
            chain_store
                .get_chunk(&chunk_header.chunk_hash())
                .unwrap()
                .to_transactions()
                .iter()
                .filter(|transaction| should_include_transaction(transaction, select_account_ids))
                .cloned(),
        );
    }
    res
}

/// A transaction as `dump_tx` writes it: its borsh encoding in base64, the same string a
/// `SignedTransaction` serializes to. An ECC starts with the escape byte `0xFF`, so its string
/// starts with `/w`.
pub fn encode_transaction(transaction: &TransactionEnvelope) -> String {
    to_base64(&borsh::to_vec(transaction).expect("failed to serialize a transaction"))
}

fn should_include_transaction(
    transaction: &TransactionEnvelope,
    select_account_ids: Option<&[AccountId]>,
) -> bool {
    match select_account_ids {
        None => true,
        Some(specified_ids) => specified_ids.contains(transaction.receiver_id()),
    }
}

#[cfg(test)]
mod tests {
    use super::{encode_transaction, should_include_transaction};
    use borsh::BorshDeserialize;
    use near_crypto::{InMemorySigner, KeyType};
    use near_primitives::hash::CryptoHash;
    use near_primitives::serialize::from_base64;
    use near_primitives::transaction::{EccTransaction, SignedTransaction, TransactionEnvelope};
    use near_primitives::types::{AccountId, Balance};
    use serde_json::Value;

    fn signed_transaction(signer_id: &AccountId, receiver_id: &AccountId) -> SignedTransaction {
        let signer = InMemorySigner::from_seed(signer_id.clone(), KeyType::ED25519, "seed");
        SignedTransaction::send_money(
            1,
            signer_id.clone(),
            receiver_id.clone(),
            &signer,
            Balance::from_yoctonear(1),
            CryptoHash::default(),
        )
    }

    fn signed(signer_id: &AccountId, receiver_id: &AccountId) -> TransactionEnvelope {
        TransactionEnvelope::Signed(signed_transaction(signer_id, receiver_id))
    }

    fn ecc(contract_id: &AccountId) -> TransactionEnvelope {
        TransactionEnvelope::Unsigned(EccTransaction::new(contract_id.clone(), vec![1, 2, 3]))
    }

    #[test]
    fn test_should_include_transaction() {
        let alice: AccountId = "alice.near".parse().unwrap();
        let bob: AccountId = "bob.near".parse().unwrap();
        let carol: AccountId = "carol.near".parse().unwrap();
        let select = [bob.clone()];

        assert!(should_include_transaction(&signed(&alice, &bob), None));
        assert!(should_include_transaction(&ecc(&carol), None));

        assert!(should_include_transaction(&signed(&alice, &bob), Some(&select)));
        assert!(!should_include_transaction(&signed(&alice, &carol), Some(&select)));
        // The selection is by receiver: a transaction signed by a selected account is not
        // included on that account alone.
        assert!(!should_include_transaction(&signed(&bob, &carol), Some(&select)));
        // The receiver of an ECC is its contract.
        assert!(should_include_transaction(&ecc(&bob), Some(&select)));
        assert!(!should_include_transaction(&ecc(&carol), Some(&select)));
    }

    #[test]
    fn test_encode_transaction() {
        let alice: AccountId = "alice.near".parse().unwrap();
        let bob: AccountId = "bob.near".parse().unwrap();

        // A signed transaction is written as it always was.
        let signed_transaction = signed_transaction(&alice, &bob);
        let encoded = encode_transaction(&TransactionEnvelope::Signed(signed_transaction.clone()));
        let serialized = serde_json::to_value(&signed_transaction).unwrap();
        assert_eq!(serialized, Value::String(encoded));

        // An ECC starts with the escape byte and decodes back to itself.
        let ecc = ecc(&bob);
        let encoded = encode_transaction(&ecc);
        assert!(encoded.starts_with("/w"));
        let bytes = from_base64(&encoded).unwrap();
        assert_eq!(bytes[0], 0xFF);
        assert_eq!(TransactionEnvelope::try_from_slice(&bytes).unwrap(), ecc);
    }
}

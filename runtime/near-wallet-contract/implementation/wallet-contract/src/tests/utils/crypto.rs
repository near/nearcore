use aurora_engine_transactions::{eip_2930::Transaction2930, EthTransactionKind};
use aurora_engine_types::U256;
use near_crypto::{SecretKey, Signature};

pub fn sign_transaction(transaction: Transaction2930, sk: &SecretKey) -> EthTransactionKind {
    let mut rlp_stream = rlp::RlpStream::new();
    rlp_stream.append(&aurora_engine_transactions::eip_2930::TYPE_BYTE);
    transaction.rlp_append_unsigned(&mut rlp_stream);
    let message_hash = crate::internal::keccak256(rlp_stream.as_raw());
    let signature = sk.sign(&message_hash);
    let bytes: [u8; 65] = match signature {
        Signature::SECP256K1(x) => x.into(),
        _ => panic!(),
    };
    let v = bytes[64];
    let r = U256::from_big_endian(&bytes[0..32]);
    let s = U256::from_big_endian(&bytes[32..64]);
    let signed_transaction = aurora_engine_transactions::eip_2930::SignedTransaction2930 {
        transaction,
        parity: v,
        r,
        s,
    };
    EthTransactionKind::Eip2930(signed_transaction)
}

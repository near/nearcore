use itertools::Itertools;

use near_async::messaging::CanSend;
use near_chain_primitives::Error;
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_primitives::sharding::ReedSolomonWrapper;
use near_primitives::stateless_validation::{ChunkStateWitness, PartialEncodedStateWitness};
use near_primitives::types::AccountId;

pub fn send_chunk_witness_to_chunk_validators(
    chunk_witness: &ChunkStateWitness,
    chunk_validators: Vec<AccountId>,
    peer_manager_adapter: &PeerManagerAdapter,
) -> Result<(), Error> {
    let (parts, state_witness_size) = encode_state_witness(chunk_witness, &chunk_validators)?;
    let partial_encoded_state_witnesses =
        parts_to_partial_encoded_state_witness(&chunk_validators, parts, state_witness_size);
    peer_manager_adapter.send(PeerManagerMessageRequest::NetworkRequests(
        NetworkRequests::PartialEncodedStateWitness(partial_encoded_state_witnesses),
    ));
    Ok(())
}

fn encode_state_witness(
    chunk_witness: &ChunkStateWitness,
    chunk_validators: &Vec<AccountId>,
) -> Result<(Vec<Option<Box<[u8]>>>, usize), Error> {
    assert!(chunk_validators.len() >= 3);
    let total_parts = chunk_validators.len();
    let data_parts = total_parts * 2 / 3;
    let rs = ReedSolomonWrapper::new(data_parts, total_parts - data_parts);

    let mut bytes = borsh::to_vec(chunk_witness).unwrap();
    let encoded_length = bytes.len();
    if bytes.len() % data_parts != 0 {
        bytes.extend((bytes.len() % data_parts..data_parts).map(|_| 0));
    }
    let shard_length = (encoded_length + data_parts - 1) / data_parts;
    assert_eq!(bytes.len(), shard_length * data_parts);

    let mut parts = Vec::with_capacity(rs.total_shard_count());
    for i in 0..data_parts {
        parts.push(Some(
            bytes[i * shard_length..(i + 1) * shard_length].to_vec().into_boxed_slice()
                as Box<[u8]>,
        ));
    }
    for _ in data_parts..total_parts {
        parts.push(None);
    }

    Ok((parts, encoded_length))
}

fn parts_to_partial_encoded_state_witness(
    chunk_validators: &Vec<AccountId>,
    parts: Vec<Option<Box<[u8]>>>,
    witness_size: usize,
) -> Vec<PartialEncodedStateWitness> {
    let mut partial_encoded_state_witnesses = Vec::with_capacity(chunk_validators.len());
    for (part_ord, (account_id, part)) in chunk_validators.iter().zip_eq(parts).enumerate() {
        let partial_encoded_state_witness = PartialEncodedStateWitness {
            part_ord,
            part: part.unwrap(),
            part_owner: account_id.clone(),
            forward_accounts: chunk_validators.clone(),
            witness_size,
        };
        partial_encoded_state_witnesses.push(partial_encoded_state_witness);
    }
    partial_encoded_state_witnesses
}

#[cfg(test)]
mod test {
    use near_primitives::sharding::ReedSolomonWrapper;

    #[test]
    fn test_rs() {
        let data_shards = 5;
        let parity_shards = 2;
        let mut rs = ReedSolomonWrapper::new(data_shards, parity_shards);
        let mut data = vec![
            Some(b"hello world 1".to_vec()),
            Some(b"hello world 2".to_vec()),
            Some(b"hello world 3".to_vec()),
            Some(b"hello world 4".to_vec()),
            Some(b"hello world 5".to_vec()),
            None,
            None,
        ];
        rs.reconstruct(data.as_mut_slice()).unwrap();
        assert!(data[0].is_some());

        data[0] = None;
        data[3] = None;
        // data[4] = None;

        rs.reconstruct(data.as_mut_slice()).unwrap();
        // Cool stuff!!
        assert!(data[0].is_some());
    }
}

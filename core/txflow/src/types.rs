use primitives::types::{SignedTransaction, SignedEpochBlockHeader};

/// Endorsement of a representative message. Includes the epoch of the message that it endorses as
/// well as the BLS signature part. The leader should also include such self-endorsement upon
/// creation of the representative message.
#[derive(Hash, Debug)]
pub struct Endorsement {
    pub epoch: u64,
    pub signature: u128
}

#[derive(Hash, Debug)]
pub struct InShardPayload {
    pub transactions: Vec<SignedTransaction>,
    pub epoch_block_header: Option<SignedEpochBlockHeader>,
}


#[derive(Hash, Debug)]
pub struct BeaconChainPayload {

}

#[derive(Hash, Debug)]
/// Not signed data representing TxFlow message.
pub struct MessageDataBody<T> {
    pub owner_uid: u64,
    pub parents: Vec<u64>,
    pub epoch: u64,
    pub payload: T,
    /// Optional endorsement of this or other representative block.
    pub endorsements: Vec<Endorsement>,
}

#[derive(Debug)]
pub struct SignedMessageData<T> {
    /// Signature of the hash.
    pub owner_sig: u128,
    /// Hash of the body.
    pub hash: u64,
    pub body: MessageDataBody<T>,
}

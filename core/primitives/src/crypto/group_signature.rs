use crate::crypto::aggregate_signature::{
    BlsAggregatePublicKey, BlsAggregateSignature, BlsPublicKey, BlsSignature,
};
use crate::crypto::signature::bs58_serializer;
use crate::logging::pretty_hash;
use crate::traits::{Base58Encoded, ToBytes};
use crate::types::{AuthorityMask, PartialSignature};
use core::fmt;
use std::convert::TryFrom;
use near_protos::types as types_proto;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GroupSignature {
    #[serde(with = "bs58_serializer")]
    pub signature: BlsSignature,
    pub authority_mask: AuthorityMask,
}

impl TryFrom<types_proto::GroupSignature> for GroupSignature {
    type Error = String;

    fn try_from(proto: types_proto::GroupSignature) -> Result<Self, String> {
        Base58Encoded::from_base58(&proto.signature)
            .map(|signature| GroupSignature { signature, authority_mask: proto.authority_mask })
            .map_err(|e| format!("cannot decode signature {:?}", e))
    }
}

impl From<GroupSignature> for types_proto::GroupSignature {
    fn from(signature: GroupSignature) -> Self {
        types_proto::GroupSignature {
            signature: Base58Encoded::to_base58(&signature.signature),
            authority_mask: signature.authority_mask,
            ..Default::default()
        }
    }
}

impl fmt::Debug for GroupSignature {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{:?} {:?}",
            self.authority_mask,
            pretty_hash(&bs58::encode(&self.signature.to_bytes()).into_string())
        )
    }
}

impl GroupSignature {
    // TODO (optimization): It's better to keep the signature in projective coordinates while
    // building it, then switch to affine coordinates at the end.  For the time being we just keep
    // it in affine coordinates always.
    pub fn add_signature(&mut self, signature: &PartialSignature, authority_id: usize) {
        if authority_id >= self.authority_mask.len() {
            self.authority_mask.resize(authority_id + 1, false);
        }
        if self.authority_mask[authority_id] {
            return;
        }
        let mut new_sig = BlsAggregateSignature::new();
        new_sig.aggregate(&signature);
        if self.signature != BlsSignature::default() {
            new_sig.aggregate(&self.signature);
        }
        self.signature = new_sig.get_signature();
        self.authority_mask[authority_id] = true;
    }

    pub fn authority_count(&self) -> usize {
        self.authority_mask.iter().filter(|&x| *x).count()
    }

    pub fn verify(&self, keys: &Vec<BlsPublicKey>, message: &[u8]) -> bool {
        if keys.len() < self.authority_mask.len() {
            return false;
        }
        // Empty signature + empty public key would pass verification
        if self.authority_count() == 0 {
            return false;
        }
        let mut group_key = BlsAggregatePublicKey::new();
        for (index, key) in keys.iter().enumerate() {
            if let Some(true) = self.authority_mask.get(index) {
                group_key.aggregate(&key);
            }
        }
        group_key.get_key().verify(message, &self.signature)
    }
}

impl Default for GroupSignature {
    fn default() -> Self {
        GroupSignature {
            signature: BlsSignature::empty(),
            authority_mask: AuthorityMask::default(),
        }
    }
}

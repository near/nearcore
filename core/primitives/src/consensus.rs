use std::fmt::Debug;
use std::hash::Hash;

pub use crate::serialize::{Decode, Encode};
use crate::hash::CryptoHash;
use crate::crypto::signature::bs58_serializer;
use crate::types::{AuthorityId, PartialSignature};

pub type GenericResult = Result<(), &'static str>;


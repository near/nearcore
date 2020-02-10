#[macro_use]
extern crate arrayref;

pub use key_file::KeyFile;
pub use signature::{KeyType, PublicKey, SecretKey, Signature};
pub use signer::{EmptySigner, InMemorySigner, Signer};

#[macro_use]
mod traits;

mod key_file;
mod signature;
mod signer;
mod test_utils;
pub mod vrf;

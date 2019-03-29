use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use serde_json;

use primitives::signer::{BlockSigner, InMemorySigner, TransactionSigner};
use primitives::types::{AccountId, Balance, ReadableBlsPublicKey, ReadablePublicKey};
use std::io::Write;

#[derive(Clone, Serialize, Deserialize)]
pub enum AuthorityRotation {
    /// Authorities stay the same, just rotate circularly to change order.
    ProofOfAuthority,
    /// Use Thresholded Proof of Stake to rotate authorities.
    ThresholdedProofOfStake { epoch_length: u64, num_seats_per_slot: u64 },
}

/// Specification of the blockchain in general.
#[derive(Clone, Serialize, Deserialize)]
pub struct ChainSpec {
    /// Genesis state accounts: (AccountId, PK, Initial Balance, Initial TX Stake)
    pub accounts: Vec<(AccountId, ReadablePublicKey, Balance, Balance)>,

    /// Genesis smart contract code.
    pub genesis_wasm: Vec<u8>,

    /// Genesis state authorities that bootstrap the chain.
    pub initial_authorities: Vec<(AccountId, ReadablePublicKey, ReadableBlsPublicKey, Balance)>,

    /// Define authority rotation strategy.
    pub authority_rotation: AuthorityRotation,
}
impl ChainSpec {
    /// Serializes ChainSpec to a string.
    pub fn to_string(&self) -> String {
        serde_json::to_string(self).expect("Error serializing the chain spec.")
    }

    /// Deserializes ChainSpec from a string.
    pub fn from_str(config: &str) -> Self {
        serde_json::from_str(config).expect("Error deserializing the chain spec.")
    }

    /// Reads ChainSpec from a file.
    pub fn from_file(path: &PathBuf) -> Self {
        let mut file = File::open(path).expect("Could not open chain spec file.");
        let mut contents = String::new();
        file.read_to_string(&mut contents).expect("Could not read from chain spec file.");
        ChainSpec::from_str(&contents)
    }

    /// Read ChainSpec from a file or use the default value.
    pub fn from_file_or_default(path: &Option<PathBuf>) -> Self {
        path.as_ref().map(|p| Self::from_file(p)).unwrap_or_default()
    }

    /// Writes ChainSpec to the file.
    pub fn write_to_file(&self, path: &PathBuf) {
        let mut file = File::create(path).expect("Failed to create/write a chain spec file");
        if let Err(err) = file.write_all(self.to_string().as_bytes()) {
            panic!("Failed to write a chain spec file {}", err)
        }
    }

    /// Default ChainSpec used by PoA.
    pub fn default_poa() -> Self {
        let genesis_wasm =
            include_bytes!("../../../core/wasm/runtest/res/wasm_with_mem.wasm").to_vec();
        let alice_id = "alice.near";
        let bob_id = "bob.near";
        let john_id = "john.near";
        let alice_signer = InMemorySigner::from_seed(alice_id, alice_id);
        let bob_signer = InMemorySigner::from_seed(bob_id, bob_id);
        let john_signer = InMemorySigner::from_seed(john_id, john_id);
        ChainSpec {
            accounts: vec![
                (alice_id.to_string(), alice_signer.public_key().to_readable(), 10000000, 1000),
                (bob_id.to_string(), bob_signer.public_key().to_readable(), 100, 10),
                (john_id.to_string(), john_signer.public_key().to_readable(), 10, 10),
            ],
            initial_authorities: vec![
                (
                    alice_id.to_string(),
                    alice_signer.public_key().to_readable(),
                    alice_signer.bls_public_key().to_readable(),
                    100,
                ),
                (
                    bob_id.to_string(),
                    bob_signer.public_key().to_readable(),
                    bob_signer.bls_public_key().to_readable(),
                    100,
                ),
            ],
            genesis_wasm,
            authority_rotation: AuthorityRotation::ProofOfAuthority,
        }
    }
}

impl Default for ChainSpec {
    fn default() -> Self {
        let genesis_wasm =
            include_bytes!("../../../core/wasm/runtest/res/wasm_with_mem.wasm").to_vec();
        let alice_id = "alice.near";
        let bob_id = "bob.near";
        let john_id = "john.near";
        let alice_signer = InMemorySigner::from_seed(alice_id, alice_id);
        let bob_signer = InMemorySigner::from_seed(bob_id, bob_id);
        let john_signer = InMemorySigner::from_seed(john_id, john_id);
        ChainSpec {
            accounts: vec![
                (alice_id.to_string(), alice_signer.public_key().to_readable(), 10000000, 1000),
                (bob_id.to_string(), bob_signer.public_key().to_readable(), 100, 10),
                (john_id.to_string(), john_signer.public_key().to_readable(), 10, 10),
            ],
            initial_authorities: vec![(
                alice_id.to_string(),
                alice_signer.public_key().to_readable(),
                alice_signer.bls_public_key().to_readable(),
                100,
            )],
            genesis_wasm,
            authority_rotation: AuthorityRotation::ProofOfAuthority,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ChainSpec;
    use primitives::types::ReadableBlsPublicKey;
    use primitives::types::ReadablePublicKey;

    #[test]
    fn test_deserialize() {
        let data = json!({
            "accounts": [["alice.near", "6fgp5mkRgsTWfd5UWw1VwHbNLLDYeLxrxw3jrkCeXNWq", 100, 10]],
            "initial_authorities": [("alice.near", "6fgp5mkRgsTWfd5UWw1VwHbNLLDYeLxrxw3jrkCeXNWq", "7AnjkhbpbtqbZHwg4gTZJd4ZGc84EN3FUj5diEbipGinQfYA2MDfaoe5uo1qRhCnkD", 50)],
            "genesis_wasm": [0,1],
            "authority_rotation": {"ThresholdedProofOfStake": {"epoch_length": 10, "num_seats_per_slot": 100}},
        });
        let spec = ChainSpec::from_str(&data.to_string());
        assert_eq!(
            spec.initial_authorities[0],
            (
                "alice.near".to_string(),
                ReadablePublicKey("6fgp5mkRgsTWfd5UWw1VwHbNLLDYeLxrxw3jrkCeXNWq".to_string()),
                ReadableBlsPublicKey(
                    "7AnjkhbpbtqbZHwg4gTZJd4ZGc84EN3FUj5diEbipGinQfYA2MDfaoe5uo1qRhCnkD"
                        .to_string()
                ),
                50
            )
        );
    }

    #[test]
    fn test_default_spec() {
        let spec = ChainSpec::default();
        let spec_str1 = spec.to_string();
        let spec_str2 = ChainSpec::from_str(spec_str1.as_str()).to_string();
        assert_eq!(spec_str1, spec_str2);
    }
}

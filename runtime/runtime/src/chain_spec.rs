use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use serde_json;

use primitives::crypto::signer::{BLSSigner, EDSigner, InMemorySigner};
use primitives::types::{AccountId, Balance, ReadableBlsPublicKey, ReadablePublicKey};
use std::cmp::max;
use std::io::Write;
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum AuthorityRotation {
    /// Authorities stay the same, just rotate circularly to change order.
    ProofOfAuthority,
    /// Use Thresholded Proof of Stake to rotate authorities.
    ThresholdedProofOfStake { epoch_length: u64, num_seats_per_slot: u64 },
}

/// Specification of the blockchain in general.
#[derive(Clone, Serialize, Deserialize, Debug)]
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

/// Initial balance used in tests.
pub const TESTING_INIT_BALANCE: Balance = 1_000_000_000_000;
/// Initial transactions stake used in tests.
pub const TESTING_INIT_TX_STAKE: Balance = 1_000;
/// Stake used by authorities to validate used in tests.
pub const TESTING_INIT_STAKE: Balance = 100;

impl ChainSpec {
    /// Serializes ChainSpec to a string.
    pub fn to_string(&self) -> String {
        serde_json::to_string(self).expect("Error serializing the chain spec.")
    }

    /// Reads ChainSpec from a file.
    pub fn from_file(path: &PathBuf) -> Self {
        let mut file = File::open(path).expect("Could not open chain spec file.");
        let mut contents = String::new();
        file.read_to_string(&mut contents).expect("Could not read from chain spec file.");
        ChainSpec::from(contents.as_str())
    }

    /// Read ChainSpec from a file or use the default value.
    pub fn from_file_or_default(path: &Option<PathBuf>, default: Self) -> Self {
        path.as_ref().map(|p| Self::from_file(p)).unwrap_or(default)
    }

    /// Writes ChainSpec to the file.
    pub fn write_to_file(&self, path: &PathBuf) {
        let mut file = File::create(path).expect("Failed to create/write a chain spec file");
        if let Err(err) = file.write_all(self.to_string().as_bytes()) {
            panic!("Failed to write a chain spec file {}", err)
        }
    }

    /// Generates a `ChainSpec` that can be used for testing. The signers are seeded from the account
    /// names and therefore not secure to use in production.
    /// Args:
    /// * `id_type`: What is the style of the generated account ids, e.g. `alice.near` or `near.0`;
    /// * `num_accounts`: how many initial accounts should be created;
    /// * `num_initial_authorities`: how many initial authorities should be created;
    /// * `authority_rotation`: type of the authority rotation.
    /// Returns:
    /// * generated `ChainSpec`;
    /// * signers that can be used for assertions and mocking in tests.
    #[allow(clippy::needless_range_loop)]
    pub fn testing_spec(
        id_type: DefaultIdType,
        num_accounts: usize,
        num_initial_authorities: usize,
        authority_rotation: AuthorityRotation,
    ) -> (Self, Vec<Arc<InMemorySigner>>) {
        let num_signers = max(num_accounts, num_initial_authorities);

        let mut signers = vec![];
        let mut accounts = vec![];
        let mut initial_authorities = vec![];
        for i in 0..num_signers {
            let account_id = match id_type {
                DefaultIdType::Named => NAMED_IDS[i].to_string(),
                DefaultIdType::Enumerated => format!("near.{}", i),
            };
            let signer =
                Arc::new(InMemorySigner::from_seed(account_id.as_str(), account_id.as_str()));
            if i < num_accounts {
                accounts.push((
                    account_id.clone(),
                    signer.public_key().to_readable(),
                    TESTING_INIT_BALANCE,
                    TESTING_INIT_TX_STAKE,
                ));
            }
            if i < num_initial_authorities {
                initial_authorities.push((
                    account_id.clone(),
                    signer.public_key().to_readable(),
                    signer.bls_public_key().to_readable(),
                    TESTING_INIT_STAKE,
                ));
            }
            signers.push(signer);
        }
        {
            let account_id = "alice.near".to_owned();
            let signer = InMemorySigner::from_seed(account_id.as_str(), account_id.as_str());
            // Add alice.near.
            accounts.push((
                account_id.clone(),
                signer.public_key().to_readable(),
                TESTING_INIT_BALANCE,
                TESTING_INIT_TX_STAKE,
            ));
        }

        let spec = ChainSpec {
            accounts,
            initial_authorities,
            genesis_wasm: include_bytes!("../../../runtime/wasm/runtest/res/wasm_with_mem.wasm")
                .to_vec(),
            authority_rotation,
        };
        (spec, signers)
    }

    /// Default ChainSpec used by PoA for testing.
    pub fn default_poa() -> Self {
        Self::testing_spec(DefaultIdType::Named, 3, 2, AuthorityRotation::ProofOfAuthority).0
    }

    /// Default ChainSpec used by DevNet for testing.
    pub fn default_devnet() -> Self {
        Self::testing_spec(DefaultIdType::Named, 18, 1, AuthorityRotation::ProofOfAuthority).0
    }
}

// Some of the standard named identifiers that we use for testing.
pub const ALICE_ID: &str = "alice.near";
pub const BOB_ID: &str = "bob.near";
pub const CAROL_ID: &str = "carol.near";
pub const NAMED_IDS: [&str; 18] = [
    ALICE_ID,
    BOB_ID,
    CAROL_ID,
    "dan.near",
    "eve.near",
    "frank.near",
    "grace.near",
    "heidi.near",
    "ivan.near",
    "judy.near",
    "mike.near",
    "niaj.near",
    "olivia.near",
    "pat.near",
    "sybil.near",
    "trudy.near",
    "victor.near",
    "wendy.near",
];

/// Type of id to use for the default ChainSpec. "alice.near" is a named id, "near.0" is an
/// enumerated id.
pub enum DefaultIdType {
    Named,
    Enumerated,
}

impl From<&str> for ChainSpec {
    fn from(config: &str) -> Self {
        serde_json::from_str(config).expect("Error deserializing the chain spec.")
    }
}

#[cfg(test)]
mod tests {
    use super::ChainSpec;
    use primitives::types::ReadableBlsPublicKey;
    use primitives::types::ReadablePublicKey;
    use serde_json::json;

    #[test]
    fn test_deserialize() {
        let data = json!({
            "accounts": [["alice.near", "6fgp5mkRgsTWfd5UWw1VwHbNLLDYeLxrxw3jrkCeXNWq", 100, 10]],
            "initial_authorities": [("alice.near", "6fgp5mkRgsTWfd5UWw1VwHbNLLDYeLxrxw3jrkCeXNWq", "7AnjkhbpbtqbZHwg4gTZJd4ZGc84EN3FUj5diEbipGinQfYA2MDfaoe5uo1qRhCnkD", 50)],
            "genesis_wasm": [0,1],
            "authority_rotation": {"ThresholdedProofOfStake": {"epoch_length": 10, "num_seats_per_slot": 100}},
        });
        let spec = ChainSpec::from(data.to_string().as_str());
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
        let spec = ChainSpec::default_devnet();
        let spec_str1 = spec.to_string();
        let spec_str2 = ChainSpec::from(spec_str1.as_str()).to_string();
        assert_eq!(spec_str1, spec_str2);
    }
}

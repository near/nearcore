//! Constructs state of token holders from the csv file.
use chrono::DateTime;
use chrono::Utc;
use csv::ReaderBuilder;
use near_crypto::{KeyType, PublicKey};
use near_network::types::PeerInfo;
use near_primitives::account::{AccessKey, AccessKeyPermission, Account, FunctionCallPermission};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::receipt::{ActionReceipt, Receipt, ReceiptEnum};
use near_primitives::state_record::StateRecord;
use near_primitives::transaction::{Action, FunctionCallAction};
use near_primitives::types::{AccountId, AccountInfo, Balance, Gas};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

/// Methods that can be called by a non-privileged access key.
const REGULAR_METHOD_NAMES: &[&str] = &["stake", "transfer"];
/// Methods that can be called by a privileged access key.
const PRIVILEGED_METHOD_NAMES: &[&str] = &["add_access_key", "remove_access_key"];
/// Methods that can be called by an access key owned by a "foundation".
const FOUNDATION_METHOD_NAMES: &[&str] =
    &["add_access_key", "remove_access_key", "permanently_unstake", "terminate", "init"];
/// Amount of gas that we pass to run the contract initialization function.
const INIT_GAS: Gas = 1_000_000;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

impl Row {
    pub fn verify(&self) -> Result<()> {
        if self.validator_stake > 0 && self.validator_key.is_none() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Validator key must be specified if validator stake is not 0.",
            )));
        }
        if self.validator_stake == 0 && self.validator_key.is_some() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Validator stake should be greater than 0 if validator stake is specified.",
            )));
        }

        match (self.vesting_start, self.vesting_end) {
            (None, None) => {
                if self.vesting_cliff.is_some() {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Vesting cliff cannot be set without vesting start and vesting end dates.",
                    )));
                }
            }
            (Some(ref vesting_start), Some(ref vesting_end)) => {
                if vesting_end <= vesting_start {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Vesting end date should be greater than vesting start date.",
                    )));
                }
                if let Some(ref vesting_cliff) = self.vesting_cliff {
                    if vesting_cliff <= vesting_start {
                        return Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "Vesting cliff date should be greater than vesting start date.",
                        )));
                    }
                    if vesting_cliff >= vesting_end {
                        return Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "Vesting cliff date should be less than vesting end date.",
                        )));
                    }
                }
                if let Some(ref lockup) = self.lockup {
                    if lockup <= vesting_start {
                        return Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "Lockup date should be greater than vesting start date.",
                        )));
                    }
                }
            }
            _ => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Vesting start and vesting end should be either both set or neither is set.",
                )));
            }
        }
        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct Row {
    genesis_time: Option<DateTime<Utc>>,
    account_id: AccountId,
    #[serde(with = "crate::serde_with::pks_as_str")]
    regular_pks: Vec<PublicKey>,
    #[serde(with = "crate::serde_with::pks_as_str")]
    privileged_pks: Vec<PublicKey>,
    #[serde(with = "crate::serde_with::pks_as_str")]
    foundation_pks: Vec<PublicKey>,
    #[serde(with = "crate::serde_with::pks_as_str")]
    full_pks: Vec<PublicKey>,
    amount: Balance,
    is_treasury: bool,
    validator_stake: Balance,
    validator_key: Option<PublicKey>,
    #[serde(with = "crate::serde_with::peer_info_to_str")]
    peer_info: Option<PeerInfo>,
    smart_contract: Option<String>,
    lockup: Option<DateTime<Utc>>,
    vesting_start: Option<DateTime<Utc>>,
    vesting_end: Option<DateTime<Utc>>,
    vesting_cliff: Option<DateTime<Utc>>,
}

/// Given path to the csv file produces:
/// * `StateRecord`s that represent the state of the token holders;
/// * `AccountInfo`s that represent validators;
/// * `PeerInfo`s that represent boot nodes;
/// * `AccountId` of the treasury.
/// *  Genesis time
pub fn keys_to_state_records<R>(
    reader: R,
    gas_price: Balance,
) -> Result<(Vec<StateRecord>, Vec<AccountInfo>, Vec<PeerInfo>, AccountId, DateTime<Utc>)>
where
    R: std::io::Read,
{
    let mut reader = ReaderBuilder::new().has_headers(true).from_reader(reader);

    let mut state_records = vec![];
    let mut initial_validators = vec![];
    let mut boot_nodes = vec![];
    let mut treasury = None;
    let mut genesis_time = None;
    for row in reader.deserialize() {
        let row: Row = row?;
        row.verify()?;
        if row.is_treasury {
            if treasury.is_none() {
                treasury = Some(row.account_id.clone());
            } else {
                panic!("Only one account can be marked as treasury.");
            }
        }
        if row.genesis_time.is_some() {
            genesis_time = row.genesis_time;
        }

        state_records.extend(account_records(&row, gas_price));
        if let Some(ref validator_key) = row.validator_key {
            initial_validators.push(AccountInfo {
                account_id: row.account_id.clone(),
                public_key: validator_key.clone(),
                amount: row.validator_stake,
            });
        }

        if let Some(peer_info) = row.peer_info {
            boot_nodes.push(peer_info);
        }
    }
    let treasury = treasury.expect("At least one account should be marked as treasury");
    let genesis_time = genesis_time.expect("Genesis time must be set");
    Ok((state_records, initial_validators, boot_nodes, treasury, genesis_time))
}

/// Returns the records representing state of an individual token holder.
fn account_records(row: &Row, gas_price: Balance) -> Vec<StateRecord> {
    let smart_contract_hash;
    let smart_contract_code;
    if let Some(ref smart_contract) = row.smart_contract {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push(smart_contract);
        let mut f = File::open(path).expect("Failed to open smart contract file.");
        let mut code = vec![];
        f.read_to_end(&mut code).expect("Failed reading smart contract file.");
        smart_contract_hash = hash(&code);
        smart_contract_code = Some(code);
    } else {
        smart_contract_hash = CryptoHash::default();
        smart_contract_code = None;
    }

    let mut res = vec![StateRecord::Account {
        account_id: row.account_id.clone(),
        account: Account::new(row.amount, row.validator_stake, smart_contract_hash, 0),
    }];

    // Add restricted access keys.
    for (pks, method_names) in vec![
        (row.regular_pks.clone(), REGULAR_METHOD_NAMES),
        (row.privileged_pks.clone(), PRIVILEGED_METHOD_NAMES),
        (row.foundation_pks.clone(), FOUNDATION_METHOD_NAMES),
    ] {
        for pk in pks {
            res.push(StateRecord::AccessKey {
                account_id: row.account_id.clone(),
                public_key: pk,
                access_key: AccessKey {
                    nonce: 0,
                    permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                        allowance: None,
                        receiver_id: row.account_id.to_string(),
                        method_names: method_names.iter().map(|x| (*x).to_string()).collect(),
                    }),
                },
            })
        }
    }

    // Add full access keys.
    for pk in row.full_pks.clone() {
        res.push(StateRecord::AccessKey {
            account_id: row.account_id.clone(),
            public_key: pk,
            access_key: AccessKey::full_access(),
        })
    }

    // Add smart contract code if was specified.
    if let Some(code) = smart_contract_code {
        res.push(StateRecord::Contract { account_id: row.account_id.clone(), code });
    }

    // Add init function call if smart contract was provided.
    if let Some(ref smart_contract) = row.smart_contract {
        let args = match smart_contract.as_str() {
            "lockup.wasm" => {
                let lockup = row.lockup.unwrap().timestamp_nanos();
                lockup.to_le_bytes().to_vec()
            }
            "lockup_and_vesting.wasm" => {
                let mut res = vec![];
                // Encode four dates as timestamps as i64 with LE encoding.
                for date in &[&row.lockup, &row.vesting_start, &row.vesting_end, &row.vesting_cliff]
                {
                    let date = date.unwrap().timestamp_nanos();
                    res.extend_from_slice(&date.to_le_bytes());
                }
                res
            }
            _ => unimplemented!(),
        };
        let receipt = Receipt {
            predecessor_id: row.account_id.clone(),
            receiver_id: row.account_id.clone(),
            // `receipt_id` can be anything as long as it is unique.
            receipt_id: hash(row.account_id.as_ref().as_bytes()),
            receipt: ReceiptEnum::Action(ActionReceipt {
                signer_id: row.account_id.clone(),
                // `signer_public_key` can be anything because the key checks are not applied when
                // a transaction is already converted to a receipt.
                signer_public_key: PublicKey::empty(KeyType::ED25519),
                gas_price,
                output_data_receivers: vec![],
                input_data_ids: vec![],
                actions: vec![Action::FunctionCall(FunctionCallAction {
                    method_name: "init".to_string(),
                    args,
                    gas: INIT_GAS,
                    deposit: 0,
                })],
            }),
        };
        res.push(StateRecord::PostponedReceipt(Box::new(receipt)));
    }
    res
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use chrono::TimeZone;
    use csv::WriterBuilder;
    use tempfile::NamedTempFile;

    use near_crypto::KeyType;

    use super::*;
    use near_primitives::network::PeerId;

    #[test]
    fn test_with_file() {
        fn timestamp(hour: u32, min: u32, sec: u32) -> Option<DateTime<Utc>> {
            Some(Utc.with_ymd_and_hms(2019, 12, 21, hour, min, sec).single().unwrap())
        }

        let file = NamedTempFile::new().unwrap();
        let mut writer = WriterBuilder::new().has_headers(true).from_writer(file.reopen().unwrap());
        writer
            .serialize(Row {
                genesis_time: Some(Utc::now()),
                account_id: "alice_near".parse().unwrap(),
                regular_pks: vec![
                    PublicKey::empty(KeyType::ED25519),
                    PublicKey::empty(KeyType::ED25519),
                ],
                privileged_pks: vec![PublicKey::empty(KeyType::ED25519)],
                foundation_pks: vec![PublicKey::empty(KeyType::ED25519)],
                full_pks: vec![],
                amount: 1000,
                lockup: timestamp(23, 0, 0),
                vesting_start: timestamp(22, 0, 0),
                vesting_end: timestamp(23, 30, 0),
                vesting_cliff: timestamp(22, 30, 20),
                validator_stake: 100,
                validator_key: Some(PublicKey::empty(KeyType::ED25519)),
                peer_info: Some(PeerInfo {
                    id: PeerId::new(PublicKey::empty(KeyType::ED25519)),
                    addr: Some("127.0.0.1:8080".parse().unwrap()),
                    account_id: None,
                }),
                is_treasury: false,
                smart_contract: None,
            })
            .unwrap();
        writer
            .serialize(Row {
                genesis_time: None,
                account_id: "bob_near".parse().unwrap(),
                regular_pks: vec![],
                privileged_pks: vec![PublicKey::empty(KeyType::ED25519)],
                foundation_pks: vec![PublicKey::empty(KeyType::ED25519)],
                full_pks: vec![],
                amount: 2000,
                lockup: timestamp(23, 0, 0),
                vesting_start: None,
                vesting_end: None,
                vesting_cliff: None,
                validator_stake: 0,
                validator_key: None,
                peer_info: None,
                is_treasury: true,
                smart_contract: None,
            })
            .unwrap();
        writer.flush().unwrap();
        keys_to_state_records(file.reopen().unwrap(), 1).unwrap();
    }

    #[test]
    fn test_res_file() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("res/test_accounts.csv");
        let res = std::fs::read(path).unwrap();
        keys_to_state_records(&res[..], 1).unwrap();
    }
}

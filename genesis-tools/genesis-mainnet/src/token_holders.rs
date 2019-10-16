//! Constructs state of token holders from the csv file.
use chrono::{DateTime, Utc};
use csv::ReaderBuilder;
use near::config::AccountInfo;
use near_crypto::PublicKey;
use near_network::PeerInfo;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::serialize::to_base64;
use near_primitives::types::{AccountId, Balance};
use near_primitives::views::{AccessKeyPermissionView, AccessKeyView, AccountView};
use node_runtime::StateRecord;
use serde::{Deserialize, Serialize};

/// Methods that can be called by a non-privileged access key.
const REGULAR_METHOD_NAMES: &[&str] = &["stake", "transfer"];
/// Methods that can be called by a privileged access key.
const PRIVILEGED_METHOD_NAMES: &[&str] = &["add_access_key", "remove_access_key"];
/// Methods that can be called by an access key owned by a "supervisor".
const SUPERVISOR_METHOD_NAMES: &[&str] = &["permanently_unstake", "terminate", "init"];
/// The number of non-leap seconds since January 1, 1970 0:00:00 UTC (aka "UNIX timestamp").
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

#[derive(Serialize, Deserialize, Debug)]
struct Row {
    account_id: AccountId,
    #[serde(with = "crate::serde_with::pks_as_str")]
    regular_pks: Vec<PublicKey>,
    #[serde(with = "crate::serde_with::pks_as_str")]
    privileged_pks: Vec<PublicKey>,
    #[serde(with = "crate::serde_with::pks_as_str")]
    supervisor_pks: Vec<PublicKey>,
    amount: Balance,
    lockup: Option<DateTime<Utc>>,
    vesting_start: Option<DateTime<Utc>>,
    vesting_end: Option<DateTime<Utc>>,
    vesting_cliff: Option<DateTime<Utc>>,
    validator_stake: Balance,
    validator_key: Option<PublicKey>,
    #[serde(with = "crate::serde_with::peer_info_to_str")]
    peer_info: Option<PeerInfo>,
}

/// Given path to the csv file produces:
/// * `StateRecord`s that represent the state of the token holders;
/// * `AccountInfo`s that represent validators;
/// * `PeerInfo`s that represent boot nodes.
pub fn keys_to_state_records<R>(
    reader: R,
    contract_code: &[u8],
) -> Result<(Vec<StateRecord>, Vec<AccountInfo>, Vec<PeerInfo>)>
where
    R: std::io::Read,
{
    let mut reader = ReaderBuilder::new().has_headers(true).from_reader(reader);

    let contract_code_base64 = to_base64(contract_code);
    let contract_code_hash = hash(contract_code);

    let mut state_records = vec![];
    let mut initial_validators = vec![];
    let mut boot_nodes = vec![];
    for row in reader.deserialize() {
        let row: Row = row?;
        row.verify()?;

        state_records.extend(account_records(
            &row,
            contract_code_base64.clone(),
            contract_code_hash.clone(),
        ));
        if let Some(ref validator_key) = row.validator_key {
            initial_validators.push(AccountInfo {
                account_id: row.account_id.clone(),
                public_key: validator_key.into(),
                amount: row.validator_stake,
            });
        }

        if let Some(peer_info) = row.peer_info {
            boot_nodes.push(peer_info);
        }
    }
    Ok((state_records, initial_validators, boot_nodes))
}

/// Returns the records representing state of an individual token holder.
fn account_records(
    row: &Row,
    contract_code_base64: String,
    contract_code_hash: CryptoHash,
) -> Vec<StateRecord> {
    let mut res = vec![StateRecord::Account {
        account_id: row.account_id.clone(),
        account: AccountView {
            amount: row.amount,
            locked: row.validator_stake,
            code_hash: contract_code_hash.into(),
            storage_usage: 0,
            storage_paid_at: 0,
        },
    }];

    // Add public keys.
    for (pks, method_names) in vec![
        (row.regular_pks.clone(), REGULAR_METHOD_NAMES),
        (row.privileged_pks.clone(), PRIVILEGED_METHOD_NAMES),
        (row.supervisor_pks.clone(), SUPERVISOR_METHOD_NAMES),
    ] {
        for pk in pks {
            res.push(StateRecord::AccessKey {
                account_id: row.account_id.clone(),
                public_key: pk,
                access_key: AccessKeyView {
                    nonce: 0,
                    permission: AccessKeyPermissionView::FunctionCall {
                        allowance: None,
                        receiver_id: row.account_id.clone(),
                        method_names: method_names.iter().map(|x| x.to_string()).collect(),
                    },
                },
            })
        }
    }

    // Add smart contract code.
    res.push(StateRecord::Contract {
        account_id: row.account_id.clone(),
        code: contract_code_base64,
    });

    // TODO: Add init function call.
    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use csv::WriterBuilder;
    use near_crypto::KeyType;
    use near_network::types::PeerId;
    use tempfile::NamedTempFile;

    #[test]
    fn test_with_file() {
        let file = NamedTempFile::new().unwrap();
        let mut writer = WriterBuilder::new().has_headers(true).from_writer(file.reopen().unwrap());
        writer
            .serialize(Row {
                account_id: "alice_near".to_string(),
                regular_pks: vec![
                    PublicKey::empty(KeyType::ED25519),
                    PublicKey::empty(KeyType::ED25519),
                ],
                privileged_pks: vec![PublicKey::empty(KeyType::ED25519)],
                supervisor_pks: vec![PublicKey::empty(KeyType::ED25519)],
                amount: 1000,
                lockup: Some(Utc.ymd(2019, 12, 21).and_hms(23, 0, 0)),
                vesting_start: Some(Utc.ymd(2019, 12, 21).and_hms(22, 0, 0)),
                vesting_end: Some(Utc.ymd(2019, 12, 21).and_hms(23, 30, 0)),
                vesting_cliff: Some(Utc.ymd(2019, 12, 21).and_hms(22, 30, 20)),
                validator_stake: 100,
                validator_key: Some(PublicKey::empty(KeyType::ED25519)),
                peer_info: Some(PeerInfo {
                    id: PeerId::new(PublicKey::empty(KeyType::ED25519)),
                    addr: Some("127.0.0.1:8080".parse().unwrap()),
                    account_id: None,
                }),
            })
            .unwrap();
        writer
            .serialize(Row {
                account_id: "bob_near".to_string(),
                regular_pks: vec![],
                privileged_pks: vec![PublicKey::empty(KeyType::ED25519)],
                supervisor_pks: vec![PublicKey::empty(KeyType::ED25519)],
                amount: 2000,
                lockup: Some(Utc.ymd(2019, 12, 21).and_hms(23, 0, 0)),
                vesting_start: None,
                vesting_end: None,
                vesting_cliff: None,
                validator_stake: 0,
                validator_key: None,
                peer_info: None,
            })
            .unwrap();
        writer.flush().unwrap();
        println!("{}", file.path().to_str().unwrap());
        keys_to_state_records(file.reopen().unwrap(), &[]).unwrap();
    }

    #[test]
    fn test_res_file() {
        let res = include_bytes!("../res/test_token_holders.csv");
        keys_to_state_records(&res[..], &[]).unwrap();
    }
}

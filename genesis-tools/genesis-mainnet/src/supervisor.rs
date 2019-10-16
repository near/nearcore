use csv::ReaderBuilder;
use near::config::AccountInfo;
use near_crypto::PublicKey;
use near_network::PeerInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, Balance};
use near_primitives::views::{AccessKeyPermissionView, AccessKeyView, AccountView};
use node_runtime::StateRecord;
use serde::{Deserialize, Serialize};

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
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Row {
    account_id: AccountId,
    #[serde(with = "crate::serde_with::pks_as_str")]
    full_pks: Vec<PublicKey>,
    amount: Balance,
    is_treasury: bool,
    validator_stake: Balance,
    validator_key: Option<PublicKey>,
    #[serde(with = "crate::serde_with::peer_info_to_str")]
    peer_info: Option<PeerInfo>,
}

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub fn keys_to_state_records<R>(
    reader: R,
) -> Result<(Vec<StateRecord>, Vec<AccountInfo>, Vec<PeerInfo>, AccountId)>
where
    R: std::io::Read,
{
    let mut reader = ReaderBuilder::new().has_headers(true).from_reader(reader);

    let mut state_records = vec![];
    let mut initial_validators = vec![];
    let mut boot_nodes = vec![];
    let mut treasury = None;
    for row in reader.deserialize() {
        let row: Row = row?;
        row.verify()?;

        state_records.extend(account_records(&row));

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

        if row.is_treasury {
            if treasury.is_some() {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "More than one treasury account specified.",
                )));
            }
            treasury = Some(row.account_id.clone());
        }
    }

    let treasury = treasury.expect("At least one supervisor account should be marked as treasury.");
    Ok((state_records, initial_validators, boot_nodes, treasury))
}
/// Returns the records representing state of an individual account of a supervisor.
fn account_records(row: &Row) -> Vec<StateRecord> {
    let mut res = vec![StateRecord::Account {
        account_id: row.account_id.clone(),
        account: AccountView {
            amount: row.amount,
            locked: row.validator_stake,
            code_hash: CryptoHash::default().into(),
            storage_usage: 0,
            storage_paid_at: 0,
        },
    }];

    // Add public keys.
    for pk in row.full_pks.clone() {
        res.push(StateRecord::AccessKey {
            account_id: row.account_id.clone(),
            public_key: pk,
            access_key: AccessKeyView { nonce: 0, permission: AccessKeyPermissionView::FullAccess },
        })
    }
    res
}

#[cfg(test)]
mod tests {
    use super::*;
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
                account_id: "carol_near".to_string(),
                full_pks: vec![
                    PublicKey::empty(KeyType::ED25519),
                    PublicKey::empty(KeyType::ED25519),
                ],
                amount: 1000,
                is_treasury: false,
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
                account_id: "dave_near".to_string(),
                full_pks: vec![],
                amount: 2000,
                is_treasury: true,
                validator_stake: 0,
                validator_key: None,
                peer_info: None,
            })
            .unwrap();
        writer.flush().unwrap();
        println!("{}", file.path().to_str().unwrap());
        keys_to_state_records(file.reopen().unwrap()).unwrap();
    }

    #[test]
    fn test_res_file() {
        let res = include_bytes!("../res/test_supervisor.csv");
        keys_to_state_records(&res[..]).unwrap();
    }
}

//! Constructs state of token holders from the csv file.
use chrono::{DateTime, Utc};
use csv::{Reader, ReaderBuilder};
use near_crypto::{PublicKey, ReadablePublicKey};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, Balance};
use near_primitives::views::{AccessKeyPermissionView, AccessKeyView, AccountView};
use node_runtime::StateRecord;
use std::convert::TryFrom;
use std::path::Path;

/// Character delimiter used by the csv.
const CSV_DELIMITER: u8 = b';';
/// Character that we use to delimit public keys in the csv file.
const PUBLIC_KEY_DELIMITER: char = ',';
/// Methods that can be called by a non-privileged access key.
const REGULAR_METHOD_NAMES: &[&str] = &["stake", "transfer"];
/// Methods that can be called by a privileged access key.
const PRIVILEGED_METHOD_NAMES: &[&str] = &["add_access_key", "remove_access_key"];
/// Methods that can be called by an access key owned by a "supervisor".
const SUPERVISOR_METHOD_NAMES: &[&str] = &["permanently_unstake", "terminate", "init"];
/// The number of non-leap seconds since January 1, 1970 0:00:00 UTC (aka "UNIX timestamp").
type UnixTimestamp = i64;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Given path to the csv file produces `StateRecord`s that represent the state of the token
/// holders.
fn keys_to_state_records(keys_file: &Path) -> Result<Vec<StateRecord>> {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .delimiter(CSV_DELIMITER)
        .from_path(keys_file)
        .expect("Failed to open csv file.");

    assert_eq!(
        reader.headers()?,
        vec![
            "AccountId",            // 0
            "PublicKeys",           // 1
            "PrivilegedPublicKeys", // 2
            "SupervisorPublicKeys", // 3
            "Amount",               // 4
            "Lockup",               // 5
            "VestingStart",         // 6
            "VestingEnd",           // 7
            "VestingCliff"          // 8
        ],
        "Expected different csv structure."
    );

    let mut state_records = vec![];
    for record in reader.records() {
        let record = record?;
        let account_id: AccountId = record[0].to_string();
        let regular_pks = record[1]
            .split(PUBLIC_KEY_DELIMITER)
            .map(|x| PublicKey::try_from(ReadablePublicKey::new(x)))
            .collect::<Result<Vec<PublicKey>>>()?;
        let privileged_pks: Vec<PublicKey> = record[2]
            .split(PUBLIC_KEY_DELIMITER)
            .map(|x| PublicKey::try_from(ReadablePublicKey::new(x)))
            .collect::<Result<Vec<PublicKey>>>()?;
        let supervisor_pks: Vec<PublicKey> = record[2]
            .split(PUBLIC_KEY_DELIMITER)
            .map(|x| PublicKey::try_from(ReadablePublicKey::new(x)))
            .collect::<Result<Vec<PublicKey>>>()?;
        let amount: Balance = record[3].parse()?;
        let lockup: UnixTimestamp = DateTime::parse_from_rfc3339(&record[4])?.timestamp();
        let vesting_start: UnixTimestamp = DateTime::parse_from_rfc3339(&record[5])?.timestamp();
        let vesting_end: UnixTimestamp = DateTime::parse_from_rfc3339(&record[6])?.timestamp();
        let vesting_cliff: UnixTimestamp = DateTime::parse_from_rfc3339(&record[7])?.timestamp();
        state_records.extend(account_records(
            account_id,
            regular_pks,
            privileged_pks,
            supervisor_pks,
            amount,
            lockup,
            vesting_start,
            vesting_end,
            vesting_cliff,
        ))
    }
    Ok(state_records)
}

/// Returns the records representing state of an individual token holder.
fn account_records(
    account_id: AccountId,
    regular_pks: Vec<PublicKey>,
    privileged_pks: Vec<PublicKey>,
    supervisor_pks: Vec<PublicKey>,
    amount: Balance,
    lockup: UnixTimestamp,
    vesting_start: UnixTimestamp,
    vesting_end: UnixTimestamp,
    vesting_cliff: UnixTimestamp,
) -> Vec<StateRecord> {
    let mut res = vec![StateRecord::Account {
        account_id: account_id.clone(),
        account: AccountView {
            amount,
            locked: 0,
            code_hash: CryptoHash::default().into(),
            storage_usage: 0,
            storage_paid_at: 0,
        },
    }];

    // Add regular public keys.
    for (pks, method_names) in vec![
        (regular_pks, REGULAR_METHOD_NAMES),
        (privileged_pks, PRIVILEGED_METHOD_NAMES),
        (supervisor_pks, SUPERVISOR_METHOD_NAMES),
    ] {
        for pk in pks {
            res.push(StateRecord::AccessKey {
                account_id: account_id.clone(),
                public_key: pk,
                access_key: AccessKeyView {
                    nonce: 0,
                    permission: AccessKeyPermissionView::FunctionCall {
                        allowance: None,
                        receiver_id: account_id.clone(),
                        method_names: method_names.iter().map(|x| x.to_string()).collect(),
                    },
                },
            })
        }
    }

    res
}

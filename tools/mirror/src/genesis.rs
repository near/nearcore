use near_primitives::state_record::StateRecord;
use serde::ser::{SerializeSeq, Serializer};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

pub fn map_records<P: AsRef<Path>>(
    records_file_in: P,
    records_file_out: P,
    no_secret: bool,
    secret_file_out: P,
) -> anyhow::Result<()> {
    let secret = if !no_secret {
        Some(crate::secret::generate(secret_file_out)?)
    } else {
        crate::secret::write_empty(secret_file_out)?;
        None
    };
    let reader = BufReader::new(File::open(records_file_in)?);
    let records_out = BufWriter::new(File::create(records_file_out)?);
    let mut records_ser = serde_json::Serializer::new(records_out);
    let mut records_seq = records_ser.serialize_seq(None).unwrap();

    near_chain_configs::stream_records_from_file(reader, |r| {
        match r {
            StateRecord::AccessKey { account_id, public_key, access_key } => {
                let replacement = crate::key_mapping::map_key(&public_key, secret.as_ref());
                let new_record = StateRecord::AccessKey {
                    account_id,
                    public_key: replacement.public_key(),
                    access_key,
                };
                // TODO: would be nice for stream_records_from_file() to let you return early on error so
                // we dont have to unwrap here
                records_seq.serialize_element(&new_record).unwrap();
            }
            _ => records_seq.serialize_element(&r).unwrap(),
        };
    })?;
    records_seq.end()?;
    Ok(())
}

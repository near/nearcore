use anyhow::Context;

use near_crypto::{PublicKey, SecretKey};

pub(crate) fn map_pub_key(
    public_key: &str,
    secret: Option<&[u8; crate::secret::SECRET_LEN]>,
) -> anyhow::Result<SecretKey> {
    let public_key: PublicKey = public_key.parse().context("Could not parse public key")?;
    Ok(crate::key_mapping::map_key(&public_key, secret))
}

use std::fs::File;
use std::io;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{PublicKey, SecretKey};

use near_account_id::AccountId;

#[derive(Serialize, Deserialize)]
pub struct KeyFile {
    pub account_id: AccountId,
    pub public_key: PublicKey,
    pub secret_key: SecretKey,
}

impl KeyFile {
    pub fn write_to_file(&self, path: &Path) -> io::Result<()> {
        let mut file = File::create(path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perm = std::fs::Permissions::from_mode((libc::S_IWUSR | libc::S_IRUSR).into());
            file.set_permissions(perm)?;
        }
        let str = serde_json::to_string_pretty(self)?;
        file.write_all(str.as_bytes())
    }

    pub fn from_file(path: &Path) -> io::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Ok(serde_json::from_str(&content)?)
    }
}

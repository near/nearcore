use std::fs::File;
use std::io::{Read, Write};
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
    pub fn write_to_file(&self, path: &Path) -> std::io::Result<()> {
        let mut file = File::create(path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perm = std::fs::Permissions::from_mode(u32::from(libc::S_IWUSR | libc::S_IRUSR));
            file.set_permissions(perm)?;
        }
        let str = serde_json::to_string_pretty(self)?;
        file.write_all(str.as_bytes())
    }

    pub fn from_file(path: &Path) -> Self {
        let mut file = File::open(path).expect("Could not open key file.");
        let mut content = String::new();
        file.read_to_string(&mut content).expect("Could not read from key file.");
        serde_json::from_str(&content).expect("Failed to deserialize KeyFile")
    }
}

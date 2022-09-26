/// Database version type.
pub type DbVersion = u32;

/// Current version of the database.
pub const DB_VERSION: DbVersion = 32;

/// Returns serialisation of the version.
///
/// The serialisation simply converts the version into decimal integer and
/// returns that as ASCII string.  [`deserialise`] is inverse of this operation.
pub fn serialise(version: DbVersion) -> Vec<u8> {
    version.to_string().into_bytes()
}

/// Deserialises database version from data read from database.
///
/// This is an inverse of [`serialise`].
pub fn deserialise(bytes: &[u8]) -> Result<DbVersion, String> {
    let value = std::str::from_utf8(bytes)
        .map_err(|err| format!("invalid DbVersion (‘{bytes:x?}’): {err}"))?;
    let ver = DbVersion::from_str_radix(value, 10)
        .map_err(|err| format!("invalid DbVersion (‘{value}’): {err}"))?;
    Ok(ver)
}

pub fn set_store_version(store: &crate::Store, db_version: DbVersion) -> std::io::Result<()> {
    let mut store_update = store.store_update();
    store_update.set(crate::DBCol::DbVersion, crate::db::VERSION_KEY, &serialise(db_version));
    store_update.commit()
}

pub(super) fn get_db_version(db: &dyn crate::Database) -> std::io::Result<Option<DbVersion>> {
    db.get_raw_bytes(crate::DBCol::DbVersion, crate::db::VERSION_KEY)?
        .map(|bytes| deserialise(&bytes))
        .transpose()
        .map_err(|msg| std::io::Error::new(std::io::ErrorKind::Other, msg))
}

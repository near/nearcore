/// Database version type.
pub type DbVersion = u32;

/// Current version of the database.
pub const DB_VERSION: DbVersion = 31;

/// Deserialises database version from data read from database.
///
/// The data is first converted to a string (i.e. verified if it’s UTF-8) and
/// then into a number.
fn deserialise(bytes: &[u8]) -> Result<DbVersion, String> {
    let value =
        std::str::from_utf8(bytes).map_err(|_err| format!("invalid DbVersion: {bytes:x?}"))?;
    let ver = DbVersion::from_str_radix(value, 10)
        .map_err(|_err| format!("invalid DbVersion: ‘{value}’"))?;
    Ok(ver)
}

pub(super) fn set_store_version(
    store: &crate::Store,
    db_version: DbVersion,
) -> std::io::Result<()> {
    let db_version = db_version.to_string();
    let mut store_update = store.store_update();
    store_update.set(crate::DBCol::DbVersion, crate::db::VERSION_KEY, db_version.as_bytes());
    store_update.commit()
}

pub(super) fn get_db_version(db: &dyn crate::Database) -> std::io::Result<Option<DbVersion>> {
    db.get_raw_bytes(crate::DBCol::DbVersion, crate::db::VERSION_KEY)?
        .map(|bytes| deserialise(&bytes))
        .transpose()
        .map_err(|msg| std::io::Error::new(std::io::ErrorKind::Other, msg))
}

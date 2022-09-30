use crate::{DBCol, NodeStorage, Temperature};

/// Database version type.
pub type DbVersion = u32;

/// Current version of the database.
pub const DB_VERSION: DbVersion = 32;

/// Database version at which point DbKind was introduced.
pub(super) const DB_VERSION_WITH_KIND: DbVersion = 32;

/// Key for the version entry in DBCol::DbVersion.
///
/// The key holds [`DbVersion`] value serialised to a string.
///
/// The version is strictly increasing.  We bump it each time
/// a backwards-incompatible change to the database is required.  Increasing the
/// version involves performing a migration since node can only open databases
/// with version they was built for (see [`DB_VERSION`]).
pub(super) const VERSION_KEY: &[u8; 7] = b"VERSION";

/// Key for the database kind entry in DBCol::DbVersion.
///
/// The key holds a [`DbKind`] value serialised to a string.
pub(super) const KIND_KEY: &[u8; 4] = b"KIND";

/// Describes what kind the storage is.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, strum::Display, strum::EnumString, strum::IntoStaticStr,
)]
pub enum DbKind {
    /// The database is an RPC database meaning that it is garbage collected and
    /// stores only a handful of epochs worth of data.
    RPC,
    /// The database is an archive database meaning that it is not garbage
    /// collected and stores all chain data.
    Archive,
}

pub(super) fn set_store_version(storage: &NodeStorage, version: DbVersion) -> std::io::Result<()> {
    set_store_metadata(storage, DbMetadata { version, kind: None })
}

pub(super) fn set_store_metadata(
    storage: &NodeStorage,
    metadata: DbMetadata,
) -> std::io::Result<()> {
    let version = metadata.version.to_string().into_bytes();
    let kind = metadata.kind.map(|kind| <&str>::from(kind).as_bytes());
    let mut store_update = storage.get_store(Temperature::Hot).store_update();
    store_update.set(DBCol::DbVersion, VERSION_KEY, &version);
    if metadata.version >= DB_VERSION_WITH_KIND {
        if let Some(kind) = kind {
            store_update.set(DBCol::DbVersion, KIND_KEY, kind);
        }
    }
    store_update.commit()
}

/// Metadata about a database.
#[derive(Clone, Copy)]
pub(super) struct DbMetadata {
    /// Version of the database.
    pub version: DbVersion,

    /// Kind of the database.
    ///
    /// This is always set if version is ≥ [`DB_VERSION_WITH_KIND`] and always
    /// `None` otherwise.
    pub kind: Option<DbKind>,
}

impl DbMetadata {
    /// Reads metadata from the database.
    ///
    /// If the database version is not given, returns an error.  Similarly, if
    /// database version is ≥ [`DB_VERSION_WITH_KIND`] but the kind is not
    /// specified, returns an error.
    pub(super) fn read(db: &dyn crate::Database) -> std::io::Result<Self> {
        let version = read("DbVersion", db, VERSION_KEY)?;
        let kind = if version < DB_VERSION_WITH_KIND {
            // If database is at version less than DB_VERSION_WITH_KIND then it
            // doesn’t have kind.  Return None as kind.
            None
        } else {
            Some(read("DbKind", db, KIND_KEY)?)
        };
        Ok(Self { version, kind })
    }
}

/// Reads value from DbVersion column and parses it using `FromStr`.
///
/// Reads value for given `key` from [`DBCol::DbVersion`], verifies if it’s
/// valid UTF-8 and then converts into `T` using `from_str`.  If the value is
/// missing or parsing fails returns an error.
fn read<T: std::str::FromStr>(
    what: &str,
    db: &dyn crate::Database,
    key: &[u8],
) -> std::io::Result<T> {
    let msg = "it’s not a neard database or database is corrupted";
    if let Some(bytes) = db.get_raw_bytes(crate::DBCol::DbVersion, key)? {
        let value = std::str::from_utf8(&bytes)
            .map_err(|_err| format!("invalid {what}: {bytes:?}; {msg}"))
            .map_err(other_error)?;
        let value = T::from_str(value)
            .map_err(|_err| format!("invalid {what}: ‘{value}’; {msg}"))
            .map_err(other_error)?;
        Ok(value)
    } else {
        Err(other_error(format!("missing {what}; {msg}")))
    }
}

fn other_error(msg: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, msg)
}

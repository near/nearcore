#[cfg(feature = "cold_store")]
pub mod cli;
#[cfg(feature = "cold_store")]
pub use cli::ColdStoreCommand;

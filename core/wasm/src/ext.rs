pub mod ids {
    // Storate related
    pub const STORAGE_READ_LEN_FUNC: usize = 100;
    pub const STORAGE_READ_INTO_FUNC: usize = 110;
    pub const STORAGE_WRITE_FUNC: usize = 120;

    // Balance
    pub const BALANCE_FUNC: usize = 200;
    pub const TRANSFER_FUNC: usize = 210;

    // Contract
    pub const GAS_FUNC: usize = 300;
    /// Function from gas counter
    pub const ASSERT_HAS_MANA_FUNC: usize = 310;

    pub const PANIC_FUNC: usize = 1000;
    pub const DEBUG_FUNC: usize = 1010;
}

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    SAD,
}

pub type Result<T> = ::std::result::Result<T, Error>;

pub trait Externalities {
    fn storage_put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    fn storage_get(&self, key: &[u8]) -> Result<Option<&[u8]>>;
}

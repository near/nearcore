use primitives::types::{AccountAlias, PromiseId, Balance, Mana};

pub mod ids {
    // Storate related
    pub const STORAGE_READ_LEN_FUNC: usize = 100;
    pub const STORAGE_READ_INTO_FUNC: usize = 110;
    pub const STORAGE_WRITE_FUNC: usize = 120;

    // Balance
    pub const BALANCE_FUNC: usize = 200;
    pub const TRANSFER_FUNC: usize = 210;

    // Contract
    /// Function from gas counter
    pub const GAS_FUNC: usize = 300;
    pub const ASSERT_HAS_MANA_FUNC: usize = 310;

    // Promises
    pub const PROMISE_CREATE_FUNC: usize = 400;
    pub const PROMISE_THEN_FUNC: usize = 410;
    pub const PROMISE_AND_FUNC: usize = 420;

    // Function arguments, result and returns
    pub const INPUT_READ_LEN_FUNC: usize = 500;
    pub const INPUT_READ_INTO_FUNC: usize = 510;
    pub const RESULT_COUNT_FUNC: usize = 520;
    pub const RESULT_IS_OK_FUNC: usize = 530;
    pub const RESULT_READ_LEN_FUNC: usize = 540;
    pub const RESULT_READ_INTO_FUNC: usize = 550;
    pub const RETURN_VALUE_FUNC: usize = 560;
    pub const RETURN_PROMISE_FUNC: usize = 570;

    // Dev
    pub const PANIC_FUNC: usize = 1000;
    pub const DEBUG_FUNC: usize = 1010;
}

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    NotImplemented,
    PromiseIdNotFound,
    WrongPromise,
    PromiseAlreadyHasCallback,
}

pub type Result<T> = ::std::result::Result<T, Error>;

pub trait External {
    fn storage_set(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    fn storage_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn promise_create(
        &mut self,
        account_alias: AccountAlias,
        method_name: Vec<u8>,
        arguments: Vec<u8>,
        mana: Mana,
        amount: Balance,
    ) -> Result<PromiseId>;

    fn promise_then(
        &mut self,
        promise_id: PromiseId,
        method_name: Vec<u8>,
        arguments: Vec<u8>,
        mana: Mana,
    ) -> Result<PromiseId>;
}

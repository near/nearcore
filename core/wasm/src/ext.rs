use primitives::types::{AccountId, PromiseId, Balance, Mana};

pub mod ids {
    // Storage related
    pub const STORAGE_READ_LEN_FUNC: usize = 100;
    pub const STORAGE_READ_INTO_FUNC: usize = 110;
    pub const STORAGE_WRITE_FUNC: usize = 120;
    pub const STORAGE_ITER_FUNC: usize = 130;
    pub const STORAGE_ITER_NEXT_FUNC: usize = 131;
    pub const STORAGE_ITER_PEEK_LEN_FUNC: usize = 132;
    pub const STORAGE_ITER_PEEK_INTO_FUNC: usize = 133;
    // TODO(#350): Refactor all reads and writes into generic reads. 
    /// Generic data read. Returns the length of the buffer for the type/key.
    pub const READ_LEN_FUNC: usize = 140;
    /// Generic data read. Writes content of the buffer for the type/key into the given pointer.
    pub const READ_INTO_FUNC: usize = 150;

    /// Returns the current balance.
    pub const BALANCE_FUNC: usize = 200;
    /// Returns the amount of MANA left.
    pub const MANA_LEFT_FUNC: usize = 210;
    /// Returns the amount of GAS left.
    pub const GAS_LEFT_FUNC: usize = 220;
    /// Returns the amount of balance received for this call.
    pub const RECEIVED_AMOUNT_FUNC: usize = 230;
    /// Returns currently produced block index.
    pub const BLOCK_INDEX_FUNC: usize = 240;
    /// Fills given buffer of given length with random values.
    pub const RANDOM_BUF_FUNC: usize = 250;
    /// Returns random u32.
    pub const RANDOM_32_FUNC: usize = 260;

    /// Function from gas counter. Automatically called by the gas meter.
    pub const GAS_FUNC: usize = 300;
    /// Contracts can assert properties. E.g. check the amount available mana.
    pub const ASSERT_FUNC: usize = 310;
    pub const ABORT_FUNC: usize = 320;

    /// Creates a new promise that makes an async call to some other contract.
    pub const PROMISE_CREATE_FUNC: usize = 400;
    /// Attaches a callback to a given promise. This promise can be either an
    /// async call or multiple joined promises.
    /// NOTE: The given promise can't be a callback.
    pub const PROMISE_THEN_FUNC: usize = 410;
    /// Joins 2 given promises together and returns a new promise.
    pub const PROMISE_AND_FUNC: usize = 420;

    /// Returns total byte length of the arguments.
    pub const INPUT_READ_LEN_FUNC: usize = 500;
    pub const INPUT_READ_INTO_FUNC: usize = 510;
    /// Returns the number of returned results for this callback.
    pub const RESULT_COUNT_FUNC: usize = 520;
    pub const RESULT_IS_OK_FUNC: usize = 530;
    pub const RESULT_READ_LEN_FUNC: usize = 540;
    pub const RESULT_READ_INTO_FUNC: usize = 550;
    /// Called to return value from the function.
    pub const RETURN_VALUE_FUNC: usize = 560;
    /// Called to return promise from the function.
    pub const RETURN_PROMISE_FUNC: usize = 570;

    // Crypto and hashing
    /// Hashes given buffer and writes 32 bytes of result in the given pointer.
    pub const HASH_FUNC: usize = 600;
    /// Returns hash of the given buffer into u32.
    pub const HASH_32_FUNC: usize = 610;

    // Dev
    pub const PANIC_FUNC: usize = 1000;
    pub const DEBUG_FUNC: usize = 1010;
    pub const LOG_FUNC: usize = 1020;
}

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    NotImplemented,
    PromiseIdNotFound,
    WrongPromise,
    PromiseAlreadyHasCallback,
    TrieIteratorError,
    TrieIteratorMissing,
}

pub type Result<T> = ::std::result::Result<T, Error>;

pub trait External {
    fn storage_set(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    fn storage_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn storage_iter(&mut self, prefix: &[u8]) -> Result<u32>;

    fn storage_iter_next(&mut self, id: u32) -> Result<Option<Vec<u8>>>;

    fn storage_iter_peek(&mut self, id: u32) -> Result<Option<Vec<u8>>>;

    fn promise_create(
        &mut self,
        account_id: AccountId,
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

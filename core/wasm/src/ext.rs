pub mod ids {
    // Storate related
	pub const STORAGE_READ_FUNC: usize = 100;
	pub const STORAGE_WRITE_FUNC: usize = 110;

    // Balance
	pub const BALANCE_FUNC: usize = 200;
	pub const TRANSFER_FUNC: usize = 210;

    // Contract
	pub const ASSERT_HAS_MANA_FUNC: usize = 300;

	pub const PANIC_FUNC: usize = 1000;
	pub const DEBUG_FUNC: usize = 1010;
}

pub trait Externalities {

}
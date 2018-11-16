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

/// Wasm cost table
pub struct WasmCosts {
	/// Default opcode cost
	pub regular: u32,
	/// Div operations multiplier.
	pub div: u32,
	/// Div operations multiplier.
	pub mul: u32,
	/// Memory (load/store) operations multiplier.
	pub mem: u32,
	/// General static query of U256 value from env-info
	pub static_u256: u32,
	/// General static query of Address value from env-info
	pub static_address: u32,
	/// Memory stipend. Amount of free memory (in 64kb pages) each contract can use for stack.
	pub initial_mem: u32,
	/// Grow memory cost, per page (64kb)
	pub grow_mem: u32,
	/// Memory copy cost, per byte
	pub memcpy: u32,
	/// Max stack height (native WebAssembly stack limiter)
	pub max_stack_height: u32,
	/// Cost of wasm opcode is calculated as TABLE_ENTRY_COST * `opcodes_mul` / `opcodes_div`
	pub opcodes_mul: u32,
	/// Cost of wasm opcode is calculated as TABLE_ENTRY_COST * `opcodes_mul` / `opcodes_div`
	pub opcodes_div: u32,
}

impl Default for WasmCosts {
	fn default() -> Self {
		WasmCosts {
			regular: 1,
			div: 16,
			mul: 4,
			mem: 2,
			static_u256: 64,
			static_address: 40,
			initial_mem: 4096,
			grow_mem: 8192,
			memcpy: 1,
			max_stack_height: 64*1024,
			opcodes_mul: 3,
			opcodes_div: 8,
		}
	}
}


pub trait Externalities {
    fn wasm_costs(&self) -> &WasmCosts;
}
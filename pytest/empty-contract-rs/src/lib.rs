use near_sdk::{env, metadata, near_bindgen};
use near_sdk::borsh::{self, BorshDeserialize, BorshSerialize};

#[global_allocator]
static ALLOC: near_sdk::wee_alloc::WeeAlloc = near_sdk::wee_alloc::WeeAlloc::INIT;

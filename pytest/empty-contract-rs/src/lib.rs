use near_sdk::{env, metadata, near_bindgen};
use near_sdk::borsh::{self, BorshDeserialize, BorshSerialize};

#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

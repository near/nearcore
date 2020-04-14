use borsh::{BorshDeserialize, BorshSerialize};
use near_sdk::{env, metadata, near_bindgen};

#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

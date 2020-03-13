use borsh::{BorshDeserialize, BorshSerialize};
use near_bindgen::{env, near_bindgen, Promise};

#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

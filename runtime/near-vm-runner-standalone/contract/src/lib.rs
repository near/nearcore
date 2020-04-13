use borsh::{BorshDeserialize, BorshSerialize};
use near_bindgen::{env, near_bindgen};

#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[near_bindgen]
#[derive(Default, BorshDeserialize, BorshSerialize)]
pub struct Basic {
    pings: i32
}

#[near_bindgen]
impl Basic {
    pub fn ping(&mut self) -> String {
        self.pings = self.pings + 1;
        format!("PONG {}: {}", env::current_account_id(), self.pings)
    }
}

use borsh::{BorshDeserialize, BorshSerialize};
use near_sdk::{env, near_bindgen};

#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[near_bindgen]
#[derive(Default, BorshDeserialize, BorshSerialize)]
pub struct Basic {
    pings: i32,
}

#[near_bindgen]
impl Basic {
    pub fn ping(&mut self) -> String {
        self.pings = self.pings + 1;
        env::syscall(2, 0, 1, 2, 3, 4, 5, 6, 7);
        format!("PONG {}: {}", env::current_account_id(), self.pings)
    }
}

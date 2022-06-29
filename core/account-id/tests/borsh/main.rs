use borsh::{BorshDeserialize, BorshSerialize};
use bolero::check;
use near_account_id::AccountId;

fn main() {
    check!().for_each(|input: &[u8]| {
        if let Ok(account_id) = AccountId::try_from_slice(input) {
            assert_eq!(
                account_id,
                AccountId::try_from_slice(account_id.try_to_vec().unwrap().as_slice()).unwrap()
            );
        }
    });
}

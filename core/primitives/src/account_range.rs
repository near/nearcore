use crate::types::AccountId;

#[derive(Debug)]
pub enum AccountRangeBoundary {
    Unbounded,
    Inclusive(AccountId),
    Exlusive(AccountId),
}

#[derive(Debug)]
pub struct AccountRange {
    pub from: AccountRangeBoundary,
    pub to: AccountRangeBoundary,
}

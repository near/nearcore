use crate::types::AccountId;

#[derive(Debug, Clone)]
pub enum AccountRangeBoundary {
    Unbounded,
    Inclusive(AccountId),
    Exlusive(AccountId),
}

#[derive(Debug, Clone)]
pub struct AccountRange {
    pub from: AccountRangeBoundary,
    pub to: AccountRangeBoundary,
}

impl AccountRange {
    pub fn contains(&self, account_id: &AccountId) -> bool {
        let ok_from = match self.from {
            AccountRangeBoundary::Unbounded => true,
            AccountRangeBoundary::Inclusive(ref boundary_account) => account_id >= boundary_account,
            AccountRangeBoundary::Exlusive(ref boundary_account) => account_id > boundary_account,
        };
        let ok_to = match self.to {
            AccountRangeBoundary::Unbounded => true,
            AccountRangeBoundary::Inclusive(ref boundary_account) => account_id <= boundary_account,
            AccountRangeBoundary::Exlusive(ref boundary_account) => account_id < boundary_account,
        };
        ok_from && ok_to
    }
}

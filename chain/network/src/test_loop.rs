use near_primitives::types::AccountId;

/// A multi-instance test using the TestLoop framework can support routing
/// lookup for network messages, as long as the Data type contains AccountId.
/// This trait is just a helper for looking up the index.
pub trait SupportsRoutingLookup {
    fn index_for_account(&self, account: &AccountId) -> usize;
}

impl<InnerData: AsRef<AccountId>> SupportsRoutingLookup for Vec<InnerData> {
    fn index_for_account(&self, account: &AccountId) -> usize {
        self.iter()
            .position(|data| data.as_ref() == account)
            .expect(&format!("Account not found: {}", account))
    }
}

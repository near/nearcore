use crate::models;

lazy_static::lazy_static! {
    pub(crate) static ref YOCTO_NEAR_CURRENCY: models::Currency =
        models::Currency { symbol: "yoctoNEAR".to_string(), decimals: 0, metadata: None };
}

pub(crate) enum SubAccount {
    LiquidBalanceForStorage,
    Locked,
}

impl From<SubAccount> for crate::models::SubAccountIdentifier {
    fn from(sub_account: SubAccount) -> Self {
        let address = match sub_account {
            SubAccount::LiquidBalanceForStorage => "liquid-for-storage".into(),
            SubAccount::Locked => "locked".into(),
        };
        crate::models::SubAccountIdentifier { address, metadata: None }
    }
}

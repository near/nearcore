use crate::models;

lazy_static::lazy_static! {
    pub(crate) static ref YOCTO_NEAR_CURRENCY: models::Currency =
        models::Currency { symbol: "yoctoNEAR".to_string(), decimals: 0, metadata: None };
}

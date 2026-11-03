mod fetch;
mod serve;

pub(in crate::spice::data_manager) use fetch::{is_done, made_pullable_by, needed_items};
pub(super) use serve::ReceiptProofServePolicy;

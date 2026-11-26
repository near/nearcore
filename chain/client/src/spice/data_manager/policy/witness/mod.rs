mod fetch;
mod serve;

pub(crate) use fetch::FALLBACK_TRACKING_DELAY;
pub(in crate::spice::data_manager) use fetch::{
    certified_by, fallback_items, is_done, is_pushed_to, last_certified_as_of_final_head,
    lowest_needed_height, made_pullable_by, needed_items,
};
pub(super) use serve::WitnessServePolicy;

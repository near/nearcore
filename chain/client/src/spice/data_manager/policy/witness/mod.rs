mod fetch;
mod serve;

#[cfg(test)]
pub(crate) use fetch::FALLBACK_TRACKING_LOOKAHEAD;
pub(in crate::spice::data_manager) use fetch::{
    certified_by, is_done, is_pushed_to, last_certified_as_of_final_head, lowest_needed_height,
    made_pullable_by, nearing_fallback, needed_items,
};
pub(super) use serve::WitnessServePolicy;

mod fetch;
mod serve;

pub(in crate::spice::data_manager) use fetch::{
    certified_by_parent, is_done, last_certified_as_of_final_head, lowest_needed_height,
    made_pullable_by, needed_items,
};
pub(super) use serve::WitnessServePolicy;

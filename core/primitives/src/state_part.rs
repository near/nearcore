// to specify a part we always specify both part_id and num_parts together
#[derive(Copy, Clone, Debug)]
pub struct PartId {
    pub idx: u64,
    pub total: u64,
}
impl PartId {
    pub fn new(part_id: u64, num_parts: u64) -> PartId {
        assert!(part_id < num_parts);
        PartId { idx: part_id, total: num_parts }
    }
}

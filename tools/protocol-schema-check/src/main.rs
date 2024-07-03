use near_structs_checker_core::collect_protocol_structs;

fn main() {
    let protocol_structs = collect_protocol_structs();
    let json = serde_json::to_string_pretty(&protocol_structs).unwrap();
    std::fs::write("protocol_structs.json", json).unwrap();
}

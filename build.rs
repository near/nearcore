use std::fs::rename;
use protoc_rust::Customize;

fn main() {
    let proto_files = [
        ("message.proto", "node/network/src/message_proto.rs")];
    for (input_file, out_name) in proto_files.iter() {
        protoc_rust::run(protoc_rust::Args {
            out_dir: "protos",
            input: &[&format!("protos/{}", input_file)],
            includes: &["protos"],
            customize: Customize {
                ..Default::default()
            },
        }).expect("protoc");
        let prefix = &input_file[..input_file.len() - 6];
        rename(format!("protos/{}.rs", prefix), out_name).expect("Failed to move the file");
    }
}

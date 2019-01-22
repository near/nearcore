use protoc_rust::Customize;

const PROTO_OUTPUT_DIR: &str = "core/protos/src";

fn main() {
    let proto_files = [
        "block.proto",
        "message.proto",
        "transaction.proto",
        "txflow.proto",
    ];
    for input_file in proto_files.iter() {
        protoc_rust::run(protoc_rust::Args {
            out_dir: PROTO_OUTPUT_DIR,
            input: &[&format!("protos/{}", input_file)],
            includes: &["protos"],
            customize: Customize {
                expose_oneof: Some(true),
                serde_derive: Some(true),
                ..Default::default()
            },
        }).expect("protoc");
    }
}

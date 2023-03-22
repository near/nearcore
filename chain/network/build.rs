fn main() -> anyhow::Result<()> {
    println!("cargo:rerun-if-changed=src/network_protocol/network.proto");
    protobuf_codegen::Codegen::new()
        .pure()
        .includes(["src/"])
        .input("src/network_protocol/network.proto")
        .cargo_out_dir("proto")
        .run()
}

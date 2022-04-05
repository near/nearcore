fn main() -> std::io::Result<()> {
    prost_build::compile_protos(&["src/network_protocol/network.proto"], &["src/"])
}

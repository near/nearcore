use std::env;
use std::fs;
use std::io::{Read, Write};
use std::path::Path;

use protoc_rust::Customize;

fn extract_mod_name(file_name: &str) -> String {
    let last = file_name.split('/').collect::<Vec<_>>().pop().unwrap();
    last.split('.').collect::<Vec<_>>()[0].to_string()
}

pub fn autogenerate() {
    // dumb vector hack because https://bit.ly/2RJcIH1
    let input_files: Vec<String> = fs::read_dir(Path::new("protos"))
        .expect("could not read protos directory")
        .map(|dir_entry| dir_entry.expect("unable to get entry").path().display().to_string())
        .collect();
    let input_files: Vec<&str> = input_files.iter().map(std::convert::AsRef::as_ref).collect();
    let out_dir = env::var("OUT_DIR").unwrap();
    println!("out dir: {}", out_dir);
    protoc_rust::run(protoc_rust::Args {
        out_dir: &out_dir,
        input: input_files.as_slice(),
        includes: &["protos", "/usr/local/include"],
        customize: Customize { expose_oneof: Some(true), ..Default::default() },
    })
    .expect("protoc");

    // a hack from https://github.com/googlecartographer/point_cloud_viewer/blob/bb73289523a3cee8091e9b6547b7b989d0fc61c7/build.rs
    // to avoid rust protobuf issue https://github.com/stepancheg/rust-protobuf/issues/117
    let output_files: Vec<String> = fs::read_dir(Path::new(&out_dir))
        .expect("could not read protos directory")
        .map(|dir_entry| dir_entry.expect("unable to get entry").path().display().to_string())
        .collect();
    for output_file in output_files {
        let mut contents = String::new();
        fs::File::open(&output_file).unwrap().read_to_string(&mut contents).unwrap();
        let mod_name = extract_mod_name(&output_file);
        let new_contents = format!("pub mod {} {{\n{}\n}}", mod_name, contents);

        fs::File::create(&output_file).unwrap().write_all(new_contents.as_bytes()).unwrap();
    }
}

use std::process::Command;

#[test]
fn code_is_formatted() {
    let mut child = Command::new("cargo").args(&["fmt", "--", "--check"]).spawn().unwrap();

    let status = child.wait().unwrap();

    if !status.success() {
        panic!("Please format the code by running `cargo fmt`");
    }
}

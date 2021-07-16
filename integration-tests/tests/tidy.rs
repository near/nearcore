use std::path::PathBuf;
use std::process::Command;

#[test]
fn code_is_formatted() {
    let mut child = Command::new("cargo")
        .current_dir(project_root())
        .args(&["fmt", "--", "--check"])
        .spawn()
        .unwrap();

    let status = child.wait().unwrap();

    if !status.success() {
        panic!("Please format the code by running `cargo fmt`");
    }
}

fn project_root() -> PathBuf {
    let dir = env!("CARGO_MANIFEST_DIR");
    let res = PathBuf::from(dir).parent().unwrap().to_owned();
    assert!(res.join(".github").exists());
    res
}

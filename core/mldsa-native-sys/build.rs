use std::env;
use std::path::PathBuf;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let mldsa = manifest_dir.join("mldsa-native/mldsa");
    let csrc = manifest_dir.join("csrc");
    println!("cargo:rerun-if-changed={}", mldsa.display());
    println!("cargo:rerun-if-changed={}", csrc.display());
    println!("cargo:rerun-if-env-changed=NEAR_MLDSA_NO_NATIVE");

    let arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    let os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    let native = env::var_os("NEAR_MLDSA_NO_NATIVE").is_none()
        && matches!(arch.as_str(), "x86_64" | "aarch64")
        && matches!(os.as_str(), "linux" | "macos");

    let mut build = cc::Build::new();
    build
        .include(&csrc)
        .include(&mldsa)
        .include(mldsa.join("src"))
        .define("MLD_CONFIG_FILE", Some("\"near_mldsa_config.h\""))
        .file(mldsa.join("mldsa_native.c"))
        .std("c90")
        .warnings(true);
    if native {
        build.define("NEAR_MLDSA_NATIVE", None);
        if arch == "x86_64" {
            build.define("NEAR_MLDSA_NATIVE_FIPS202", None);
        }
        build.file(mldsa.join("mldsa_native_asm.S"));
    }
    if arch == "wasm32" {
        // No libc on wasm32: declare memcpy/memset, which compiler_builtins provides.
        build.include(manifest_dir.join("wasm-shim"));
    }
    build.compile("near_mldsa_native");
}

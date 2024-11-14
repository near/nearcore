# FIXME: some of these tests don't work very well on MacOS at the moment. Should fix
# them at earliest convenience :)
# Also in addition to this, the `nextest-integration` test is currently disabled on macos
platform_excludes := if os() == "macos" {
    "--exclude node-runtime --exclude runtime-params-estimator --exclude near-network --exclude estimator-warehouse --exclude integration-tests"
} else if os() == "windows" {
    "--exclude node-runtime --exclude runtime-params-estimator --exclude near-network --exclude estimator-warehouse --exclude integration-tests"
} else {
    ""
}

nightly_flags := "--features nightly,test_features"
public_libraries := "-p near-primitives -p near-crypto -p near-jsonrpc-primitives -p near-chain-configs -p near-primitives-core"

export RUST_BACKTRACE := env("RUST_BACKTRACE", "short")
ci_hack_nextest_profile := if env("CI_HACKS", "0") == "1" { "--profile ci" } else { "" }

# all the tests, as close to CI as possible
test *FLAGS: (test-ci FLAGS) test-extra

# only the tests that are exactly the same as the ones in CI
test-ci *FLAGS: check-cargo-fmt \
                python-style-checks \
                check-cargo-deny \
                check-themis \
                check-cargo-clippy \
                check-non-default \
                check-cargo-udeps \
                (nextest "stable" FLAGS) \
                (nextest "nightly" FLAGS) \
                doctests
# order them with the fastest / most likely to fail checks first
# when changing this, remember to adjust the CI workflow in parallel, as CI runs each of these in a separate job

# tests that are as close to CI as possible, but not exactly the same code
test-extra: check-lychee

# all cargo tests,
# TYPE is one of "stable", "nightly"
nextest TYPE *FLAGS:
    env RUSTFLAGS="-D warnings" \
    cargo nextest run \
        --locked \
        --workspace \
        --cargo-profile dev-release \
        {{ ci_hack_nextest_profile }} \
        {{ platform_excludes }} \
        {{ if TYPE == "nightly" { nightly_flags } \
           else if TYPE == "stable" { "" } \
           else { error("TYPE is neighter 'nightly' nor 'stable'") } }} \
        {{ FLAGS }}

nextest-slow TYPE *FLAGS: (nextest TYPE "--ignore-default-filter -E 'default() + test(/^(.*::slow_test|slow_test)/)'" FLAGS)
nextest-all TYPE *FLAGS: (nextest TYPE "--ignore-default-filter -E 'all()'" FLAGS)

doctests:
    cargo test --doc

# check various build configurations compile as anticipated
check-non-default:
    # Ensure that near-vm-runner always builds without default features enabled
    RUSTFLAGS="-D warnings" \
    cargo check -p near-vm-runner --no-default-features

# check rust formatting
check-cargo-fmt:
    cargo fmt -- --check

# check clippy lints
check-cargo-clippy *FLAGS:
    CARGO_TARGET_DIR="target/clippy" \
    RUSTFLAGS="-D warnings" \
    cargo clippy --all-features --all-targets --locked {{ FLAGS }}

# check cargo deny lints
check-cargo-deny:
    cargo deny --all-features --locked check bans

# themis-based checks
check-themis:
    env CARGO_TARGET_DIR="target/themis" cargo run --locked -p themis

# generate a codecov report for RULE
codecov RULE:
    #!/usr/bin/env bash
    set -euxo pipefail
    # Note: macos seems to not support `source <()` as a way to set environment variables, but
    # this variant seems to work on both linux and macos.
    # TODO: remove the RUSTFLAGS hack, see also https://github.com/rust-lang/cargo/issues/13040
    cargo llvm-cov show-env --export-prefix | grep -v RUSTFLAGS= > env
    source ./env
    export RUSTC_WORKSPACE_WRAPPER="{{ absolute_path("scripts/coverage-wrapper-rustc") }}"
    {{ just_executable() }} {{ RULE }}
    mkdir -p coverage/codecov
    cargo llvm-cov report --profile dev-release --codecov --output-path coverage/codecov/new.json

# generate a codecov report for RULE, CI version
codecov-ci RULE:
    #!/usr/bin/env bash
    set -euxo pipefail
    {{ just_executable() }} codecov "{{ RULE }}"
    pushd target
    tar -c --zstd -f ../coverage/profraw/new.tar.zst *.profraw
    popd
    rm -rf target/*.profraw

# generate a tarball with all the binaries for coverage CI
tar-bins-for-coverage-ci:
    #!/usr/bin/env bash
    find target/dev-release/ \( -name incremental -or -name .fingerprint -or -name out \) -exec rm -rf '{}' \; || true
    find target/dev-release/ -not -executable -delete || true
    find target/dev-release/ -name 'build*script*build*' -delete || true
    tar -c --zstd -f coverage/profraw/binaries/new.tar.zst target/dev-release/

# style checks from python scripts
python-style-checks:
    python3 scripts/check_nightly.py
    python3 scripts/check_pytests.py
    python3 scripts/fix_nightly_feature_flags.py
    ./scripts/formatting --check

install-rustc-nightly:
    rustup toolchain install nightly
    rustup target add wasm32-unknown-unknown --toolchain nightly
    rustup component add rust-src --toolchain nightly

# verify there is no unused dependency specified in a Cargo.toml
check-cargo-udeps: install-rustc-nightly
    env CARGO_TARGET_DIR={{justfile_directory()}}/target/udeps RUSTFLAGS='--cfg=udeps --cap-lints=allow' cargo +nightly udeps

# lychee-based url validity checks
check-lychee:
    # This is not actually run in CI. GITHUB_TOKEN can still be set locally by people who want
    # to reproduce CI behavior in a better way.
    git ls-files | grep 'md$\|mkd$\|html\?$' | xargs lychee {{ if env("GITHUB_TOKEN", "") != "" { "" } else { "-a 429" } }}
    @echo {{ if env("GITHUB_TOKEN", "") != "" { "" } \
             else { "Note: 'Too Many Requests' errors are allowed here but not in CI, set GITHUB_TOKEN to check them" } }}

# check tools/protocol-schema-check/res/protocol_schema.toml
# On MacOS, not all structs are collected by `inventory`. Non-incremental build fixes that.
# See https://github.com/dtolnay/inventory/issues/52.
protocol_schema_env := "CARGO_TARGET_DIR=" + justfile_directory() + "/target/schema-check RUSTC_BOOTSTRAP=1 RUSTFLAGS='--cfg enable_const_type_id' CARGO_INCREMENTAL=0"
check-protocol-schema:
    # Below, we *should* have been used `cargo +nightly ...` instead of
    # `RUSTC_BOOTSTRAP=1`. However, the env var appears to be more stable.
    # `nightly` builds are updated daily and may be broken sometimes, e.g.
    # https://github.com/rust-lang/rust/issues/130769.
    #
    # If there is an issue with the env var, fall back to `cargo +nightly ...`.
    env {{protocol_schema_env}} cargo test -p protocol-schema-check --profile dev-artifacts
    env {{protocol_schema_env}} cargo run -p protocol-schema-check --profile dev-artifacts

check_build_public_libraries:
    cargo check {{public_libraries}}

# FIXME: some of these tests don't work very well on MacOS at the moment. Should fix
# them at earliest convenience :)
# Also in addition to this, the `nextest-integration` test is currently disabled on macos
with_macos_excludes := if os() == "macos" {
    "--exclude node-runtime --exclude runtime-params-estimator --exclude near-network --exclude estimator-warehouse"
} else {
    ""
}
nightly_flags := "--features nightly,test_features"

export RUST_BACKTRACE := env("RUST_BACKTRACE", "short")
ci_hack_nextest_profile := if env("CI_HACKS", "0") == "1" { "--profile ci" } else { "" }

# all the tests, as close to CI as possible
test *FLAGS: (test-ci FLAGS) test-extra

# only the tests that are exactly the same as the ones in CI
test-ci *FLAGS: (nextest "stable" FLAGS) (nextest "nightly" FLAGS) check-non-default check-udeps python-style-checks

# tests that are as close to CI as possible, but not exactly the same code
test-extra: check-lychee

# all cargo tests, TYPE is "stable" or "nightly"
nextest TYPE *FLAGS: (nextest-unit TYPE FLAGS) (nextest-integration TYPE FLAGS)

# cargo unit tests, TYPE is "stable" or "nightly"
nextest-unit TYPE *FLAGS:
    cargo nextest run \
        --locked \
        --workspace \
        --exclude integration-tests \
        --cargo-profile dev-release \
        {{ ci_hack_nextest_profile }} \
        {{ with_macos_excludes }} \
        {{ if TYPE == "nightly" { nightly_flags } \
           else if TYPE == "stable" { "" } \
           else { error("TYPE is neighter 'nightly' nor 'stable'") } }} \
        {{ FLAGS }}

# cargo integration tests, TYPE is "stable" or "nightly"
[linux]
nextest-integration TYPE *FLAGS:
    cargo nextest run \
        --locked \
        --package integration-tests \
        --cargo-profile dev-release \
        {{ ci_hack_nextest_profile }} \
        {{ if TYPE == "nightly" { nightly_flags } \
           else if TYPE == "stable" { "" } \
           else { error("TYPE is neither 'nightly' nor 'stable'") } }} \
        {{ FLAGS }}
# Note: when re-enabling this on macos, ci.yml will need to be adjusted to report code coverage again
[macos]
nextest-integration TYPE *FLAGS:
    @echo "Nextest integration tests are currently disabled on macos!"

# Check various build configurations that aren’t exercised by a plain `cargo nextest` work as
# anticipated.
check-non-default:
    # Ensure that near-vm-runner always builds without default features enabled
    cargo check -p near-vm-runner --no-default-features

# generate a codecov report for RULE
codecov RULE:
    #!/usr/bin/env bash
    set -euxo pipefail
    # Note: macos seems to not support `source <()` as a way to set environment variables, but
    # this variant seems to work on both linux and macos.
    # TODO: remove the RUSTFLAGS hack, see also https://github.com/rust-lang/cargo/issues/13040
    cargo llvm-cov show-env --export-prefix | grep -v RUSTFLAGS > env
    source ./env
    export RUSTC_WORKSPACE_WRAPPER="{{ absolute_path("scripts/rustc-coverage-wrapper.sh") }}"
    {{ just_executable() }} {{ RULE }}
    cargo llvm-cov report --profile dev-release --codecov --output-path codecov.json
    # See https://github.com/taiki-e/cargo-llvm-cov/issues/292
    find target -name '*.profraw' -delete

# style checks from python scripts
python-style-checks:
    python3 scripts/check_nightly.py
    python3 scripts/check_pytests.py
    python3 scripts/fix_nightly_feature_flags.py
    ./scripts/formatting --check

check-udeps:
    rustup toolchain install nightly
    rustup target add wasm32-unknown-unknown --toolchain nightly
    env CARGO_TARGET_DIR={{justfile_directory()}}/target/udeps RUSTFLAGS='--cfg=udeps --cap-lints=allow' cargo +nightly udeps

# lychee-based url validity checks
check-lychee:
    # This is not actually run in CI. GITHUB_TOKEN can still be set locally by people who want
    # to reproduce CI behavior in a better way.
    git ls-files | grep 'md$\|mkd$\|html\?$' | xargs lychee {{ if env("GITHUB_TOKEN", "") != "" { "" } else { "-a 429" } }}
    @echo {{ if env("GITHUB_TOKEN", "") != "" { "" } \
             else { "Note: 'Too Many Requests' errors are allowed here but not in CI, set GITHUB_TOKEN to check them" } }}

# build target/rpc_errors_schema.json
build-rpc-errors-schema:
    rm -f target/rpc_errors_schema.json
    cargo check -p near-jsonrpc --features dump_errors_schema

# update chain/jsonrpc/res/rpc_errors_schema.json
update-rpc-errors-schema: build-rpc-errors-schema
    cp target/rpc_errors_schema.json chain/jsonrpc/res/rpc_errors_schema.json

# check chain/jsonrpc/res/rpc_errors_schema.json
check-rpc-errors-schema: build-rpc-errors-schema
    diff target/rpc_errors_schema.json chain/jsonrpc/res/rpc_errors_schema.json

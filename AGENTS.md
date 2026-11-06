# nearcore

## Rust

- NEVER use fully qualified paths (e.g., `std::collections::HashMap::new()`). Always add a `use` declaration and reference the type directly. This applies to all Rust code — production, tests, and examples.
- Don't capitalize string literal messages in logs, errors, `Option::expect`, `panic!`, etc.
- To borrow from an `Arc<T>` / `Arc<dyn Trait>`, use `arc.as_ref()`, not `&*arc`. Both compile to the same thing; `as_ref()` is cleaner.

## Pull Requests

Always use the `/pr` skill to create pull requests. It handles branch creation,
title format, description, and confirmation. Check memory for description
formatting preferences before creating PRs.

## Protocol Schema Check

- Do NOT run the protocol schema check unless explicitly asked. It is expensive.
- When asked to update the schema, first read `tools/protocol-schema-check/README.md`, then follow its instructions exactly. Do NOT guess the command.

## Testing

- To run specific tests, use `cargo test --package {package} --features test_features -- {path::to::test} --exact --show-output`. Include `--features test_features` only if the package defines it. Use `--features nightly` if needed.
- For SPICE tests use `--features protocol_feature_spice,nightly,test_features`. Without it, they are skipped as ignored.

## Formatting

- For Rust run `cargo fmt` after making changes. It's cheap.
- For Python run `scripts/formatting --fix`.

## Clippy

- Only run clippy prior to making a commit. It is somewhat expensive.
- Run exact command `RUSTFLAGS="-D warnings" cargo clippy --all-features --all-targets`
- Never run clippy on individual packages

## Reference Docs

Before working in these areas, read the relevant documentation first. Keep
these documents up to date when making changes to the corresponding code.

- **Dynamic resharding**: `docs/architecture/how/dynamic_resharding.md`
- **Sync**: `docs/architecture/how/sync.md`
- **Test-loop tests**: `test-loop-tests/README.md` for the framework, and read examples in `test-loop-tests/src/examples/` for latest API patterns and best practices.
- **Chain**: `chain/chain/AGENTS.md` — read before working in `chain/chain/`.
- **Runtime**: `runtime/runtime/AGENTS.md` — read before working in `runtime/runtime/`.

## OpenAPI Spec

- Do NOT update the OpenAPI spec unless explicitly asked.
- When asked, follow the `openapi-spec` recipe in the `Justfile`.

## Protocol Model

- Do NOT edit files under `protocol-model/` in code PRs, even when a change makes a reference there stale. The model describes one release commit (see the header of `protocol-model/README.md`). It is regenerated separately for each release by following `protocol-model/REGENERATION.md`.

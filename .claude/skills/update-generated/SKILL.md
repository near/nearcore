---
name: update-generated
description: Regenerate the checked-in artifacts (OpenAPI spec, insta snapshots, protocol schema). Use when the user asks to update generated files, refresh snapshots, bump the OpenAPI spec, or fix a protocol-schema-check failure.
allowed-tools: Bash, AskUserQuestion
---

Update the generated artifacts tracked in this repo. Run each step from the repo root, in order. Stop and report on any failure.

## 1. OpenAPI spec

Generate into a tmp file first; only overwrite the tracked file if the generator succeeded.

```
tmp=$(mktemp)
cargo run -p near-jsonrpc-openapi-spec > "$tmp"
```

- If `cargo run` exits non-zero, delete `$tmp` and stop with the error.
- On success, move `$tmp` to `chain/jsonrpc/openapi/openapi.json`.

## 2. Insta snapshots

```
cargo insta test --accept -p near-parameters -p near-primitives
```

Stop on failure.

## 3. Protocol schema

Run the check and capture the exit code without aborting:

```
set +e
cargo run -p protocol-schema-check
schema_status=$?
set -e
```

- **0** — schema is up-to-date, nothing to do.
- **1** — schema drift. The regenerated schema is at `${CARGO_TARGET_DIR:-target}/protocol_schema.toml`. Before copying:
  1. Show the user the diff against the tracked file:
     ```
     git diff --no-index tools/protocol-schema-check/res/protocol_schema.toml \
       "${CARGO_TARGET_DIR:-target}/protocol_schema.toml"
     ```
  2. Use AskUserQuestion with options "Accept and copy" / "Abort" to confirm.
  3. Only on confirmation, copy the regenerated file over `tools/protocol-schema-check/res/protocol_schema.toml`.
- **Any other code** — report the failure and stop; do not copy.

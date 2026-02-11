# nearcore

## Protocol Schema Check
- Do NOT run the protocol schema check unless explicitly asked. It is expensive.
- When asked to update the schema, follow the instructions in `tools/protocol-schema-check/README.md` exactly.

## Testing
- When running tests, use `--features test_features`.

## Formatting
- Run `cargo fmt` after making changes. It's cheap.

## Clippy
- Only run clippy prior to making a commit. It is somewhat expensive.

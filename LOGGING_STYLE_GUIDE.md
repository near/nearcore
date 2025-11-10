# Nearcore Logging Style Guide

Quick reference for writing standardized logs in the nearcore codebase.

## ✅ DO: Use Structured Fields

```rust
// Good - structured fields with named parameters
tracing::info!(%block_height, ?block_hash, "processing block");
tracing::warn!(%peer_id, ?error, "peer connection failed");
tracing::error!(%shard_id, %chunk_count, "shard has too many pending chunks");
```

## ❌ DON'T: Use Format Strings

```rust
// Bad - format strings
tracing::info!("Processing block {}, hash: {:?}", block_height, block_hash);
tracing::warn!("Peer {} connection failed: {}", peer_id, error);
```

## Field Prefixes

- **`%field`** - Use for types implementing `Display` (numbers, strings, most primitives)
- **`?field`** - Use for types implementing `Debug` (errors, hashes, complex types)
- **`field = expr`** - Use for computed values or method calls

## Examples

### Simple Values
```rust
tracing::info!(%height, %shard_id, "validator info");
tracing::debug!(?hash, ?signature, "verifying signature");
```

### Nested Field Access
```rust
// Use named fields for nested access
tracing::debug!(
    height = %msg.block.header.height,
    signer = %tx.transaction.signer_id,
    "block and transaction info"
);
```

### Method Calls
```rust
// Use named fields for method calls
tracing::warn!(
    node_id = %self.config.node_id(),
    count = %shards.len(),
    "processing shards"
);
```

### With Targets
```rust
tracing::info!(target: "network", %peer_id, "peer connected");
tracing::warn!(target: "chain", %block_height, "block validation failed");
```

## Message Style

1. **Lowercase first letter**: `"processing block"` not `"Processing block"`
2. **No trailing punctuation**: `"connection failed"` not `"connection failed."`
3. **Descriptive but concise**: Data in fields, context in message
4. **No mid-sentence periods**: Use commas for complex sentences

## Complete Examples

### ✅ Good
```rust
tracing::info!(target: "chain", %height, ?hash, "applying block");
tracing::warn!(target: "network", %peer_id, ?error, "peer disconnected");
tracing::error!(target: "runtime", %account_id, ?reason, "transaction failed");
tracing::debug!(target: "store", %key, value = %data.len(), "storing data");
```

### ❌ Bad
```rust
tracing::info!("Applying block {}, hash: {:?}.", height, hash);
tracing::warn!(target: "network", "Peer {} disconnected: {}", peer_id, error);
error!("Transaction failed for account {}", account_id);
tracing::debug!("Storing {} bytes at key {}", data.len(), key);
```

## Enforcement

- All changes must pass `cargo clippy --all-features --all-targets`
- Use `tracing::` qualifier (not importing macros directly in most cases)
- Code review will check for compliance with this guide

## Reference

See `LOGGING_STANDARDIZATION_SUMMARY.md` for full details and rationale.

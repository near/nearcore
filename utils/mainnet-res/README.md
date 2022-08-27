## Core Resource Files

Stores resource data which is part of the protocol stable enough to be moved outside of the code.

### `mainnet_genesis.json`

Stores genesis of mainnet.

### `mainnet_restored_receipts.json`

Stores receipts restored after the fix of applying chunks. See [#4248](https://github.com/near/nearcore/pull/4248) for more details.

### `storage_usage_delta.json`

Stores difference of storage usage applied to mainnet after observed bug related to delete key action. See [#3824](https://github.com/near/nearcore/issues/3824) for more details.

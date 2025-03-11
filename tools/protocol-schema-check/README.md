# Protocol Schema Check Tool

## Overview

This tool verifies that the protocol schema remains consistent, preventing accidental changes that could lead to critical issues.

## Purpose

The Protocol Schema Check Tool helps maintain the integrity of the NEAR protocol by:

1. Ensuring backward compatibility
2. Preventing unintended modifications to structures stored in the database or involved in the protocol
3. Safeguarding against breaks in block replayability
4. Avoiding obscure database read errors

## Background

For context on why this tool is necessary, refer to [this pull request](https://github.com/near/nearcore/pull/11569) and the following ones.

## Usage

Run the tool locally using:
`RUSTFLAGS="--cfg enable_const_type_id" cargo +nightly run -p protocol-schema-check`

On MacOS, prepend this with `CARGO_INCREMENTAL=0` to avoid a [known issue](https://github.com/dtolnay/inventory/issues/52) with incremental compilation.

## What To Do If It Fails

If the tool fails, it indicates that you've made changes to the protocol schema. Follow these steps:

1. Review all impacted structures carefully.
2. Confirm that you intended to modify these structures.
3. Take appropriate action based on the nature of the change:

   a. For structures stored only locally in the database:
      - Implement a database migration for the affected structure.
   
        Otherwise, newly deployed nodes have a risk to not recognize the old data format and fail to start. 

   b. For structures involved in the protocol or network communication:
      - Add a new version to the structure that maintains backward compatibility with the previous version.
   
    Otherwise, old and new nodes have a risk to not recognize the messaging format of each other and fail to communicate.

4. Copy the newly generated file to `res/protocol_schema.toml` to reflect your changes.

Note that the tool can provide a false positive, including the cases when
- only the field names have changed
- type name changed but serialization remained the same
- Borsh-compatible changes were made, such as adding a new enum variant.

This is done to strengthen the check and ensure that no changes are missed.
The real failure case was when we accidentally swapped the enum variants. 
If both are units, serialization don't break, but the impacted logic will be broken.
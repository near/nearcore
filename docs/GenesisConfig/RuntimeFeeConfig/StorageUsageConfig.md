# StorageUsageConfig

Describes how the storage that an account uses is counted for storage staking.

## num_bytes_account

_type: u64_
Number of bytes counted for the account record, including rounding up for the account ID.
The value is 100.

## num_extra_bytes_record

_type: u64_
Number of bytes counted for each key/value record, such as a contract data entry or an access key, in addition to the bytes of its key and value.
The value is 40.

The balance that an account must hold for each byte of storage is `storage_amount_per_byte` in [RuntimeConfig](../RuntimeConfig.md).
It is not part of this struct.

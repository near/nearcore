# Experimental: State Sync from External Storage

## Purpose

[State Sync](../architecture/how/sync.md#step-2-state-sync-normal-node) is being
reworked.

A new version is available for experimental use. This version gets state parts
from external storage. The following kinds of external storage are supported:
* Local filesystem
* Google Cloud Storage
* Amazon S3

A new version of decentralized state sync is work in progress.

## How-to

neard release `1.36.0-rc.1` adds an experimental option to sync state from
external storage.

The reference `config.json` file by default enables state sync from external
storage and configures it to get state parts from a location managed by Pagoda.

Note: to obtain the reference configuration file, download it by running the
following command:
```shell
neard init --chain-id <testnet or mainnet> --download-config --download-genesis
```

To make use of your own external storage, see the corresponding [how-to](state_sync_dump.md).

### Google Cloud Storage

To enable Google Cloud Storage as your external storage, add this to your
`config.json` file:

```json
"state_sync_enabled": true,
"state_sync": {
  "sync": {
    "ExternalStorage": {
      "location": {
        "GCS": {
          "bucket": "my-gcs-bucket",
        }
      }
    }
  }
}
```

Then run the `neard` binary and it will access GCS anonymously:
```shell
./neard run
```

### Amazon S3

To enable Amazon S3 as your external storage, add this to your `config.json`
file:

```json
"state_sync_enabled": true,
"state_sync": {
  "sync": {
    "ExternalStorage": {
      "location": {
        "S3": {
          "bucket": "my-aws-bucket",
          "region": "my-aws-region"
        }
      }
    }
  }
}
```

Then run the `neard` binary and it will access S3 anonymously:
```shell
./neard run
```

## Sync from a local filesystem

To enable, add this to your `config.json` file:

```json
"state_sync_enabled": true,
"state_sync": {
  "sync": {
    "ExternalStorage": {
      "location": {
        "Filesystem": {
          "root_dir": "/tmp/state-parts"
        }
      }
    }
  }
}
```

Then run the `neard` binary:
```shell
./neard run
```

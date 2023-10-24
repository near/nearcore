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

To create your own State dumps to external storage, see the corresponding [how-to](state_sync_dump.md).

### Google Cloud Storage

To enable Google Cloud Storage as your external storage, add the following to
your `config.json` file:

```json
"state_sync_enabled": true,
"state_sync": {
  "sync": {
    "ExternalStorage": {
      "location": {
        "GCS": {
          "bucket": "my-gcs-bucket",
        }
      },
      "num_concurrent_requests": 4,
      "num_concurrent_requests_during_catchup": 4,
    }
  }
},
"consensus": {          
  "state_sync_timeout": {
    "secs": 30,
    "nanos": 0
  }
}
```

Then run the `neard` binary and it will access GCS anonymously:
```shell
./neard run
```

#### Extra Options

The options suggested above will most likely work fine.

* `num_concurrent_requests` determines the number of state parts across all
shards that can be downloaded in parallel during state sync.
* `num_concurrent_requests_during_catchup` determines the number of state parts 
across all shards that can be downloaded in parallel during catchup. Generally,
this number should not be higher than `num_concurrent_requests`. Keep it
reasonably low to allow the node to process chunks of other shards.
* `consensus.state_sync_timeout` determines the max duration of an attempt to download a
state part. Setting it too low may cause too many unsuccessful attempts.

### Amazon S3

To enable Amazon S3 as your external storage, add the following to your
`config.json` file.
You may add the other mentioned options too.

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
},
```

Then run the `neard` binary and it will access Amazon S3 anonymously:
```shell
./neard run
```

## Sync from a local filesystem

To enable, add the following to your `config.json` file.
You may add the other mentioned options too.

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

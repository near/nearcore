# How to use

## Run the benchmark

- compile the `neard` with the `tx_generator` feature

```
cargo build --release --features tx_generator
```

- create a link to the release binary of neard in the `benchmarks/transactions-generator`

```
cd benchmarks/transactions-generator && ln -s ../../target/release/neard ./
```

- build `synth-bm`

```
cd benchmarks/synth-bm && cargo build --release
```

- run the thing

```
just do-it
```

The last command will init a single-node network, apply customizations to the config, create the accounts and run the
benchmark. All of it.
After running that command once, you can repeat the benchmark without re-executing the setup steps by:

```
just run-localnet
```

For a more fine-grained control feel free to dive into the `justfile` for the commands executing the intermediate steps.
By default benchmark is running the 20000 tps load for the duration of 2 minutes.
To run it with a different schedule one needs to modify the contents of "tx-generator-settings.json" file.

### Tweak the load generation parameters
The load generation parameters are expected as a part of the `neard` `config.json`.
The node name is "tx_generator" and is optional. 
In case the node is missing the error message is printed out and the `neard` continues running as usually.
The node currently consists of the path to  "schedule" and "controller" sections
```json
{
	"tx_generator": {
		"accounts_path": "{{near_accounts_path}}",
		"schedule": [
			{ "tps": 1000, "duration_s": 60 },
			{ "tps": 2000, "duration_s": 60 }
		],
		"controller": {
			"target_block_production_time_s": 1.4,
			"bps_filter_window_length": 64,
			"gain_proportional": 20,
			"gain_integral": 0.0,
			"gain_derivative": 0.0
		}
	}
}

```
The "schedule" section is required and defines the load applying schedule. 
After the schedule completes the `neard` will exit in case "controller" is not defined, or else will run the control loop defined by the parameters indefinitely.
The last scheduled TPS is than used as a starting point and the loop adjusts TPS to keep the target block production time at a desired value.
Note that the resulting production time currently tends to be slightly under the target value.

## What you should see

- observe the load reported in the logs

```
...
2025-02-12T16:42:37.351118Z  INFO stats: #    1584 6nxZon12xBTmARmUm3ngtgaA2K7V9dX1J13EtPZ2kEhe Validator | 1 validator 0 peers ⬇ 0 B/s ⬆ 0 B/s 1.60 bps 131 Tgas/s CPU: 70%, Mem: 2.98 GB
...
2025-02-18T13:04:29.790195Z  INFO transaction-generator: total="Stats { pool_accepted: 158247, pool_rejected: 237, processed: 155338, failed:0 }"
2025-02-18T13:04:29.790208Z  INFO transaction-generator: diff="Stats { pool_accepted: 6049, pool_rejected: 0, processed: 6439, failed: 0 } rate=7114.914800849541"
...
```

- see the number of processed transactions in the metrics

```
$ curl http://localhost:4040/metrics -s | rg "near_transaction_processed_successfully_total (\\d+)"
near_transaction_processed_successfully_total 852883
```

## Gotchas
- by default the `.near` directory is created right in the script directory. That may be suboptimal as the prod environment mounts the `.near` on a separate drive. To change that one needs to modify the `justfile` `near_localnet_home` variable. 

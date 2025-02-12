# How to use

- prepare the localnet
```
just init_localnet
just run_localnet
```
- in another console create the subaccounts
```
just create_sub_accounts
```
- modify the config (`.near/config.json`) to enable the transactions generator
```json
  "tx_generator": {
    "tps": 200,
    "volume": 5000,
    "accounts_path": "/home/slavas/proj/nearcore/benchmarks/transactions-generator/user-data"
  },

```
- restart the neard
```
killall neard
just run_localnet
```
- observe the load reported in the logs
```
2025-02-12T16:42:37.351118Z  INFO stats: #    1584 6nxZon12xBTmARmUm3ngtgaA2K7V9dX1J13EtPZ2kEhe Validator | 1 validator 0 peers ⬇ 0 B/s ⬆ 0 B/s 1.60 bps 131 Tgas/s CPU: 70%, Mem: 2.98 GB
```

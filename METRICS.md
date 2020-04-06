# Near Metrics Instructions

## Setup Prometheus

Download a copy of Prometheus from https://prometheus.io/ selecting the OS and extracting the relevant files.

Alternatively, you can also use your favorite package manager. For example, in Arch linux:

`$ sudo pacman -S prometheus`

Or on Mac OS:

`$ brew install prometheus`

## Running

First run Prometheus, using the `prometheus-near.yml` config file located in the `./conf` directory of this repository:

`$ prometheus --config.file=$PATH_TO_REPO/conf/prometheus-near.yml`

Build the NEARCore node:

`$ cargo build -p neard`

Generate the initial configuration (genesis state, validator keys and node keys):

`$ $PATH_TO_REPO/target/debug/near init`
`$ $PATH_TO_REPO/target/debug/near testnet`

Then run a NEAR node0:

`$ $PATH_TO_REPO/target/debug/near --home ~/.near/node0 run`

Extract the server details from the NEAR boot node, it will look similar to:

`ed25519:BT6VhbcNeuq9KXUYpELanWMnSho2LFC5EiGP2Wx7S7gj@0.0.0.0:24567`

Run nodes 1 to 3, replacing `--boot-nodes` with the details output from `node0`
and X with 1, 2 then 3 (executing the addition symbols).

```
target/release/near --home ~/.near/nodeX run \
                    --boot-nodes "ed25519:BT6VhbcNeuq9KXUYpELanWMnSho2LFC5EiGP2Wx7S7gj@0.0.0.0:24567" \
                    --network-addr 0.0.0.0:(24567 + X) --rpc-addr 0.0.0.0:(3031 + X)
```

## Setup and run Grafana

Download a copy of Grafana from https://grafana.com/ selecting the OS and extracting the relevant files.

Alternatively, you can also use your favorite package manager. For example, in Arch linux:

`$ sudo pacman -S grafana`

Start grafana as a service:

`$ systemctl start grafana`

Browse to the following address: `localhost:3000` (default username/password is `admin/admin`):

Click `Create your first datasource` -> `Prometheus` and update the following fields:

`Name`: Prometheus
`URL`: http://localhost:9090
`Scrape interval`: 5 seconds
`Query timeout`: 5 seconds

Click `Save & Test` then click `back`.

After this, click the `+` -> `Import` -> `Upload .json file`

Select the configuration file at `$PATH_TO_REPO/conf/grafana-dashboard.json` and click `Load`

You should then be able to view a dashboard with a few metrics!

## Details

Near has a HTTP endpoint for Prometheus using the HTTP Server. The default port
for the server is `3030` and the endpoint is `/metrics`. Running a node locally raw
Prometheus data can be reached at `localhost:3030/metrics`.

Prometheus can be connected to on `localhost:9090`. Using this data can me
manipulated into graphs or more useful forms.

## Add an additional metrics

Define the metrics in the crate in a file named `metrics.rs`. Each new metric should
be defined as a constant reference within a `lazy_static` macro.

It is recommended to use the safe `near-metrics` crate operations when interacting
with the metrics to handle the case where instantiation fails.

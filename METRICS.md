# Near Metrics Instructions
## Setup
Download a copy of Promtheus from https://prometheus.io/ selecting the OS and
extracting the files.

## Running
First run Prometheus binary downloaded from the website listed above, using the config
located in the `conf` directory of this repository `prometheus-near.yml`.

`./prometheus config.file=$PATH_TO_REPO/conf/prometheus-near.yml`

Then run the near node.

`cargo build --release --all`

`./target/release/near run`

## Details
Near has a HTTP endpoint for Prometheus using the HTTP Server. The default port
for the server is `3030` and the endpoint is `/metrics`. Running a node locally raw
Prometheus data can be reached at `localhost:3030/metrics`.

Prometheus can be connected to on `localhost:9090`. Using this data can me
manipulated into graphs or more useful forms.

## TODO: Details about running grafana
Grafana makes pretty graphs :)

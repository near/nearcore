# Near Metrics Instructions

## Setup Prometheus

Download a copy of Prometheus from https://prometheus.io/ selecting the OS and extracting the relevant files.

Alternatively, you can also use your favorite package manager. For example, in Arch linux:

`$ sudo pacman -S prometheus`

## Setup Grafana

Download a copy of Grafana from https://grafana.com/ selecting the OS and extracting the relevant files.

Alternatively, you can also use your favorite package manager. For example, in Arch linux:

`$ sudo pacman -S grafana`



## Running
First run Prometheus, using the `prometheus-near.yml` config file located in the `./conf` directory of this repository:

`$ prometheus config.file=$PATH_TO_REPO/conf/prometheus-near.yml`

Build the NEARCore node:

`$ cargo build --release --all`

Generate the initial configuration (genesis state, validator keys and node keys):

`$ $PATH_TO_REPO/target/debug/near init`

Then run a NEAR node:

`$ $PATH_TO_REPO/target/debug/near run`

## Details
Near has a HTTP endpoint for Prometheus using the HTTP Server. The default port
for the server is `3030` and the endpoint is `/metrics`. Running a node locally raw
Prometheus data can be reached at `localhost:3030/metrics`.

Prometheus can be connected to on `localhost:9090`. Using this data can me
manipulated into graphs or more useful forms.

## TODO: Details about running grafana
Grafana makes pretty graphs :)

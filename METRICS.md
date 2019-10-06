# Near Metrics Instructions

## Setup Prometheus

Download a copy of Prometheus from https://prometheus.io/ selecting the OS and extracting the relevant files.

Alternatively, you can also use your favorite package manager. For example, in Arch linux:

`$ sudo pacman -S prometheus`

## Running

First run Prometheus, using the `prometheus-near.yml` config file located in the `./conf` directory of this repository:

`$ prometheus config.file=$PATH_TO_REPO/conf/prometheus-near.yml`

Build the NEARCore node:

`$ cargo build --release --all`

Generate the initial configuration (genesis state, validator keys and node keys):

`$ $PATH_TO_REPO/target/debug/near init`

Then run a NEAR node:

`$ $PATH_TO_REPO/target/debug/near run`

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

## TODO: Details about running grafana
Grafana makes pretty graphs :)

# Nightly Testing Suite

This is an infrastructure of nearcore end-to-end tests which is running in an
endless loop on our CI.

## Setup

### Dependencies

This testing suite is implemented in Python, so you will need to have Python
installed (Python 3.8 is confirmed to work fine).

Besides basic Python installation, extra Python packages are required, so it is
recommended to use python-virtualenv, or pyenv, or similar projects to install
those requirements:

```
$ cd /path-to-nearcore/
$ pip install -r ./nightly/requirements.txt -r ./pytest/requirements.txt
```

### Nearcore

The testing suite expects `nearcore` to be built before you run the suite, so
make sure that everything is built:

```
$ cd /path-to-nearcore/
$ cargo build --workspace --tests
```

## Run

### a single test

NOTE: Don't forget to activate your Python environment before running this.

```
$ cd /path-to-nearcore/pytest/
$ python tests/sanity/block_production.py
```

### the whole suite

NOTE: Don't forget to activate your Python environment before running this.

```
$ cd /path-to-nearcore/nightly/
$ python nightly.py run nightly.txt /tmp/nearcore-testruns/$(git rev-parse HEAD)_$(date +%y%m%d_%H%M%S)
```

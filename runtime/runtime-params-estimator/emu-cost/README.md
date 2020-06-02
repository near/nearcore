# Exact gas price estimator

## Theory of operations

 Operation execution cost (aka gas cost) is computed basing on the number of userland x86 instructions required to perform the
particular operation in current NEAR runtime implementation. To compute this cost, we use instrumented QEMU binary
translating engine to execute required operations in the userland Linux simulator.
Thus, to measure the execution cost we have to compile NEAR runtime benchmark for Linux, execute the benchmark under
instrumented QEMU running in Docker, and count how many x86 instructions are executed between start and end of execution.

 Instrumentation of QEMU is implemented in the following manner. We install instrumentation callback which conditionally increments
the instruction counter on every instruction during translation by QEMU's JIT, TCG. We activate counting when specific Linux syscall
(currently, 0 aka sys_read) is executed with the certain arguments (file descriptor argument == 0xcafebabe or 0xcafebabf).
On start event we clear instruction counter, on stop event we stop counting and return counted instructions into the buffer provided
to read syscall. As result, NEAR benchmark will know the exact instruction counter passeed between two moments and this value
is the pure function of Docker image used, Rust compiler version and the NEAR implementation and is fully reproducible.

## Usage

We build and run the cost estimator in the Docker container to make sure config is fully reproducible.
Please make sure that Docker is given at least 4G of RAM, as running under emulator is rather resouce consuming.
First fetch appropriate base image, with `docker pull rust`.
Then create a Docker image with `build.sh`, it will create a Docker image with additional build deps.

Set `HOST_DIR` environment variable to local folder where relevant sources are present.
It will be mounted under `/host` in the Docker container.

Start container and build estimator with:

    host> ./run.sh
    docker> cd /host/nearcore/runtime/runtime-params-estimator
    docker> cargo run --release --package neard --bin neard -- --home /tmp/data init --chain-id= --test-seed=alice.near --account-id=test.near --fast
    docker> cargo run --release --package genesis-populate --bin genesis-populate -- --additional-accounts-num=200000 --home /tmp/data
    docker> cargo build --release --package runtime-params-estimator

Now start the estimator under QEMU with the counter plugin enabled (note, that Rust compiler produces SSE4, so specify recent CPU):

    docker> ./emu-cost/counter_plugin/qemu-x86_64 -cpu Westmere-v1 -plugin file=./emu-cost/counter_plugin/libcounter.so ../../target/release/runtime-params-estimator --home /tmp/data --accounts-num 20000 --iters 1 --warmup-iters 1

Note that it may take some time, as we execute instrumented code under the binary translator.

## Optional: re-building QEMU and the instruction counter plugin

We ship prebuilt QEMU and TCG instruction counter plugin, so in many cases one doesn't have to build it.
However, in case you still want to build it - use the following steps.

Important: we build QEMU and the TCG plugin inside the container, so execute following commands inside Docker.
Set environment variable HOST_DIR (on the host) to location where both QEMU and nearcore sourcecode is checked
out, it will be mounted as `/host` inside the Docker container.
Start container with:

    ./run.sh

To build QEMU use:

    cd /host/qemu
    ./configure --disable-system --enable-user --enable-plugins  --prefix=/host/qemu-linux --target-list=x86_64-linux-user
    make && make install

Then build and test the QEMU's JIT plugin:

    cd /host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin
    make QEMU_DIR=/host/qemu
    make test

To execute commands in already running container first find its id with:

    > docker ps

    CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                  NAMES
    e9dcb52cc91b        ubuntu-emu          "/bin/bash"   2 hours ago         Up 2 hours          0.0.0.0:5000->22/tcp   reverent_carson

and the use container ID for `docker exec` command, like:

    docker exec -it e9dcb52cc91b /host/qemu-linux/bin/qemu-x86_64 -d plugin -plugin file=/host/qemu-linux/plugins/libcounter.so /host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/test_binary



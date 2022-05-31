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
to read syscall. As result, NEAR benchmark will know the exact instruction counter passed between two moments and this value
is the pure function of Docker image used, Rust compiler version and the NEAR implementation and is fully reproducible.

## Usage

We build and run the cost estimator in the Docker container to make sure config is fully reproducible.
Please make sure that Docker is given at least 4G of RAM, as running under emulator is rather resource consuming.
Note that for Mac the limit is configured in the desktop client, and default value most likely will be very low.

First fetch appropriate base image, with `docker pull rust`.
Then create a Docker image with `build.sh`, it will create a Docker image with additional build deps.

Set `HOST_DIR` environment variable to local folder where relevant sources are present.
It will be mounted under `/host` in the Docker container.

Start container and build estimator with:

    host> ./run.sh
    docker> cd /host/nearcore
    docker> cd /host/nearcore/runtime/runtime-params-estimator
    docker> pushd ./test-contract && ./build.sh && popd
    docker> cargo build --release --package runtime-params-estimator --features required

Now start the estimator under QEMU with the counter plugin enabled (note, that Rust compiler produces SSE4, so specify recent CPU):

    docker> ./emu-cost/counter_plugin/qemu-x86_64 -cpu Westmere-v1 -plugin file=./emu-cost/counter_plugin/libcounter.so \
         ../../target/release/runtime-params-estimator --accounts-num 20000 --additional-accounts-num 200000 --iters 1 --warmup-iters 1

### Notes

* Estimation may take some time, as we execute instrumented code under the binary translator.

* You may observe tangible differences between instructions number got by `params-estimator` and the actual number of instructions executed by production nodes.
  This is explained by the LTO (Link Time Optimization) which is disabled by default for release builds to reduce compilation time.
  To get better results, enable LTO via environment variable:

      CARGO_PROFILE_RELEASE_LTO=fat
      CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
      export CARGO_PROFILE_RELEASE_LTO CARGO_PROFILE_RELEASE_CODEGEN_UNITS
  
  See [#4678](https://github.com/near/nearcore/issues/4678) for more details.
  
* You also may observe slight differences in different launches, because number of instructions operating with disk cache is not fully determined, as well as weight of RocksDB operations. 
  To improve estimation, you can launch it several times and take the worst result.

## IO cost calibration

We need to calibrate IO operations cost to instruction counts. Technically instruction count and IO costs are orthogonal,
however, as we measure our gas in instructions, we have to compute abstract scaling coefficients binding
the number of bytes read/written in IO to instructions executed.
We do that by computing following operation:

    ./emu-cost/counter_plugin/qemu-x86_64  -d plugin -cpu Westmere-v1 -plugin file=./emu-cost/counter_plugin/libcounter.so \
        ../../target/release/genesis-populate --home /tmp/data --additional-accounts-num <NUM_ACCOUNTS>

and checking how much data to be read/written depending on number of create accounts.
Then we could figure out:
   * 1 account creation cost in instructions
   * 1 account creation cost in bytes read and written
For example, experiments performed in mid Oct 2020 shown the following numbers:
10M accounts:
    * 6_817_684_914_212 instructions executed
    * 168_398_590_013 bytes read
    * 48_486_537_178 bytes written

Thus 1 account approximately costs:
    * 681_768 instructions executed
    * 16840 bytes read
    * 4849 bytes written

Let's presume that execution, read and write each takes following shares in account cost creation.
   * Execution: *3/6*
   * Read: *2/6*
   * Write: *1/6*

Then we could conclude that:
   * 1 byte read costs 681768 * 2 / 3 / 16840 = 27 instructions
   * 1 byte written costs 681768 * 1 / 3 / 4849 = 47 instructions

Thus, when measuring costs we set the operation cost to be:

    cost = number_of_instructions + bytes_read * 27 + bytes_written * 47

## Optional: re-building QEMU and the instruction counter plugin

We ship prebuilt QEMU and TCG instruction counter plugin, so in many cases one doesn't have to build it.
However, in case you still want to build it - use the following steps.

Important: we build QEMU and the TCG plugin inside the container, so execute following commands inside Docker.
Set environment variable HOST_DIR (on the host) to location where both QEMU and nearcore source code is checked
out, it will be mounted as `/host` inside the Docker container.
Start container with:

    ./run.sh

To build QEMU use:

    cd /host/qemu
    ./configure --disable-system --enable-user --enable-plugins  --prefix=/host/qemu-linux --target-list=x86_64-linux-user
    make && make install

Then build and test the QEMU's JIT plugin:

    cd /host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin
    cp /host/qemu-linux/bin/qemu-x86_64 ./
    make QEMU_DIR=/host/qemu
    make test

To execute commands in already running container first find its id with:

    > docker ps

    CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                  NAMES
    e9dcb52cc91b        ubuntu-emu         "/usr/bin/env bash"   	2 hours ago         Up 2 hours          0.0.0.0:5000->22/tcp   reverent_carson

and the use container ID for `docker exec` command, like:

    docker exec -it e9dcb52cc91b /host/qemu-linux/bin/qemu-x86_64 -d plugin -plugin file=/host/qemu-linux/plugins/libcounter.so /host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/test_binary

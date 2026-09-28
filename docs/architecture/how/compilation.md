# Contract compilation

When the compiler daemon is enabled, `neard` compiles prepared WebAssembly in
worker subprocesses rather than inside the node process. This contains compiler
crashes and gives compilation a separate resource boundary. The resulting
artifact and its cache semantics are the same as for in-process compilation.

## Parent and workers

The parent maintains a lazy, reusable pool of workers. A worker is another
invocation of the configured executable with the `compile-wasm` subcommand. The
parent sends one compilation at a time over length-prefixed stdin/stdout frames.
The worker uses its own Rayon pool for the compilation itself.

Before accepting work, a worker installs its configured virtual-address-space
limit and, on Linux, a Landlock sandbox. It then reports its effective settings,
isolation status, and Wasmtime compatibility hash. The parent rejects a worker
whose acknowledgement does not match its request. A worker's thread count,
stack size, and address-space limit are immutable for its lifetime.

Waiting requests are admitted in `Critical`, `Interactive`, then `Background`
order. The coordinator tracks starting, idle, leased, and terminating workers.
It reserves both a process slot and the worker's configured address-space limit
before spawning. The aggregate reservation is a conservative virtual-memory
admission budget, not a limit on physical RSS or on the rest of `neard`.

## Failure and recovery

A normal compiler error is returned as a compilation result. Process, startup,
protocol, and resource failures stay outside that result, so they cannot become
cached deterministic compiler errors. An ordinary worker failure gets one
retry.

Instrumented allocation failure and typed local address-space exhaustion make
the worker exit with a dedicated status. Only this evidence is classified as
local memory exhaustion. An unexplained signal, including `SIGKILL`, is not.
For confirmed exhaustion, the parent serializes recovery and retries in a fresh
worker at successively larger memory tiers, up to the configured maximum.
Recovery can retire idle workers and evict eligible ordinary work to remain
inside the shared process and address-space budgets. Enlarged recovery workers
are retired after the request instead of reducing normal pool concurrency.

## Process lifetime and reaping

A lease exclusively owns a worker's IPC streams while the coordinator retains
a shared process-control handle. Healthy workers return to the idle set. Failed
or selected-for-eviction workers are first marked terminating, which prevents
reuse, and are then terminated and reaped without holding the coordinator lock.
An evicted active request observes a typed eviction and may requeue within a
bounded displacement count.

Starting and terminating workers keep their process and memory reservations.
Capacity is released only after the parent has observed the child exit and
reaped it. Teardown waits are bounded for compilation callers. If necessary, a
detached reaper completes the wait. If reaping cannot be confirmed, the
reservation is deliberately retained rather than risking budget
oversubscription.

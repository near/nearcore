<!-- cspell:words futexes futexctn kptr libelf libbpf pgrep futexctn's msecs vtable -->
# Futex contention

Futex contention occurs when multiple threads compete for access to shared resources protected by futexes (fast user-space mutexes). Futexes are efficient synchronization mechanisms in the Linux kernel, but contention can lead to performance bottlenecks in multithreaded programs.

Analyzing futex contention helps identify which locks are most contended and take the longest to resolve.

## Setup

The first step is to build a profiling-tuned build of `neard`:

```sh
cargo build --release --config .cargo/config.profiling.toml -p neard
```

The second step involves compiling a tool designed to identify futex contention: [futexctn](https://github.com/iovisor/bcc/blob/master/libbpf-tools/futexctn.c). The following snippet demonstrates how to set up a `neard` node on GCP to collect lock contention data:

```sh
sudo sysctl kernel.perf_event_paranoid=0 && sudo sysctl kernel.kptr_restrict=0

sudo apt install -y clang llvm git libelf-dev

git clone https://github.com/iovisor/bcc.git

cd bcc/libbpf-tools/

git submodule update --init --recursive

make -j8
```

Finally, execute the `futexctn` binary for a chosen duration to gather contention statistics:

```sh
sudo ./futexctn -p $(pgrep neard) -T
```

## Understanding futexctn's output

Below is an example section of the tool's output, illustrating the contention of a lock:

```text
neard0[55783] lock 0x79a1b1dfcfb0 contended 41 times, 22 avg msecs [max: 48 msecs, min 5 msecs]
    -
    syscall
    _ZN10near_store4trie4Trie17get_optimized_ref17h8d5fd8c67e262ab1E
    _ZN10near_store4trie4Trie3get17h11d4bcd3667e3c8fE
    _ZN85_$LT$near_store..trie..update..TrieUpdate$u20$as$u20$near_store..trie..TrieAccess$GT$3get17h041c58f313ed6d6cE
    _ZN10near_store5utils11get_account17hf3071e79288206c0E
    _ZN12node_runtime8verifier25get_signer_and_access_key17hca85f102c5a0241bE
    _ZN92_$LT$near_chain..runtime..NightshadeRuntime$u20$as$u20$near_chain..types..RuntimeAdapter$GT$24can_verify_and_charge_tx17hb1907151d54f3c6bE
    _ZN11near_client11rpc_handler10RpcHandler10process_tx17h3ca7fec97c6dd01eE
    _ZN110_$LT$actix..sync..SyncContextEnvelope$LT$M$GT$$u20$as$u20$actix..address..envelope..EnvelopeProxy$LT$A$GT$$GT$6handle17h0f78e99e6ce63395E
    _ZN3std3sys9backtrace28__rust_begin_short_backtrace17hd5ed81d58d44f867E
    _ZN4core3ops8function6FnOnce40call_once$u7b$$u7b$vtable.shim$u7d$$u7d$17hc18f360a04c1b975E
    _ZN3std3sys3pal4unix6thread6Thread3new12thread_start17hcc5ed016d554f327E
    [unknown]
    -
     msecs               : count    distribution
         0 -> 1          : 0        |                                        |
         2 -> 3          : 0        |                                        |
         4 -> 7          : 10       |***********************                 |
         8 -> 15         : 10       |***********************                 |
        16 -> 31         : 4        |*********                               |
        32 -> 63         : 17       |****************************************|
```

Notes on the output:

- The first line shows the number of times the lock was contended and the average duration of contention.
- The middle section provides the stack trace of the thread that locked the futex.
- The bottom section contains a histogram representing the contention duration distribution.

### Things to watch out for

Some instances of contention are not caused by locks being held for extended periods but are instead due to programmatic waits or sleeps. These can be identified either by examining the stack trace or by noticing that their durations are consistently rounded numbers.

Example:

```text
tokio-runtime-w[56073] lock 0x79a1ae9f9fb0 contended 9 times, 1000 avg msecs [max: 1003 msecs, min 1000 msecs]
```

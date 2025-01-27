<!-- cspell:ignore Heaptrack Jemalloc bytehound highcpu jemallocator koute kptr libbytehound myexportedfile mylittleprofile readwrite rgba setcap tikv -->
# Profiling neard

## Sampling performance profiling

It is a common task to need to look where `neard` is spending time. Outside of instrumentation
we've also been successfully using sampling profilers to gain an intuition over how the code works
and where it spends time. It is a very quick way to get some baseline understanding of the
performance characteristics, but due to its probabilistic nature it is also not particularly precise
when it comes to small details.

Linux's `perf` has been a tool of choice in most cases, although tools like Intel VTune could be
used too. In order to use either, first prepare your system:

```command
sudo sysctl kernel.perf_event_paranoid=0
sudo sysctl kernel.kptr_restrict=0
```

<blockquote style="background: rgba(255, 200, 0, 0.1); border: 5px solid rgba(255, 200, 0, 0.4);">

Beware that this gives access to certain kernel state and environment to the unprivileged user.
Once investigation is over either set these properties back to the more restricted settings or,
better yet, reboot.

Definitely do not run untrusted code after running these commands.

</blockquote>

Then collect a profile as such:

```command
$ perf record -e cpu-clock -F1000 -g --call-graph dwarf,65528 YOUR_COMMAND_HERE
# or attach to a running process:
$ perf record -e cpu-clock -F1000 -g --call-graph dwarf,65528 -p NEARD_PID
```

This command will use the CPU time clock to determine when to trigger a sampling process and will
do such sampling roughly 1000 times (the `-F` argument) every CPU second.

Once terminated, this command will produce a profile file in the current working directory.
Although you can inspect the profile already with `perf report`, we've had much better experience
with using [Firefox Profiler](https://profiler.firefox.com/) as the viewer. Although Firefox
Profiler supports `perf` and many other different data formats, for `perf` in particular a
conversion step is necessary:

```command
perf script -F +pid > mylittleprofile.script
```

Then, load this `mylittleprofile.script` file with the profiler.

### Low overhead stack frame collection

The command above uses `-g --call-graph dwarf,65528` parameter to instruct `perf` to collect
stack trace for each sample using DWARF unwinding metadata. This will work no matter how `neard` is
built, but is expensive and not super precise (e.g. it has trouble with JIT code.) If you have an
ability to build a profiling-tuned build of `neard`, you can use higher quality stack frame
collection.

```command
cargo build --release --config .cargo/config.profiling.toml -p neard
```

Then, replace the `--call-graph dwarf` with `--call-graph fp`:

```command
perf record -e cpu-clock -F1000 -g --call-graph fp,65528 YOUR_COMMAND_HERE
```

### Profiling with hardware counters

As mentioned earlier, sampling profiler is probabilistic and the data it produces is only really
suitable for a broad overview. Any attempt to analyze the performance of the code at the
microarchitectural level (which you might want to do if investigating how to speed up a small but
frequently invoked function) will be severely hampered by the low quality of data.

For a long time now, CPUs are able to expose information about how it operates on the code at a
very fine grained level: how many cycles have passed, how many instructions have been processed,
how many branches have been taken, how many predictions were incorrect, how many cycles were spent
waiting of a memory accesses and many more. These allow a much better look at how the code behaves.

Until recently, use of these detailed counters was still sampling based -- the CPU would produce
some information at a certain cadence of these counters (e.g. every 1000 instructions or cycles)
which still shares a fair number of the same downsides as sampling `cpu-clock`. In order to address
this downside, recent CPUs from both Intel and AMD have implemented a list of recent branches taken
-- [Last Branch Record](https://lwn.net/Articles/680985/) or LBR. This is available on reasonably
recent Intel architectures as well as starting with Zen 4 on the side of AMD. With LBRs profilers
are able to gather information about the cycle counts between each branch, giving an accurate and
precise evaluation of the performance at a [basic block](https://en.wikipedia.org/wiki/Basic_block)
or function call level.

It all sounds really nice, so why are we not using these mechanisms all the time? That's because
GCP VMs don't allow access to these counters! In order to access them the code has to be run on
your own hardware, or a VM instance that provides direct access to the hardware, such as the (quite
expensive) `c3-highcpu-192-metal` type.

Once everything is set up, though, the following command can gather some interesting information
for you.

```command
perf record -e cycles:u -b -g --call-graph fp,65528 YOUR_COMMAND_HERE
```

Analyzing this data is, unfortunately, not as easy as chucking it away to Firefox Profiler. I'm not
aware of any other ways to inspect the data other than using `perf report`:

```command
perf report -g --branch-history
perf report -g --branch-stack
```

You may also be able to gather some interesting results if you use `--call-graph lbr` and the
relevant reporting options as well.

## Memory usage profiling

`neard` is a pretty memory-intensive application with many allocations occurring constantly.
Although Rust makes it pretty hard to introduce memory problems, it is still possible to leak
memory or to inadvertently retain too much of it.

Unfortunately, “just” throwing a random profiler at neard does not work for many reasons. Valgrind
for example is introducing enough slowdown to significantly alter the behavior of the run, not to
mention that to run it successfully and without crashing it will be necessary to comment out
`neard`’s use of `jemalloc` for yet another substantial slowdown.

So far the only tool that worked out well out of the box was
[`bytehound`](https://github.com/koute/bytehound). Using it is quite straightforward, but needs
Linux, and ability to execute privileged commands.

First, checkout and build the profiler (you will need to have nodejs `yarn` thing available as
well):

```command
git clone git@github.com:koute/bytehound.git
cargo build --release -p bytehound-preload
cargo build --release -p bytehound-cli
```

You will also need a build of your `neard`, once you have that, give it some ambient capabilities
necessary for profiling:

```command
sudo sysctl kernel.perf_event_paranoid=0
sudo setcap 'CAP_SYS_ADMIN+ep' /path/to/neard
sudo setcap 'CAP_SYS_ADMIN+ep' /path/to/libbytehound.so
```

And finally run the program with the profiler enabled (in this case `neard run` command is used):

```command
/lib64/ld-linux-x86-64.so.2 --preload /path/to/libbytehound.so /path/to/neard run
```

### Viewing the profile

<blockquote style="background: rgba(255, 200, 0, 0.1); border: 5px solid rgba(255, 200, 0, 0.4);">

Do note that you will need about twice the amount of RAM as the size of the input file in order to
load it successfully.

</blockquote>

Once enough profiling data has been gathered, terminate the program. Use the `bytehound` CLI tool
to operate on the profile. I recommend `bytehound server` over directly converting to e.g. heaptrack
format using other subcommands as each invocation will read and parse the profile data from
scratch. This process can take quite some time. `server` parses the inputs once and makes
conversions and other introspection available as interactive steps.

You can use `server` interface to inspect the profile in one of the few ways, download a flamegraph
or a heaptrack file. Heaptrack in particular provides some interesting additional visualizations
and has an ability to show memory use over time from different allocation sources.

I personally found it a bit troublesome to figure out how to open the heaptrack file from the GUI.
However, `heaptrack myexportedfile` worked perfectly. I recommend opening the file exactly this way.

### Troubleshooting

#### No output file

1. Set a higher profiler logging level. Verify that the profiler gets loaded at all. If you're not
   seeing any log messages, then something about your working environment is preventing the loader
   from including the profiler library.
2. Try specifying an exact output file with e.g. environment variables that the profiler reads.

#### Crashes

If the profiled `neard` crashes in your tests, there are a couple of things you can try to get past
it. First, make sure your binary has the necessary ambient capabilities (`setcap` command above
needs to be executed every time binary is replaced!)

Another thing to try is disabling `jemalloc`. Comment out this code in `neard/src/main.rs`:

```rust
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
```

The other thing you can try is different profilers, different versions of the profilers or
different options made available (in particular disabling the shadow stack in bytehound), although
I don't have specific recommendations here.

We don't know what exactly it is about neard that leads to it crashing under the profiler as easily
as it does. I have seen valgrind reporting that we have libraries that are deallocating with a
wrong size class, so that might be the reason? Do definitely look into this if you have time.

## What to profile?

This section provides some ideas on programs you could consider profiling if you are not sure where
to start.

First and foremost you could go shotgun and profile a full running `neard` node that's operating on
mainnet or testnet traffic. There are a couple ways to set up such a node: search for
[my-own-mainnet](https://docs.nearone.org/doc/my-own-mainnettestnet-MZTRLQjXCz) or a forknet based
tooling.

From there either attach to a running `neard run` process or stop the running one and start a new
instance under the profiler.

This approach will give you a good overview of the entire system, but at the same time the
information might be so dense, it might be difficult to derive any signal from the noise. There are
alternatives that isolate certain components of the runtime:

### `Runtime::apply`

Profiling just the `Runtime::apply` is going to include the work done by transaction runtime and
the contract runtime only. A smart use of the tools already present in the `neard` binary can
achieve that today.

First, make sure all deltas in flat storage are applied and written:

```
neard view-state --readwrite apply-range --shard-id $SHARD_ID --storage flat sequential
```

You will need to do this for all shards you're interested in profiling. Then pick a block or a
range of blocks you want to re-apply and set the flat head to the specified height:

```
neard flat-storage move-flat-head --shard-id 0 --version 0 back --blocks 17
```

Finally the following commands will apply the block or blocks from the height in various different
ways. Experiment with the different modes and flags to find the best fit for your task. Don't
forget to run these commands under the profiler :)

```
# Apply blocks from current flat head to the highest known block height in sequence
# using the memtrie storage (note that after this you will need to move the flat head again)
neard view-state --readwrite apply-range --shard-id 0 --storage memtrie sequential
# Same but with flat storage
neard view-state --readwrite apply-range --shard-id 0 --storage flat sequential

# Repeatedly apply a single block at the flat head using the memtrie storage.
# Will not modify the storage on the disk.
neard view-state apply-range --shard-id 0 --storage memtrie benchmark
# Same but with flat storage
neard view-state apply-range --shard-id 0 --storage flat benchmark
```

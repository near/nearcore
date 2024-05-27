# Profiling neard

## Sampling performance profiling

It is a common task to need to look where `neard` is spending time. Outside of instrumentation
we've also been successfully using sampling profilers to gain an intuition over how the code works
and where it spends time.

Linux's `perf` has been a tool of choice in most cases. In order to use it, first prepare your
system:

```command
$ sudo sysctl kernel.perf_event_paranoid=0
$ sudo sysctl kernel.restrict_kptr=0
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
```

(you may omit `-e cpu-clock` in certain environments for a more precise sampling timer.)

This will produce a profile file in the current working directory. Although you can inspect the
profile already with `perf report`, we've had much better experience with using [Firefox
Profiler](https://profiler.firefox.com/) as the viewer. Although Firefox Profiler supports `perf`
and many other different data formats, for `perf` in particular a conversion step is necessary:

```command
$ perf script -F +pid > mylittleprofile.script
```

Then, load this `mylittleprofile.script` with the tool.

## Memory profiling

`neard` is a pretty memory-intensive application with many allocations occurring constantly.
Although Rust makes it pretty hard to introduce memory problems, it is still possible to leak
memory or to inadvertently retain too much of it.

Unfortunately, “just” throwing a random profiler at neard does not work for many reasons. Valgrind
for example is introducing enough slowdown to significantly alter the behaviour of the run, not to
mention that to run it successfully and without crashing it will be necessary to comment out
`neard`’s use of `jemalloc` for yet another substantial slowdown.

So far the only took that worked out well out of the box was
[`bytehound`](https://github.com/koute/bytehound). Using it is quite straightforward, but needs
Linux, and ability to execute privileged commands.

First, checkout and build the profiler (you will need to have nodejs `yarn` thing available as
well):

```command
$ git clone git@github.com:koute/bytehound.git
$ cargo build --release -p bytehound-preload
$ cargo build --release -p bytehound-cli
```

You will also need a build of your `neard`, once you have that, give it some ambient cabapilities
necessary for profiling:

```command
$ sudo sysctl kernel.perf_event_paranoid=0
$ sudo setcap 'CAP_SYS_ADMIN+ep' /path/to/neard
$ sudo setcap 'CAP_SYS_ADMIN+ep' /path/to/libbytehound.so
```

And finally run the program with the profiler enabled (in this case `neard run` command is used):

```command
$ /lib64/ld-linux-x86-64.so.2 --preload /path/to/libbytehound.so /path/to/neard run
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

If the profiled `neard` crashes in your tests, there are a couple things you can try to get past
it. First, makes sure your binary has the necessary ambient capabilities (`setcap` command above
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

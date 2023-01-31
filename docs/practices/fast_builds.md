# Fast Builds

nearcore is implemented in Rust and is a fairly sizable project, so it takes a
while to build. This chapter collects various tips to make the development
process faster.

Optimizing build times is a bit of a black art, so please do benchmarks on your
machine to verify that the improvements work for you. Changing some configuration
and making a typo, which prevents it from improving build times is an
extremely common failure mode!

[Rust Perf Book](https://nnethercote.github.io/perf-book/compile-times.html)
contains a section on compilation times as well!

## Release Builds and Link Time Optimization

`cargo build --release` is obviously slower than `cargo build`. We enable full
lto (link-time optimization), so our `-r` builds are very slow, use a lot of
RAM, and don't utilize the available parallelism fully.

As debug builds are much too slow at runtime for many purposes, we have a custom
profile `--profile quick-release` which is equivalent to `-r`, except that the
time-consuming options such as LTO are disabled.

Use `--profile quick-release` when doing comparative benchmarking, or when
connecting a locally built node to a network. Use `-r` if you want to get
absolute performance numbers.

## Linker

By default, `rustc` uses the default system linker, which tends to be quite
slow. Using `lld` (LLVM linker) or `mold` (very new, very fast linker) provides
big wins for many setups.

I don't know what's the official source of truth for using alternative linkers,
I usually refer to [this
comment](https://github.com/rust-lang/rust/issues/39915#issuecomment-538049306).

Usually, adding

```toml
[build]
rustflags = ["-C", "link-arg=-fuse-ld=lld"]
```

to `~/.cargo/config` is the most convenient approach.

`lld` itself can be installed with `sudo apt install lld` (or the equivalent in 
the distro/package manager of your choice).

## Prebuilt RocksDB

By default, we compile RocksDB (a C++ project) from source during the neard
build. By linking to a prebuilt copy of RocksDB this work can be avoided
entirely. This is a huge win, especially if you clean the `./target` directory
frequently.

To use a prebuilt RocksDB, set the `ROCKSDB_LIB_DIR` environment variable to
a location containing `librocksdb.a`:

```console
$ export ROCKSDB_LIB_DIR=/usr/lib/x86_64-linux-gnu
$ cargo build -p neard
```

Note, that the system must provide a recent version of the library which,
depending on which operating system you’re using, may require installing packages
from a testing branch. For example, on Debian it requires installing
`librocksdb-dev` from the `experimental` repository:

**Note:** Based on which distro you are using this process will look different.
Please refer to the documentation of the package manager you are using.

```bash
echo 'deb http://ftp.debian.org/debian experimental main contrib non-free' |
    sudo tee -a /etc/apt/sources.list
sudo apt update
sudo apt -t experimental install librocksdb-dev

ROCKSDB_LIB_DIR=/usr/lib/x86_64-linux-gnu
export ROCKSDB_LIB_DIR
```

## Global Compilation Cache

By default, Rust compiles incrementally, with the incremental cache and
intermediate outputs stored in the project-local `./target` directory.

The [`sccache`](https://github.com/mozilla/sccache) utility can be used to share
these artifacts between machines or checkouts within the same machine. `sccache`
works by intercepting calls to `rustc` and will fetch the cached outputs from
the global cache whenever possible. This tool can be set up as such:

```console
$ cargo install sccache
$ export RUSTC_WRAPPER="sccache"
$ export SCCACHE_CACHE_SIZE="30G"
$ cargo build -p neard
```

Refer to the [project’s README](https://github.com/mozilla/sccache) for further
configuration options.

## IDEs Are Bad For Environment Handling

Generally, the knobs in this section are controlled either via global
configuration in `~/.cargo/config` or environment variables.

Environment variables are notoriously easy to lose, especially if you are
working both from a command line and a graphical IDE. Double-check that the
environment within which builds are executed is identical to avoid nasty
failure modes such as full cache invalidation when switching from the
CLI to an IDE or vice-versa.

[`direnv`](https://direnv.net) sometimes can be used to conveniently manage
project-specific environment variables.

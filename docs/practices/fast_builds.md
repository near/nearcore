# Fast Builds

nearcore is implemented in Rust and is a fairly sizable project, so it takes a
while to build. This chapter collects various tips to make the process faster.

Optimizing build times is a bit of a black art, so please do benchmarks on your
machine to verify that the improvement work for you. Changing some configuration
and making some type, which prevents it from improving build times is an
extremely common failure mode!

[Rust Perf Book](https://nnethercote.github.io/perf-book/compile-times.html)
contains a section on compilation time as well!

## Release Builds and Link Time Optimization

Obviously, `cargo build --release` is slower than `cargo build`. What's not
entirely obvious is that `cargo build -r` is not as slow as it could be: our
`--release` profile is somewhat optimized for fast builds, as it doesn't enable
full LTO.

When building production binaries, we use `lto=true` and `codegen-units=1`
options, which make the build significantly slower (but the resulting binary
somewhat faster). Keep this in mind when running benchmarks or parameter
estimation.

## Linker

By default, `rustc` uses system's linker, which might be quite slow. Using `lld`
(LLVM linker) or `mold` (very new, very fast linker) is usually a big win.

I don't know what's the official source of truth for using alternative linkers,
I usually refer to [this
comment](https://github.com/rust-lang/rust/issues/39915#issuecomment-538049306).

Usually, adding

```toml
[build]
rustflags = ["-C", "link-arg=-fuse-ld=lld"]
```

to `~/.cargo/config` is the most convenient approach.

lld itself can be installed with `sudo apt install lld`.

## Prebuilt RocksDB

By default, we compile RocksDB (a C++ project) from source, which takes a lot of
time. A faster alternative is to link to a prebuilt copy of RocksDB. This is a
huge win, especially if you clean `./target` directory frequently.

To use prebuilt RocksDB set `ROCKSDB_LIB_DIR` environment variable to location
where `librocksdb.a` file is installed:

```console
$ export ROCKSDB_LIB_DIR=/usr/lib/x86_64-linux-gnu
$ cargo build -p neard
```

Note that the system must provide a recent version of the library which,
depending on operating system you’re using, may require installing packages from
testing branches.  For example, on Debian it requires installing
`librocksdb-dev` from `experimental` version:

```bash
echo 'deb http://ftp.debian.org/debian experimental main contrib non-free' |
    sudo tee -a /etc/apt/sources.list
sudo apt update
sudo apt -t experimental install librocksdb-dev

ROCKSDB_LIB_DIR=/usr/lib/x86_64-linux-gnu
export ROCKSDB_LIB_DIR
```

## Global Compilation Cache

By default, Rust uses incremental compilation, with intermediate artifacts
stored in the project-local `./target` directory.

[`sccache`](https://github.com/mozilla/sccache) utility can be used to add a
global compilation to the mix:

```console
$ cargo install sccache
$ export RUSTC_WRAPPER="sccache"
$ export SCCACHE_CACHE_SIZE="30G"
$ cargo build -p neard
```

`sccache` intercepts calls to `rustc` and pattern-matches compiler's command
line to get a cached result.

## IDEs Are Bad For Environment

Generally, the knobs in this section are controlled either via global
configuration in `~/.cargo/config` or environmental variables.

Environmental variables are notoriously easy to lose, especially if you are
working both from a command line and from a graphical IDE. Double check that you
are not missing any of our build optimizations, the failure mode here is nasty,
as the stuff just takes longer to compile without givin any visual indication of
an error.

[`direnv`](https://direnv.net) sometimes can be used to conveniently manage
project-specific environmentalvariable.

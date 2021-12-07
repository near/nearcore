# Fuzzing 

### Requirements 
`rustup toolchain install nightly`
 `cargo +nightly install grcov`
 `rustup component add llvm-tools-preview`

### Listing available targets for fuzzing
navigate to fuzz directory

`cd chain/chain/fuzz`
`cargo +nightly fuzz list`

In current version it shows one fuzz target. 

`fuzz_build_chain`

Next step is to inkove the fuzzing, I use

`cargo +nightly fuzz run  fuzz_build_chain --release --jobs 4 -- -max_total_time=60 -len_control=0 -prefer_small=0 -max_len=400000000 -rss_limit_mb=30000`


This command will run fuzzing for 1 minute ( 60 seconds) using 4 jobs in release mode. The `+nightly` is important for instrumenting the code with ability to measure coverage.

When the run is successful, the output looks like this

     Running `target/x86_64-unknown-linux-gnu/release/fuzz_build_chain -artifact_prefix=/home/yordan/Work/nearcore/chain/chain/fuzz/artifacts/fuzz_build_chain/ -max_total_time=60 /home/yordan/Work/nearcore/chain/chain/fuzz/corpus/fuzz_build_chain -fork=4`
```
INFO: Running with entropic power schedule (0xFF, 100).
INFO: Seed: 952803121
INFO: Loaded 1 modules   (1233723 inline 8-bit counters): 1233723 [0x5586f8657130, 0x5586f878446b), 
INFO: Loaded 1 PC tables (1233723 PCs): 1233723 [0x5586f8784470,0x5586f9a57820), 
INFO: -fork=4: fuzzing in separate process(s)
INFO: -fork=4: 33 seed inputs, starting to fuzz in /tmp/libFuzzerTemp.FuzzWithFork1325367.dir
#19: cov: 24383 ft: 24383 corp: 33 exec/s 9 oom/timeout/crash: 0/0/0 time: 5s job: 1 dft_time: 0
  NEW_FUNC: 0x5586f395db60  (/home/yordan/Work/nearcore/chain/chain/fuzz/target/x86_64-unknown-linux-gnu/release/fuzz_build_chain+0x2060b60)
  NEW_FUNC: 0x5586f396b580  (/home/yordan/Work/nearcore/chain/chain/fuzz/target/x86_64-unknown-linux-gnu/release/fuzz_build_chain+0x206e580)
    .....
  NEW_FUNC: 0x5586f7aa9950  (/home/yordan/Work/nearcore/chain/chain/fuzz/target/x86_64-unknown-linux-gnu/release/fuzz_build_chain+0x61ac950)
  NEW_FUNC: 0x5586f7aaa3c0  (/home/yordan/Work/nearcore/chain/chain/fuzz/target/x86_64-unknown-linux-gnu/release/fuzz_build_chain+0x61ad3c0)
#39: cov: 34803 ft: 28603 corp: 43 exec/s 6 oom/timeout/crash: 0/0/0 time: 6s job: 2 dft_time: 0
#67: cov: 34820 ft: 29011 corp: 55 exec/s 7 oom/timeout/crash: 0/0/0 time: 8s job: 3 dft_time: 0
....
#921: cov: 34870 ft: 39172 corp: 248 exec/s 6 oom/timeout/crash: 0/0/0 time: 57s job: 15 dft_time: 0
#1032: cov: 34871 ft: 39204 corp: 257 exec/s 6 oom/timeout/crash: 0/0/0 time: 63s job: 16 dft_time: 0
INFO: fuzzed for 66 seconds, wrapping up soon
INFO: exiting: 0 time: 66s
```

## Aggregation coverage data
The fuzz run will produce a lot of files like:

`fuzz/coverage/fuzz_build_chain/raw/*.profraw`

TO aggregate the into one file 

`cargo +nightly fuzz coverage fuzz_build_chain`

After long waiting time the successful output looks like this

```
     Running `target/x86_64-unknown-linux-gnu/release/fuzz_build_chain -artifact_prefix=/home/yordan/Work/nearcore/chain/chain/fuzz/artifacts/fuzz_build_chain/ /home/yordan/Work/nearcore/chain/chain/fuzz/corpus/fuzz_build_chain/c5dffe833f32de6b4d0688307596eac378fed214`
INFO: Running with entropic power schedule (0xFF, 100).
INFO: Seed: 806106256
INFO: Loaded 1 modules   (1270948 inline 8-bit counters): 1270948 [0x55a9bba68610, 0x55a9bbb9eab4), 
INFO: Loaded 1 PC tables (1270948 PCs): 1270948 [0x55a9bbb9eab8,0x55a9bcf034f8), 
target/x86_64-unknown-linux-gnu/release/fuzz_build_chain: Running 1 inputs 1 time(s) each.
Running: /home/yordan/Work/nearcore/chain/chain/fuzz/corpus/fuzz_build_chain/c5dffe833f32de6b4d0688307596eac378fed214
Executed /home/yordan/Work/nearcore/chain/chain/fuzz/corpus/fuzz_build_chain/c5dffe833f32de6b4d0688307596eac378fed214 in 354 ms
***
*** NOTE: fuzzing was not performed, you have only
***       executed the target code on a fixed set of inputs.
***
Merging raw coverage data...
Coverage data merged and saved in "/home/yordan/Work/nearcore/chain/chain/fuzz/coverage/fuzz_build_chain/coverage.profdata".
```



## Visualization of coverage
[grcov](https://lib.rs/crates/grcov "grcov") produces the best results and it's most appealing 

`grcov . -s ../../../ --binary-path ./target/x86_64-unknown-linux-gnu/release/ -t html --branch --llvm -o ./target/release/coverage`

Open it with your favorite browser ( mine is firefox)

`firefox ./target/release/coverage/index.html`

### clean target coverage data
For target `fuzz_build_chain`

`rm -rf coverage/fuzz_build_chain/*`



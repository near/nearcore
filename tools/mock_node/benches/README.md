The benchmarks in this directory use the mock node framework to define
benchmarks that measure the time taken to sync from an empty home dir
to a particular height in the chain defined by the sample home
directory archives included here. To run all the benchmarks:

```shell
$ cargo bench -p mock_node
```

This will take quite a while though, as each iteration of the
benchmark `mock_node_sync_full` takes several minutes, and it's run 10
times. To run just the quicker one:

```shell
$ cargo bench -p mock_node -- mock_node_sync_empty
```

You can pretty easily define and run your own benchmark based on some
other source home directory by creating a gzipped tar archive and
moving it to, say, `tools/mock_node/benches/foo.tar.gz`, and modifying
the code like so:

```diff
--- a/tools/mock_node/benches/sync.rs
+++ b/tools/mock_node/benches/sync.rs
@@ -123,5 +123,9 @@ fn sync_full_chunks(c: &mut Criterion) {
     do_bench(c, "./benches/full.tar.gz", Some(100))
 }

-criterion_group!(benches, sync_empty_chunks, sync_full_chunks);
+fn sync_foo_chunks(c: &mut Criterion) {
+    do_bench(c, "./benches/foo.tar.gz", Some(123))
+}
+
+criterion_group!(benches, sync_empty_chunks, sync_full_chunks, sync_foo_chunks);
```
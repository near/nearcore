# Optimistic block

This short document will tell you about what optimistic block is and why it was introduced with protocol version 77.


## Improving Stateless Validation Latency with Optimistic Block

### Why 2× Latency Existed Before v77 — and How the Idea of Optimistic Block Emerged

![image](https://raw.githubusercontent.com/near/nearcore/7f0299041e3c3c9241a8a300900ec9a8d07fd7c1/docs/images/optimistic_block_before.png)


In this diagram: yellow = data; rectangle = process. Arrow = dependency on a process; dashed arrow = production and dependency on data.

This was the critical path between two consecutive blocks. The two red blocks are the slow parts. It's better to have only one of them on the critical path.
* Chunk production could be skipped and as validation of the state witness can be done as soon as the chunk producer finishes applying the chunk. But that doesn’t help with 2x latency.
* Chunk validation necessarily depend on chunk application to produce the state witness, so there’s no way to parallelize chunk application with the chunk validation of the next chunk
  * The best you can do is stream that data, but that’s complicated.
* If they cannot be stacked this way, only option is to stack them the other way: apply a chunk while validating the same chunk.
* But now the challenge is: chunk application requires the block, and block production is the only way to have the block, but block production depends on chunk endorsement which depends on chunk validation, so how do we break this chain?

### Introduction to Optimistic Block

![image](https://raw.githubusercontent.com/near/nearcore/7f0299041e3c3c9241a8a300900ec9a8d07fd7c1/docs/images/optimistic_block_after.png)

Optimistic Block accelerates chunk execution by speculatively applying chunks before the actual block is finalized. Here's how it works:

* **Block production:** as soon as the previous block producer produces a block, the new block producer produces an optimistic block.
  * The optimistic block contains all parts of the block except those that depend on chunks. That means no chunk headers, no chunk endorsements, but everything else including block height, timestamp, vrf, etc.
* **Chunk execution:** The chunk producer who is responsible for producing the next chunk and needs to apply the just produced chunk of the same shard, listens to the ShardsManager for other chunks with the same parent. As soon as it receives chunks from all other shards (if it happens before the new block is received), and it also receives the optimistic block, it starts applying the chunk optimistically.
  * As a reminder, applying a chunk requires the block that the chunk is in, for two reasons:
    * The runtime requires several fields from the block: block height, timestamp, and VRF. These can be obtained from the optimistic block.
    * A chunk does not contain the incoming receipts of that shard; rather it contains the outgoing receipts of the previous chunk of that shard. In order to collect the incoming receipts, other chunks in the same block are needed. During optimistic execution, these are obtained from the other chunks that are received via ShardsManager.
  * The optimistic chunk application should produce the exact same result as the real chunk application as long as the optimistic block matches all of the real block’s fields, and the chunks used for optimistic application are exactly the chunks included in the real block (in other words, no chunk producer double-signed, and no chunks were missed).
* **Fallback and safety:** It is possible for the optimistic chunk application to incorrectly predict the actual block produced. Therefore, the original chunk application logic still needs to be executed in that case.
  * To implement cleanly, the optimistic chunk application shall serve only as a cache for the actual chunk application. It should not write any data or do anything else other than acting as a cache. The actual chunk application always happens except it may be skipped if the inputs match exactly.
* **Changes to block timestamp:** Note that the way validators choose the block timestamp must be changed, as it needs to be predicted by the time the block producer produces the optimistic block.
  * The timestamp is not a critical field; it only needs to be reasonable (tethered to real world time) and monotonically increasing.
  * In the current implementation, the current timestamp is used when producing the optimistic block.
  * This is not a protocol change as the block producer has freedom in picking this value.
  * If optimistic block was produced, the block and optimistic block timestamp must be the same. Otherwise, the optimistic chunk application will be invalidated. 
* **Backward compatibility:** Optimistic block itself is also not a protocol change. It is also very safe as long as the optimistic chunk application’s implementation is consistent with the actual.

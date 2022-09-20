This document covers the basics of Chain garbage collection.

Currently we run garbage collection only in non-archival nodes,  to keep the
size of the storage under control. Therefore, we remove blocks, chunks and state
that is ‘old’ enough  - which in current configuration means 5 epochs ago.

We run a single ‘round’ of GC after a new block is accepted to the chain - and
in order not to delay the chain too much, we make sure that each round removes
at most 2 blocks from the chain.


## How it works:

Imagine the following chain (with 2 forks)

![](https://user-images.githubusercontent.com/1711539/191359369-0a77cc0a-34ab-4bbf-89b6-1dc90ad5c59c.png)


In the pictures below, let’s assume that epoch length is 5 and we keep only 3
epochs (rather than 5 that is currently set in production) - otherwise the image
becomes too large :wink:


If head is in the middle of the epoch, the *gc_stop* will be set to the first
block of epoch T-2,  and tail & fork_tail will be sitting at the last block of
epoch T-3.

(and no GC is happening in this round - as tail is next to gc_stop).

![](https://user-images.githubusercontent.com/1711539/191359402-3036f854-606d-4021-a7a8-618ffc19faf7.png)


Next block was accepted on the chain (head jumped ahead), but still no GC
happening in this round:

![](https://user-images.githubusercontent.com/1711539/191359451-664a4351-571d-4ba7-8f00-504ef8eb0c8b.png)


Now interesting things will start happening, once head ‘crosses’ over to the
next epoch.

First, the gc_stop will jump to the beginning of the next epoch.

![](https://user-images.githubusercontent.com/1711539/191359501-546cd5cb-76aa-4b86-9508-fe235c9a41ef.png)


Then we’ll start the GC of the forks: by first moving the ‘fork_tail’ to match
the gc_stop and going backwards from there.


![](https://user-images.githubusercontent.com/1711539/191359623-8784efc2-24de-4fa4-a8dc-4f67fdd12a7c.png)


It will start removing all the blocks that don’t have a successor (a.k.a the tip
of the fork). And then it will proceed to lower height.

![](https://user-images.githubusercontent.com/1711539/191359654-4bd96f37-0c4a-471a-ab96-1495369ffe02.png)

Will keep going until it ‘hits’ the tail.

![](https://user-images.githubusercontent.com/1711539/191359778-204570c9-7e53-47d0-9837-e7f54c05cee2.png)


In order not to do too much in one go, we’d only remove up to 2 block in each
run  (that happens after each head update).

Now, the forks are gone, so we can proceed with GCing of the blocks from
canonical chain:

![](https://user-images.githubusercontent.com/1711539/191359974-a002a8a9-7143-4a10-a506-5744426bb0af.png)

Same as before, we’d remove up to 2 blocks in each run:

![](https://user-images.githubusercontent.com/1711539/191359818-5ce228c4-a772-45ca-8161-a7ce9c2431b9.png)

Until we catchup to the gc_stop.


(the original drawings for this document are here: <https://docs.google.com/document/d/1BiEuJqm4phwQbi-fjzHMZPzDL-94z9Dqkc3XPNnxKJM/edit?usp=sharing>)

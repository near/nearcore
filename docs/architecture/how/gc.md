# Garbage Collection

This document covers the basics of Chain garbage collection.

Currently we run garbage collection only in non-archival nodes,  to keep the
size of the storage under control. Therefore, we remove blocks, chunks and state
that is ‘old’ enough  - which in current configuration means 5 epochs ago.

We run a single ‘round’ of GC after a new block is accepted to the chain - and
in order not to delay the chain too much, we make sure that each round removes
at most 2 blocks from the chain.

## How it works:

Imagine the following chain (with 2 forks)

![](https://user-images.githubusercontent.com/1711539/195649805-e7997192-be3a-4bf0-992d-d35b2ad80847.png)

In the pictures below, let’s assume that epoch length is 5 and we keep only 3
epochs (rather than 5 that is currently set in production) - otherwise the image
becomes too large 😉.

If head is in the middle of the epoch, the `gc_stop` will be set to the first
block of epoch T-2, and `tail` & `fork_tail` will be sitting at the last block of
epoch T-3.

(and no GC is happening in this round - as tail is next to `gc_stop`).

![](https://user-images.githubusercontent.com/1711539/195649850-95dee667-b88b-4ef6-b08c-77a17b8d4ae2.png)

Next block was accepted on the chain (head jumped ahead), but still no GC
happening in this round:

![](https://user-images.githubusercontent.com/1711539/195649879-e29cc826-dfd8-4cbc-a66d-72e42202d26a.png)

Now interesting things will start happening once head ‘crosses’ over to the
next epoch.

First, the `gc_stop` will jump to the beginning of the next epoch.

![](https://user-images.githubusercontent.com/1711539/195649928-0401b221-b6b3-4986-8931-54fbdd1adda0.png)

Then we’ll start the GC of the forks: by first moving the `fork_tail` to match
the `gc_stop` and going backwards from there.

![](https://user-images.githubusercontent.com/1711539/195649966-dac6a4dd-f04b-4131-887a-58efe89d456a.png)

It will start removing all the blocks that don’t have a successor (a.k.a the tip
of the fork). And then it will proceed to lower height.

![](https://user-images.githubusercontent.com/1711539/195650003-90e1fde7-18a6-4343-b0dd-9a10a596f136.png)

Will keep going until it ‘hits’ the tail.

![](https://user-images.githubusercontent.com/1711539/195650059-dd6b3d30-7dd5-4324-8e65-80f955960c47.png)

In order not to do too much in one go, we’d only remove up to 2 block in each
run  (that happens after each head update).

Now, the forks are gone, so we can proceed with GCing of the blocks from
the canonical chain:

![](https://user-images.githubusercontent.com/1711539/195650101-dc6953a7-0d55-4db8-a78b-6a52310410b2.png)

Same as before, we’d remove up to 2 blocks in each run:

![](https://user-images.githubusercontent.com/1711539/195650127-b30865e1-d9c1-4950-8607-67d82a185b76.png)

Until we catch up to the `gc_stop`.

(the original drawings for this document are 
[here](https://docs.google.com/document/d/1BiEuJqm4phwQbi-fjzHMZPzDL-94z9Dqkc3XPNnxKJM/edit?usp=sharing))

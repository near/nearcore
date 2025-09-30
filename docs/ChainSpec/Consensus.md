# Consensus

## Definitions and notation

<!-- cspell:ignore operatorname preconfigured -->
For the purpose of maintaining consensus, transactions are grouped into *blocks*. There is a single preconfigured block $G$ called *genesis block*. Every block except $G$ has a link pointing to the *previous block* $\operatorname{prev}(B)$, where $B$ is the block, and $G$ is reachable from every block by following those links (that is, there are no cycles).

The links between blocks give rise to a partial order: for blocks $A$ and $B$, $A < B$ means that $A \ne B$ and $A$ is reachable from $B$ by following links to previous blocks, and $A \le B$ means that $A < B$ or $A = B$. The relations $>$ and $\ge$ are defined as the reflected versions of $<$ and $\le$, respectively. Finally, $A \sim B$ means that either $A < B$, $A = B$ or $A > B$, and $A \nsim B$ means the opposite.

A *chain* $\operatorname{chain}(T)$ is a set of blocks reachable from block $T$, which is called its *tip*. That is, $\operatorname{chain}(T) = \{B \mid B \le T\}$. For any blocks $A$ and $B$, there is a chain that both $A$ and $B$ belong to iff $A \sim B$. In this case, $A$ and $B$ are said to be *on the same chain*.

Each block has an integer *height* $\operatorname{h}(B)$. It is guaranteed that block heights are monotonic (that is, for any block $B \ne G$, $\operatorname{h}(B) > \operatorname{h}(\operatorname{prev}(B))$), but they need not be consecutive. Also, $\operatorname{h}(G)$ may not be zero. Each node keeps track of a valid block with the largest height it knows about, which is called its *head*.

Blocks are grouped into *epochs*. In a chain, the set of blocks that belongs to some epoch forms a contiguous range: if blocks $A$ and $B$ such that $A < B$ belong to the same epoch, then every block $X$ such that $A < X < B$ also belongs to that epoch. Epochs can be identified by sequential indices: $G$ belongs to an epoch with index $0$, and for every other block $B$, the index of its epoch is either the same as that of $\operatorname{prev}(B)$, or one greater.

Each epoch is associated with a set of block producers that are validating blocks in that epoch, as well as an assignment of block heights to block producers that are responsible for producing a block at that height. A block producer responsible for producing a block at height $h$ is called *block proposer at $h$*. This information (the set and the assignment) for an epoch with index $i \ge 2$ is determined by the last block of the epoch with index $i-2$. For epochs with indices $0$ and $1$, this information is preconfigured. Therefore, if two chains share the last block of some epoch, they will have the same set and the same assignment for the next two epochs, but not necessarily for any epoch after that.

The consensus protocol defines a notion of *finality*. Informally, if a block $B$ is final, any future final blocks may only be built on top of $B$. Therefore, transactions in $B$ and preceding blocks are never going to be reversed. Finality is not a function of a block itself, rather, a block may be final or not final in some chain it is a member of. Specifically, $\operatorname{final}(B, T)$, where $B \le T$, means that $B$ is final in $\operatorname{chain}(T)$. A block that is final in a chain is final in all of its extensions: specifically, if $\operatorname{final}(B, T)$ is true, then $\operatorname{final}(B, T')$ is also true for all $T' \ge T$.

## Data structures

The fields in the Block header relevant to the consensus process are:

```rust
struct BlockHeader {
    ...
    prev_hash: BlockHash,
    height: BlockHeight,
    epoch_id: EpochId,
    last_final_block_hash: BlockHash,
    approvals: Vec<Option<Signature>>
    ...
}
```

Block producers in the particular epoch exchange many kinds of messages. The two kinds that are relevant to the consensus are **Blocks** and **Approvals**. The approval contains the following fields:

```rust
enum ApprovalInner {
    Endorsement(BlockHash),
    Skip(BlockHeight),
}

struct Approval {
    inner: ApprovalInner,
    target_height: BlockHeight,
    signature: Signature,
    account_id: AccountId
}
```

Where the parameter of the `Endorsement` is the hash of the approved block, the parameter of the `Skip` is the height of the approved block, `target_height` is the specific height at which the approval can be used (an approval with a particular `target_height` can be only included in the `approvals` of a block that has `height = target_height`), `account_id` is the account of the block producer who created the approval, and `signature` is their signature on the tuple `(inner, target_height)`.

## Approvals Requirements

Every block $B$ except the genesis block must logically contain approvals of a form described in the next paragraph from block producers whose cumulative stake exceeds $^2\!/_3$ of the total stake in the current epoch, and in specific conditions described in section [epoch switches](#epoch-switches) also the approvals of the same form from block producers whose cumulative stake exceeds $^2\!/_3$ of the total stake in the next epoch.

The approvals logically included in the block must be an `Endorsement` with the hash of $\operatorname{prev}(B)$ if and only if $\operatorname{h}(B) = \operatorname{h}(\operatorname{prev}(B))+1$, otherwise it must be a `Skip` with the height of $\operatorname{prev}(B)$. See [this section](#approval-condition) below for details on why the endorsements must contain the hash of the previous block, and skips must contain the height.

Note that since each approval that is logically stored in the block is the same for each block producer (except for the `account_id` of the sender and the `signature`), it is redundant to store the full approvals. Instead physically we only store the signatures of the approvals. The specific way they are stored is the following: we first fetch the ordered set of block producers from the current epoch. If the block is on the epoch boundary and also needs to include approvals from the next epoch (see [epoch switches](#epoch-switches)), we add new accounts from the new epoch

```python
def get_accounts_for_block_ordered(h, prev_block):
    cur_epoch = get_next_block_epoch(prev_block)
    next_epoch = get_next_block_next_epoch(prev_block)

    account_ids = get_epoch_block_producers_ordered(cur_epoch)
    if next_block_needs_approvals_from_next_epoch(prev_block):
        for account_id in get_epoch_block_producers_ordered(next_epoch):
            if account_id not in account_ids:
                account_ids.append(account_id)

    return account_ids
```

The block then contains a vector of optional signatures of the same or smaller size than the resulting set of `account_ids`, with each element being `None` if the approval for such account is absent, or the signature on the approval message if it is present. It's easy to show that the actual approvals that were signed by the block producers can easily be reconstructed from the information available in the block, and thus the signatures can be verified. If the vector of signatures is shorter than the length of `account_ids`, the remaining signatures are assumed to be `None`.

## Messages

On receipt of the approval message the participant just stores it in the collection of approval messages.

```python
def on_approval(self, approval):
    self.approvals.append(approval)
```

Whenever a participant receives a block, the operations relevant to the consensus include updating the `head` and initiating a timer to start sending the approvals on the block to the block producers at the consecutive `target_height`s. The timer delays depend on the height of the last final block, so that information is also persisted.

```python
def on_block(self, block):
    header = block.header

    if header.height <= self.head_height:
        return

    last_final_block = store.get_block(header.last_final_block_hash)

    self.head_height = header.height
    self.head_hash = block.hash()
    self.largest_final_height = last_final_block.height

    self.timer_height = self.head_height + 1
    self.timer_started = time.time()

    self.endorsement_pending = True
```

The timer needs to be checked periodically, and contain the following logic:

```python
def get_delay(n):
    min(MAX_DELAY, MIN_DELAY + DELAY_STEP * (n-2))

def process_timer(self):
    now = time.time()

    skip_delay = get_delay(self.timer_height - self.largest_final_height)

    if self.endorsement_pending and now > self.timer_started + ENDORSEMENT_DELAY:

        if self.head_height >= self.largest_target_height:
            self.largest_target_height = self.head_height + 1
            self.send_approval(head_height + 1)

        self.endorsement_pending = False

    if now > self.timer_started + skip_delay:
        assert not self.endorsement_pending

        self.largest_target_height = max(self.largest_target_height, self.timer_height + 1)
        self.send_approval(self.timer_height + 1)

        self.timer_started = now
        self.timer_height += 1

def send_approval(self, target_height):
    if target_height == self.head_height + 1:
        inner = Endorsement(self.head_hash)
    else:
        inner = Skip(self.head_height)

    approval = Approval(inner, target_height)
    send(approval, to_whom = get_block_proposer(self.head_hash, target_height))
```

Where `get_block_proposer` returns the next block proposer given the previous block and the height of the next block.

It is also necessary that `ENDORSEMENT_DELAY < MIN_DELAY`. Moreover, while not necessary for correctness, we require that `ENDORSEMENT_DELAY * 2 <= MIN_DELAY`.

## Block Production

<!-- cspell:ignore isinstance -->
We first define a convenience function to fetch approvals that can be included in a block at particular height:

```python
def get_approvals(self, target_height):
    return [approval for approval
                     in self.approvals
                     if approval.target_height == target_height and
                        (isinstance(approval.inner, Skip) and approval.prev_height == self.head_height or
                         isinstance(approval.inner, Endorsement) and approval.prev_hash == self.head_hash)]
```

A block producer assigned for a particular height produces a block at that height whenever they have `get_approvals` return approvals from block producers whose stake collectively exceeds 2/3 of the total stake.

## Finality condition

A block $B$ is final in $\operatorname{chain}(T)$, where $T \ge B$, when either $B = G$ or there is a block $X \le T$ such that $B = \operatorname{prev}(\operatorname{prev}(X))$ and $\operatorname{h}(X) = \operatorname{h}(\operatorname{prev}(X))+1 = \operatorname{h}(B)+2$. That is, either $B$ is the genesis block, or $\operatorname{chain}(T)$ includes at least two blocks on top of $B$, and these three blocks ($B$ and the two following blocks) have consecutive heights.

## Epoch switches

There's a parameter $\operatorname{epoch\_ length} \ge 3$ that defines the minimum length of an epoch. Suppose that a particular epoch $e_{cur}$ started at height $h$, and say the next epoch will be $e_{next}$. Say $\operatorname{BP}(e)$ is a set of block producers in epoch $e$. Say $\operatorname{last\_ final}(T)$ is the highest final block in $\operatorname{chain}(T)$. The following are the rules of what blocks contain approvals from what block producers, and belong to what epoch.

- Any block $B$ with $\operatorname{h}(\operatorname{prev}(B)) < h+\operatorname{epoch\_ length}-3$ is in the epoch $e_{cur}$ and must have approvals from more than $^2\!/_3$ of $\operatorname{BP}(e_{cur})$ (stake-weighted).
- Any block $B$ with $\operatorname{h}(\operatorname{prev}(B)) \ge h+\operatorname{epoch\_ length}-3$ for which $\operatorname{h}(\operatorname{last\_ final}(\operatorname{prev}(B))) < h+\operatorname{epoch\_ length}-3$ is in the epoch $e_{cur}$ and must logically include approvals from both more than $^2\!/_3$ of $\operatorname{BP}(e_{cur})$ and more than $^2\!/_3$ of $\operatorname{BP}(e_{next})$ (both stake-weighted).
- The first block $B$ with $\operatorname{h}(\operatorname{last\_ final}(\operatorname{prev}(B))) >= h+\operatorname{epoch\_ length}-3$ is in the epoch $e_{next}$ and must logically include approvals from more than $^2\!/_3$ of $\operatorname{BP}(e_{next})$ (stake-weighted).

(see the definition of *logically including* approvals in [approval requirements](#approvals-requirements))

## Safety

Note that with the implementation above a honest block producer can never produce two endorsements with the same `prev_height` (call this condition *conflicting endorsements*), neither can they produce a skip message `s` and an endorsement `e` such that `s.prev_height < e.prev_height and s.target_height >= e.target_height` (call this condition *conflicting skip and endorsement*).

**Theorem** Suppose that there are blocks $B_1$, $B_2$, $T_1$ and $T_2$ such that $B_1 \nsim B_2$, $\operatorname{final}(B_1, T_1)$ and $\operatorname{final}(B_2, T_2)$. Then, more than $^1\!/_3$ of the block producer in some epoch must have signed either conflicting endorsements or conflicting skip and endorsement.

**Proof** Without loss of generality, we can assume that these blocks are chosen such that their heights are smallest possible. Specifically, we can assume that $\operatorname{h}(T_1) = \operatorname{h}(B_1)+2$ and $\operatorname{h}(T_2) = \operatorname{h}(B_2)+2$. Also, letting $B_c$ be the highest block that is an ancestor of both $B_1$ and $B_2$, we can assume that there is no block $X$ such that $\operatorname{final}(X, T_1)$ and $B_c < X < B_1$ or $\operatorname{final}(X, T_2)$ and $B_c < X < B_2$.

**Lemma** There is such an epoch $E$ that all blocks $X$ such that $B_c < X \le T_1$ or $B_c < X \le T_2$ include approvals from more than $^2\!/_3$ of the block producers in $E$.

**Proof** There are two cases.

Case 1: Blocks $B_c$, $T_1$ and $T_2$ are all in the same epoch. Because the set of blocks in a given epoch in a given chain is a contiguous range, all blocks between them (specifically, all blocks $X$ such that $B_c < X < T_1$ or $B_c < X < T_2$) are also in the same epoch, so all those blocks include approvals from more than $^2\!/_3$ of the block producers in that epoch.

Case 2: Blocks $B_c$, $T_1$ and $T_2$ are not all in the same epoch. Suppose that $B_c$ and $T_1$ are in different epochs. Let $E$ be the epoch of $T_1$ and $E_p$ be the preceding epoch ($T_1$ cannot be in the same epoch as the genesis block). Let $R$ and $S$ be the first and the last block of $E_p$ in $\operatorname{chain}(T_1)$. Then, there must exist a block $F$ in epoch $E_p$ such that $\operatorname{h}(F)+2 = \operatorname{h}(S) < \operatorname{h}(T_1)$. Because $\operatorname{h}(F) < \operatorname{h}(T_1)-2$, we have $F < B_1$, and since there are no final blocks $X$ such that $B_c < X < B_1$, we conclude that $F \le B_c$. Because there are no epochs between $E$ and $E_p$, we conclude that $B_c$ is in epoch $E_p$. Also, $\operatorname{h}(B_c) \ge \operatorname{h}(F) \ge \operatorname{h}(R)+\operatorname{epoch\_ length}-3$. Thus, any block after $B_c$ and until the end of $E$ must include approvals from more than $^2\!/_3$ of the block producers in $E$. Applying the same argument to $\operatorname{chain}(T_2)$, we can determine that $T_2$ is either in $E$ or $E_p$, and in both cases all blocks $X$ such that $B_c < X \le T_2$ include approvals from more than $^2\!/_3$ of block producers in $E$ (the set of block producers in $E$ is the same in $\operatorname{chain}(T_1)$ and $\operatorname{chain}(T_2)$ because the last block of the epoch preceding $E_p$, if any, is before $B_c$ and thus is shared by both chains). The case where $B_c$ and $T_1$ are in the same epoch, but $B_c$ and $T_2$ are in different epochs is handled similarly. Thus, the lemma is proven.

Now back to the theorem. Without loss of generality, assume that $\operatorname{h}(B_1) \le \operatorname{h}(B_2)$. On the one hand, if $\operatorname{chain}(T_2)$ doesn't include a block at height $\operatorname{h}(B_1)$, then the first block at height greater than $\operatorname{h}(B_1)$ must include skips from more than $^2\!/_3$ of the block producers in $E$ which conflict with endorsements in $\operatorname{prev}(T_1)$, therefore, more than $^1\!/_3$ of the block producers in $E$ must have signed conflicting skip and endorsement. Similarly, if $\operatorname{chain}(T_2)$ doesn't include a block at height $\operatorname{h}(B_1)+1$, more than $^1\!/_3$ of the block producers in $E$ signed both an endorsement in $T_1$ and a skip in the first block in $\operatorname{chain}(T_2)$ at height greater than $\operatorname{h}(T_1)$. On the other hand, if $\operatorname{chain}(T_2)$ includes both a block at height $\operatorname{h}(B_1)$ and a block at height $\operatorname{h}(B_1)+1$, the latter must include endorsements for the former, which conflict with endorsements for $B_1$. Therefore, more than $^1\!/_3$ of the block producers in $E$ must have signed conflicting endorsements. Thus, the theorem is proven.

## Liveness

See the proof of liveness in [Doomslug Whitepaper](https://discovery-domain.org/papers/doomslug.pdf) and the recent [Nightshade](https://discovery-domain.org/papers/nightshade.pdf) sharding protocol.

The consensus in this section differs in that it requires two consecutive blocks with endorsements. The proof in the linked paper trivially extends, by observing that once the delay is sufficiently long for a honest block producer to collect enough endorsements, the next block producer ought to have enough time to collect all the endorsements too.

## Approval condition

The approval condition above

> Any valid block must logically include approvals from block producers whose cumulative stake exceeds $^2\!/_3$ of the total stake in the epoch. For a block $B$ and its previous block $B'$ each approval in $B$ must be an `Endorsement` with the hash of $B'$ if and only if `B.height == B'.height + 1`, otherwise it must be a `Skip` with the height of $B'$.

Is more complex that desired, and it is tempting to unify the two conditions. Unfortunately, they cannot be unified.

It is critical that for endorsements each approval has the `prev_hash` equal to the hash of the previous block, because otherwise the [safety proof](#safety) above doesn't work, in the second case the endorsements in $B_1$ and $B_x$ can be the very same approvals.

It is critical that for the skip messages we do **not** require the hashes in the approvals to match the hash of the previous block, because otherwise a malicious actor can create two blocks at the same height, and distribute them such that half of the block producers have one as their head, and the other half has the other. The two halves of the block producers will be sending skip messages with different `prev_hash` but the same `prev_height` to the future block producers, and if there's a requirement that the `prev_hash` in the skip matches exactly the `prev_hash` of the block, no block producer will be able to create their blocks.

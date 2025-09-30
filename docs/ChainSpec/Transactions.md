# Transactions in the Blockchain Layer

A client creates a transaction, computes the transaction hash and signs this hash to get a signed transaction.
Now this signed transaction can be sent to a node.

When a node receives a new signed transaction, it validates the transaction (if the node tracks the shard) and gossips about it to all its peers. Eventually, the valid transaction is added to a transaction pool.

Every validating node has its own transaction pool. The transaction pool maintains transactions that were either not yet discarded, or not yet included onto the chain.

Before producing a chunk, transactions are ordered and validated again. This is done to produce chunks with only valid transactions.

## Transaction ordering

The transaction pool groups transactions by a pair of `(signer_id, signer_public_key)`.
The `signer_id` is the account ID of the user who signed the transaction, the `signer_public_key` is the public key of the account's access key that was used to sign the transactions.
Transactions within a group are not ordered.

The valid order of the transactions in a chunk is the following:

- transactions are ordered in batches.
- within a batch all transactions keys should be different.
- a set of transaction keys in each subsequent batch should be a sub-set of keys from the previous batch.
- transactions with the same key should be ordered in strictly increasing order of their corresponding nonces.

Note:

- the order within a batch is undefined. Each node should use a unique secret seed for that ordering to prevent users from finding the lowest keys, and then using that information to take advantage of every node.

Transaction pool provides a draining structure that allows it to pull transactions in a proper order.

## Transaction validation

The transaction validation happens twice, once before adding it to the transaction pool, then before adding it to a chunk.

### Before adding to a transaction pool

This is done to quickly filter out transactions that have an invalid signature or are invalid on the latest state.

### Before adding to a chunk

A chunk producer has to create a chunk with valid and ordered transactions limited by two criteria:

- the maximum number of transactions for a chunk. 
- the total gas burnt for transactions within a chunk.

To order and filter transactions, the chunk producer gets a pool iterator and passes it to the runtime adapter.
The runtime adapter pulls transactions one by one.
The valid transactions are added to the result; invalid transactions are discarded.
Once one of the chunk limits is reached, all the remaining transactions from the iterator are returned back to the pool.

## Pool iterator

Pool Iterator is a trait that iterates over transaction groups until all transaction group are empty.
Pool Iterator returns a mutable reference to a transaction group that implements a draining iterator.
The draining iterator is like a normal iterator, but it removes the returned entity from the group.
It pulls transactions from the group in order from the smallest nonce to largest.

The pool iterator and draining iterators for transaction groups allow the runtime adapter to create proper order.
For every transaction group, the runtime adapter keeps pulling transactions until the valid transaction is found.
If the transaction group becomes empty, then it's skipped.

The runtime adapter may implement the following code to pull all valid transactions:

```rust
let mut valid_transactions = vec![];
let mut pool_iter = pool.pool_iterator();
while let Some(group_iter) = pool_iter.next() {
    while let Some(tx) = group_iter.next() {
        if is_valid(tx) {
            valid_transactions.push(tx);
            break;
        }
    }
}
valid_transactions
```

### Transaction ordering example using pool iterator

Let's say:

- account IDs as uppercase letters (`"A"`, `"B"`, `"C"` ...)
- public keys are lowercase letters (`"a"`, `"b"`, `"c"` ...)
- nonces are numbers (`1`, `2`, `3` ...)

A pool might have group of transactions in the hashmap:

```
transactions: {
  ("A", "a") -> [1, 3, 2, 1, 2]
  ("B", "b") -> [13, 14]
  ("C", "d") -> [7]
  ("A", "c") -> [5, 2, 3]
}
```

There are 3 accounts (`"A"`, `"B"`, `"C"`). Account `"A"` used 2 public keys (`"a"`, `"c"`). Other accounts used 1 public key each.
Transactions within each group may have repeated nonces while in the pool.
That's because the pool doesn't filter transactions with the same nonce, only transactions with the same hash.

For this example, let's say that transactions are valid if the nonce is even and strictly greater than the previous nonce for the same key.

##### Initialization

When `.pool_iterator()` is called, a new `PoolIteratorWrapper` is created and it holds the mutable reference to the pool,
so the pool can't be modified outside of this iterator. The wrapper looks like this:

```
pool: {
    transactions: {
      ("A", "a") -> [1, 3, 2, 1, 2]
      ("B", "b") -> [13, 14]
      ("C", "d") -> [7]
      ("A", "c") -> [5, 2, 3]
    }
}
sorted_groups: [],
```

`sorted_groups` is a queue of sorted transaction groups that were already sorted and pulled from the pool.

##### Transaction #1

The first group to be selected is for key `("A", "a")`, the pool iterator sorts transactions by nonces and returns the mutable references to the group. Sorted nonces are:
`[1, 1, 2, 2, 3]`. Runtime adapter pulls `1`, then `1`, and then `2`. Both transactions with nonce `1` are invalid because of odd nonce.

Transaction with nonce `2` is even, and since we don't know of any previous nonces, it is valid, and therefore added to the list of valid transactions.

Since the runtime adapter found a valid transaction, the transaction group is dropped, and the pool iterator wrapper becomes the following:

```
pool: {
    transactions: {
      ("B", "b") -> [13, 14]
      ("C", "d") -> [7]
      ("A", "c") -> [5, 2, 3]
    }
}
sorted_groups: [
  ("A", "a") -> [2, 3]
],
```

##### Transaction #2

The next group is for key `("B", "b")`, the pool iterator sorts transactions by nonce and returns the mutable references to the group. Sorted nonces are:
`[13, 14]`. Runtime adapter pulls `13`, then `14`. The transaction with nonce `13` is invalid because of odd nonce.

Transaction with nonce `14` is added to the list of valid transactions.

The transaction group is dropped, but it's empty, so the pool iterator drops it completely:

```
pool: {
    transactions: {
      ("C", "d") -> [7]
      ("A", "c") -> [5, 2, 3]
    }
}
sorted_groups: [
  ("A", "a") -> [2, 3]
],
```

##### Transaction #3

The next group is for key `("C", "d")`, the pool iterator sorts transactions by nonces and returns the mutable references to the group. Sorted nonces are:
`[7]`. Runtime adapter pulls `7`. The transaction with nonce `7` is invalid because of odd nonce.

No valid transactions is added for this group.

The transaction group is dropped, it's empty, so the pool iterator drops it completely:

```
pool: {
    transactions: {
      ("A", "c") -> [5, 2, 3]
    }
}
sorted_groups: [
  ("A", "a") -> [2, 3]
],
```

The next group is for key `("A", "c")`, the pool iterator sorts transactions by nonces and returns the mutable references to the group. Sorted nonces are:
`[2, 3, 5]`. Runtime adapter pulls `2`.

It's a valid transaction, so it's added to the list of valid transactions.

Again, the transaction group is dropped, it's empty, so the pool iterator drops it completely:

```
pool: {
    transactions: { }
}
sorted_groups: [
  ("A", "a") -> [2, 3]
  ("A", "c") -> [3, 5]
],
```

##### Transaction #4

The next group is pulled not from the pool, but from the sorted_groups. The key is `("A", "a")`.
It's already sorted, so the iterator returns the mutable reference. Nonces are:
`[2, 3]`. Runtime adapter pulls `2`, then pulls `3`.

The transaction with nonce `2` is invalid, because we've already pulled a transaction #1 from this group and it had nonce `2`.
The new nonce has to be larger than the previous nonce, so this transaction is invalid.

The transaction with nonce `3` is invalid because of odd nonce.

No valid transactions are added for this group.

The transaction group is dropped, it's empty, so the pool iterator drops it completely:

```
pool: {
    transactions: { }
}
sorted_groups: [
  ("A", "c") -> [3, 5]
],
```

The next group is for key `("A", "c")`, with nonces `[3, 5]`.
Runtime adapter pulls `3`, then pulls `5`. Both transactions are invalid, because the nonce is odd.

No transactions are added.

The transaction group is dropped, the pool iterator wrapper becomes empty:

```
pool: {
    transactions: { }
}
sorted_groups: [ ],
```

When runtime adapter tries to pull the next group, the pool iterator returns `None`, so the runtime adapter drops the iterator.

##### Dropping iterator

If the iterator was not fully drained, but some transactions still remained, they would be reinserted back into the pool.

##### Chunk Transactions

Transactions that were pulled from the pool:

```
// First batch
("A", "a", 1),
("A", "a", 1),
("A", "a", 2),
("B", "b", 13),
("B", "b", 14),
("C", "d", 7),
("A", "c", 2),

// Next batch
("A", "a", 2),
("A", "a", 3),
("A", "c", 3),
("A", "c", 5),
```

The valid transactions are:

```
("A", "a", 2),
("B", "b", 14),
("A", "c", 2),
```

In total there were only 3 valid transactions that resulted in one batch.

### Order validation

Other validators need to check the order of transactions in the produced chunk.
It can be done in linear time using a greedy algorithm.

To select a first batch we need to iterate over transactions one by one until we see a transaction
with the key that we've already included in the first batch.
This transaction belongs to the next batch.

Now all transactions in the N+1 batch should have a corresponding transaction with the same key in the N batch.
If there are no transaction with the same key in the N batch, then the order is invalid.

We also enforce the order of the sequence of transactions for the same key, the nonces of them should be in strictly increasing order.

Here is the algorithm that validates the order:

```rust
fn validate_order(txs: &Vec<Transaction>) -> bool {
    let mut nonces: HashMap<Key, Nonce> = HashMap::new();
    let mut batches: HashMap<Key, usize> = HashMap::new();
    let mut current_batch = 1;

    for tx in txs {
        let key = tx.key();

        // Verifying nonce
        let nonce = tx.nonce();
        if let Some(last_nonce) = nonces.get(key) {
            if nonce <= last_nonce {
                // Nonces should increase.
                return false;
            }
        }
        nonces.insert(key, nonce);

        // Verifying batch
        if let Some(last_batch) = batches.get(key) {
            if last_batch == current_batch {
                current_batch += 1;
            } else if last_batch < current_batch - 1 {
                // Was skipped this key in the previous batch
                return false;
            }
        } else {
            if current_batch > 1 {
                // Not in first batch
                return false;
            }
        }
        batches.insert(key, batch);
    }
    true
}
```

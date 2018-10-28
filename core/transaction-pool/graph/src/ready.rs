// Copyright 2018 Parity Technologies (UK) Ltd.
// This file is part of Substrate.

// Substrate is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Substrate is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Substrate.  If not, see <http://www.gnu.org/licenses/>.

use std::{
	collections::{HashMap, HashSet, BTreeSet},
	cmp,
	hash,
	sync::Arc,
};

use parking_lot::RwLock;
use sr_primitives::traits::Member;
use sr_primitives::transaction_validity::{
	TransactionTag as Tag,
};

use error;
use future::WaitingTransaction;
use base_pool::Transaction;

#[derive(Debug)]
pub struct TransactionRef<Hash, Ex> {
	pub transaction: Arc<Transaction<Hash, Ex>>,
	pub insertion_id: u64,
}

impl<Hash, Ex> Clone for TransactionRef<Hash, Ex> {
	fn clone(&self) -> Self {
		TransactionRef {
			transaction: self.transaction.clone(),
			insertion_id: self.insertion_id,
		}
	}
}

impl<Hash, Ex> Ord for TransactionRef<Hash, Ex> {
	fn cmp(&self, other: &Self) -> cmp::Ordering {
		self.transaction.priority.cmp(&other.transaction.priority)
			.then(other.transaction.valid_till.cmp(&self.transaction.valid_till))
			.then(other.insertion_id.cmp(&self.insertion_id))
	}
}

impl<Hash, Ex> PartialOrd for TransactionRef<Hash, Ex> {
	fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl<Hash, Ex> PartialEq for TransactionRef<Hash, Ex> {
	fn eq(&self, other: &Self) -> bool {
		self.cmp(other) == cmp::Ordering::Equal
	}
}
impl<Hash, Ex> Eq for TransactionRef<Hash, Ex> {}

#[derive(Debug)]
struct ReadyTx<Hash, Ex> {
	/// A reference to a transaction
	pub transaction: TransactionRef<Hash, Ex>,
	/// A list of transactions that get unlocked by this one
	pub unlocks: Vec<Hash>,
	/// How many required tags are provided inherently
	///
	/// Some transactions might be already pruned from the queue,
	/// so when we compute ready set we may consider this transactions ready earlier.
	pub requires_offset: usize,
}

impl<Hash: Clone, Ex> Clone for ReadyTx<Hash, Ex> {
	fn clone(&self) -> Self {
		ReadyTx {
			transaction: self.transaction.clone(),
			unlocks: self.unlocks.clone(),
			requires_offset: self.requires_offset,
		}
	}
}

const HASH_READY: &str = r#"
Every time transaction is imported its hash is placed in `ready` map and tags in `provided_tags`;
Every time transaction is removed from the queue we remove the hash from `ready` map and from `provided_tags`;
Hence every hash retrieved from `provided_tags` is always present in `ready`;
qed
"#;

#[derive(Debug)]
pub struct ReadyTransactions<Hash: hash::Hash + Eq, Ex> {
	/// Insertion id
	insertion_id: u64,
	/// tags that are provided by Ready transactions
	provided_tags: HashMap<Tag, Hash>,
	/// Transactions that are ready (i.e. don't have any requirements external to the pool)
	ready: Arc<RwLock<HashMap<Hash, ReadyTx<Hash, Ex>>>>,
	/// Best transactions that are ready to be included to the block without any other previous transaction.
	best: BTreeSet<TransactionRef<Hash, Ex>>,
}

impl<Hash: hash::Hash + Eq, Ex> Default for ReadyTransactions<Hash, Ex> {
	fn default() -> Self {
		ReadyTransactions {
			insertion_id: Default::default(),
			provided_tags: Default::default(),
			ready: Default::default(),
			best: Default::default(),
		}
	}
}

impl<Hash: hash::Hash + Member, Ex> ReadyTransactions<Hash, Ex> {
	/// Borrows a map of tags that are provided by transactions in this queue.
	pub fn provided_tags(&self) -> &HashMap<Tag, Hash> {
		&self.provided_tags
	}

	/// Returns an iterator of ready transactions.
	///
	/// Transactions are returned in order:
	/// 1. First by the dependencies:
	///	- never return transaction that requires a tag, which was not provided by one of the previously returned transactions
	/// 2. Then by priority:
	/// - If there are two transactions with all requirements satisfied the one with higher priority goes first.
	/// 3. Then by the ttl that's left
	/// - transactions that are valid for a shorter time go first
	/// 4. Lastly we sort by the time in the queue
	/// - transactions that are longer in the queue go first
	pub fn get(&self) -> impl Iterator<Item=Arc<Transaction<Hash, Ex>>> {
		BestIterator {
			all: self.ready.clone(),
			best: self.best.clone(),
			awaiting: Default::default(),
		}
	}

	/// Imports transactions to the pool of ready transactions.
	///
	/// The transaction needs to have all tags satisfied (be ready) by transactions
	/// that are in this queue.
	pub fn import(
		&mut self,
		tx: WaitingTransaction<Hash, Ex>,
	) -> error::Result<Vec<Arc<Transaction<Hash, Ex>>>> {
		assert!(tx.is_ready(), "Only ready transactions can be imported.");
		assert!(!self.ready.read().contains_key(&tx.transaction.hash), "Transaction is already imported.");

		self.insertion_id += 1;
		let insertion_id = self.insertion_id;
		let hash = tx.transaction.hash.clone();
		let tx = tx.transaction;

		let replaced = self.replace_previous(&tx)?;

		let mut goes_to_best = true;
		let mut ready = self.ready.write();
		// Add links to transactions that unlock the current one
		for tag in &tx.requires {
			// Check if the transaction that satisfies the tag is still in the queue.
			if let Some(other) = self.provided_tags.get(tag) {
				let mut tx = ready.get_mut(other).expect(HASH_READY);
				tx.unlocks.push(hash.clone());
				// this transaction depends on some other, so it doesn't go to best directly.
				goes_to_best = false;
			}
	 	}

		// update provided_tags
		for tag in tx.provides.clone() {
			self.provided_tags.insert(tag, hash.clone());
		}

		let transaction = TransactionRef {
			insertion_id,
			transaction: Arc::new(tx),
		};

		// insert to best if it doesn't require any other transaction to be included before it
		if goes_to_best {
			self.best.insert(transaction.clone());
		}

		// insert to Ready
		ready.insert(hash, ReadyTx {
			transaction,
			unlocks: vec![],
			requires_offset: 0,
		});

		Ok(replaced)
	}

	/// Returns true if given hash is part of the queue.
	pub fn contains(&self, hash: &Hash) -> bool {
		self.ready.read().contains_key(hash)
	}

	/// Removes invalid transactions from the ready pool.
	///
	/// NOTE removing a transaction will also cause a removal of all transactions that depend on that one
	/// (i.e. the entire subgraph that this transaction is a start of will be removed).
	/// All removed transactions are returned.
	pub fn remove_invalid(&mut self, hashes: &[Hash]) -> Vec<Arc<Transaction<Hash, Ex>>> {
		let mut removed = vec![];
		let mut to_remove = hashes.iter().cloned().collect::<Vec<_>>();

		let mut ready = self.ready.write();
		loop {
			let hash = match to_remove.pop() {
				Some(hash) => hash,
				None => return removed,
			};

			if let Some(mut tx) = ready.remove(&hash) {
				// remove entries from provided_tags
				for tag in &tx.transaction.transaction.provides {
					self.provided_tags.remove(tag);
				}
				// remove from unlocks
				for tag in &tx.transaction.transaction.requires {
					if let Some(hash) = self.provided_tags.get(tag) {
						if let Some(tx) = ready.get_mut(hash) {
							remove_item(&mut tx.unlocks, &hash);
						}
					}
				}

				// remove from best
				self.best.remove(&tx.transaction);

				// remove all transactions that the current one unlocks
				to_remove.append(&mut tx.unlocks);

				// add to removed
				debug!(target: "txpool", "[{:?}] Removed as invalid: ", hash);
				removed.push(tx.transaction.transaction);
			}
		}
	}

	/// Removes transactions that provide given tag.
	///
	/// All transactions that lead to a transaction, which provides this tag
	/// are going to be removed from the queue, but no other transactions are touched -
	/// i.e. all other subgraphs starting from given tag are still considered valid & ready.
	pub fn prune_tags(&mut self, tag: Tag) -> Vec<Arc<Transaction<Hash, Ex>>> {
		let mut removed = vec![];
		let mut to_remove = vec![tag];

		loop {
			let tag = match to_remove.pop() {
				Some(tag) => tag,
				None => return removed,
			};

			let res = self.provided_tags.remove(&tag)
					.and_then(|hash| self.ready.write().remove(&hash));

			if let Some(tx) = res {
				let unlocks = tx.unlocks;
				let tx = tx.transaction.transaction;

				// prune previous transactions as well
				{
					let hash = &tx.hash;
					let mut ready = self.ready.write();
					let mut find_previous = |tag| -> Option<Vec<Tag>> {
						let prev_hash = self.provided_tags.get(tag)?;
						let tx2 = ready.get_mut(&prev_hash)?;
						remove_item(&mut tx2.unlocks, hash);
						// We eagerly prune previous transactions as well.
						// But it might not always be good.
						// Possible edge case:
						// - tx provides two tags
						// - the second tag enables some subgraph we don't know of yet
						// - we will prune the transaction
						// - when we learn about the subgraph it will go to future
						// - we will have to wait for re-propagation of that transaction
						// Alternatively the caller may attempt to re-import these transactions.
						if tx2.unlocks.is_empty() {
							Some(tx2.transaction.transaction.provides.clone())
						} else {
							None
						}
					};

					// find previous transactions
					for tag in &tx.requires {
						if let Some(mut tags_to_remove) = find_previous(tag) {
							to_remove.append(&mut tags_to_remove);
						}
					}
				}

				// add the transactions that just got unlocked to `best`
				for hash in unlocks {
					if let Some(tx) = self.ready.write().get_mut(&hash) {
						tx.requires_offset += 1;
						// this transaction is ready
						if tx.requires_offset == tx.transaction.transaction.requires.len() {
							self.best.insert(tx.transaction.clone());
						}
					}
				}

				debug!(target: "txpool", "[{:?}] Pruned.", tx.hash);
				removed.push(tx);
			}
		}
	}

	/// Checks if the transaction is providing the same tags as other transactions.
	///
	/// In case that's true it determines if the priority of transactions that
	/// we are about to replace is lower than the priority of the replacement transaction.
	/// We remove/replace old transactions in case they have lower priority.
	///
	/// In case replacement is succesful returns a list of removed transactions.
	fn replace_previous(&mut self, tx: &Transaction<Hash, Ex>) -> error::Result<Vec<Arc<Transaction<Hash, Ex>>>> {
		let mut to_remove = {
			// check if we are replacing a transaction
			let replace_hashes = tx.provides
				.iter()
				.filter_map(|tag| self.provided_tags.get(tag))
				.collect::<HashSet<_>>();

			// early exit if we are not replacing anything.
			if replace_hashes.is_empty() {
				return Ok(vec![]);
			}

			// now check if collective priority is lower than the replacement transaction.
			let old_priority = {
				let ready = self.ready.read();
				replace_hashes
					.iter()
					.filter_map(|hash| ready.get(hash))
					.fold(0u64, |total, tx| total.saturating_add(tx.transaction.transaction.priority))
			};

			// bail - the transaction has too low priority to replace the old ones
			if old_priority >= tx.priority {
				bail!(error::ErrorKind::TooLowPriority(old_priority, tx.priority))
			}

			replace_hashes.into_iter().cloned().collect::<Vec<_>>()
		};

		let new_provides = tx.provides.iter().cloned().collect::<HashSet<_>>();
		let mut removed = vec![];
		loop {
			let hash = match to_remove.pop() {
				Some(hash) => hash,
				None => return Ok(removed),
			};

			let tx = self.ready.write().remove(&hash).expect(HASH_READY);
			// check if this transaction provides stuff that is not provided by the new one.
			let (mut unlocks, tx) = (tx.unlocks, tx.transaction.transaction);
			{
				let invalidated = tx.provides
					.iter()
					.filter(|tag| !new_provides.contains(&**tag));

				for tag in invalidated {
					// remove the tag since it's no longer provided by any transaction
					self.provided_tags.remove(tag);
					// add more transactions to remove
					to_remove.append(&mut unlocks);
				}
			}

			removed.push(tx);
		}
	}

	/// Returns number of transactions in this queue.
	pub fn len(&self) -> usize {
		self.ready.read().len()
	}

}

pub struct BestIterator<Hash, Ex> {
	all: Arc<RwLock<HashMap<Hash, ReadyTx<Hash, Ex>>>>,
	awaiting: HashMap<Hash, (usize, TransactionRef<Hash, Ex>)>,
	best: BTreeSet<TransactionRef<Hash, Ex>>,
}

impl<Hash: hash::Hash + Member, Ex> BestIterator<Hash, Ex> {
	/// Depending on number of satisfied requirements insert given ref
	/// either to awaiting set or to best set.
	fn best_or_awaiting(&mut self, satisfied: usize, tx_ref: TransactionRef<Hash, Ex>) {
		if satisfied == tx_ref.transaction.requires.len() {
			// If we have satisfied all deps insert to best
			self.best.insert(tx_ref);

		} else {
			// otherwise we're still awaiting for some deps
			self.awaiting.insert(tx_ref.transaction.hash.clone(), (satisfied, tx_ref));
		}
	}
}

impl<Hash: hash::Hash + Member, Ex> Iterator for BestIterator<Hash, Ex> {
	type Item = Arc<Transaction<Hash, Ex>>;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			let best = self.best.iter().next_back()?.clone();
			let best = self.best.take(&best)?;

			let next = self.all.read().get(&best.transaction.hash).cloned();
			let ready = match next {
				Some(ready) => ready,
				// The transaction is not in all, maybe it was removed in the meantime?
				None => continue,
			};

			// Insert transactions that just got unlocked.
			for hash in &ready.unlocks {
				// first check local awaiting transactions
				let res = if let Some((mut satisfied, tx_ref)) = self.awaiting.remove(hash) {
					satisfied += 1;
					Some((satisfied, tx_ref))
				// then get from the pool
				} else if let Some(next) = self.all.read().get(hash) {
					Some((next.requires_offset + 1, next.transaction.clone()))
				} else {
					None
				};

				if let Some((satisfied, tx_ref)) = res {
					self.best_or_awaiting(satisfied, tx_ref)
				}
			}

			return Some(best.transaction.clone())
		}
	}
}

// See: https://github.com/rust-lang/rust/issues/40062
fn remove_item<T: PartialEq>(vec: &mut Vec<T>, item: &T) {
	if let Some(idx) = vec.iter().position(|i| i == item) {
		vec.swap_remove(idx);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn tx(id: u8) -> Transaction<u64, Vec<u8>> {
		Transaction {
			data: vec![id],
			hash: id as u64,
			priority: 1,
			valid_till: 2,
			requires: vec![vec![1], vec![2]],
			provides: vec![vec![3], vec![4]],
		}
	}

	#[test]
	fn should_replace_transaction_that_provides_the_same_tag() {
		// given
		let mut ready = ReadyTransactions::default();
		let mut tx1 = tx(1);
		tx1.requires.clear();
		let mut tx2 = tx(2);
		tx2.requires.clear();
		tx2.provides = vec![vec![3]];
		let mut tx3 = tx(3);
		tx3.requires.clear();
		tx3.provides = vec![vec![4]];

		// when
		let x = WaitingTransaction::new(tx2, &ready.provided_tags());
		ready.import(x).unwrap();
		let x = WaitingTransaction::new(tx3, &ready.provided_tags());
		ready.import(x).unwrap();
		assert_eq!(ready.get().count(), 2);

		// too low priority
		let x = WaitingTransaction::new(tx1.clone(), &ready.provided_tags());
		ready.import(x).unwrap_err();

		tx1.priority = 10;
		let x = WaitingTransaction::new(tx1.clone(), &ready.provided_tags());
		ready.import(x).unwrap();

		// then
		assert_eq!(ready.get().count(), 1);
	}


	#[test]
	fn should_return_best_transactions_in_correct_order() {
		// given
		let mut ready = ReadyTransactions::default();
		let mut tx1 = tx(1);
		tx1.requires.clear();
		let mut tx2 = tx(2);
		tx2.requires = tx1.provides.clone();
		tx2.provides = vec![vec![106]];
		let mut tx3 = tx(3);
		tx3.requires = vec![tx1.provides[0].clone(), vec![106]];
		tx3.provides = vec![];
		let mut tx4 = tx(4);
		tx4.requires = vec![tx1.provides[0].clone()];
		tx4.provides = vec![];
		let tx5 = Transaction {
			data: vec![5],
			hash: 5,
			priority: 1,
			valid_till: u64::max_value(),	// use the max_value() here for testing.
			requires: vec![tx1.provides[0].clone()],
			provides: vec![],
		};

		// when
		let x = WaitingTransaction::new(tx1, &ready.provided_tags());
		ready.import(x).unwrap();
		let x = WaitingTransaction::new(tx2, &ready.provided_tags());
		ready.import(x).unwrap();
		let x = WaitingTransaction::new(tx3, &ready.provided_tags());
		ready.import(x).unwrap();
		let x = WaitingTransaction::new(tx4, &ready.provided_tags());
		ready.import(x).unwrap();
		let x = WaitingTransaction::new(tx5, &ready.provided_tags());
		ready.import(x).unwrap();

		// then
		assert_eq!(ready.best.len(), 1);

		let mut it = ready.get().map(|tx| tx.data[0]);

		assert_eq!(it.next(), Some(1));
		assert_eq!(it.next(), Some(2));
		assert_eq!(it.next(), Some(3));
		assert_eq!(it.next(), Some(4));
		assert_eq!(it.next(), Some(5));
		assert_eq!(it.next(), None);
	}

	#[test]
	fn should_order_refs() {
		let mut id = 1;
		let mut with_priority = |priority, longevity| {
			id += 1;
			let mut tx = tx(id);
			tx.priority = priority;
			tx.valid_till = longevity;
			tx
		};
		// higher priority = better
		assert!(TransactionRef {
			transaction: Arc::new(with_priority(3, 3)),
			insertion_id: 1,
		} > TransactionRef {
			transaction: Arc::new(with_priority(2, 3)),
			insertion_id: 2,
		});
		// lower validity = better
		assert!(TransactionRef {
			transaction: Arc::new(with_priority(3, 2)),
			insertion_id: 1,
		} > TransactionRef {
			transaction: Arc::new(with_priority(3, 3)),
			insertion_id: 2,
		});
		// lower insertion_id = better
		assert!(TransactionRef {
			transaction: Arc::new(with_priority(3, 3)),
			insertion_id: 1,
		} > TransactionRef {
			transaction: Arc::new(with_priority(3, 3)),
			insertion_id: 2,
		});
	}
}

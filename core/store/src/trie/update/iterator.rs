use std::iter::Peekable;
use std::ops::Bound;

use crate::trie::update::*;
use crate::StorageError;

use crate::trie::TrieIterator;

struct MergeIter<'a> {
    left: Peekable<Box<dyn Iterator<Item = (&'a [u8], Option<&'a [u8]>)> + 'a>>,
    right: Peekable<Box<dyn Iterator<Item = (&'a [u8], Option<&'a [u8]>)> + 'a>>,
}

impl<'a> Iterator for MergeIter<'a> {
    type Item = (&'a [u8], Option<&'a [u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        let res = match (self.left.peek(), self.right.peek()) {
            (Some(&(ref left_key, _)), Some(&(ref right_key, _))) => left_key.cmp(right_key),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => return None,
        };

        // Check which elements comes first and only advance the corresponding iterator.
        // If two keys are equal, take the value from `right`.
        match res {
            std::cmp::Ordering::Less => self.left.next(),
            std::cmp::Ordering::Greater => self.right.next(),
            std::cmp::Ordering::Equal => {
                self.left.next();
                self.right.next()
            }
        }
    }
}

pub struct TrieUpdateIterator<'a>(Option<(Peekable<TrieIterator<'a>>, Peekable<MergeIter<'a>>)>);

impl<'a> TrieUpdateIterator<'a> {
    #![allow(clippy::new_ret_no_self)]
    pub fn new(state_update: &'a TrieUpdate, prefix: &[u8]) -> Result<Self, StorageError> {
        let mut trie_iter = state_update.trie.iter()?;
        trie_iter.seek_prefix(prefix)?;

        let end_bound = make_prefix_range_end_bound(prefix);
        let end_bound = if let Some(end_bound) = &end_bound {
            Bound::Excluded(end_bound.as_slice())
        } else {
            Bound::Unbounded
        };
        let range = (Bound::Included(prefix), end_bound);

        let committed_iter = state_update.committed.range::<[u8], _>(range).map(
            |(raw_key, changes_with_trie_key)| {
                let key = raw_key.as_slice();
                let value = changes_with_trie_key
                    .changes
                    .last()
                    .as_ref()
                    .expect("Committed entry should have at least one change.")
                    .data
                    .as_deref();
                (key, value)
            },
        );
        let prospective_iter = state_update
            .prospective
            .range::<[u8], _>(range)
            .map(|(raw_key, key_value)| (raw_key.as_slice(), key_value.value.as_deref()));
        let overlay_iter = MergeIter {
            left: (Box::new(committed_iter) as Box<dyn Iterator<Item = _>>).peekable(),
            right: (Box::new(prospective_iter) as Box<dyn Iterator<Item = _>>).peekable(),
        }
        .peekable();
        Ok(TrieUpdateIterator(Some((trie_iter.peekable(), overlay_iter))))
    }
}

impl<'a> Iterator for TrieUpdateIterator<'a> {
    type Item = Result<Vec<u8>, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        #[derive(Eq, PartialEq)]
        enum Ordering {
            Trie,
            Overlay,
            Both,
        }
        // Usually one iteration, unless need to skip None values in prospective / committed.
        let iterators = self.0.as_mut()?;
        loop {
            let res = match (iterators.0.peek(), iterators.1.peek()) {
                (Some(Err(_)), _) => {
                    let err = iterators.0.next().unwrap().unwrap_err();
                    self.0 = None;
                    return Some(Err(err));
                }

                (Some(Ok((left_key, _))), Some((right_key, _))) => {
                    match left_key.as_slice().cmp(right_key) {
                        std::cmp::Ordering::Less => Ordering::Trie,
                        std::cmp::Ordering::Equal => Ordering::Both,
                        std::cmp::Ordering::Greater => Ordering::Overlay,
                    }
                }
                (Some(_), None) => Ordering::Trie,
                (None, Some(_)) => Ordering::Overlay,
                (None, None) => {
                    self.0 = None;
                    return None;
                }
            };

            // Check which element comes first and advance the corresponding
            // iterator only.  If both keys are equal, check if overlay doesn’t
            // delete the value.
            let trie_item = if res != Ordering::Overlay { iterators.0.next() } else { None };
            if res == Ordering::Trie {
                if let Some(Ok((key, _))) = trie_item {
                    return Some(Ok(key));
                }
            } else if let Some((overlay_key, Some(_))) = iterators.1.next() {
                return Some(Ok(if let Some(Ok((trie_key, _))) = trie_item {
                    debug_assert_eq!(trie_key.as_slice(), overlay_key);
                    trie_key
                } else {
                    overlay_key.to_vec()
                }));
            }
        }
    }
}

impl<'a> std::iter::FusedIterator for TrieUpdateIterator<'a> {}

/// Returns an end bound for a range which corresponds to all values with
/// a given prefix.
///
/// In other words, the smallest value larger than the `prefix` which does not
/// start with the `prefix`.  If no such value exists, returns `None`.
fn make_prefix_range_end_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let ffs = prefix.iter().rev().take_while(|&&byte| byte == u8::MAX).count();
    let next = &prefix[..(prefix.len() - ffs)];
    if next.is_empty() {
        // Prefix consisted of \xff bytes.  There is no key that follows it.
        None
    } else {
        let mut next = next.to_vec();
        *next.last_mut().unwrap() += 1;
        Some(next)
    }
}

#[test]
fn test_make_prefix_range_end_bound() {
    fn test(want: Option<&[u8]>, prefix: &[u8]) {
        assert_eq!(want, make_prefix_range_end_bound(prefix).as_deref());
    }

    test(None, b"");
    test(None, b"\xff");
    test(None, b"\xff\xff\xff\xff");
    test(Some(b"b"), b"a");
    test(Some(b"b"), b"a\xff\xff\xff");
}

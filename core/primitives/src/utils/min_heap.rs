use std::cmp::Reverse;
use std::collections::binary_heap::{BinaryHeap, IntoIter};

/// Wrapper around `BinaryHeap` to be default min heap instead of max heap.
#[derive(Debug, Clone)]
pub struct MinHeap<T> {
    inner: BinaryHeap<Reverse<T>>,
}

impl<T: Ord> MinHeap<T> {
    pub fn push(&mut self, t: T) {
        self.inner.push(Reverse(t));
    }

    pub fn pop(&mut self) -> Option<T> {
        self.inner.pop().map(|Reverse(t)| t)
    }

    pub fn peek(&self) -> Option<&T> {
        self.inner.peek().map(|Reverse(t)| t)
    }

    pub fn peek_mut(&mut self) -> Option<PeekMut<'_, T>> {
        self.inner.peek_mut().map(PeekMut)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        self.inner.extend(iter.into_iter().map(|item| Reverse(item)))
    }
}

impl<T> Default for MinHeap<T>
where
    T: Default + Eq + PartialEq + PartialOrd + Ord,
{
    fn default() -> Self {
        Self { inner: BinaryHeap::default() }
    }
}

impl<T: Ord> FromIterator<T> for MinHeap<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let inner = iter.into_iter().map(|t| Reverse(t)).collect();
        Self { inner }
    }
}

impl<T> IntoIterator for MinHeap<T> {
    type Item = T;
    type IntoIter = std::iter::Map<IntoIter<Reverse<T>>, fn(Reverse<T>) -> T>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter().map(|Reverse(t)| t)
    }
}

pub struct PeekMut<'a, T: std::cmp::Ord>(std::collections::binary_heap::PeekMut<'a, Reverse<T>>);

impl<'a, T: std::cmp::Ord> PeekMut<'a, T> {
    pub fn pop(this: Self) -> T {
        std::collections::binary_heap::PeekMut::pop(this.0).0
    }
}

impl<'a, T: std::cmp::Ord> std::ops::Deref for PeekMut<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0 .0
    }
}

impl<'a, T: std::cmp::Ord> std::ops::DerefMut for PeekMut<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0 .0
    }
}

#[cfg(test)]
mod tests {
    use super::{MinHeap, PeekMut};

    #[test]
    fn test_push_pop() {
        // Elements pushed into the heap should be popped back in increasing order.
        let mut heap = MinHeap::default();

        heap.push(7);
        heap.push(11);
        heap.push(4);
        heap.push(1);

        assert_eq!(heap.len(), 4);
        assert_eq!(heap.pop(), Some(1));
        assert_eq!(heap.pop(), Some(4));
        assert_eq!(heap.pop(), Some(7));
        assert_eq!(heap.pop(), Some(11));
        assert_eq!(heap.len(), 0);
        assert_eq!(heap.pop(), None);
    }

    #[test]
    fn test_push_pop_push() {
        // Elements pushed into the heap should be popped back in increasing order.
        let mut heap = MinHeap::default();

        heap.push(8);
        heap.push(16);
        heap.push(2);
        heap.push(1);

        assert_eq!(heap.len(), 4);
        assert_eq!(heap.pop(), Some(1));
        assert_eq!(heap.pop(), Some(2));
        assert_eq!(heap.len(), 2);

        heap.push(4);
        heap.push(32);

        assert_eq!(heap.len(), 4);
        assert_eq!(heap.pop(), Some(4));
        assert_eq!(heap.pop(), Some(8));
        assert_eq!(heap.pop(), Some(16));
        assert_eq!(heap.pop(), Some(32));
        assert_eq!(heap.pop(), None);
    }

    #[test]
    fn test_collect_pop() {
        // Elements collected into a heap should be popped back in increasing order.
        let mut heap: MinHeap<usize> = [9, 3, 100, 10, 5].iter().copied().collect();

        assert_eq!(heap.len(), 5);
        assert_eq!(heap.pop(), Some(3));
        assert_eq!(heap.pop(), Some(5));
        assert_eq!(heap.pop(), Some(9));
        assert_eq!(heap.pop(), Some(10));
        assert_eq!(heap.pop(), Some(100));
        assert_eq!(heap.pop(), None);
    }

    #[test]
    fn test_peek() {
        // Peek should reveal the smallest element, but not remove it
        let mut heap = MinHeap::default();

        heap.push(37);
        heap.push(17);
        heap.push(101);

        assert_eq!(Some(&17), heap.peek());
        assert_eq!(Some(17), heap.pop());

        assert_eq!(Some(&37), heap.peek());
        assert_eq!(Some(37), heap.pop());

        assert_eq!(Some(&101), heap.peek());
        assert_eq!(Some(101), heap.pop());

        assert_eq!(None, heap.peek());
        assert_eq!(None, heap.pop());
    }

    #[test]
    fn test_peek_mut() {
        // Peek should reveal the smallest element, but not remove it
        let mut heap = MinHeap::default();

        heap.push(37);
        heap.push(17);
        heap.push(101);

        let top = heap.peek_mut().unwrap();
        assert_eq!(17, *top);
        PeekMut::pop(top);
        assert_eq!(Some(&37), heap.peek());

        let mut top = heap.peek_mut().unwrap();
        assert_eq!(37, *top);
        *top = 42;
        // Drop so `top` no longer holds exclusive reference to `heap`.
        core::mem::drop(top);
        assert_eq!(Some(&42), heap.peek());

        let mut top = heap.peek_mut().unwrap();
        assert_eq!(42, *top);
        *top = 111;
        // This time dropping will also update position of the element in the
        // heap.
        core::mem::drop(top);

        assert_eq!(Some(101), heap.pop());
        assert_eq!(Some(111), heap.pop());
    }
}

use std::mem;
use std::ops::{Index, IndexMut};

#[derive(Debug, Clone)]
pub struct FreeList<T> {
    first_free: Option<usize>,
    store: Vec<Slot<T>>,
}

#[derive(Debug, Clone)]
enum Slot<T> {
    Full(T),
    Free(Option<usize>),
}

impl<T> FreeList<T> {
    fn new() -> Self {
        Self { first_free: None, store: Vec::new() }
    }

    pub fn insert(&mut self, item: T) -> usize {
        match self.first_free {
            None => {
                self.store.push(Slot::Full(item));
                self.store.len() - 1
            }
            Some(idx) => {
                let mut new_elem: Slot<T> = Slot::Full(item);
                mem::swap(&mut new_elem, &mut self.store[idx]);
                idx
            }
        }
    }

    pub fn delete(&mut self, idx: usize) -> T {
        let mut item: Slot<T> = Slot::Free(self.first_free);
        mem::swap(&mut item, &mut self.store[idx]);

        self.first_free = Some(idx);

        match item {
            Slot::Full(item) => item,
            Slot::Free(_) => panic!("bug"),
        }
    }

    pub fn len(&self) -> usize {
        self.store.len()
    }

    pub fn get(&self, idx: usize) -> Option<&T> {
        match &self.store[idx] {
            Slot::Full(item) => Some(item),
            Slot::Free(..) => None,
        }
    }

    pub fn iter(&self) -> FreeListIter<T> {
        FreeListIter { inner: self, pos: 0 }
    }
}

impl<T> Default for FreeList<T> {
    fn default() -> Self {
        FreeList::new()
    }
}

pub struct FreeListIter<'a, T: 'a> {
    inner: &'a FreeList<T>,
    pos: usize,
}

impl<'a, T> Iterator for FreeListIter<'a, T> {
    type Item = (usize, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        while self.pos < self.inner.store.len() {
            match &self.inner.store[self.pos] {
                Slot::Full(item) => {
                    self.pos += 1;
                    return Some((self.pos - 1, item));
                }
                Slot::Free { .. } => {}
            }
            self.pos += 1;
        }
        None
    }
}

impl<T> Index<usize> for FreeList<T> {
    type Output = T;

    fn index(&self, idx: usize) -> &Self::Output {
        match &self.store[idx] {
            Slot::Full(item) => &item,
            Slot::Free(_) => panic!("not found"),
        }
    }
}

impl<T> IndexMut<usize> for FreeList<T> {
    fn index_mut(&mut self, idx: usize) -> &mut Self::Output {
        match &mut self.store[idx] {
            Slot::Full(item) => item,
            Slot::Free(_) => panic!("not found"),
        }
    }
}

#[test]
fn free_list_test() {
    let mut list = FreeList::new();

    let a = list.insert("a");
    let b = list.insert("b");
    let c = list.insert("c");
    let d = list.insert("d");
    assert_eq!(a, 0);
    assert_eq!(b, 1);
    assert_eq!(c, 2);
    assert_eq!(d, 3);

    assert_eq!(list.delete(b), "b");
    assert_eq!(list.delete(c), "c");
    assert_eq!(list.delete(a), "a");

    assert_eq!(list.get(c), None);

    let e = list.insert("e");
    let f = list.insert("f");
    let g = list.insert("g");
    let h = list.insert("g");

    assert_eq!(list.len(), 5);
    assert_eq!(e, a);
    assert_eq!(f, b);
    assert_eq!(g, c);
    assert_eq!(h, 4);

    assert_eq!(list[d] == "d");
    assert_eq!(list[e] == "e");
    assert_eq!(list[f] == "f");
    assert_eq!(list[g] == "g");
    assert_eq!(list.get(4), Some(g));

    let vec = Vec::FromIter(list.iter());
    assert_eq!(vec, vec!["e", "f", "g", "h", "g"]);
}

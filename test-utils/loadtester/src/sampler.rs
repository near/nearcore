//! Functions to sample a collection of objects. The functions that accept predicate optimize not
//! for the runtime complexity but for how many times it runs the predicate. It is useful for
//! predicates that do expensive invocations, e.g. querying a node.
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::HashSet;

const EMPTY_COLLECTION: &str = "Collection is expected to be non-empty";
const COLLECTION_TOO_SMALL: &str = "Collection size is less than sample size";
const COLLECTION_EXHAUSTED: &str =
    "Not enough elements in the collection that satisfy the predicate";

/// Get one item from the collection.
pub fn sample_one<T>(collection: &[T]) -> &T {
    collection.choose(&mut thread_rng()).expect(EMPTY_COLLECTION)
}

/// Get one item from the collection that satisfy the given predicate.
pub fn sample_one_fn<T, F>(collection: &[T], f: F) -> &T
where
    F: Fn(&T) -> bool,
{
    let mut attempted_indices = HashSet::new();
    let mut i;
    let mut res;
    loop {
        i = rand::random::<usize>() % collection.len();
        // Check if already attempted.
        if attempted_indices.contains(&i) {
            continue;
        };

        res = &collection[i];
        if f(res) {
            break;
        } else {
            attempted_indices.insert(i);
            assert!(attempted_indices.len() < collection.len(), "{}", COLLECTION_EXHAUSTED);
        }
    }
    res
}

/// Get several items from the collections.
pub fn sample<T>(collection: &[T], sample_size: usize) -> Vec<&T> {
    assert!(collection.len() >= sample_size, "{}", COLLECTION_TOO_SMALL);
    collection.choose_multiple(&mut thread_rng(), sample_size).collect()
}

/// Get several items from the collection that satisfy the given predicate.
pub fn sample_fn<T, F>(collection: &[T], sample_size: usize, f: F) -> Vec<&T>
where
    F: Fn(&T) -> bool,
{
    let mut attempted_indices = HashSet::new();
    let mut i;
    let mut res = vec![];
    loop {
        i = rand::random::<usize>() % collection.len();
        // Check if already attempted.
        if attempted_indices.contains(&i) {
            continue;
        };

        let el = &collection[i];
        if f(el) {
            res.push(el);
            if res.len() == sample_size {
                break;
            }
        }
        attempted_indices.insert(i);
        assert!(attempted_indices.len() < collection.len(), "{}", COLLECTION_EXHAUSTED);
    }
    res
}

/// Binds a tuple to a vector.
/// # Examples:
///
/// ```
/// let v = vec![1,2,3];
/// tuplet!((a,b,c) = v);
/// assert_eq!(a, &1);
/// assert_eq!(b, &2);
/// assert_eq!(c, &3);
/// ```
macro_rules! tuplet {
    { ($y:ident $(, $x:ident)*) = $v:expr } => {
        let ($y, $($x),*) = tuplet!($v ; 1 ; ($($x),*) ; (&$v[0]) );
    };
    { $v:expr ; $j:expr ; ($y:ident $(, $x:ident)*) ; ($($a:expr),*) } => {
        tuplet!( $v ; $j+1 ; ($($x),*) ; ($($a),*,&$v[$j]) )
    };
    { $v:expr ; $j:expr ; () ; $accu:expr } => {
        $accu
    }
}

/// Sample two elements from the slice.
pub fn sample_two<T>(collection: &[T]) -> (&T, &T) {
    tuplet!((a, b) = sample(collection, 2));
    (a, b)
}

/// Sample two elements from the slice that satisfy the predicate.
pub fn sample_two_fn<T, F>(collection: &[T], f: F) -> (&T, &T)
where
    F: Fn(&T) -> bool,
{
    tuplet!((a, b) = sample_fn(collection, 2, &f));
    (a, b)
}

/// Sample three elements from the slice.
pub fn sample_three<T>(collection: &[T]) -> (&T, &T, &T) {
    tuplet!((a, b, c) = sample(collection, 3));
    (a, b, c)
}

/// Sample three elements from the slice that satisfy the predicate.
pub fn sample_three_fn<T, F>(collection: &[T], f: F) -> (&T, &T, &T)
where
    F: Fn(&T) -> bool,
{
    tuplet!((a, b, c) = sample_fn(collection, 3, &f));
    (a, b, c)
}

#[cfg(test)]
mod tests {
    use crate::sampler::{
        sample, sample_fn, sample_one, sample_one_fn, sample_three, sample_three_fn, sample_two,
        sample_two_fn,
    };

    #[test]
    fn test_sample_one() {
        let c = vec![1];
        assert_eq!(*sample_one(&c), 1);
    }

    #[test]
    #[should_panic]
    fn test_sample_one_panics() {
        let c: Vec<usize> = vec![];
        sample_one(&c);
    }

    #[test]
    fn test_sample_one_many() {
        for _ in 0..100 {
            let len = rand::random::<usize>() % 10 + 1;
            let c: Vec<usize> = (0..len).collect();
            sample_one(&c);
        }
    }

    #[test]
    fn test_sample_one_fn() {
        let c = vec![1, 2];
        assert_eq!(*sample_one_fn(&c, |el| *el == 1), 1);
    }

    #[test]
    #[should_panic]
    fn test_sample_one_fn_panics() {
        let c = vec![1, 2];
        sample_one_fn(&c, |_| false);
    }

    #[test]
    fn test_sample_one_fn_many() {
        for _ in 0..100 {
            let len = rand::random::<usize>() % 10 + 1;
            let c: Vec<usize> = (0..len).collect();
            sample_one_fn(&c, |el| *el % 2 == 0);
        }
    }

    #[test]
    fn test_sample() {
        let c = vec![1, 2, 3];
        let mut res = sample(&c, 3);
        res.sort();
        assert_eq!(res, c.iter().collect::<Vec<_>>());
    }

    #[test]
    #[should_panic]
    fn test_sample_panic1() {
        let c = vec![1, 2, 3];
        sample(&c, 4);
    }

    #[test]
    #[should_panic]
    fn test_sample_panic2() {
        let c: Vec<usize> = vec![];
        sample(&c, 1);
    }

    #[test]
    fn test_sample_many() {
        for _ in 0..100 {
            let len = rand::random::<usize>() % 10 + 1;
            let c: Vec<usize> = (0..len).collect();
            let mut res = sample(&c, len);
            res.sort();
            assert_eq!(res, c.iter().collect::<Vec<_>>());
        }
    }

    #[test]
    fn test_sample_fn() {
        let c = vec![1, 2, 3];
        let mut res = sample_fn(&c, 2, |e| *e > 1);
        res.sort();
        assert_eq!(res, c[1..].iter().collect::<Vec<_>>());
    }

    #[test]
    #[should_panic]
    fn test_sample_fn_panic1() {
        let c = vec![1, 2, 3];
        sample_fn(&c, 3, |e| *e > 1);
    }

    #[test]
    #[should_panic]
    fn test_sample_fn_panic2() {
        let c: Vec<usize> = vec![];
        sample_fn(&c, 1, |_| true);
    }

    #[test]
    fn test_sample_fn_many() {
        for _ in 0..100 {
            let len = rand::random::<usize>() % 10 + 1;
            let c: Vec<usize> = (0..len).collect();
            sample_fn(&c, (len + 1) / 2, |e| (*e) % 2 == 0);
        }
    }

    #[test]
    fn test_sample_two() {
        for _ in 0..100 {
            let len = rand::random::<usize>() % 4 + 2;
            let c: Vec<usize> = (0..len).collect();
            sample_two(&c);
        }
    }

    #[test]
    fn test_sample_two_fn() {
        for _ in 0..100 {
            let len = rand::random::<usize>() % 4 + 3;
            let c: Vec<usize> = (0..len).collect();
            sample_two_fn(&c, |e| *e > 0);
        }
    }

    #[test]
    fn test_sample_three() {
        for _ in 0..100 {
            let len = rand::random::<usize>() % 4 + 3;
            let c: Vec<usize> = (0..len).collect();
            sample_three(&c);
        }
    }

    #[test]
    fn test_sample_three_fn() {
        for _ in 0..100 {
            let len = rand::random::<usize>() % 4 + 4;
            let c: Vec<usize> = (0..len).collect();
            sample_three_fn(&c, |e| *e > 0);
        }
    }
}

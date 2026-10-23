use super::*;

fn pool() -> OrphanPool<u32> {
    OrphanPool::new(NonZeroUsize::new(2).unwrap())
}

#[test]
fn a_released_block_refuses_new_data() {
    let mut pool = pool();
    let block = hash(b"block");
    assert!(pool.insert(block, 1));

    assert_eq!(pool.release(5, &block), vec![1]);

    assert!(!pool.insert(block, 2));
    assert_eq!(pool.release(5, &block), Vec::<u32>::new());
}

#[test]
fn released_block_refuses_data_only_above_final_head() {
    let mut pool = pool();
    let (at_final, above_final) = (hash(b"at"), hash(b"above"));
    let _ = pool.release(5, &at_final);
    let _ = pool.release(6, &above_final);

    pool.prune_at_or_below(5);

    assert!(pool.insert(at_final, 1));
    assert!(!pool.insert(above_final, 2));
}

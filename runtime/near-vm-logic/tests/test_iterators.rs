mod fixtures;

use crate::fixtures::get_context;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::mocks::mock_memory::MockedMemory;
use near_vm_logic::{Config, VMLogic};

fn add_key_vals(logic: &mut VMLogic, key_vals: &[(&[u8], &[u8])]) {
    for (key, val) in key_vals {
        logic
            .storage_write(key.len() as _, key.as_ptr() as _, val.len() as _, val.as_ptr() as _, 0)
            .expect("storage write ok");
    }
}

fn iter_prefix_check(logic: &mut VMLogic, prefix: &[u8], key_vals: &[(&[u8], &[u8])]) -> u64 {
    let iter_id = logic
        .storage_iter_prefix(prefix.len() as _, prefix.as_ptr() as _)
        .expect("create iterator ok");
    for (key, val) in key_vals {
        assert_eq!(
            logic.storage_iter_next(iter_id, 0, 1).unwrap(),
            1,
            "key: {:?}, val: {:?} expected",
            key,
            val
        );
        let res_key = vec![0u8; logic.register_len(0).unwrap() as usize];
        let res_val = vec![0u8; logic.register_len(1).unwrap() as usize];
        logic.read_register(0, res_key.as_ptr() as _).unwrap();
        logic.read_register(1, res_val.as_ptr() as _).unwrap();
        assert_eq!(&res_key, key);
        assert_eq!(&res_val, val);
    }
    iter_id
}

fn iter_range_check(
    logic: &mut VMLogic,
    start: &[u8],
    end: &[u8],
    key_vals: &[(&[u8], &[u8])],
) -> u64 {
    let iter_id = logic
        .storage_iter_range(
            start.len() as _,
            start.as_ptr() as _,
            end.len() as _,
            end.as_ptr() as _,
        )
        .expect("create iterator ok");
    for (key, val) in key_vals {
        assert_eq!(
            logic.storage_iter_next(iter_id, 0, 1).unwrap(),
            1,
            "key: {:?}, val: {:?} expected",
            key,
            val
        );
        let res_key = vec![0u8; logic.register_len(0).unwrap() as usize];
        let res_val = vec![0u8; logic.register_len(1).unwrap() as usize];
        logic.read_register(0, res_key.as_ptr() as _).unwrap();
        logic.read_register(1, res_val.as_ptr() as _).unwrap();
        assert_eq!(&res_key, key);
        assert_eq!(&res_val, val);
    }
    iter_id
}

#[test]
fn test_iterator() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![]);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);

    add_key_vals(&mut logic, &[(b"foo1", b"bar1"), (b"foo2", b"bar2")]);
    let iter_id = iter_prefix_check(&mut logic, b"foo", &[(b"foo1", b"bar1"), (b"foo2", b"bar2")]);
    // iterator exhausted
    assert_eq!(logic.storage_iter_next(iter_id, 0, 1).unwrap(), 0);
}

#[test]
fn test_iterator_invalidation() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![]);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);

    let iter_id = logic.storage_iter_prefix(1, b"a".as_ptr() as _).unwrap();
    add_key_vals(&mut logic, &[(b"f3", b"a")]);
    logic.storage_iter_next(iter_id, 0, 1).expect_err("storage has changed, iterator is invalid");
}

#[test]
fn test_iterator_from_second() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![]);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);

    add_key_vals(&mut logic, &[(b"aa", b"bar1"), (b"aaa", b"bar2"), (b"aaaa", b"bar3")]);
    let iter_id = iter_prefix_check(&mut logic, b"aaa", &[(b"aaa", b"bar2"), (b"aaaa", b"bar3")]);
    // iterator exhausted
    assert_eq!(logic.storage_iter_next(iter_id, 0, 1).unwrap(), 0);
}

#[test]
fn test_iterator_range() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![]);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);

    add_key_vals(
        &mut logic,
        &[(b"aa", b"bar1"), (b"aaa", b"bar2"), (b"ab", b"bar2"), (b"abb", b"bar3")],
    );
    iter_range_check(
        &mut logic,
        b"aaa",
        b"abb",
        &[(b"aaa", b"bar2"), (b"ab", b"bar2")],
    );
}

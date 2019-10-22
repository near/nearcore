mod fixtures;

use crate::fixtures::get_context;
use near_runtime_fees::RuntimeFeesConfig;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::mocks::mock_memory::MockedMemory;
use near_vm_logic::{VMConfig, VMLogic};

struct KeyVal<'a>(&'a [u8], &'a [u8]);

fn add_key_vals(logic: &mut VMLogic, key_vals: &[KeyVal]) {
    for KeyVal(key, val) in key_vals {
        logic
            .storage_write(key.len() as _, key.as_ptr() as _, val.len() as _, val.as_ptr() as _, 0)
            .expect("storage write ok");
    }
}

fn iter_prefix_check(
    logic: &mut VMLogic,
    prefix: &[u8],
    key_vals: &[KeyVal],
    use_register: bool,
) -> u64 {
    let iter_id = if use_register {
        logic.write_register(3, prefix).unwrap();
        logic.storage_iter_prefix(std::u64::MAX, 3)
    } else {
        logic.storage_iter_prefix(prefix.len() as _, prefix.as_ptr() as _)
    }
    .expect("create iterator ok");

    for KeyVal(key, val) in key_vals {
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
    key_vals: &[KeyVal],
    use_register: bool,
) -> u64 {
    let iter_id = if use_register {
        logic.write_register(3, start).unwrap();
        logic.write_register(4, end).unwrap();
        logic.storage_iter_range(std::u64::MAX, 0, std::u64::MAX, 1)
    } else {
        logic.storage_iter_range(
            start.len() as _,
            start.as_ptr() as _,
            end.len() as _,
            end.as_ptr() as _,
        )
    }
    .expect("create iterator ok");

    for KeyVal(key, val) in key_vals {
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

fn test_iterator(use_register: bool) {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], false);
    let config = VMConfig::default();
    let fees = RuntimeFeesConfig::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &fees, &promise_results, &mut memory);

    add_key_vals(&mut logic, &[KeyVal(b"foo1", b"bar1"), KeyVal(b"foo2", b"bar2")]);
    let iter_id = iter_prefix_check(
        &mut logic,
        b"foo",
        &[KeyVal(b"foo1", b"bar1"), KeyVal(b"foo2", b"bar2")],
        use_register,
    );
    // iterator exhausted
    assert_eq!(logic.storage_iter_next(iter_id, 0, 1).unwrap(), 0);
}

fn test_iterator_from_second(use_register: bool) {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], false);
    let config = VMConfig::default();
    let fees = RuntimeFeesConfig::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &fees, &promise_results, &mut memory);

    add_key_vals(
        &mut logic,
        &[KeyVal(b"aaa", b"bar1"), KeyVal(b"aaa", b"bar2"), KeyVal(b"aaaa", b"bar3")],
    );
    let iter_id = iter_prefix_check(
        &mut logic,
        b"aaa",
        &[KeyVal(b"aaa", b"bar2"), KeyVal(b"aaaa", b"bar3")],
        use_register,
    );
    // iterator exhausted
    assert_eq!(logic.storage_iter_next(iter_id, 0, 1).unwrap(), 0);
}

#[test]
fn test_iterator_invalidation() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], false);
    let config = VMConfig::default();
    let fees = RuntimeFeesConfig::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &fees, &promise_results, &mut memory);

    let iter_id = logic.storage_iter_prefix(1, b"a".as_ptr() as _).unwrap();
    add_key_vals(&mut logic, &[KeyVal(b"f3", b"a")]);
    logic.storage_iter_next(iter_id, 0, 1).expect_err("storage has changed, iterator is invalid");
}

#[test]
fn test_iterator_range() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], false);
    let config = VMConfig::default();
    let fees = RuntimeFeesConfig::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &fees, &promise_results, &mut memory);

    add_key_vals(
        &mut logic,
        &[
            KeyVal(b"aa", b"bar1"),
            KeyVal(b"aaa", b"bar2"),
            KeyVal(b"ab", b"bar2"),
            KeyVal(b"abb", b"bar3"),
        ],
    );
    let id0 = iter_range_check(
        &mut logic,
        b"aaa",
        b"abb",
        &[KeyVal(b"aaa", b"bar2"), KeyVal(b"ab", b"bar2")],
        false,
    );
    assert_eq!(id0, 0);
    let id1 = iter_range_check(
        &mut logic,
        b"aaa",
        b"abb",
        &[KeyVal(b"aaa", b"bar2"), KeyVal(b"ab", b"bar2")],
        false,
    );
    assert_eq!(id1, 1);
}

#[test]
#[should_panic]
fn test_iterator_range_intersect() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], false);
    let config = VMConfig::default();
    let fees = RuntimeFeesConfig::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &fees, &promise_results, &mut memory);

    add_key_vals(
        &mut logic,
        &[
            KeyVal(b"aa", b"bar1"),
            KeyVal(b"aaa", b"bar2"),
            KeyVal(b"ab", b"bar2"),
            KeyVal(b"abb", b"bar3"),
        ],
    );
    iter_range_check(&mut logic, b"ab", b"a", &[], false);
}

#[test]
fn test_iterator_from_register() {
    test_iterator(true);
}

#[test]
fn test_iterator_memory() {
    test_iterator(false);
}

#[test]
fn test_iterator_from_second_register() {
    test_iterator_from_second(true);
}

#[test]
fn test_iterator_from_second_guest_mem() {
    test_iterator_from_second(false);
}

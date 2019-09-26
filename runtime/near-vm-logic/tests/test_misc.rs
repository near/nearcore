mod fixtures;

use crate::fixtures::get_context;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::mocks::mock_memory::MockedMemory;
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{Config, VMLogic};
use serde_json;

#[test]
fn test_iterator() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![]);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);

    let key = b"foo1";
    let val = b"bar1";

    logic
        .storage_write(key.len() as _, key.as_ptr() as _, val.len() as _, val.as_ptr() as _, 0)
        .expect("storage write ok");

    let key2 = b"foo2";
    let val2 = b"bar2";

    logic
        .storage_write(key2.len() as _, key2.as_ptr() as _, val2.len() as _, val2.as_ptr() as _, 0)
        .expect("storage write ok");

    let prefix = b"foo";
    let iterator_id = logic
        .storage_iter_prefix(prefix.len() as _, prefix.as_ptr() as _)
        .expect("create iterator ok");
    let key_vals = vec![(key, val), (key2, val2)];
    for (key, val) in key_vals {
        if logic.storage_iter_next(iterator_id, 0, 1).unwrap() == 1 {
            let res_key = vec![0u8; logic.register_len(0).unwrap() as usize];
            let res_val = vec![0u8; logic.register_len(1).unwrap() as usize];
            logic.read_register(0, res_key.as_ptr() as _);
            logic.read_register(1, res_val.as_ptr() as _);
            assert_eq!(res_key, key);
            assert_eq!(res_val, val);
        } else {
            panic!("No value in iterator");
        }
    }
    assert_eq!(logic.storage_iter_next(iterator_id, 0, 1).unwrap(), 0);
}

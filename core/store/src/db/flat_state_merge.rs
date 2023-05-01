use borsh::BorshDeserialize;
use near_primitives::hash::hash;

use crate::db::RocksDB;
use crate::flat::FlatStateValue;

impl RocksDB {
    pub(crate) fn flat_state_merge(
        _new_key: &[u8],
        existing: Option<&[u8]>,
        operands: &rocksdb::MergeOperands,
    ) -> Option<Vec<u8>> {
        existing.map(|existing_value| {
            if let Ok(FlatStateValue::Ref(value_ref)) = FlatStateValue::try_from_slice(existing_value) {
                for operand in operands {
                    if let Ok(FlatStateValue::Inlined(value)) = FlatStateValue::try_from_slice(operand) {
                        if hash(&value) == value_ref.hash {
                            return operand.to_vec();
                        }
                    }
                }
            }
            existing_value.to_vec()
        })
    }
}


#[cfg(test)]
mod tests {
    use crate::flat::FlatStateValue;
    use crate::{DBCol, NodeStorage, Store};
    use borsh::BorshSerialize;

    const KEY: &[u8] = b"123";
    const VALUE: &[u8] = b"456";

    #[test]
    fn inline_success() {
        let store = create_store_with_value_ref();
        merge_value(&store, &inlined(VALUE));
        assert_eq!(get_value(&store), Some(inlined(VALUE)));
    }

    #[test]
    fn inlined_after_update() {
        fn check_with_updated_value(updated_value: Vec<u8>) {
            let store = create_store_with_value_ref();
            set_value(&store, &updated_value);
            merge_value(&store, &inlined(VALUE));
            assert_eq!(get_value(&store), Some(updated_value));
        }

        check_with_updated_value(value_ref(b"updated_value"));
        check_with_updated_value(inlined(b"updated_value"));
    }

    #[test]
    fn inlined_after_delete() {
        let store = create_store_with_value_ref();
        delete_value(&store);
        merge_value(&store, &inlined(VALUE));
        assert_eq!(get_value(&store), None);
    }

    fn create_store_with_value_ref() -> Store {
        let store = NodeStorage::test_opener().1.open().unwrap().get_hot_store();
        let mut store_update = store.store_update();
        store_update.set(DBCol::FlatState, KEY, &value_ref(VALUE));
        store_update.commit().unwrap();
        store
    }

    fn set_value(store: &Store, value: &[u8]) {
        let mut store_update = store.store_update();
        store_update.set(DBCol::FlatState, KEY, &value);
        store_update.commit().unwrap();
    }

    fn delete_value(store: &Store) {
        let mut store_update = store.store_update();
        store_update.delete(DBCol::FlatState, KEY);
        store_update.commit().unwrap();
    }

    fn merge_value(store: &Store, value: &[u8]) {
        let mut store_update = store.store_update();
        store_update.merge_value(DBCol::FlatState, KEY, value);
        store_update.commit().unwrap();
    }

    fn get_value(store: &Store) -> Option<Vec<u8>> {
        store.get(DBCol::FlatState, KEY).unwrap().map(|slice| slice.to_vec())
    }

    fn inlined(bytes: &[u8]) -> Vec<u8> {
        FlatStateValue::inlined(bytes).try_to_vec().unwrap()
    }

    fn value_ref(bytes: &[u8]) -> Vec<u8> {
        FlatStateValue::value_ref(bytes).try_to_vec().unwrap()
    }
}

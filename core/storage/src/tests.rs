use super::*;

#[test]
fn state_db() {
    let storage = Arc::new(kvdb_memorydb::create(5));
    let state_db = Arc::new(StateDb::new(storage.clone()));
    let root = CryptoHash::default();
    let mut state_db_update = StateDbUpdate::new(state_db.clone(), root);
    state_db_update.set(b"dog", DBValue::from_slice(b"puppy"));
    state_db_update.set(b"dog2", DBValue::from_slice(b"puppy"));
    state_db_update.set(b"dog3", DBValue::from_slice(b"puppy"));
    let (mut transaction, new_root) = state_db_update.finalize();
    state_db.commit(&mut transaction).ok();
    let state_db_update2 = StateDbUpdate::new(state_db.clone(), new_root);
    assert_eq!(state_db_update2.get(b"dog").unwrap(), DBValue::from_slice(b"puppy"));
}

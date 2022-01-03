use near_store::create_store;
use nearcore::{get_default_home, get_store_path};

fn main() {
    let home_dir = get_default_home();
    let store = create_store(&get_store_path(&home_dir));
    let db = store.get_rocksdb().unwrap();
    db.dump_stats();
}

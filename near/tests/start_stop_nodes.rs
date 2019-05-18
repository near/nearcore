use near_primitives::test_utils::init_test_logger;
use near::{GenesisConfig, load_test_config};
use near_network::test_utils::convert_boot_nodes;
use actix::System;

// Start both clients, stop one of them, restart again, stop first one.
// At the end they should be in sync.
//#[test]
//fn start_stop_nodes() {
//    init_test_logger();
//
//    let genesis_config = GenesisConfig::test(vec!["test1", "test2"]);
//
//    let (mut near1, bp1) = load_test_configs("test1", 25123, &genesis_config);
//    let (mut near2, bp2) = load_test_configs("test2", 25124, &genesis_config);
//    near2.network_config.boot_nodes = convert_boot_nodes(vec![("test1", 25123)]);
//
//    let system = System::new("NEAR");
//
//}
use actix::System;
use futures::future;
use futures::future::Future;

use near_client::test_utils::setup_no_network;
use near_client::Query;
use near_primitives::test_utils::init_test_logger;

/// Query account from view client
#[test]
fn query_client() {
    init_test_logger();
    System::run(|| {
        let (_, view_client) = setup_no_network(vec!["test"], "other", true);
        actix::spawn(
            view_client.send(Query { path: "account/test".to_string(), data: vec![] }).then(
                |res| {
                    assert_eq!(res.unwrap().unwrap().log, "exists");
                    System::current().stop();
                    future::result(Ok(()))
                },
            ),
        );
    })
    .unwrap();
}

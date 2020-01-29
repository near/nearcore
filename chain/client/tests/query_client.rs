use actix::System;
use futures::{future, FutureExt};

use near_client::test_utils::setup_no_network;
use near_client::Query;
use near_primitives::test_utils::init_test_logger;
use near_primitives::views::QueryResponseKind;

/// Query account from view client
#[test]
fn query_client() {
    init_test_logger();
    System::run(|| {
        let (_, view_client) = setup_no_network(vec!["test"], "other", true, true);
        actix::spawn(view_client.send(Query::new("account/test".to_string(), vec![])).then(
            |res| {
                match res.unwrap().unwrap().unwrap().kind {
                    QueryResponseKind::ViewAccount(_) => (),
                    _ => panic!("Invalid response"),
                }
                System::current().stop();
                future::ready(())
            },
        ));
    })
    .unwrap();
}

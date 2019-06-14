use std::sync::{Arc, RwLock};

use actix::actors::mocker::Mocker;
use actix::{Actor, Addr, AsyncContext, Context, Recipient};
use chrono::Utc;

use near_chain::test_utils::KeyValueRuntime;
use near_network::{NetworkRequests, NetworkResponses, PeerManagerActor};
use near_primitives::crypto::signer::InMemorySigner;
use near_store::test_utils::create_test_store;

use crate::{BlockProducer, ClientActor, ClientConfig, ViewClientActor};

pub type NetworkMock = Mocker<PeerManagerActor>;

/// Sets up ClientActor and ViewClientActor viewing the same store/runtime.
pub fn setup(
    validators: Vec<&str>,
    account_id: &str,
    skip_sync_wait: bool,
    recipient: Recipient<NetworkRequests>,
) -> (ClientActor, ViewClientActor) {
    let store = create_test_store();
    let runtime = Arc::new(KeyValueRuntime::new_with_validators(
        store.clone(),
        validators.into_iter().map(Into::into).collect(),
    ));
    let signer = Arc::new(InMemorySigner::from_seed(account_id, account_id));
    let genesis_time = Utc::now();
    let view_client =
        ViewClientActor::new(store.clone(), genesis_time.clone(), runtime.clone()).unwrap();
    let client = ClientActor::new(
        ClientConfig::test(skip_sync_wait),
        store,
        genesis_time,
        runtime,
        recipient,
        Some(signer.into()),
    )
    .unwrap();
    (client, view_client)
}

/// Sets up ClientActor and ViewClientActor with mock PeerManager.
pub fn setup_mock(
    validators: Vec<&'static str>,
    account_id: &'static str,
    skip_sync_wait: bool,
    mut network_mock: Box<
        dyn FnMut(
            &NetworkRequests,
            &mut Context<NetworkMock>,
            Addr<ClientActor>,
        ) -> NetworkResponses,
    >,
) -> (Addr<ClientActor>, Addr<ViewClientActor>) {
    let view_client_addr = Arc::new(RwLock::new(None));
    let view_client_addr1 = view_client_addr.clone();
    let client_addr = ClientActor::create(move |ctx| {
        let client_addr = ctx.address();
        let pm = NetworkMock::mock(Box::new(move |msg, ctx| {
            let msg = msg.downcast_ref::<NetworkRequests>().unwrap();
            let resp = network_mock(msg, ctx, client_addr.clone());
            Box::new(Some(resp))
        }))
        .start();
        let (client, view_client) = setup(validators, account_id, skip_sync_wait, pm.recipient());
        *view_client_addr1.write().unwrap() = Some(view_client.start());
        client
    });
    (client_addr, view_client_addr.clone().read().unwrap().clone().unwrap())
}

/// Sets up ClientActor and ViewClientActor without network.
pub fn setup_no_network(
    validators: Vec<&'static str>,
    account_id: &'static str,
    skip_sync_wait: bool,
) -> (Addr<ClientActor>, Addr<ViewClientActor>) {
    setup_mock(
        validators,
        account_id,
        skip_sync_wait,
        Box::new(|_, _, _| NetworkResponses::NoResponse),
    )
}

impl BlockProducer {
    pub fn test(seed: &str) -> Self {
        Arc::new(InMemorySigner::from_seed(seed, seed)).into()
    }
}

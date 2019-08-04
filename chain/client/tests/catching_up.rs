#[cfg(test)]
#[cfg(feature = "expensive_tests")]
mod tests {
    use actix::{Addr, System};
    use near_client::test_utils::setup_mock_all_validators;
    use near_client::{ClientActor, ViewClientActor};
    use near_network::{NetworkClientMessages, NetworkRequests, NetworkResponses, PeerInfo};
    use near_primitives::hash::CryptoHash;
    use near_primitives::test_utils::init_test_logger;
    use near_primitives::transaction::SignedTransaction;
    use std::collections::hash_map::Entry;
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    fn get_validators_and_key_pairs() -> (Vec<Vec<&'static str>>, Vec<PeerInfo>) {
        let validators = vec![
            vec!["test1.1", "test1.2", "test1.3", "test1.4"],
            vec!["test2.1", "test2.2", "test2.3", "test2.4"],
            vec![
                "test3.1", "test3.2", "test3.3", "test3.4", "test3.5", "test3.6", "test3.7",
                "test3.8",
            ],
        ];
        let key_pairs = vec![
            PeerInfo::random(),
            PeerInfo::random(),
            PeerInfo::random(),
            PeerInfo::random(), // 4
            PeerInfo::random(),
            PeerInfo::random(),
            PeerInfo::random(),
            PeerInfo::random(), // 8
            PeerInfo::random(),
            PeerInfo::random(),
            PeerInfo::random(),
            PeerInfo::random(),
            PeerInfo::random(),
            PeerInfo::random(),
            PeerInfo::random(),
            PeerInfo::random(), // 16
        ];
        (validators, key_pairs)
    }

    fn send_tx(connector: &Addr<ClientActor>, from: String, to: String, amount: u128, nonce: u64) {
        connector.do_send(NetworkClientMessages::Transaction(
            SignedTransaction::create_payment_tx(from, to, amount, nonce),
        ));
    }

    enum ReceiptsSyncPhases {
        WaitingForFirstBlock,
        WaitingForSecondBlock,
        WaitingForSecondEpoch,
        VerifyingOutgoingReceipts,
    }

    /// Sanity checks that the incoming and outgoing receipts are properly sent and received
    #[test]
    fn test_catchup_receipts_sync() {
        let validator_groups = 1;
        init_test_logger();
        System::run(move || {
            let connectors: Arc<RwLock<Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>>> =
                Arc::new(RwLock::new(vec![]));

            let (validators, key_pairs) = get_validators_and_key_pairs();

            let phase = Arc::new(RwLock::new(ReceiptsSyncPhases::WaitingForFirstBlock));

            let connectors1 = connectors.clone();
            *connectors.write().unwrap() = setup_mock_all_validators(
                validators.clone(),
                key_pairs.clone(),
                validator_groups,
                true,
                200,
                Arc::new(RwLock::new(move |_account_id: String, msg: &NetworkRequests| {
                    let mut phase = phase.write().unwrap();
                    match *phase {
                        ReceiptsSyncPhases::WaitingForFirstBlock => {
                            if let NetworkRequests::Block { block } = msg {
                                assert_eq!(block.header.height, 1);
                                send_tx(
                                    &connectors1.write().unwrap()[2].0,
                                    "test1.1".to_string(),
                                    "test1.2".to_string(),
                                    100,
                                    1,
                                );
                                *phase = ReceiptsSyncPhases::WaitingForSecondBlock;
                            }
                        }
                        ReceiptsSyncPhases::WaitingForSecondBlock => {
                            // This block now contains a chunk with the transaction sent above.
                            if let NetworkRequests::Block { block } = msg {
                                assert!(block.header.height <= 2);
                                if block.header.height == 2 {
                                    *phase = ReceiptsSyncPhases::WaitingForSecondEpoch;
                                }
                            }
                        }
                        ReceiptsSyncPhases::WaitingForSecondEpoch => {
                            // This block now contains a chunk with the transaction sent above.
                            if let NetworkRequests::Block { block } = msg {
                                assert!(block.header.height >= 2);
                                assert!(block.header.height <= 8);
                                if block.header.height == 8 {
                                    *phase = ReceiptsSyncPhases::VerifyingOutgoingReceipts;
                                }
                            }
                        }
                        ReceiptsSyncPhases::VerifyingOutgoingReceipts => {
                            System::current().stop();
                        }
                    };
                    (NetworkResponses::NoResponse, true)
                })),
            );

            near_network::test_utils::wait_or_panic(30000);
        })
        .unwrap();
    }

    /// Makes sure that 24 consecutive blocks are produced by 12 validators split into three epochs.
    /// This ensures that at no point validators get stuck with state sync
    #[test]
    fn test_catchup_sanity_blocks_produced() {
        let validator_groups = 2;
        init_test_logger();
        System::run(move || {
            let connectors: Arc<RwLock<Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>>> =
                Arc::new(RwLock::new(vec![]));

            let heights = Arc::new(RwLock::new(HashMap::new()));
            let heights1 = heights.clone();

            let check_height =
                move |hash: CryptoHash, height| match heights1.write().unwrap().entry(hash.clone())
                {
                    Entry::Occupied(entry) => {
                        assert_eq!(*entry.get(), height);
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(height);
                    }
                };

            let (validators, key_pairs) = get_validators_and_key_pairs();

            *connectors.write().unwrap() = setup_mock_all_validators(
                validators.clone(),
                key_pairs.clone(),
                validator_groups,
                true,
                200,
                Arc::new(RwLock::new(move |_account_id: String, msg: &NetworkRequests| {
                    if let NetworkRequests::Block { block } = msg {
                        check_height(block.hash(), block.header.height);
                        check_height(block.header.prev_hash, block.header.height - 1);

                        if block.header.height >= 25 {
                            System::current().stop();
                        }
                    }
                    (NetworkResponses::NoResponse, true)
                })),
            );

            near_network::test_utils::wait_or_panic(30000);
        })
        .unwrap();
    }
}

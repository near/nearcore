#[cfg(test)]
#[cfg(feature = "expensive_tests")]
mod tests {
    use actix::{Addr, System};
    use futures::future;
    use futures::future::Future;
    use near_chain::test_utils::account_id_to_shard_id;
    use near_client::test_utils::setup_mock_all_validators;
    use near_client::{ClientActor, Query, ViewClientActor};
    use near_network::{NetworkClientMessages, NetworkRequests, NetworkResponses, PeerInfo};
    use near_primitives::hash::CryptoHash;
    use near_primitives::rpc::QueryResponse::ViewAccount;
    use near_primitives::test_utils::init_integration_logger;
    use near_primitives::transaction::SignedTransaction;
    use near_primitives::types::BlockIndex;
    use std::collections::hash_map::Entry;
    use std::collections::{HashMap, HashSet};
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
        WaitingForThirdEpoch,
        VerifyingOutgoingReceipts,
        WaitingForFifthEpoch,
    }

    /// Sanity checks that the incoming and outgoing receipts are properly sent and received
    #[test]
    fn test_catchup_receipts_sync() {
        let validator_groups = 1;
        init_integration_logger();
        System::run(move || {
            let connectors: Arc<RwLock<Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>>> =
                Arc::new(RwLock::new(vec![]));

            let (validators, key_pairs) = get_validators_and_key_pairs();

            let phase = Arc::new(RwLock::new(ReceiptsSyncPhases::WaitingForFirstBlock));
            let seen_heights_with_receipts = Arc::new(RwLock::new(HashSet::<BlockIndex>::new()));

            let connectors1 = connectors.clone();
            *connectors.write().unwrap() = setup_mock_all_validators(
                validators.clone(),
                key_pairs.clone(),
                validator_groups,
                true,
                400,
                Arc::new(RwLock::new(move |_account_id: String, msg: &NetworkRequests| {
                    let account_from = "test3.3".to_string();
                    let account_to = "test1.1".to_string();
                    let source_shard_id = account_id_to_shard_id(&account_from, 4);
                    let destination_shard_id = account_id_to_shard_id(&account_to, 4);

                    let mut phase = phase.write().unwrap();
                    let mut seen_heights_with_receipts =
                        seen_heights_with_receipts.write().unwrap();
                    match *phase {
                        ReceiptsSyncPhases::WaitingForFirstBlock => {
                            if let NetworkRequests::Block { block } = msg {
                                assert_eq!(block.header.height, 1);
                                // This tx is rather fragile, specifically it's important that
                                //   1. the `from` and `to` account are not in the same shard;
                                //   2. ideally the producer of the chunk at height 3 for the shard
                                //      in which `from` resides should not also be a block producer
                                //      at height 3
                                //   3. The `from` shard should also not match the block producer
                                //      for height 1, because such block producer will produce
                                //      the chunk for height 2 right away, before we manage to send
                                //      the transaction.
                                println!(
                                    "From shard: {}, to shard: {}",
                                    source_shard_id, destination_shard_id,
                                );
                                send_tx(
                                    &connectors1.write().unwrap()[0].0,
                                    account_from,
                                    account_to,
                                    111,
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
                                    *phase = ReceiptsSyncPhases::WaitingForThirdEpoch;
                                }
                            }
                        }
                        ReceiptsSyncPhases::WaitingForThirdEpoch => {
                            // This block now contains a chunk with the transaction sent above.
                            if let NetworkRequests::Block { block } = msg {
                                assert!(block.header.height >= 2);
                                assert!(block.header.height <= 13);
                                if block.header.height == 13 {
                                    *phase = ReceiptsSyncPhases::VerifyingOutgoingReceipts;
                                }
                            }
                            if let NetworkRequests::ChunkOnePartMessage {
                                header_and_part, ..
                            } = msg
                            {
                                // The chunk producers in all three epochs need to be trying to
                                //     include the receipt. The third epoch is the first one that
                                //     will get the receipt through the state sync.
                                if header_and_part.receipts.len() > 0 {
                                    assert_eq!(header_and_part.shard_id, source_shard_id);
                                    seen_heights_with_receipts
                                        .insert(header_and_part.header.height_created);
                                } else {
                                    assert_ne!(header_and_part.shard_id, source_shard_id);
                                }
                                // Do not propagate any one parts, this will prevent any chunk from
                                //    being included in the block
                                return (NetworkResponses::NoResponse, false);
                            }
                        }
                        ReceiptsSyncPhases::VerifyingOutgoingReceipts => {
                            for height in 3..=13 {
                                assert!(seen_heights_with_receipts.contains(&height));
                            }
                            *phase = ReceiptsSyncPhases::WaitingForFifthEpoch;
                        }
                        ReceiptsSyncPhases::WaitingForFifthEpoch => {
                            // This block now contains a chunk with the transaction sent above.
                            if let NetworkRequests::Block { block } = msg {
                                assert!(block.header.height >= 13);
                                assert!(block.header.height <= 23);
                                if block.header.height == 23 {
                                    actix::spawn(
                                        connectors1.write().unwrap()[5] // 5th account is one of the validators of epoch 5
                                            .1
                                            .send(Query {
                                                path: "account/".to_owned() + &account_to,
                                                data: vec![],
                                            })
                                            .then(move |res| {
                                                let query_responce = res.unwrap().unwrap();
                                                if let ViewAccount(view_account_result) =
                                                    query_responce
                                                {
                                                    assert_eq!(view_account_result.amount, 1111);
                                                    System::current().stop();
                                                }

                                                future::result(Ok(()))
                                            }),
                                    );
                                }
                            }
                        }
                    };
                    (NetworkResponses::NoResponse, true)
                })),
            );

            near_network::test_utils::wait_or_panic(30000);
        })
        .unwrap();
    }

    enum RandomSinglePartPhases {
        WaitingForFirstBlock,
        WaitingForThirdEpoch,
        WaitingForSixEpoch,
    }

    /// Test test
    #[test]
    fn test_catchup_random_single_part_sync() {
        let validator_groups = 2;
        init_integration_logger();
        System::run(move || {
            let connectors: Arc<RwLock<Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>>> =
                Arc::new(RwLock::new(vec![]));

            let (validators, key_pairs) = get_validators_and_key_pairs();
            let flat_validators = validators.iter().flatten().map(|x| *x).collect::<Vec<_>>();

            let phase = Arc::new(RwLock::new(RandomSinglePartPhases::WaitingForFirstBlock));
            let seen_heights_same_block = Arc::new(RwLock::new(HashSet::<CryptoHash>::new()));
            let seen_receipts_size = Arc::new(RwLock::new(HashSet::<usize>::new()));

            let amounts = Arc::new(RwLock::new(HashMap::new()));

            let check_amount =
                move |amounts: Arc<RwLock<HashMap<_, _, _>>>, account_id: String, amount: u128| {
                    match amounts.write().unwrap().entry(account_id.clone()) {
                        Entry::Occupied(entry) => {
                            println!("OCCUPIED {:?}", entry);
                            assert_eq!(*entry.get(), amount);
                        }
                        Entry::Vacant(entry) => {
                            println!("VACANT {:?}", entry);
                            assert_eq!(amount % 100, 0);
                            entry.insert(amount);
                        }
                    }
                };

            let connectors1 = connectors.clone();
            *connectors.write().unwrap() = setup_mock_all_validators(
                validators.clone(),
                key_pairs.clone(),
                validator_groups,
                true,
                1500,
                Arc::new(RwLock::new(move |_account_id: String, msg: &NetworkRequests| {
                    let mut seen_heights_same_block = seen_heights_same_block.write().unwrap();
                    let mut seen_receipts_size = seen_receipts_size.write().unwrap();
                    let mut phase = phase.write().unwrap();
                    match *phase {
                        RandomSinglePartPhases::WaitingForFirstBlock => {
                            if let NetworkRequests::Block { block } = msg {
                                assert_eq!(block.header.height, 1);
                                *phase = RandomSinglePartPhases::WaitingForThirdEpoch;
                            }
                        }
                        RandomSinglePartPhases::WaitingForThirdEpoch => {
                            if let NetworkRequests::Block { block } = msg {
                                assert!(block.header.height >= 2);
                                assert!(block.header.height <= 13);
                                let mut tx_count = 0;
                                if block.header.height == 13 {
                                    for (i, validator1) in flat_validators.iter().enumerate() {
                                        for (j, validator2) in flat_validators.iter().enumerate() {
                                            println!(
                                                "VALUES {:?} {:?} {:?}",
                                                validator1.to_string(),
                                                validator2.to_string(),
                                                (((i + j + 17) * 701) % 42 + 1) as u128
                                            );
                                            for conn in 0..flat_validators.len() {
                                                send_tx(
                                                    &connectors1.write().unwrap()[conn].0,
                                                    validator1.to_string(),
                                                    validator2.to_string(),
                                                    (((i + j + 17) * 701) % 42 + 1) as u128,
                                                    (12345 + tx_count) as u64,
                                                );
                                            }
                                            tx_count += 1;
                                        }
                                    }
                                    *phase = RandomSinglePartPhases::WaitingForSixEpoch;
                                    assert_eq!(tx_count, 16 * 16);
                                }
                            }
                        }
                        RandomSinglePartPhases::WaitingForSixEpoch => {
                            if let NetworkRequests::Block { block } = msg {
                                assert!(block.header.height >= 13);
                                assert!(block.header.height <= 32);
                                if block.header.height >= 26 {
                                    println!("BLOCK HEIGHT {:?}", block.header.height);
                                    for i in 0..16 {
                                        for j in 0..16 {
                                            let amounts1 = amounts.clone();
                                            actix::spawn(
                                                connectors1.write().unwrap()[i]
                                                    .1
                                                    .send(Query {
                                                        path: "account/".to_owned()
                                                            + flat_validators[j],
                                                        data: vec![],
                                                    })
                                                    .then(move |res| {
                                                        let res_inner = res.unwrap();
                                                        if res_inner.is_ok() {
                                                            let query_response = res_inner.unwrap();
                                                            if let ViewAccount(
                                                                view_account_result,
                                                            ) = query_response
                                                            {
                                                                check_amount(
                                                                    amounts1,
                                                                    view_account_result
                                                                        .account_id
                                                                        .clone(),
                                                                    view_account_result.amount,
                                                                );
                                                            }
                                                        }
                                                        future::result(Ok(()))
                                                    }),
                                            );
                                        }
                                    }
                                }
                                if block.header.height == 32 {
                                    println!(
                                        "SEEN HEIGHTS SAME BLOCK {:?}",
                                        seen_heights_same_block.len()
                                    );
                                    assert_eq!(seen_heights_same_block.len(), 1);
                                    println!("SEEN RECEIPTS SIZE {:?}", seen_receipts_size.len());
                                    assert_ne!(seen_receipts_size.len(), 1);
                                    let amounts1 = amounts.clone();
                                    for flat_validator in &flat_validators {
                                        match amounts1
                                            .write()
                                            .unwrap()
                                            .entry(flat_validator.to_string())
                                        {
                                            Entry::Occupied(_) => {
                                                continue;
                                            }
                                            Entry::Vacant(entry) => {
                                                println!(
                                                    "VALIDATOR = {:?}, ENTRY = {:?}",
                                                    flat_validator, entry
                                                );
                                                assert!(false);
                                            }
                                        };
                                    }
                                    System::current().stop();
                                }
                            }
                            if let NetworkRequests::ChunkOnePartMessage {
                                header_and_part, ..
                            } = msg
                            {
                                seen_receipts_size.insert(header_and_part.receipts.len());
                                if header_and_part.header.height_created == 22 {
                                    seen_heights_same_block
                                        .insert(header_and_part.header.prev_block_hash);
                                }
                            }
                        }
                    };
                    (NetworkResponses::NoResponse, true)
                })),
            );

            near_network::test_utils::wait_or_panic(240000);
        })
        .unwrap();
    }

    /// Makes sure that 24 consecutive blocks are produced by 12 validators split into three epochs.
    /// This ensures that at no point validators get stuck with state sync
    #[test]
    fn test_catchup_sanity_blocks_produced() {
        let validator_groups = 2;
        init_integration_logger();
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

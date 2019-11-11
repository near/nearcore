#[cfg(test)]
mod tests {
    use actix::{Addr, System};
    use borsh::{BorshDeserialize, BorshSerialize};
    use futures::future;
    use futures::future::Future;
    use near_chain::test_utils::account_id_to_shard_id;
    use near_client::test_utils::setup_mock_all_validators;
    use near_client::{ClientActor, Query, ViewClientActor};
    use near_crypto::{InMemorySigner, KeyType};
    use near_network::{NetworkClientMessages, NetworkRequests, NetworkResponses, PeerInfo};
    use near_primitives::hash::hash as hash_func;
    use near_primitives::hash::CryptoHash;
    use near_primitives::receipt::Receipt;
    use near_primitives::test_utils::{init_integration_logger, init_test_logger};
    use near_primitives::transaction::SignedTransaction;
    use near_primitives::types::BlockIndex;
    use near_primitives::views::QueryResponse::ViewAccount;
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

    fn send_tx(
        connector: &Addr<ClientActor>,
        from: String,
        to: String,
        amount: u128,
        nonce: u64,
        block_hash: CryptoHash,
    ) {
        let signer = InMemorySigner::from_seed("test1", KeyType::ED25519, "test1");
        connector.do_send(NetworkClientMessages::Transaction(SignedTransaction::send_money(
            nonce, from, to, &signer, amount, block_hash,
        )));
    }

    enum ReceiptsSyncPhases {
        WaitingForFirstBlock,
        WaitingForSecondBlock,
        WaitingForDistantEpoch,
        VerifyingOutgoingReceipts,
        WaitingForValidate,
    }

    #[derive(
        Hash, Eq, PartialEq, Clone, Debug, BorshSerialize, BorshDeserialize, Default, PartialOrd,
    )]
    pub struct StateRequestStruct {
        pub shard_id: u64,
        pub hash: CryptoHash,
        pub need_header: bool,
        pub part_ids: Vec<u64>,
        pub num_parts: u64,
        pub account_id: String,
    }

    /// Sanity checks that the incoming and outgoing receipts are properly sent and received
    #[test]
    fn test_catchup_receipts_sync_third_epoch() {
        test_catchup_receipts_sync_common(13, 1, false)
    }

    #[test]
    /// WARNING! Set manually STATE_SYNC_TIMEOUT to 1 before running the test.
    /// Otherwise epochs will be changing faster than state sync.
    ///
    /// The test aggressively blocks lots of state requests
    /// and causes at least two timeouts per node (first for header, second for parts).
    fn test_catchup_receipts_sync_hold() {
        test_catchup_receipts_sync_common(13, 1, true)
    }

    #[test]
    #[ignore]
    fn test_catchup_receipts_sync_last_block() {
        test_catchup_receipts_sync_common(13, 5, false)
    }

    #[test]
    fn test_catchup_receipts_sync_distant_epoch() {
        test_catchup_receipts_sync_common(35, 1, false)
    }

    fn test_catchup_receipts_sync_common(wait_till: u64, send: u64, sync_hold: bool) {
        if !cfg!(feature = "expensive_tests") {
            return;
        }
        let validator_groups = 1;
        init_integration_logger();
        System::run(move || {
            let connectors: Arc<RwLock<Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>>> =
                Arc::new(RwLock::new(vec![]));

            let (validators, key_pairs) = get_validators_and_key_pairs();

            let phase = Arc::new(RwLock::new(ReceiptsSyncPhases::WaitingForFirstBlock));
            let seen_heights_with_receipts = Arc::new(RwLock::new(HashSet::<BlockIndex>::new()));
            let seen_hashes_with_state = Arc::new(RwLock::new(HashSet::<CryptoHash>::new()));

            let connectors1 = connectors.clone();
            let (_, conn) = setup_mock_all_validators(
                validators.clone(),
                key_pairs.clone(),
                validator_groups,
                true,
                1200,
                false,
                5,
                Arc::new(RwLock::new(move |_account_id: String, msg: &NetworkRequests| {
                    let account_from = "test3.3".to_string();
                    let account_to = "test1.1".to_string();
                    let source_shard_id = account_id_to_shard_id(&account_from, 4);
                    let destination_shard_id = account_id_to_shard_id(&account_to, 4);

                    let mut phase = phase.write().unwrap();
                    let mut seen_heights_with_receipts =
                        seen_heights_with_receipts.write().unwrap();
                    let mut seen_hashes_with_state = seen_hashes_with_state.write().unwrap();
                    match *phase {
                        ReceiptsSyncPhases::WaitingForFirstBlock => {
                            if let NetworkRequests::Block { block } = msg {
                                assert!(block.header.inner.height <= send);
                                // This tx is rather fragile, specifically it's important that
                                //   1. the `from` and `to` account are not in the same shard;
                                //   2. ideally the producer of the chunk at height 3 for the shard
                                //      in which `from` resides should not also be a block producer
                                //      at height 3
                                //   3. The `from` shard should also not match the block producer
                                //      for height 1, because such block producer will produce
                                //      the chunk for height 2 right away, before we manage to send
                                //      the transaction.
                                if block.header.inner.height == send {
                                    println!(
                                        "From shard: {}, to shard: {}",
                                        source_shard_id, destination_shard_id,
                                    );
                                    for i in 0..16 {
                                        send_tx(
                                            &connectors1.write().unwrap()[i].0,
                                            account_from.clone(),
                                            account_to.clone(),
                                            111,
                                            1,
                                            block.header.inner.prev_hash,
                                        );
                                    }
                                    *phase = ReceiptsSyncPhases::WaitingForSecondBlock;
                                }
                            }
                        }
                        ReceiptsSyncPhases::WaitingForSecondBlock => {
                            // This block now contains a chunk with the transaction sent above.
                            if let NetworkRequests::Block { block } = msg {
                                assert!(block.header.inner.height <= send + 1);
                                if block.header.inner.height == send + 1 {
                                    *phase = ReceiptsSyncPhases::WaitingForDistantEpoch;
                                }
                            }
                        }
                        ReceiptsSyncPhases::WaitingForDistantEpoch => {
                            // This block now contains a chunk with the transaction sent above.
                            if let NetworkRequests::Block { block } = msg {
                                assert!(block.header.inner.height >= send + 1);
                                assert!(block.header.inner.height <= wait_till);
                                if block.header.inner.height == wait_till {
                                    *phase = ReceiptsSyncPhases::VerifyingOutgoingReceipts;
                                }
                            }
                            if let NetworkRequests::PartialEncodedChunkMessage {
                                partial_encoded_chunk,
                                ..
                            } = msg
                            {
                                // The chunk producers in all epochs before `distant` need to be trying to
                                //     include the receipt. The `distant` epoch is the first one that
                                //     will get the receipt through the state sync.
                                let receipts: Vec<Receipt> = partial_encoded_chunk
                                    .receipts
                                    .iter()
                                    .map(|x| x.0.clone())
                                    .flatten()
                                    .collect();
                                if receipts.len() > 0 {
                                    assert_eq!(partial_encoded_chunk.shard_id, source_shard_id);
                                    seen_heights_with_receipts.insert(
                                        partial_encoded_chunk
                                            .header
                                            .as_ref()
                                            .unwrap()
                                            .inner
                                            .height_created,
                                    );
                                } else {
                                    assert_ne!(partial_encoded_chunk.shard_id, source_shard_id);
                                }
                                // Do not propagate any one parts, this will prevent any chunk from
                                //    being included in the block
                                return (NetworkResponses::NoResponse, false);
                            }
                            if let NetworkRequests::StateRequest {
                                shard_id,
                                hash,
                                need_header,
                                part_ids,
                                num_parts,
                                account_id,
                            } = msg
                            {
                                if sync_hold {
                                    let srs = StateRequestStruct {
                                        shard_id: *shard_id,
                                        hash: *hash,
                                        need_header: *need_header,
                                        part_ids: part_ids.clone(),
                                        num_parts: *num_parts,
                                        account_id: account_id.clone(),
                                    };
                                    if !seen_hashes_with_state
                                        .contains(&hash_func(&srs.try_to_vec().unwrap()))
                                    {
                                        seen_hashes_with_state
                                            .insert(hash_func(&srs.try_to_vec().unwrap()));
                                        return (NetworkResponses::NoResponse, false);
                                    }
                                }
                            }
                        }
                        ReceiptsSyncPhases::VerifyingOutgoingReceipts => {
                            for height in send + 2..=wait_till {
                                println!("checking height {:?} out of {:?}", height, wait_till);
                                assert!(seen_heights_with_receipts.contains(&height));
                            }
                            *phase = ReceiptsSyncPhases::WaitingForValidate;
                        }
                        ReceiptsSyncPhases::WaitingForValidate => {
                            // This block now contains a chunk with the transaction sent above.
                            if let NetworkRequests::Block { block } = msg {
                                assert!(block.header.inner.height >= wait_till);
                                assert!(block.header.inner.height <= wait_till + 20);
                                if block.header.inner.height == wait_till + 20 {
                                    System::current().stop();
                                }
                                if block.header.inner.height == wait_till + 10 {
                                    for i in 0..16 {
                                        actix::spawn(
                                            connectors1.write().unwrap()[i]
                                                .1
                                                .send(Query {
                                                    path: "account/".to_owned() + &account_to,
                                                    data: vec![],
                                                })
                                                .then(move |res| {
                                                    let res_inner = res.unwrap();
                                                    if res_inner.is_ok() {
                                                        let query_response = res_inner.unwrap();
                                                        if let ViewAccount(view_account_result) =
                                                            query_response
                                                        {
                                                            assert_eq!(
                                                                view_account_result.amount,
                                                                1111
                                                            );
                                                        }
                                                    }
                                                    future::result(Ok(()))
                                                }),
                                        );
                                    }
                                }
                            }
                        }
                    };
                    (NetworkResponses::NoResponse, true)
                })),
            );
            *connectors.write().unwrap() = conn;

            near_network::test_utils::wait_or_panic(240000);
        })
        .unwrap();
    }

    enum RandomSinglePartPhases {
        WaitingForFirstBlock,
        WaitingForThirdEpoch,
        WaitingForSixEpoch,
    }

    /// Verifies that fetching of random parts works properly by issuing transactions during the
    /// third epoch, and then making sure that the balances are correct for the next three epochs.
    /// If random one parts fetched during the epoch preceding the epoch a block producer is
    /// assigned to were to have incorrect receipts, the balances in the fourth epoch would have
    /// been incorrect due to wrong receipts applied during the third epoch.
    #[test]
    fn test_catchup_random_single_part_sync() {
        test_catchup_random_single_part_sync_common(false, false, 13)
    }

    // Same test as `test_catchup_random_single_part_sync`, but skips the chunks on height 14 and 15
    // It causes all the receipts to be applied only on height 16, which is the next epoch.
    // It tests that the incoming receipts are property synced through epochs
    #[test]
    #[ignore]
    fn test_catchup_random_single_part_sync_skip_15() {
        test_catchup_random_single_part_sync_common(true, false, 13)
    }

    #[test]
    #[ignore]
    fn test_catchup_random_single_part_sync_send_15() {
        test_catchup_random_single_part_sync_common(false, false, 15)
    }

    // Make sure that transactions are at least applied.
    #[test]
    fn test_catchup_random_single_part_sync_non_zero_amounts() {
        test_catchup_random_single_part_sync_common(false, true, 13)
    }

    // Use another height to send txs.
    #[test]
    fn test_catchup_random_single_part_sync_height_6() {
        test_catchup_random_single_part_sync_common(false, false, 6)
    }

    fn test_catchup_random_single_part_sync_common(skip_15: bool, non_zero: bool, height: u64) {
        if !cfg!(feature = "expensive_tests") {
            return;
        }
        let validator_groups = 2;
        init_integration_logger();
        System::run(move || {
            let connectors: Arc<RwLock<Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>>> =
                Arc::new(RwLock::new(vec![]));

            let (validators, key_pairs) = get_validators_and_key_pairs();
            let flat_validators = validators.iter().flatten().map(|x| *x).collect::<Vec<_>>();

            let phase = Arc::new(RwLock::new(RandomSinglePartPhases::WaitingForFirstBlock));
            let seen_heights_same_block = Arc::new(RwLock::new(HashSet::<CryptoHash>::new()));

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
                            if non_zero {
                                assert_ne!(amount % 100, 0);
                            } else {
                                assert_eq!(amount % 100, 0);
                            }
                            entry.insert(amount);
                        }
                    }
                };

            let connectors1 = connectors.clone();
            let (_, conn) = setup_mock_all_validators(
                validators.clone(),
                key_pairs.clone(),
                validator_groups,
                true,
                1500,
                false,
                5,
                Arc::new(RwLock::new(move |_account_id: String, msg: &NetworkRequests| {
                    let mut seen_heights_same_block = seen_heights_same_block.write().unwrap();
                    let mut phase = phase.write().unwrap();
                    match *phase {
                        RandomSinglePartPhases::WaitingForFirstBlock => {
                            if let NetworkRequests::Block { block } = msg {
                                assert_eq!(block.header.inner.height, 1);
                                *phase = RandomSinglePartPhases::WaitingForThirdEpoch;
                            }
                        }
                        RandomSinglePartPhases::WaitingForThirdEpoch => {
                            if let NetworkRequests::Block { block } = msg {
                                assert!(block.header.inner.height >= 2);
                                assert!(block.header.inner.height <= height);
                                let mut tx_count = 0;
                                if block.header.inner.height == height {
                                    for (i, validator1) in flat_validators.iter().enumerate() {
                                        for (j, validator2) in flat_validators.iter().enumerate() {
                                            let mut amount =
                                                (((i + j + 17) * 701) % 42 + 1) as u128;
                                            if non_zero {
                                                if i > j {
                                                    amount = 2;
                                                } else {
                                                    amount = 1;
                                                }
                                            }
                                            println!(
                                                "VALUES {:?} {:?} {:?}",
                                                validator1.to_string(),
                                                validator2.to_string(),
                                                amount
                                            );
                                            for conn in 0..flat_validators.len() {
                                                send_tx(
                                                    &connectors1.write().unwrap()[conn].0,
                                                    validator1.to_string(),
                                                    validator2.to_string(),
                                                    amount,
                                                    (12345 + tx_count) as u64,
                                                    block.header.inner.prev_hash,
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
                                assert!(block.header.inner.height >= height);
                                assert!(block.header.inner.height <= 32);
                                if block.header.inner.height >= 26 {
                                    println!("BLOCK HEIGHT {:?}", block.header.inner.height);
                                    for i in 0..16 {
                                        for j in 0..16 {
                                            let amounts1 = amounts.clone();
                                            let validator = flat_validators[j].to_string();
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
                                                                    validator,
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
                                if block.header.inner.height == 32 {
                                    println!(
                                        "SEEN HEIGHTS SAME BLOCK {:?}",
                                        seen_heights_same_block.len()
                                    );
                                    assert_eq!(seen_heights_same_block.len(), 1);
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
                            if let NetworkRequests::PartialEncodedChunkMessage {
                                partial_encoded_chunk,
                                ..
                            } = msg
                            {
                                if partial_encoded_chunk
                                    .header
                                    .as_ref()
                                    .unwrap()
                                    .inner
                                    .height_created
                                    == 22
                                {
                                    seen_heights_same_block.insert(
                                        partial_encoded_chunk
                                            .header
                                            .as_ref()
                                            .unwrap()
                                            .inner
                                            .prev_block_hash,
                                    );
                                }
                                if skip_15 {
                                    if partial_encoded_chunk
                                        .header
                                        .as_ref()
                                        .unwrap()
                                        .inner
                                        .height_created
                                        == 14
                                        || partial_encoded_chunk
                                            .header
                                            .as_ref()
                                            .unwrap()
                                            .inner
                                            .height_created
                                            == 15
                                    {
                                        return (NetworkResponses::NoResponse, false);
                                    }
                                }
                            }
                        }
                    };
                    (NetworkResponses::NoResponse, true)
                })),
            );
            *connectors.write().unwrap() = conn;

            near_network::test_utils::wait_or_panic(240000);
        })
        .unwrap();
    }

    /// Makes sure that 24 consecutive blocks are produced by 12 validators split into three epochs.
    /// This ensures that at no point validators get stuck with state sync
    #[test]
    fn test_catchup_sanity_blocks_produced() {
        if !cfg!(feature = "expensive_tests") {
            return;
        }
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

            let (_, conn) = setup_mock_all_validators(
                validators.clone(),
                key_pairs.clone(),
                validator_groups,
                true,
                400,
                false,
                5,
                Arc::new(RwLock::new(move |_account_id: String, msg: &NetworkRequests| {
                    if let NetworkRequests::Block { block } = msg {
                        check_height(block.hash(), block.header.inner.height);
                        check_height(block.header.inner.prev_hash, block.header.inner.height - 1);

                        if block.header.inner.height >= 25 {
                            System::current().stop();
                        }
                    }
                    (NetworkResponses::NoResponse, true)
                })),
            );
            *connectors.write().unwrap() = conn;

            near_network::test_utils::wait_or_panic(30000);
        })
        .unwrap();
    }
}

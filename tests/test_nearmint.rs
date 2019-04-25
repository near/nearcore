//#[cfg(test)]
//mod test {
//    use node_runtime::chain_spec::ChainSpec;
//    use primitives::crypto::signer::InMemorySigner;
//    use primitives::transaction::TransactionBody;
//    use protobuf::Message;
//    use serde_json::json;
//    use std::process::Command;
//    use std::thread;
//
//    fn start_nearmint() {
//        let chain_spec = ChainSpec::default_devnet();
//        let nearmint = Nearmint::new_for_test(chain_spec);
//        let mut command = Command::new("tendermint");
//        command.arg("node");
//        command.spawn().expect("fail to spawn tendermint");
//        let addr = "127.0.0.1:26658".parse().unwrap();
//        thread::spawn(|| abci::run(addr, nearmint));
//    }
//
//    #[test]
//    fn naive_test() {
//        start_nearmint();
//        let client = reqwest::Client::new();
//        let signer = InMemorySigner::from_seed("alice.near", "alice.near");
//        let tx: near_protos::signed_transaction::SignedTransaction =
//            TransactionBody::send_money(1, "alice.near", "bob.near", 10).sign(&signer).into();
//
//        let body = json!({"tx": hex::encode(tx.write_to_bytes().unwrap())});
//        let url = format!("{}{}", "127.0.0.1:3030", "/broadcast_tx_commit");
//        let mut response =
//            client.post(url.as_str()).body(serde_json::to_string(&body).unwrap()).send().unwrap();
//        println!("{:?}", response);
//    }
//}
